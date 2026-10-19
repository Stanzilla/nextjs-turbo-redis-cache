import { describe, it, expect, vi, afterEach } from 'vitest';
import { DeduplicatedRequestHandler } from '../../../src/DeduplicatedRequestHandler';
import type { SyncedMap } from '../../../src/SyncedMap';

type Get = (key: string, signal: AbortSignal) => Promise<string | null>;

// DRH only uses get/set/delete of the map, and only locally.
function localMap() {
  const map = new Map<string, Promise<string | null>>();
  return {
    get: (key: string) => map.get(key),
    set: async (key: string, value: Promise<string | null>) => {
      map.set(key, value);
    },
    delete: async (key: string) => {
      map.delete(key);
    },
  } as unknown as SyncedMap<Promise<string | null>>;
}

const signal = AbortSignal.timeout(10_000);

describe('DeduplicatedRequestHandler', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('shares one request between concurrent callers', async () => {
    const get = vi.fn<Get>(async () => 'value');
    const handler = new DeduplicatedRequestHandler(get, 1_000, localMap());

    const results = await Promise.all([
      handler.deduplicatedFunction('k')('k', signal),
      handler.deduplicatedFunction('k')('k', signal),
    ]);

    expect(results).toEqual(['value', 'value']);
    expect(get).toHaveBeenCalledOnce();
  });

  it('does not replay a rejected request to the next caller', async () => {
    const get = vi
      .fn<Get>()
      .mockRejectedValueOnce(new Error('connection lost'))
      .mockResolvedValueOnce('value');
    const handler = new DeduplicatedRequestHandler(get, 10_000, localMap());

    await expect(
      handler.deduplicatedFunction('k')('k', signal),
    ).rejects.toThrow('connection lost');

    await expect(handler.deduplicatedFunction('k')('k', signal)).resolves.toBe(
      'value',
    );
    expect(get).toHaveBeenCalledTimes(2);
  });

  it('the expiry timer of an older seed does not remove a newer seed', async () => {
    vi.useFakeTimers();
    const get = vi.fn<Get>(async () => 'from redis');
    const handler = new DeduplicatedRequestHandler(get, 100, localMap());
    const read = () => handler.deduplicatedFunction('k')('k', signal);

    handler.seedRequestReturn('k', 'v1');
    await vi.advanceTimersByTimeAsync(60);
    handler.seedRequestReturn('k', 'v2');
    await vi.advanceTimersByTimeAsync(60);

    expect(await read()).toBe('v2');
    expect(get).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(60);
    expect(await read()).toBe('from redis');
  });

  it('forget() removes only the entry it was given', async () => {
    const get = vi.fn<Get>(async () => 'from redis');
    const handler = new DeduplicatedRequestHandler(get, 10_000, localMap());
    const read = () => handler.deduplicatedFunction('k')('k', signal);

    const failedWrite = handler.seedRequestReturn('k', 'v1');
    handler.seedRequestReturn('k', 'v2');
    handler.forget('k', failedWrite);
    expect(await read()).toBe('v2');

    handler.forget('k', handler.seedRequestReturn('k', 'v3'));
    expect(await read()).toBe('from redis');
  });
});
