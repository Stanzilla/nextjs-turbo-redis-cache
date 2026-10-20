import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import RedisStringsHandler from '../../../src/RedisStringsHandler';
import CachedHandler from '../../../src/CachedHandler';
import {
  getRedisCacheComponentsHandler,
  redisCacheHandler,
} from '../../../src/CacheComponentsHandler';
import { RedisNotReadyError } from '../../../src/utils/redisConnection';
import { startPausableRedisServer } from './utils/pausableRedisServer';

const failures = vi.hoisted(() => ({ createAbortableGet: false }));

vi.mock('../../../src/utils/abortableGet', async (importOriginal) => {
  const original =
    await importOriginal<typeof import('../../../src/utils/abortableGet')>();
  return {
    ...original,
    createAbortableGet: (
      ...args: Parameters<typeof original.createAbortableGet>
    ) => {
      if (failures.createAbortableGet) {
        throw new Error('constructor step failed');
      }
      return original.createAbortableGet(...args);
    },
  };
});

describe('closing a handler', () => {
  let server: Awaited<ReturnType<typeof startPausableRedisServer>>;
  let options: {
    redisUrl: string;
    killContainerOnErrorThreshold: number;
    redisGetDeduplication: boolean;
  };

  beforeEach(async () => {
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(console, 'log').mockImplementation(() => {});
    failures.createAbortableGet = false;
    server = await startPausableRedisServer();
    options = {
      redisUrl: server.url,
      killContainerOnErrorThreshold: 0,
      redisGetDeduplication: true,
    };
  });

  afterEach(async () => {
    await server.close();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('RedisStringsHandler.close() closes its connections, and later calls fail fast', async () => {
    const handler = new RedisStringsHandler({ ...options, keyPrefix: 'a:' });
    await vi.waitFor(() => expect(server.connectionCount).toBe(2));

    await handler.close();
    await handler.close();

    await vi.waitFor(() => expect(server.connectionCount).toBe(0));

    const started = Date.now();
    expect(
      await handler.get('/page', { kind: 'APP_PAGE' } as never),
    ).toBeNull();
    await expect(
      handler.set('/page', null, {} as never),
    ).rejects.toBeInstanceOf(RedisNotReadyError);
    await expect(handler.revalidateTag('t')).rejects.toBeInstanceOf(
      RedisNotReadyError,
    );
    expect(Date.now() - started).toBeLessThan(500);
    expect(server.connectionCount).toBe(0);
  });

  it('CachedHandler.close() closes the shared handler, and the next one connects again', async () => {
    const first = new CachedHandler({ ...options, keyPrefix: 'b:' });
    await vi.waitFor(() => expect(server.connectionCount).toBe(2));

    await first.close();
    await vi.waitFor(() => expect(server.connectionCount).toBe(0));

    const second = new CachedHandler({ ...options, keyPrefix: 'b:' });
    await vi.waitFor(() => expect(server.connectionCount).toBe(2));
    await second.close();
  });

  it('the Cache Components singleton is created again after close()', async () => {
    const first = getRedisCacheComponentsHandler({
      ...options,
      keyPrefix: 'c:',
    });
    await vi.waitFor(() => expect(server.connectionCount).toBe(2));

    await redisCacheHandler.close();
    await vi.waitFor(() => expect(server.connectionCount).toBe(0));

    const second = getRedisCacheComponentsHandler({
      ...options,
      keyPrefix: 'c:',
    });
    expect(second).not.toBe(first);
    await vi.waitFor(() => expect(server.connectionCount).toBe(2));
    await second.close();
  });

  it('a failed constructor closes the subscriber that its maps opened', async () => {
    failures.createAbortableGet = true;

    expect(
      () => new RedisStringsHandler({ ...options, keyPrefix: 'd:' }),
    ).toThrow('constructor step failed');

    await new Promise((r) => setTimeout(r, 300));
    expect(server.connectionCount).toBe(0);
  });
});
