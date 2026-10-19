import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const hoisted = await vi.hoisted(async () =>
  (await import('./utils/redisMock')).createRedisMockState(),
);

vi.mock('redis', async () =>
  (await import('./utils/redisMock')).createRedisModuleMock(hoisted),
);

const KEY_PREFIX = 'stream:';

function entry(value: ReadableStream<Uint8Array>) {
  return {
    value,
    tags: ['tag-a'],
    stale: 1,
    timestamp: Date.now(),
    expire: 3600,
    revalidate: 60,
  };
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<string> {
  return new Response(stream).text();
}

describe('RedisCacheComponentsHandler.set() stream handling', () => {
  let errorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    hoisted.reset();
    vi.resetModules();
    errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function createHandler() {
    const { getRedisCacheComponentsHandler } = await import(
      '../../../src/CacheComponentsHandler'
    );
    return getRedisCacheComponentsHandler({
      keyPrefix: KEY_PREFIX,
      redisGetDeduplication: false,
    });
  }

  it('stores every chunk of a multi-chunk stream and get() returns the same bytes', async () => {
    const chunks = ['<div>', 'cached ', 'payload', '</div>'];
    const handler = await createHandler();

    await handler.set(
      'multi',
      Promise.resolve(
        entry(
          new ReadableStream<Uint8Array>({
            start(controller) {
              for (const chunk of chunks) {
                controller.enqueue(new TextEncoder().encode(chunk));
              }
              controller.close();
            },
          }),
        ),
      ),
    );

    const stored = JSON.parse(hoisted.store.get(KEY_PREFIX + 'multi')!);
    expect(Buffer.from(stored.value, 'base64').toString()).toBe(
      chunks.join(''),
    );

    const cached = await handler.get('multi', []);
    expect(cached).toBeDefined();
    expect(await readAll(cached!.value)).toBe(chunks.join(''));
  });

  it('stores nothing and rejects with the stream error when the stream fails during reading', async () => {
    const handler = await createHandler();

    await expect(
      handler.set(
        'broken',
        Promise.resolve(
          entry(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new TextEncoder().encode('partial'));
                controller.error(new Error('render failed'));
              },
            }),
          ),
        ),
      ),
    ).rejects.toThrow('render failed');

    expect(hoisted.store.has(KEY_PREFIX + 'broken')).toBe(false);
    expect(errorSpy).toHaveBeenCalled();
  });
});
