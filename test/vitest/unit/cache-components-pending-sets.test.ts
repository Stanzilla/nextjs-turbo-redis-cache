import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const hoisted = vi.hoisted(() => {
  const store = new Map<string, string>();
  return { store };
});

vi.mock('redis', () => {
  const createClient = () => ({
    isReady: true,
    isOpen: true,
    on: vi.fn(),
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(),
    quit: vi.fn(),
    duplicate: vi.fn(() => ({
      connect: vi.fn(async () => undefined),
      subscribe: vi.fn(async () => undefined),
      on: vi.fn(),
      quit: vi.fn(async () => undefined),
      configGet: vi.fn(async () => ({ 'notify-keyspace-events': 'Exe' })),
      isOpen: true,
      isReady: true,
    })),
    get: vi.fn(
      async (_opts: unknown, key: string) => hoisted.store.get(key) ?? null,
    ),
    sendCommand: vi.fn(async (args: string[]) => {
      const [command] = args;
      if (command === 'HSCAN' || command === 'SCAN') {
        return ['0', []];
      }
      throw new Error(`Unexpected sendCommand: ${command}`);
    }),
    hSet: vi.fn(async () => 1),
    hDel: vi.fn(async () => 1),
    publish: vi.fn(async () => 1),
    unlink: vi.fn(async () => 1),
    set: vi.fn(async (key: string, value: string) => {
      hoisted.store.set(key, value);
      return 'OK';
    }),
  });

  return {
    createClient,
    commandOptions: vi.fn((opts) => opts),
  };
});

const KEY_PREFIX = 'pending:';

function entry(text: string) {
  return {
    value: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(text));
        controller.close();
      },
    }),
    tags: [],
    stale: 1,
    timestamp: Date.now(),
    expire: 3600,
    revalidate: 60,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('RedisCacheComponentsHandler.get() during a set() of the same key', () => {
  beforeEach(() => {
    hoisted.store.clear();
    vi.resetModules();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function createHandler(getTimeoutMs?: number) {
    const { getRedisCacheComponentsHandler } = await import(
      '../../../src/CacheComponentsHandler'
    );
    return getRedisCacheComponentsHandler({
      keyPrefix: KEY_PREFIX,
      redisGetDeduplication: false,
      ...(getTimeoutMs ? { getTimeoutMs } : {}),
    });
  }

  it('waits for the pending set and returns the new entry, as DefaultCacheHandler does', async () => {
    const handler = await createHandler();
    const render = deferred<ReturnType<typeof entry>>();

    const setting = handler.set('page', render.promise);
    const reading = handler.get('page', []);
    render.resolve(entry('fresh'));
    await setting;

    const cached = await reading;
    expect(cached).toBeDefined();
    expect(await new Response(cached!.value).text()).toBe('fresh');
  });

  it('reads Redis after a failed set and does not reject', async () => {
    const handler = await createHandler();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    await handler.set('page', Promise.resolve(entry('old')));
    const render = deferred<ReturnType<typeof entry>>();

    const setting = handler.set('page', render.promise);
    const reading = handler.get('page', []);
    render.reject(new Error('render failed'));
    await expect(setting).rejects.toThrow('render failed');

    const cached = await reading;
    expect(await new Response(cached!.value).text()).toBe('old');
  });

  it('stops waiting for a slow render after getTimeoutMs', async () => {
    const handler = await createHandler(50);
    void handler.set('page', new Promise(() => undefined));

    await expect(handler.get('page', [])).resolves.toBeUndefined();
  });
});
