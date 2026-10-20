import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import RedisStringsHandler from '../../../src/RedisStringsHandler';

// One in-memory Redis shared by all handler instances: a string store and a
// pub/sub bus that delivers each message to every subscriber.
const hoisted = vi.hoisted(() => {
  const store = new Map<string, string>();
  // A dropped client stops getting messages, like a broken socket.
  type Listener = ((message: string) => void) & { dropped?: () => boolean };
  const subscribers = new Map<string, Set<Listener>>();
  const published: { channel: string; message: string }[] = [];
  const control = {
    failSet: false,
    // When set, GET reads the store at call time but resolves only after
    // this promise, like a slow Redis reply.
    holdGet: undefined as Promise<void> | undefined,
    // When set, SET stores the value only after this promise.
    holdSet: undefined as Promise<void> | undefined,
  };
  return { store, subscribers, published, control };
});

vi.mock('redis', () => {
  const createClient = () => {
    const errorListeners = new Set<(error: Error) => void>();
    const client = {
      isReady: true,
      isOpen: true,
      dropped: false,
      on: vi.fn((event: string, listener: (error: Error) => void) => {
        if (event === 'error') errorListeners.add(listener);
      }),
      removeAllListeners: vi.fn(() => errorListeners.clear()),
      emitError: (error: Error) => {
        for (const listener of [...errorListeners]) listener(error);
      },
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      quit: vi.fn(async () => undefined),
      duplicate: vi.fn(() => createClient()),
      configGet: vi.fn(async () => ({ 'notify-keyspace-events': 'Exe' })),
      subscribe: vi.fn(
        async (channel: string, listener: (message: string) => void) => {
          if (!hoisted.subscribers.has(channel)) {
            hoisted.subscribers.set(channel, new Set());
          }
          hoisted.subscribers
            .get(channel)!
            .add(Object.assign(listener, { dropped: () => client.dropped }));
        },
      ),
      publish: vi.fn(async (channel: string, message: string) => {
        hoisted.published.push({ channel, message });
        const listeners = hoisted.subscribers.get(channel) ?? new Set();
        for (const listener of listeners) {
          if (!listener.dropped?.()) listener(message);
        }
        return listeners.size;
      }),
      get: vi.fn(async (_opts: unknown, key: string) => {
        const value = hoisted.store.get(key) ?? null;
        await hoisted.control.holdGet;
        return value;
      }),
      set: vi.fn(async (key: string, value: string) => {
        if (hoisted.control.failSet) throw new Error('SET failed');
        await hoisted.control.holdSet;
        hoisted.store.set(key, value);
        return 'OK';
      }),
      unlink: vi.fn(async (keys: string[]) => {
        for (const key of keys) hoisted.store.delete(key);
        return keys.length;
      }),
      sendCommand: vi.fn(async (args: string[]) => {
        const [command] = args;
        if (command === 'HSCAN' || command === 'SCAN') {
          return ['0', []];
        }
        throw new Error(`Unexpected sendCommand: ${command}`);
      }),
      eval: vi.fn(
        async (
          _script: string,
          options: { keys: string[]; arguments: string[] },
        ) => {
          const [key] = options.keys;
          if (hoisted.store.get(key) !== options.arguments[0]) return 0;
          hoisted.store.delete(key);
          return 1;
        },
      ),
      hSet: vi.fn(async () => 1),
      hDel: vi.fn(async () => 1),
    };
    return client;
  };
  return {
    createClient,
    commandOptions: vi.fn((opts) => opts),
  };
});

const KEY = '/page';
const setCtx = {
  isRoutePPREnabled: false,
  isFallback: false,
  cacheControl: { revalidate: 60, expire: 600 },
};
const getCtx = {
  kind: 'PAGES' as const,
  isRoutePPREnabled: false,
  isFallback: false,
};

function page(html: string) {
  return {
    kind: 'PAGES' as const,
    html,
    pageData: {},
    headers: {},
    status: 200,
  };
}

// One handler per pod/worker. They share Redis, but each has its own
// in-memory read cache.
function createInstance(inMemoryCachingTime: number) {
  return new RedisStringsHandler({
    redisUrl: 'redis://localhost:6379',
    keyPrefix: 'test:',
    database: 0,
    redisGetDeduplication: true,
    inMemoryCachingTime,
  });
}

async function readHtml(handler: RedisStringsHandler) {
  const entry = await handler.get(KEY, getCtx);
  return (entry?.value as { html?: string } | undefined)?.html ?? null;
}

type SubscriberClient = { dropped: boolean; emitError(error: Error): void };
type WithSubscriptions = {
  subscriptions: { client: SubscriberClient; ready(): Promise<void> };
};

// The subscriber of an instance loses its connection: messages published
// meanwhile do not arrive. The error then makes the instance subscribe again
// on a new connection.
async function loseSubscriber(
  map: WithSubscriptions,
  during: () => Promise<void>,
) {
  vi.spyOn(console, 'error').mockImplementation(() => undefined);
  const subscriber = map.subscriptions.client;
  subscriber.dropped = true;
  await during();
  subscriber.emitError(new Error('Socket closed unexpectedly'));
  await map.subscriptions.ready();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function publishRaw(channel: string, keys: string[]) {
  const listeners = hoisted.subscribers.get(channel) ?? new Set();
  for (const listener of listeners) {
    listener(JSON.stringify({ type: 'delete', keys }));
  }
}

function readCacheMessages() {
  return hoisted.published.filter(({ channel }) =>
    channel.endsWith('inMemoryDeduplicationCache'),
  );
}

describe('in-memory read cache across instances', () => {
  beforeEach(() => {
    hoisted.store.clear();
    hoisted.subscribers.clear();
    hoisted.published.length = 0;
    hoisted.control.failSet = false;
    hoisted.control.holdGet = undefined;
    hoisted.control.holdSet = undefined;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('does not publish a sync message when a cached read expires', async () => {
    const writer = createInstance(20);
    const reader = createInstance(20);
    for (const key of ['/a', '/b', '/c']) {
      await writer.set(key, page(key), setCtx);
    }
    await new Promise((resolve) => setTimeout(resolve, 60));
    hoisted.published.length = 0;

    for (const key of ['/a', '/b', '/c']) {
      await reader.get(key, getCtx);
    }
    await new Promise((resolve) => setTimeout(resolve, 60));

    expect(readCacheMessages()).toEqual([]);
  });

  it('serves the new value on another instance right after set()', async () => {
    const writer = createInstance(10_000);
    const reader = createInstance(10_000);

    await writer.set(KEY, page('v1'), setCtx);
    expect(await readHtml(reader)).toBe('v1');

    await writer.set(KEY, page('v2'), setCtx);

    expect(await readHtml(reader)).toBe('v2');
    expect(await readHtml(writer)).toBe('v2');
  });

  it('serves the new value on the writing instance while its Redis SET runs', async () => {
    const writer = createInstance(10_000);
    // A cache miss before the write, as when Next.js fetches the data.
    expect(await readHtml(writer)).toBeNull();

    let releaseSet!: () => void;
    hoisted.control.holdSet = new Promise((resolve) => {
      releaseSet = resolve;
    });
    const write = writer.set(KEY, page('v1'), setCtx);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(await readHtml(writer)).toBe('v1');

    releaseSet();
    await write;
    expect(await readHtml(writer)).toBe('v1');
  });

  it('keeps the cached copy on other instances when the Redis SET fails', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const writer = createInstance(10_000);
    const reader = createInstance(10_000);

    await writer.set(KEY, page('v1'), setCtx);
    expect(await readHtml(reader)).toBe('v1');
    hoisted.published.length = 0;

    hoisted.control.failSet = true;
    await expect(writer.set(KEY, page('v2'), setCtx)).rejects.toThrow(
      'SET failed',
    );

    expect(readCacheMessages()).toEqual([]);
    expect(await readHtml(reader)).toBe('v1');
  });

  it('does not serve the value of a failed Redis SET on the writing instance', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const writer = createInstance(10_000);
    await writer.set(KEY, page('v1'), setCtx);
    // Let the successful SET remove the seed of v1.
    await new Promise((resolve) => setTimeout(resolve, 0));

    hoisted.control.failSet = true;
    await expect(writer.set(KEY, page('v2'), setCtx)).rejects.toThrow(
      'SET failed',
    );

    expect(await readHtml(writer)).toBe('v1');
  });

  it('does not keep an old value read while set() runs on another instance', async () => {
    const writer = createInstance(10_000);
    const reader = createInstance(10_000);
    await writer.set(KEY, page('v1'), setCtx);

    let releaseGet!: () => void;
    hoisted.control.holdGet = new Promise((resolve) => {
      releaseGet = resolve;
    });
    const slowRead = readHtml(reader);
    // Let the reader send its GET before the writer changes the value.
    await new Promise((resolve) => setTimeout(resolve, 0));
    hoisted.control.holdGet = undefined;

    await writer.set(KEY, page('v2'), setCtx);
    releaseGet();

    expect(await slowRead).toBe('v1');
    expect(await readHtml(reader)).toBe('v2');
  });

  it('drops the cached copy on another instance after revalidateTag()', async () => {
    const writer = createInstance(10_000);
    const reader = createInstance(10_000);

    await writer.set(KEY, page('v1'), setCtx);
    expect(await readHtml(reader)).toBe('v1');

    await writer.revalidateTag(`_N_T_${KEY}`);

    expect(await readHtml(reader)).toBeNull();
  });

  it('does not serve a value that changed while its subscriber was lost', async () => {
    const writer = createInstance(10_000);
    const reader = createInstance(10_000);
    await writer.set(KEY, page('v1'), setCtx);
    expect(await readHtml(reader)).toBe('v1');

    await loseSubscriber(
      (reader as unknown as { sharedTagsMap: WithSubscriptions }).sharedTagsMap,
      () => writer.set(KEY, page('v2'), setCtx),
    );

    expect(await readHtml(reader)).toBe('v2');
  });

  it('drops the cached copy on a delete message from an older version', async () => {
    const writer = createInstance(10_000);
    const reader = createInstance(10_000);
    await writer.set(KEY, page('v2'), setCtx);
    const v2 = hoisted.store.get('test:' + KEY)!;
    await writer.set(KEY, page('v1'), setCtx);
    expect(await readHtml(reader)).toBe('v1');
    // A change that this instance does not see, for example from a script.
    hoisted.store.set('test:' + KEY, v2);
    expect(await readHtml(reader)).toBe('v1');

    publishRaw('test::sync-channel:inMemoryDeduplicationCache', [KEY]);

    expect(await readHtml(reader)).toBe('v2');
  });
});

describe('Cache Components in-memory read cache across instances', () => {
  beforeEach(() => {
    hoisted.store.clear();
    hoisted.subscribers.clear();
    hoisted.published.length = 0;
    hoisted.control.failSet = false;
  });

  async function createCacheComponentsInstance(redisGetDeduplication = true) {
    vi.resetModules();
    const { getRedisCacheComponentsHandler } = await import(
      '../../../src/CacheComponentsHandler'
    );
    return getRedisCacheComponentsHandler({
      redisUrl: 'redis://localhost:6379',
      keyPrefix: 'test:',
      database: 0,
      redisGetDeduplication,
      inMemoryCachingTime: 10_000,
    });
  }

  type Handler = Awaited<ReturnType<typeof createCacheComponentsInstance>>;

  function write(handler: Handler, text: string, tags: string[] = []) {
    return handler.set(
      KEY,
      Promise.resolve({
        value: new Blob([text]).stream(),
        tags,
        stale: 60,
        timestamp: Date.now(),
        expire: 600,
        revalidate: 60,
      }),
    );
  }

  async function read(handler: Handler) {
    const entry = await handler.get(KEY, []);
    return entry ? new Response(entry.value).text() : null;
  }

  it.each([true, false])(
    'serves the new value on another instance right after set() (writer cache %s)',
    async (writerDeduplication) => {
      const writer = await createCacheComponentsInstance(writerDeduplication);
      const reader = await createCacheComponentsInstance();

      await write(writer, 'v1');
      expect(await read(reader)).toBe('v1');

      await write(writer, 'v2');
      expect(await read(reader)).toBe('v2');
    },
  );

  it('drops a cached miss on another instance after set()', async () => {
    const writer = await createCacheComponentsInstance();
    const reader = await createCacheComponentsInstance();
    expect(await read(reader)).toBeNull();

    await write(writer, 'v1');

    expect(await read(reader)).toBe('v1');
  });

  it('does not serve a value that changed while its subscriber was lost', async () => {
    const writer = await createCacheComponentsInstance();
    const reader = await createCacheComponentsInstance();
    await write(writer, 'v1');
    expect(await read(reader)).toBe('v1');

    await loseSubscriber(
      (reader as unknown as { revalidatedTagsMap: WithSubscriptions })
        .revalidatedTagsMap,
      () => write(writer, 'v2'),
    );

    expect(await read(reader)).toBe('v2');
  });

  it('drops the cached copy on a delete message from an older version', async () => {
    const writer = await createCacheComponentsInstance();
    const reader = await createCacheComponentsInstance();
    await write(writer, 'v2');
    const v2 = hoisted.store.get('test:' + KEY)!;
    await write(writer, 'v1');
    expect(await read(reader)).toBe('v1');
    hoisted.store.set('test:' + KEY, v2);
    expect(await read(reader)).toBe('v1');

    publishRaw(
      'test::sync-channel:__cacheComponents_inMemoryDeduplicationCache__',
      [KEY],
    );

    expect(await read(reader)).toBe('v2');
  });

  it('does not serve an in-memory copy on another instance after updateTags()', async () => {
    const writer = await createCacheComponentsInstance();
    const reader = await createCacheComponentsInstance();
    await write(writer, 'v1', ['posts']);
    expect(await read(reader)).toBe('v1');

    await new Promise((resolve) => setTimeout(resolve, 2));
    await writer.updateTags(['posts']);

    expect(await read(reader)).toBeNull();
    expect(hoisted.store.has('test:' + KEY)).toBe(false);
  });
});
