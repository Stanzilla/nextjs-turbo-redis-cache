import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// A subscriber connection cannot send normal commands, so each handler needs
// one. It must not need one per map: at 3 connections per pod, a large
// deployment reaches the Redis client limit sooner.

const redisState = vi.hoisted(() => ({
  duplicates: [] as unknown[],
  subscribeFailuresLeft: 0,
}));

type MockClient = ReturnType<typeof createMockClient>;

function createMockClient() {
  const listeners = new Map<string, Set<(...args: unknown[]) => void>>();
  const client = {
    isOpen: true,
    isReady: true,
    on(event: string, cb: (...args: unknown[]) => void) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event)!.add(cb);
      return client;
    },
    emit(event: string, ...args: unknown[]) {
      for (const cb of listeners.get(event) ?? []) cb(...args);
    },
    removeAllListeners(event?: string) {
      if (event) listeners.delete(event);
      else listeners.clear();
      return client;
    },
    async connect() {},
    async disconnect() {},
    async quit() {},
    duplicate(): MockClient {
      const duplicate = createMockClient();
      redisState.duplicates.push(duplicate);
      return duplicate;
    },
    async configGet() {
      return { 'notify-keyspace-events': 'Exe' };
    },
    async subscribe() {
      if (redisState.subscribeFailuresLeft > 0) {
        redisState.subscribeFailuresLeft--;
        throw new Error('Mock subscribe failure');
      }
    },
    async sendCommand() {
      return ['0', []];
    },
    async eval() {
      return [];
    },
    async get() {
      return null;
    },
  };
  return client;
}

vi.mock('redis', () => ({
  createClient: vi.fn(() => createMockClient()),
  commandOptions: vi.fn((opts) => opts),
}));

const subscribers = () => redisState.duplicates as MockClient[];

async function allMapsReady(handler: object) {
  const { SyncedMap } = await import('../../../src/SyncedMap');
  const maps = Object.values(handler).filter(
    (value) => value instanceof SyncedMap,
  );
  expect(maps.length).toBeGreaterThan(1);
  await vi.waitFor(() => SyncedMap.waitUntilAllReady(maps), {
    timeout: 3_000,
  });
}

describe('one subscriber connection per handler', () => {
  beforeEach(() => {
    redisState.duplicates = [];
    redisState.subscribeFailuresLeft = 0;
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('RedisStringsHandler opens one subscriber for all of its maps', async () => {
    const { default: RedisStringsHandler } = await import(
      '../../../src/RedisStringsHandler'
    );
    const handler = new RedisStringsHandler({ keyPrefix: 'subs:' });
    await allMapsReady(handler);

    expect(subscribers()).toHaveLength(1);
  });

  it('CacheComponentsHandler opens one subscriber for all of its maps', async () => {
    vi.resetModules();
    const { getRedisCacheComponentsHandler } = await import('../../../src');
    const handler = getRedisCacheComponentsHandler({ keyPrefix: 'subs:' });
    await allMapsReady(handler);

    expect(subscribers()).toHaveLength(1);
  });

  it('a lost subscriber makes every map reload, with one new connection', async () => {
    const { default: RedisStringsHandler } = await import(
      '../../../src/RedisStringsHandler'
    );
    const handler = new RedisStringsHandler({ keyPrefix: 'subs:' });
    await allMapsReady(handler);
    const { SyncedMap } = await import('../../../src/SyncedMap');
    const sharedTagsMap = (handler as unknown as { sharedTagsMap: never })
      .sharedTagsMap as InstanceType<typeof SyncedMap>;

    subscribers()[0].emit('error', new Error('Socket closed unexpectedly'));
    await expect(sharedTagsMap.waitUntilReady(0)).rejects.toThrow();

    await allMapsReady(handler);
    expect(subscribers()).toHaveLength(2);
  });
});

describe('createRedisSubscriptions()', () => {
  let originalSetTimeout: typeof global.setTimeout;

  beforeEach(() => {
    redisState.duplicates = [];
    redisState.subscribeFailuresLeft = 0;
    originalSetTimeout = global.setTimeout;
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    global.setTimeout = originalSetTimeout;
    vi.restoreAllMocks();
  });

  async function subscriptionsWithListener() {
    const { createRedisSubscriptions } = await import(
      '../../../src/utils/redisSubscriptions'
    );
    const subscriptions = createRedisSubscriptions(createMockClient() as never);
    subscriptions.add({ channels: [['sync', () => {}]], onError: () => {} });
    return subscriptions;
  }

  it('waits 500ms before the first retry', async () => {
    const subscriptions = await subscriptionsWithListener();
    const capturedDelays: number[] = [];
    global.setTimeout = ((callback: () => void, delay?: number) => {
      capturedDelays.push(Number(delay ?? 0));
      return originalSetTimeout(callback, 0);
    }) as typeof global.setTimeout;
    redisState.subscribeFailuresLeft = 3;

    await subscriptions.ready();

    expect(capturedDelays[0]).toBe(500);
  });

  it('ignores a delayed error from a replaced subscriber', async () => {
    const subscriptions = await subscriptionsWithListener();
    await subscriptions.ready();
    const oldSubscriber = subscriptions.client as unknown as MockClient;

    oldSubscriber.emit('error', new Error('Socket closed unexpectedly'));
    await subscriptions.ready();
    const newSubscriber = subscriptions.client;
    expect(newSubscriber).not.toBe(oldSubscriber);

    oldSubscriber.emit('error', new Error('delayed zombie error'));
    await subscriptions.ready();

    expect(subscriptions.client).toBe(newSubscriber);
  });
});
