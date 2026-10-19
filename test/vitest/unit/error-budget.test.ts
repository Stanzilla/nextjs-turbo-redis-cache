import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

type MockClient = {
  isOpen: boolean;
  isReady: boolean;
  emit: (event: string, ...args: unknown[]) => void;
  connect: ReturnType<typeof vi.fn>;
  disconnect: ReturnType<typeof vi.fn>;
};

const failures = vi.hoisted(() => ({ syncedMapConstructor: false }));

vi.mock('redis', () => ({
  createClient: vi.fn(() => {
    const listeners = new Map<string, ((...args: unknown[]) => void)[]>();
    return {
      // Open but not ready: the error listener does not try to reconnect.
      isOpen: true,
      isReady: false,
      on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
        listeners.set(event, [...(listeners.get(event) ?? []), cb]);
      }),
      emit: (event: string, ...args: unknown[]) => {
        (listeners.get(event) ?? []).forEach((cb) => cb(...args));
      },
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      quit: vi.fn(async () => undefined),
      get: vi.fn(async () => null),
    };
  }),
  commandOptions: vi.fn((opts) => opts),
}));

vi.mock('../../../src/SyncedMap', () => {
  class SyncedMap {
    static waitUntilAllReady = vi.fn(async () => undefined);
    get = vi.fn(() => undefined);
    set = vi.fn(async () => undefined);
    delete = vi.fn(async () => undefined);
    entries = vi.fn(function* () {});
    constructor() {
      if (failures.syncedMapConstructor) throw new Error('setup failed');
    }
  }
  return { SyncedMap };
});

const threshold = 2;

async function createStringsHandler() {
  const { default: RedisStringsHandler } = await import(
    '../../../src/RedisStringsHandler'
  );
  const handler = new RedisStringsHandler({
    redisUrl: 'redis://127.0.0.1:6399',
    killContainerOnErrorThreshold: threshold,
  });
  return {
    client: (handler as unknown as { client: MockClient }).client,
    readWhileNotReady: () => handler.get('key', { kind: 'APP_PAGE' } as never),
  };
}

async function createComponentsHandler() {
  const { getRedisCacheComponentsHandler } = await import('../../../src');
  const handler = getRedisCacheComponentsHandler({
    redisUrl: 'redis://127.0.0.1:6399',
    killContainerOnErrorThreshold: threshold,
  });
  return {
    client: (handler as unknown as { client: MockClient }).client,
    readWhileNotReady: () => handler.get('key', []),
  };
}

describe.each([
  ['RedisStringsHandler', createStringsHandler],
  ['RedisCacheComponentsHandler', createComponentsHandler],
])('%s error budget', (_name, create) => {
  let exit: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetModules();
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    vi.spyOn(console, 'error').mockImplementation(() => {});
    exit = vi
      .spyOn(process, 'exit')
      .mockImplementation((() => undefined) as never);
  });

  afterEach(() => {
    failures.syncedMapConstructor = false;
    vi.useRealTimers();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('resets the count when the client is ready again', async () => {
    const { client } = await create();

    client.emit('error', new Error('first'));
    client.emit('ready');
    client.emit('error', new Error('second'));
    await vi.advanceTimersByTimeAsync(1000);

    expect(exit).not.toHaveBeenCalled();
  });

  it('exits exactly once when the threshold is reached', async () => {
    const { client } = await create();

    client.emit('error', new Error('first'));
    client.emit('error', new Error('second'));
    client.emit('error', new Error('third'));
    await vi.advanceTimersByTimeAsync(1000);

    expect(client.disconnect).toHaveBeenCalledTimes(1);
    expect(exit).toHaveBeenCalledTimes(1);
  });

  it('does not count reads while the client is not ready', async () => {
    const { client, readWhileNotReady } = await create();
    client.isOpen = false;
    client.connect.mockRejectedValue(new Error('ECONNREFUSED'));

    for (let i = 0; i < 100; i++) {
      await readWhileNotReady();
    }
    client.isOpen = true;
    client.emit('error', new Error('only real failure'));
    await vi.advanceTimersByTimeAsync(1000);

    expect(exit).not.toHaveBeenCalled();
  });

  it('counts constructor failures across instances and closes each client', async () => {
    const { createClient } = await import('redis');
    vi.mocked(createClient).mockClear();
    failures.syncedMapConstructor = true;

    for (let i = 0; i < threshold; i++) {
      await expect(create()).rejects.toThrow('setup failed');
    }
    await vi.advanceTimersByTimeAsync(1000);

    expect(exit).toHaveBeenCalledTimes(1);
    const clients = vi
      .mocked(createClient)
      .mock.results.map((result) => result.value as MockClient);
    expect(clients).toHaveLength(threshold);
    for (const client of clients) {
      expect(client.disconnect).toHaveBeenCalled();
    }
  });
});
