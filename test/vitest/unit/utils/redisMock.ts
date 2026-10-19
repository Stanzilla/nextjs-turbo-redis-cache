import { vi } from 'vitest';

/**
 * Shared in-memory stand-in for the `redis` module in unit tests.
 *
 * Create the state in `vi.hoisted` so the test and the mocked client see the
 * same store, then build the module in `vi.mock`:
 *
 *   const redis = await vi.hoisted(async () =>
 *     (await import('./utils/redisMock')).createRedisMockState(),
 *   );
 *   vi.mock('redis', async () =>
 *     (await import('./utils/redisMock')).createRedisModuleMock(redis),
 *   );
 *
 * Each call to `createClient()` or `duplicate()` returns a new client. Pass
 * `overrides` to replace methods for a test file that needs special behavior.
 */
export function createRedisMockState() {
  const state = {
    store: new Map<string, string>(),
    /** EX (seconds) from the last SET of each key, if any. */
    expiry: new Map<string, number>(),
    unlinkedKeys: [] as string[],
    reset() {
      state.store.clear();
      state.expiry.clear();
      state.unlinkedKeys.length = 0;
    },
  };
  return state;
}

export type RedisMockState = ReturnType<typeof createRedisMockState>;

export function createRedisModuleMock(
  state: RedisMockState,
  overrides: () => Record<string, unknown> = () => ({}),
) {
  const deleteKey = (key: string) => {
    state.unlinkedKeys.push(key);
    state.store.delete(key);
    state.expiry.delete(key);
  };

  const createClient = (): Record<string, unknown> => ({
    isReady: true,
    isOpen: true,
    on: vi.fn(),
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    quit: vi.fn(async () => undefined),
    duplicate: vi.fn(() => createClient()),
    configGet: vi.fn(async () => ({ 'notify-keyspace-events': 'Exe' })),
    subscribe: vi.fn(async () => undefined),
    publish: vi.fn(async () => 1),
    get: vi.fn(
      async (_opts: unknown, key: string) => state.store.get(key) ?? null,
    ),
    set: vi.fn(async (key: string, value: string, opts?: { EX?: number }) => {
      state.store.set(key, value);
      if (opts?.EX === undefined) state.expiry.delete(key);
      else state.expiry.set(key, opts.EX);
      return 'OK';
    }),
    unlink: vi.fn(async (keys: string | string[]) => {
      const list = Array.isArray(keys) ? keys : [keys];
      list.forEach(deleteKey);
      return list.length;
    }),
    // Only compareAndUnlink is simulated. A test file that runs the SyncedMap
    // scripts must override eval.
    eval: vi.fn(
      async (
        script: string,
        options: { keys: string[]; arguments: string[] },
      ) => {
        if (!script.includes("redis.call('GET', KEYS[1]) == ARGV[1]")) {
          throw new Error('Unexpected eval script');
        }
        const [key] = options.keys;
        if (state.store.get(key) !== options.arguments[0]) return 0;
        deleteKey(key);
        return 1;
      },
    ),
    sendCommand: vi.fn(async (args: string[]) => {
      const [command] = args;
      if (command === 'HSCAN' || command === 'SCAN') return ['0', []];
      throw new Error(`Unexpected sendCommand: ${command}`);
    }),
    hSet: vi.fn(async () => 1),
    hDel: vi.fn(async () => 1),
    ...overrides(),
  });

  return {
    createClient,
    commandOptions: vi.fn((opts: unknown) => opts),
  };
}
