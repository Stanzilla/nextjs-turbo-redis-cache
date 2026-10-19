import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('redis', () => ({
  createClient: vi.fn(() => ({
    isOpen: false,
    isReady: false,
    on: vi.fn(),
    connect: vi.fn(async () => {
      throw new Error('ECONNREFUSED');
    }),
    disconnect: vi.fn(async () => {
      throw new Error('The client is closed');
    }),
  })),
  commandOptions: vi.fn((opts) => opts),
}));

vi.mock('../../../src/SyncedMap', () => {
  class SyncedMap {
    static waitUntilAllReady = vi.fn(async () => undefined);
  }
  return { SyncedMap };
});

type MockClient = { connect: ReturnType<typeof vi.fn> };

async function createStringsHandler() {
  const { default: RedisStringsHandler } = await import(
    '../../../src/RedisStringsHandler'
  );
  return new RedisStringsHandler({ redisUrl: 'redis://127.0.0.1:6399' });
}

async function createComponentsHandler() {
  const { getRedisCacheComponentsHandler } = await import('../../../src');
  return getRedisCacheComponentsHandler({ redisUrl: 'redis://127.0.0.1:6399' });
}

describe.each([
  ['RedisStringsHandler', createStringsHandler],
  ['RedisCacheComponentsHandler', createComponentsHandler],
])('%s initial connect failure', (_name, create) => {
  const unhandled = vi.fn();

  beforeEach(() => {
    vi.resetModules();
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    unhandled.mockClear();
    process.on('unhandledRejection', unhandled);
  });

  afterEach(() => {
    process.off('unhandledRejection', unhandled);
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('logs the failure without an unhandled rejection or a second connect', async () => {
    const handler = await create();
    const client = (handler as unknown as { client: MockClient }).client;

    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(unhandled).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });
});
