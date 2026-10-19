import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EventEmitter } from 'node:events';

const clients = vi.hoisted(() => [] as unknown[]);

vi.mock('redis', async () => {
  const { EventEmitter } = await import('node:events');
  return {
    createClient: vi.fn(() => {
      const client = Object.assign(new EventEmitter(), {
        isOpen: false,
        connect: vi.fn(async () => undefined),
      });
      clients.push(client);
      return client;
    }),
  };
});

type MockClient = EventEmitter & {
  isOpen: boolean;
  connect: ReturnType<typeof vi.fn>;
};

async function createClosedClient(
  socketOptions?: Record<string, unknown>,
): Promise<MockClient> {
  const { createCacheClient } = await import('../../../src/utils/cacheClient');
  const { ErrorBudget } = await import('../../../src/utils/errorBudget');
  createCacheClient({
    label: 'Redis client',
    redisUrl: 'redis://127.0.0.1:6399',
    database: 0,
    socketOptions,
    errorBudget: new ErrorBudget(0, () => undefined),
    deferConnection: true,
  });
  return clients.at(-1) as MockClient;
}

describe('createCacheClient reconnect after a closed connection', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    clients.length = 0;
  });

  it('reconnects with the default reconnect strategy', async () => {
    const client = await createClosedClient();

    client.emit('error', new Error('connection lost'));
    await vi.advanceTimersByTimeAsync(1000);

    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('does not reconnect when a user reconnectStrategy gave up', async () => {
    const client = await createClosedClient({ reconnectStrategy: () => false });

    client.emit('error', new Error('reconnect strategy gave up'));
    await vi.advanceTimersByTimeAsync(1000);

    expect(client.connect).not.toHaveBeenCalled();
  });
});
