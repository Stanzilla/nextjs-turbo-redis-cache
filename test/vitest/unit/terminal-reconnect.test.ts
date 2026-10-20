import { describe, it, expect, vi, afterEach } from 'vitest';
import net from 'node:net';
import RedisStringsHandler from '../../../src/RedisStringsHandler';
import { getRedisCacheComponentsHandler } from '../../../src/CacheComponentsHandler';

// A user reconnectStrategy that gives up closes the node-redis client for
// good. The setup of the tag maps must then stop, and cache calls must miss.

async function closedPortUrl(): Promise<string> {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return `redis://127.0.0.1:${port}`;
}

const giveUp = {
  reconnectStrategy: () => new Error('the user strategy gave up'),
};

function retryLogCount(errorLog: ReturnType<typeof vi.spyOn>) {
  return errorLog.mock.calls.filter((call: unknown[]) =>
    / attempt \d+ failed, retrying in /.test(String(call[0])),
  ).length;
}

async function elapsedMs(operation: Promise<unknown>) {
  const started = performance.now();
  const value = await operation;
  return { value, elapsedMs: performance.now() - started };
}

describe('a user reconnectStrategy that gives up', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('RedisStringsHandler stops the setup retries and misses at once', async () => {
    const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    const handler = new RedisStringsHandler({
      redisUrl: await closedPortUrl(),
      keyPrefix: 'terminal-strings:',
      socketOptions: giveUp,
      getTimeoutMs: 5_000,
    });

    // The retry delays are 500 ms, then 1000 ms.
    await new Promise((r) => setTimeout(r, 1_700));
    expect(retryLogCount(errorLog)).toBeLessThanOrEqual(1);

    const { value, elapsedMs: ms } = await elapsedMs(
      handler.get('/page', { kind: 'APP_PAGE' } as never),
    );
    expect(value).toBeNull();
    expect(ms).toBeLessThan(1_000);
  });

  it('RedisCacheComponentsHandler stops the setup retries and misses at once', async () => {
    const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    const handler = getRedisCacheComponentsHandler({
      redisUrl: await closedPortUrl(),
      keyPrefix: 'terminal-components:',
      socketOptions: giveUp,
      getTimeoutMs: 5_000,
    });

    await new Promise((r) => setTimeout(r, 1_700));
    expect(retryLogCount(errorLog)).toBeLessThanOrEqual(1);

    const { value, elapsedMs: ms } = await elapsedMs(handler.get('entry', []));
    expect(value).toBeUndefined();
    expect(ms).toBeLessThan(1_000);
  });
});
