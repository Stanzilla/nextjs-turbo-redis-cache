import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import RedisStringsHandler from '../../../src/RedisStringsHandler';
import { getRedisCacheComponentsHandler } from '../../../src/CacheComponentsHandler';
import { startPausableRedisServer } from './utils/pausableRedisServer';

// The tag maps load with HSCAN. A paused HSCAN keeps the initial sync
// pending, so the handlers stay not ready for the whole test.

const getTimeoutMs = 50;

async function elapsedMs(operation: Promise<unknown>) {
  const started = performance.now();
  const value = await operation;
  return { value, elapsedMs: performance.now() - started };
}

describe('reads while the tag maps are still loading', () => {
  let server: Awaited<ReturnType<typeof startPausableRedisServer>>;
  let strings: RedisStringsHandler;
  let components: ReturnType<typeof getRedisCacheComponentsHandler>;

  beforeAll(async () => {
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    server = await startPausableRedisServer();
    server.pause(['HSCAN']);
    const options = {
      redisUrl: server.url,
      database: 0,
      getTimeoutMs,
      redisGetDeduplication: false,
    };
    strings = new RedisStringsHandler({ ...options, keyPrefix: 'strings:' });
    components = getRedisCacheComponentsHandler({
      ...options,
      keyPrefix: 'components:',
    });
  });

  afterAll(async () => {
    await server.close();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('RedisStringsHandler.get() misses within the read timeout', async () => {
    const { value, elapsedMs: ms } = await elapsedMs(
      strings.get('/page', { kind: 'APP_PAGE' } as never),
    );
    expect(value).toBeNull();
    expect(ms).toBeLessThan(1_000);
  });

  it('RedisCacheComponentsHandler.get() misses within the read timeout', async () => {
    const { value, elapsedMs: ms } = await elapsedMs(
      components.get('entry', []),
    );
    expect(value).toBeUndefined();
    expect(ms).toBeLessThan(1_000);
  });

  it('RedisCacheComponentsHandler.getExpiration() returns within the read timeout', async () => {
    const { elapsedMs: ms } = await elapsedMs(
      components.getExpiration(['tag']),
    );
    expect(ms).toBeLessThan(1_000);
  });
});
