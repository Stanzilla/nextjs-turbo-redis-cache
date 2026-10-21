import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import RedisStringsHandler from '../../../src/RedisStringsHandler';
import { getRedisCacheComponentsHandler } from '../../../src/CacheComponentsHandler';
import { startPausableRedisServer } from './utils/pausableRedisServer';

// A paused SUBSCRIBE keeps the tag maps not ready for the whole test, as on a
// pod that starts or whose subscriber reconnects. The command connection still
// answers. Next.js does not retry a failed invalidation, so the tag record
// must reach Redis anyway.

describe('invalidations while the tag maps are still loading', () => {
  let server: Awaited<ReturnType<typeof startPausableRedisServer>>;
  let strings: RedisStringsHandler;
  let components: ReturnType<typeof getRedisCacheComponentsHandler>;

  const tagRecordWrites = (hash: string, tag: string) =>
    server.received.filter(
      ([command, key, field]) =>
        command.toUpperCase() === 'HSET' && key === hash && field === tag,
    );

  beforeAll(async () => {
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    server = await startPausableRedisServer();
    server.pause(['SUBSCRIBE']);
    const options = {
      redisUrl: server.url,
      database: 0,
      writeTimeoutMs: 50,
      redisGetDeduplication: false,
    };
    strings = new RedisStringsHandler({ ...options, keyPrefix: 'strings:' });
    components = getRedisCacheComponentsHandler({
      ...options,
      keyPrefix: 'components:',
    });
    await vi.waitFor(() =>
      expect(
        server.received.filter(
          ([command]) => command.toUpperCase() === 'SUBSCRIBE',
        ),
      ).not.toHaveLength(0),
    );
  });

  afterAll(async () => {
    await Promise.all([strings.close(), components.close()]);
    await server.close();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('RedisStringsHandler.revalidateTag() writes the tag record', async () => {
    await expect(strings.revalidateTag('posts')).resolves.toBeUndefined();

    expect(
      tagRecordWrites('strings:__revalidated_tags__', 'posts'),
    ).toHaveLength(1);
  });

  it('RedisCacheComponentsHandler.updateTags() writes the tag manifest', async () => {
    await expect(components.updateTags(['posts'])).resolves.toBeUndefined();

    expect(
      tagRecordWrites(
        'components:__cacheComponents_revalidated_tags__',
        'posts',
      ),
    ).toHaveLength(1);
  });
});
