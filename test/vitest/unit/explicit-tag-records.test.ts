import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import RedisStringsHandler from '../../../src/RedisStringsHandler';
import { startPausableRedisServer } from './utils/pausableRedisServer';

const KEY_PREFIX = 'explicit:';
const setCtx = {
  isRoutePPREnabled: false,
  isFallback: false,
  cacheControl: { revalidate: 60 as const, expire: 600 },
};

const tick = (ms = 5) => new Promise((resolve) => setTimeout(resolve, ms));

// A write that started before revalidateTag() and reached Redis after its
// UNLINK. The sharedTagsMap index cannot find it any more.
async function writeRacesTheUnlink(
  store: Map<string, string>,
  key: string,
  revalidate: () => Promise<void>,
) {
  const raw = store.get(KEY_PREFIX + key)!;
  await tick();
  await revalidate();
  expect(store.has(KEY_PREFIX + key)).toBe(false);
  store.set(KEY_PREFIX + key, raw);
}

describe('RedisStringsHandler tag records for every tag and every kind', () => {
  let server: Awaited<ReturnType<typeof startPausableRedisServer>>;
  let handler: RedisStringsHandler;

  beforeAll(async () => {
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    server = await startPausableRedisServer();
    handler = new RedisStringsHandler({
      redisUrl: server.url,
      database: 0,
      keyPrefix: KEY_PREFIX,
      redisGetDeduplication: false,
    });
  });

  afterAll(async () => {
    await server.close();
    vi.unstubAllEnvs();
  });

  it('a FETCH entry with an explicit tag is a miss when its write raced revalidateTag()', async () => {
    await handler.set(
      'fetch',
      {
        kind: 'FETCH',
        data: { headers: {}, body: 'old', status: 200, url: 'https://x' },
        revalidate: 60,
      },
      { ...setCtx, tags: ['posts'] },
    );
    await writeRacesTheUnlink(server.store, 'fetch', () =>
      handler.revalidateTag('posts'),
    );

    expect(
      await handler.get('fetch', {
        kind: 'FETCH',
        revalidate: 60,
        fetchUrl: 'https://x',
        fetchIdx: 0,
        tags: ['posts'],
        softTags: [],
        isFallback: false,
      }),
    ).toBeNull();
  });

  it('a PAGES entry is a miss when its write raced revalidatePath()', async () => {
    await handler.set(
      '/blog',
      { kind: 'PAGES', html: '<p>old</p>', pageData: {}, headers: {} },
      setCtx,
    );
    await writeRacesTheUnlink(server.store, '/blog', () =>
      handler.revalidateTag('_N_T_/blog'),
    );

    expect(
      await handler.get('/blog', { kind: 'PAGES', isFallback: false }),
    ).toBeNull();
  });
});
