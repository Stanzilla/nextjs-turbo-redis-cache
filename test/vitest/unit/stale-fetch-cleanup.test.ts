import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import RedisStringsHandler from '../../../src/RedisStringsHandler';
import { startPausableRedisServer } from './utils/pausableRedisServer';

const PAGE_TAG = '_N_T_/page';

function fetchValue(body: string) {
  return {
    kind: 'FETCH' as const,
    data: { headers: {}, body, status: 200, url: 'https://example.com' },
    revalidate: 60,
  };
}

function fetchCtx(key: string) {
  return {
    kind: 'FETCH' as const,
    revalidate: 60,
    fetchUrl: 'https://example.com/' + key,
    fetchIdx: 0,
    tags: [],
    softTags: [PAGE_TAG],
    isFallback: false,
  };
}

const setCtx = {
  isRoutePPREnabled: false,
  isFallback: false,
  cacheControl: { revalidate: 60 as const, expire: 600 },
};

const tick = (ms = 5) => new Promise((resolve) => setTimeout(resolve, ms));

describe('RedisStringsHandler stale FETCH reads', () => {
  let server: Awaited<ReturnType<typeof startPausableRedisServer>>;
  let handler: RedisStringsHandler;
  // A second instance has its own connection, so its writes are not held
  // behind a paused command of the first one.
  let otherInstance: RedisStringsHandler;

  beforeAll(async () => {
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    server = await startPausableRedisServer();
    const options = {
      redisUrl: server.url,
      database: 0,
      keyPrefix: 'stale:',
      redisGetDeduplication: false,
    };
    handler = new RedisStringsHandler(options);
    otherInstance = new RedisStringsHandler(options);
  });

  afterAll(async () => {
    await server.close();
    vi.unstubAllEnvs();
  });

  it('a stale read of one entry does not make the other entries under the same page tag fresh', async () => {
    await handler.set('fetch-a', fetchValue('a'), setCtx);
    await handler.set('fetch-b', fetchValue('b'), setCtx);
    await tick();
    await handler.revalidateTag(PAGE_TAG);

    expect(await handler.get('fetch-a', fetchCtx('fetch-a'))).toBeNull();
    await tick(20);
    expect(await handler.get('fetch-b', fetchCtx('fetch-b'))).toBeNull();
  });

  it('the cleanup of a stale entry does not delete a replacement written before the cleanup runs', async () => {
    await handler.set('fetch-c', fetchValue('old'), setCtx);
    await tick();
    await handler.revalidateTag(PAGE_TAG);
    server.pause(['EVAL', 'UNLINK']);

    expect(await handler.get('fetch-c', fetchCtx('fetch-c'))).toBeNull();
    await tick();
    await otherInstance.set('fetch-c', fetchValue('new'), setCtx);
    server.resume();
    await tick(20);

    const entry = await handler.get('fetch-c', fetchCtx('fetch-c'));
    expect(entry?.value).toMatchObject({ data: { body: 'new' } });
  });

  it('a replacement written during the stale cleanup keeps its tag membership', async () => {
    const taggedCtx = { ...setCtx, tags: ['posts'] };
    await handler.set('fetch-e', fetchValue('old'), taggedCtx);
    await tick();
    await handler.revalidateTag(PAGE_TAG);
    server.pause(['EVAL', 'UNLINK']);

    expect(await handler.get('fetch-e', fetchCtx('fetch-e'))).toBeNull();
    await tick();
    await otherInstance.set('fetch-e', fetchValue('new'), taggedCtx);
    server.resume();
    await tick(20);
    const sharedTags = (
      handler as unknown as { sharedTagsMap: Map<string, string[]> }
    ).sharedTagsMap;
    expect(sharedTags.get('fetch-e')).toEqual(['posts']);

    await handler.revalidateTag('posts');
    await tick(20);
    expect(server.store.has('stale:fetch-e')).toBe(false);
  });

  it('removes a stale entry that nobody replaced', async () => {
    await handler.set('fetch-d', fetchValue('d'), setCtx);
    await tick();
    await handler.revalidateTag(PAGE_TAG);

    expect(await handler.get('fetch-d', fetchCtx('fetch-d'))).toBeNull();
    await vi.waitFor(() =>
      expect(server.store.has('stale:fetch-d')).toBe(false),
    );
  });
});
