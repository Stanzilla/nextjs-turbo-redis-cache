import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import RedisStringsHandler from '../../../src/RedisStringsHandler';

const hoisted = vi.hoisted(() => ({
  strings: new Map<string, string>(),
  hashes: new Map<string, Map<string, string>>(),
}));

function hash(name: string) {
  let fields = hoisted.hashes.get(name);
  if (!fields) hoisted.hashes.set(name, (fields = new Map()));
  return fields;
}

vi.mock('redis', () => ({
  createClient: () => ({
    isReady: true,
    on: vi.fn(),
    connect: vi.fn(async () => undefined),
    duplicate: vi.fn(() => ({
      connect: vi.fn(async () => undefined),
      subscribe: vi.fn(async () => undefined),
      on: vi.fn(),
      configGet: vi.fn(async () => ({ 'notify-keyspace-events': 'Exe' })),
    })),
    get: vi.fn(
      async (_opts: unknown, key: string) => hoisted.strings.get(key) ?? null,
    ),
    set: vi.fn(async (key: string, value: string) => {
      hoisted.strings.set(key, value);
      return 'OK';
    }),
    unlink: vi.fn(async (keys: string[]) => {
      for (const key of keys) hoisted.strings.delete(key);
      return keys.length;
    }),
    sendCommand: vi.fn(async ([command, name]: string[]) => {
      if (command !== 'HSCAN') throw new Error(`Unexpected ${command}`);
      return ['0', [...hash(name)].flat()];
    }),
    eval: vi.fn(async () => []),
    hSet: vi.fn(async (name: string, field: string, value: string) => {
      hash(name).set(field, value);
      return 1;
    }),
    hDel: vi.fn(async (name: string, fields: string | string[]) => {
      for (const field of [fields].flat()) hash(name).delete(field);
      return 1;
    }),
    publish: vi.fn(async () => 1),
  }),
  commandOptions: vi.fn((opts) => opts),
}));

const PREFIX = 'tags:';
const TAGS_HASH = PREFIX + '__sharedTags__';

const fetchData = (body: string) => ({
  kind: 'FETCH' as const,
  data: { headers: {}, body, status: 200, url: 'https://example.com' },
  revalidate: 60,
});

function createHandler(options: { tagIndex?: boolean } = {}) {
  return new RedisStringsHandler({
    ...options,
    redisUrl: 'redis://localhost:6379',
    keyPrefix: PREFIX,
    database: 0,
    getTimeoutMs: 100,
    redisGetDeduplication: false,
    inMemoryCachingTime: 0,
  });
}

describe('RedisStringsHandler tag membership writes', () => {
  beforeEach(() => {
    hoisted.strings.clear();
    hoisted.hashes.clear();
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('writes the membership again when the tags did not change', async () => {
    const handler = createHandler();
    await handler.set('k', fetchData('v1'), { tags: ['posts'] } as never);
    // The orphan cleanup of another instance removed the field in Redis,
    // and its delete message did not arrive yet.
    hash(TAGS_HASH).delete('k');

    await handler.set('k', fetchData('v2'), { tags: ['posts'] } as never);

    expect(JSON.parse(hash(TAGS_HASH).get('k')!)).toEqual(['posts']);
  });

  it('removes the old membership when an untagged entry replaces a tagged one', async () => {
    const handler = createHandler();
    await handler.set('k', fetchData('v1'), { tags: ['posts'] } as never);
    await handler.set('k', fetchData('v2'), { tags: [] } as never);

    expect(hash(TAGS_HASH).has('k')).toBe(false);
    await handler.revalidateTag('posts');
    expect(hoisted.strings.has(PREFIX + 'k')).toBe(true);
  });

  it.each([false, true])(
    'revalidateTag deletes only the entries whose current tags match (tagIndex: %s)',
    async (tagIndex) => {
      const handler = createHandler({ tagIndex });
      await handler.set('a', fetchData('a'), { tags: ['posts'] } as never);
      await handler.set('b', fetchData('b1'), {
        tags: ['posts', 'users'],
      } as never);
      await handler.set('b', fetchData('b2'), { tags: ['users'] } as never);
      await handler.set('c', fetchData('c1'), { tags: ['posts'] } as never);
      await handler.set('c', fetchData('c2'), { tags: [] } as never);

      await handler.revalidateTag('posts');

      expect(hoisted.strings.has(PREFIX + 'a')).toBe(false);
      expect(hoisted.strings.has(PREFIX + 'b')).toBe(true);
      expect(hoisted.strings.has(PREFIX + 'c')).toBe(true);
    },
  );

  it('does not change the tags of the caller context', async () => {
    const handler = createHandler();
    const ctx = { tags: ['posts'] };
    await handler.set(
      'page',
      {
        kind: 'APP_PAGE',
        html: '',
        rscData: undefined,
        headers: { 'x-next-cache-tags': '_N_T_/page' },
        postponed: undefined,
        status: 200,
        segmentData: undefined,
      } as never,
      ctx as never,
    );

    expect(ctx.tags).toEqual(['posts']);
    expect(JSON.parse(hash(TAGS_HASH).get('page')!)).toEqual([
      'posts',
      '_N_T_/page',
    ]);
  });
});
