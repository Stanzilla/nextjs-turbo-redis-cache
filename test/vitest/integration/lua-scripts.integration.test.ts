import { randomUUID } from 'node:crypto';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createClient } from 'redis';
import { SyncedMap } from '../../../src/SyncedMap';
import { compareAndUnlink } from '../../../src/utils/compareAndUnlink';
import { syncChannelName } from '../../../src/utils/redisSubscriptions';

// Runs the Lua scripts on a real Redis. The unit tests use mocks that only
// imitate the scripts. The suite also runs on a cluster-mode server, which
// rejects a command whose keys are in different hash slots.
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const HASH = '__tags__';

type Client = ReturnType<typeof createClient>;

describe('Lua scripts on a real Redis', () => {
  let client: Client;
  let listener: Client;
  let prefix: string;
  let maps: SyncedMap<unknown>[];

  beforeEach(async () => {
    vi.stubEnv('SKIP_KEYSPACE_CONFIG_CHECK', 'TRUE');
    prefix = `lua-test:${randomUUID()}:`;
    maps = [];
    client = createClient({ url: redisUrl });
    listener = client.duplicate();
    await Promise.all([client.connect(), listener.connect()]);
  });

  afterEach(async () => {
    await Promise.all(maps.map((map) => map.close()));
    // One key per UNLINK: a cluster rejects keys in different slots.
    for await (const keys of client.scanIterator({ MATCH: `${prefix}*` })) {
      await Promise.all(
        ([] as string[]).concat(keys).map((key) => client.unlink(key)),
      );
    }
    await Promise.all([client.disconnect(), listener.disconnect()]);
    vi.unstubAllEnvs();
  });

  /** Collects the field names of each delete message on the sync channel. */
  async function recordDeletes(): Promise<string[][]> {
    const deletes: string[][] = [];
    await listener.subscribe(syncChannelName(prefix, HASH), (message) => {
      const parsed = JSON.parse(message);
      if (parsed.type === 'delete') deletes.push(parsed.keys);
    });
    return deletes;
  }

  /** A client whose eval() first runs `before`. */
  function withBeforeEval(before: () => Promise<void>): Client {
    return new Proxy(client, {
      get(target, prop) {
        if (prop === 'eval') {
          return async (...args: Parameters<Client['eval']>) => {
            await before();
            return target.eval(...args);
          };
        }
        const value = Reflect.get(target, prop);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
  }

  function startMap(
    options: Partial<ConstructorParameters<typeof SyncedMap>[0]> = {},
    mapClient: Client = client,
  ) {
    const map = new SyncedMap<unknown>({
      client: mapClient,
      keyPrefix: prefix,
      redisKey: HASH,
      database: 0,
      querySize: 250,
      filterKeys: () => true,
      ...options,
    });
    maps.push(map);
    return map;
  }

  it('removes only the fields whose cache key is gone, and each peer sees the delete once', async () => {
    await client.set(`${prefix}kept`, 'value');
    await client.hSet(`${prefix}${HASH}`, {
      kept: '["t"]',
      gone1: '["t"]',
      gone2: '["t"]',
    });
    const deletes = await recordDeletes();

    // Both instances read the hash before either one runs the script.
    let waiting = 0;
    let release!: () => void;
    const bothScanned = new Promise<void>((resolve) => (release = resolve));
    const barrier = async () => {
      if (++waiting === 2) release();
      await bothScanned;
    };
    const first = startMap({}, withBeforeEval(barrier));
    const second = startMap({}, withBeforeEval(barrier));
    await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);

    expect(await client.hKeys(`${prefix}${HASH}`)).toEqual(['kept']);
    await vi.waitFor(() =>
      expect(deletes.flat().sort()).toEqual(['gone1', 'gone2']),
    );
    await vi.waitFor(() => {
      for (const map of [first, second]) {
        expect([...map.entries()].map(([key]) => key)).toEqual(['kept']);
      }
    });
  });

  it('keeps the field of a key that a writer stores again with the same tags during the cleanup', async () => {
    await client.hSet(`${prefix}${HASH}`, { page: '["t"]', gone: '["t"]' });
    const deletes = await recordDeletes();

    // The writer runs once, after the reload read the hash and before the
    // cleanup changes it. It writes the value that the hash already holds.
    let written = false;
    const writer = withBeforeEval(async () => {
      if (written) return;
      written = true;
      await client.set(`${prefix}page`, 'value');
      await client.hSet(`${prefix}${HASH}`, 'page', '["t"]');
    });
    const map = startMap({}, writer);
    await map.waitUntilReady();

    expect(await client.hGetAll(`${prefix}${HASH}`)).toEqual({
      page: '["t"]',
    });
    await vi.waitFor(() => expect(deletes.flat()).toContain('gone'));
    await vi.waitFor(() => {
      expect(map.get('page')).toEqual(['t']);
      expect(map.has('gone')).toBe(false);
    });
  });

  it('removes the field of a cache key that expires', async () => {
    const deletes = await recordDeletes();
    const map = startMap();
    await map.waitUntilReady();

    await client.set(`${prefix}page`, 'value', { PX: 100 });
    await map.set('page', ['t']);

    await vi.waitFor(
      async () => expect(await client.hKeys(`${prefix}${HASH}`)).toEqual([]),
      { timeout: 2_000 },
    );
    await vi.waitFor(() => expect(deletes).toEqual([['page']]));
    await vi.waitFor(() => expect(map.has('page')).toBe(false));
  });

  it('keeps an expired field that a writer changed after the reload read it', async () => {
    await client.hSet(`${prefix}${HASH}`, {
      old: '{"expired":true}',
      rewritten: '{"expired":true}',
    });
    const deletes = await recordDeletes();

    // The writer runs between the scan and the script.
    const writer = withBeforeEval(async () => {
      await client.hSet(`${prefix}${HASH}`, 'rewritten', '{"expired":false}');
    });
    const map = startMap(
      {
        customizedSync: { fieldsAreNotRedisKeys: true },
        isExpiredValue: (value) => (value as { expired: boolean }).expired,
      },
      writer,
    );
    await map.waitUntilReady();

    expect(await client.hGetAll(`${prefix}${HASH}`)).toEqual({
      rewritten: '{"expired":false}',
    });
    await vi.waitFor(() => expect(deletes).toEqual([['old']]));
    await vi.waitFor(() => expect(map.has('old')).toBe(false));
  });

  it('compareAndUnlink deletes the key only while it holds the expected value', async () => {
    const key = `${prefix}entry`;
    await client.set(key, 'v1');

    expect(await compareAndUnlink(client, key, 'v2')).toBe(false);
    expect(await client.get(key)).toBe('v1');

    expect(await compareAndUnlink(client, key, 'v1')).toBe(true);
    expect(await client.exists(key)).toBe(0);

    expect(await compareAndUnlink(client, key, 'v1')).toBe(false);
  });
});
