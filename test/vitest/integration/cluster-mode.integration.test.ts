import { randomUUID } from 'node:crypto';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createClient } from 'redis';
import RedisStringsHandler from '../../../src/RedisStringsHandler';

// Runs on a Redis or Valkey with cluster-enabled yes. A cluster rejects a
// command whose keys are in different hash slots with CROSSSLOT, also when
// one node owns all slots.
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

async function isClusterMode(): Promise<boolean> {
  const client = createClient({ url: redisUrl });
  await client.connect();
  try {
    return (await client.info('cluster')).includes('cluster_enabled:1');
  } finally {
    await client.disconnect();
  }
}

const clusterMode = await isClusterMode();
if (!clusterMode && process.env.REQUIRE_CLUSTER) {
  throw new Error(`REQUIRE_CLUSTER is set, but ${redisUrl} is not a cluster`);
}

type Client = ReturnType<typeof createClient>;

const fetchData = (body: string) => ({
  kind: 'FETCH' as const,
  data: { headers: {}, body, status: 200, url: 'https://example.com' },
  revalidate: 60,
});

describe.skipIf(!clusterMode)('RedisStringsHandler on a cluster', () => {
  let client: Client;
  let handler: RedisStringsHandler;
  let prefix: string;

  beforeEach(async () => {
    prefix = `cluster-test:${randomUUID()}:`;
    client = createClient({ url: redisUrl });
    await client.connect();
    handler = new RedisStringsHandler({
      redisUrl,
      keyPrefix: prefix,
      database: 0,
      redisGetDeduplication: false,
      inMemoryCachingTime: 0,
    });
  });

  afterEach(async () => {
    await handler.close();
    for await (const keys of client.scanIterator({ MATCH: `${prefix}*` })) {
      await Promise.all(
        ([] as string[]).concat(keys).map((key) => client.unlink(key)),
      );
    }
    await client.disconnect();
  });

  /** Two cache keys whose Redis keys the server puts in different slots. */
  async function keysInTwoSlots(): Promise<[string, string]> {
    const slot = (key: string) =>
      client.sendCommand(['CLUSTER', 'KEYSLOT', prefix + key]);
    const first = 'page-0';
    for (let i = 1; i < 100; i++) {
      const second = `page-${i}`;
      if ((await slot(first)) !== (await slot(second))) return [first, second];
    }
    throw new Error('no two keys in different slots');
  }

  it('revalidateTag unlinks tagged entries that are in different slots', async () => {
    const [first, second] = await keysInTwoSlots();
    const ctx = { isRoutePPREnabled: false, isFallback: false, tags: ['t'] };
    await handler.set(first, fetchData('a'), ctx);
    await handler.set(second, fetchData('b'), ctx);
    expect(await client.exists(prefix + first)).toBe(1);
    expect(await client.exists(prefix + second)).toBe(1);

    await expect(handler.revalidateTag('t')).resolves.toBeUndefined();

    expect(await client.exists(prefix + first)).toBe(0);
    expect(await client.exists(prefix + second)).toBe(0);
  });
});
