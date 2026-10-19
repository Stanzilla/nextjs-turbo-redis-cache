import {
  detectContainerRuntime,
  getFreePort,
  sh,
  sleep,
  startRedis,
  waitUntil,
} from './redis-test-helpers';

async function waitOrThrow(
  what: string,
  fn: () => Promise<boolean>,
  timeoutMs: number,
) {
  if (!(await waitUntil(fn, timeoutMs))) {
    throw new Error(`timed out waiting for ${what}`);
  }
}

async function main() {
  const runtime = detectContainerRuntime();
  const name = `redis-e2e-${Math.random().toString(36).slice(2, 8)}`;

  const port = await getFreePort();

  // cleanup
  sh(runtime, ['rm', '-f', name]);

  // Start redis with keyspace notifications enabled (required by SyncedMap)
  startRedis(runtime, name, port);

  // Set env BEFORE importing in case any module-level code reads REDIS_URL.
  // (With lazy init the singleton is no longer created at import time, but
  //  setting env first is still good practice.)
  process.env.REDIS_URL = `redis://127.0.0.1:${port}`;
  process.env.VERCEL_URL = `e2e-${name}-`;

  const { getRedisCacheComponentsHandler } = await import(
    '../../../../../src/CacheComponentsHandler'
  );
  const { default: RedisStringsHandler } = await import(
    '../../../../../src/RedisStringsHandler'
  );

  // Cache Components handler
  const cacheComponentsHandler = getRedisCacheComponentsHandler({
    redisUrl: `redis://127.0.0.1:${port}`,
    socketOptions: {
      connectTimeout: 2_000,
      // allow redis client to reconnect; this scenario is about stability under restart
      reconnectStrategy: (retries) => Math.min(50 + retries * 50, 500),
    },
  });

  const cacheComponentsClient = (cacheComponentsHandler as any).client as {
    ping: () => Promise<string>;
    isReady: boolean;
  };

  await waitOrThrow(
    'cache components client ready',
    async () => cacheComponentsClient.isReady,
    20_000,
  );
  if ((await cacheComponentsClient.ping()) !== 'PONG') {
    throw new Error('expected PONG (cache components)');
  }

  // Regular handler
  const regularHandler = new RedisStringsHandler({
    redisUrl: `redis://127.0.0.1:${port}`,
    socketOptions: {
      connectTimeout: 2_000,
      reconnectStrategy: (retries: number) => Math.min(50 + retries * 50, 500),
    },
    // Keep default offline queue behavior here; SyncedMap does initial sync during construction.
    keyPrefix: `e2e-${name}-regular-`,
  } as any);

  const regularClient = (regularHandler as any).client as {
    ping: () => Promise<string>;
    isReady: boolean;
  };

  await waitOrThrow(
    'regular client ready',
    async () => regularClient.isReady,
    20_000,
  );
  if ((await regularClient.ping()) !== 'PONG') {
    throw new Error('expected PONG (regular handler)');
  }

  // Seed a small entry so we know regular handler does real ops
  await regularHandler.set(
    'e2e-key',
    {
      kind: 'FETCH',
      data: {
        headers: {},
        body: Buffer.from('hello').toString('base64'),
        status: 200,
        url: 'https://example.com/e2e',
      },
      revalidate: 10,
    },
    { isRoutePPREnabled: false, isFallback: false, tags: ['e2e'] },
  );

  const got = await regularHandler.get('e2e-key', {
    kind: 'FETCH',
    revalidate: 10,
    fetchUrl: 'https://example.com/e2e',
    fetchIdx: 0,
    tags: ['e2e'],
    softTags: [],
    isFallback: false,
  });
  if (!got) throw new Error('expected cache hit (regular handler)');

  // Kill redis
  sh(runtime, ['stop', '-t', '0', name], { timeoutMs: 30_000 });

  // Wait a bit, then start again
  await sleep(500);
  startRedis(runtime, name, port);

  // Should recover (both)
  await waitOrThrow(
    'cache components client recovery',
    async () => {
      try {
        return (await cacheComponentsClient.ping()) === 'PONG';
      } catch {
        return false;
      }
    },
    30_000,
  );

  await waitOrThrow(
    'regular client recovery',
    async () => {
      try {
        return (await regularClient.ping()) === 'PONG';
      } catch {
        return false;
      }
    },
    30_000,
  );

  // Shut down clients before cleaning up Redis, otherwise reconnect loops keep the process alive.
  try {
    await (cacheComponentsHandler as any).client?.quit?.();
  } catch {}
  try {
    await (regularHandler as any).client?.quit?.();
  } catch {}

  // cleanup best effort
  sh(runtime, ['rm', '-f', name]);

  // If we reach here without crashing due to Socket already opened, we're good.
  // (Vitest will check stdout for this line.)
  process.stdout.write('OK\n');

  // Ensure we exit even if background reconnect timers exist.
  process.exit(0);
}

main().catch((err) => {
  // Ensure we don't hide the key error string
  console.error(err);
  process.exit(1);
});
