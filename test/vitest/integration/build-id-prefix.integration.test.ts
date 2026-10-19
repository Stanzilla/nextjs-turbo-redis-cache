import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createClient, RedisClientType } from 'redis';
import path from 'path';
import fs from 'fs';
import fetch from 'node-fetch';
import { NextFixture, startNextFixture } from './next-fixture';

const NEXT_APP = 'next-app-15-4-11';
const PORT = 3075;
const BASE_URL = `http://localhost:${PORT}`;

describe('BUILD_ID-based key prefix (Next handlers)', () => {
  let nextFixture: NextFixture | undefined;
  let redis: RedisClientType;
  let appDir: string;
  let buildId: string;

  beforeAll(async () => {
    appDir = path.join(__dirname, '..', '..', 'nextjs-test-projects', NEXT_APP);

    // Ensure clean env so BUILD_ID precedence kicks in
    delete (process.env as Record<string, string | undefined>).KEY_PREFIX;
    delete (process.env as Record<string, string | undefined>).VERCEL_URL;
    process.env.VERCEL_ENV = 'production';
    process.env.REDISHOST = process.env.REDISHOST || 'localhost';
    process.env.REDISPORT = process.env.REDISPORT || '6379';

    // Start server without KEY_PREFIX/VERCEL_URL
    nextFixture = await startNextFixture({
      appDir,
      ports: [PORT],
      env: { SKIP_KEYSPACE_CONFIG_CHECK: 'true' },
      readyPath: '/api/cached-static-fetch',
    });

    // Read BUILD_ID from the built app
    buildId = fs
      .readFileSync(path.join(appDir, '.next', 'BUILD_ID'), 'utf8')
      .trim();

    // Connect Redis
    redis = createClient({
      url: `redis://${process.env.REDISHOST}:${process.env.REDISPORT}`,
    });
    await redis.connect();
  }, 180000);

  afterAll(async () => {
    try {
      if (redis) await redis.quit();
    } finally {
      await nextFixture?.stop();
    }
  });

  it('uses BUILD_ID as the Redis key prefix when no KEY_PREFIX/VERCEL_URL set', async () => {
    // Warm cache twice to ensure a set occurs even if first was during startup
    const res1 = await fetch(BASE_URL + '/api/cached-static-fetch');
    expect(res1.ok).toBe(true);
    await new Promise((r) => setTimeout(r, 500));
    const res2 = await fetch(BASE_URL + '/api/cached-static-fetch');
    expect(res2.ok).toBe(true);

    // Allow background syncs
    await new Promise((r) => setTimeout(r, 1500));

    const keys = await redis.keys(`${buildId}*`);
    expect(keys.length).toBeGreaterThan(0);
  });
});
