import { describe, it, expect, vi, afterEach } from 'vitest';

import { parseEnvInt } from '../../../src/utils/env';

const hoisted = vi.hoisted(() => ({ store: new Map<string, string>() }));

vi.mock('redis', () => {
  const createClient = () => ({
    isReady: true,
    isOpen: true,
    on: vi.fn(),
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    quit: vi.fn(async () => undefined),
    duplicate: vi.fn(() => createClient()),
    configGet: vi.fn(async () => ({ 'notify-keyspace-events': 'Exe' })),
    subscribe: vi.fn(async () => undefined),
    publish: vi.fn(async () => 1),
    get: vi.fn(
      async (_opts: unknown, key: string) => hoisted.store.get(key) ?? null,
    ),
    set: vi.fn(async (key: string, value: string) => {
      hoisted.store.set(key, value);
      return 'OK';
    }),
    sendCommand: vi.fn(async () => ['0', []]),
    hSet: vi.fn(async () => 1),
    hDel: vi.fn(async () => 1),
  });
  return { createClient, commandOptions: vi.fn((opts) => opts) };
});

describe('parseEnvInt', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it.each([
    ['not set', undefined, 7],
    ['empty', '', 7],
    ['not a number', 'abc', 7],
    ['below the minimum', '-5', 7],
    ['a valid number', '250', 250],
    ['zero', '0', 0],
  ])(
    'returns the expected value when the variable is %s',
    (_, raw, expected) => {
      vi.stubEnv('TEST_INT', raw);
      expect(parseEnvInt('TEST_INT', 7)).toBe(expected);
    },
  );

  it('applies the minimum', () => {
    vi.stubEnv('TEST_INT', '0');
    expect(parseEnvInt('TEST_INT', 500, { min: 1 })).toBe(500);
  });
});

describe('invalid REDIS_COMMAND_TIMEOUT_MS', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it('uses the default timeout, so reads from Redis still work', async () => {
    vi.stubEnv('REDIS_COMMAND_TIMEOUT_MS', 'abc');
    vi.resetModules();
    const { default: RedisStringsHandler } = await import(
      '../../../src/RedisStringsHandler'
    );
    const options = {
      redisUrl: 'redis://localhost:6379',
      keyPrefix: 'env:',
      database: 0,
      redisGetDeduplication: false,
    };
    const writer = new RedisStringsHandler(options);
    const reader = new RedisStringsHandler(options);
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await writer.set(
      '/page',
      { kind: 'PAGES', html: 'v1', pageData: {}, headers: {}, status: 200 },
      {
        isRoutePPREnabled: false,
        isFallback: false,
        cacheControl: { revalidate: 60, expire: 600 },
      },
    );
    const entry = await reader.get('/page', {
      kind: 'PAGES',
      isRoutePPREnabled: false,
      isFallback: false,
    });

    expect((entry?.value as { html?: string } | undefined)?.html).toBe('v1');
  });
});
