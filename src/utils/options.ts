import type { CreateRedisStringsHandlerOptions } from '../RedisStringsHandler';
import { parseEnvInt } from './env';

type SharedOptionNames =
  | 'redisUrl'
  | 'database'
  | 'getTimeoutMs'
  | 'writeTimeoutMs'
  | 'revalidateTagQuerySize'
  | 'avgResyncIntervalMs'
  | 'redisGetDeduplication'
  | 'inMemoryCachingTime'
  | 'killContainerOnErrorThreshold'
  | 'socketOptions'
  | 'clientOptions';

type SharedOptions = Pick<CreateRedisStringsHandlerOptions, SharedOptionNames>;

export type ResolvedSharedOptions = Required<
  Omit<SharedOptions, 'socketOptions' | 'clientOptions'>
> &
  Pick<SharedOptions, 'socketOptions' | 'clientOptions'>;

/**
 * The defaults of the options that both handlers have. The environment is
 * read at each call, so each handler gets the values at its construction.
 */
export function resolveSharedOptions({
  redisUrl = process.env.REDIS_URL
    ? process.env.REDIS_URL
    : process.env.REDISHOST
      ? `redis://${process.env.REDISHOST}:${process.env.REDISPORT}`
      : 'redis://localhost:6379',
  database = process.env.VERCEL_ENV === 'production' ? 0 : 1,
  getTimeoutMs = parseEnvInt('REDIS_COMMAND_TIMEOUT_MS', 500, { min: 1 }),
  writeTimeoutMs = parseEnvInt('REDIS_WRITE_TIMEOUT_MS', getTimeoutMs, {
    min: 1,
  }),
  revalidateTagQuerySize = 250,
  avgResyncIntervalMs = 60 * 60 * 1_000,
  redisGetDeduplication = true,
  inMemoryCachingTime = 10_000,
  killContainerOnErrorThreshold = parseEnvInt(
    'KILL_CONTAINER_ON_ERROR_THRESHOLD',
    0,
  ),
  socketOptions,
  clientOptions,
}: SharedOptions): ResolvedSharedOptions {
  return {
    redisUrl,
    database,
    getTimeoutMs,
    writeTimeoutMs,
    revalidateTagQuerySize,
    avgResyncIntervalMs,
    redisGetDeduplication,
    inMemoryCachingTime,
    killContainerOnErrorThreshold,
    socketOptions,
    clientOptions,
  };
}

/**
 * A random resync interval near `avgMs`. The cache-key map uses 'before'
 * (0.9 to 1.0 of avgMs) and the tag manifest uses 'after' (1.1 to 1.2 of
 * avgMs), so that the two maps of a handler do not reload at the same time,
 * and the instances of a deployment spread their reloads.
 */
export function resyncDelay(avgMs: number, slot: 'before' | 'after'): number {
  const tenth = avgMs / 10;
  const base = slot === 'before' ? avgMs - tenth : avgMs + tenth;
  return base + Math.random() * tenth;
}
