import type { CreateRedisStringsHandlerOptions } from './RedisStringsHandler';
import { SyncedMap } from './SyncedMap';
import {
  createRedisSubscriptions,
  syncChannelName,
} from './utils/redisSubscriptions';
import {
  DeduplicatedRequestHandler,
  syncDeduplicationCache,
} from './DeduplicatedRequestHandler';
import { resolveKeyPrefix } from './utils/prefix';
import {
  type Client,
  redisErrorHandler,
  shouldDeferRedisConnection,
  waitForReadiness,
} from './utils/redisConnection';
import {
  ErrorBudget,
  closeClient,
  disconnectAndExit,
} from './utils/errorBudget';
import { createCacheClient } from './utils/cacheClient';
import { AbortableGet, createAbortableGet } from './utils/abortableGet';
import { compareAndUnlink } from './utils/compareAndUnlink';
import { RedisTimeoutError, untilDeadline } from './utils/deadline';
import {
  applyTagUpdate,
  areTagsExpired,
  areTagsStale,
  maxExpiredTimestamp,
  normalizeTagManifest,
  persistableTagManifest,
  type TagManifestEntry,
} from './utils/tagRevalidation';
import { parseEnvInt } from './utils/env';

export interface CacheComponentsEntry {
  value: ReadableStream<Uint8Array>;
  tags: string[];
  stale: number;
  timestamp: number;
  expire: number;
  revalidate: number;
}

export interface CacheComponentsHandler {
  get(
    cacheKey: string,
    softTags: string[],
  ): Promise<CacheComponentsEntry | undefined>;
  set(
    cacheKey: string,
    pendingEntry: Promise<CacheComponentsEntry>,
  ): Promise<void>;
  refreshTags(): Promise<void>;
  getExpiration(tags: string[]): Promise<number>;
  updateTags(tags: string[], durations?: { expire?: number }): Promise<void>;
}

// The JSON form in Redis. The value is the stream content as base64.
type StoredCacheEntry = Omit<CacheComponentsEntry, 'value'> & {
  value: string;
};

const REVALIDATED_TAGS_KEY = '__cacheComponents_revalidated_tags__';
// Older versions kept a tag index in this hash. It is not read anymore, but
// it can still be in Redis, so it must not look like a cache entry.
const LEGACY_SHARED_TAGS_KEY = '__cacheComponents_sharedTags__';

export type CreateCacheComponentsHandlerOptions =
  CreateRedisStringsHandlerOptions & { serverDistDir?: string };

async function streamToBuffer(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
    }
  }

  if (chunks.length === 1) {
    return chunks[0];
  }

  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

async function encodeValue(
  stream: ReadableStream<Uint8Array>,
): Promise<string> {
  return Buffer.from(await streamToBuffer(stream)).toString('base64');
}

function decodeValue(base64: string): ReadableStream<Uint8Array> {
  const buffer = new Uint8Array(Buffer.from(base64, 'base64'));
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(buffer);
      controller.close();
    },
  });
}

// The caller creates a new handler after a constructor failure, so these
// failures need one count across instances.
let constructorErrorBudget: ErrorBudget | undefined;

class RedisCacheComponentsHandler implements CacheComponentsHandler {
  // Not set when createClient() throws in the constructor.
  private client!: Client;
  private errorBudget: ErrorBudget;
  private revalidatedTagsMap: SyncedMap<TagManifestEntry | number>;
  private invalidateDeduplicationCache: (keys: string[]) => Promise<void>;
  private keyPrefix: string;
  private getTimeoutMs: number;
  private writeTimeoutMs: number;
  private redisGet: AbortableGet;
  private redisDeduplicationHandler: DeduplicatedRequestHandler<
    AbortableGet,
    string | null
  >;
  private deduplicatedRedisGet: (key: string) => AbortableGet;
  private redisGetDeduplication: boolean;
  private redisConnectionDeferred: boolean;

  constructor({
    redisUrl = process.env.REDIS_URL
      ? process.env.REDIS_URL
      : process.env.REDISHOST
        ? `redis://${process.env.REDISHOST}:${process.env.REDISPORT}`
        : 'redis://localhost:6379',
    database = process.env.VERCEL_ENV === 'production' ? 0 : 1,
    keyPrefix,
    getTimeoutMs = parseEnvInt('REDIS_COMMAND_TIMEOUT_MS', 500, { min: 1 }),
    writeTimeoutMs = parseEnvInt('REDIS_WRITE_TIMEOUT_MS', getTimeoutMs, {
      min: 1,
    }),
    revalidateTagQuerySize = 250,
    avgResyncIntervalMs = 60 * 60 * 1_000,
    redisGetDeduplication = true,
    inMemoryCachingTime = 10_000,
    socketOptions,
    clientOptions,
    killContainerOnErrorThreshold = parseEnvInt(
      'KILL_CONTAINER_ON_ERROR_THRESHOLD',
      0,
    ),
    serverDistDir,
  }: CreateCacheComponentsHandlerOptions) {
    this.errorBudget = new ErrorBudget(killContainerOnErrorThreshold, () =>
      disconnectAndExit(this.client),
    );
    try {
      this.keyPrefix = resolveKeyPrefix({
        optionKeyPrefix: keyPrefix,
        serverDistDir,
        env: process.env,
      });
      this.getTimeoutMs = getTimeoutMs;
      this.writeTimeoutMs = writeTimeoutMs;
      this.redisGetDeduplication = redisGetDeduplication;
      this.redisConnectionDeferred = shouldDeferRedisConnection();

      this.client = createCacheClient({
        label: 'RedisCacheComponentsHandler client',
        redisUrl,
        database,
        socketOptions,
        clientOptions,
        errorBudget: this.errorBudget,
        deferConnection: this.redisConnectionDeferred,
      });

      const filterKeys = (key: string): boolean =>
        key !== REVALIDATED_TAGS_KEY && key !== LEGACY_SHARED_TAGS_KEY;

      const subscriptions = createRedisSubscriptions(this.client);

      this.revalidatedTagsMap = new SyncedMap<TagManifestEntry | number>({
        client: this.client,
        subscriptions,
        keyPrefix: this.keyPrefix,
        redisKey: REVALIDATED_TAGS_KEY,
        database,
        querySize: revalidateTagQuerySize,
        filterKeys,
        customizedSync: { fieldsAreNotRedisKeys: true },
        resyncIntervalMs:
          avgResyncIntervalMs +
          avgResyncIntervalMs / 10 +
          Math.random() * (avgResyncIntervalMs / 10),
      });

      const redisGet = createAbortableGet(this.client);
      this.redisDeduplicationHandler = new DeduplicatedRequestHandler(
        redisGet,
        inMemoryCachingTime,
      );
      this.invalidateDeduplicationCache = syncDeduplicationCache(
        this.redisDeduplicationHandler,
        {
          client: this.client,
          subscriptions,
          channel: syncChannelName(
            this.keyPrefix,
            '__cacheComponents_inMemoryDeduplicationCache__',
          ),
        },
      );
      this.redisGet = redisGet;
      this.deduplicatedRedisGet =
        this.redisDeduplicationHandler.deduplicatedFunction;
    } catch (error) {
      console.error('RedisCacheComponentsHandler constructor error', error);
      closeClient(this.client);
      constructorErrorBudget ??= new ErrorBudget(
        killContainerOnErrorThreshold,
        () => disconnectAndExit(undefined),
      );
      constructorErrorBudget.record(
        error,
        'RedisCacheComponentsHandler constructor',
      );
      throw error;
    }
  }

  // Reads never reconnect: a closed client stays closed until its reconnect
  // owner (createCacheClient or a user reconnectStrategy) opens it again.
  private async assertClientIsReady(readyTimeoutMs: number): Promise<void> {
    await waitForReadiness(
      (timeoutMs) =>
        SyncedMap.waitUntilAllReady([this.revalidatedTagsMap], timeoutMs),
      readyTimeoutMs,
    );
  }

  private isClientUnavailable(): boolean {
    return this.redisConnectionDeferred || !this.client.isReady;
  }

  private tagManifestFor(tag: string, now = Date.now()): TagManifestEntry {
    return normalizeTagManifest(this.revalidatedTagsMap.get(tag), now);
  }

  async get(
    cacheKey: string,
    _softTags: string[],
  ): Promise<CacheComponentsEntry | undefined> {
    // Soft-tag staleness is handled by Next.js via getExpiration() (implicit
    // `_N_T_` tags). Explicit cacheTag()s are checked below via the tag
    // manifest, matching DefaultCacheHandler.get() in Next.js 16.0–16.3.
    void _softTags;

    // Construct the full Redis key
    // For cache components, Next.js provides the full key including environment prefix
    // We prepend our keyPrefix for multi-tenant isolation
    const redisKey = `${this.keyPrefix}${cacheKey}`;

    try {
      await this.assertClientIsReady(this.getTimeoutMs);
      if (this.isClientUnavailable()) {
        return undefined;
      }

      const readSerialized = (useDedup: boolean) =>
        redisErrorHandler(
          'RedisCacheComponentsHandler.get(), operation: get' +
            (useDedup ? ' deduplicated' : '') +
            ' ' +
            this.getTimeoutMs +
            'ms ' +
            redisKey,
          (useDedup ? this.deduplicatedRedisGet(cacheKey) : this.redisGet)(
            redisKey,
            AbortSignal.timeout(this.getTimeoutMs),
          ),
        );

      let usedDedup = this.redisGetDeduplication;
      let serialized = await readSerialized(usedDedup);

      for (let attempt = 0; attempt < 2; attempt++) {
        if (!serialized) {
          return undefined;
        }

        const stored: StoredCacheEntry = JSON.parse(serialized);
        const now = Date.now();
        const tags = stored.tags || [];
        const expiryTime = stored.timestamp + stored.expire * 1000;
        const ttlExpired =
          Number.isFinite(stored.expire) &&
          stored.expire > 0 &&
          now > expiryTime;
        const tagsExpired = areTagsExpired(tags, stored.timestamp, now, (tag) =>
          this.tagManifestFor(tag, now),
        );

        if (ttlExpired || tagsExpired) {
          if (usedDedup) {
            usedDedup = false;
            serialized = await readSerialized(false);
            continue;
          }

          const removal = compareAndUnlink(this.client, redisKey, serialized);
          try {
            await untilDeadline(
              removal,
              AbortSignal.timeout(this.getTimeoutMs),
            );
          } catch (error) {
            if (!(error instanceof RedisTimeoutError)) {
              throw error;
            }
            console.warn(
              'RedisCacheComponentsHandler.get() Redis did not confirm the removal of a stale entry within',
              this.getTimeoutMs,
              'ms. Returning a cache miss without it:',
              redisKey,
            );
          }
          return undefined;
        }

        const entry: CacheComponentsEntry = {
          ...stored,
          value: decodeValue(stored.value),
        };

        if (
          areTagsStale(tags, stored.timestamp, (tag) =>
            this.tagManifestFor(tag, now),
          )
        ) {
          entry.revalidate = -1;
        }

        return entry;
      }

      return undefined;
    } catch (error) {
      console.error(
        'RedisCacheComponentsHandler.get() Error occurred while getting cache entry. Returning undefined so site can continue to serve content while cache is disabled. The original error was:',
        error,
      );
      this.errorBudget.record(error, 'RedisCacheComponentsHandler.get()');
      return undefined;
    }
  }

  async set(
    cacheKey: string,
    pendingEntry: Promise<CacheComponentsEntry>,
  ): Promise<void> {
    try {
      await this.assertClientIsReady(this.writeTimeoutMs);
      if (this.isClientUnavailable()) {
        return;
      }

      const entry = await pendingEntry;

      const stored: StoredCacheEntry = {
        value: await encodeValue(entry.value),
        tags: Array.isArray(entry.tags) ? [...entry.tags] : [],
        stale: Number(entry.stale),
        timestamp: Number(entry.timestamp),
        expire: Number(entry.expire),
        revalidate: Number(entry.revalidate),
      };
      const serialized = JSON.stringify(stored);

      const seed = this.redisGetDeduplication
        ? this.redisDeduplicationHandler.seedRequestReturn(cacheKey, serialized)
        : undefined;

      // expire is already a duration in seconds, use it directly
      const ttlSeconds =
        Number.isFinite(stored.expire) && stored.expire > 0
          ? Math.floor(stored.expire)
          : undefined;

      const redisKey = `${this.keyPrefix}${cacheKey}`;

      const setOperation = redisErrorHandler(
        'RedisCacheComponentsHandler.set(), operation: set ' + redisKey,
        this.client.set(redisKey, serialized, {
          ...(ttlSeconds ? { EX: ttlSeconds } : {}),
        }),
      );
      // Redis did not store the value, so reads must not get it from the seed.
      if (seed) {
        setOperation.catch(() =>
          this.redisDeduplicationHandler.forget(cacheKey, seed),
        );
      }

      // When the new value is in Redis, remove the old copy and any cached
      // miss from the in-memory cache of every instance. Also when this
      // instance has the cache off, because other instances can have it on.
      const invalidateDeduplicationCacheOperation = setOperation.then(() =>
        this.invalidateDeduplicationCache([cacheKey]),
      );

      await untilDeadline(
        Promise.all([setOperation, invalidateDeduplicationCacheOperation]),
        AbortSignal.timeout(this.writeTimeoutMs),
      );
    } catch (error) {
      if (error instanceof RedisTimeoutError) {
        console.warn(
          'RedisCacheComponentsHandler.set() Redis did not confirm the write within',
          this.writeTimeoutMs,
          'ms. Returning without it, the entry may not be stored:',
          cacheKey,
        );
        return;
      }
      console.error(
        'RedisCacheComponentsHandler.set() Error occurred while setting cache entry. The original error was:',
        error,
      );
      this.errorBudget.record(error, 'RedisCacheComponentsHandler.set()');
      throw error;
    }
  }

  async refreshTags(): Promise<void> {
    try {
      await this.assertClientIsReady(this.getTimeoutMs);
    } catch (error) {
      console.error(
        'RedisCacheComponentsHandler.refreshTags() Redis is not ready. The original error was:',
        error,
      );
    }
  }

  async getExpiration(tags: string[]): Promise<number> {
    try {
      await this.assertClientIsReady(this.getTimeoutMs);
      return maxExpiredTimestamp(tags || [], (tag) => this.tagManifestFor(tag));
    } catch (error) {
      console.error(
        'RedisCacheComponentsHandler.getExpiration() Error occurred while getting expiration for tags. The original error was:',
        error,
      );
      return 0;
    }
  }

  async updateTags(
    tags: string[],
    durations?: { expire?: number },
  ): Promise<void> {
    try {
      await this.assertClientIsReady(this.writeTimeoutMs);
      if (this.isClientUnavailable()) {
        return;
      }

      const now = Date.now();
      const updated = new Set(tags || []);
      const writes: Promise<void>[] = [];
      for (const tag of updated) {
        const next = applyTagUpdate(this.tagManifestFor(tag), durations, now);
        writes.push(
          this.revalidatedTagsMap.set(tag, persistableTagManifest(next, now)),
        );
      }

      // No in-memory cache eviction is necessary: get() checks every entry,
      // also one from the in-memory cache, against the tag manifest.
      await untilDeadline(
        Promise.all(writes),
        AbortSignal.timeout(this.writeTimeoutMs),
      );
    } catch (error) {
      if (error instanceof RedisTimeoutError) {
        console.warn(
          'RedisCacheComponentsHandler.updateTags() Redis did not confirm the writes within',
          this.writeTimeoutMs,
          'ms. Returning without them, other instances may serve stale entries for tags:',
          tags,
        );
        return;
      }
      console.error(
        'RedisCacheComponentsHandler.updateTags() Error occurred while updating tags. The original error was:',
        error,
      );
      this.errorBudget.record(
        error,
        'RedisCacheComponentsHandler.updateTags()',
      );
      throw error;
    }
  }
}

let singletonHandler: CacheComponentsHandler | undefined;

export function getRedisCacheComponentsHandler(
  options: CreateCacheComponentsHandlerOptions = {},
): CacheComponentsHandler {
  if (!singletonHandler) {
    singletonHandler = new RedisCacheComponentsHandler(options);
  }
  return singletonHandler;
}

// Lazily resolve the default Cache Components handler.
//
// Constructing a RedisCacheComponentsHandler opens a Redis connection in its
// constructor. Building the singleton at module-eval time therefore means that simply
// *importing this package* — e.g. only for `RedisStringsHandler` (the legacy
// `cacheHandler`), with Cache Components never enabled — eagerly connects to Redis,
// defaulting to `redis://localhost:6379` when neither `REDIS_URL` nor `REDISHOST` is
// set. In a deployment whose Redis is not on localhost that yields a non-stop
// `RedisCacheComponentsHandler client error ECONNREFUSED 127.0.0.1:6379` reconnect
// loop, and it also makes a consumer's later `getRedisCacheComponentsHandler(options)`
// a no-op, because the singleton was already built with defaults (see #84).
//
// Defer construction to first use via a Proxy: importing the package never connects, a
// consumer that configures the handler via `getRedisCacheComponentsHandler(options)`
// before it is first used has that configuration honored, and a consumer that never
// touches Cache Components never opens a Redis connection at all.
let resolvedHandler: CacheComponentsHandler | undefined;

export const redisCacheHandler: CacheComponentsHandler = new Proxy(
  {} as CacheComponentsHandler,
  {
    get(_target, prop) {
      if (!resolvedHandler) {
        resolvedHandler = getRedisCacheComponentsHandler();
      }
      const value = resolvedHandler[prop as keyof CacheComponentsHandler];
      return typeof value === 'function'
        ? (value as (...args: unknown[]) => unknown).bind(resolvedHandler)
        : value;
    },
    has(_target, prop) {
      return prop in RedisCacheComponentsHandler.prototype;
    },
  },
);
