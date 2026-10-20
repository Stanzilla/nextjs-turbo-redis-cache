import type { RedisClientOptions } from 'redis';
import { SyncedMap } from './SyncedMap';
import {
  createRedisSubscriptions,
  syncChannelName,
} from './utils/redisSubscriptions';
import { resolveSharedOptions, resyncDelay } from './utils/options';
import {
  DeduplicatedRequestHandler,
  syncDeduplicationCache,
} from './DeduplicatedRequestHandler';
import { debug } from './utils/debug';
import { CacheValueSerializer, jsonCacheValueSerializer } from './serializer';
import { resolveCacheEntryTtlSeconds } from './utils/cacheTtl';
import {
  type Client,
  RedisNotReadyError,
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
import { RedisTimeoutError, untilDeadline } from './utils/deadline';
import { compareAndUnlink } from './utils/compareAndUnlink';
import { TagIndex } from './utils/tagIndex';

// Re-exported for imports that used this module before.
export { type Client, redisErrorHandler };

export type CacheControl = {
  revalidate: number | false;
  expire: number | undefined;
};

export type CacheEntry = {
  value: unknown;
  lastModified: number;
  tags: string[];
  /** The lifetime from `ctx.cacheControl` in set(). Next.js 16.4+ reads it
   * from get() so that every instance that shares Redis uses the lifetime of
   * the instance that rendered the entry. Older versions ignore it. */
  cacheControl?: CacheControl;
};

/** Discriminated union of the `ctx` argument Next.js passes to
 * {@link RedisStringsHandler.get}. The Pages Router (`PAGES`) has no PPR
 * concept, so `isRoutePPREnabled` is optional for it.
 *
 * Note: `REDIRECT` is intentionally absent. Next.js always calls `get()` with
 * `kind: 'PAGES'` for Pages Router routes, even when the stored value has
 * `kind: 'REDIRECT'`. The handler returns whatever value is cached and Next.js
 * interprets it accordingly. */
export type GetContext =
  | {
      kind: 'APP_ROUTE' | 'APP_PAGE';
      isRoutePPREnabled: boolean;
      isFallback: boolean;
    }
  | {
      kind: 'PAGES';
      isRoutePPREnabled?: boolean;
      isFallback: boolean;
    }
  | {
      kind: 'FETCH';
      revalidate: number;
      fetchUrl: string;
      fetchIdx: number;
      tags: string[];
      softTags: string[];
      isFallback: boolean;
    };

/** Discriminated union of cache payloads accepted by
 * {@link RedisStringsHandler.set}. `null` is a valid payload: the Pages Router
 * stores `notFound: true` results as a cache entry with a null value. */
export type SetCacheValue =
  | {
      kind: 'APP_PAGE';
      status?: number;
      headers: {
        'x-nextjs-stale-time': string; // timestamp in ms
        'x-next-cache-tags': string; // comma separated paths (tags)
      };
      html: string;
      rscData: Buffer;
      segmentData: unknown;
      postponed: unknown;
    }
  | {
      kind: 'APP_ROUTE';
      status: number;
      headers: {
        'cache-control'?: string;
        'x-nextjs-stale-time': string; // timestamp in ms
        'x-next-cache-tags': string; // comma separated paths (tags)
      };
      body: Buffer;
    }
  | {
      kind: 'PAGES';
      html: string;
      pageData: Record<string, unknown>;
      headers?: Record<string, number | string | string[] | undefined>;
      status?: number;
    }
  | {
      kind: 'REDIRECT';
      props: Record<string, unknown>;
    }
  | {
      kind: 'FETCH';
      data: {
        headers: Record<string, string>;
        body: string; // base64 encoded
        status: number;
        url: string;
      };
      revalidate: number | false;
    }
  | null;

if (process.env.DEBUG_CACHE_HANDLER) {
  // This is a test to check if the event loop is lagging. If it lags, increase CPU of container.
  // unref() so that this probe does not keep the process alive.
  setInterval(() => {
    const start = performance.now();
    setImmediate(() => {
      const duration = performance.now() - start;
      if (duration > 100) {
        debug(
          'yellow',
          `RedisStringsHandler detected an event loop lag of: ${duration.toFixed(2)}ms. If your container is hosted in a cloud provider with container suspension this is normal. If not you should increase the CPU of your container.`,
        );
      }
    });
  }, 500).unref();
}

export type CreateRedisStringsHandlerOptions = {
  /** Redis redisUrl to use.
   * @default process.env.REDIS_URL? process.env.REDIS_URL : process.env.REDISHOST
          ? `redis://${process.env.REDISHOST}:${process.env.REDISPORT}`
          : 'redis://localhost:6379'
   */
  redisUrl?: string;
  /** Redis database number to use. Uses DB 0 for production, DB 1 otherwise
   * @default process.env.VERCEL_ENV === 'production' ? 0 : 1
   */
  database?: number;
  /** Prefix added to all Redis keys
   * @default process.env.KEY_PREFIX || process.env.VERCEL_URL || 'UNDEFINED_URL_'
   */
  keyPrefix?: string;
  /** Timeout in milliseconds for time critical Redis operations (during cache get, which blocks site rendering).
   *  If redis get is not fulfilled within this time, the cache handler will return null so site rendering will
   *  not be blocked further and site can fallback to re-render/re-fetch the content.
   * @default 500
   */
  getTimeoutMs?: number;
  /** Timeout in milliseconds for the Redis writes that a request waits for (set, revalidateTag).
   *  If Redis does not confirm them within this time, `set` logs a warning and returns, so the
   *  request is not blocked further. The write can still be applied later, or be lost.
   *  `revalidateTag` logs an error and throws a `RedisTimeoutError` instead, because a lost
   *  invalidation leaves stale entries on every instance.
   * @default process.env.REDIS_WRITE_TIMEOUT_MS, else getTimeoutMs
   */
  writeTimeoutMs?: number;
  /** Number of entries to query in one batch during full sync of shared tags hash map
   * @default 250
   */
  revalidateTagQuerySize?: number;
  /** Key used to store shared tags hash map in Redis
   * @default '__sharedTags__'
   */
  sharedTagsKey?: string;
  /** Keep an in-memory tag to keys index next to the shared tags map, so
   *  revalidateTag looks up the keys of its tags instead of scanning every
   *  entry. It costs memory and CPU for each set. Enable it only with about
   *  1e5 tagged entries or more, or when a measured scan p95 is 5 ms or more
   *  with 1 or more revalidateTag per second. Only RedisStringsHandler uses it.
   * @default false
   */
  tagIndex?: boolean;
  /** Average interval in milliseconds between tag map full re-syncs
   * @default 3600000 (1 hour)
   */
  avgResyncIntervalMs?: number;
  /** Enable deduplication of Redis get requests via internal in-memory cache
   * @default true
   */
  redisGetDeduplication?: boolean;
  /** Time in milliseconds to cache Redis get results in memory. Set this to 0 to disable in-memory caching completely
   * @default 10000
   */
  inMemoryCachingTime?: number;
  /** Default stale age in seconds for cached items
   * @default 1209600 (14 days)
   */
  defaultStaleAge?: number;
  /** Function to calculate expire age (redis TTL value) from stale age
   * @default Production: staleAge * 2, Other: staleAge * 1.2
   */
  estimateExpireAge?: (staleAge: number) => number;
  /** Kill container on Redis client error if error threshold is reached
   * @default 0 (0 means no error threshold)
   */
  killContainerOnErrorThreshold?: number;
  /** Additional Redis client socket options
   * @example { tls: true, rejectUnauthorized: false }
   */
  socketOptions?: RedisClientOptions['socket'];
  /** Additional Redis client options to be passed directly to createClient
   * @example { username: 'user', password: 'pass' }
   */
  clientOptions?: Omit<RedisClientOptions, 'url' | 'database' | 'socket'>;
  /** Pluggable wire-format codec for Redis string values. Lets you plug in
   * compression (gzip/brotli), encryption, or any other custom encoding without
   * forking this package or losing the existing dedup / batch / keyspace features.
   *
   * Both `serialize` and `deserialize` may return a `Promise`, enabling
   * non-blocking async codecs (e.g. `zlib.brotliCompress`) that don't block the
   * Node.js event loop. Synchronous implementations continue to work unchanged.
   *
   * Only the main cache-entry storage path is routed through the serializer.
   * The shared-tags map and the revalidated-tags map are not. The in-memory
   * deduplication cache stores the wire-format string verbatim - its contents
   * change with the serializer, but the cache itself is not re-encoded.
   *
   * Operational note: changing the serializer (or any of its parameters such as
   * a compression level or encryption key) makes existing Redis keys unreadable.
   * Either flush the affected keys or bump `keyPrefix` before deploying.
   *
   * @default jsonCacheValueSerializer (JSON.stringify with bufferAndMapReplacer
   * so native Buffer and Map values inside a CacheEntry round-trip transparently)
   */
  valueSerializer?: CacheValueSerializer;
};

// Identifier prefix used by Next.js to mark automatically generated cache tags
// These tags are created internally by Next.js for route-based invalidation
const NEXT_CACHE_IMPLICIT_TAG_ID = '_N_T_';

// Next.js 16.4+ keys route entries as /route-cache/<kind>/<sha256(sourceRoute)>/$/<path>
// (next/dist/server/lib/route-cache-key.js). Older versions use /<path> as the key,
// and that path can itself start with /route-cache/, so match the full shape.
const ROUTE_CACHE_KEY = /^\/route-cache\/[A-Z_]+\/[0-9a-f]{64}\/\$(\/.*)$/;

function getRoutePath(key: string): string {
  return ROUTE_CACHE_KEY.exec(key)?.[1] ?? key;
}

// Redis key used to store a map of tags and their last revalidation timestamps
// This helps track when specific tags were last invalidated
const REVALIDATED_TAGS_KEY = '__revalidated_tags__';

// The caller creates a new handler after a constructor failure, so these
// failures need one count across instances.
let constructorErrorBudget: ErrorBudget | undefined;

// Cache kinds this handler is designed and tested for. Kept as the single
// source of truth for both payload validation and the warning message, so
// adding a kind is a one-line change here instead of edits scattered across
// the type union, the guard chain, and the log string.
const SUPPORTED_GET_KINDS = [
  'APP_ROUTE',
  'APP_PAGE',
  'PAGES',
  'FETCH',
] as const;
const SUPPORTED_SET_KINDS = [
  'APP_ROUTE',
  'APP_PAGE',
  'PAGES',
  'REDIRECT',
  'FETCH',
] as const;

/**
 * Returns why a deserialized entry cannot be served, or undefined if it can.
 * `value: null` is valid: the Pages Router stores `notFound: true` results as
 * an entry with a null value.
 */
function malformedCacheEntryReason(entry: CacheEntry): string | undefined {
  if (typeof entry !== 'object') return 'not an object';
  if (!Array.isArray(entry.tags)) return 'missing tags';
  if (entry.value === undefined) return 'missing value';
  if (!Number.isFinite(entry.lastModified)) return 'missing lastModified';
  return undefined;
}

export default class RedisStringsHandler {
  // Not set when createClient() throws in the constructor.
  private client!: Client;
  private sharedTagsMap: SyncedMap<string[], TagIndex>;
  private revalidatedTagsMap: SyncedMap<number>;
  private invalidateDeduplicationCache: (keys: string[]) => Promise<void>;
  private getTimeoutMs: number;
  private writeTimeoutMs: number;
  private redisGet: AbortableGet;
  private redisDeduplicationHandler: DeduplicatedRequestHandler<
    AbortableGet,
    string | null
  >;
  private deduplicatedRedisGet: (key: string) => AbortableGet;
  private keyPrefix: string;
  private redisGetDeduplication: boolean;
  private inMemoryCachingTime: number;
  private defaultStaleAge: number;
  private estimateExpireAge: (staleAge: number) => number;
  private errorBudget: ErrorBudget;
  private valueSerializer: CacheValueSerializer;
  private redisConnectionDeferred: boolean;

  constructor(options: CreateRedisStringsHandlerOptions) {
    const {
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
    } = resolveSharedOptions(options);
    const {
      keyPrefix = process.env.KEY_PREFIX ||
        process.env.VERCEL_URL ||
        'UNDEFINED_URL_',
      sharedTagsKey = '__sharedTags__',
      defaultStaleAge = 60 * 60 * 24 * 14,
      estimateExpireAge = (staleAge) =>
        process.env.VERCEL_ENV === 'production' ? staleAge * 2 : staleAge * 1.2,
      valueSerializer = jsonCacheValueSerializer,
      tagIndex = false,
    } = options;
    this.errorBudget = new ErrorBudget(killContainerOnErrorThreshold, () =>
      disconnectAndExit(this.client),
    );
    try {
      this.keyPrefix = keyPrefix;
      this.redisGetDeduplication = redisGetDeduplication;
      this.inMemoryCachingTime = inMemoryCachingTime;
      this.defaultStaleAge = defaultStaleAge;
      this.estimateExpireAge = estimateExpireAge;
      this.getTimeoutMs = getTimeoutMs;
      this.writeTimeoutMs = writeTimeoutMs;
      this.valueSerializer = valueSerializer;
      this.redisConnectionDeferred = shouldDeferRedisConnection();

      try {
        this.client = createCacheClient({
          label: 'Redis client',
          redisUrl,
          database,
          socketOptions,
          clientOptions,
          errorBudget: this.errorBudget,
          deferConnection: this.redisConnectionDeferred,
        });
      } catch (error: unknown) {
        console.error('Failed to initialize Redis client');
        throw error;
      }

      const filterKeys = (key: string): boolean =>
        key !== REVALIDATED_TAGS_KEY && key !== sharedTagsKey;

      const subscriptions = createRedisSubscriptions(this.client);

      this.sharedTagsMap = new SyncedMap<string[], TagIndex>({
        client: this.client,
        subscriptions,
        keyPrefix,
        redisKey: sharedTagsKey,
        database,
        querySize: revalidateTagQuerySize,
        filterKeys,
        resyncIntervalMs: resyncDelay(avgResyncIntervalMs, 'before'),
        createIndex: tagIndex ? () => new TagIndex() : undefined,
      });

      this.revalidatedTagsMap = new SyncedMap<number>({
        client: this.client,
        subscriptions,
        keyPrefix,
        redisKey: REVALIDATED_TAGS_KEY,
        database,
        querySize: revalidateTagQuerySize,
        filterKeys,
        customizedSync: { fieldsAreNotRedisKeys: true },
        resyncIntervalMs: resyncDelay(avgResyncIntervalMs, 'after'),
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
          channel: syncChannelName(keyPrefix, 'inMemoryDeduplicationCache'),
        },
      );
      this.redisGet = redisGet;
      this.deduplicatedRedisGet =
        this.redisDeduplicationHandler.deduplicatedFunction;
    } catch (error) {
      console.error('RedisStringsHandler constructor error', error);
      closeClient(this.client);
      constructorErrorBudget ??= new ErrorBudget(
        killContainerOnErrorThreshold,
        () => disconnectAndExit(undefined),
      );
      constructorErrorBudget.record(error, 'RedisStringsHandler constructor');
      throw error;
    }
  }

  resetRequestCache(): void {}

  private async assertClientIsReady(readyTimeoutMs: number): Promise<void> {
    await waitForReadiness(
      (timeoutMs) =>
        SyncedMap.waitUntilAllReady(
          [this.sharedTagsMap, this.revalidatedTagsMap],
          timeoutMs,
        ),
      readyTimeoutMs,
    );
    if (!this.client.isReady) {
      if (this.redisConnectionDeferred) {
        return;
      }
      throw new RedisNotReadyError(
        'assertClientIsReady: Redis client is not ready yet or connection is lost.',
      );
    }
  }

  private isClientUnavailable(): boolean {
    return this.redisConnectionDeferred || !this.client.isReady;
  }

  public async get(key: string, ctx: GetContext): Promise<CacheEntry | null> {
    try {
      if (!(SUPPORTED_GET_KINDS as readonly string[]).includes(ctx.kind)) {
        console.warn(
          'RedisStringsHandler.get() called with',
          key,
          ctx,
          `this cache handler is only designed and tested for kinds ${SUPPORTED_GET_KINDS.join(', ')} and not for kind`,
          (ctx as { kind: string })?.kind,
        );
      }

      debug('green', 'RedisStringsHandler.get() called with', key, ctx);
      await this.assertClientIsReady(this.getTimeoutMs);
      if (this.isClientUnavailable()) {
        return null;
      }

      const clientGet = this.redisGetDeduplication
        ? this.deduplicatedRedisGet(key)
        : this.redisGet;

      const redisGetOperation = clientGet(
        this.keyPrefix + key,
        AbortSignal.timeout(this.getTimeoutMs),
      ).catch((error) => {
        if (error instanceof RedisTimeoutError) {
          return null;
        }
        throw error;
      });
      const serializedCacheEntry = await redisErrorHandler(
        'RedisStringsHandler.get(), operation: get' +
          (this.redisGetDeduplication ? 'deduplicated' : '') +
          ' ' +
          this.getTimeoutMs +
          'ms ' +
          this.keyPrefix +
          ' ' +
          key,
        redisGetOperation,
      );

      debug(
        'green',
        'RedisStringsHandler.get() finished with result (serializedCacheEntry)',
        serializedCacheEntry?.substring(0, 200),
      );

      if (!serializedCacheEntry) {
        return null;
      }

      let cacheEntry: CacheEntry | null;
      try {
        cacheEntry =
          await this.valueSerializer.deserialize(serializedCacheEntry);
      } catch (err) {
        // A decode failure (e.g. legacy/corrupted entry after swapping codecs,
        // bumping a compression level or rotating an encryption key) is treated
        // as a cache miss rather than a hard error - this matches the
        // null-from-deserialize semantics in the CacheValueSerializer contract
        // and prevents a single unreadable entry from incrementing
        // the error budget on every read.
        console.warn(
          'RedisStringsHandler.get() valueSerializer.deserialize failed, treating as cache miss',
          this.keyPrefix + key,
          err,
        );
        return null;
      }

      debug(
        'green',
        'RedisStringsHandler.get() finished with result (cacheEntry)',
        JSON.stringify(cacheEntry).substring(0, 200),
      );

      if (!cacheEntry) {
        return null;
      }

      const malformed = malformedCacheEntryReason(cacheEntry);
      if (malformed) {
        // Without tags or lastModified the staleness checks below cannot
        // run, so a malformed entry is a miss. It is not an error.
        console.warn(
          'RedisStringsHandler.get() cacheEntry is malformed, treating as cache miss',
          this.keyPrefix + key,
          malformed,
        );
        return null;
      }

      if (ctx.kind === 'FETCH') {
        const combinedTags = new Set([
          ...(ctx?.softTags || []),
          ...(ctx?.tags || []),
        ]);

        if (combinedTags.size === 0) {
          return cacheEntry;
        }

        // INFO: implicit tags (revalidate of nested fetch in api route/page on revalidatePath call of the page/api route). See revalidateTag() for more information
        //
        // This code checks if any of the cache tags associated with this entry (normally the internal tag of the parent page/api route containing the fetch request)
        // have been revalidated since the entry was last modified. If any tag was revalidated more recently than the entry's
        // lastModified timestamp, then the cached content is considered stale (therefore return null) and should be removed.
        for (const tag of combinedTags) {
          // Get the last revalidation time for this tag from our revalidatedTagsMap
          const revalidationTime = this.revalidatedTagsMap.get(tag);

          // If we have a revalidation time for this tag and it's more recent than when
          // this cache entry was last modified, the entry is stale
          if (revalidationTime && revalidationTime > cacheEntry.lastModified) {
            const redisKey = this.keyPrefix + key;

            // Remove only this key, and only while it still holds the stale
            // value: a concurrent set() can already have written the
            // replacement. Keep the tag record in revalidatedTagsMap, because
            // every other entry under the tag needs it to see that it is
            // stale. Keep the sharedTagsMap membership too: the replacement
            // can own it, and the orphan cleanup removes it once the key is gone.
            void compareAndUnlink(
              this.client,
              redisKey,
              serializedCacheEntry,
            ).catch((err) => {
              console.error(
                'Error occurred while unlinking stale data. Error was:',
                err,
              );
            });

            debug(
              'green',
              'RedisStringsHandler.get() found revalidation time for tag. Cache entry is stale and will be deleted and "null" will be returned.',
              tag,
              redisKey,
              revalidationTime,
              cacheEntry,
            );

            // Return null to indicate no valid cache entry was found
            return null;
          }
        }
      }

      // Match Next's FileSystemCache: fallback and postponed PPR entries do not
      // expose a complete Flight payload via rscData on read.
      const value = cacheEntry.value as SetCacheValue;
      if (
        ctx.kind === 'APP_PAGE' &&
        value != null &&
        value.kind === 'APP_PAGE'
      ) {
        const postponedState =
          value.postponed ?? (value as { postboned?: unknown }).postboned;
        if (
          ctx.isFallback ||
          (ctx.isRoutePPREnabled && postponedState != null)
        ) {
          return {
            ...cacheEntry,
            value: { ...value, rscData: undefined },
          };
        }
      }

      return cacheEntry;
    } catch (error) {
      // This catch block is necessary to handle any errors that may occur during:
      // 1. Redis operations (get, unlink)
      // 2. Tag validation and cleanup
      // (Deserialization errors are handled locally above and treated as cache misses,
      // so they do not reach this branch and do not count toward the kill threshold.)
      // If any error occurs, we return null to indicate no valid cache entry was found,
      // allowing the application to regenerate the content rather than crash
      console.error(
        'RedisStringsHandler.get() Error occurred while getting cache entry. Returning null so site can continue to serve content while cache is disabled. The original error was:',
        error,
      );
      this.errorBudget.record(error, 'RedisStringsHandler.get()');
      return null;
    }
  }
  public async set(
    key: string,
    data: SetCacheValue,
    ctx: {
      isRoutePPREnabled: boolean;
      isFallback: boolean;
      tags?: string[];
      // Different versions of Next.js use different arguments for the same functionality
      revalidate?: number | false; // Version 15.0.3
      cacheControl?: CacheControl; // Version 15.0.3+
    },
  ) {
    try {
      if (
        data !== null &&
        !(SUPPORTED_SET_KINDS as readonly string[]).includes(data.kind)
      ) {
        console.warn(
          'RedisStringsHandler.set() called with',
          key,
          ctx,
          data,
          `this cache handler is only designed and tested for kinds ${SUPPORTED_SET_KINDS.join(', ')} and not for kind`,
          (data as { kind: string })?.kind,
        );
      }

      await this.assertClientIsReady(this.writeTimeoutMs);
      if (this.isClientUnavailable()) {
        return;
      }

      const tags = [...(ctx.tags || [])];
      if (data?.kind === 'APP_PAGE' || data?.kind === 'APP_ROUTE') {
        tags.push(...(data.headers['x-next-cache-tags']?.split(',') || []));
      }

      // Pages Router entries (PAGES, REDIRECT and null/notFound results) do not
      // carry an x-next-cache-tags header. Attach the implicit path tag
      // (_N_T_/<path>) so that revalidatePath()/revalidateTag('_N_T_/<path>')
      // invalidates them the same way as App Router entries.
      if (data === null || data.kind === 'PAGES' || data.kind === 'REDIRECT') {
        const implicitTag = NEXT_CACHE_IMPLICIT_TAG_ID + getRoutePath(key);
        if (!tags.includes(implicitTag)) tags.push(implicitTag);
      }

      // Constructing and serializing the value for storing it in redis
      const cacheEntry: CacheEntry = {
        lastModified: Date.now(),
        tags,
        value: data,
        cacheControl: ctx.cacheControl,
      };
      const serializedCacheEntry =
        await this.valueSerializer.serialize(cacheEntry);

      // pre seed data into deduplicated get client. This will reduce redis load by not requesting
      // the same value from redis which was just set. It also replaces a cached miss, so that reads
      // on this instance during the redis SET do not return the old result.
      const seed = this.redisGetDeduplication
        ? this.redisDeduplicationHandler.seedRequestReturn(
            key,
            serializedCacheEntry,
          )
        : undefined;

      const expireAt = resolveCacheEntryTtlSeconds(ctx, data, {
        estimateExpireAge: this.estimateExpireAge,
        defaultStaleAge: this.defaultStaleAge,
      });

      // Setting the cache entry in redis
      const setOperation: Promise<string | null> = redisErrorHandler(
        'RedisStringsHandler.set(), operation: set ' +
          this.keyPrefix +
          ' ' +
          key,
        this.client.set(this.keyPrefix + key, serializedCacheEntry, {
          ...(expireAt ? { EX: expireAt } : {}),
        }),
      );
      // Redis did not store the value, so reads must not get it from the seed.
      if (seed) {
        setOperation.catch(() =>
          this.redisDeduplicationHandler.forget(key, seed),
        );
      }

      // When the new value is in redis, remove the old copy from the in-memory deduplication cache
      // of every instance. Otherwise the other instances serve the old value for up to inMemoryCachingTime.
      // This also removes the seed on this instance, so its next read gets the new value from redis.
      const invalidateDeduplicationCacheOperation =
        this.redisGetDeduplication && this.inMemoryCachingTime > 0
          ? setOperation.then(() => this.invalidateDeduplicationCache([key]))
          : undefined;

      debug(
        'blue',
        'RedisStringsHandler.set() will set the following serializedCacheEntry',
        this.keyPrefix,
        key,
        data,
        ctx,
        serializedCacheEntry?.substring(0, 200),
        expireAt,
      );

      // Write the tag membership after every successful SET, also when the
      // tags did not change. The orphan cleanup of another instance can remove
      // the field between a SET and this write, so each write heals it. An
      // untagged entry removes the membership of the entry it replaces.
      const setTagsOperation = setOperation.then(() => {
        if (tags.length > 0) return this.sharedTagsMap.set(key, tags);
        if (this.sharedTagsMap.has(key)) return this.sharedTagsMap.delete(key);
      });

      debug(
        'blue',
        'RedisStringsHandler.set() will set the following sharedTagsMap',
        key,
        tags,
      );

      await untilDeadline(
        Promise.all([
          setOperation,
          setTagsOperation,
          invalidateDeduplicationCacheOperation,
        ]),
        AbortSignal.timeout(this.writeTimeoutMs),
      );
    } catch (error) {
      if (error instanceof RedisTimeoutError) {
        console.warn(
          'RedisStringsHandler.set() Redis did not confirm the write within',
          this.writeTimeoutMs,
          'ms. Returning without it, the entry may not be stored:',
          this.keyPrefix + key,
        );
        return;
      }
      console.error(
        'RedisStringsHandler.set() Error occurred while setting cache entry. The original error was:',
        error,
      );
      this.errorBudget.record(error, 'RedisStringsHandler.set()');
      throw error;
    }
  }

  /** The cache keys that depend on at least one of the tags. */
  private keysWithAnyTag(tags: Set<string>): Set<string> {
    const index = this.sharedTagsMap.index;
    if (index) return index.keysWithAnyTag(tags);
    const keys = new Set<string>();
    for (const [key, sharedTags] of this.sharedTagsMap.entries()) {
      if (sharedTags.some((tag) => tags.has(tag))) keys.add(key);
    }
    return keys;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public async revalidateTag(tagOrTags: string | string[], ...rest: any[]) {
    try {
      debug(
        'red',
        'RedisStringsHandler.revalidateTag() called with',
        tagOrTags,
        rest,
      );
      const tags = new Set([tagOrTags || []].flat());
      await this.assertClientIsReady(this.writeTimeoutMs);
      if (this.isClientUnavailable()) {
        return;
      }

      // Start every write before waiting, so that one deadline bounds them
      // all and a timeout does not skip the writes after it.
      const writes: Promise<unknown>[] = [];

      for (const tag of tags) {
        // INFO: implicit tags (revalidate of nested fetch in api route/page on revalidatePath call of the page/api route)
        //
        // Invalidation logic for fetch requests that are related to a invalidated page.
        // revalidateTag is called for the page tag (_N_T_...) and the fetch request needs to be invalidated as well
        // unfortunately this is not possible since the revalidateTag is not called with any data that would allow us to find the cache entry of the fetch request
        // in case of a fetch request get method call, the get method of the cache handler is called with some information about the pages/routes the fetch request is inside
        // therefore we only mark the page/route as stale here (with help of the revalidatedTagsMap)
        // and delete the cache entry of the fetch request on the next request to the get function
        if (tag.startsWith(NEXT_CACHE_IMPLICIT_TAG_ID)) {
          const now = Date.now();
          debug(
            'red',
            'RedisStringsHandler.revalidateTag() set revalidation time for tag',
            tag,
            'to',
            now,
          );
          writes.push(this.revalidatedTagsMap.set(tag, now));
        }
      }

      const keysToDelete = this.keysWithAnyTag(tags);

      debug(
        'red',
        'RedisStringsHandler.revalidateTag() found',
        keysToDelete,
        'keys to delete',
      );

      if (keysToDelete.size > 0) {
        // prepare deletion of all keys in redis that are related to this tag
        const redisKeys = Array.from(keysToDelete);
        const fullRedisKeys = redisKeys.map((key) => this.keyPrefix + key);
        writes.push(
          redisErrorHandler(
            'RedisStringsHandler.revalidateTag(), operation: unlink ' +
              this.keyPrefix +
              ' ' +
              fullRedisKeys,
            this.client.unlink(fullRedisKeys),
          ),
        );

        // also delete entries from in-memory deduplication cache if they get revalidated
        if (this.redisGetDeduplication && this.inMemoryCachingTime > 0) {
          writes.push(this.invalidateDeduplicationCache(redisKeys));
        }

        // prepare deletion of entries from shared tags map if they get revalidated so that the map will not grow indefinitely
        writes.push(this.sharedTagsMap.delete(redisKeys));
      }

      await untilDeadline(
        Promise.all(writes),
        AbortSignal.timeout(this.writeTimeoutMs),
      );
      debug(
        'red',
        'RedisStringsHandler.revalidateTag() finished delete operations',
      );
    } catch (error) {
      // Unlike set(), a lost invalidation leaves stale entries on every
      // instance, so the caller must see it fail.
      if (error instanceof RedisTimeoutError) {
        console.error(
          'RedisStringsHandler.revalidateTag() Redis did not confirm the writes within',
          this.writeTimeoutMs,
          'ms. Other instances may serve stale entries for tags:',
          tagOrTags,
        );
        throw error;
      }
      console.error(
        'RedisStringsHandler.revalidateTag() Error occurred while revalidating tags. The original error was:',
        error,
      );
      this.errorBudget.record(error, 'RedisStringsHandler.revalidateTag()');
      throw error;
    }
  }
}
