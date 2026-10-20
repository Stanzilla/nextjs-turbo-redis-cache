import { debugVerbose } from './utils/debug';
import { type Client, redisErrorHandler } from './utils/redisConnection';
import type { RedisSubscriptions } from './utils/redisSubscriptions';
import type { SyncMessage } from './SyncedMap';

export class DeduplicatedRequestHandler<
  T extends (...args: [never, never]) => Promise<K>,
  K,
> {
  private inMemoryDeduplicationCache = new Map<string, Promise<K>>();
  private expiryTimers = new Set<ReturnType<typeof setTimeout>>();
  private cachingTimeMs: number;
  private fn: T;

  constructor(fn: T, cachingTimeMs: number) {
    this.fn = fn;
    this.cachingTimeMs = cachingTimeMs;
  }

  /** Removes the entries for `keys` on this instance only. */
  invalidateLocal(keys: string[]): void {
    for (const key of keys) this.inMemoryDeduplicationCache.delete(key);
  }

  /** Removes all entries on this instance only. */
  clearLocal(): void {
    this.inMemoryDeduplicationCache.clear();
  }

  /** Removes all entries and stops their expiry timers. */
  close(): void {
    for (const timer of this.expiryTimers) clearTimeout(timer);
    this.expiryTimers.clear();
    this.clearLocal();
  }

  /**
   * Put a known result into the cache, for example the value that set() is
   * writing. It expires `cachingTimeMs` after now. Returns the cached promise,
   * so that the caller can {@link forget} exactly this entry later.
   */
  seedRequestReturn(key: string, value: K): Promise<K> {
    const resultPromise = Promise.resolve(value);
    this.inMemoryDeduplicationCache.set(key, resultPromise);

    debugVerbose(
      'DeduplicatedRequestHandler.seedRequestReturn() seeded result ',
      key,
      String(value).substring(0, 200),
    );

    this.expireLater(key, resultPromise);
    return resultPromise;
  }

  /**
   * Remove the entry for `key` only while it is still `promise`. A newer
   * entry for the same key stays. The removal is local to this instance.
   */
  forget(key: string, promise: Promise<K>): void {
    if (this.inMemoryDeduplicationCache.get(key) === promise) {
      this.inMemoryDeduplicationCache.delete(key);
    }
  }

  // Each instance runs its own timer, so the expiry is local and does not
  // publish a sync message.
  private expireLater(key: string, promise: Promise<K>): void {
    const timer = setTimeout(() => {
      this.expiryTimers.delete(timer);
      debugVerbose(
        'DeduplicatedRequestHandler expiring key from inMemoryDeduplicationCache after',
        key,
        this.cachingTimeMs,
        'ms',
      );
      this.forget(key, promise);
    }, this.cachingTimeMs);
    this.expiryTimers.add(timer);
  }

  /**
   * Returns a function that shares one pending or recent result per key. A
   * fetched result expires `cachingTimeMs` after it settles. A rejected
   * request is removed at once, so the next caller tries again instead of
   * getting the same error replayed.
   *
   * Callers get the cached promise itself, not a copy. Both handlers cache
   * strings, which are immutable.
   */
  deduplicatedFunction = (key: string): T => {
    debugVerbose(
      'DeduplicatedRequestHandler.deduplicatedFunction() called with',
      key,
    );
    const dedupedFn = (...args: [never, never]): Promise<K> => {
      const cached = this.inMemoryDeduplicationCache.get(key);
      if (cached) {
        debugVerbose(
          'DeduplicatedRequestHandler.deduplicatedFunction().dedupedFn ',
          key,
          'found key in inMemoryDeduplicationCache',
        );
        return cached;
      }

      const promise = this.fn(...args);
      this.inMemoryDeduplicationCache.set(key, promise);

      debugVerbose(
        'DeduplicatedRequestHandler.deduplicatedFunction().dedupedFn ',
        key,
        'did not find key in inMemoryDeduplicationCache. Fetching it now',
      );

      promise.then(
        () => this.expireLater(key, promise),
        () => this.forget(key, promise),
      );
      return promise;
    };
    return dedupedFn as T;
  };
}

/**
 * Keeps the in-memory cache of `handler` in step with the other instances.
 * A delete message on `channel` removes the given keys. When the subscriber
 * connection is lost, delete messages can be lost too, so the whole cache is
 * cleared, and cleared again when the subscriber listens again. Expired and
 * evicted Redis keys are not tracked; such an entry stays for at most
 * `cachingTimeMs`.
 *
 * Returns a function that removes keys on this instance and then publishes
 * the delete message to the other instances.
 */
export function syncDeduplicationCache(
  handler: Pick<
    DeduplicatedRequestHandler<never, unknown>,
    'invalidateLocal' | 'clearLocal'
  >,
  options: {
    client: Client;
    subscriptions: RedisSubscriptions;
    channel: string;
  },
): (keys: string[]) => Promise<void> {
  const { client, subscriptions, channel } = options;
  subscriptions.add({
    channels: [
      [
        channel,
        (message) => {
          const syncMessage: SyncMessage<unknown> = JSON.parse(message);
          if (syncMessage.type === 'delete' && syncMessage.keys) {
            handler.invalidateLocal(syncMessage.keys);
          }
        },
      ],
    ],
    onError: (_error, lost) => {
      if (!lost) return;
      handler.clearLocal();
      // Reads before the new subscriber listens can cache a value that a
      // lost message would have removed.
      subscriptions.ready().then(
        () => handler.clearLocal(),
        () => undefined,
      );
    },
  });

  return async (keys) => {
    handler.invalidateLocal(keys);
    const message: SyncMessage<unknown> = { type: 'delete', keys };
    await redisErrorHandler(
      'DeduplicatedRequestHandler, operation: publish ' + channel + ' ' + keys,
      client.publish(channel, JSON.stringify(message)),
    );
  };
}
