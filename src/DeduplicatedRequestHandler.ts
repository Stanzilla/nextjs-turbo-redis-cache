import { debugVerbose } from './utils/debug';
import { SyncedMap } from './SyncedMap';
export class DeduplicatedRequestHandler<
  T extends (...args: [never, never]) => Promise<K>,
  K,
> {
  private inMemoryDeduplicationCache: SyncedMap<Promise<K>>;
  private cachingTimeMs: number;
  private fn: T;

  constructor(
    fn: T,
    cachingTimeMs: number,
    inMemoryDeduplicationCache: SyncedMap<Promise<K>>,
  ) {
    this.fn = fn;
    this.cachingTimeMs = cachingTimeMs;
    this.inMemoryDeduplicationCache = inMemoryDeduplicationCache;
  }

  /**
   * Put a known result into the cache, for example the value that set() is
   * writing. It expires `cachingTimeMs` after now. Returns the cached promise,
   * so that the caller can {@link forget} exactly this entry later.
   */
  seedRequestReturn(key: string, value: K): Promise<K> {
    const resultPromise = Promise.resolve(value);
    void this.inMemoryDeduplicationCache.set(key, resultPromise);

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
      void this.inMemoryDeduplicationCache.delete(key, true);
    }
  }

  // Each instance runs its own timer, so the expiry is local and does not
  // publish a sync message.
  private expireLater(key: string, promise: Promise<K>): void {
    setTimeout(() => {
      debugVerbose(
        'DeduplicatedRequestHandler expiring key from inMemoryDeduplicationCache after',
        key,
        this.cachingTimeMs,
        'ms',
      );
      this.forget(key, promise);
    }, this.cachingTimeMs);
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
      void this.inMemoryDeduplicationCache.set(key, promise);

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
