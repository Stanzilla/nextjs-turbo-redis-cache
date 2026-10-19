// SyncedMap.ts
import { Client, redisErrorHandler } from './RedisStringsHandler';
import { debugVerbose, debug } from './utils/debug';
import { shouldDeferRedisConnection } from './utils/redisConnection';

type CustomizedSync = {
  withoutRedisHashmap?: boolean;
  withoutSetSync?: boolean;
  /**
   * Skip SCAN-based orphan cleanup. Required for maps whose keys are not Redis
   * string keys (tag manifests). Otherwise startup/resync HDELs every tag.
   */
  withoutOrphanCleanup?: boolean;
};

type SyncedMapOptions = {
  client: Client;
  keyPrefix: string;
  redisKey: string; // Redis Hash key
  database: number;
  querySize: number;
  filterKeys: (key: string) => boolean;
  resyncIntervalMs?: number;
  customizedSync?: CustomizedSync;
};

export type SyncMessage<V> = {
  type: 'insert' | 'delete';
  key?: string;
  value?: V;
  keys?: string[];
};

const SYNC_CHANNEL_SUFFIX = ':sync-channel:';

/**
 * KEYS[1] is the hash and ARGV[1] is the sync channel. For i >= 2, KEYS[i] is
 * the cache key of field ARGV[i]. The delete message is published in the same
 * step, so peers cannot miss a removed field.
 */
const DELETE_FIELDS_WITHOUT_KEY = `
local removed = {}
for i = 2, #ARGV do
  if redis.call('EXISTS', KEYS[i]) == 0 then
    redis.call('HDEL', KEYS[1], ARGV[i])
    removed[#removed + 1] = ARGV[i]
  end
end
if #removed > 0 then
  redis.call('PUBLISH', ARGV[1], cjson.encode({ type = 'delete', keys = removed }))
end
return removed
`;
const READY_TIMEOUT_MS = 30_000;

type ReadyWaiter = {
  ready: () => void;
  fail: (error: Error) => void;
};

const retryDelayMs = (attempt: number) =>
  Math.min(500 * Math.pow(2, Math.min(attempt, 5)), 10_000);

export class SyncedMap<V> {
  private client: Client;
  private subscriberClient?: Client;
  private map: Map<string, V>;
  private keyPrefix: string;
  private syncChannel: string;
  private redisKey: string;
  private database: number;
  private querySize: number;
  private filterKeys: (key: string) => boolean;
  private resyncIntervalMs?: number;
  private customizedSync?: CustomizedSync;

  private isSetUp = false;
  private lastSetupError: unknown;
  private readyWaiters = new Set<ReadyWaiter>();
  private subscriberSetup?: Promise<void>;
  private sync?: Promise<void>;
  private resubscribeNeeded = true;
  private reloadNeeded = false;
  private resyncTimer?: ReturnType<typeof setInterval>;
  /** Changes made while a reload scans the hash, newest per key. */
  private changesDuringReload?: Map<string, { value: V } | 'deleted'>;
  private closed = false;
  // Retry delays that close() must end.
  private retryWaits = new Set<() => void>();

  constructor(options: SyncedMapOptions) {
    this.client = options.client;
    this.keyPrefix = options.keyPrefix;
    this.redisKey = options.redisKey;
    this.syncChannel = `${options.keyPrefix}${SYNC_CHANNEL_SUFFIX}${options.redisKey}`;
    this.database = options.database;
    this.querySize = options.querySize;
    this.filterKeys = options.filterKeys;
    this.resyncIntervalMs = options.resyncIntervalMs;
    this.customizedSync = options.customizedSync;

    this.map = new Map<string, V>();

    void this.setup();
  }

  private async setup() {
    if (shouldDeferRedisConnection()) {
      this.markReady();
      return;
    }
    if (!this.customizedSync?.withoutRedisHashmap) {
      this.setupPeriodicResync();
    }
    await this.runSync();
  }

  /**
   * The only path that changes the subscriber or reloads the hash, so two
   * scans never overlap. A request that arrives during a run sets a flag and
   * the run repeats. Sync messages published while no subscriber listens are
   * lost, so a new subscriber is always followed by a reload.
   */
  private runSync(): Promise<void> {
    this.sync ??= (async () => {
      while (this.resubscribeNeeded || this.reloadNeeded) {
        if (this.resubscribeNeeded) {
          this.resubscribeNeeded = false;
          await this.reconnectSubscriber();
          this.reloadNeeded = true;
        }
        this.reloadNeeded = false;
        if (!this.customizedSync?.withoutRedisHashmap) {
          await this.retryUntilSuccess('Initial sync', () =>
            this.initialSync(),
          );
        }
      }
      this.markReady();
    })()
      .catch((error) => {
        if (!this.closed) throw error;
      })
      .finally(() => {
        this.sync = undefined;
      });
    return this.sync;
  }

  private markReady() {
    this.lastSetupError = undefined;
    this.isSetUp = true;
    for (const waiter of this.readyWaiters) waiter.ready();
  }

  /**
   * Until the subscriber is back and the map is reloaded, the map can miss
   * entries, so revalidateTag() could skip keys. Report not ready meanwhile.
   */
  private onSubscriberError(error: unknown) {
    console.error('Subscriber client error:', error);
    this.isSetUp = false;
    this.recordSetupError(error);
    if (this.closed) return;
    // A subscriber setup in flight already creates a new client.
    if (!this.subscriberSetup) this.resubscribeNeeded = true;
    void this.runSync();
  }

  /**
   * A pod can start while Redis is down, or lose the socket during the first
   * HSCAN. One failed attempt must not leave the map unready for good.
   */
  private async retryUntilSuccess(
    label: string,
    operation: () => Promise<void>,
  ): Promise<void> {
    for (let attempt = 0; ; attempt++) {
      if (this.closed) throw new Error(`SyncedMap ${this.redisKey} is closed`);
      try {
        return await operation();
      } catch (error) {
        if (this.closed) throw error;
        this.recordSetupError(error);
        const delay = retryDelayMs(attempt);
        console.error(
          `${label} attempt ${attempt + 1} failed, retrying in ${delay}ms`,
          error,
        );
        await new Promise<void>((resolve) => {
          const wake = () => {
            clearTimeout(timer);
            this.retryWaits.delete(wake);
            resolve();
          };
          const timer = setTimeout(wake, delay);
          this.retryWaits.add(wake);
        });
      }
    }
  }

  private async initialSync() {
    // Redis cursors are opaque uint64 strings. redis@4's hScan() coerces them
    // through Number(), which loses precision above 2^53-1 and can loop forever
    // on large keyspaces (see issue #97). Bypass with sendCommand until redis ^5,
    // where SCAN/HSCAN keep cursor as a string.
    let cursor = '0';
    const loaded = new Map<string, V>();
    this.changesDuringReload = new Map();

    try {
      do {
        const [nextCursor, tuples] = (await redisErrorHandler(
          'SyncedMap.initialSync(), operation: HSCAN ' +
            this.keyPrefix +
            this.redisKey,
          this.client.sendCommand([
            'HSCAN',
            this.keyPrefix + this.redisKey,
            cursor,
            'COUNT',
            String(this.querySize),
          ]),
        )) as [string, string[]];
        for (let i = 0; i < tuples.length; i += 2) {
          const field = tuples[i];
          if (this.filterKeys(field)) {
            loaded.set(field, JSON.parse(tuples[i + 1]));
          }
        }
        cursor = nextCursor;
      } while (cursor !== '0');

      // The scan can read a field before a change that this instance saw
      // during the scan, so those changes win over the scanned values.
      for (const [key, change] of this.changesDuringReload) {
        if (change === 'deleted') loaded.delete(key);
        else loaded.set(key, change.value);
      }
      this.map = loaded;
      this.changesDuringReload = undefined;

      // sharedTagsMap keys are cache keys (Redis strings). Tag-manifest maps
      // use tag names as hash fields, which are never Redis keys.
      if (!this.customizedSync?.withoutOrphanCleanup) {
        await this.cleanupKeysNotInRedis();
      }
    } catch (error) {
      this.changesDuringReload = undefined;
      console.error('Error during initial sync:', error);
      throw error;
    }
  }

  /**
   * Removes fields whose cache key is gone from Redis. The script checks and
   * deletes in one step, so a key that a writer stores during the cleanup
   * keeps its field. Only keys of the local snapshot are checked, so the cost
   * follows the map size, not the size of the keyspace.
   *
   * The cleanup is best-effort: the map is already loaded, and a failure only
   * keeps some orphan fields until the next resync. It must not keep the map
   * unready, for example when a busy server blocks the script.
   */
  private async cleanupKeysNotInRedis() {
    const fields = [...this.map.keys()].filter((key) => this.filterKeys(key));
    try {
      for (let i = 0; i < fields.length; i += this.querySize) {
        const batch = fields.slice(i, i + this.querySize);
        // The script publishes the delete. The subscriber applies it in
        // channel order, so a later insert from a peer is not lost.
        await redisErrorHandler(
          'SyncedMap.cleanupKeysNotInRedis(), operation: EVAL ' +
            this.keyPrefix +
            this.redisKey,
          this.client.eval(DELETE_FIELDS_WITHOUT_KEY, {
            keys: [
              this.keyPrefix + this.redisKey,
              ...batch.map((field) => this.keyPrefix + field),
            ],
            arguments: [this.syncChannel, ...batch],
          }),
        );
      }
    } catch (error) {
      console.error('Error during cleanup of keys not in Redis:', error);
    }
  }

  private setupPeriodicResync() {
    if (this.resyncIntervalMs && this.resyncIntervalMs > 0) {
      this.resyncTimer = setInterval(() => {
        this.reloadNeeded = true;
        this.runSync().catch((error) => {
          console.error('Error during periodic resync:', error);
        });
      }, this.resyncIntervalMs);
      // The resync must not keep the process alive.
      this.resyncTimer.unref?.();
    }
  }

  /**
   * Stops the periodic resync, the subscriber and the pending setup retries.
   * The shared main client stays open.
   */
  public async close(): Promise<void> {
    this.closed = true;
    clearInterval(this.resyncTimer);
    this.resyncTimer = undefined;
    for (const wake of this.retryWaits) wake();
    const subscriber = this.subscriberClient;
    this.subscriberClient = undefined;
    await this.disconnectSubscriber(subscriber);
  }

  private async disconnectSubscriber(subscriber: Client | undefined) {
    try {
      subscriber?.removeAllListeners('error');
      await subscriber?.disconnect();
    } catch {
      // expected during outage
    }
  }

  private async setupPubSub(subscriber: Client) {
    const syncHandler = async (message: string) => {
      const syncMessage: SyncMessage<V> = JSON.parse(message);
      if (syncMessage.type === 'insert') {
        if (syncMessage.key !== undefined && syncMessage.value !== undefined) {
          this.setLocal(syncMessage.key, syncMessage.value);
        }
      } else if (syncMessage.type === 'delete') {
        if (syncMessage.keys) {
          for (const key of syncMessage.keys) {
            this.deleteLocal(key);
          }
        }
      }
    };

    const keyEventHandler = async (key: string, message: string) => {
      debug(
        'yellow',
        'SyncedMap.keyEventHandler() called with message',
        this.redisKey,
        message,
        key,
      );
      // const key = message;
      if (key.startsWith(this.keyPrefix)) {
        const keyInMap = key.substring(this.keyPrefix.length);
        if (this.filterKeys(keyInMap)) {
          debugVerbose(
            'SyncedMap.keyEventHandler() key matches filter and will be deleted',
            this.redisKey,
            message,
            key,
          );
          await this.delete(keyInMap, true);
        }
      } else {
        debugVerbose(
          'SyncedMap.keyEventHandler() key does not have prefix',
          this.redisKey,
          message,
          key,
        );
      }
    };

    try {
      // Attach error handler BEFORE subscribing so that errors during
      // subscribe or after connection loss are always caught and trigger
      // a reconnection. Previously this was attached after the subscribe()
      // calls, which meant if subscribe threw during an outage the handler
      // was never attached and the subscriber was permanently dead.
      subscriber.on('error', (err) => this.onSubscriberError(err));

      await subscriber.connect();

      // Check if keyspace event configuration is set correctly
      if (
        (process.env.SKIP_KEYSPACE_CONFIG_CHECK || '').toUpperCase() !== 'TRUE'
      ) {
        const keyspaceEventConfig = (
          await subscriber.configGet('notify-keyspace-events')
        )?.['notify-keyspace-events'];
        if (!keyspaceEventConfig.includes('E')) {
          throw new Error(
            'Keyspace event configuration is set to "' +
              keyspaceEventConfig +
              " but has to include 'E' for Keyevent events, " +
              'published with __keyevent@<db>__ prefix. We recommend to set it to ' +
              "'Exe' like so `redis-cli -h localhost config set notify-keyspace-events Exe`",
          );
        }
        if (
          !keyspaceEventConfig.includes('A') &&
          !(
            keyspaceEventConfig.includes('x') &&
            keyspaceEventConfig.includes('e')
          )
        ) {
          throw new Error(
            'Keyspace event configuration is set to "' +
              keyspaceEventConfig +
              " but has to include 'A' or 'x' and 'e' for expired and " +
              'evicted events. We recommend to set it to ' +
              "'Exe' like so `redis-cli -h localhost config set notify-keyspace-events Exe`",
          );
        }
      }

      await Promise.all([
        // We use a custom channel for insert/delete For the following reason:
        // With custom channel we can delete multiple entries in one message. If we would listen to unlink / del we
        // could get thousands of messages for one revalidateTag (For example revalidateTag("algolia") would send an enormous amount of network packages)
        // Also we can send the value in the message for insert
        subscriber.subscribe(this.syncChannel, syncHandler),
        // Subscribe to Redis keyevent notifications for evicted and expired keys
        subscriber.subscribe(
          `__keyevent@${this.database}__:evicted`,
          keyEventHandler,
        ),
        subscriber.subscribe(
          `__keyevent@${this.database}__:expired`,
          keyEventHandler,
        ),
      ]);
    } catch (error) {
      console.error('Error setting up pub/sub client:', error);
      throw error;
    }
  }

  /**
   * Replace the subscriber client and subscribe again, with exponential
   * backoff until it works. Concurrent callers (setup and every error event
   * during an outage) share one attempt loop.
   */
  private reconnectSubscriber(): Promise<void> {
    this.subscriberSetup ??= this.retryUntilSuccess(
      'Subscriber setup',
      async () => {
        // Forcibly tear down the old subscriber so its internal reconnect
        // loop cannot come back to life once Redis recovers. `quit()` waits
        // for the server and can silently fail during an outage, leaving a
        // zombie client that still fires our error handler. Remove that
        // handler first so a delayed error from the old socket cannot tear
        // down the replacement subscriber we are about to create.
        await this.disconnectSubscriber(this.subscriberClient);
        const subscriber = this.client.duplicate();
        this.subscriberClient = subscriber;
        await this.setupPubSub(subscriber);
        if (this.closed) await this.disconnectSubscriber(subscriber);
      },
    ).finally(() => {
      this.subscriberSetup = undefined;
    });
    return this.subscriberSetup;
  }

  private recordSetupError(error: unknown) {
    this.lastSetupError = error;
    // A failed periodic reload leaves the loaded map usable.
    if (this.isSetUp) return;
    for (const waiter of this.readyWaiters) waiter.fail(this.notReadyError());
  }

  private notReadyError() {
    return new Error(
      `SyncedMap ${this.redisKey} is not ready, setup is retrying after: ${String(this.lastSetupError)}`,
    );
  }

  /**
   * Waits for the initial sync and the subscriber of every map. Rejects as
   * soon as a setup attempt fails, so cache calls miss instead of hanging.
   * A ready map can lose its subscriber during the wait, so the wait watches
   * every map, not only the ones that were loading when it started.
   */
  public static async waitUntilAllReady(
    maps: SyncedMap<unknown>[],
    timeoutMs = READY_TIMEOUT_MS,
  ): Promise<void> {
    const notReady = () => maps.filter((map) => !map.isSetUp);
    const throwIfFailed = () => {
      const failed = notReady().find((map) => map.lastSetupError !== undefined);
      if (failed) throw failed.notReadyError();
    };
    if (notReady().length === 0) return;
    throwIfFailed();

    let readyTimeout: ReturnType<typeof setTimeout> | undefined;
    let waiter: ReadyWaiter | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        waiter = {
          ready: () => {
            if (notReady().length === 0) resolve();
          },
          fail: reject,
        };
        for (const map of maps) map.readyWaiters.add(waiter);
        readyTimeout = setTimeout(() => {
          reject(
            new Error(
              `SyncedMap ${notReady()
                .map((map) => map.redisKey)
                .join(', ')} is not ready after ${timeoutMs}ms`,
            ),
          );
        }, timeoutMs);
      });
    } finally {
      // Timers capture the ambient async context (AsyncLocalStorage), so in a
      // Next.js server a leaked timer pins the request's object graph.
      clearTimeout(readyTimeout);
      for (const map of maps) map.readyWaiters.delete(waiter!);
    }
    throwIfFailed();
  }

  public waitUntilReady(timeoutMs?: number): Promise<void> {
    return SyncedMap.waitUntilAllReady([this], timeoutMs);
  }

  public get(key: string): V | undefined {
    debugVerbose(
      'SyncedMap.get() called with key',
      key,
      JSON.stringify(this.map.get(key))?.substring(0, 100),
    );
    return this.map.get(key);
  }

  public async set(key: string, value: V): Promise<void> {
    debugVerbose(
      'SyncedMap.set() called with key',
      key,
      JSON.stringify(value)?.substring(0, 100),
    );
    this.setLocal(key, value);
    const operations = [];

    // This is needed if we only want to sync delete commands. This is especially useful for non serializable data like a promise map
    if (this.customizedSync?.withoutSetSync) {
      return;
    }
    if (!this.customizedSync?.withoutRedisHashmap) {
      operations.push(
        redisErrorHandler(
          'SyncedMap.set(), operation: hSet ' +
            this.syncChannel +
            ' ' +
            this.keyPrefix +
            ' ' +
            key,
          this.client.hSet(
            this.keyPrefix + this.redisKey,
            key as unknown as string,
            JSON.stringify(value),
          ),
        ),
      );
    }

    const insertMessage: SyncMessage<V> = {
      type: 'insert',
      key: key as unknown as string,
      value,
    };
    operations.push(
      redisErrorHandler(
        'SyncedMap.set(), operation: publish ' +
          this.syncChannel +
          ' ' +
          this.keyPrefix +
          ' ' +
          key,
        this.client.publish(this.syncChannel, JSON.stringify(insertMessage)),
      ),
    );
    await Promise.all(operations);
  }

  public async delete(
    keys: string[] | string,
    withoutSyncMessage = false,
  ): Promise<void> {
    debugVerbose(
      'SyncedMap.delete() called with keys',
      this.redisKey,
      keys,
      withoutSyncMessage,
    );

    const keysArray = Array.isArray(keys) ? keys : [keys];
    const operations = [];

    for (const key of keysArray) {
      this.deleteLocal(key);
    }

    if (!this.customizedSync?.withoutRedisHashmap) {
      operations.push(
        redisErrorHandler(
          'SyncedMap.delete(), operation: hDel ' +
            this.syncChannel +
            ' ' +
            this.keyPrefix +
            ' ' +
            this.redisKey +
            ' ' +
            keysArray,
          this.client.hDel(this.keyPrefix + this.redisKey, keysArray),
        ),
      );
    }

    if (!withoutSyncMessage) {
      operations.push(this.publishDeletion(keysArray));
    }

    await Promise.all(operations);
    debugVerbose(
      'SyncedMap.delete() finished operations',
      this.redisKey,
      keys,
      operations.length,
    );
  }

  private setLocal(key: string, value: V) {
    this.map.set(key, value);
    this.changesDuringReload?.set(key, { value });
  }

  private deleteLocal(key: string) {
    this.map.delete(key);
    this.changesDuringReload?.set(key, 'deleted');
  }

  private async publishDeletion(keys: string[]): Promise<void> {
    const deletionMessage: SyncMessage<V> = { type: 'delete', keys };
    await redisErrorHandler(
      'SyncedMap.delete(), operation: publish ' +
        this.syncChannel +
        ' ' +
        this.keyPrefix +
        ' ' +
        keys,
      this.client.publish(this.syncChannel, JSON.stringify(deletionMessage)),
    );
  }

  public has(key: string): boolean {
    return this.map.has(key);
  }

  public entries(): IterableIterator<[string, V]> {
    return this.map.entries();
  }
}
