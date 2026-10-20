// SyncedMap.ts
import { type Client, redisErrorHandler } from './utils/redisConnection';
import { debugVerbose, debug } from './utils/debug';
import { shouldDeferRedisConnection } from './utils/redisConnection';
import {
  createRedisSubscriptions,
  type MessageHandler,
  type RedisSubscriptions,
  syncChannelName,
} from './utils/redisSubscriptions';
import { createRetrier, type Retrier } from './utils/retry';

type CustomizedSync = {
  /**
   * The hash fields are not Redis string keys (tag names). Such a map skips
   * the SCAN-based orphan cleanup, which would HDEL every tag, and the
   * expired/evicted keyevents, which would HDEL a tag whose name equals an
   * expired cache key.
   */
  fieldsAreNotRedisKeys?: boolean;
};

/**
 * Data derived from the map entries, for example a reverse lookup. The map
 * calls it for every local change: set, delete and the entries of a reload.
 */
export interface MapIndex<V> {
  set(key: string, value: V, previous: V | undefined): void;
  delete(key: string, previous: V): void;
}

type SyncedMapOptions<V, I extends MapIndex<V>> = {
  client: Client;
  keyPrefix: string;
  redisKey: string; // Redis Hash key
  database: number;
  querySize: number;
  filterKeys: (key: string) => boolean;
  resyncIntervalMs?: number;
  customizedSync?: CustomizedSync;
  /**
   * The subscriber connection of the handler, shared by its maps. Without
   * it, the map opens its own subscriber connection.
   */
  subscriptions?: RedisSubscriptions;
  /** Creates an empty index. A reload fills a new one next to the new map. */
  createIndex?: () => I;
  /**
   * Checks a value read from the hash or the sync channel. A value that fails
   * is skipped, so one bad field does not keep the map from becoming ready.
   */
  isValidValue?: (value: unknown) => value is V;
};

type Change<V> = { value: V } | 'deleted';

/** The only place that changes a map and its index. */
function applyChange<V>(
  map: Map<string, V>,
  index: MapIndex<V> | undefined,
  key: string,
  change: Change<V>,
) {
  const previous = map.get(key);
  if (change === 'deleted') {
    if (map.delete(key)) index?.delete(key, previous as V);
  } else {
    map.set(key, change.value);
    index?.set(key, change.value, previous);
  }
}

export type SyncMessage<V> = {
  type: 'insert' | 'delete';
  key?: string;
  value?: V;
  keys?: string[];
};

/**
 * KEYS[1] is the hash and ARGV[1] is the sync channel. For i >= 2, KEYS[i] is
 * the cache key of field ARGV[i]. The delete message is published in the same
 * step, so peers cannot miss a removed field. A field that is already gone is
 * not published again, so many instances can run the script for one key.
 */
const DELETE_FIELDS_WITHOUT_KEY = `
local removed = {}
for i = 2, #ARGV do
  if redis.call('EXISTS', KEYS[i]) == 0 and redis.call('HDEL', KEYS[1], ARGV[i]) == 1 then
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

export class SyncedMap<V, I extends MapIndex<V> = MapIndex<V>> {
  private client: Client;
  private subscriptions: RedisSubscriptions;
  private ownsSubscriptions: boolean;
  private removeSubscriptionListener: () => void;
  private map: Map<string, V>;
  private createIndex?: () => I;
  private currentIndex?: I;
  private keyPrefix: string;
  private syncChannel: string;
  private redisKey: string;
  private database: number;
  private querySize: number;
  private filterKeys: (key: string) => boolean;
  private resyncIntervalMs?: number;
  private customizedSync?: CustomizedSync;
  private isValidValue: (value: unknown) => value is V;

  private isSetUp = false;
  private lastSetupError: unknown;
  private readyWaiters = new Set<ReadyWaiter>();
  private sync?: Promise<void>;
  private resubscribeNeeded = true;
  private reloadNeeded = false;
  private resyncTimer?: ReturnType<typeof setInterval>;
  /** Changes made while a reload scans the hash, newest per key. */
  private changesDuringReload?: Map<string, Change<V>>;
  private closed = false;
  private retrier: Retrier;

  constructor(options: SyncedMapOptions<V, I>) {
    this.client = options.client;
    this.keyPrefix = options.keyPrefix;
    this.redisKey = options.redisKey;
    this.syncChannel = syncChannelName(options.keyPrefix, options.redisKey);
    this.database = options.database;
    this.querySize = options.querySize;
    this.filterKeys = options.filterKeys;
    this.resyncIntervalMs = options.resyncIntervalMs;
    this.customizedSync = options.customizedSync;
    this.isValidValue =
      options.isValidValue ?? ((value: unknown): value is V => true);

    this.map = new Map<string, V>();
    this.createIndex = options.createIndex;
    this.currentIndex = this.createIndex?.();
    this.retrier = createRetrier(`SyncedMap ${this.redisKey}`);

    this.ownsSubscriptions = !options.subscriptions;
    this.subscriptions =
      options.subscriptions ?? createRedisSubscriptions(options.client);
    this.removeSubscriptionListener = this.subscriptions.add({
      channels: this.channels(),
      onError: (error, lost) => this.onSubscriberError(error, lost),
    });

    void this.setup();
  }

  private async setup() {
    if (shouldDeferRedisConnection()) {
      this.markReady();
      return;
    }
    this.setupPeriodicResync();
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
          await this.subscriptions.ready();
          this.reloadNeeded = true;
        }
        this.reloadNeeded = false;
        await this.retrier.run(
          'Initial sync',
          () => this.initialSync(),
          (error) => this.recordSetupError(error),
        );
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
  private onSubscriberError(error: unknown, lost: boolean) {
    this.isSetUp = false;
    this.recordSetupError(error);
    if (this.closed) return;
    if (lost) this.resubscribeNeeded = true;
    void this.runSync();
  }

  private async initialSync() {
    // Redis cursors are opaque uint64 strings. redis@4's hScan() coerces them
    // through Number(), which loses precision above 2^53-1 and can loop forever
    // on large keyspaces (see issue #97). Bypass with sendCommand until redis ^5,
    // where SCAN/HSCAN keep cursor as a string.
    let cursor = '0';
    const loaded = new Map<string, V>();
    // Filled per HSCAN batch, so a large map does not block the event loop
    // with one long rebuild at the swap.
    const loadedIndex = this.createIndex?.();
    this.changesDuringReload = new Map();
    // Fields that another writer or package version stored in a form that
    // this version cannot read. They are skipped, not deleted.
    const skipped: string[] = [];

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
            const value = this.parseValue(tuples[i + 1]);
            if (value === undefined) {
              skipped.push(field);
            } else {
              applyChange(loaded, loadedIndex, field, { value });
            }
          }
        }
        cursor = nextCursor;
      } while (cursor !== '0');

      if (skipped.length > 0) {
        console.warn(
          `SyncedMap ${this.redisKey} skipped ${skipped.length} malformed field(s), first:`,
          skipped.slice(0, 5),
        );
      }

      // The scan can read a field before a change that this instance saw
      // during the scan, so those changes win over the scanned values.
      for (const [key, change] of this.changesDuringReload) {
        applyChange(loaded, loadedIndex, key, change);
      }
      this.map = loaded;
      this.currentIndex = loadedIndex;
      this.changesDuringReload = undefined;

      // sharedTagsMap keys are cache keys (Redis strings). Tag-manifest maps
      // use tag names as hash fields, which are never Redis keys.
      if (!this.customizedSync?.fieldsAreNotRedisKeys) {
        await this.cleanupKeysNotInRedis();
      }
    } catch (error) {
      this.changesDuringReload = undefined;
      console.error('Error during initial sync:', error);
      throw error;
    }
  }

  /** Returns undefined for a value that is not JSON or fails the check. */
  private parseValue(raw: string): V | undefined {
    let value: unknown;
    try {
      value = JSON.parse(raw);
    } catch {
      return undefined;
    }
    return this.isValidValue(value) ? value : undefined;
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
        await this.deleteFieldsWithoutKey(
          fields.slice(i, i + this.querySize),
          'SyncedMap.cleanupKeysNotInRedis()',
        );
      }
    } catch (error) {
      console.error('Error during cleanup of keys not in Redis:', error);
    }
  }

  /**
   * Deletes the fields whose cache key is not in Redis. The local map is not
   * changed here: the script publishes the delete, and the subscriber applies
   * it in channel order, so a later insert from a peer is not lost.
   */
  private async deleteFieldsWithoutKey(fields: string[], context: string) {
    await redisErrorHandler(
      `${context}, operation: EVAL ${this.keyPrefix}${this.redisKey}`,
      this.client.eval(DELETE_FIELDS_WITHOUT_KEY, {
        keys: [
          this.keyPrefix + this.redisKey,
          ...fields.map((field) => this.keyPrefix + field),
        ],
        arguments: [this.syncChannel, ...fields],
      }),
    );
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
    this.retrier.stop();
    this.removeSubscriptionListener();
    if (this.ownsSubscriptions) await this.subscriptions.close();
  }

  /**
   * The sync channel carries insert and delete messages. A custom channel can
   * delete many entries in one message and send the value with an insert,
   * while unlink/del keyevents would send one message per key.
   */
  private channels(): [string, MessageHandler][] {
    const syncHandler: MessageHandler = (message) => {
      const syncMessage: SyncMessage<V> = JSON.parse(message);
      if (syncMessage.type === 'insert') {
        if (syncMessage.key === undefined || syncMessage.value === undefined) {
          return;
        }
        if (!this.isValidValue(syncMessage.value)) {
          console.warn(
            `SyncedMap ${this.redisKey} skipped a malformed insert message for`,
            syncMessage.key,
          );
          return;
        }
        this.setLocal(syncMessage.key, syncMessage.value);
      } else if (syncMessage.type === 'delete') {
        if (syncMessage.keys) {
          for (const key of syncMessage.keys) {
            this.deleteLocal(key);
          }
        }
      }
    };

    const keyEventHandler: MessageHandler = (key, message) => {
      debug(
        'yellow',
        'SyncedMap.keyEventHandler() called with message',
        this.redisKey,
        message,
        key,
      );
      if (key.startsWith(this.keyPrefix)) {
        const keyInMap = key.substring(this.keyPrefix.length);
        if (this.filterKeys(keyInMap)) {
          debugVerbose(
            'SyncedMap.keyEventHandler() key matches filter and will be deleted',
            this.redisKey,
            message,
            key,
          );
          // The event can arrive after a writer stored the key again. A
          // plain HDEL then removes the new field, so check and delete in
          // one step.
          this.deleteFieldsWithoutKey(
            [keyInMap],
            'SyncedMap.keyEventHandler()',
          ).catch((error) => {
            console.error('SyncedMap.keyEventHandler() delete failed', error);
          });
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

    // Expired and evicted cache keys. Tag-manifest fields are not Redis keys.
    if (this.customizedSync?.fieldsAreNotRedisKeys) {
      return [[this.syncChannel, syncHandler]];
    }
    return [
      [this.syncChannel, syncHandler],
      [`__keyevent@${this.database}__:evicted`, keyEventHandler],
      [`__keyevent@${this.database}__:expired`, keyEventHandler],
    ];
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
    this.applyLocal(key, { value });
  }

  private deleteLocal(key: string) {
    this.applyLocal(key, 'deleted');
  }

  private applyLocal(key: string, change: Change<V>) {
    applyChange(this.map, this.currentIndex, key, change);
    this.changesDuringReload?.set(key, change);
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

  /** The index of the current entries, if the map has one. */
  public get index(): I | undefined {
    return this.currentIndex;
  }
}
