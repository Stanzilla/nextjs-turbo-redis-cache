import { describe, it, expect } from 'vitest';
import { SyncedMap } from '../../../src/SyncedMap';

class HashMockClient {
  hash = new Map<string, string>();
  stringKeys: string[] = [];
  hDelCalls: string[][] = [];
  evalBatches: string[][] = [];
  publishes: string[] = [];
  /** Runs when the cleanup checks if the cache keys exist. */
  beforeKeyCheck?: () => void;
  evalError?: Error;
  isOpen = true;
  isReady = true;

  on() {
    return this;
  }

  async connect() {
    this.isOpen = true;
    this.isReady = true;
  }

  async configGet() {
    return { 'notify-keyspace-events': 'Exe' };
  }

  async subscribe() {}

  async quit() {}

  async disconnect() {
    this.isOpen = false;
    this.isReady = false;
  }

  duplicate() {
    return new HashMockClient();
  }

  async sendCommand(args: string[]) {
    const [command] = args;
    if (command === 'HSCAN') {
      const tuples: string[] = [];
      for (const [field, value] of this.hash) {
        tuples.push(field, value);
      }
      return ['0', tuples];
    }
    throw new Error(`Unexpected sendCommand: ${command}`);
  }

  async hSet(_key: string, field: string, value: string) {
    this.hash.set(field, value);
    return 1;
  }

  async hDel(_key: string, fields: string | string[]) {
    const list = Array.isArray(fields) ? fields : [fields];
    this.hDelCalls.push(list);
    for (const field of list) {
      this.hash.delete(field);
    }
    return list.length;
  }

  async publish(_channel: string, message: string) {
    this.publishes.push(message);
    return 1;
  }

  /** Emulates the cleanup script: the check, HDEL and PUBLISH are one step. */
  async eval(
    _script: string,
    {
      keys,
      arguments: [channel, ...fields],
    }: { keys: string[]; arguments: string[] },
  ) {
    if (this.evalError) throw this.evalError;
    this.evalBatches.push(fields);
    this.beforeKeyCheck?.();
    const removed = fields.filter((field, i) => {
      if (this.stringKeys.includes(keys[i + 1])) return false;
      this.hash.delete(field);
      return true;
    });
    if (removed.length > 0) {
      await this.publish(
        channel,
        JSON.stringify({ type: 'delete', keys: removed }),
      );
    }
    return removed;
  }
}

function createMap(client: HashMockClient, querySize = 50) {
  return new SyncedMap<number>({
    client: client as never,
    keyPrefix: 'app:',
    redisKey: '__sharedTags__',
    database: 0,
    querySize,
    filterKeys: () => true,
  });
}

describe('SyncedMap orphan cleanup', () => {
  it('deletes hash fields whose cache key is not in Redis', async () => {
    const client = new HashMockClient();
    const map = createMap(client);
    await map.waitUntilReady();
    await map.set('posts', 1);
    await map.set('live', 2);

    client.stringKeys = ['app:live'];
    await (map as any).initialSync();

    expect([...client.hash.keys()]).toEqual(['live']);
    expect(map.get('posts')).toBeUndefined();
    expect(map.get('live')).toBe(2);
    expect(client.publishes.at(-1)).toBe(
      JSON.stringify({ type: 'delete', keys: ['posts'] }),
    );
  });

  it('keeps the field of a key that a writer stores during the cleanup', async () => {
    const client = new HashMockClient();
    const map = createMap(client);
    await map.waitUntilReady();
    await map.set('old', 1);

    client.beforeKeyCheck = () => {
      client.stringKeys.push('app:old');
    };
    await (map as any).initialSync();

    expect(client.hash.has('old')).toBe(true);
    expect(map.get('old')).toBe(1);
  });

  it('checks the fields in batches of querySize', async () => {
    const client = new HashMockClient();
    const map = createMap(client, 2);
    await map.waitUntilReady();
    for (const key of ['a', 'b', 'c', 'd', 'e']) await map.set(key, 1);

    await (map as any).initialSync();

    expect(client.evalBatches).toEqual([['a', 'b'], ['c', 'd'], ['e']]);
    expect(client.hash.size).toBe(0);
  });

  it('stays ready when the cleanup script fails', async () => {
    const client = new HashMockClient();
    const map = createMap(client);
    await map.waitUntilReady();
    await map.set('posts', 1);

    client.evalError = new Error('BUSY Redis is busy running a script');
    await expect((map as any).initialSync()).resolves.toBeUndefined();

    expect(map.get('posts')).toBe(1);
  });

  it('keeps tag-manifest fields when withoutOrphanCleanup is set', async () => {
    const client = new HashMockClient();
    const map = new SyncedMap<number>({
      client: client as never,
      keyPrefix: 'app:',
      redisKey: '__cacheComponents_revalidated_tags__',
      database: 0,
      querySize: 50,
      filterKeys: () => true,
      customizedSync: { withoutOrphanCleanup: true },
    });
    await map.waitUntilReady();
    await map.set('posts', 1);

    client.stringKeys = ['app:some-cache-entry'];
    await (map as any).initialSync();

    expect(client.hDelCalls).toEqual([]);
    expect(map.get('posts')).toBe(1);
  });
});
