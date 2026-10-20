import { describe, it, expect, afterEach } from 'vitest';
import { SyncedMap } from '../../../src/SyncedMap';

type Listener = (message: string, channel: string) => void;

class MockClient {
  hash = new Map<string, string>();
  listeners = new Map<string, Listener>();
  isOpen = true;
  isReady = true;

  on() {
    return this;
  }
  removeAllListeners() {
    return this;
  }
  async connect() {}
  async configGet() {
    return { 'notify-keyspace-events': 'Exe' };
  }
  async subscribe(channel: string, listener: Listener) {
    this.listeners.set(channel, listener);
  }
  async quit() {}
  async disconnect() {}
  duplicate() {
    return this;
  }
  async publish() {
    return 1;
  }
  evalCalls: { keys: string[]; arguments: string[] }[] = [];
  async eval(
    _script: string,
    options: { keys: string[]; arguments: string[] },
  ) {
    this.evalCalls.push(options);
    return [];
  }
  async sendCommand([command]: string[]): Promise<[string, string[]]> {
    if (command !== 'HSCAN') throw new Error(`unexpected ${command}`);
    return ['0', [...this.hash].flat()];
  }
}

describe('SyncedMap with expired values', () => {
  const maps: SyncedMap<number>[] = [];

  afterEach(async () => {
    await Promise.all(maps.splice(0).map((map) => map.close()));
  });

  it('a reload asks Redis to remove only the expired fields, with the value that it read', async () => {
    const client = new MockClient();
    client.hash.set('old-tag', '100');
    client.hash.set('new-tag', '5000');

    const map = new SyncedMap<number>({
      client: client as never,
      keyPrefix: 'test:',
      redisKey: '__revalidated_tags__',
      database: 0,
      querySize: 100,
      filterKeys: () => true,
      customizedSync: { fieldsAreNotRedisKeys: true },
      isExpiredValue: (revalidatedAt) => revalidatedAt < 1000,
    });
    maps.push(map);
    await SyncedMap.waitUntilAllReady([map], 1_000);

    expect(client.evalCalls).toHaveLength(1);
    expect(client.evalCalls[0].keys).toEqual(['test:__revalidated_tags__']);
    expect(client.evalCalls[0].arguments.slice(1)).toEqual(['old-tag', '100']);
    // The local map changes only when the delete message arrives.
    expect(map.get('old-tag')).toBe(100);
  });
});
