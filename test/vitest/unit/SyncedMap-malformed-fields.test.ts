import { describe, it, expect, vi, afterEach } from 'vitest';
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
  async eval() {
    return [];
  }
  async sendCommand([command]: string[]): Promise<[string, string[]]> {
    if (command !== 'HSCAN') throw new Error(`unexpected ${command}`);
    return ['0', [...this.hash].flat()];
  }
}

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((tag) => typeof tag === 'string');

function createMap(client: MockClient) {
  return new SyncedMap<string[]>({
    client: client as never,
    keyPrefix: 'test:',
    redisKey: '__sharedTags__',
    database: 0,
    querySize: 100,
    filterKeys: () => true,
    customizedSync: { fieldsAreNotRedisKeys: true },
    isValidValue: isStringArray,
  });
}

describe('SyncedMap with malformed values', () => {
  const maps: SyncedMap<string[]>[] = [];

  afterEach(async () => {
    await Promise.all(maps.splice(0).map((map) => map.close()));
    vi.restoreAllMocks();
  });

  it('becomes ready and loads the other fields when one field is not JSON', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const client = new MockClient();
    client.hash.set('/good', JSON.stringify(['tag-a']));
    client.hash.set('/bad', '{not json');

    const map = createMap(client);
    maps.push(map);

    await SyncedMap.waitUntilAllReady([map], 1_000);
    expect(map.get('/good')).toEqual(['tag-a']);
    expect(map.has('/bad')).toBe(false);
    expect(warn).toHaveBeenCalledOnce();
  });

  it('skips a field whose value has the wrong shape', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const client = new MockClient();
    client.hash.set('/good', JSON.stringify(['tag-a']));
    client.hash.set('/bad', JSON.stringify(42));

    const map = createMap(client);
    maps.push(map);

    await SyncedMap.waitUntilAllReady([map], 1_000);
    expect(map.get('/good')).toEqual(['tag-a']);
    expect(map.has('/bad')).toBe(false);
  });

  it('skips an insert message whose value has the wrong shape', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const client = new MockClient();
    const map = createMap(client);
    maps.push(map);
    await SyncedMap.waitUntilAllReady([map], 1_000);
    const syncListener = [...client.listeners.values()][0];

    syncListener(
      JSON.stringify({ type: 'insert', key: '/bad', value: 42 }),
      'channel',
    );
    syncListener(
      JSON.stringify({ type: 'insert', key: '/good', value: ['tag-a'] }),
      'channel',
    );

    expect(map.has('/bad')).toBe(false);
    expect(map.get('/good')).toEqual(['tag-a']);
  });
});
