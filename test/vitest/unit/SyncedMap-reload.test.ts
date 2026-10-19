import { describe, expect, it } from 'vitest';
import { SyncedMap } from '../../../src/SyncedMap';

type Handler = (message: string) => void;

class ReloadClient {
  hash = new Map<string, string>();
  handlers = new Map<string, Handler>();
  /** Runs after the scan read the hash and before it returns. */
  duringScan?: () => void;

  on() {
    return this;
  }
  removeAllListeners() {
    return this;
  }
  async connect() {}
  async disconnect() {}
  async configGet() {
    return { 'notify-keyspace-events': 'Exe' };
  }
  async subscribe(channel: string, handler: Handler) {
    this.handlers.set(channel, handler);
  }
  duplicate() {
    return this;
  }
  async sendCommand([command]: string[]) {
    if (command !== 'HSCAN') throw new Error(`Unexpected ${command}`);
    const tuples = [...this.hash].flat();
    this.duringScan?.();
    this.duringScan = undefined;
    return ['0', tuples];
  }
  async hSet(_key: string, field: string, value: string) {
    this.hash.set(field, value);
    return 1;
  }
  async hDel() {
    return 1;
  }
  async publish() {
    return 1;
  }

  /** Delivers a sync message of another instance. */
  receive(message: object) {
    this.handlers.get('app::sync-channel:__revalidated_tags__')!(
      JSON.stringify(message),
    );
  }
}

async function createMap(client: ReloadClient) {
  const map = new SyncedMap<number>({
    client: client as never,
    keyPrefix: 'app:',
    redisKey: '__revalidated_tags__',
    database: 0,
    querySize: 50,
    filterKeys: () => true,
    customizedSync: { withoutOrphanCleanup: true },
  });
  await map.waitUntilReady();
  return map;
}

describe('SyncedMap reload', () => {
  it('drops fields that Redis no longer has', async () => {
    const client = new ReloadClient();
    client.hash.set('posts', '1');
    const map = await createMap(client);
    expect(map.get('posts')).toBe(1);

    // The delete message was lost while the subscriber was down.
    client.hash.delete('posts');
    await (map as any).initialSync();

    expect(map.has('posts')).toBe(false);
  });

  it('keeps a newer value that arrives while the scan runs', async () => {
    const client = new ReloadClient();
    client.hash.set('posts', '1');
    const map = await createMap(client);

    client.duringScan = () => {
      client.hash.set('posts', '2');
      client.receive({ type: 'insert', key: 'posts', value: 2 });
    };
    await (map as any).initialSync();

    expect(map.get('posts')).toBe(2);
  });

  it('keeps a delete that arrives while the scan runs', async () => {
    const client = new ReloadClient();
    client.hash.set('posts', '1');
    const map = await createMap(client);

    client.duringScan = () => {
      client.hash.delete('posts');
      client.receive({ type: 'delete', keys: ['posts'] });
    };
    await (map as any).initialSync();

    expect(map.has('posts')).toBe(false);
  });
});
