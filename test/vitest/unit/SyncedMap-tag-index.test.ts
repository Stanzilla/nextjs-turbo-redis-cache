import { describe, expect, it } from 'vitest';
import { SyncedMap } from '../../../src/SyncedMap';
import { TagIndex } from '../../../src/utils/tagIndex';
import { syncChannelName } from '../../../src/utils/redisSubscriptions';

type Handler = (message: string, channel: string) => void;

const PREFIX = 'app:';
const HASH = '__sharedTags__';
const SYNC_CHANNEL = syncChannelName(PREFIX, HASH);
const EXPIRED_CHANNEL = '__keyevent@0__:expired';
const KEYS = Array.from({ length: 8 }, (_, i) => `k${i}`);
const TAGS = Array.from({ length: 5 }, (_, i) => `t${i}`);

/** mulberry32: a small seeded PRNG, so a failure can be replayed. */
function random(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

class Client {
  hash = new Map<string, string>();
  /** Cache keys (without prefix) that exist as Redis strings. */
  liveKeys = new Set<string>();
  handlers = new Map<string, Handler>();
  /** Runs once, after the first HSCAN batch. */
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
  /** HSCAN in batches of 3, over the fields at the start of the scan. */
  async sendCommand([command, , cursor]: string[]) {
    if (command !== 'HSCAN') throw new Error(`Unexpected ${command}`);
    if (cursor === '0') this.scanSnapshot = [...this.hash];
    const start = Number(cursor);
    const batch = this.scanSnapshot.slice(start, start + 3);
    const next = start + 3 < this.scanSnapshot.length ? String(start + 3) : '0';
    if (start === 0) {
      this.duringScan?.();
      this.duringScan = undefined;
    }
    return [next, batch.flat()];
  }
  private scanSnapshot: [string, string][] = [];
  async exists(key: string) {
    return this.liveKeys.has(key.slice(PREFIX.length)) ? 1 : 0;
  }
  /**
   * Emulates the take and restore scripts. Their messages are not delivered,
   * as publish() does nothing.
   */
  async eval(
    script: string,
    { arguments: [, ...args] }: { arguments: string[] },
  ) {
    if (script.includes('HSETNX')) {
      for (let i = 0; i < args.length; i += 2) {
        if (!this.hash.has(args[i])) this.hash.set(args[i], args[i + 1]);
      }
      return null;
    }
    const taken: string[] = [];
    for (const field of args) {
      const value = this.hash.get(field);
      if (value === undefined) continue;
      this.hash.delete(field);
      taken.push(field, value);
    }
    return taken;
  }
  async hSet(_key: string, field: string, value: string) {
    this.hash.set(field, value);
    return 1;
  }
  async hDel(_key: string, fields: string[]) {
    for (const field of fields) this.hash.delete(field);
    return 1;
  }
  async publish() {
    return 1;
  }

  deliver(channel: string, message: string) {
    this.handlers.get(channel)!(message, channel);
  }
}

/** One random change, made by this instance or by another one. */
async function step(
  rand: () => number,
  client: Client,
  map: SyncedMap<string[], TagIndex>,
) {
  const pick = <T>(items: T[]) => items[Math.floor(rand() * items.length)];
  const tags = () =>
    Array.from({ length: Math.floor(rand() * 4) }, () => pick(TAGS));
  const delivered = rand() > 0.2;
  const key = pick(KEYS);

  switch (pick(['set', 'delete', 'remoteSet', 'remoteDelete', 'expire'])) {
    case 'set':
      client.liveKeys.add(key);
      await map.set(key, tags());
      break;
    case 'delete':
      await map.delete([key, pick(KEYS)]);
      break;
    case 'remoteSet': {
      const value = tags();
      client.liveKeys.add(key);
      client.hash.set(key, JSON.stringify(value));
      if (delivered) {
        client.deliver(
          SYNC_CHANNEL,
          JSON.stringify({ type: 'insert', key, value }),
        );
      }
      break;
    }
    case 'remoteDelete':
      client.hash.delete(key);
      if (delivered) {
        client.deliver(
          SYNC_CHANNEL,
          JSON.stringify({ type: 'delete', keys: [key] }),
        );
      }
      break;
    case 'expire':
      // Without the event, the orphan cleanup of the next reload removes it.
      client.liveKeys.delete(key);
      if (delivered) client.deliver(EXPIRED_CHANNEL, PREFIX + key);
      break;
  }
}

function scan(map: SyncedMap<string[], TagIndex>, tags: Set<string>) {
  const keys = new Set<string>();
  for (const [key, keyTags] of map.entries()) {
    if (keyTags.some((tag) => tags.has(tag))) keys.add(key);
  }
  return keys;
}

describe('SyncedMap tag index', () => {
  it.each([1, 2, 3, 4, 5, 6, 7, 8])(
    'finds the same keys as a full scan after random changes and reloads (seed %i)',
    async (seed) => {
      const rand = random(seed);
      const client = new Client();
      const map = new SyncedMap<string[], TagIndex>({
        client: client as never,
        keyPrefix: PREFIX,
        redisKey: HASH,
        database: 0,
        querySize: 50,
        filterKeys: () => true,
        createIndex: () => new TagIndex(),
      });
      await map.waitUntilReady();

      for (let i = 0; i < 300; i++) {
        if (rand() < 0.1) {
          // Changes that arrive while the reload scans the hash.
          client.duringScan = () => {
            for (let j = 0; j < 3; j++) void step(rand, client, map);
          };
          await (map as any).initialSync();
        } else {
          await step(rand, client, map);
        }

        const queries = [
          ...TAGS.map((tag) => new Set([tag])),
          new Set(TAGS.filter(() => rand() < 0.5)),
        ];
        for (const tags of queries) {
          expect(map.index!.keysWithAnyTag(tags), `step ${i}`).toEqual(
            scan(map, tags),
          );
        }
      }
    },
  );
});
