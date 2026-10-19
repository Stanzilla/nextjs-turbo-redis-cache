import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SyncedMap } from '../../../src/SyncedMap';

type Scan = { resolve: () => void; reject: (error: Error) => void };

class ScanClient {
  scans: Scan[] = [];
  activeScans = 0;
  maxActiveScans = 0;

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
  async subscribe() {}
  duplicate() {
    return this;
  }

  sendCommand([command]: string[]) {
    if (command !== 'HSCAN') throw new Error(`Unexpected ${command}`);
    this.activeScans++;
    this.maxActiveScans = Math.max(this.maxActiveScans, this.activeScans);
    return new Promise<[string, string[]]>((resolve, reject) => {
      const done = () => this.activeScans--;
      this.scans.push({
        resolve: () => {
          done();
          resolve(['0', []]);
        },
        reject: (error) => {
          done();
          reject(error);
        },
      });
    });
  }

  finishScan(index: number) {
    this.scans[index].resolve();
  }
}

const maps: SyncedMap<unknown>[] = [];

function createMap(client: ScanClient) {
  const map = new SyncedMap<unknown>({
    client: client as any,
    keyPrefix: 'lifecycle:',
    redisKey: '__sharedTags__',
    database: 0,
    querySize: 250,
    filterKeys: () => true,
    resyncIntervalMs: 1_000,
    customizedSync: { withoutOrphanCleanup: true },
  });
  maps.push(map);
  return map;
}

describe('SyncedMap lifecycle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(async () => {
    await Promise.all(maps.splice(0).map((map) => map.close()));
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('runs a periodic resync after the running scan, not next to it', async () => {
    const client = new ScanClient();
    const map = createMap(client);
    await vi.advanceTimersByTimeAsync(0);
    expect(client.scans).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(client.scans).toHaveLength(1);

    client.finishScan(0);
    await vi.advanceTimersByTimeAsync(0);
    expect(client.scans).toHaveLength(2);
    expect(client.maxActiveScans).toBe(1);

    client.finishScan(1);
    await expect(map.waitUntilReady(100)).resolves.toBeUndefined();
  });

  it('does not reject a ready wait when a loaded map fails a periodic resync', async () => {
    const readyClient = new ScanClient();
    const loadingClient = new ScanClient();
    const readyMap = createMap(readyClient);
    const loadingMap = createMap(loadingClient);
    await vi.advanceTimersByTimeAsync(0);
    readyClient.finishScan(0);
    await vi.advanceTimersByTimeAsync(0);

    const wait = SyncedMap.waitUntilAllReady([readyMap, loadingMap], 5_000);
    let settled = false;
    wait.then(
      () => (settled = true),
      () => (settled = true),
    );

    // The periodic tick also loads the loading map again later.
    await vi.advanceTimersByTimeAsync(1_000);
    readyClient.scans[1].reject(new Error('socket closed'));
    await vi.advanceTimersByTimeAsync(0);
    expect(settled).toBe(false);
    await expect(readyMap.waitUntilReady(100)).resolves.toBeUndefined();

    loadingClient.finishScan(0);
    await vi.advanceTimersByTimeAsync(0);
    loadingClient.finishScan(1);
    await vi.advanceTimersByTimeAsync(0);
    await expect(wait).resolves.toBeUndefined();
  });

  it('stops the periodic resync on close()', async () => {
    const client = new ScanClient();
    const map = createMap(client);
    await vi.advanceTimersByTimeAsync(0);
    client.finishScan(0);
    await map.close();

    await vi.advanceTimersByTimeAsync(5_000);
    expect(client.scans).toHaveLength(1);
  });

  it('leaves no retry timer behind after close()', async () => {
    const client = new ScanClient();
    const map = createMap(client);
    await vi.advanceTimersByTimeAsync(0);
    client.scans[0].reject(new Error('socket closed'));
    await vi.advanceTimersByTimeAsync(0);

    await map.close();
    await vi.advanceTimersByTimeAsync(0);

    expect(vi.getTimerCount()).toBe(0);
  });
});
