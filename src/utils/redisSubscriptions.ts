import type { Client } from './redisConnection';
import { closeClient } from './errorBudget';
import { createRetrier } from './retry';

export type MessageHandler = (message: string, channel: string) => void;

export type SubscriptionListener = {
  channels: [channel: string, handler: MessageHandler][];
  /**
   * Called for each subscriber error and each failed setup attempt. `lost`
   * is true when this listener was subscribed, so it can have missed
   * messages. Call ready() to subscribe again.
   */
  onError(error: unknown, lost: boolean): void;
};

export type RedisSubscriptions = {
  /** Registers the listener. Returns a function that removes it. */
  add(listener: SubscriptionListener): () => void;
  /**
   * Resolves when one live subscriber client listens on the channels of
   * every listener. Concurrent callers share one setup with backoff.
   */
  ready(): Promise<void>;
  close(): Promise<void>;
  /** The current subscriber client, for diagnostics. */
  readonly client: Client | undefined;
};

type Connection = {
  client: Client;
  subscribed: Set<SubscriptionListener>;
  lost: boolean;
  keyspaceChecked: boolean;
};

const KEYEVENT_CHANNEL_PREFIX = '__keyevent@';

/** The Pub/Sub channel for the changes to the data named `redisKey`. */
export function syncChannelName(keyPrefix: string, redisKey: string): string {
  return `${keyPrefix}:sync-channel:${redisKey}`;
}

async function disconnect(client: Client | undefined) {
  // A delayed error from the old socket must not reach the listeners, and
  // must not be an unhandled 'error' event while closeClient waits.
  if (!client) return;
  client.removeAllListeners('error');
  client.on('error', () => {});
  await closeClient(client);
}

async function assertKeyspaceEventsEnabled(client: Client) {
  if ((process.env.SKIP_KEYSPACE_CONFIG_CHECK || '').toUpperCase() === 'TRUE') {
    return;
  }
  const keyspaceEventConfig = (
    await client.configGet('notify-keyspace-events')
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
    !(keyspaceEventConfig.includes('x') && keyspaceEventConfig.includes('e'))
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

/**
 * One subscriber connection (a duplicate of `client`) for all listeners of a
 * handler. A subscriber connection cannot send normal commands, so each
 * handler needs one, but not one per map.
 */
export function createRedisSubscriptions(client: Client): RedisSubscriptions {
  const listeners = new Set<SubscriptionListener>();
  const retrier = createRetrier('Redis subscriptions');
  let current: Connection | undefined;
  let setup: Promise<void> | undefined;
  let closed = false;

  const healthy = () =>
    current !== undefined &&
    !current.lost &&
    [...listeners].every((listener) => current!.subscribed.has(listener));

  const fail = (
    connection: Connection | undefined,
    error: unknown,
    replace: boolean,
  ) => {
    if (connection && replace) connection.lost = true;
    for (const listener of [...listeners]) {
      listener.onError(error, connection?.subscribed.has(listener) ?? false);
    }
  };

  const onClientError = (connection: Connection, error: unknown) => {
    if (connection !== current) return;
    console.error('Subscriber client error:', error);
    // While no listener is subscribed, the pending setup attempt owns this
    // client: node-redis reconnects it, and connect() or subscribe() fails
    // if it cannot.
    fail(connection, error, connection.subscribed.size > 0);
  };

  const replaceClient = async () => {
    // Forcibly tear down the old subscriber so its internal reconnect loop
    // cannot come back to life once Redis recovers. quit() waits for the
    // server and can silently fail during an outage. The new client does
    // not wait for this: the old client can still have a connect attempt.
    const old = current;
    current = undefined;
    void disconnect(old?.client);
    const connection: Connection = {
      client: client.duplicate(),
      subscribed: new Set(),
      lost: false,
      keyspaceChecked: false,
    };
    current = connection;
    // Attach the error handler before connect, so an error during setup is
    // never unhandled.
    connection.client.on('error', (error) => onClientError(connection, error));
    await connection.client.connect();
  };

  const subscribe = async (
    connection: Connection,
    listener: SubscriptionListener,
  ) => {
    const usesKeyEvents = listener.channels.some(([channel]) =>
      channel.startsWith(KEYEVENT_CHANNEL_PREFIX),
    );
    if (usesKeyEvents && !connection.keyspaceChecked) {
      await assertKeyspaceEventsEnabled(connection.client);
      connection.keyspaceChecked = true;
    }
    await Promise.all(
      listener.channels.map(([channel, handler]) =>
        connection.client.subscribe(channel, (message, from) => {
          if (!listeners.has(listener)) return;
          try {
            handler(message, from);
          } catch (error) {
            console.error('Subscriber message handler failed', from, error);
          }
        }),
      ),
    );
    connection.subscribed.add(listener);
  };

  const attempt = async () => {
    if (!current || current.lost) await replaceClient();
    const connection = current!;
    for (const listener of [...listeners]) {
      if (!connection.subscribed.has(listener)) {
        await subscribe(connection, listener);
      }
    }
  };

  const ensureSetup = () =>
    (setup ??= retrier
      .run('Subscriber setup', attempt, (error) => fail(current, error, true))
      .finally(() => {
        setup = undefined;
        if (closed) {
          const connection = current;
          current = undefined;
          void disconnect(connection?.client);
        }
      }));

  return {
    add(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    async ready() {
      while (!closed && !healthy()) await ensureSetup();
      if (closed) throw new Error('Redis subscriptions are closed');
    },
    async close() {
      closed = true;
      retrier.stop();
      listeners.clear();
      const connection = current;
      current = undefined;
      await disconnect(connection?.client);
    },
    get client() {
      return current?.client;
    },
  };
}
