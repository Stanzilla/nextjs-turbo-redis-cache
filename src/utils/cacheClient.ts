import { createClient, type RedisClientOptions } from 'redis';
import { debug } from './debug';
import type { ErrorBudget } from './errorBudget';
import type { Client } from './redisConnection';

export type CacheClientOptions = {
  /** Log label, for example 'Redis client'. */
  label: string;
  redisUrl: string;
  database: number;
  socketOptions?: RedisClientOptions['socket'];
  clientOptions?: Omit<RedisClientOptions, 'url' | 'database' | 'socket'>;
  errorBudget: ErrorBudget;
  /** Do not connect now, for example during `next build`. */
  deferConnection: boolean;
  /**
   * Called once when a user `reconnectStrategy` stopped reconnecting. The
   * client stays closed, so setup retries on it cannot succeed.
   */
  onTerminated?: (error: unknown) => void;
};

/**
 * Creates the main client of a cache handler with one reconnect rule and one
 * error budget. The initial connect only logs a failure: the 'error' listener
 * owns reconnects, and nothing awaits the constructor's connect promise.
 *
 * When `socketOptions.reconnectStrategy` is set, the listener does not
 * reconnect. A closed client then means that the user strategy gave up on
 * purpose, and `onTerminated` is called. node-redis 4 to 6 close the client
 * before they emit the error that ends the reconnects.
 */
export function createCacheClient({
  label,
  redisUrl,
  database,
  socketOptions,
  clientOptions,
  errorBudget,
  deferConnection,
  onTerminated,
}: CacheClientOptions): Client {
  const client = createClient({
    url: redisUrl,
    // Restarts the connection if it is idle for too long. Useful with Redis
    // deployments that do not use TCP keep-alive.
    pingInterval: 10_000,
    ...(database !== 0 ? { database } : {}),
    ...(socketOptions ? { socket: { ...socketOptions } } : {}),
    ...(clientOptions || {}),
  });

  const hasUserReconnectStrategy =
    socketOptions?.reconnectStrategy !== undefined;

  let terminated = false;
  function terminateIfClosed(error: unknown) {
    if (!hasUserReconnectStrategy || client.isOpen || terminated) return;
    terminated = true;
    console.error(
      `${label} is closed for good: the reconnectStrategy stopped reconnecting. Cache calls miss until the process restarts.`,
      error,
    );
    onTerminated?.(error);
  }

  client.on('ready', () => errorBudget.reset());
  client.on('error', (error) => {
    console.error(`${label} error`, error);
    if (!hasUserReconnectStrategy) scheduleReconnect();
    errorBudget.record(error, label);
    terminateIfClosed(error);
  });

  function scheduleReconnect() {
    setTimeout(() => {
      // An open socket that is not ready is already reconnecting. A second
      // connect() throws "Socket already opened".
      if (client.isOpen) return;
      client.connect().catch((err) => {
        console.error(
          `Failed to reconnect ${label} after connection loss:`,
          err,
        );
      });
    }, 1000);
  }

  if (!deferConnection) {
    client
      .connect()
      .then(() => debug('green', `${label} connected.`))
      .catch((error) => {
        console.error(`Failed to connect ${label}:`, error);
        terminateIfClosed(error);
      });
  }

  return client as Client;
}
