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
};

/**
 * Creates the main client of a cache handler with one reconnect rule and one
 * error budget. The initial connect only logs a failure: the 'error' listener
 * owns reconnects, and nothing awaits the constructor's connect promise.
 */
export function createCacheClient({
  label,
  redisUrl,
  database,
  socketOptions,
  clientOptions,
  errorBudget,
  deferConnection,
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

  client.on('ready', () => errorBudget.reset());
  client.on('error', (error) => {
    console.error(`${label} error`, error);
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
    errorBudget.record(error, label);
  });

  if (!deferConnection) {
    client
      .connect()
      .then(() => debug('green', `${label} connected.`))
      .catch((error) => {
        console.error(`Failed to connect ${label}:`, error);
      });
  }

  return client as Client;
}
