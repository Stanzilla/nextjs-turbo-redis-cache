import { type Client, RedisNotReadyError } from './redisConnection';

/**
 * Counts Redis failures of one handler and calls `onTrip` once when the count
 * reaches the threshold. A threshold of 0 or less turns the budget off.
 */
export class ErrorBudget {
  private count = 0;
  private tripped = false;
  // Callers that share one deduplicated request get the same error object.
  private readonly recorded = new WeakSet<object>();

  constructor(
    private readonly threshold: number,
    private readonly onTrip: () => void,
  ) {}

  record(error: unknown, context: string): void {
    if (this.threshold <= 0 || this.tripped) return;
    if (error instanceof RedisNotReadyError) return;
    if (typeof error === 'object' && error !== null) {
      if (this.recorded.has(error)) return;
      this.recorded.add(error);
    }
    this.count++;
    if (this.count < this.threshold) return;

    this.tripped = true;
    console.error(
      `${context} error threshold of ${this.threshold} reached, disconnecting and exiting (please implement a restart process/container watchdog to handle this error)`,
      error,
    );
    this.onTrip();
  }

  /** Call when the client is connected again. */
  reset(): void {
    this.count = 0;
  }
}

// The longest connect attempt with the node-redis default connectTimeout.
const CONNECT_ATTEMPT_MAX_MS = 5_000;

/**
 * node-redis keeps the socket of a connect attempt that ends after
 * disconnect(), and redis 4 cannot close that socket later. So wait until a
 * pending attempt ends before the disconnect.
 */
function connectAttemptEnded(client: Client): Promise<void> {
  if (!client.isOpen || client.isReady) return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      client.off('ready', done);
      client.off('error', done);
      resolve();
    };
    const timer = setTimeout(done, CONNECT_ATTEMPT_MAX_MS);
    timer.unref?.();
    client.once('ready', done);
    client.once('error', done);
  });
}

export async function closeClient(client: Client | undefined): Promise<void> {
  if (!client) return;
  await connectAttemptEnded(client);
  try {
    await client.disconnect();
  } catch {
    // The client is already closed.
  }
}

export function disconnectAndExit(client: Client | undefined): void {
  // The process exits soon, so a pending connect attempt cannot leak.
  client?.disconnect().catch(() => {});
  setTimeout(() => {
    process.exit(1);
  }, 500);
}
