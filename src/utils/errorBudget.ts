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

export async function closeClient(client: Client | undefined): Promise<void> {
  // node-redis can finish a pending connect() after disconnect(), and the
  // client is then open again.
  client?.once('ready', () => void closeClient(client));
  try {
    await client?.disconnect();
  } catch {
    // The client was not created or is already closed.
  }
}

export function disconnectAndExit(client: Client | undefined): void {
  void closeClient(client);
  setTimeout(() => {
    process.exit(1);
  }, 500);
}
