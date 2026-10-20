export const retryDelayMs = (attempt: number) =>
  Math.min(500 * Math.pow(2, Math.min(attempt, 5)), 10_000);

export type Retrier = {
  /** Runs the operation with exponential backoff until it works. */
  run(
    label: string,
    operation: () => Promise<void>,
    onFailure: (error: unknown) => void,
  ): Promise<void>;
  /** Ends the waiting delays. Later runs throw. */
  stop(): void;
};

/**
 * A pod can start while Redis is down, or lose the socket during setup. One
 * failed attempt must not stop the setup for good.
 */
export function createRetrier(name: string): Retrier {
  let stopped = false;
  const waits = new Set<() => void>();

  return {
    async run(label, operation, onFailure) {
      for (let attempt = 0; ; attempt++) {
        if (stopped) throw new Error(`${name} is closed`);
        try {
          return await operation();
        } catch (error) {
          if (stopped) throw error;
          onFailure(error);
          const delay = retryDelayMs(attempt);
          console.error(
            `${label} attempt ${attempt + 1} failed, retrying in ${delay}ms`,
            error,
          );
          await new Promise<void>((resolve) => {
            const wake = () => {
              clearTimeout(timer);
              waits.delete(wake);
              resolve();
            };
            const timer = setTimeout(wake, delay);
            waits.add(wake);
          });
        }
      }
    },
    stop() {
      stopped = true;
      for (const wake of waits) wake();
    },
  };
}
