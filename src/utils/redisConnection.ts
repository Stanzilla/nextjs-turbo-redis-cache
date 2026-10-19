import type { createClient } from 'redis';

export type Client = ReturnType<typeof createClient>;

/**
 * Matches `PHASE_PRODUCTION_BUILD` from `next/constants`.
 *
 * Hardcoded so ESM builds (tsup `shims: false`) never call `require()`.
 * A dynamic `require('next/constants')` throws in ESM, and catching that used
 * to cache `undefined` for the process lifetime — disabling the build-phase
 * Redis skip entirely.
 */
export const PHASE_PRODUCTION_BUILD = 'phase-production-build';

/**
 * Returns true during `next build` so handlers can skip opening Redis
 * connections. Matches the official Next.js cache-handler-redis example.
 */
export function shouldDeferRedisConnection(): boolean {
  return process.env.NEXT_PHASE === PHASE_PRODUCTION_BUILD;
}

/** Logs a failed Redis command with its duration, then rethrows the error. */
export function redisErrorHandler<T extends Promise<unknown>>(
  debugInfo: string,
  redisCommandResult: T,
): T {
  const beforeTimestamp = performance.now();
  return redisCommandResult.catch((error) => {
    console.error(
      'Redis command error',
      (performance.now() - beforeTimestamp).toFixed(2),
      'ms',
      debugInfo,
      error,
    );
    throw error;
  }) as T;
}

/** The client cannot send commands now. This is a cache miss, not a failure. */
export class RedisNotReadyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RedisNotReadyError';
  }
}

/**
 * Waits for setup with the budget of the calling cache operation. A cache
 * call must not wait longer than its own Redis timeout before it misses.
 */
export async function waitForReadiness(
  wait: (timeoutMs: number) => Promise<void>,
  timeoutMs: number,
): Promise<void> {
  try {
    await wait(timeoutMs);
  } catch (error) {
    throw new RedisNotReadyError(
      `Redis cache is not ready: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}
