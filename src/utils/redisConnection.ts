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
