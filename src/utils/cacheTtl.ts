export type ResolveCacheEntryTtlContext = {
  revalidate?: number | false;
  cacheControl?: { revalidate?: number | false; expire?: number | undefined };
};

export type ResolveCacheEntryTtlData = {
  kind?: string;
  revalidate?: number | false;
} | null;

export type ResolveCacheEntryTtlOptions = {
  estimateExpireAge: (staleAge: number) => number;
  defaultStaleAge: number;
};

/**
 * Resolves the Redis TTL (seconds) for an ISR / incremental-cache entry.
 *
 * Official Next.js semantics (cache-handler-redis example + self-hosting docs):
 * - Key TTL on `cacheControl.expire`, never on `revalidate` alone.
 * - Past `revalidate` an entry is only stale (SWR); evicting at that boundary
 *   would defeat background refresh.
 * - When no finite `expire` is provided, fall back to `estimateExpireAge(revalidate)`
 *   for legacy Pages Router / pre-cacheLife callers that only pass `revalidate`.
 * - When neither `expire` nor `revalidate` is available, return `undefined` (no TTL;
 *   rely on tag-based invalidation). This is intentional — not a missing
 *   `defaultStaleAge` fallback. `revalidate: false` is a separate branch above
 *   and is covered by unit + Pages Router integration tests (`/static-forever`).
 */
export function resolveCacheEntryTtlSeconds(
  ctx: ResolveCacheEntryTtlContext,
  data: ResolveCacheEntryTtlData,
  options: ResolveCacheEntryTtlOptions,
): number | undefined {
  const expire = ctx.cacheControl?.expire;

  if (Number.isFinite(expire) && expire! > 0) {
    return Math.max(1, Math.ceil(expire!));
  }

  const revalidate = resolveRevalidateValue(ctx, data);

  if (revalidate === false) {
    return toWholeSeconds(options.estimateExpireAge(options.defaultStaleAge));
  }

  if (revalidate && Number.isSafeInteger(revalidate) && revalidate > 0) {
    return toWholeSeconds(options.estimateExpireAge(revalidate));
  }

  return undefined;
}

// Redis `SET ... EX` rejects non-integer values, and the default
// estimateExpireAge (revalidate * 1.2) is often fractional. The minimum TTL is
// 1 second, so a result of 0 or less becomes 1. A result of NaN or Infinity
// sets no TTL.
function toWholeSeconds(seconds: number): number | undefined {
  if (!Number.isFinite(seconds)) return undefined;
  return Math.max(1, Math.ceil(seconds));
}

function resolveRevalidateValue(
  ctx: ResolveCacheEntryTtlContext,
  data: ResolveCacheEntryTtlData,
): number | false | undefined {
  if (data?.kind === 'FETCH' && data.revalidate !== undefined) {
    return data.revalidate;
  }
  if (ctx.revalidate !== undefined) {
    return ctx.revalidate;
  }
  if (ctx.cacheControl?.revalidate !== undefined) {
    return ctx.cacheControl.revalidate;
  }
  if (data?.revalidate !== undefined) {
    return data.revalidate;
  }
  return undefined;
}
