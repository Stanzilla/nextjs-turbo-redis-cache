import { describe, it, expect, vi, afterEach } from 'vitest';
import { resolveSharedOptions, resyncDelay } from '../../../src/utils/options';

describe('resolveSharedOptions()', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it('uses a custom get timeout as the write timeout', () => {
    vi.stubEnv('REDIS_WRITE_TIMEOUT_MS', '');

    expect(resolveSharedOptions({ getTimeoutMs: 2_000 }).writeTimeoutMs).toBe(
      2_000,
    );
  });
});

describe('resyncDelay()', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function delays(slot: 'before' | 'after') {
    return [0, 0.5, 0.999].map((random) => {
      vi.spyOn(Math, 'random').mockReturnValue(random);
      return resyncDelay(60_000, slot);
    });
  }

  it('spreads the delays, and the two maps of a handler never reload in the same window', () => {
    const before = delays('before');
    const after = delays('after');

    expect(new Set(before).size).toBe(3);
    expect(new Set(after).size).toBe(3);
    expect(Math.max(...before)).toBeLessThan(Math.min(...after));
  });
});
