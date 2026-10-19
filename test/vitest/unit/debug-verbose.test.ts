import { describe, it, expect, vi, afterEach } from 'vitest';
import { debugVerbose } from '../../../src/utils/debug';

describe('debugVerbose', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it.each([
    'DEBUG_CACHE_HANDLER_VERBOSE',
    'DEBUG_CACHE_HANDLER_VERBOSE_VERBOSE',
  ])('logs the message when %s is set', (name) => {
    vi.stubEnv('DEBUG_CACHE_HANDLER_VERBOSE', '');
    vi.stubEnv('DEBUG_CACHE_HANDLER_VERBOSE_VERBOSE', '');
    vi.stubEnv(name, 'true');
    const log = vi.spyOn(console, 'log').mockImplementation(() => undefined);

    debugVerbose('SyncedMap.delete() called with keys', ['a']);

    expect(log).toHaveBeenCalledTimes(1);
    expect(log.mock.calls[0]).toContain('SyncedMap.delete() called with keys');
    expect(log.mock.calls[0]).toContainEqual(['a']);
  });
});
