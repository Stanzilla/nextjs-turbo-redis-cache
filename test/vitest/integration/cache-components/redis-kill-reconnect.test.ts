import { describe, it, expect } from 'vitest';
import path from 'path';
import { hasContainerRuntime, runNode } from './scripts/redis-test-helpers';

describe('redis kill/reconnect end-to-end (both handlers)', () => {
  const runOrSkip = hasContainerRuntime() ? it : it.skip;

  runOrSkip(
    'survives redis restart without Socket already opened',
    async () => {
      const script = path.join(__dirname, 'scripts', 'redis-kill-reconnect.ts');

      const res = await runNode(script, 180_000);

      if (res.code !== 0) {
        console.error('Script exited with code', res.code);
        console.error('stdout:', res.stdout);
        console.error('stderr:', res.stderr);
      }

      expect(res.code).toBe(0);
      expect(res.stdout).toContain('OK');
      expect(res.stderr).not.toContain('Socket already opened');
    },
    180_000,
  );
});
