import { defineConfig } from '@playwright/test';

const testApp =
  process.env.PLAYWRIGHT_TEST_APP || 'next-app-16-2-6-cache-components';

// PLAYWRIGHT_BASE_URL selects an external server. Without it, Playwright
// starts and owns the test app on MANAGED_PORT.
const externalBaseURL = process.env.PLAYWRIGHT_BASE_URL;
const MANAGED_PORT = 3101;
const managedBaseURL = `http://localhost:${MANAGED_PORT}`;

export default defineConfig({
  testDir: 'test/playwright',
  use: {
    baseURL: externalBaseURL || managedBaseURL,
    trace: 'on-first-retry',
  },
  webServer: externalBaseURL
    ? undefined
    : {
        command: `pnpm -C test/nextjs-test-projects/${testApp} dev -p ${MANAGED_PORT}`,
        url: managedBaseURL,
        reuseExistingServer: true,
        timeout: 120_000,
      },
});
