import { defineConfig, devices } from '@playwright/test';

// End-to-end tests drive the running service the way a host's front end does:
// the REST inbox, the SSE stream, the unsubscribe link, provider callbacks.
// Producers are driven through the operator broadcast route, so no test needs a
// gRPC client. See docs/testing.md for where e2e sits among the test layers.
//
// The service under test is the docker compose stack, started here unless
// NOTIFY_E2E_BASE_URL points at one already running.

const external = process.env.NOTIFY_E2E_BASE_URL;
const baseURL = external ?? 'http://localhost:8080';
const ci = !!process.env.CI;

export default defineConfig({
  testDir: './tests',
  // Every test builds its own tenants and recipients (support/fixtures.ts), so
  // tests are isolated by data and run fully in parallel.
  fullyParallel: true,
  forbidOnly: ci,
  // One retry in CI, with a trace of the retry: a flaky test is visible in the
  // report instead of silently green.
  retries: ci ? 1 : 0,
  reporter: ci ? [['github'], ['html', { open: 'never' }]] : 'list',
  globalSetup: './support/global-setup.ts',
  use: {
    baseURL,
    trace: 'on-first-retry',
  },
  projects: [
    // The REST contract, over HTTP only.
    { name: 'api', testMatch: /.*\.api\.spec\.ts/ },
    // The live stream, through a real browser's EventSource.
    { name: 'chromium', testMatch: /.*\.browser\.spec\.ts/, use: { ...devices['Desktop Chrome'] } },
  ],
  webServer: external
    ? undefined
    : {
        command: 'docker compose -f ../deploy/docker-compose.yml --profile e2e up --build --wait',
        url: `${baseURL}/readyz`,
        reuseExistingServer: !ci,
        timeout: 240_000,
        stdout: 'pipe',
      },
});
