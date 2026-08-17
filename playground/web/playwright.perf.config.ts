import { defineConfig, devices } from "@playwright/test";

/**
 * Isolated configuration for the density throughput measurement.
 *
 * Kept separate from playwright.config.ts because a CPU measurement cannot share a
 * machine with a parallel browser suite and remain meaningful: the identical draw
 * measures 5.60 ms alone and 22.20 ms while six workers run, which is a property of
 * the runner, not of the painter. One project, one worker, no parallelism.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /density-throughput\.spec\.ts$/,
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  // No retries: a measurement that needs a retry to pass is not a measurement.
  retries: 0,
  reporter: "list",
  use: {
    // Port 4175: one port per config, so the three sequential CI steps cannot race a lingering
    // server from the previous step. See playwright.a11y.config.ts for the full reasoning.
    baseURL: "http://127.0.0.1:4175",
    trace: "off",
  },
  webServer: {
    command: "npm run build && npm run preview -- --port 4175",
    url: "http://127.0.0.1:4175",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
