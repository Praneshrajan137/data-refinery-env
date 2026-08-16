import { defineConfig, devices } from "@playwright/test";

/**
 * Isolated configuration for the exhaustive accessibility sweep.
 *
 * Kept separate from playwright.config.ts for the same reason as the density measurement:
 * this is a CPU-bound sweep, not a functional flow. It runs 28 axe analyses (7 routes x
 * light/dark x two contrast settings), and axe is expensive enough that sharing a machine
 * with a parallel browser suite starved an unrelated pre-existing test past its timeout --
 * the same contention already documented for the density painter, which measures 5.30 ms
 * alone and 22.20 ms under six workers.
 *
 * One project rather than two: the sweep verifies token contrast and landmark structure,
 * which are viewport-independent. Mobile-specific interaction remains covered by the main
 * suite.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /a11y-sweep\.spec\.ts$/,
  timeout: 300_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "off",
  },
  webServer: {
    command: "npm run build && npm run preview -- --port 4173",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
