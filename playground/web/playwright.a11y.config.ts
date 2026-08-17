import { defineConfig, devices } from "@playwright/test";

/**
 * Isolated configuration for the exhaustive accessibility sweep.
 *
 * Kept separate from playwright.config.ts for the same reason as the density measurement:
 * this is a CPU-bound sweep, not a functional flow. It runs 56 axe analyses (8 paths x
 * light/dark x two contrast settings, plus reduced-motion, mobile-viewport and
 * forced-colours passes), and axe is expensive enough that sharing a machine with a
 * parallel browser suite starved an unrelated pre-existing test past its timeout -- the
 * same contention already documented for the density painter, which measures 5.30 ms alone
 * and 22.20 ms under six workers.
 *
 * One project rather than two: viewport is varied inside the sweep with setViewportSize
 * where it changes the rendered DOM, which is cheaper than a second device project and
 * keeps every scan in one run. The earlier claim that these checks are
 * "viewport-independent" was true of token contrast and false of landmark structure, since
 * three width breakpoints restack the layout.
 *
 * Structural checks that axe cannot make -- focus order, reflow, target size -- live in
 * e2e/a11y-structure.spec.ts and run under this same config.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /a11y-(sweep|structure)\.spec\.ts$/,
  timeout: 300_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: "list",
  use: {
    // Port 4174, not 4173. CI runs `test`, `test:a11y` and `perf:density` as three sequential
    // steps, and `reuseExistingServer` is FALSE under CI, so all three used to bind the same
    // port. Playwright normally tears its webServer down between runs, but a lagging teardown
    // makes the next step fail hard with "port already in use" -- a flake introduced by wiring
    // these two suites into CI. Distinct ports make the collision unrepresentable.
    baseURL: "http://127.0.0.1:4174",
    trace: "off",
  },
  webServer: {
    command: "npm run build && npm run preview -- --port 4174",
    url: "http://127.0.0.1:4174",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
