import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  // Three specs are excluded from the default run on purpose, all because they are
  // CPU-bound sweeps rather than functional flows, and this suite runs browser workers in
  // parallel.
  //
  // The throughput measurement: the same draw measures 5.60 ms isolated and 22.20 ms under
  // that contention, so inside the parallel suite the assertion reports the test runner
  // rather than the painter. Run it with `npm run perf:density`, which pins a single worker.
  //
  // The accessibility sweep: 56 axe analyses across 8 paths x light/dark x two contrast
  // settings, plus reduced-motion, mobile-viewport and forced-colours passes. Measured
  // while inside this suite, it starved an unrelated pre-existing test past its timeout.
  //
  // The structural accessibility checks: they set their own viewport (320px for reflow,
  // 412px for target size) and walk every element on every route. Left in the default run
  // they also executed under the Pixel 7 project, which re-measured the same thing at a
  // viewport the test itself overrides.
  //
  // Both accessibility specs run with `npm run test:a11y`.
  testIgnore: /(density-throughput|a11y-sweep|a11y-structure)\.spec\.ts$/,
  timeout: 60_000,
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [["github"], ["html", { open: "never" }]] : "list",
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "npm run build && npm run preview -- --port 4173",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "mobile", use: { ...devices["Pixel 7"] } },
  ],
});
