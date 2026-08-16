import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  // Two specs are excluded from the default run on purpose, both because they are CPU-bound
  // sweeps rather than functional flows, and this suite runs browser workers in parallel.
  //
  // The throughput measurement: the same draw measures 5.60 ms isolated and 22.20 ms under
  // that contention, so inside the parallel suite the assertion reports the test runner
  // rather than the painter. Run it with `npm run perf:density`, which pins a single worker.
  //
  // The accessibility sweep: 28 axe analyses across 7 routes x light/dark x two contrast
  // settings. Measured while inside this suite, it starved an unrelated pre-existing test
  // past its timeout. Run it with `npm run test:a11y`.
  testIgnore: /(density-throughput|a11y-sweep)\.spec\.ts$/,
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
