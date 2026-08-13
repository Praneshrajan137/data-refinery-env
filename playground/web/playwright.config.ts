import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  // The throughput measurement is excluded from the default run on purpose. It
  // measures CPU cost, and this suite runs six browser workers in parallel: the same
  // draw measures 5.60 ms isolated and 22.20 ms under that contention, so inside the
  // parallel suite the assertion reports the test runner rather than the painter.
  // Run it with `npm run perf:density`, which pins a single worker.
  testIgnore: /density-throughput\.spec\.ts$/,
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
