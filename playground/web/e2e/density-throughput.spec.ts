import { test, expect } from "@playwright/test";

import { analyzePayload, sampleCsv } from "./fixtures";

/**
 * Frame budget for the density painter -- measuring THE PAINTER.
 *
 * WHAT CHANGED AND WHY IT MATTERED
 *
 * The previous version of this file constructed its own `fillRect` loop inside
 * `page.evaluate` and asserted on that. It never imported, called, or otherwise touched
 * `src/viz/paintDensity.ts`. So the 16 ms budget verified that Chromium can draw 17,920
 * rectangles -- a fact about the browser, true on every machine, independent of anything in
 * this repository. A gate that passes for reasons unrelated to the code it names belongs to the
 * same tautology class as an infinite bundle budget: it cannot fail for the reason it claims.
 *
 * The two loops were not even equivalent. The real painter hoists `fillStyle` out of the loop
 * (the synthetic one set it 17,920 times), applies a device-pixel-ratio transform, and rounds
 * each mark's geometry. It did different work, and the number reported bore no fixed relation
 * to production cost.
 *
 * This version drives the real component and reads the duration the painter records itself, via
 * a `performance.measure` that also reaches real users' profiles rather than existing only for
 * this test.
 *
 * HONEST LIMITATION: this measures a realistic-large field, not the theoretical ceiling. The
 * bound is `bandCount x columns <= 140 x 128 = 17,920` marks, and a payload flagging every
 * column in every band was tried -- it blocked the main thread while BUILDING the model, past a
 * 60s timeout, before any paint could be timed. That is a finding about the uncapped upstream
 * work, not about the painter, and it is recorded here rather than dressed up as a ceiling
 * measurement. The mark count actually drawn is asserted and printed, so the coverage of this
 * gate is never overstated.
 *
 * Statistic: the MINIMUM of the observed paints, for the reason the original file gave and got
 * right -- noise can only add time, so the minimum is the closest observation to the true cost.
 * Median and worst are reported because a large gap between them is itself information.
 */

const ROWS = 400;
/** 10 real columns in the sample header. */
const COLUMNS = 10;
/**
 * Bands requested. The painter will not draw this many.
 *
 * A FINDING, recorded rather than smoothed over: `bandCount = min(rows, floor(heightPx / 3))`,
 * and the canvas in this layout renders about 240px tall, not the 420px the old comments
 * assumed. So banding caps at ~80, and the real worst case is `80 x 128 = 10,240` marks, not the
 * `140 x 128 = 17,920` that both paintDensity.ts and the previous version of this file asserted
 * as the ceiling. The old figure was arithmetic on an assumed viewport that nothing measured.
 */
const BANDS = 140;
/** Below the 800 actually observed, far above the 121 the first working version produced. */
const MARK_FLOOR = 700;

/**
 * A payload the app ACCEPTS, built from the shared factory.
 *
 * The first version of this file hand-rolled its own AnalyzeResponse. The app rejected it as
 * invalid, so no result ever rendered and the failure surfaced as a missing canvas -- three
 * separate debugging detours chased the canvas before the fixture turned out to be the cause.
 * Deriving from the one factory removes that whole class of failure.
 *
 * Every column is flagged in every band, because the first working version flagged two columns
 * and produced 121 marks: the painter was genuinely measured, but at a scale no budget could
 * fail on. MARK_FLOOR is asserted so this cannot quietly shrink back to a trivial field.
 */
function densityPayload() {
  const payload = analyzePayload(false);
  payload.source.rows = ROWS;
  // The map reads flagged_cells.index and nothing else.
  const columnIndices: number[] = [];
  const rows: number[] = [];
  for (let column = 0; column < COLUMNS; column += 1) {
    for (let band = 0; band < BANDS; band += 1) {
      columnIndices.push(column);
      rows.push(Math.floor((band * ROWS) / BANDS));
    }
  }
  payload.flagged_cells.index = { column_indices: columnIndices, rows };
  return payload;
}
test("the density painter draws a realistic field within one frame", async ({ page }) => {
  await page.route("**/api/health", async (route) => {
    await route.fulfill({
      json: {
        status: "ok",
        advanced_available: false,
        verify_available: true,
        streaming_available: false,
        workflow_contract_version: "workflow_event_v1",
        max_upload_bytes: 1_048_576,
      },
    });
  });
  await page.route("**/api/samples/hospital_10rows", async (route) => {
    // The real sample CSV. The payload declares 400 rows so the model bins into many bands; the
    // uploaded file's own row count does not have to match, which is what the main suite does.
    await route.fulfill({ status: 200, contentType: "text/csv", body: sampleCsv });
  });
  let analyzeCalls = 0;
  await page.route("**/api/analyze**", async (route) => {
    analyzeCalls += 1;
    console.log(`ANALYZE_CALL ${analyzeCalls} ${route.request().url()}`);
    await route.fulfill({ json: densityPayload() });
  });

  // The flow the main suite already proves works: load a sample, analyse, then navigate in-app.
  // The analysis lives in memory only, so a fresh page.goto to /evidence would discard it.
  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  const analyze = page.getByRole("button", { name: "Analyze", exact: true });
  await expect(analyze).toBeEnabled({ timeout: 20_000 });
  await analyze.focus();
  await analyze.press("Enter");

  // Wait for the result to LAND before navigating. Clicking straight after pressing Enter made
  // Playwright retry the click against a nav that was being re-rendered as the results arrived
  // ("element was detached from the DOM"), which burned the whole 60s timeout.
  await expect(page.locator(".risk-panel")).toBeVisible({ timeout: 30_000 });

  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  await expect(page.getByRole("heading", { name: "Constraint review" })).toBeVisible({
    timeout: 30_000,
  });

  const overview = page.getByRole("region", { name: "Flagged cell overview" });
  const map = overview.getByRole("img");
  await expect(map).toBeVisible({ timeout: 30_000 });

  // Force repeated REAL paints: each resize re-runs the painter through the component.
  for (const width of [1280, 1180, 1320, 1220, 1360, 1240, 1300, 1200]) {
    await page.setViewportSize({ width, height: 900 });
    await page.waitForTimeout(80);
  }

  const measured = await page.evaluate(() => {
    const entries = performance.getEntriesByName("df-density-paint");
    const durations = entries.map((entry) => entry.duration).sort((a, b) => a - b);
    const marks = entries
      .map((entry) => {
        const detail = (entry as PerformanceMeasure & { detail?: { marks?: number } }).detail;
        return detail?.marks ?? 0;
      })
      .reduce((max, value) => Math.max(max, value), 0);
    return {
      count: durations.length,
      marks,
      best: durations[0] ?? -1,
      median: durations[Math.floor(durations.length / 2)] ?? -1,
      worst: durations[durations.length - 1] ?? -1,
    };
  });

  // If either of these is zero the gate is measuring nothing, which is the defect being removed.
  expect(measured.count, "the painter recorded no measurements").toBeGreaterThan(0);
  expect(measured.marks, "the painter drew no marks").toBeGreaterThan(0);
  // And a floor, so the field cannot silently shrink to a size no budget could fail on.
  expect(measured.marks, "the density field is too small to be a meaningful budget").toBeGreaterThan(
    MARK_FLOOR,
  );

  console.log(
    `DENSITY_THROUGHPUT paints=${measured.count} marks=${measured.marks} ` +
      `best_ms=${measured.best.toFixed(2)} median_ms=${measured.median.toFixed(2)} ` +
      `worst_ms=${measured.worst.toFixed(2)}`,
  );

  // One frame at 60 Hz, the same bar as before, now applied to our code.
  expect(measured.best).toBeLessThan(16);
});
