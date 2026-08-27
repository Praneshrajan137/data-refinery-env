import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

import { analyzePayload, sampleCsv, sourceHash } from "./fixtures";

const primaryRepairNote = "Row 6 rating: 45.0 -> 4.5 passed safety and verifier gates.";

async function allowClipboardWrite(page: import("@playwright/test").Page) {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: () => Promise.resolve(),
      },
    });
  });
}

function primaryRepairMoment(page: import("@playwright/test").Page) {
  return page.locator(".loop-panel--repair .primary-repair-note strong").filter({ hasText: primaryRepairNote });
}

async function activateAnalyze(page: import("@playwright/test").Page) {
  const analyze = page.getByRole("button", { name: "Analyze", exact: true });
  // 15s, not the default 5s. Enablement waits on the sample dataset arriving, so this is a
  // network-dependent condition being asserted with a UI-latency timeout. It passed by luck
  // until the suite grew: the same worker contention already documented for the density perf
  // assertion (5.30ms isolated, 22.20ms under six workers) pushes this past 5s, and with
  // `retries: 0` outside CI a flake becomes a hard failure. The state is correct either way,
  // only slower, so the timeout is the thing that was wrong.
  await expect(analyze).toBeEnabled({ timeout: 15_000 });
  await analyze.focus();
  await analyze.press("Enter");
}


function workflowStreamBody(accepted = false) {
  const analysis = analyzePayload(accepted);
  const runId = accepted ? "run-accepted" : "run-default";
  const now = "2026-06-02T00:00:00Z";
  const stages: Array<[string, string, string, Record<string, number | boolean>]> = [
    ["intake", "running", "Reading CSV upload and establishing the dry-run boundary.", { bytes: sampleCsv.length }],
    ["intake", "completed", "Accepted hospital_10rows.csv for stateless dry-run analysis.", { bytes: sampleCsv.length }],
    ["schema_inference", "running", "Inferring schema assumptions before repair semantics are applied.", {}],
    [
      "schema_inference",
      "completed",
      "Inferred 2 reviewable constraint candidate(s).",
      { candidates: 2, repair_supported_pending: accepted ? 0 : 1 },
    ],
    [
      "constraint_review",
      "completed",
      `${accepted ? 1 : 0} accepted constraint(s) were used for repair semantics.`,
      { accepted: accepted ? 1 : 0, pending_supported: accepted ? 0 : 1 },
    ],
    ["detectors", "completed", "Detected 1 issue group across the uploaded CSV.", { issues: 1, review: 1, unsafe: 0 }],
    [
      "repair_candidates",
      "completed",
      "Produced 1 candidate repair(s); 1 became verified fix(es).",
      { candidate_repairs: 1, verified_fixes: 1, failures: 1 },
    ],
    ["safety_gate", "completed", "Safety gate returned allow.", { proof_obligations: 1 }],
    ["smt_verifier", "completed", "SMT verifier returned accept.", { proof_obligations: 1, abstentions: 1 }],
    [
      "dry_run_transaction",
      "completed",
      "Created dry-run transaction txn-demo; no uploaded data was mutated.",
      { fixes: 1, applied: false },
    ],
    ["receipt", "completed", analysis.receipt.reason, { issues: 1, fixes: 1, limitations: 2 }],
  ];
  const events = stages.map(([stage_id, status, summary, counts], sequence) => ({
    schema_version: "workflow_event_v1",
    run_id: runId,
    sequence,
    stage_id,
    status,
    summary,
    started_at: now,
    completed_at: status === "running" ? undefined : now,
    counts,
    confidence: stage_id === "schema_inference" ? 0.96 : stage_id === "repair_candidates" ? 0.91 : undefined,
    uncertainty: stage_id === "schema_inference" ? "Inference is advisory until accepted for the current run." : undefined,
    requires_human: ["constraint_review", "detectors", "repair_candidates", "smt_verifier", "receipt"].includes(
      String(stage_id),
    ),
    analysis: stage_id === "receipt" ? analysis : undefined,
  }));
  return `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
}

function verifyScenarioPayload() {
  return {
    name: "hospital_10rows",
    proposer: "triage-agent",
    fixes: [
      { row: 0, column: "er_wait_time", new_value: "30" },
      { row: 1, column: "rating", new_value: "abc" },
      { row: 2, column: "rating", new_value: "4.0", expected_old_value: "WRONG" },
      { row: 0, column: "ghost_column", new_value: "x" },
    ],
    accepted_constraint_ids: ["cnd-er-type", "cnd-rating-type"],
    note: "A triage agent proposed four edits. Only the correctly-typed edit is proven.",
  };
}

function verifyFixesPayload() {
  const sourceHash = "d".repeat(64);
  return {
    source: {
      name: "hospital_10rows.csv",
      size_bytes: 512,
      sha256: sourceHash,
      rows: 10,
      columns: 10,
      column_names: ["provider_number", "rating", "er_wait_time"],
    },
    proposer: "triage-agent",
    proposed_count: 4,
    authoritative_schema: true,
    would_apply: [
      {
        row: 0,
        column: "er_wait_time",
        old_value: "28",
        new_value: "30",
        detector_id: "external",
        reason: "External proposal by 'triage-agent'.",
        confidence: 1,
        provenance: "external",
        verifier_reason: "Accepted by the safety constitution and the shared prove gate.",
        verification_strength: "proven",
      },
    ],
    receipt: {
      schema_version: "repair_receipt_v1",
      receipt_version: "repair_receipt_v1",
      contract_version: "repair_contract_v2",
      mode: "dry_run",
      applied: false,
      reversible: true,
      source_sha256: sourceHash,
      post_sha256: null,
      txn_id: null,
      safety_verdict: "allow",
      verifier_verdict: "accept",
      independent_verification: "agreed",
      issues_count: 4,
      fixes_count: 1,
      candidate_provenance: ["external"],
      root_causes: [],
      candidate_repairs: [],
      applied_fixes: [],
      suggested_fixes: [
        {
          row: 1,
          column: "rating",
          old_value: "3.8",
          new_value: "abc",
          detector_id: "external",
          operation: "update",
          reason: "External proposal.",
          confidence: 1,
          provenance: "external",
          verifier_reason: "Rejected: the prove gate could not verify this external value.",
          verification_strength: "plausibility_only",
          review_reason: "verifier_rejected",
        },
        {
          row: 2,
          column: "rating",
          old_value: "4.1",
          new_value: "4.0",
          detector_id: "external",
          operation: "update",
          reason: "External proposal.",
          confidence: 1,
          provenance: "external",
          verifier_reason: "Rejected: expected_old_value did not match the current cell.",
          verification_strength: "plausibility_only",
          review_reason: "stale_precondition",
        },
        {
          row: 0,
          column: "ghost_column",
          old_value: "",
          new_value: "x",
          detector_id: "external",
          operation: "update",
          reason: "External proposal.",
          confidence: 1,
          provenance: "external",
          verifier_reason: "Rejected: unknown column, out-of-range row, or duplicate cell edit.",
          verification_strength: "plausibility_only",
          review_reason: "invalid_target",
        },
      ],
      proof_obligations: [],
      accepted_constraint_ids: ["cnd-er-type", "cnd-rating-type"],
      constraints_artifact_sha256: null,
      patch_plan_sha256: null,
      revert_command: null,
      limitations: [],
      reason: "Dry run: 1 external fix is proven; no source data was mutated.",
    },
    verification: {
      safety_verdict: "allow",
      verifier_verdict: "accept",
      accepted_constraint_ids: ["cnd-er-type", "cnd-rating-type"],
      failures: [],
      abstentions: [],
      failure_reasons: [],
    },
    certificate: {
      ok: true,
      checks: [
        { name: "schema_recognized", ok: true, detail: "schema_version='repair_receipt_v1'" },
        { name: "data_identity", ok: true, detail: "sha256(data) matches source_sha256." },
        {
          name: "auto_apply_is_proven_deterministic",
          ok: true,
          detail: "auto-applied set is proven (deterministic).",
        },
      ],
    },
    apply_handoff: {
      source_name: "hospital_10rows.csv",
      dry_run_command:
        "dataforge verify-apply path/to/hospital_10rows.csv --fixes fixes.json --dry-run",
      apply_command:
        "dataforge verify-apply path/to/hospital_10rows.csv --fixes fixes.json --apply",
      audit_command: "dataforge audit <txn-id>",
      revert_command: "dataforge revert <txn-id>",
      note: "The hosted playground never mutates uploads.",
    },
    limitations: ["Hosted verification is stateless and dry-run only; no upload is ever mutated."],
    meta: { api_version: "0.1.0", contract_version: "repair_contract_v2" },
  };
}

test.beforeEach(async ({ page }) => {
  await page.route("**/api/health", async (route) => {
    await route.fulfill({
      json: {
        status: "ok",
        advanced_available: false,
        verify_available: true,
        streaming_available: true,
        workflow_contract_version: "workflow_event_v1",
        max_upload_bytes: 1_048_576,
      },
    });
  });
  await page.route("**/api/samples/hospital_10rows", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "text/csv",
      body: sampleCsv,
      headers: { "content-disposition": 'attachment; filename="hospital_10rows.csv"' },
    });
  });
  await page.route("**/api/samples/flights_10rows", async (route) => {
    await route.fulfill({ status: 200, contentType: "text/csv", body: sampleCsv });
  });
  await page.route("**/api/samples/beers_10rows", async (route) => {
    await route.fulfill({ status: 200, contentType: "text/csv", body: sampleCsv });
  });
  await page.route("**/api/analyze/stream**", async (route) => {
    const posted = route.request().postData() ?? "";
    await route.fulfill({
      status: 200,
      contentType: "application/x-ndjson",
      body: workflowStreamBody(posted.includes("cnd-state-fd")),
    });
  });
  await page.route("**/api/analyze", async (route) => {
    const posted = route.request().postData() ?? "";
    await route.fulfill({ json: analyzePayload(posted.includes("cnd-state-fd")) });
  });
  await page.route("**/api/verify-scenarios/**", async (route) => {
    await route.fulfill({ json: verifyScenarioPayload() });
  });
  await page.route("**/api/verify-fixes", async (route) => {
    await route.fulfill({ json: verifyFixesPayload() });
  });
});

test("sample path analyzes, accepts constraints, exports evidence, and passes accessibility", async ({ page }) => {
  await allowClipboardWrite(page);
  await page.goto("/playground/run");

  await expect(page.getByRole("region", { name: "DataForge mission bar" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "CSV repair workbench" })).toBeVisible();
  await expect(page.getByText("Stateless dry run")).toBeVisible();
  await expect(page.getByLabel("CSV repair loop").getByText("Upload", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: /Upload CSV -> profile -> issues -> verified repair -> receipt -> safe revert/ })).toBeVisible();
  await page.getByRole("button", { name: /Hospital/ }).click();
  await expect(page.locator(".loop-panel--profile").getByRole("heading", { name: "Current CSV" })).toBeVisible();
  // The row preview lives in `section.dataset-panel` (lenses.tsx `OverviewLens`), not in
  // "Dataset intake" -- that region renders only the upload control and the sample chips. Two
  // panels carry a "Current CSV" heading, hence the class selector.
  //
  // This asserted `45.0` for the dirty `rating` cell, which the preview cannot show:
  // `parseCsvPreview` caps it at five rows and that value is on row six. The assertion was
  // unsatisfiable against any correct implementation, so it proved nothing about the preview.
  // Asserting the preview's own shape line plus a first-row cell keeps it non-vacuous.
  await expect(page.locator(".dataset-panel").getByText("5 preview rows, 10 columns")).toBeVisible();
  await expect(page.locator(".dataset-panel").getByText("2175550101")).toBeVisible();

  await activateAnalyze(page);
  await expect(page.getByRole("heading", { name: "1 issue group(s)" })).toBeVisible();
  await expect(page.getByText("decimal shift")).toBeVisible();
  await expect(primaryRepairMoment(page)).toBeVisible();
  await expect(page.getByText("All proposed fixes passed the SMT verifier.")).toBeVisible();
  await expect(page.getByRole("heading", { name: "1 proven fix ready to apply" })).toBeVisible();
  await expect(page.getByText("No unproven change would be written.", { exact: false })).toBeVisible();
  await expect(page.getByText("Export dry-run receipt")).toBeVisible();
  await expect(page.getByText("Hosted analysis is dry-run only and never mutates uploaded CSV files.")).toBeVisible();
  await expect(page.getByText("Local revert refuses if the file has drifted from the recorded post-state hash.")).toBeVisible();
  await expect(page).toHaveURL(/\/playground\/run(\?|$)/);

  await page.locator('.product-nav a[href="/playground/atlas"]').click();
  const reviewQueue = page.getByLabel("Human review queue");
  await reviewQueue.getByRole("checkbox", { name: /functional_dependency constraint cnd-state-fd/ }).check();
  await reviewQueue.getByRole("button", { name: "Rerun with accepted constraints" }).click();
  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  await expect(page).toHaveURL(/\/playground\/evidence(\?|$)/);
  await expect(page.getByRole("heading", { name: "Constraint review" })).toBeVisible();
  await expect(page.locator(".risk-lens").getByRole("cell", { name: "accepted" })).toBeVisible();

  // The overview: aggregated bands carry no rung, so its accessible name must state
  // what it does NOT claim. The per-column table beside it carries the exact counts,
  // which is the channel allowed to carry magnitude.
  const overview = page.getByRole("region", { name: "Flagged cell overview" });
  await expect(overview.getByRole("heading", { name: "Where cells are flagged" })).toBeVisible();
  await expect(overview.getByRole("img")).toHaveAttribute(
    "aria-label",
    /Flagged cell overview: \d+ rows by \d+ columns/,
  );
  await expect(overview.getByRole("img")).toHaveAttribute(
    "aria-label",
    /where cells are flagged, not what was proven/,
  );
  await expect(overview).toContainText("It does not show what was proven");
  await expect(overview).toContainText(/flagged cells are located|Showing \d+ of \d+|measured result/);
  await expect(overview.getByRole("columnheader", { name: "Flagged cells" })).toBeVisible();
  await expect(overview.getByRole("columnheader", { name: "Bands affected" })).toBeVisible();

  // The confidence panel must state what the signal cannot do, and must NOT draw a
  // threshold: those thresholds gate the corrector path, not deterministic auto-apply.
  const confidence = page.getByRole("region", { name: "Detector confidence" });
  await expect(
    confidence.getByRole("heading", { name: "What detector confidence can tell you" }),
  ).toBeVisible();
  await expect(confidence).toContainText("gate that never fires on this run");

  // The dependency graph: 2D and deterministic. Its accessible name and edge table
  // are the contract, since an SVG diagram alone conveys nothing to a screen reader.
  const graph = page.getByRole("region", { name: "Column dependency graph" });
  await expect(graph.getByRole("heading", { name: "What determines what" })).toBeVisible();
  await expect(graph.getByRole("img")).toHaveAttribute(
    "aria-label",
    /Column dependency graph: \d+ columns, \d+ inferred dependencies/,
  );
  await expect(graph.getByRole("columnheader", { name: "Dependent" })).toBeVisible();

  // /evidence was previously never axe-scanned; the surface lives here.
  const evidenceScan = await new AxeBuilder({ page }).analyze();
  expect(evidenceScan.violations).toEqual([]);

  await page.locator('.product-nav a[href="/playground/repairs"]').click();
  const repairsPanel = page.locator(".repairs-lens");
  await expect(repairsPanel.getByText("Value 45 in column rating appears to be ~10x the typical value.")).toBeVisible();
  await expect(repairsPanel.getByText("All proposed fixes passed the SMT verifier.")).toBeVisible();
  await expect(repairsPanel.getByText("Attempted but not fixed")).toBeVisible();
  await expect(repairsPanel.getByText("proven", { exact: true }).first()).toBeVisible();

  await page.locator('.product-nav a[href="/playground/receipt"]').click();
  const receiptPanel = page.locator(".receipt-lens");
  await expect(receiptPanel.getByText("txn-demo", { exact: true })).toBeVisible();
  await expect(receiptPanel.getByLabel("Repair receipt summary").getByText("Independent verify")).toBeVisible();
  await expect(receiptPanel.getByRole("heading", { name: "Re-verified 3/3 checks" })).toBeVisible();
  await expect(receiptPanel.getByRole("button", { name: "Download portable certificate" })).toBeVisible();
  await expect(receiptPanel).toContainText("constraints.json");

  const receiptToolbar = page.locator(".receipt-toolbar");
  await receiptToolbar.getByRole("button", { name: "Copy" }).click();
  await expect(receiptToolbar.getByRole("button", { name: "Copied" })).toBeVisible();

  const download = page.waitForEvent("download");
  await receiptToolbar.getByRole("button", { name: "Export" }).click();
  await expect((await download).suggestedFilename()).toContain("dataforge-dry-run");

  const scan = await new AxeBuilder({ page }).analyze();
  expect(scan.violations).toEqual([]);
});

/**
 * The exhaustive route x theme x contrast axe sweep lives in `e2e/a11y-sweep.spec.ts`, run
 * with `playwright.a11y.config.ts` at one worker.
 *
 * It was written here first and moved out on measurement: 28 axe analyses is CPU-bound
 * enough that sharing this suite starved an unrelated pre-existing test past its timeout,
 * the same contention already documented for the density painter. A sweep and a functional
 * flow are different kinds of test and the repo already separates them that way.
 */

/**
 * The forced-colours canvas check lives in `e2e/a11y-sweep.spec.ts`.
 *
 * It was written here and moved out on evidence: running a full analyze from this file
 * without the dataset selection the other flows perform left shared state that made the
 * following test ("failed upload keeps the last valid dataset") fail even at one worker.
 * Reproduced by running just those two together. An environmental check does not belong in
 * the middle of a functional sequence.
 */

test("guardrail verifies an untrusted agent batch and passes accessibility", async ({ page }) => {
  await page.goto("/playground/guardrail");

  await expect(
    page.getByRole("heading", { name: /Verify an untrusted actor/ }),
  ).toBeVisible();

  // 1. Choose a dataset, then load the scripted untrusted-agent batch.
  await page.locator(".guardrail-intake").getByRole("button", { name: /Hospital/ }).click();
  await page.getByRole("button", { name: "Load scripted agent batch" }).click();
  await expect(page.getByText(/Authoritative schema: 2 accepted constraints/)).toBeVisible();

  // 2. Verify the proposals (keyboard-activate to stay robust across viewports).
  const verifyButton = page.getByRole("button", { name: "Verify proposed fixes" });
  await verifyButton.scrollIntoViewIfNeeded();
  await verifyButton.focus();
  await page.keyboard.press("Enter");

  // 3. The guardrail verdict shows the proven/blocked split and a re-verifying certificate.
  await expect(page.getByRole("heading", { name: "1 proven, 3 blocked" })).toBeVisible();
  await expect(page.getByText("zero corruptions", { exact: false })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Verified external fixes" })).toBeVisible();
  await expect(
    page.locator(".would-apply-row").getByText("proven", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Proposals not proven safe to auto-apply" }),
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: "Re-verified 3/3 checks" })).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Download portable certificate" }),
  ).toBeVisible();

  const scan = await new AxeBuilder({ page }).analyze();
  expect(scan.violations).toEqual([]);
});

test("uploaded CSV path validates and analyzes without samples", async ({ page }) => {
  await page.goto("/playground/run");

  await page
    .locator("#csv-upload")
    .setInputFiles({ name: "upload.csv", mimeType: "text/csv", buffer: Buffer.from(sampleCsv) });

  await expect(page.getByLabel("Dataset intake").getByText("upload.csv", { exact: true })).toBeVisible();
  await activateAnalyze(page);
  await expect(primaryRepairMoment(page)).toBeVisible();
});

test("failed upload keeps the last valid dataset and shows a copy fallback", async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: () => Promise.reject(new Error("permission denied")),
      },
    });
  });
  await page.goto("/playground/run");

  await page
    .locator("#csv-upload")
    .setInputFiles({ name: "upload.csv", mimeType: "text/csv", buffer: Buffer.from(sampleCsv) });
  // Both assertions here previously looked for `45.0`, which the five-row preview never shows
  // (that cell is on row six). The point of this test is that a REJECTED second upload leaves
  // the first dataset intact, so it needs a value the preview actually renders -- a row-one
  // cell -- asserted before and after the failed upload.
  await expect(page.getByText("2175550101")).toBeVisible();

  await page.locator("#csv-upload").setInputFiles({
    name: "broken.csv",
    mimeType: "text/csv",
    buffer: Buffer.from('id,name\n1,"unterminated'),
  });
  await expect(page.getByRole("alert")).toContainText("Dataset validation failed");
  await expect(page.getByText("2175550101")).toBeVisible();

  await activateAnalyze(page);
  await expect(primaryRepairMoment(page)).toBeVisible();
  await page.locator('.product-nav a[href="/playground/receipt"]').click();
  const receiptToolbar = page.locator(".receipt-toolbar");
  await receiptToolbar.getByRole("button", { name: "Copy" }).click();
  await expect(receiptToolbar.getByRole("button", { name: "Copy failed" })).toBeVisible();
  await expect(page.locator(".copy-fallback").getByLabel("Copyable repair evidence")).toHaveValue(/transaction_journal/);
});

test("client rejects files above the health capability limit", async ({ page }) => {
  await page.goto("/playground/run");

  await page.locator("#csv-upload").setInputFiles({
    name: "big.csv",
    mimeType: "text/csv",
    buffer: Buffer.from(`id\n${"x".repeat(1_048_577)}`),
  });

  await expect(page.getByRole("alert")).toContainText("larger than the hosted playground limit");
});

test("product routes support direct load, navigation, and browser history", async ({ page }) => {
  for (const path of ["/playground/", "/playground/run", "/playground/atlas", "/playground/evidence", "/playground/repairs", "/playground/receipt", "/playground/system"]) {
    await page.goto(path);
    await expect(page.getByLabel("DataForge product navigation")).toBeVisible();
  }

  await page.goto("/playground/");
  await page.locator('.product-nav a[href="/playground/run"]').click();
  await expect(page).toHaveURL(/\/playground\/run(\?|$)/);
  await page.locator('.product-nav a[href="/playground/system"]').click();
  await expect(page).toHaveURL(/\/playground\/system(\?|$)/);
  await page.goBack();
  await expect(page).toHaveURL(/\/playground\/run(\?|$)/);
});

test("reduced motion keeps route and workflow state visible without overflow", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("/playground/run");

  await expect(page.getByRole("region", { name: "DataForge mission bar" })).toBeVisible();
  await expect(page.locator(".route-motion-frame")).toHaveAttribute("data-motion-route", "run");
  await expect(page.evaluate(() => window.matchMedia("(prefers-reduced-motion: reduce)").matches)).resolves.toBe(true);

  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);
  await expect(primaryRepairMoment(page)).toBeVisible();
  await page.locator('.product-nav a[href="/playground/atlas"]').click();

  await expect(page.locator("[data-agent-motion]").first()).toBeVisible();
  await expect(page.locator("[data-workflow-status='completed']").first()).toBeVisible();

  // The reduced-motion twin must remove the depth OFFSETS without upgrading any rung:
  // the hue, the form and the written verdict all have to survive, because removing
  // an offset must never make a weaker claim look stronger. Checked on the real marks
  // rather than asserted in prose.
  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  const overview = page.getByRole("region", { name: "Flagged cell overview" });
  const inspect = overview.getByRole("button", { name: /^Inspect / }).first();
  await inspect.scrollIntoViewIfNeeded();
  await inspect.focus();
  await page.keyboard.press("Enter");

  const claims = page.getByRole("region", { name: "Individual claims" }).locator(".claim");
  await expect(claims.first()).toBeVisible();
  const marks = await claims.evaluateAll((nodes) =>
    nodes.map((node) => {
      const button = node.querySelector(".claim__button")!;
      const style = getComputedStyle(button);
      return {
        rung: node.getAttribute("data-rung"),
        transform: style.transform,
        text: (button.textContent ?? "").trim(),
      };
    }),
  );
  expect(marks.length).toBeGreaterThan(0);
  for (const mark of marks) {
    // No translation survives reduced motion, whatever the rung.
    expect(["none", "matrix(1, 0, 0, 1, 0, 0)"]).toContain(mark.transform);
    // The verdict is still written out, so the rung is not carried by depth alone.
    expect(mark.text).toMatch(/Proven|Held|Plausibility|Rejected|Downgraded|Corroborated/);
  }

  // Horizontal overflow is a hard failure, not a cosmetic one: it makes content
  // unreachable on a phone. Measured against innerWidth on every route this test
  // visits, including the claim detail, whose canvas sibling previously grew an
  // auto-sized grid track past the viewport.
  const layout = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    innerWidth: window.innerWidth,
  }));
  expect(layout.scrollWidth).toBeLessThanOrEqual(layout.innerWidth + 1);
});

for (const colorScheme of ["light", "dark"] as const) {
  test(`DataForge multi-page product supports the full sample flow in ${colorScheme} mode`, async ({ page }) => {
    await allowClipboardWrite(page);
    await page.emulateMedia({ colorScheme });
    await page.goto("/playground/run");

    const rootTokens = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return {
        bg: styles.getPropertyValue("--df-bg").trim(),
        text: styles.getPropertyValue("--df-text-1").trim(),
        action: styles.getPropertyValue("--df-action-bg").trim(),
        success: styles.getPropertyValue("--df-status-safe-bg").trim(),
        agent: styles.getPropertyValue("--df-agent-bg").trim(),
        stage: styles.getPropertyValue("--df-stage-active-bg").trim(),
      };
    });
    expect(rootTokens.bg).not.toEqual("");
    expect(rootTokens.text).not.toEqual("");
    expect(rootTokens.action).not.toEqual("");
    expect(rootTokens.action).not.toEqual(rootTokens.success);
    expect(rootTokens.action).not.toContain("0, 0");
    expect(rootTokens.agent).not.toEqual("");
    expect(rootTokens.stage).not.toEqual("");
    await expect(page.getByRole("region", { name: "DataForge mission bar" })).toBeVisible();
    await expect(page.getByText("CSV repair workbench")).toBeVisible();
    await expect(page.getByText("Stateless dry run")).toBeVisible();

    await page.getByRole("button", { name: /Hospital/ }).click();
    await activateAnalyze(page);
    await expect(primaryRepairMoment(page)).toBeVisible();
    await expect(page.getByRole("heading", { name: "1 issue group(s)" })).toBeVisible();

    await page.locator('.product-nav a[href="/playground/atlas"]').click();
    await expect(page.getByRole("heading", { name: "Live agent workflow" })).toBeVisible();

    await page.locator('.product-nav a[href="/playground/repairs"]').click();
    const repairsPanel = page.locator(".repairs-lens");
    await expect(repairsPanel.getByText("Value 45 in column rating appears to be ~10x the typical value.")).toBeVisible();
    await expect(repairsPanel.getByText("Verified dry-run evidence")).toBeVisible();

    const layout = await page.evaluate(() => ({
      scrollWidth: document.documentElement.scrollWidth,
      innerWidth: window.innerWidth,
    }));
    expect(layout.scrollWidth).toBeLessThanOrEqual(layout.innerWidth + 1);

    await page.locator('.product-nav a[href="/playground/receipt"]').click();
    await expect(page.locator(".receipt-lens").getByText("txn-demo", { exact: true })).toBeVisible();

    const receiptToolbar = page.locator(".receipt-toolbar");
    await receiptToolbar.getByRole("button", { name: "Copy" }).click();
    await expect(receiptToolbar.getByRole("button", { name: "Copied" })).toBeVisible();

    const download = page.waitForEvent("download");
    await receiptToolbar.getByRole("button", { name: "Export" }).click();
    await expect((await download).suggestedFilename()).toContain("dataforge-dry-run");

    const scan = await new AxeBuilder({ page }).analyze();
    expect(scan.violations).toEqual([]);
  });
}

// --- Pixel verification -------------------------------------------------------
// Every test in the first iteration of the visualisation work passed while the
// canvas could have been entirely blank: jsdom yields no drawing context, so unit
// tests exercised the encoder and never the painter, and the WebGL path could not be
// read back without preserveDrawingBuffer. getImageData on a 2D canvas closes that
// gap, and is the reason the 2D painter is preferable rather than merely adequate.

async function useJsonAnalyze(page: Page): Promise<void> {
  // The default mocks stream, and workflowStreamBody builds its own payload
  // internally, so a route override on /api/analyze alone never fires. Reporting no
  // streaming support makes the client use the JSON endpoint these tests override.
  await page.route("**/api/health", async (route) => {
    await route.fulfill({
      json: {
        status: "ok",
        advanced_available: false,
        verify_available: true,
        streaming_available: false,
        max_upload_bytes: 1_048_576,
      },
    });
  });
}

async function countInk(page: Page): Promise<number> {
  return page.evaluate(() => {
    const canvas = document.querySelector<HTMLCanvasElement>(".evidence-overview__canvas");
    if (canvas === null) {
      return -1;
    }
    const ctx = canvas.getContext("2d");
    if (ctx === null) {
      return -2;
    }
    const { data } = ctx.getImageData(0, 0, canvas.width, canvas.height);
    let opaque = 0;
    for (let index = 3; index < data.length; index += 4) {
      if (data[index] > 0) {
        opaque += 1;
      }
    }
    return opaque;
  });
}

test("the density map draws real ink and says nothing about proof", async ({ page }) => {
  await useJsonAnalyze(page);
  await page.route("**/api/analyze", async (route) => {
    const payload = analyzePayload(false);
    payload.source.rows = 400;
    // The index is what the map reads, so it is what this test must populate.
    // rating is column 4 and state is column 6 in the hospital sample header.
    payload.flagged_cells = {
      index: {
        column_indices: Array.from({ length: 120 }, (_, index) => (index % 2 === 0 ? 4 : 6)),
        rows: Array.from({ length: 120 }, (_, index) => index * 3),
      },
      confidence_histogram: [
        {
          issue_type: "fd_violation",
          bins: Array.from({ length: 10 }, (_, index) => ({
            from_value: index / 10,
            to_value: (index + 1) / 10,
            count: index === 9 ? 120 : 0,
          })),
          count: 120,
          distinct_values: 1,
          mode_value: 0.95,
          mode_share: 1,
        },
      ],
      cells: Array.from({ length: 120 }, (_, index) => ({
        row: index * 3,
        column: index % 2 === 0 ? "rating" : "state",
        issue_type: "fd_violation",
        severity: "unsafe",
        confidence: 0.95,
        actual: "x",
        expected: null,
        reason: "violates dependency",
      })),
      total: 120,
      truncated: false,
      note: "All 120 flagged cells are located and individually listed.",
    };
    await route.fulfill({ json: payload });
  });

  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);
  await expect(page.getByRole("heading", { name: "1 issue group(s)" })).toBeVisible();

  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  const overview = page.getByRole("region", { name: "Flagged cell overview" });
  const map = overview.getByRole("img");
  await expect(map).toBeVisible();

  // The decisive assertion, absent from the previous work entirely.
  expect(await countInk(page)).toBeGreaterThan(0);

  const label = await map.getAttribute("aria-label");
  expect(label).toMatch(/Flagged cell overview: 400 rows by \d+ columns/);
  expect(label).toContain("where cells are flagged, not what was proven");

  // Exact counts live in text, which is the channel allowed to carry magnitude.
  await expect(overview.getByRole("columnheader", { name: "Flagged cells" })).toBeVisible();
});

test("an empty run renders a stated absence, not a blank canvas", async ({ page }) => {
  await useJsonAnalyze(page);
  await page.route("**/api/analyze", async (route) => {
    const payload = analyzePayload(false);
    payload.issues = [];
    payload.repairs = [];
    payload.verification.failures = [];
    payload.receipt.root_causes = [];
    payload.receipt.suggested_fixes = [];
    payload.receipt.issues_count = 0;
    payload.flagged_cells = { index: { column_indices: [], rows: [] }, confidence_histogram: [], cells: [], total: 0, truncated: false, note: "none" };
    await route.fulfill({ json: payload });
  });

  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);

  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  const overview = page.getByRole("region", { name: "Flagged cell overview" });

  // Zero is a measured result, not silence (L3), and no canvas pretends otherwise.
  await expect(overview).toContainText("measured result");
  await expect(overview.getByRole("img")).toHaveCount(0);
});

test("selecting a column reveals addressable claims with earned depth", async ({ page }) => {
  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);
  await page.locator('.product-nav a[href="/playground/evidence"]').click();

  const overview = page.getByRole("region", { name: "Flagged cell overview" });
  const inspect = overview.getByRole("button", { name: /^Inspect / }).first();
  await inspect.scrollIntoViewIfNeeded();
  await inspect.focus();
  await page.keyboard.press("Enter");

  const detail = page.getByRole("region", { name: "Individual claims" });
  await expect(detail).toBeVisible();

  const claim = detail.locator(".claim").first();
  await expect(claim).toBeVisible();

  // Depth is only lawful on an addressable mark, so the mark must actually be tall
  // enough to have a ground. This is the check whose absence let depth ship dead.
  const box = await claim.locator(".claim__button").first().boundingBox();
  expect(box).not.toBeNull();
  expect(box!.height).toBeGreaterThanOrEqual(16);

  // Ground contact is reserved for proof. Assert it where a proven claim exists, and
  // assert its ABSENCE on unproven claims -- the absence is the signal. Guarded by
  // count so a payload without a given rung does not hang on a locator wait.
  const provenButtons = detail.locator('.claim[data-rung="proven"] .claim__button');
  if ((await provenButtons.count()) > 0) {
    const shadow = await provenButtons.first().evaluate((el) => getComputedStyle(el).boxShadow);
    expect(shadow).not.toBe("none");
  }
  const plausibleButtons = detail.locator('.claim[data-rung="plausibility_only"] .claim__button');
  if ((await plausibleButtons.count()) > 0) {
    const shadow = await plausibleButtons
      .first()
      .evaluate((el) => getComputedStyle(el).boxShadow);
    expect(shadow).toBe("none");
  }

  // The rung survives the removal of colour and depth: it is written out.
  await expect(claim).toContainText(/Proven|Held|Plausibility|Rejected|Downgraded/);

  const scan = await new AxeBuilder({ page }).analyze();
  expect(scan.violations).toEqual([]);
});

/**
 * Behaviours shipped in the previous round with unit tests only.
 *
 * Every test below is here because the App -> page -> component wiring is exactly what a
 * component test cannot reach. The first one failed on the code as shipped: the shared-link
 * feature loaded a dataset and never analysed, so a recipient landed on the same empty prompt
 * the feature claimed to remove -- while a comment in routes.ts asserted the opposite.
 */
test("a shared sample link reproduces the run instead of landing on an empty prompt", async ({
  page,
}) => {
  await page.goto("/playground/receipt?sample=hospital_10rows");

  await expect(page.getByRole("region", { name: "Repair receipt summary" })).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("Run analysis to unlock receipt")).toBeHidden();
});

test("a shared link keeps the route it names rather than redirecting to the run page", async ({
  page,
}) => {
  await page.goto("/playground/receipt?sample=hospital_10rows");
  await expect(page.locator("main")).toBeVisible();
  await expect(page).toHaveURL(/\/playground\/receipt/);
});

test("a shared link carrying no sample still renders its page", async ({ page }) => {
  await page.goto("/playground/evidence");
  await expect(page.locator("main")).toBeVisible();
  await expect(page.getByText("Run analysis to unlock evidence")).toBeVisible();
});

test("a failed run marks the previous result as stale on every route", async ({ page }) => {
  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);
  await expect(page.getByText("No unproven change would be written.", { exact: false })).toBeVisible();
  await expect(page.getByText("Trust verdict")).toBeVisible();

  // The next attempt fails. The previous receipt is still real and is kept -- but it no longer
  // describes what just happened, and every route that renders it must say so.
  await page.route("**/api/analyze/stream**", async (route) => {
    await route.fulfill({ status: 500, contentType: "text/plain", body: "boom" });
  });
  await page.route("**/api/analyze", async (route) => {
    await route.fulfill({ status: 500, contentType: "text/plain", body: "boom" });
  });
  await activateAnalyze(page);

  await expect(page.getByRole("alert")).toBeVisible();
  await expect(page.getByText("Previous run")).toBeVisible();
  await expect(page.getByText(/describes an earlier run/i)).toBeVisible();
  await expect(page.getByText("Trust verdict")).toBeHidden();

  // The header marker is the part that must hold on routes with no trust panel.
  const posture = page.getByLabel("Current run posture");
  await expect(posture).toContainText(/superseded/i);

  for (const route of ["receipt", "evidence", "repairs", "atlas"]) {
    await page.locator(`.product-nav a[href="/playground/${route}"]`).click();
    await expect(page.getByLabel("Current run posture"), route).toContainText(/superseded/i);
  }
});

test("going offline is stated, and Analyze is paused rather than left to fail", async ({
  page,
  context,
}) => {
  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await expect(page.locator(".loop-panel--profile").getByRole("heading", { name: "Current CSV" })).toBeVisible();

  await context.setOffline(true);
  // navigator.onLine does not fire the event by itself under CDP offline emulation.
  await page.evaluate(() => window.dispatchEvent(new Event("offline")));

  const banner = page.getByText("You are offline");
  await expect(banner).toBeVisible();
  // The user's real question mid-run is whether their work survived.
  await expect(page.getByText(/still here/i)).toBeVisible();
  await expect(page.getByRole("button", { name: "Analyze" })).toBeDisabled();

  await context.setOffline(false);
  await page.evaluate(() => window.dispatchEvent(new Event("online")));
  await expect(banner).toBeHidden();
  await expect(page.getByRole("button", { name: "Analyze" })).toBeEnabled();
});

test("an unreachable backend is named as such, not as a bad CSV", async ({ page }) => {
  await page.route("**/api/analyze/stream**", async (route) => route.abort("failed"));
  await page.route("**/api/analyze", async (route) => route.abort("failed"));

  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);

  const alert = page.getByRole("alert");
  await expect(alert).toBeVisible();
  // The defect this replaces: every locally-originated failure was titled as CSV validation.
  await expect(alert).toContainText(/Cannot reach the backend|You are offline/);
  await expect(alert).not.toContainText("Dataset validation failed");
  // And a retry that does not cost the user their loaded dataset.
  await expect(alert.getByRole("button", { name: /Try again/i })).toBeVisible();
  await expect(page.locator(".loop-panel--profile").getByRole("heading", { name: "Current CSV" })).toBeVisible();
});

test("a truncated workflow stream reports a cut stream, not a raw JSON error", async ({ page }) => {
  await page.route("**/api/analyze/stream**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/x-ndjson",
      // A complete line, then an object cut in half.
      body: '{"schema_version":"workflow_event_v1","run_id":"r","sequence":1,"stage_id":"intake","status":"completed","summary":"s","started_at":"2026-06-02T00:00:00Z","completed_at":"2026-06-02T00:00:01Z","counts":{},"requires_human":false}\n{"stage_i',
    });
  });

  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);

  const alert = page.getByRole("alert");
  await expect(alert).toBeVisible();
  await expect(alert).toContainText(/cut short|dropped part-way/i);
  await expect(alert).not.toContainText(/JSON/i);
});

test("an unknown address says so instead of silently rendering a real page", async ({ page }) => {
  await page.goto("/playground/nonsense");
  await expect(page.getByRole("region", { name: "No page at this address" })).toBeVisible();
  await expect(page.getByText("/playground/nonsense")).toBeVisible();
  // No real page's content may be what an unknown URL renders.
  await expect(page.getByRole("region", { name: "DataForge mission bar" })).toBeHidden();
  await expect(page.getByRole("heading", { name: "CSV repair loop", level: 1 })).toBeHidden();
  await expect(
    page.getByRole("heading", { name: "Unproven fixes are refused, not applied", level: 1 }),
  ).toBeHidden();

  // Recovery goes to the declared front door.
  await page.getByRole("button", { name: "Go to the front page" }).click();
  await expect(page).toHaveURL(/\/playground\/guardrail(\?|$)/);
  await expect(
    page.getByRole("heading", { name: "Unproven fixes are refused, not applied", level: 1 }),
  ).toBeVisible();
});

test("the backend-unavailable retry re-probes without discarding the loaded dataset", async ({
  page,
}) => {
  let healthCalls = 0;
  await page.route("**/api/health", async (route) => {
    healthCalls += 1;
    if (healthCalls <= 6) {
      await route.fulfill({ status: 503, contentType: "text/plain", body: "asleep" });
      return;
    }
    await route.fulfill({
      json: {
        status: "ok",
        advanced_available: false,
        verify_available: true,
        streaming_available: true,
        workflow_contract_version: "workflow_event_v1",
        max_upload_bytes: 1_048_576,
      },
    });
  });

  await page.goto("/playground/run");
  const chip = page.getByRole("button", { name: /Backend unavailable/i });
  await expect(chip).toBeVisible({ timeout: 30_000 });

  // The defect this replaces: this control was window.location.reload(), which threw away the
  // dataset and any completed receipt in order to recover from a transient error.
  await chip.click();
  await expect(page.getByRole("button", { name: "Analyze" })).toBeVisible({ timeout: 30_000 });
  expect(healthCalls).toBeGreaterThan(6);
});

test("the evidence page shows one risk summary, not two", async ({ page }) => {
  await page.goto("/playground/run");
  await page.getByRole("button", { name: /Hospital/ }).click();
  await activateAnalyze(page);
  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  await expect(page.getByRole("heading", { name: "Constraint review" })).toBeVisible();

  // Two panels used to render here -- one smuggled in by the overview lens, one owned by the
  // risk lens -- and the accepted mitigation was to give them different accessible names.
  await expect(page.locator(".risk-panel")).toHaveCount(1);

  // The run page still gets its risk summary; the fix must not remove it, only de-duplicate.
  await page.locator('.product-nav a[href="/playground/run"]').click();
  await expect(page.locator(".risk-panel")).toHaveCount(1);
});
