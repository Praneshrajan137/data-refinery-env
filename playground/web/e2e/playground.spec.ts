import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const sampleCsv = [
  "provider_number,hospital_name,city,state,zip_code,phone_number,rating,mortality_rate,readmission_rate,er_wait_time",
  "PRV001,General Hospital,Springfield,IL,62701,2175550101,4.2,0.023,0.145,28",
  "PRV002,St. Mary Medical Center,Chicago,IL,60601,3125550202,3.8,0.031,0.162,35",
  "PRV001,Springfield Medical,Springfield,IL,62701,2175550303,4.5,0.019,0.138,22",
  "PRV003,Mercy Hospital,Peoria,IL,61602,3095550404,3.5,0.028,0.158,31",
  "PRV004,Northwestern Memorial,Chicago,IL,60611,3125550505,4.1,0.025,0.149,26",
  "PRV005,Rush University MC,Chicago,IL,60612,3125550606,45.0,0.022,0.141,29",
  "PRV006,Advocate Christ,Oak Lawn,IL,60453,7085550707,3.9,0.027,0.155,33",
  "PRV007,Loyola University MC,Maywood,IL,60153,7085550808,4.3,0.020,0.142,25",
  "PRV008,Presence St. Joseph,Joliet,IL,60435,8155550909,4.0,0.026,0.151,30",
  "PRV009,Edward Hospital,Naperville,IL,60540,6305551010,3.7,0.029,0.160,34",
].join("\n");
const sourceHash = "a".repeat(64);
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
  await expect(analyze).toBeEnabled();
  await analyze.focus();
  await analyze.press("Enter");
}

function analyzePayload(accepted = false) {
  return {
    source: {
      name: "hospital_10rows.csv",
      size_bytes: sampleCsv.length,
      sha256: sourceHash,
      rows: 10,
      columns: 10,
      column_names: [
        "provider_number",
        "hospital_name",
        "city",
        "state",
        "zip_code",
        "phone_number",
        "rating",
        "mortality_rate",
        "readmission_rate",
        "er_wait_time",
      ],
    },
    schema_inference: {
      schema_version: "constraint_review_v1",
      source_sha256: sourceHash,
      row_count: 10,
      candidates: [
        {
          candidate_id: "cnd-state-fd",
          kind: "functional_dependency",
          columns: ["provider_number"],
          dependent: "hospital_name",
          inferred_type: null,
          pattern: null,
          min_value: null,
          max_value: null,
          confidence: 0.92,
          evidence: "provider_number determines hospital_name in 9/10 rows.",
          decision: accepted ? "accepted" : "pending",
          repair_supported: true,
        },
        {
          candidate_id: "cnd-amount-regex",
          kind: "regex",
          columns: ["rating"],
          dependent: null,
          inferred_type: null,
          pattern: "^\\d+$",
          min_value: null,
          max_value: null,
          confidence: 1,
          evidence: "10 non-empty values matched numeric rating values.",
          decision: "pending",
          repair_supported: false,
        },
      ],
    },
    risk_summary: {
      dataset_level: "medium",
      repair_readiness: "verified",
      severity_counts: { safe: 0, review: 1, unsafe: 0 },
      pending_repair_supported_constraints: accepted ? 0 : 1,
      reasons: [
        "1 review-level issue(s) were detected.",
        accepted
          ? "Accepted constraints were used for this dry run."
          : "1 repair-supported inferred constraint(s) remain pending.",
      ],
    },
    issues: [
      {
        column: "rating",
        issue_type: "decimal_shift",
        severity: "review",
        row_indices: [5],
        row_indices_truncated: false,
        count: 1,
      },
    ],
    // The untruncated per-cell channel the evidence surface is built from. Kept in
    // lockstep with `issues` above: the same cell, with the fields the grouping
    // destroys (confidence, actual, expected, reason).
    flagged_cells: {
      // rating is column index 4 in the hospital sample header.
      index: { column_indices: [4], rows: [5] },
      confidence_histogram: [
        {
          issue_type: "decimal_shift",
          bins: Array.from({ length: 10 }, (_, index) => ({
            from_value: index / 10,
            to_value: (index + 1) / 10,
            count: index === 8 ? 1 : 0,
          })),
          count: 1,
          distinct_values: 1,
          mode_value: 0.86,
          mode_share: 1,
        },
      ],
      cells: [
        {
          row: 5,
          column: "rating",
          issue_type: "decimal_shift",
          severity: "review",
          confidence: 0.86,
          actual: "45.0",
          expected: "4.5",
          reason: "Value 45 in column rating appears to be ~10x the typical value.",
        },
      ],
      total: 1,
      truncated: false,
      note: "All 1 flagged cells are individually listed.",
    },
    repairs: [
      {
        row: 5,
        column: "rating",
        old_value: "45.0",
        new_value: "4.5",
        detector_id: "decimal_shift",
        reason: "Value 45 in column rating appears to be ~10x the typical value.",
        confidence: 0.94,
        provenance: "heuristic",
        verifier_reason: "All proposed fixes passed the SMT verifier.",
        verification_strength: "proven",
      },
    ],
    verification: {
      safety_verdict: "allow",
      verifier_verdict: "accept",
      accepted_constraint_ids: accepted ? ["cnd-state-fd"] : [],
      failures: [
        {
          row: 2,
          column: "hospital_name",
          issue_type: "fd_violation",
          status: "attempted_not_fixed",
          reason: "No repair proposal was available for this issue.",
          attempt_count: 1,
          unsat_core: [],
        },
      ],
      abstentions: ["No repair proposal was available for this issue."],
      failure_reasons: ["No repair proposal was available for this issue."],
    },
    certificate: {
      ok: true,
      checks: [
        {
          name: "schema_recognized",
          ok: true,
          detail: "schema_version='repair_receipt_v1'",
        },
        {
          name: "data_identity",
          ok: true,
          detail: "sha256(data) matches source_sha256.",
        },
        {
          name: "auto_apply_is_proven_deterministic",
          ok: true,
          detail: "auto-applied set is proven (deterministic).",
        },
      ],
    },
    txn_journal: {
      txn_id: "txn-demo",
      created_at: "2026-05-20T12:00:00Z",
      source_name: "hospital_10rows.csv",
      source_sha256: sourceHash,
      fixes_count: 1,
      applied: false,
      events: [{ event_type: "created" }],
      note: "Playground is stateless.",
    },
    receipt: {
      schema_version: "repair_receipt_v1",
      receipt_version: "repair_receipt_v1",
      contract_version: "repair_contract_v2",
      mode: "dry_run",
      applied: false,
      reversible: true,
      source_sha256: sourceHash,
      post_sha256: null,
      txn_id: "txn-demo",
      safety_verdict: "allow",
      verifier_verdict: "accept",
      independent_verification: "not_run",
      issues_count: 1,
      fixes_count: 1,
      applied_fixes: [],
      suggested_fixes: [],
      candidate_provenance: ["heuristic"],
      root_causes: [
        {
          row: 5,
          column: "rating",
          issue_type: "decimal_shift",
          category: "decimal_shift",
          confidence: 0.94,
          reason: "Rating appears to be shifted one decimal place.",
        },
        {
          row: 2,
          column: "hospital_name",
          issue_type: "fd_violation",
          category: "fd_conflict",
          confidence: 0.9,
          reason: "FD conflict.",
        },
      ],
      candidate_repairs: [
        {
          row: 5,
          column: "rating",
          old_value: "45.0",
          new_value: "4.5",
          detector_id: "decimal_shift",
          operation: "update",
          reason: "Move decimal point one place left.",
          confidence: 0.94,
          provenance: "heuristic",
          verifier_reason: "accepted",
          verification_strength: "proven",
        },
      ],
      proof_obligations: [
        {
          obligation_id: "smt::decimal_shift::5::rating::attempt::1",
          verifier: "smt",
          status: "accepted",
          reason: "accepted",
          unsat_core: [],
        },
      ],
      accepted_constraint_ids: accepted ? ["cnd-state-fd"] : [],
      constraints_artifact_sha256: accepted ? "b".repeat(64) : null,
      patch_plan_sha256: "c".repeat(64),
      revert_command: "dataforge revert txn-demo",
      limitations: ["Dry run only; no source data was mutated."],
      reason: "Dry run completed without mutating the source file.",
    },
    apply_handoff: {
      source_name: "hospital_10rows.csv",
      dry_run_command: accepted
        ? "dataforge repair path/to/hospital_10rows.csv --constraints constraints.json --dry-run"
        : "dataforge repair path/to/hospital_10rows.csv --dry-run",
      apply_command: accepted
        ? "dataforge repair path/to/hospital_10rows.csv --constraints constraints.json --apply"
        : "dataforge repair path/to/hospital_10rows.csv --apply",
      audit_command: "dataforge audit txn-demo",
      revert_command: "dataforge revert txn-demo",
      note: "The hosted playground never mutates uploads.",
    },
    limitations: [
      "Hosted analysis is stateless and dry-run only.",
      "Inferred constraints are pending unless explicitly accepted for this run.",
    ],
    meta: {
      api_version: "0.1.0",
      contract_version: "repair_contract_v2",
    },
  };
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
  await expect(page.getByLabel("Dataset intake").getByText("45.0")).toBeVisible();

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
  await expect(page).toHaveURL(/\/playground\/run$/);

  await page.locator('.product-nav a[href="/playground/atlas"]').click();
  const reviewQueue = page.getByLabel("Human review queue");
  await reviewQueue.getByRole("checkbox", { name: /functional_dependency constraint cnd-state-fd/ }).check();
  await reviewQueue.getByRole("button", { name: "Rerun with accepted constraints" }).click();
  await page.locator('.product-nav a[href="/playground/evidence"]').click();
  await expect(page).toHaveURL(/\/playground\/evidence$/);
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
  await expect(page.getByText("45.0")).toBeVisible();

  await page.locator("#csv-upload").setInputFiles({
    name: "broken.csv",
    mimeType: "text/csv",
    buffer: Buffer.from('id,name\n1,"unterminated'),
  });
  await expect(page.getByRole("alert")).toContainText("Dataset validation failed");
  await expect(page.getByText("45.0")).toBeVisible();

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
  await expect(page).toHaveURL(/\/playground\/run$/);
  await page.locator('.product-nav a[href="/playground/system"]').click();
  await expect(page).toHaveURL(/\/playground\/system$/);
  await page.goBack();
  await expect(page).toHaveURL(/\/playground\/run$/);
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
