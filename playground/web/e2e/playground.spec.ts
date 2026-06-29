import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

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
      issues_count: 1,
      fixes_count: 1,
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

test.beforeEach(async ({ page }) => {
  await page.route("**/api/health", async (route) => {
    await route.fulfill({
      json: {
        status: "ok",
        advanced_available: false,
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
  await expect(page.getByRole("cell", { name: "accepted" })).toBeVisible();

  await page.locator('.product-nav a[href="/playground/repairs"]').click();
  const repairsPanel = page.locator(".repairs-lens");
  await expect(repairsPanel.getByText("Value 45 in column rating appears to be ~10x the typical value.")).toBeVisible();
  await expect(repairsPanel.getByText("All proposed fixes passed the SMT verifier.")).toBeVisible();
  await expect(repairsPanel.getByText("Attempted but not fixed")).toBeVisible();

  await page.locator('.product-nav a[href="/playground/receipt"]').click();
  const receiptPanel = page.locator(".receipt-lens");
  await expect(receiptPanel.getByText("txn-demo", { exact: true })).toBeVisible();
  await expect(receiptPanel.getByLabel("Repair receipt summary").getByText("Accepted constraints")).toBeVisible();
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
