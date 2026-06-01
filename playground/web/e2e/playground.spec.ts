import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const sampleCsv = "id,amount,state\n1,100,AL\n2,1020,AX\n3,105,AL\n";
const sourceHash = "a".repeat(64);

function analyzePayload(accepted = false) {
  return {
    source: {
      name: "hospital_10rows.csv",
      size_bytes: sampleCsv.length,
      sha256: sourceHash,
      rows: 3,
      columns: 3,
      column_names: ["id", "amount", "state"],
    },
    schema_inference: {
      schema_version: "constraint_review_v1",
      source_sha256: sourceHash,
      row_count: 3,
      candidates: [
        {
          candidate_id: "cnd-state-fd",
          kind: "functional_dependency",
          columns: ["id"],
          dependent: "state",
          inferred_type: null,
          pattern: null,
          min_value: null,
          max_value: null,
          confidence: 0.92,
          evidence: "id determines state in 3/3 rows.",
          decision: accepted ? "accepted" : "pending",
          repair_supported: true,
        },
        {
          candidate_id: "cnd-amount-regex",
          kind: "regex",
          columns: ["amount"],
          dependent: null,
          inferred_type: null,
          pattern: "^\\d+$",
          min_value: null,
          max_value: null,
          confidence: 1,
          evidence: "3 non-empty values matched ^\\d+$.",
          decision: "pending",
          repair_supported: false,
        },
      ],
    },
    risk_summary: {
      dataset_level: "high",
      repair_readiness: "partial",
      severity_counts: { safe: 0, review: 1, unsafe: 1 },
      pending_repair_supported_constraints: accepted ? 0 : 1,
      reasons: [
        "1 unsafe issue(s) require review.",
        "1 review-level issue(s) were detected.",
        accepted
          ? "Accepted constraints were used for this dry run."
          : "1 repair-supported inferred constraint(s) remain pending.",
      ],
    },
    issues: [
      {
        column: "state",
        issue_type: "fd_violation",
        severity: "unsafe",
        row_indices: [1],
        row_indices_truncated: false,
        count: 1,
      },
      {
        column: "amount",
        issue_type: "decimal_shift",
        severity: "review",
        row_indices: [2],
        row_indices_truncated: false,
        count: 1,
      },
    ],
    repairs: [
      {
        row: 2,
        column: "amount",
        old_value: "1020",
        new_value: "102",
        detector_id: "decimal_shift",
        reason: "Tenfold outlier relative to neighboring rows.",
        confidence: 0.91,
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
          row: 1,
          column: "state",
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
      contract_version: "repair_contract_v2",
      mode: "dry_run",
      applied: false,
      reversible: true,
      source_sha256: sourceHash,
      post_sha256: null,
      txn_id: "txn-demo",
      safety_verdict: "allow",
      verifier_verdict: "accept",
      issues_count: 2,
      fixes_count: 1,
      candidate_provenance: ["heuristic"],
      root_causes: [
        {
          row: 1,
          column: "state",
          issue_type: "fd_violation",
          category: "fd_conflict",
          confidence: 0.9,
          reason: "FD conflict.",
        },
      ],
      candidate_repairs: [
        {
          row: 1,
          column: "state",
          old_value: "Californa",
          new_value: "California",
          detector_id: "fd_violation",
          operation: "update",
          reason: "Repair.",
          confidence: 0.9,
          provenance: "heuristic",
          verifier_reason: "accepted",
        },
      ],
      proof_obligations: [
        {
          obligation_id: "smt::fd_violation::1::state::attempt::1",
          verifier: "smt",
          status: "accepted",
          reason: "accepted",
          unsat_core: [],
        },
      ],
      accepted_constraint_ids: accepted ? ["cnd-state-fd"] : [],
      constraints_artifact_sha256: accepted ? "b".repeat(64) : null,
      patch_plan_sha256: "c".repeat(64),
      revert_command: "dataforge15 revert txn-demo",
      limitations: ["Dry run only; no source data was mutated."],
      reason: "Dry run completed without mutating the source file.",
    },
    apply_handoff: {
      source_name: "hospital_10rows.csv",
      dry_run_command: accepted
        ? "dataforge15 repair path/to/hospital_10rows.csv --constraints constraints.json --dry-run"
        : "dataforge15 repair path/to/hospital_10rows.csv --dry-run",
      apply_command: accepted
        ? "dataforge15 repair path/to/hospital_10rows.csv --constraints constraints.json --apply"
        : "dataforge15 repair path/to/hospital_10rows.csv --apply",
      audit_command: "dataforge15 audit txn-demo",
      revert_command: "dataforge15 revert txn-demo",
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

test.beforeEach(async ({ page }) => {
  await page.route("**/api/health", async (route) => {
    await route.fulfill({
      json: {
        status: "ok",
        advanced_available: false,
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
  await page.route("**/api/analyze**", async (route) => {
    const posted = route.request().postData() ?? "";
    await route.fulfill({ json: analyzePayload(posted.includes("cnd-state-fd")) });
  });
});

test("sample path analyzes, accepts constraints, exports evidence, and passes accessibility", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-write"]);
  await page.goto("/");

  await expect(page.getByRole("banner", { name: "DataForge command bar" })).toBeVisible();
  await expect(page.getByText("Stateless dry run")).toBeVisible();
  await page.getByRole("button", { name: /Hospital/ }).click();
  await expect(page.getByRole("heading", { name: "Current CSV" })).toBeVisible();
  await expect(page.getByText("1020")).toBeVisible();

  await page.getByRole("button", { name: "Analyze" }).click();
  await expect(page.getByRole("cell", { name: "fd_violation" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Constraint review" })).toBeVisible();

  await page.getByRole("checkbox", { name: /functional_dependency constraint cnd-state-fd/ }).check();
  await page.getByRole("button", { name: "Rerun with accepted constraints" }).click();
  await expect(page.getByText("accepted", { exact: true })).toBeVisible();

  await page.getByRole("tab", { name: "Repairs" }).click();
  await expect(page.getByText("Tenfold outlier")).toBeVisible();
  await expect(page.getByText("All proposed fixes passed the SMT verifier.")).toBeVisible();
  await expect(page.getByText("Attempted but not fixed")).toBeVisible();

  const repairsTab = page.getByRole("tab", { name: "Repairs" });
  await repairsTab.focus();
  await repairsTab.press("ArrowRight");
  await expect(page.getByText("txn-demo", { exact: true })).toBeVisible();
  const receiptPanel = page.locator("#panel-receipt");
  await expect(receiptPanel.getByText("Accepted constraints")).toBeVisible();
  await expect(receiptPanel).toContainText("constraints.json");

  await page.getByRole("button", { name: "Copy" }).click();
  await expect(page.getByRole("button", { name: "Copied" })).toBeVisible();

  const download = page.waitForEvent("download");
  await page.getByRole("button", { name: "Export" }).click();
  await expect((await download).suggestedFilename()).toContain("dataforge-dry-run");

  const scan = await new AxeBuilder({ page }).analyze();
  expect(scan.violations).toEqual([]);
});

test("uploaded CSV path validates and analyzes without samples", async ({ page }) => {
  await page.goto("/");

  await page
    .locator("#csv-upload")
    .setInputFiles({ name: "upload.csv", mimeType: "text/csv", buffer: Buffer.from(sampleCsv) });

  await expect(page.getByText("upload.csv")).toBeVisible();
  await page.getByRole("button", { name: "Analyze" }).click();
  await expect(page.getByRole("cell", { name: "decimal_shift" })).toBeVisible();
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
  await page.goto("/");

  await page
    .locator("#csv-upload")
    .setInputFiles({ name: "upload.csv", mimeType: "text/csv", buffer: Buffer.from(sampleCsv) });
  await expect(page.getByText("1020")).toBeVisible();

  await page.locator("#csv-upload").setInputFiles({
    name: "broken.csv",
    mimeType: "text/csv",
    buffer: Buffer.from('id,name\n1,"unterminated'),
  });
  await expect(page.getByRole("alert")).toContainText("Dataset validation failed");
  await expect(page.getByText("1020")).toBeVisible();

  await page.getByRole("button", { name: "Analyze" }).click();
  await page.getByRole("button", { name: "Copy" }).click();
  await expect(page.getByRole("button", { name: "Copy failed" })).toBeVisible();
  await expect(page.getByLabel("Copyable repair evidence")).toHaveValue(/transaction_journal/);
});

test("client rejects files above the health capability limit", async ({ page }) => {
  await page.goto("/");

  await page.locator("#csv-upload").setInputFiles({
    name: "big.csv",
    mimeType: "text/csv",
    buffer: Buffer.from(`id\n${"x".repeat(1_048_577)}`),
  });

  await expect(page.getByRole("alert")).toContainText("larger than the hosted playground limit");
});

test("tabs support arrow-key navigation", async ({ page }) => {
  await page.goto("/");

  const riskTab = page.getByRole("tab", { name: "Risk" });
  await riskTab.focus();
  await riskTab.press("ArrowRight");

  await expect(page.getByRole("tab", { name: "Repairs" })).toHaveAttribute("aria-selected", "true");
});

for (const colorScheme of ["light", "dark"] as const) {
  test(`premium institutional console supports the full sample flow in ${colorScheme} mode`, async ({
    page,
    context,
  }) => {
    await context.grantPermissions(["clipboard-write"]);
    await page.emulateMedia({ colorScheme });
    await page.goto("/");

    const rootTokens = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return {
        bg: styles.getPropertyValue("--df-bg").trim(),
        text: styles.getPropertyValue("--df-text-1").trim(),
        action: styles.getPropertyValue("--df-action-bg").trim(),
        success: styles.getPropertyValue("--df-status-safe-bg").trim(),
        agent: styles.getPropertyValue("--df-agent-bg").trim(),
      };
    });
    expect(rootTokens.bg).not.toEqual("");
    expect(rootTokens.text).not.toEqual("");
    expect(rootTokens.action).not.toEqual("");
    expect(rootTokens.action).not.toEqual(rootTokens.success);
    expect(rootTokens.agent).not.toEqual("");
    await expect(page.getByRole("banner", { name: "DataForge command bar" })).toBeVisible();
    await expect(page.getByText("Verified CSV repair workbench")).toBeVisible();
    await expect(page.getByText("Stateless dry run")).toBeVisible();

    await page.getByRole("button", { name: /Hospital/ }).click();
    await page.getByRole("button", { name: "Analyze" }).click();
    await expect(page.getByText("unsafe", { exact: true })).toBeVisible();

    await page.getByRole("tab", { name: "Repairs" }).click();
    await expect(page.getByText("Tenfold outlier")).toBeVisible();
    await expect(page.getByText("Verified dry-run evidence")).toBeVisible();

    const layout = await page.evaluate(() => ({
      scrollWidth: document.documentElement.scrollWidth,
      innerWidth: window.innerWidth,
    }));
    expect(layout.scrollWidth).toBeLessThanOrEqual(layout.innerWidth + 1);

    await page.getByRole("tab", { name: "Receipt" }).click();
    await expect(page.getByText("txn-demo", { exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Copy" }).click();
    await expect(page.getByRole("button", { name: "Copied" })).toBeVisible();

    const download = page.waitForEvent("download");
    await page.getByRole("button", { name: "Export" }).click();
    await expect((await download).suggestedFilename()).toContain("dataforge-dry-run");

    const scan = await new AxeBuilder({ page }).analyze();
    expect(scan.violations).toEqual([]);
  });
}
