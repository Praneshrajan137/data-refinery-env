import { describe, expect, it } from "vitest";
import {
  DEFAULT_MAX_UPLOAD_BYTES,
  buildEvidenceExport,
  groupIssues,
  parseCsvPreview,
  validateCsvFile,
} from "./csv";
import type { AnalyzeResponse } from "./types";

describe("CSV validation and preview", () => {
  it("accepts CSV files within the backend cap", () => {
    const file = new File(["id,amount\n1,100"], "sample.csv", { type: "text/csv" });

    expect(validateCsvFile(file)).toEqual({ ok: true });
  });

  it("rejects empty, non-CSV, and oversize files", () => {
    expect(validateCsvFile(new File([""], "sample.csv", { type: "text/csv" })).ok).toBe(false);
    expect(validateCsvFile(new File(["x"], "sample.txt", { type: "text/plain" })).ok).toBe(true);
    expect(
      validateCsvFile(
        new File(["x".repeat(DEFAULT_MAX_UPLOAD_BYTES + 1)], "big.csv", {
          type: "text/csv",
        }),
      ).ok,
    ).toBe(false);
  });

  it("parses a stable preview with headers and rows", () => {
    const preview = parseCsvPreview('id,amount,note\n1,100,"ok"\n2,1020,"needs review"');

    expect(preview.columns).toEqual(["id", "amount", "note"]);
    expect(preview.rows).toHaveLength(2);
    expect(preview.rows[1].amount).toBe("1020");
  });

  it("rejects header-only CSV snippets", () => {
    expect(() => parseCsvPreview("id,amount\n")).toThrow(/at least one data row/i);
  });
});

describe("result shaping", () => {
  it("groups issues by severity, count, and column", () => {
    const groups = groupIssues([
      { column: "amount", issue_type: "decimal_shift", severity: "review", row_indices: [2], count: 1 },
      { column: "state", issue_type: "fd_violation", severity: "unsafe", row_indices: [4, 5], count: 2 },
      { column: "name", issue_type: "type_mismatch", severity: "safe", row_indices: [1], count: 1 },
    ]);

    expect(groups.map((group) => group.column)).toEqual(["state", "amount", "name"]);
  });

  it("exports repair evidence as deterministic JSON", () => {
    const analysis: AnalyzeResponse = {
      source: {
        name: "sample.csv",
        size_bytes: 16,
        sha256: "a".repeat(64),
        rows: 2,
        columns: 2,
        column_names: ["id", "amount"],
      },
      schema_inference: {
        schema_version: "constraint_review_v1",
        source_sha256: "a".repeat(64),
        row_count: 2,
        candidates: [],
      },
      risk_summary: {
        dataset_level: "none",
        repair_readiness: "no_action",
        severity_counts: { safe: 0, review: 0, unsafe: 0 },
        pending_repair_supported_constraints: 0,
        reasons: ["No current detector findings were reported for this CSV."],
      },
      flagged_cells: { index: { column_indices: [], rows: [] }, confidence_histogram: [], cells: [], total: 0, truncated: false, note: "No cells were flagged. This is a measured result, not a missing one." },
      issues: [],
      repairs: [
        {
          row: 5,
          column: "rating",
          old_value: "45.0",
          new_value: "4.5",
          detector_id: "decimal_shift",
          reason: "Value 45 in column rating appears to be ~10x the typical value.",
          confidence: 0.94,
          provenance: "deterministic",
          verifier_reason: "All proposed fixes passed structural verification.",
        },
      ],
      verification: {
        safety_verdict: "allow",
        verifier_verdict: "accept",
        accepted_constraint_ids: [],
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
      txn_journal: {
        txn_id: "txn-demo",
        created_at: "2026-05-20T12:00:00Z",
        source_name: "sample.csv",
        source_sha256: "a".repeat(64),
        fixes_count: 1,
        applied: false,
        events: [{ event_type: "created" }],
        note: "Dry run.",
      },
      receipt: {
        schema_version: "repair_receipt_v1",
        receipt_version: "repair_receipt_v1",
        contract_version: "repair_contract_v2",
        mode: "dry_run",
        applied: false,
        reversible: true,
        source_sha256: "a".repeat(64),
        post_sha256: null,
        txn_id: "txn-demo",
        safety_verdict: "allow",
        verifier_verdict: "accept",
        issues_count: 0,
        fixes_count: 1,
        candidate_provenance: [],
        root_causes: [],
        candidate_repairs: [],
        proof_obligations: [],
        accepted_constraint_ids: [],
        constraints_artifact_sha256: null,
        patch_plan_sha256: "b".repeat(64),
        revert_command: "dataforge revert txn-demo",
        limitations: ["Hosted analysis is stateless and dry-run only."],
        reason: "Dry run completed without mutating the source file.",
      },
      apply_handoff: {
        source_name: "sample.csv",
        dry_run_command: "dataforge repair path/to/sample.csv --dry-run",
        apply_command: "dataforge repair path/to/sample.csv --apply",
        audit_command: "dataforge audit txn-demo",
        revert_command: "dataforge revert txn-demo",
        note: "Local CLI only.",
      },
      limitations: ["Hosted analysis is stateless and dry-run only."],
      meta: {
        api_version: "0.1.0",
        contract_version: "repair_contract_v2",
      },
    };

    const payload = JSON.parse(buildEvidenceExport("sample.csv", analysis));

    expect(payload).toMatchObject({
      product: "DataForge Playground",
      schema_version: "dataforge_playground_receipt_v2",
      dataset_name: "sample.csv",
      dry_run: true,
      contract_version: "repair_contract_v2",
      primary_repair_note: "Row 6 rating: 45.0 -> 4.5 passed safety and verifier gates.",
      local_commands: {
        apply: "dataforge repair path/to/sample.csv --apply",
        revert: "dataforge revert txn-demo",
      },
      hashes: {
        source_sha256: "a".repeat(64),
        patch_plan_sha256: "b".repeat(64),
      },
      source: { sha256: "a".repeat(64) },
      repair_receipt: { txn_id: "txn-demo" },
      raw_receipt: { txn_id: "txn-demo" },
    });
    expect(payload.safety_revert_explanation.join(" ")).toContain("post-state hash");

    // The artifact describes its own currency.
    //
    // A banner cannot travel with a downloaded file. After a failed attempt the UI still
    // hands over the PREVIOUS run's receipt -- which is internally honest, since it carries
    // that run's own source hash -- so the fact that something newer was attempted has to be
    // recorded inside the payload rather than only on screen.
    expect(payload.superseded_by_later_attempt).toBe(false);

    const superseded = JSON.parse(buildEvidenceExport("sample.csv", analysis, true));
    expect(superseded.superseded_by_later_attempt).toBe(true);
    // Being superseded must not strip or alter the evidence for the run it does describe.
    expect(superseded.hashes).toEqual(payload.hashes);
    expect(superseded.schema_version).toBe(payload.schema_version);
  });
});
