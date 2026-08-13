import { describe, expect, it } from "vitest";
import {
  SAFETY_REVERT_EXPLANATION,
  localCommands,
  selectPrimaryRepairMoment,
} from "./productLoop";
import type { AnalyzeResponse } from "./types";

function analysisFixture(overrides: Partial<AnalyzeResponse> = {}): AnalyzeResponse {
  const sourceHash = "a".repeat(64);
  const base: AnalyzeResponse = {
    source: {
      name: "hospital_10rows.csv",
      size_bytes: 420,
      sha256: sourceHash,
      rows: 10,
      columns: 10,
      column_names: ["provider_number", "hospital_name", "rating"],
    },
    schema_inference: {
      schema_version: "constraint_review_v1",
      source_sha256: sourceHash,
      row_count: 10,
      candidates: [],
    },
    risk_summary: {
      dataset_level: "medium",
      repair_readiness: "verified",
      severity_counts: { safe: 0, review: 1, unsafe: 0 },
      pending_repair_supported_constraints: 0,
      reasons: ["1 review-level issue was detected."],
    },
    flagged_cells: { index: { column_indices: [], rows: [] }, confidence_histogram: [], cells: [], total: 0, truncated: false, note: "No cells were flagged. This is a measured result, not a missing one." },
    issues: [
      {
        column: "rating",
        issue_type: "decimal_shift",
        severity: "review",
        row_indices: [5],
        count: 1,
      },
    ],
    repairs: [
      {
        row: 4,
        column: "phone_number",
        old_value: "not available",
        new_value: "",
        detector_id: "type_mismatch",
        reason: "Normalize sentinel value.",
        confidence: 0.9,
        provenance: "deterministic",
        verifier_reason: "All proposed fixes passed structural verification.",
      },
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
      created_at: "2026-06-13T00:00:00Z",
      source_name: "hospital_10rows.csv",
      source_sha256: sourceHash,
      fixes_count: 2,
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
      fixes_count: 2,
      candidate_provenance: ["deterministic"],
      root_causes: [],
      candidate_repairs: [],
      proof_obligations: [],
      accepted_constraint_ids: [],
      constraints_artifact_sha256: null,
      patch_plan_sha256: "b".repeat(64),
      revert_command: "dataforge revert txn-demo",
      limitations: ["Dry run only; no source data was mutated."],
      reason: "Dry run completed without mutating the source file.",
    },
    apply_handoff: {
      source_name: "hospital_10rows.csv",
      dry_run_command: "dataforge repair path/to/hospital_10rows.csv --dry-run",
      apply_command: "dataforge repair path/to/hospital_10rows.csv --apply",
      audit_command: "dataforge audit txn-demo",
      revert_command: "dataforge revert txn-demo",
      note: "The hosted playground never mutates uploads.",
    },
    limitations: ["Hosted analysis is stateless and dry-run only."],
    meta: {
      api_version: "0.1.0",
      contract_version: "repair_contract_v2",
    },
  };
  return { ...base, ...overrides };
}

describe("primary repair moment", () => {
  it("prefers the Hospital rating decimal-shift story over earlier repairs", () => {
    const moment = selectPrimaryRepairMoment(analysisFixture());

    expect(moment).toMatchObject({
      kind: "verified",
      humanRow: 6,
      column: "rating",
      oldValue: "45.0",
      newValue: "4.5",
      detectorId: "decimal_shift",
      safetyVerdict: "allow",
      verifierVerdict: "accept",
      txnId: "txn-demo",
    });
    expect(moment.note).toContain("Row 6 rating: 45.0 -> 4.5");
  });

  it("surfaces the strongest abstention when no verified fix exists", () => {
    const analysis = analysisFixture({
      repairs: [],
      verification: {
        safety_verdict: "allow",
        verifier_verdict: "not_run",
        accepted_constraint_ids: [],
        failures: [
          {
            row: 2,
            column: "state",
            issue_type: "fd_violation",
            status: "attempted_not_fixed",
            reason: "No repair proposal was available.",
            attempt_count: 1,
            unsat_core: [],
          },
        ],
        abstentions: ["No repair proposal was available."],
        failure_reasons: ["No repair proposal was available."],
      },
    });

    expect(selectPrimaryRepairMoment(analysis)).toMatchObject({
      kind: "abstention",
      humanRow: 3,
      column: "state",
      issueType: "fd_violation",
    });
  });

  it("keeps local command names and safety explanation explicit", () => {
    const analysis = analysisFixture();

    expect(localCommands(analysis)).toEqual({
      dry_run: "dataforge repair path/to/hospital_10rows.csv --dry-run",
      apply: "dataforge repair path/to/hospital_10rows.csv --apply",
      audit: "dataforge audit txn-demo",
      revert: "dataforge revert txn-demo",
    });
    expect(SAFETY_REVERT_EXPLANATION.join(" ")).toContain("hash-chained transaction");
    expect(SAFETY_REVERT_EXPLANATION.join(" ")).toContain("post-state hash");
  });
});
