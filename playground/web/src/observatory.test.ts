import { describe, expect, it } from "vitest";
import type { AnalyzeResponse } from "./types";
import { createWorkflowState, workflowReducer } from "./workflow";
import {
  buildEvidenceGroups,
  buildObservatoryView,
  buildReviewQueue,
  buildTrustVerdict,
  humanizeReviewReason,
  stageToProofNode,
  strengthOf,
} from "./observatory";

function analysisFixture(): AnalyzeResponse {
  const sourceHash = "a".repeat(64);
  return {
    source: {
      name: "sample.csv",
      size_bytes: 20,
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
          evidence: "id determines state.",
          decision: "pending",
          repair_supported: true,
        },
      ],
    },
    risk_summary: {
      dataset_level: "high",
      repair_readiness: "partial",
      severity_counts: { safe: 0, review: 1, unsafe: 1 },
      pending_repair_supported_constraints: 1,
      reasons: ["Unsafe issue requires review."],
    },
    issues: [
      {
        column: "state",
        issue_type: "fd_violation",
        severity: "unsafe",
        row_indices: [1],
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
        reason: "Tenfold outlier.",
        confidence: 0.91,
        provenance: "heuristic",
        verifier_reason: "accepted",
      },
    ],
    verification: {
      safety_verdict: "allow",
      verifier_verdict: "accept",
      accepted_constraint_ids: [],
      failures: [
        {
          row: 1,
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
    certificate: {
      ok: true,
      checks: [
        {
          name: "schema_recognized",
          ok: true,
          detail: "schema_version='repair_receipt_v1'",
        },
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
      created_at: "2026-06-02T00:00:00Z",
      source_name: "sample.csv",
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
      issues_count: 2,
      fixes_count: 1,
      candidate_provenance: ["heuristic"],
      root_causes: [],
      candidate_repairs: [],
      proof_obligations: [
        {
          obligation_id: "smt::1",
          verifier: "smt",
          status: "accepted",
          reason: "accepted",
          unsat_core: [],
        },
      ],
      accepted_constraint_ids: [],
      constraints_artifact_sha256: null,
      patch_plan_sha256: "b".repeat(64),
      revert_command: "dataforge revert txn-demo",
      limitations: ["Dry run only."],
      reason: "Dry run completed without mutating the source file.",
    },
    apply_handoff: {
      source_name: "sample.csv",
      dry_run_command: "dataforge repair path/to/sample.csv --dry-run",
      apply_command: "dataforge repair path/to/sample.csv --apply",
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
}

describe("observatory view model", () => {
  it("maps workflow stages to proof nodes with semantic tones", () => {
    const running = workflowReducer(createWorkflowState(), { type: "start" });
    const intake = stageToProofNode({ ...running.stages[0], status: "running", confidence: 0.91 });

    expect(intake.id).toBe("intake");
    expect(intake.tone).toBe("active");
    expect(intake.confidence).toBe("91%");
    expect(intake.statusLabel).toBe("running");
  });

  it("derives a human review queue from constraints, failures, abstentions, and limits", () => {
    const analysis = analysisFixture();
    const queue = buildReviewQueue(analysis, ["cnd-state-fd"]);

    expect(queue.map((item) => item.kind)).toEqual([
      "constraint",
      "failure",
      "abstention",
      "limitation",
    ]);
    expect(queue[0]).toMatchObject({ tone: "verified", meta: "accepted for rerun" });
    expect(queue[1]).toMatchObject({ tone: "danger" });
  });

  it("builds evidence groups and run posture for completed analysis", () => {
    const analysis = analysisFixture();
    const workflow = workflowReducer(createWorkflowState(), { type: "analysis", analysis });
    const view = buildObservatoryView({
      analysis,
      dataset: null,
      workflow,
      selectedConstraintIds: [],
    });

    expect(buildEvidenceGroups(analysis, null).map((group) => group.id)).toEqual([
      "source",
      "issues",
      "constraints",
      "repairs",
      "proof",
      "receipt",
    ]);
    expect(view.runPosture.title).toBe("high risk");
    expect(view.proofNodes).toHaveLength(9);
  });
});

describe("trust verdict", () => {
  it("returns a pending verdict with no analysis", () => {
    const verdict = buildTrustVerdict(null);
    expect(verdict.level).toBe("pending");
    expect(verdict.certificate.total).toBe(0);
    expect(verdict.independentVerification).toBe("not_run");
  });

  it("classifies a proven deterministic run and reports certificate coverage", () => {
    const analysis = analysisFixture();
    analysis.repairs = analysis.repairs.map((fix) => ({
      ...fix,
      verification_strength: "proven",
    }));
    const verdict = buildTrustVerdict(analysis);

    expect(verdict.provenCount).toBe(1);
    expect(verdict.plausibilityCount).toBe(0);
    expect(verdict.certificate).toEqual({ ok: true, passed: 3, total: 3 });
    // Honest: fixtures leave independent verification not run.
    expect(verdict.independentVerification).toBe("not_run");
    expect(verdict.guaranteeLine).toContain("No unproven change");
  });

  it("classifies a held run when proposals are only suggested, not applied", () => {
    const analysis = analysisFixture();
    analysis.repairs = [];
    analysis.receipt.suggested_fixes = [
      {
        row: 1,
        column: "state",
        old_value: "NY",
        new_value: "New York",
        detector_id: "categorical_normalization",
        operation: "normalize",
        reason: "candidate",
        confidence: 0.6,
        provenance: "llm_live",
        verifier_reason: "held",
        verification_strength: "plausibility_only",
        review_reason: "failed_conformal_threshold",
      },
    ];
    const verdict = buildTrustVerdict(analysis);

    expect(verdict.level).toBe("held");
    expect(verdict.heldCount).toBe(1);
    expect(verdict.provenCount).toBe(0);
  });

  it("classifies a mixed run and warns when a plausibility-only value would apply", () => {
    const analysis = analysisFixture();
    analysis.repairs = analysis.repairs.map((fix) => ({
      ...fix,
      provenance: "llm_live",
      verification_strength: "plausibility_only",
    }));
    const verdict = buildTrustVerdict(analysis);

    expect(verdict.level).toBe("mixed");
    expect(verdict.plausibilityCount).toBe(1);
    expect(verdict.guaranteeLine).toContain("not auto-applied");
  });

  it("falls back to provenance when verification_strength is absent", () => {
    expect(
      strengthOf({
        row: 0,
        column: "c",
        old_value: "",
        new_value: "x",
        detector_id: "d",
        reason: "r",
        confidence: 1,
        provenance: "deterministic",
      }),
    ).toBe("proven");
    expect(
      strengthOf({
        row: 0,
        column: "c",
        old_value: "",
        new_value: "x",
        detector_id: "d",
        reason: "r",
        confidence: 1,
        provenance: "llm_live",
      }),
    ).toBe("plausibility_only");
  });

  it("humanizes known review reasons and falls back gracefully", () => {
    expect(humanizeReviewReason("not_inferable_from_data")).toContain("not derivable");
    expect(humanizeReviewReason(null)).toContain("Held for review");
    expect(humanizeReviewReason("some_new_reason")).toBe("some new reason");
  });
});
