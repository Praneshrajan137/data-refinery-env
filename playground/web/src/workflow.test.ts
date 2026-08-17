import { describe, expect, it } from "vitest";
import type { AnalyzeResponse, ProblemDetail, WorkflowEvent } from "./types";
import { createWorkflowState, synthesizeWorkflowEvents, workflowReducer } from "./workflow";

function analysisFixture(): AnalyzeResponse {
  const sourceHash = "a".repeat(64);
  return {
    source: {
      name: "sample.csv",
      size_bytes: 18,
      sha256: sourceHash,
      rows: 2,
      columns: 2,
      column_names: ["id", "state"],
    },
    schema_inference: {
      schema_version: "constraint_review_v1",
      source_sha256: sourceHash,
      row_count: 2,
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
      severity_counts: { safe: 0, review: 0, unsafe: 1 },
      pending_repair_supported_constraints: 1,
      reasons: ["Unsafe issue requires review."],
    },
    flagged_cells: { index: { column_indices: [], rows: [] }, confidence_histogram: [], cells: [], total: 0, truncated: false, note: "No cells were flagged. This is a measured result, not a missing one." },
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
        row: 1,
        column: "state",
        old_value: "Californa",
        new_value: "California",
        detector_id: "fd_violation",
        reason: "Candidate from accepted functional dependency.",
        confidence: 0.91,
        provenance: "deterministic",
        verifier_reason: "accepted",
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
      issues_count: 1,
      fixes_count: 1,
      candidate_provenance: ["deterministic"],
      root_causes: [],
      candidate_repairs: [],
      proof_obligations: [],
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

describe("workflow reducer", () => {
  it("tracks streamed events and exposes the final analysis", () => {
    const analysis = analysisFixture();
    const event: WorkflowEvent = {
      schema_version: "workflow_event_v1",
      run_id: "run-1",
      sequence: 10,
      stage_id: "receipt",
      status: "completed",
      summary: "Done.",
      started_at: "2026-06-02T00:00:00Z",
      completed_at: "2026-06-02T00:00:01Z",
      counts: { fixes: 1 },
      requires_human: false,
      analysis,
    };

    const running = workflowReducer(createWorkflowState(), { type: "start" });
    const ready = workflowReducer(running, { type: "event", event });

    expect(ready.status).toBe("ready");
    expect(ready.runId).toBe("run-1");
    expect(ready.lastAnalysis).toBe(analysis);
    expect(ready.stages.find((stage) => stage.id === "receipt")?.status).toBe("completed");
  });

  it("synthesizes fallback workflow events from JSON analyze responses", () => {
    const analysis = analysisFixture();
    const actions = synthesizeWorkflowEvents(analysis);
    const state = workflowReducer(createWorkflowState(), { type: "analysis", analysis });

    expect(actions).toHaveLength(9);
    expect(actions.map((action) => (action.type === "event" ? action.event.stage_id : ""))).toEqual([
      "intake",
      "schema_inference",
      "constraint_review",
      "detectors",
      "repair_candidates",
      "safety_gate",
      "smt_verifier",
      "dry_run_transaction",
      "receipt",
    ]);
    expect(state.status).toBe("ready");
    expect(state.lastAnalysis?.receipt.txn_id).toBe("txn-demo");
  });

  it("keeps prior results when a run is cancelled or fails", () => {
    const analysis = analysisFixture();
    const ready = workflowReducer(createWorkflowState(), { type: "analysis", analysis });
    const running = workflowReducer(ready, { type: "start" });
    const cancelled = workflowReducer(running, { type: "cancel" });
    const problem: ProblemDetail = {
      type: "https://dataforge.local/problems/request_timeout",
      title: "Request Timeout",
      status: 504,
      detail: "Timed out.",
      error: "request_timeout",
    };
    const failed = workflowReducer(ready, { type: "problem", problem });

    expect(cancelled.status).toBe("cancelled");
    expect(cancelled.lastAnalysis).toBe(analysis);
    expect(failed.status).toBe("error");
    expect(failed.problem).toBe(problem);
    expect(failed.lastAnalysis).toBe(analysis);
  });
});

describe("stale results are labelled, not silently reused", () => {
  const analysis = analysisFixture();

  // Seeded through an event rather than the { type: "analysis" } action, because that action
  // runs the full JSON-fallback synthesiser; this suite is about staleness, not synthesis.
  function receiptEvent(sequence: number): WorkflowEvent {
    return {
      schema_version: "workflow_event_v1",
      run_id: "run-stale",
      sequence,
      stage_id: "receipt",
      status: "completed",
      summary: "Receipt ready.",
      started_at: "2026-06-02T00:00:00Z",
      completed_at: "2026-06-02T00:00:01Z",
      counts: {},
      requires_human: false,
      analysis,
    };
  }

  function withAnalysis() {
    return workflowReducer(createWorkflowState(), { type: "event", event: receiptEvent(1) });
  }

  it("does not mark a fresh result stale", () => {
    expect(withAnalysis().staleAnalysis).toBe(false);
    expect(withAnalysis().lastAnalysis).not.toBeNull();
  });

  it("marks the previous result stale when a run fails", () => {
    const failed = workflowReducer(withAnalysis(), {
      type: "problem",
      problem: { type: "about:blank", title: "t", status: 500, detail: "d", error: "e" },
    });
    expect(failed.staleAnalysis).toBe(true);
    // Kept, not discarded: it is a real receipt the user may still need.
    expect(failed.lastAnalysis).not.toBeNull();
  });

  it("marks the previous result stale when a run is cancelled", () => {
    expect(workflowReducer(withAnalysis(), { type: "cancel" }).staleAnalysis).toBe(true);
  });

  it("marks it stale as soon as a new run starts, before any receipt arrives", () => {
    expect(workflowReducer(withAnalysis(), { type: "start" }).staleAnalysis).toBe(true);
  });

  it("does not claim staleness when there was never a result to be stale", () => {
    const failed = workflowReducer(createWorkflowState(), {
      type: "problem",
      problem: { type: "about:blank", title: "t", status: 500, detail: "d", error: "e" },
    });
    expect(failed.staleAnalysis).toBe(false);
  });

  it("clears staleness only when a new receipt actually arrives", () => {
    const stale = workflowReducer(withAnalysis(), { type: "start" });
    expect(stale.staleAnalysis).toBe(true);
    const fresh = workflowReducer(stale, { type: "event", event: receiptEvent(2) });
    expect(fresh.staleAnalysis).toBe(false);
  });

  it("stops showing stages as running once a run has failed", () => {
    const running = workflowReducer(createWorkflowState(), { type: "start" });
    const failed = workflowReducer(running, {
      type: "problem",
      problem: { type: "about:blank", title: "t", status: 500, detail: "d", error: "e" },
    });
    expect(failed.stages.some((stage) => stage.status === "running")).toBe(false);
    expect(failed.stages.some((stage) => stage.status === "queued")).toBe(false);
  });
});
