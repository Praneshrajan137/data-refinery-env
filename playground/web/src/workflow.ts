import type {
  AnalyzeResponse,
  ProblemDetail,
  WorkflowEvent,
  WorkflowStageId,
  WorkflowStatus,
} from "./types";

export const WORKFLOW_STAGES: Array<{
  id: WorkflowStageId;
  label: string;
  description: string;
}> = [
  {
    id: "intake",
    label: "Intake",
    description: "Read the CSV and establish the dry-run boundary.",
  },
  {
    id: "schema_inference",
    label: "Schema Inference",
    description: "Infer assumptions without applying them silently.",
  },
  {
    id: "constraint_review",
    label: "Constraint Review",
    description: "Separate pending assumptions from accepted repair semantics.",
  },
  {
    id: "detectors",
    label: "Detectors",
    description: "Find issue evidence across the uploaded table.",
  },
  {
    id: "repair_candidates",
    label: "Repair Candidates",
    description: "Generate exact cell replacement candidates.",
  },
  {
    id: "safety_gate",
    label: "Safety Gate",
    description: "Deny unsafe edits before verifier work continues.",
  },
  {
    id: "smt_verifier",
    label: "SMT Verifier",
    description: "Prove accepted fixes respect known constraints.",
  },
  {
    id: "dry_run_transaction",
    label: "Dry-Run Transaction",
    description: "Create auditable handoff evidence without mutation.",
  },
  {
    id: "receipt",
    label: "Receipt",
    description: "Summarize limits, proof, and local CLI commands.",
  },
];

export interface WorkflowStageView {
  id: WorkflowStageId;
  label: string;
  description: string;
  status: WorkflowStatus;
  summary: string;
  counts: Record<string, number | string | boolean>;
  confidence?: number;
  uncertainty?: string;
  requiresHuman: boolean;
  startedAt?: string;
  completedAt?: string;
}

export interface WorkflowRunState {
  runId: string | null;
  status: "idle" | "running" | "ready" | "error" | "cancelled";
  stages: WorkflowStageView[];
  events: WorkflowEvent[];
  problem: ProblemDetail | null;
  lastAnalysis: AnalyzeResponse | null;
  /**
   * True when `lastAnalysis` describes an EARLIER run than the one the user just attempted.
   *
   * This exists because the alternative was worse in both directions. The UI reads
   * `workflow.lastAnalysis ?? analysis`, and a failed or cancelled run left `lastAnalysis`
   * untouched, so a red error banner rendered directly above a green "every applied change was
   * proven" verdict describing a different run -- the product's central claim, attached to the
   * wrong evidence. Clearing it instead would have thrown away a completed receipt the user
   * may still need, which is its own dishonesty. So the result is kept and labelled.
   */
  staleAnalysis: boolean;
}

export type WorkflowAction =
  | { type: "reset" }
  | { type: "start" }
  | { type: "event"; event: WorkflowEvent }
  | { type: "analysis"; analysis: AnalyzeResponse }
  | { type: "problem"; problem: ProblemDetail }
  | { type: "cancel" };

export function createWorkflowState(): WorkflowRunState {
  return {
    runId: null,
    status: "idle",
    stages: initialStages(),
    events: [],
    problem: null,
    lastAnalysis: null,
    staleAnalysis: false,
  };
}

export function workflowReducer(
  state: WorkflowRunState,
  action: WorkflowAction,
): WorkflowRunState {
  if (action.type === "reset") {
    return createWorkflowState();
  }
  if (action.type === "start") {
    return {
      ...state,
      runId: null,
      status: "running",
      stages: initialStages("queued"),
      events: [],
      problem: null,
      // The moment a new run starts, any result still on screen belongs to the previous one.
      staleAnalysis: state.lastAnalysis !== null,
    };
  }
  if (action.type === "cancel") {
    return {
      ...state,
      status: "cancelled",
      // A cancelled run leaves any previous result standing but no longer current.
      staleAnalysis: state.lastAnalysis !== null,
      stages: state.stages.map((stage) =>
        stage.status === "running" ? { ...stage, status: "cancelled" } : stage,
      ),
    };
  }
  if (action.type === "problem") {
    return {
      ...state,
      status: "error",
      problem: action.problem,
      staleAnalysis: state.lastAnalysis !== null,
      // Stages frozen mid-flight are not still running. Leaving them "running" alongside a
      // terminal error made the atlas claim work was in progress after it had stopped.
      stages: state.stages.map((stage) =>
        stage.status === "running" || stage.status === "queued"
          ? { ...stage, status: "blocked" }
          : stage,
      ),
    };
  }
  if (action.type === "analysis") {
    return synthesizeWorkflowEvents(action.analysis).reduce(workflowReducer, {
      ...state,
      status: "running",
      events: [],
      stages: initialStages("queued"),
      problem: null,
      staleAnalysis: false,
    });
  }

  const event = action.event;
  const stages = state.stages.map((stage) =>
    stage.id === event.stage_id
      ? {
          ...stage,
          status: event.status,
          summary: event.summary,
          counts: event.counts ?? {},
          confidence: event.confidence,
          uncertainty: event.uncertainty,
          requiresHuman: event.requires_human,
          startedAt: event.started_at,
          completedAt: event.completed_at,
        }
      : stage,
  );
  const nextStatus =
    event.status === "failed"
      ? "error"
      : event.analysis
        ? "ready"
        : state.status === "idle"
          ? "running"
          : state.status;

  return {
    ...state,
    runId: event.run_id,
    status: nextStatus,
    stages,
    events: [...state.events, event],
    problem: event.problem ?? state.problem,
    lastAnalysis: event.analysis ?? state.lastAnalysis,
    // A receipt arriving is the only thing that makes the displayed result current again.
    staleAnalysis: event.analysis ? false : state.staleAnalysis,
  };
}

export function synthesizeWorkflowEvents(analysis: AnalyzeResponse): WorkflowAction[] {
  const runId = `json-${analysis.source.sha256.slice(0, 12)}`;
  const pendingSupported = analysis.risk_summary.pending_repair_supported_constraints;
  const safetyBlocked = analysis.receipt.safety_verdict !== "allow";
  const verifierBlocked = ["reject", "unknown"].includes(analysis.receipt.verifier_verdict);
  const averageCandidateConfidence = average(
    analysis.schema_inference.candidates.map((candidate) => candidate.confidence),
  );
  const averageFixConfidence = average(analysis.repairs.map((fix) => fix.confidence));
  const events: WorkflowEvent[] = [
    workflowEvent(runId, 0, "intake", "completed", `Accepted ${analysis.source.name}.`, {
      rows: analysis.source.rows,
      columns: analysis.source.columns,
      bytes: analysis.source.size_bytes,
    }),
    workflowEvent(
      runId,
      1,
      "schema_inference",
      "completed",
      `Inferred ${analysis.schema_inference.candidates.length} reviewable constraint candidate(s).`,
      {
        candidates: analysis.schema_inference.candidates.length,
        repair_supported_pending: pendingSupported,
      },
      averageCandidateConfidence,
      "Inference is advisory until accepted for the current run.",
      pendingSupported > 0,
    ),
    workflowEvent(
      runId,
      2,
      "constraint_review",
      "completed",
      `${analysis.receipt.accepted_constraint_ids.length} accepted constraint(s) were used.`,
      {
        accepted: analysis.receipt.accepted_constraint_ids.length,
        pending_supported: pendingSupported,
      },
      undefined,
      undefined,
      pendingSupported > 0,
    ),
    workflowEvent(
      runId,
      3,
      "detectors",
      "completed",
      `Detected ${analysis.issues.length} issue group(s).`,
      {
        issues: analysis.issues.length,
        safe: analysis.risk_summary.severity_counts.safe,
        review: analysis.risk_summary.severity_counts.review,
        unsafe: analysis.risk_summary.severity_counts.unsafe,
      },
      undefined,
      "Severity is categorical and evidence-derived.",
      analysis.risk_summary.severity_counts.unsafe > 0,
    ),
    workflowEvent(
      runId,
      4,
      "repair_candidates",
      "completed",
      `${analysis.repairs.length} verified fix(es) from ${analysis.receipt.candidate_repairs.length} candidate(s).`,
      {
        candidate_repairs: analysis.receipt.candidate_repairs.length,
        verified_fixes: analysis.repairs.length,
        failures: analysis.verification.failures.length,
      },
      averageFixConfidence,
      undefined,
      analysis.verification.failures.length > 0,
    ),
    workflowEvent(
      runId,
      5,
      "safety_gate",
      safetyBlocked ? "blocked" : "completed",
      `Safety gate returned ${analysis.receipt.safety_verdict}.`,
      { proof_obligations: analysis.receipt.proof_obligations.length },
      undefined,
      undefined,
      safetyBlocked,
    ),
    workflowEvent(
      runId,
      6,
      "smt_verifier",
      verifierBlocked ? "blocked" : "completed",
      `SMT verifier returned ${analysis.receipt.verifier_verdict}.`,
      {
        proof_obligations: analysis.receipt.proof_obligations.length,
        abstentions: analysis.verification.abstentions.length,
      },
      undefined,
      undefined,
      verifierBlocked || analysis.verification.abstentions.length > 0,
    ),
    workflowEvent(
      runId,
      7,
      "dry_run_transaction",
      "completed",
      `Created dry-run transaction ${analysis.txn_journal.txn_id}.`,
      {
        fixes: analysis.txn_journal.fixes_count,
        applied: analysis.txn_journal.applied,
      },
    ),
    workflowEvent(
      runId,
      8,
      "receipt",
      "completed",
      analysis.receipt.reason,
      {
        issues: analysis.receipt.issues_count,
        fixes: analysis.receipt.fixes_count,
        limitations: analysis.limitations.length,
      },
      undefined,
      undefined,
      pendingSupported > 0 || analysis.verification.failures.length > 0,
      analysis,
    ),
  ];
  return events.map((event): WorkflowAction => ({ type: "event", event }));
}

function initialStages(status: WorkflowStatus = "queued"): WorkflowStageView[] {
  return WORKFLOW_STAGES.map((stage) => ({
    ...stage,
    status,
    summary: "Waiting for analysis.",
    counts: {},
    requiresHuman: false,
  }));
}

function workflowEvent(
  runId: string,
  sequence: number,
  stageId: WorkflowStageId,
  status: WorkflowStatus,
  summary: string,
  counts: Record<string, number | string | boolean> = {},
  confidence?: number,
  uncertainty?: string,
  requiresHuman = false,
  analysis?: AnalyzeResponse,
): WorkflowEvent {
  const now = new Date().toISOString();
  return {
    schema_version: "workflow_event_v1",
    run_id: runId,
    sequence,
    stage_id: stageId,
    status,
    summary,
    started_at: now,
    completed_at: status === "completed" || status === "blocked" ? now : undefined,
    counts,
    confidence,
    uncertainty,
    requires_human: requiresHuman,
    analysis,
  };
}

function average(values: number[]): number | undefined {
  if (values.length === 0) {
    return undefined;
  }
  return Number((values.reduce((total, value) => total + value, 0) / values.length).toFixed(4));
}
