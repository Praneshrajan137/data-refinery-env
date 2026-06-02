import type {
  AnalyzeResponse,
  ConstraintCandidate,
  DatasetInput,
  RepairFailure,
  WorkflowStatus,
} from "./types";
import type { WorkflowRunState, WorkflowStageView } from "./workflow";

export type SelectedEvidence =
  | { kind: "stage"; id: string }
  | { kind: "constraint"; id: string }
  | { kind: "issue"; id: string }
  | { kind: "repair"; id: string }
  | { kind: "proof"; id: string }
  | { kind: "receipt"; id: string }
  | { kind: "failure"; id: string };

export type InstrumentTone = "neutral" | "active" | "verified" | "review" | "danger";

export interface ProofNodeViewModel {
  id: string;
  label: string;
  description: string;
  status: WorkflowStatus;
  statusLabel: string;
  tone: InstrumentTone;
  summary: string;
  counts: Array<{ label: string; value: string }>;
  confidence?: string;
  uncertainty?: string;
  requiresHuman: boolean;
}

export interface ReviewItem {
  id: string;
  kind: "constraint" | "failure" | "abstention" | "limitation" | "handoff" | "clear";
  title: string;
  detail: string;
  tone: InstrumentTone;
  meta: string;
}

export interface EvidenceGroup {
  id: "source" | "issues" | "constraints" | "repairs" | "proof" | "receipt";
  title: string;
  count: number;
  tone: InstrumentTone;
  detail: string;
}

export interface ObservatoryViewModel {
  proofNodes: ProofNodeViewModel[];
  reviewQueue: ReviewItem[];
  evidenceGroups: EvidenceGroup[];
  runPosture: {
    title: string;
    detail: string;
    tone: InstrumentTone;
    metrics: Array<{ label: string; value: string | number }>;
  };
}

export function buildObservatoryView({
  analysis,
  dataset,
  workflow,
  selectedConstraintIds,
}: {
  analysis: AnalyzeResponse | null;
  dataset: DatasetInput | null;
  workflow: WorkflowRunState;
  selectedConstraintIds: string[];
}): ObservatoryViewModel {
  return {
    proofNodes: workflow.stages.map(stageToProofNode),
    reviewQueue: buildReviewQueue(analysis, selectedConstraintIds),
    evidenceGroups: buildEvidenceGroups(analysis, dataset),
    runPosture: buildRunPosture(analysis, dataset, workflow),
  };
}

export function stageToProofNode(stage: WorkflowStageView): ProofNodeViewModel {
  return {
    id: stage.id,
    label: stage.label,
    description: stage.description,
    status: stage.status,
    statusLabel: formatLabel(stage.status),
    tone: toneForWorkflowStatus(stage.status),
    summary: stage.summary || stage.description,
    counts: Object.entries(stage.counts ?? {})
      .slice(0, 4)
      .map(([key, value]) => ({ label: formatLabel(key), value: String(value) })),
    confidence: stage.confidence === undefined ? undefined : formatPercent(stage.confidence),
    uncertainty: stage.uncertainty,
    requiresHuman: stage.requiresHuman,
  };
}

export function buildReviewQueue(
  analysis: AnalyzeResponse | null,
  selectedConstraintIds: string[],
): ReviewItem[] {
  if (!analysis) {
    return [
      {
        id: "awaiting-analysis",
        kind: "handoff",
        title: "No active decisions",
        detail: "Load a dataset and run Analyze to reveal assumptions, abstentions, and handoff work.",
        tone: "neutral",
        meta: "waiting",
      },
    ];
  }

  const selected = new Set(selectedConstraintIds);
  const constraints = analysis.schema_inference.candidates
    .filter((candidate) => candidate.repair_supported)
    .map((candidate) => constraintToReviewItem(candidate, selected.has(candidate.candidate_id)));

  const failures = analysis.verification.failures.map(failureToReviewItem);
  const abstentions = analysis.verification.abstentions.map((abstention, index): ReviewItem => ({
    id: `abstention-${index}`,
    kind: "abstention",
    title: "Verifier abstention",
    detail: abstention,
    tone: "review",
    meta: "requires judgment",
  }));
  const limitations = analysis.limitations.map((limitation, index): ReviewItem => ({
    id: `limitation-${index}`,
    kind: "limitation",
    title: "Operating limit",
    detail: limitation,
    tone: "neutral",
    meta: "bounded",
  }));

  const queue = [...constraints, ...failures, ...abstentions, ...limitations];
  if (queue.length === 0) {
    queue.push({
      id: "clear",
      kind: "clear",
      title: "No human review blockers",
      detail: "The dry run completed without pending supported assumptions or verifier failures.",
      tone: "verified",
      meta: "clear",
    });
  }
  return queue;
}

export function buildEvidenceGroups(
  analysis: AnalyzeResponse | null,
  dataset: DatasetInput | null,
): EvidenceGroup[] {
  if (!analysis) {
    return [
      {
        id: "source",
        title: dataset ? "Dataset staged" : "No dataset",
        count: dataset?.preview.rows.length ?? 0,
        tone: dataset ? "active" : "neutral",
        detail: dataset
          ? `${dataset.file.name} is previewed locally before the backend run.`
          : "Sample and upload intake appear in the mission bar.",
      },
    ];
  }

  return [
    {
      id: "source",
      title: "Source fingerprint",
      count: analysis.source.rows,
      tone: "neutral",
      detail: `${analysis.source.columns} columns, hash ${shortHash(analysis.source.sha256)}.`,
    },
    {
      id: "issues",
      title: "Risk evidence",
      count: analysis.issues.length,
      tone: analysis.risk_summary.dataset_level === "high" ? "danger" : "review",
      detail: `${analysis.receipt.issues_count} issue groups across detector evidence.`,
    },
    {
      id: "constraints",
      title: "Assumption review",
      count: analysis.schema_inference.candidates.length,
      tone: analysis.risk_summary.pending_repair_supported_constraints > 0 ? "review" : "verified",
      detail: `${analysis.receipt.accepted_constraint_ids.length} accepted for this run.`,
    },
    {
      id: "repairs",
      title: "Repair comparison",
      count: analysis.repairs.length,
      tone: analysis.repairs.length > 0 ? "verified" : "neutral",
      detail: `${analysis.verification.failures.length} attempted repair failure(s).`,
    },
    {
      id: "proof",
      title: "Verifier boundary",
      count: analysis.receipt.proof_obligations.length,
      tone: analysis.receipt.verifier_verdict === "accept" ? "verified" : "review",
      detail: `${analysis.receipt.safety_verdict} safety, ${analysis.receipt.verifier_verdict} verifier.`,
    },
    {
      id: "receipt",
      title: "Handoff capsule",
      count: analysis.limitations.length,
      tone: analysis.receipt.safety_verdict === "allow" ? "verified" : "danger",
      detail: analysis.receipt.reason,
    },
  ];
}

export function toneForWorkflowStatus(status: WorkflowStatus): InstrumentTone {
  if (status === "running") {
    return "active";
  }
  if (status === "completed") {
    return "verified";
  }
  if (status === "blocked" || status === "cancelled") {
    return "review";
  }
  if (status === "failed") {
    return "danger";
  }
  return "neutral";
}

export function formatLabel(value: string): string {
  return value.replace(/_/g, " ");
}

export function formatPercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

export function shortHash(value: string): string {
  return value.slice(0, 12);
}

function buildRunPosture(
  analysis: AnalyzeResponse | null,
  dataset: DatasetInput | null,
  workflow: WorkflowRunState,
): ObservatoryViewModel["runPosture"] {
  if (analysis) {
    const humanItems =
      analysis.risk_summary.pending_repair_supported_constraints +
      analysis.verification.failures.length +
      analysis.verification.abstentions.length;
    return {
      title: `${formatLabel(analysis.risk_summary.dataset_level)} risk`,
      detail: analysis.receipt.reason,
      tone: analysis.risk_summary.dataset_level === "high" ? "danger" : "verified",
      metrics: [
        { label: "Rows", value: analysis.source.rows },
        { label: "Issues", value: analysis.receipt.issues_count },
        { label: "Verified fixes", value: analysis.repairs.length },
        { label: "Human queue", value: humanItems },
      ],
    };
  }
  if (workflow.status === "running") {
    return {
      title: "Run in progress",
      detail: "The stream is emitting workflow events while the previous result remains protected.",
      tone: "active",
      metrics: [
        { label: "Stages", value: workflow.stages.length },
        { label: "Events", value: workflow.events.length },
        { label: "Dataset", value: dataset?.file.name ?? "none" },
      ],
    };
  }
  return {
    title: dataset ? "Dataset ready" : "Awaiting dataset",
    detail: dataset
      ? "The CSV preview is local. Analyze starts a stateless backend dry run."
      : "Choose a sample or upload a CSV to begin the proof loop.",
    tone: dataset ? "active" : "neutral",
    metrics: [
      { label: "Preview rows", value: dataset?.preview.rows.length ?? 0 },
      { label: "Columns", value: dataset?.preview.columns.length ?? 0 },
      { label: "Run state", value: workflow.status },
    ],
  };
}

function constraintToReviewItem(candidate: ConstraintCandidate, selected: boolean): ReviewItem {
  return {
    id: candidate.candidate_id,
    kind: "constraint",
    title: `${formatLabel(candidate.kind)} assumption`,
    detail: candidate.evidence,
    tone: selected || candidate.decision === "accepted" ? "verified" : "review",
    meta: selected || candidate.decision === "accepted" ? "accepted for rerun" : "pending",
  };
}

function failureToReviewItem(failure: RepairFailure): ReviewItem {
  return {
    id: `${failure.row}:${failure.column}:${failure.issue_type}`,
    kind: "failure",
    title: `Unfixed ${formatLabel(failure.issue_type)}`,
    detail: failure.reason,
    tone: "danger",
    meta: `row ${failure.row}, ${failure.column}`,
  };
}
