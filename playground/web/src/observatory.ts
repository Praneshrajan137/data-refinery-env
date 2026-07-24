import type {
  AnalyzeResponse,
  CandidateRepair,
  ConstraintCandidate,
  DatasetInput,
  IndependentVerification,
  RepairFailure,
  VerificationStrength,
  VerifiedFix,
  VerifyFixesResponse,
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

// --- Trust verdict -----------------------------------------------------------
// The product's core promise is not "we fixed N cells"; it is "every change we
// would write is proven, everything we cannot prove is held with a reason, and
// nothing incorrect is silently written — and you can re-verify that yourself."
// This view model makes that promise the primary, honest object in the UI.

const LLM_PROVENANCE = new Set(["llm_live", "llm_cache", "external"]);

export type TrustLevel = "pending" | "clear" | "proven" | "held" | "mixed";

export interface TrustVerdict {
  level: TrustLevel;
  headline: string;
  detail: string;
  guaranteeLine: string;
  provenCount: number;
  plausibilityCount: number;
  heldCount: number;
  abstentionCount: number;
  independentVerification: IndependentVerification;
  certificate: { ok: boolean; passed: number; total: number };
  metrics: Array<{ label: string; value: string | number; tone: InstrumentTone; hint: string }>;
}

/**
 * Classify a fix's verification strength honestly.
 *
 * Prefers the engine's per-fix `verification_strength`. When it is absent
 * (legacy payloads), it falls back to provenance: an LLM/external value is
 * plausibility-only, everything else is deterministic and therefore proven.
 * This mirrors the server-side certificate logic so the UI never overstates
 * proof.
 */
export function strengthOf(fix: VerifiedFix | CandidateRepair): VerificationStrength {
  if (fix.verification_strength === "plausibility_only") {
    return "plausibility_only";
  }
  if (fix.verification_strength === "proven") {
    return "proven";
  }
  return LLM_PROVENANCE.has(fix.provenance) ? "plausibility_only" : "proven";
}

const REVIEW_REASON_COPY: Record<string, string> = {
  failed_conformal_threshold:
    "Confidence did not clear the distribution-free auto-apply threshold.",
  safety_escalation: "The safety constitution escalated this for human confirmation.",
  safety_denied: "The safety constitution denied this change.",
  not_inferable_from_data: "The correct value is not derivable from the data in the table.",
  verifier_rejected: "The independent verifier rejected this proposal.",
  floor_cannot_verify: "The deterministic verifier could not prove this change safe.",
  inferred_fd_not_declared:
    "The supporting dependency was inferred, not declared, so it is not auto-applied.",
  ambiguous_fd:
    "The functional dependency was ambiguous, so no single correct value could be derived.",
  out_of_inferred_domain:
    "The proposed value falls outside the values inferred from the column.",
  unverified_transposition: "A transposition was proposed but could not be proven.",
  stale_precondition: "The row changed after the proposal, so it was not applied.",
  invalid_target: "The proposed value failed the target's constraints.",
};

export function humanizeReviewReason(reason: string | null | undefined): string {
  if (!reason) {
    return "Held for review — not proven safe to auto-apply.";
  }
  return REVIEW_REASON_COPY[reason] ?? formatLabel(reason);
}

export function buildTrustVerdict(analysis: AnalyzeResponse | null): TrustVerdict {
  if (!analysis) {
    return {
      level: "pending",
      headline: "No verdict yet",
      detail: "Run Analyze to produce a proof loop and its re-verifiable certificate.",
      guaranteeLine: "Nothing is applied. Every result is a dry run you can re-verify.",
      provenCount: 0,
      plausibilityCount: 0,
      heldCount: 0,
      abstentionCount: 0,
      independentVerification: "not_run",
      certificate: { ok: false, passed: 0, total: 0 },
      metrics: [],
    };
  }

  const wouldApply = analysis.repairs;
  const provenCount = wouldApply.filter((fix) => strengthOf(fix) === "proven").length;
  const plausibilityCount = wouldApply.length - provenCount;
  const heldCount = analysis.receipt.suggested_fixes?.length ?? 0;
  const abstentionCount = analysis.verification.abstentions.length;
  const independentVerification = analysis.receipt.independent_verification ?? "not_run";
  const checks = analysis.certificate?.checks ?? [];
  const passed = checks.filter((check) => check.ok).length;
  const certificate = {
    ok: analysis.certificate?.ok ?? false,
    passed,
    total: checks.length,
  };

  let level: TrustLevel;
  let headline: string;
  if (wouldApply.length === 0 && heldCount === 0 && abstentionCount === 0) {
    level = "clear";
    headline = "Nothing to repair";
  } else if (plausibilityCount > 0) {
    level = "mixed";
    headline = `${provenCount} proven, ${plausibilityCount} plausible-only`;
  } else if (provenCount > 0 && heldCount === 0) {
    level = "proven";
    headline = `${provenCount} proven fix${provenCount === 1 ? "" : "es"} ready to apply`;
  } else if (provenCount === 0 && (heldCount > 0 || abstentionCount > 0)) {
    level = "held";
    headline = `${heldCount + abstentionCount} held for review`;
  } else {
    level = "mixed";
    headline = `${provenCount} proven, ${heldCount} held`;
  }

  const guaranteeLine =
    plausibilityCount > 0
      ? "A plausibility-only value is not auto-applied unless you opt in — it is recorded as unproven."
      : "No unproven change would be written. Everything not proven is held with a reason.";

  const detail =
    level === "clear"
      ? "The dry run found nothing to change; the certificate confirms the data is unmodified."
      : `${provenCount} proven, ${heldCount} held for review, ${abstentionCount} honest abstention(s).`;

  return {
    level,
    headline,
    detail,
    guaranteeLine,
    provenCount,
    plausibilityCount,
    heldCount,
    abstentionCount,
    independentVerification,
    certificate,
    metrics: [
      {
        label: "Proven, would apply",
        value: provenCount,
        tone: provenCount > 0 ? "verified" : "neutral",
        hint: "Deterministic or authoritative-schema-verified. Safe to auto-apply.",
      },
      {
        label: "Held for review",
        value: heldCount + abstentionCount,
        tone: heldCount + abstentionCount > 0 ? "review" : "neutral",
        hint: "Not proven from the data. Abstention is a first-class, honest outcome.",
      },
      {
        label: "Plausibility-only",
        value: plausibilityCount,
        tone: plausibilityCount > 0 ? "review" : "verified",
        hint: "Model-proposed values with no authoritative schema. Never silently written.",
      },
      {
        label: "Certificate",
        value: certificate.total > 0 ? `${passed}/${certificate.total}` : "n/a",
        tone: certificate.ok ? "verified" : "review",
        hint: "Independent re-verification of the receipt against your exact bytes.",
      },
    ],
  };
}

// --- Guardrail verdict (verify_and_apply external fixes) ---------------------
// The agent-guardrail wedge: an untrusted actor proposes edits, DataForge proves
// the correct ones and blocks the rest. This view model makes "zero corruptions"
// the primary, honest object.

const REJECTED_REVIEW_REASONS = new Set([
  "verifier_rejected",
  "stale_precondition",
  "invalid_target",
  "safety_denied",
]);

export type GuardrailLevel = "pending" | "clear" | "proven" | "held" | "mixed";

export interface GuardrailVerdict {
  level: GuardrailLevel;
  headline: string;
  detail: string;
  guaranteeLine: string;
  proposed: number;
  proven: number;
  held: number;
  rejected: number;
  authoritative: boolean;
  independentVerification: IndependentVerification;
  certificate: { ok: boolean; passed: number; total: number };
  metrics: Array<{ label: string; value: string | number; tone: InstrumentTone; hint: string }>;
}

export function buildGuardrailVerdict(response: VerifyFixesResponse | null): GuardrailVerdict {
  if (!response) {
    return {
      level: "pending",
      headline: "No verdict yet",
      detail: "Propose external fixes to see which are proven and which are blocked.",
      guaranteeLine: "Nothing is applied. External values are verified in a stateless dry run.",
      proposed: 0,
      proven: 0,
      held: 0,
      rejected: 0,
      authoritative: false,
      independentVerification: "not_run",
      certificate: { ok: false, passed: 0, total: 0 },
      metrics: [],
    };
  }

  const proposed = response.proposed_count;
  const proven = response.would_apply.filter((fix) => strengthOf(fix) === "proven").length;
  const suggested = response.receipt.suggested_fixes ?? [];
  const rejected = suggested.filter((fix) =>
    REJECTED_REVIEW_REASONS.has(fix.review_reason ?? ""),
  ).length;
  const held = suggested.length - rejected;
  const independentVerification = response.receipt.independent_verification ?? "not_run";
  const checks = response.certificate?.checks ?? [];
  const passed = checks.filter((check) => check.ok).length;
  const certificate = {
    ok: response.certificate?.ok ?? false,
    passed,
    total: checks.length,
  };

  let level: GuardrailLevel;
  if (proposed === 0) {
    level = "clear";
  } else if (proven > 0 && held + rejected === 0) {
    level = "proven";
  } else if (proven === 0 && held + rejected > 0) {
    level = "held";
  } else {
    level = "mixed";
  }

  const headline =
    proven > 0
      ? `${proven} proven, ${held + rejected} blocked`
      : `${held + rejected} of ${proposed} blocked, 0 written`;

  const guaranteeLine = response.authoritative_schema
    ? "Only schema-proven edits would apply. Everything unproven is held or rejected — zero corruptions."
    : "No authoritative schema was provided, so no external value is proven — all proposals are held (correctly).";

  return {
    level,
    headline,
    detail: `${proposed} proposed by '${response.proposer}' -> ${proven} proven, ${held} held, ${rejected} rejected.`,
    guaranteeLine,
    proposed,
    proven,
    held,
    rejected,
    authoritative: response.authoritative_schema,
    independentVerification,
    certificate,
    metrics: [
      {
        label: "Proposed",
        value: proposed,
        tone: "neutral",
        hint: "Cell edits proposed by the untrusted actor.",
      },
      {
        label: "Proven, would apply",
        value: proven,
        tone: proven > 0 ? "verified" : "neutral",
        hint: "Verified against an authoritative schema. Safe to auto-apply.",
      },
      {
        label: "Held for review",
        value: held,
        tone: held > 0 ? "review" : "neutral",
        hint: "Not proven from the data or awaiting confirmation. Never silently written.",
      },
      {
        label: "Rejected",
        value: rejected,
        tone: rejected > 0 ? "danger" : "neutral",
        hint: "Type-corrupting, stale, invalid, or safety-denied proposals.",
      },
    ],
  };
}
