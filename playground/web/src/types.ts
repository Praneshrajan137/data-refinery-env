// The trust vocabularies are GENERATED from dataforge/domain/vocabulary.py. Re-exported
// here so existing imports keep working, but never re-declared: `Severity` and
// `VerificationStrength` used to be spelled out in this file AND in the engine AND in
// six inline Literals in the HTTP models, which is how they drifted.
import type {
  Provenance,
  ReviewReason,
  Severity,
  VerificationStrength,
} from "./domain/vocabulary.generated";

export type { Provenance, ReviewReason, Severity, VerificationStrength };

export type RiskLevel = "none" | "low" | "medium" | "high";
export type RepairReadiness = "no_action" | "verified" | "partial" | "blocked";
export type ConstraintDecision = "pending" | "accepted" | "rejected";
export type RepairMode = "deterministic" | "agent";

// A fix is `proven` when it is deterministic or verified against an
// authoritative schema; `plausibility_only` when it was only checked by the
// advisory inferred guard (an untrusted value with no authoritative schema for its
// column). The distinction is the product's core guarantee and must never be blurred,
// which is exactly why the type is generated rather than typed here twice.
export type IndependentVerification = "agreed" | "not_run";

export interface BackendCapability {
  status: "ok";
  advanced_available: boolean;
  agent_available?: boolean;
  entity_consensus_available?: boolean;
  agent_policy?: string;
  agent_provider?: string | null;
  verify_available?: boolean;
  agent_max_steps?: number;
  max_upload_bytes: number;
  streaming_available?: boolean;
  workflow_contract_version?: "workflow_event_v1";
  service?: string;
  api_version?: string;
  contract_version?: string;
  build_sha?: string;
  server_time_utc?: string;
  environment?: string;
  cors_configured?: boolean;
  otel_enabled?: boolean;
  limits?: {
    max_upload_bytes: number;
    max_rows: number;
    max_columns: number;
    max_cells?: number;
  };
  metrics?: {
    requests_total?: number;
    responses_4xx?: number;
    responses_5xx?: number;
    error_rate?: number;
    latency_ms?: Record<string, number>;
  };
}

export interface RuntimeConfig {
  BACKEND_URL: string;
}

export interface DatasetInput {
  file: File;
  source: "upload" | "sample";
  sampleName?: string;
  preview: CsvPreview;
}

export interface CsvPreview {
  columns: string[];
  rows: Record<string, string>[];
  totalPreviewRows: number;
  truncated: boolean;
}

export interface Issue {
  column: string;
  issue_type: string;
  severity: Severity;
  row_indices: number[];
  row_indices_truncated?: boolean;
  count: number;
}

export interface IssueGroup extends Issue {
  key: string;
}

/**
 * One individually addressable flagged cell.
 *
 * `Issue` groups by (column, issue_type, severity) and caps `row_indices`, so a
 * cell map rebuilt from that would silently show a subset and look complete.
 * This is the untruncated channel, and it restores `confidence`, `actual`,
 * `expected` and `reason` -- which the grouping destroyed.
 */
export interface FlaggedCell {
  row: number;
  column: string;
  issue_type: string;
  severity: Severity;
  confidence: number;
  actual: string;
  expected?: string | null;
  reason: string;
}

/** Every flagged cell's position, columnar. Complete even when `cells` is bounded. */
export interface FlaggedCellIndex {
  column_indices: number[];
  rows: number[];
}

export interface ConfidenceBinPayload {
  from_value: number;
  to_value: number;
  count: number;
}

export interface ConfidenceClassPayload {
  issue_type: string;
  bins: ConfidenceBinPayload[];
  count: number;
  distinct_values: number;
  mode_value?: number | null;
  mode_share: number;
}

export interface FlaggedCells {
  index: FlaggedCellIndex;
  cells: FlaggedCell[];
  confidence_histogram: ConfidenceClassPayload[];
  total: number;
  truncated: boolean;
  note: string;
}

/**
 * One cell of an opt-in human-review ordering. Empty unless a caller supplied a
 * review ranker; the playground never does.
 */
export interface ReviewRankedCell {
  row: number;
  column: string;
  triage_score: number;
  reason: string;
}

export interface ProfileResponse {
  issues: Issue[];
  meta: {
    rows: number;
    columns: number;
    column_names: string[];
    total_issues: number;
    advanced_requested: boolean;
    api_version: string;
    contract_version: string;
  };
}

export interface SourceView {
  name: string;
  size_bytes: number;
  sha256: string;
  rows: number;
  columns: number;
  column_names: string[];
}

export interface ConstraintCandidate {
  candidate_id: string;
  kind: string;
  columns: string[];
  dependent?: string | null;
  inferred_type?: string | null;
  pattern?: string | null;
  min_value?: number | null;
  max_value?: number | null;
  confidence: number;
  evidence: string;
  decision: ConstraintDecision;
  repair_supported: boolean;
}

export interface SchemaInference {
  schema_version: "constraint_review_v1";
  source_sha256: string;
  row_count: number;
  candidates: ConstraintCandidate[];
}

export interface RiskSummary {
  dataset_level: RiskLevel;
  repair_readiness: RepairReadiness;
  severity_counts: Record<Severity, number>;
  pending_repair_supported_constraints: number;
  reasons: string[];
}

export interface VerifiedFix {
  row: number;
  column: string;
  old_value: string;
  new_value: string;
  detector_id: string;
  reason: string;
  confidence: number;
  provenance: Provenance;
  verifier_reason?: string;
  verification_strength?: VerificationStrength | null;
  review_reason?: ReviewReason | null;
}

export interface RepairFailure {
  row: number;
  column: string;
  issue_type: string;
  status: string;
  reason: string;
  attempt_count: number;
  unsat_core: string[];
}

export interface RootCause {
  row: number;
  column: string;
  issue_type: string;
  category: string;
  confidence: number;
  reason: string;
}

export interface CandidateRepair {
  row: number;
  column: string;
  old_value: string;
  new_value: string;
  detector_id: string;
  operation: string;
  reason: string;
  confidence: number;
  provenance: Provenance;
  verifier_reason: string;
  verification_strength?: VerificationStrength | null;
  review_reason?: ReviewReason | null;
}

export interface ProofObligation {
  obligation_id: string;
  verifier: string;
  status: string;
  reason: string;
  unsat_core: string[];
}

export interface RepairJournal {
  txn_id: string;
  created_at: string;
  source_name: string;
  source_sha256: string;
  fixes_count: number;
  applied: boolean;
  events: Array<{ event_type: string }>;
  note: string;
}

export interface RepairReceipt {
  schema_version: string;
  receipt_version: string;
  contract_version: string;
  mode: string;
  applied: boolean;
  reversible: boolean;
  source_sha256: string;
  post_sha256?: string | null;
  txn_id?: string | null;
  safety_verdict: string;
  verifier_verdict: string;
  independent_verification?: IndependentVerification;
  issues_count: number;
  fixes_count: number;
  candidate_provenance: Provenance[];
  root_causes: RootCause[];
  candidate_repairs: CandidateRepair[];
  applied_fixes?: VerifiedFix[];
  suggested_fixes?: CandidateRepair[];
  proof_obligations: ProofObligation[];
  accepted_constraint_ids: string[];
  constraints_artifact_sha256?: string | null;
  patch_plan_sha256?: string | null;
  revert_command?: string | null;
  review_ranking?: ReviewRankedCell[];
  limitations: string[];
  reason: string;
}

export interface VerificationSummary {
  safety_verdict: string;
  verifier_verdict: string;
  accepted_constraint_ids: string[];
  failures: RepairFailure[];
  abstentions: string[];
  failure_reasons: string[];
}

export interface CertificateCheck {
  name: string;
  ok: boolean;
  detail: string;
}

export interface Certificate {
  ok: boolean;
  checks: CertificateCheck[];
}

export interface ApplyHandoff {
  source_name: string;
  dry_run_command: string;
  apply_command: string;
  audit_command: string;
  revert_command: string;
  note: string;
}

export interface RepairResponse {
  fixes: VerifiedFix[];
  txn_journal: RepairJournal | null;
  receipt?: RepairReceipt;
  failures?: RepairFailure[];
  meta: {
    api_version: string;
    contract_version: string;
  };
}

export interface AnalyzeResponse {
  source: SourceView;
  schema_inference: SchemaInference;
  risk_summary: RiskSummary;
  issues: Issue[];
  flagged_cells: FlaggedCells;
  repairs: VerifiedFix[];
  verification: VerificationSummary;
  certificate: Certificate;
  txn_journal: RepairJournal;
  receipt: RepairReceipt;
  apply_handoff: ApplyHandoff;
  limitations: string[];
  agent?: AgentSummary | null;
  meta: {
    api_version: string;
    contract_version: string;
  };
}

export interface AgentTraceStep {
  step: number;
  action_type: string;
  accepted?: boolean | null;
  detail: string;
}

export interface AgentSummary {
  policy_name: string;
  steps_used: number;
  max_steps: number;
  floor_fix_count: number;
  agent_fix_count: number;
  residual_count: number;
  reason: string;
  agent_txn_id?: string | null;
  agent_fixes: VerifiedFix[];
  held_fixes?: VerifiedFix[];
  trace: AgentTraceStep[];
}

export interface ProblemDetail {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
  error?: string;
  [key: string]: unknown;
}

export type WorkflowStageId =
  | "intake"
  | "schema_inference"
  | "constraint_review"
  | "detectors"
  | "repair_candidates"
  | "safety_gate"
  | "smt_verifier"
  | "dry_run_transaction"
  | "receipt";

export type WorkflowStatus =
  | "queued"
  | "running"
  | "completed"
  | "blocked"
  | "failed"
  | "cancelled";

export interface WorkflowEvent {
  schema_version: "workflow_event_v1";
  run_id: string;
  sequence: number;
  stage_id: WorkflowStageId;
  status: WorkflowStatus;
  summary: string;
  started_at?: string;
  completed_at?: string;
  counts: Record<string, number | string | boolean>;
  confidence?: number;
  uncertainty?: string;
  requires_human: boolean;
  analysis?: AnalyzeResponse;
  problem?: ProblemDetail;
}

export interface AnalyzeStreamOptions {
  signal?: AbortSignal;
  onEvent: (event: WorkflowEvent) => void;
}

export interface ExternalFix {
  row: number;
  column: string;
  new_value: string;
  expected_old_value?: string | null;
}

export interface VerifyScenario {
  name: string;
  proposer: string;
  fixes: ExternalFix[];
  accepted_constraint_ids: string[];
  note: string;
  /**
   * Name of the bundled sample whose DECLARED premise authorises this batch, or null.
   *
   * Only `hospital_10rows` ships one. A mined constraint accepted in review stopped conferring
   * write authority with C4, so a scenario without a declared premise correctly proves nothing --
   * and that contrast is part of what the surface demonstrates.
   */
  declared_schema: string | null;
}

export interface VerifyFixesResponse {
  source: SourceView;
  proposer: string;
  proposed_count: number;
  authoritative_schema: boolean;
  would_apply: VerifiedFix[];
  receipt: RepairReceipt;
  verification: VerificationSummary;
  certificate: Certificate;
  apply_handoff: ApplyHandoff;
  limitations: string[];
  meta: {
    api_version: string;
    contract_version: string;
  };
}

export interface VerifyFixesOptions {
  acceptedConstraintIds?: string[];
  proposer?: string;
  confirmEscalations?: boolean;
  allowUnproven?: boolean;
  /** Bundled sample name whose declared premise should authorise this batch. */
  declaredSchema?: string | null;
}
