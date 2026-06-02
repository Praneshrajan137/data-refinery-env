export type Severity = "safe" | "review" | "unsafe";

export interface BackendCapability {
  status: "ok";
  advanced_available: boolean;
  max_upload_bytes: number;
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
  count: number;
}

export interface IssueGroup extends Issue {
  key: string;
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

export interface VerifiedFix {
  row: number;
  column: string;
  old_value: string;
  new_value: string;
  detector_id: string;
  reason: string;
  confidence: number;
  provenance: string;
  verifier_reason?: string;
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

export interface RepairResponse {
  fixes: VerifiedFix[];
  txn_journal: RepairJournal | null;
  receipt?: {
    contract_version: string;
    safety_verdict: string;
    verifier_verdict: string;
    issues_count: number;
    fixes_count: number;
    candidate_provenance: string[];
    source_sha256: string;
    reason: string;
  };
  meta: {
    api_version: string;
    contract_version: string;
  };
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
