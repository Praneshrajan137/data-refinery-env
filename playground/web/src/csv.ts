import Papa from "papaparse";
import type {
  AnalyzeResponse,
  CsvPreview,
  Issue,
  IssueGroup,
  ProblemDetail,
  Severity,
} from "./types";

export const DEFAULT_MAX_UPLOAD_BYTES = 1_048_576;
const PREVIEW_ROWS = 5;
const SEVERITY_WEIGHT: Record<Severity, number> = {
  unsafe: 3,
  review: 2,
  safe: 1,
};

export interface ValidationResult {
  ok: boolean;
  message?: string;
}

export function validateCsvFile(file: File, maxBytes = DEFAULT_MAX_UPLOAD_BYTES): ValidationResult {
  const name = file.name.toLowerCase();
  const type = file.type.toLowerCase();
  const looksLikeCsv =
    name.endsWith(".csv") ||
    type.includes("csv") ||
    type === "text/plain" ||
    type === "application/vnd.ms-excel";

  if (!looksLikeCsv) {
    return { ok: false, message: "Choose a CSV file." };
  }
  if (file.size === 0) {
    return { ok: false, message: "The CSV file is empty." };
  }
  if (file.size > maxBytes) {
    return {
      ok: false,
      message: `File is ${(file.size / 1024).toFixed(1)} KiB, which is larger than the hosted playground limit of ${Math.floor(
        maxBytes / 1024,
      )} KiB.`,
    };
  }
  return { ok: true };
}

export function parseCsvPreview(text: string): CsvPreview {
  const parsed = Papa.parse<Record<string, string>>(text, {
    header: true,
    skipEmptyLines: true,
    preview: PREVIEW_ROWS + 1,
    transformHeader: (header) => header.trim(),
  });

  const columns = (parsed.meta.fields ?? []).filter((field) => field.trim().length > 0);
  const fatalError = parsed.errors.find((error) => error.type === "Delimiter" || error.type === "Quotes");

  if (fatalError) {
    throw new Error(fatalError.message);
  }
  if (columns.length === 0) {
    throw new Error("CSV must include a header row.");
  }

  const rows = parsed.data
    .filter((row) => columns.some((column) => String(row[column] ?? "").trim().length > 0))
    .slice(0, PREVIEW_ROWS)
    .map((row) =>
      Object.fromEntries(columns.map((column) => [column, String(row[column] ?? "")])),
    );

  if (rows.length === 0) {
    throw new Error("CSV must include at least one data row.");
  }

  return {
    columns,
    rows,
    totalPreviewRows: rows.length,
    truncated: parsed.data.length > PREVIEW_ROWS,
  };
}

export function groupIssues(issues: Issue[]): IssueGroup[] {
  return [...issues]
    .map((issue) => ({
      ...issue,
      key: `${issue.column}:${issue.issue_type}:${issue.severity}`,
    }))
    .sort((a, b) => {
      const severityDelta = SEVERITY_WEIGHT[b.severity] - SEVERITY_WEIGHT[a.severity];
      if (severityDelta !== 0) {
        return severityDelta;
      }
      if (b.count !== a.count) {
        return b.count - a.count;
      }
      return a.column.localeCompare(b.column);
    });
}

export function problemToMessage(problem: ProblemDetail): string {
  if (problem.error === "advanced_mode_unavailable") {
    return "Advanced mode is unavailable because this backend has no provider key configured.";
  }
  if (problem.error === "apply_not_supported") {
    return "Hosted playground repairs are dry-run only.";
  }
  if (problem.error === "unknown_constraint_id") {
    return "One or more accepted constraints do not belong to this CSV analysis run.";
  }
  if (problem.error === "invalid_accepted_constraint_ids") {
    return "Accepted constraints must be sent as a JSON array of candidate IDs.";
  }
  if (problem.error === "file_too_large") {
    return "The uploaded CSV is larger than the hosted playground limit.";
  }
  if (problem.error === "invalid_csv") {
    return "The CSV could not be parsed. Check for unterminated quotes, inconsistent delimiters, or broken rows.";
  }
  if (problem.error === "empty_csv") {
    return "The CSV needs a header row and at least one data row.";
  }
  if (problem.error === "too_many_rows") {
    return `The CSV has more rows than this hosted playground allows${
      typeof problem.max_rows === "number" ? ` (${problem.max_rows})` : ""
    }.`;
  }
  if (problem.error === "too_many_columns") {
    return `The CSV has more columns than this hosted playground allows${
      typeof problem.max_columns === "number" ? ` (${problem.max_columns})` : ""
    }.`;
  }
  if (problem.error === "too_many_cells") {
    return "The CSV is too wide and tall for this hosted playground.";
  }
  if (problem.error === "unsupported_file_type") {
    return "Upload a CSV file with a .csv extension or text/csv content type.";
  }
  if (problem.error === "request_timeout") {
    return "The backend timed out before completing the request. Try a smaller CSV or retry after the backend cools down.";
  }
  if (problem.error === "rate_limit_exceeded") {
    return "Too many requests. Wait about a minute before trying again.";
  }
  return problem.detail || problem.title || "The request could not be completed.";
}

export function buildEvidenceExport(
  datasetName: string,
  analysis: AnalyzeResponse,
): string {
  return JSON.stringify(
    {
      product: "DataForge Playground",
      dataset_name: datasetName,
      generated_at: new Date().toISOString(),
      dry_run: true,
      source: analysis.source,
      schema_inference: analysis.schema_inference,
      risk_summary: analysis.risk_summary,
      issues: analysis.issues,
      repairs: analysis.repairs,
      verification: analysis.verification,
      transaction_journal: analysis.txn_journal,
      repair_receipt: analysis.receipt,
      apply_handoff: analysis.apply_handoff,
      limitations: analysis.limitations,
      contract_version: analysis.meta.contract_version,
    },
    null,
    2,
  );
}

export function formatRows(rows: number[], truncated = false): string {
  if (rows.length <= 5) {
    return `${rows.join(", ")}${truncated ? "..." : ""}`;
  }
  return `${rows.slice(0, 5).join(", ")}...`;
}
