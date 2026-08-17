/**
 * Pure helpers shared across the product surface.
 *
 * Moved verbatim out of App.tsx: these were the file's tail, below 3,500 lines of JSX, and
 * several of them (localProblem's hardcoded title, isAbortError's inability to tell a
 * timeout from a user cancel) are load-bearing for how honestly the UI reports failure.
 */
import { ApiProblemError } from "../api";
import { shortHash } from "../observatory";
import type { InstrumentTone, ReviewItem, SelectedEvidence } from "../observatory";
import type { AnalyzeResponse, ConstraintCandidate, IssueGroup, ProblemDetail, RepairFailure, Severity, VerifiedFix } from "../types";

export type WorkState = "idle" | "loading" | "ready" | "error";
export type SortKey = "severity" | "count" | "column";

export const SAMPLE_OPTIONS = [
  { value: "hospital_10rows", label: "Hospital", detail: "Rating 45.0 -> 4.5" },
  { value: "flights_10rows", label: "Flights", detail: "Aviation data" },
  { value: "beers_10rows", label: "Beers", detail: "Consumer data" },
];

export function filterAndSortIssues(
  issues: IssueGroup[],
  filter: string,
  severity: Severity | "all",
  sortKey: SortKey,
) {
  const severityRank: Record<Severity, number> = { unsafe: 0, review: 1, safe: 2 };
  const normalizedFilter = filter.trim().toLowerCase();
  const filtered = issues.filter((issue) => {
    const matchesSeverity = severity === "all" || issue.severity === severity;
    const matchesFilter =
      normalizedFilter.length === 0 ||
      issue.column.toLowerCase().includes(normalizedFilter) ||
      issue.issue_type.toLowerCase().includes(normalizedFilter);
    return matchesSeverity && matchesFilter;
  });

  return [...filtered].sort((a, b) => {
    if (sortKey === "column") {
      return a.column.localeCompare(b.column);
    }
    if (sortKey === "count") {
      return b.count - a.count;
    }
    return severityRank[a.severity] - severityRank[b.severity] || b.count - a.count;
  });
}

export function formatConstraintColumns(candidate: ConstraintCandidate): string {
  const left = candidate.columns.join(", ");
  return candidate.dependent ? `${left} -> ${candidate.dependent}` : left;
}

export function repairKey(fix: VerifiedFix): string {
  return `${fix.row}:${fix.column}:${fix.old_value}:${fix.new_value}`;
}

export function downloadCertificate(analysis: {
  source: AnalyzeResponse["source"];
  certificate: AnalyzeResponse["certificate"];
  receipt: AnalyzeResponse["receipt"];
  apply_handoff: AnalyzeResponse["apply_handoff"];
}): void {
  // The portable certificate is the self-contained receipt plus its independent
  // re-verification. It travels with the data and can be re-checked off-machine.
  const payload = {
    kind: "dataforge_trust_certificate",
    source: {
      name: analysis.source.name,
      sha256: analysis.source.sha256,
      rows: analysis.source.rows,
      columns: analysis.source.columns,
    },
    certificate: analysis.certificate,
    receipt: analysis.receipt,
    audit_command: analysis.apply_handoff.audit_command,
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `dataforge-certificate-${shortHash(analysis.source.sha256)}.json`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

export function failureKey(failure: RepairFailure): string {
  return `${failure.row}:${failure.column}:${failure.issue_type}`;
}

export function selectionFromReviewItem(item: ReviewItem): SelectedEvidence {
  if (item.kind === "constraint") {
    return { kind: "constraint", id: item.id };
  }
  if (item.kind === "failure") {
    return { kind: "failure", id: item.id };
  }
  return { kind: "receipt", id: item.id };
}

export function toneClass(status: string): InstrumentTone {
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

export function toneForSeverity(severity: Severity): InstrumentTone {
  if (severity === "unsafe") {
    return "danger";
  }
  if (severity === "review") {
    return "review";
  }
  return "verified";
}

export function problemFromUnknown(error: unknown): ProblemDetail {
  if (error instanceof ApiProblemError) {
    return error.problem;
  }
  return localProblem(error instanceof Error ? error.message : "The request failed.");
}

export function localProblem(message: string): ProblemDetail {
  return {
    type: "https://dataforge.local/problems/frontend_validation",
    title: "Dataset validation failed",
    status: 400,
    detail: message,
    error: "frontend_validation",
  };
}

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
