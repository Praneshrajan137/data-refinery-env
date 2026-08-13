import type { AnalyzeResponse, CandidateRepair, VerifiedFix } from "../types";
import { strengthOf } from "../observatory";
import type { AbsenceState, Rung } from "./grammar";

/**
 * The per-cell evidence model: one honest record per flagged cell.
 *
 * Built from receipt.root_causes, which is the ONLY untruncated per-cell channel
 * the API exposes. The obvious source -- issues[].row_indices -- is capped at
 * ISSUE_ROW_DISPLAY_LIMIT = 50 per group, so a map built from it would render
 * roughly 50 of 1,000 flagged cells and look complete. That is the lie of
 * omission L3 exists to prevent, so coverage is tracked explicitly and reported.
 */

export interface FlaggedCell {
  row: number;
  column: string;
  rung: Rung;
  issueType: string;
  /** Root-cause category, when the engine attributed one. */
  category: string | null;
  /**
   * Per-cell confidence, or null when not measured. Null is a real state: the
   * API's issue aggregation destroys Issue.confidence, so cells recovered from
   * the row_indices fallback genuinely have no measured confidence.
   */
  confidence: number | null;
  reviewReason: string | null;
  detail: string;
  /**
   * Engine provenance, carried so the rung can be independently re-checked. Without
   * it, any guard over a claim is tautological: it can only re-derive the rung from
   * the rung.
   */
  provenance: string | null;
  /** Verifier constraint labels, present only when this claim was blocked. */
  unsatCore?: string[];
  /** Present when an outcome proposed a value for this cell. */
  oldValue?: string | null;
  newValue?: string | null;
}

export type CoverageKind = "complete" | "truncated" | "not_measured";

export interface Coverage {
  kind: CoverageKind;
  /** Cells this model can address individually. */
  observed: number;
  /** Cells the engine says it flagged. */
  declared: number;
  source: "flagged_cells" | "root_causes" | "row_indices" | "none";
  note: string;
}

export interface EvidenceModel {
  rows: number;
  columns: string[];
  cells: FlaggedCell[];
  coverage: Coverage;
  countsByRung: Record<Rung, number>;
  /** Non-null when the whole view is an absence rather than a set of marks. */
  absence: AbsenceState | null;
  absenceText: string;
}

const emptyCounts = (): Record<Rung, number> => ({
  corroborated: 0,
  proven: 0,
  held: 0,
  downgraded: 0,
  plausibility_only: 0,
  rejected: 0,
  idle: 0,
});

const cellKey = (row: number, column: string): string => `${row}\u0000${column}`;

/**
 * Rung for a fix that the engine WOULD apply.
 *
 * `corroborated` is the only intensifier above proven and requires two
 * independent verifiers to have agreed -- a receipt-level fact, so it is passed
 * in rather than guessed per fix.
 */
function rungForAppliedFix(fix: VerifiedFix, independentlyVerified: boolean): Rung {
  if (strengthOf(fix) === "plausibility_only") {
    return "plausibility_only";
  }
  return independentlyVerified ? "corroborated" : "proven";
}

/**
 * Rung for a fix the engine held back. A held fix that is ALSO unproven reads as
 * plausibility, not held: plausibility is the weaker rung and the aggregate must
 * never overstate.
 */
function rungForHeldFix(fix: CandidateRepair): Rung {
  return strengthOf(fix) === "plausibility_only" ? "plausibility_only" : "held";
}

/**
 * Build the per-cell model.
 *
 * Sources are applied in order of OUTCOME SPECIFICITY, first claim wins -- not in
 * order of rung strength. This matters: every applied fix also has a root-cause
 * record, so resolving conflicts by weakest-rung would drag every proven fix down
 * to `held` and understate the entire run. Mirrors the strictly-additive
 * fill-only-unclaimed-cells pattern the backend detector ensemble already uses.
 */
export function buildEvidenceModel(analysis: AnalyzeResponse): EvidenceModel {
  const columns = analysis.source.column_names ?? [];
  const rows = analysis.source.rows ?? 0;
  const knownColumns = new Set(columns);
  const receipt = analysis.receipt;
  const independentlyVerified = receipt?.independent_verification === "agreed";

  const byCell = new Map<string, FlaggedCell>();

  const claim = (cell: FlaggedCell): void => {
    // Ignore cells outside the declared table: a mark we cannot place is a mark
    // we must not invent.
    if (!knownColumns.has(cell.column) || cell.row < 0 || (rows > 0 && cell.row >= rows)) {
      return;
    }
    const key = cellKey(cell.row, cell.column);
    if (!byCell.has(key)) {
      byCell.set(key, cell);
    }
  };

  // 1. Rejected: we tried and could not prove it safe.
  for (const failure of analysis.verification?.failures ?? []) {
    claim({
      row: failure.row,
      column: failure.column,
      rung: "rejected",
      issueType: failure.issue_type,
      category: null,
      confidence: null,
      reviewReason: null,
      detail: failure.reason,
      provenance: null,
      unsatCore: failure.unsat_core ?? [],
    });
  }

  // 2. Would-apply: proven, corroborated, or plausibility-only.
  for (const fix of analysis.repairs ?? []) {
    claim({
      row: fix.row,
      column: fix.column,
      rung: rungForAppliedFix(fix, independentlyVerified),
      issueType: fix.detector_id,
      category: null,
      confidence: fix.confidence,
      reviewReason: fix.review_reason ?? null,
      detail: fix.reason,
      provenance: fix.provenance,
      oldValue: fix.old_value,
      newValue: fix.new_value,
    });
  }

  // 3. Held for review, with a reason.
  for (const fix of receipt?.suggested_fixes ?? []) {
    claim({
      row: fix.row,
      column: fix.column,
      rung: rungForHeldFix(fix),
      issueType: fix.detector_id,
      category: null,
      confidence: fix.confidence,
      reviewReason: fix.review_reason ?? null,
      detail: fix.reason,
      provenance: fix.provenance,
      oldValue: fix.old_value,
      newValue: fix.new_value,
    });
  }

  // 4. Detected, no fix proposed. The COLUMNAR INDEX is the complete channel: it
  // carries every flagged cell's position in two integer arrays, so the map is whole
  // without a multi-megabyte payload. `cells` adds confidence, actual, expected and
  // reason for a bounded severity-ordered prefix, which the detail view uses.
  const flagged = analysis.flagged_cells;
  const detailByCell = new Map<string, (typeof flagged.cells)[number]>();
  for (const cell of flagged?.cells ?? []) {
    detailByCell.set(cellKey(cell.row, cell.column), cell);
  }

  const index = flagged?.index;
  const indexLength = Math.min(index?.column_indices.length ?? 0, index?.rows.length ?? 0);
  for (let position = 0; position < indexLength; position += 1) {
    const columnIndex = index!.column_indices[position];
    const column = columns[columnIndex];
    if (column === undefined) {
      continue;
    }
    const row = index!.rows[position];
    const detail = detailByCell.get(cellKey(row, column));
    claim({
      row,
      column,
      rung: "held",
      issueType: detail?.issue_type ?? "detected",
      category: null,
      confidence: detail?.confidence ?? null,
      reviewReason: null,
      detail: detail?.reason ?? "Flagged by a detector.",
      provenance: null,
    });
  }

  for (const cause of receipt?.root_causes ?? []) {
    claim({
      row: cause.row,
      column: cause.column,
      rung: "held",
      issueType: cause.issue_type,
      category: cause.category,
      confidence: cause.confidence,
      reviewReason: null,
      detail: cause.reason,
      provenance: null,
    });
  }

  const usedFlaggedCells = indexLength > 0;
  const usedRootCauses = (receipt?.root_causes ?? []).length > 0;
  let source: Coverage["source"] = usedFlaggedCells
    ? "flagged_cells"
    : usedRootCauses
      ? "root_causes"
      : "none";
  let fellBackToRowIndices = false;

  // 5. Fallback ONLY when no per-cell channel exists at all. row_indices is capped
  // at 50 per group, so anything recovered here is explicitly partial.
  if (!usedFlaggedCells && !usedRootCauses) {
    for (const issue of analysis.issues ?? []) {
      for (const row of issue.row_indices ?? []) {
        fellBackToRowIndices = true;
        claim({
          row,
          column: issue.column,
          rung: "held",
          issueType: issue.issue_type,
          category: null,
          confidence: null,
          reviewReason: null,
          detail: `${issue.issue_type} in ${issue.column}`,
          provenance: null,
        });
      }
    }
    if (fellBackToRowIndices) {
      source = "row_indices";
    }
  }

  const cells = [...byCell.values()];
  const countsByRung = emptyCounts();
  for (const cell of cells) {
    countsByRung[cell.rung] += 1;
  }

  const declared = flagged?.total ?? receipt?.issues_count ?? 0;
  const anyGroupTruncated =
    flagged?.truncated === true ||
    (analysis.issues ?? []).some((issue) => issue.row_indices_truncated === true);

  const coverage = resolveCoverage({
    observed: cells.length,
    declared,
    source,
    anyGroupTruncated,
  });

  const { absence, absenceText } = resolveAbsence(coverage, rows, columns.length);

  return { rows, columns, cells, coverage, countsByRung, absence, absenceText };
}

function resolveCoverage(input: {
  observed: number;
  declared: number;
  source: Coverage["source"];
  anyGroupTruncated: boolean;
}): Coverage {
  const { observed, declared, source, anyGroupTruncated } = input;

  if (source === "none" && declared === 0) {
    return {
      kind: "complete",
      observed,
      declared,
      source,
      note: "No cells were flagged. This is a measured result, not a missing one.",
    };
  }

  if (source === "none" && declared > 0) {
    return {
      kind: "not_measured",
      observed,
      declared,
      source,
      note: `The engine flagged ${declared} cells but published no per-cell record, so none can be located.`,
    };
  }

  if (source === "row_indices") {
    return {
      kind: "truncated",
      observed,
      declared,
      source,
      note:
        `Showing ${observed} of ${declared} flagged cells. Per-cell records were unavailable, so ` +
        "these were recovered from issue row indices, which the API caps at 50 per group.",
    };
  }

  // root_causes and flagged_cells are documented as covering every detected cell.
  // If either is short of the declared count, say so rather than presenting a
  // partial map as whole.
  if (declared > 0 && observed < declared) {
    return {
      kind: "truncated",
      observed,
      declared,
      source,
      note: `Showing ${observed} of ${declared} flagged cells; ${declared - observed} could not be located.`,
    };
  }

  return {
    kind: "complete",
    observed,
    declared,
    source,
    note: anyGroupTruncated
      ? `All ${observed} flagged cells are located. (Issue row lists are capped for display, but the per-cell record is complete.)`
      : `All ${observed} flagged cells are located.`,
  };
}

function resolveAbsence(
  coverage: Coverage,
  rows: number,
  columnCount: number,
): { absence: AbsenceState | null; absenceText: string } {
  if (rows === 0 || columnCount === 0) {
    return {
      absence: "not_measured",
      absenceText: "No table has been analysed yet.",
    };
  }
  if (coverage.kind === "not_measured") {
    return { absence: "not_measured", absenceText: coverage.note };
  }
  if (coverage.observed === 0) {
    return { absence: "zero", absenceText: coverage.note };
  }
  // Truncated coverage still draws marks; the absence is partial, not total, and
  // is surfaced by the coverage note beside the marks.
  return { absence: coverage.kind === "truncated" ? "truncated" : null, absenceText: coverage.note };
}
