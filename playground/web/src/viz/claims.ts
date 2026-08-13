import { isProvenRung, rungSpecs, type Rung } from "./grammar";
import { UNTRUSTED_PROVENANCE } from "../observatory";
import type { EvidenceModel, FlaggedCell } from "./model";

/**
 * The claim encoder: one addressable object per claim.
 *
 * This is the half of the system that IS allowed to carry epistemic strength,
 * because each mark here stands for exactly one claim and is rendered as a real DOM
 * element -- focusable, labelled, and operable -- at or above
 * `addressableMinHeightPx`. That is what makes the rung, the form, and the earned
 * depth lawful under L2 and L5.
 *
 * Bounded by construction: callers select a column, a row range, or a single
 * outcome list, all of which are small. There is no aggregation here at all, so
 * none of the collision problems that plagued the overview can arise.
 */

export interface Claim {
  key: string;
  row: number;
  column: string;
  rung: Rung;
  issueType: string;
  confidence: number | null;
  reviewReason: string | null;
  detail: string;
  provenance: string | null;
  /** Constraint labels from the verifier, when this claim was blocked. */
  unsatCore: string[];
  oldValue: string | null;
  newValue: string | null;
}

export interface ClaimSelection {
  column?: string;
  rowStart?: number;
  rowEnd?: number;
}

export interface ClaimSet {
  claims: Claim[];
  selection: ClaimSelection | null;
  /** Claims matching the selection before the display cap. */
  matched: number;
  truncated: boolean;
  absence: "zero" | "not_measured" | null;
  absenceText: string;
  countsByRung: Record<Rung, number>;
}

/** A detail view is a bounded view. Beyond this, narrow the selection. */
export const CLAIM_DISPLAY_LIMIT = 200;

const emptyCounts = (): Record<Rung, number> => ({
  corroborated: 0,
  proven: 0,
  held: 0,
  downgraded: 0,
  plausibility_only: 0,
  rejected: 0,
  idle: 0,
});

function claimFromCell(cell: FlaggedCell): Claim {
  return {
    key: `${cell.row}\u0000${cell.column}`,
    row: cell.row,
    column: cell.column,
    rung: cell.rung,
    issueType: cell.issueType,
    confidence: cell.confidence,
    reviewReason: cell.reviewReason,
    detail: cell.detail,
    provenance: cell.provenance,
    unsatCore: cell.unsatCore ?? [],
    oldValue: cell.oldValue ?? null,
    newValue: cell.newValue ?? null,
  };
}

/**
 * Build the claim set for a selection.
 *
 * Ordering is by descending epistemic strength, then position. Strongest first is
 * deliberate: the question a detail view answers is "what happened here", and the
 * proven outcomes are the ones a user can act on immediately. This is a sort order,
 * not a salience claim, so it does not interact with the earned-salience ladder.
 */
export function buildClaimSet(model: EvidenceModel, selection: ClaimSelection | null): ClaimSet {
  const matching = model.cells.filter((cell) => {
    if (selection === null) {
      return true;
    }
    if (selection.column !== undefined && cell.column !== selection.column) {
      return false;
    }
    if (selection.rowStart !== undefined && cell.row < selection.rowStart) {
      return false;
    }
    if (selection.rowEnd !== undefined && cell.row > selection.rowEnd) {
      return false;
    }
    return true;
  });

  const ordered = [...matching].sort(
    (a, b) =>
      rungSpecs[b.rung].strength - rungSpecs[a.rung].strength ||
      a.row - b.row ||
      a.column.localeCompare(b.column),
  );

  const shown = ordered.slice(0, CLAIM_DISPLAY_LIMIT);
  const countsByRung = emptyCounts();
  for (const cell of shown) {
    countsByRung[cell.rung] += 1;
  }

  let absence: ClaimSet["absence"] = null;
  let absenceText = "";
  if (model.rows === 0 || model.columns.length === 0) {
    absence = "not_measured";
    absenceText = "No table has been analysed yet.";
  } else if (ordered.length === 0) {
    absence = "zero";
    absenceText =
      selection === null
        ? "No cells were flagged. This is a measured result, not a missing one."
        : "No flagged cells in this selection. This is a measured result, not a missing one.";
  }

  return {
    claims: shown.map(claimFromCell),
    selection,
    matched: ordered.length,
    truncated: ordered.length > shown.length,
    absence,
    absenceText,
    countsByRung,
  };
}

/**
 * Overtrust check on the claim set.
 *
 * This re-derives the expected strength from PROVENANCE, independently of the rung
 * the encoder assigned, and fails when they disagree. That independence is the whole
 * point: an earlier version of this function inspected only `rungSpecs[claim.rung]`,
 * which is tautological -- it can only re-derive the rung from the rung, so it could
 * never detect a mislabelled claim and no mutation could kill it.
 */
export function claimSetViolations(set: ClaimSet): string[] {
  const problems: string[] = [];
  for (const claim of set.claims) {
    const spec = rungSpecs[claim.rung];
    const proven = isProvenRung(claim.rung);

    // The load-bearing assertion: an untrusted provenance may never be proven.
    if (proven && claim.provenance !== null && UNTRUSTED_PROVENANCE.has(claim.provenance)) {
      problems.push(
        `claim at row ${claim.row} has untrusted provenance '${claim.provenance}' but is ` +
          `labelled '${claim.rung}'`,
      );
    }

    // Form must match the rung it claims, so a hand-built set cannot dress an
    // unproven claim in proven treatment.
    if (!proven && (spec.glowEligible || spec.groundContact === "contact")) {
      problems.push(`unproven claim at row ${claim.row} would wear proven treatment`);
    }
    if (proven && spec.groundContact !== "contact") {
      problems.push(`proven claim at row ${claim.row} would not make ground contact`);
    }
  }
  return problems;
}
