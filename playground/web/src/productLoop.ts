import type { AnalyzeResponse, RepairFailure, VerifiedFix } from "./types";

export const SAFETY_REVERT_EXPLANATION = [
  "Hosted analysis is dry-run only and never mutates uploaded CSV files.",
  "Safety rules block row deletes, PII overwrites, conflicting cell writes, and unconfirmed risky edits.",
  "The verifier checks proposed repairs against known structural and accepted constraint evidence.",
  "Local apply writes a hash-chained transaction journal and immutable source snapshot before mutation.",
  "Local revert refuses if the file has drifted from the recorded post-state hash.",
];

export type PrimaryRepairMoment =
  | {
      kind: "verified";
      title: string;
      note: string;
      humanRow: number;
      rawRow: number;
      column: string;
      oldValue: string;
      newValue: string;
      detectorId: string;
      reason: string;
      confidence: number;
      verifierReason: string;
      safetyVerdict: string;
      verifierVerdict: string;
      sourceSha256: string;
      patchPlanSha256: string | null;
      txnId: string | null;
      fix: VerifiedFix;
    }
  | {
      kind: "abstention";
      title: string;
      note: string;
      humanRow: number;
      rawRow: number;
      column: string;
      issueType: string;
      status: string;
      reason: string;
      safetyVerdict: string;
      verifierVerdict: string;
      sourceSha256: string;
      failure: RepairFailure;
    }
  | {
      kind: "empty";
      title: string;
      note: string;
      safetyVerdict: string;
      verifierVerdict: string;
      sourceSha256: string;
    };

export function selectPrimaryRepairMoment(analysis: AnalyzeResponse): PrimaryRepairMoment {
  // Preference order: the most illustrative APPLIED repair, else the first one.
  //
  // This used to prefer `decimal_shift` on a `rating` column, then any `decimal_shift`,
  // then `repairs[0]`. Both `decimal_shift` branches became unreachable on 2026-08-22:
  // that detector's value is inferred from the shape of the column's own distribution,
  // so it is never auto-applied and never appears in `analysis.repairs` (see
  // specs/SPEC_autoapply_decision.md). The chain silently fell through to `repairs[0]`,
  // which is why the rendered note stayed correct while the stated intent was dead.
  //
  // Preferring `fd_violation` instead: it is constraint-checkable, it is the repair the
  // bundled premised fixture demonstrates, and unlike `type_mismatch` -- whose fix blanks
  // an unparseable cell -- it replaces a wrong value with a right one, which is the
  // clearer thing to show someone first.
  const preferredFix =
    analysis.repairs.find((fix) => fix.detector_id === "fd_violation") ??
    analysis.repairs[0];

  if (preferredFix) {
    const humanRow = toHumanRow(preferredFix.row);
    return {
      kind: "verified",
      title: "Verified repair",
      note: `Row ${humanRow} ${preferredFix.column}: ${formatCellValue(
        preferredFix.old_value,
      )} -> ${formatCellValue(preferredFix.new_value)} passed safety and verifier gates.`,
      humanRow,
      rawRow: preferredFix.row,
      column: preferredFix.column,
      oldValue: preferredFix.old_value,
      newValue: preferredFix.new_value,
      detectorId: preferredFix.detector_id,
      reason: preferredFix.reason,
      confidence: preferredFix.confidence,
      verifierReason: preferredFix.verifier_reason ?? "Accepted by verifier.",
      safetyVerdict: analysis.verification.safety_verdict,
      verifierVerdict: analysis.verification.verifier_verdict,
      sourceSha256: analysis.source.sha256,
      patchPlanSha256: analysis.receipt.patch_plan_sha256 ?? null,
      txnId: analysis.txn_journal.txn_id ?? analysis.receipt.txn_id ?? null,
      fix: preferredFix,
    };
  }

  const failure = analysis.verification.failures[0];
  if (failure) {
    const humanRow = toHumanRow(failure.row);
    return {
      kind: "abstention",
      title: "Repair abstained",
      note: `Row ${humanRow} ${failure.column}: ${failure.issue_type} was detected, but no repair passed the gates.`,
      humanRow,
      rawRow: failure.row,
      column: failure.column,
      issueType: failure.issue_type,
      status: failure.status,
      reason: failure.reason,
      safetyVerdict: analysis.verification.safety_verdict,
      verifierVerdict: analysis.verification.verifier_verdict,
      sourceSha256: analysis.source.sha256,
      failure,
    };
  }

  return {
    kind: "empty",
    title: "No repair needed",
    note: "The dry run did not produce a verified repair or repair abstention.",
    safetyVerdict: analysis.verification.safety_verdict,
    verifierVerdict: analysis.verification.verifier_verdict,
    sourceSha256: analysis.source.sha256,
  };
}

export function localCommands(analysis: AnalyzeResponse) {
  return {
    dry_run: analysis.apply_handoff.dry_run_command,
    apply: analysis.apply_handoff.apply_command,
    audit: analysis.apply_handoff.audit_command,
    revert: analysis.apply_handoff.revert_command,
  };
}

export function toHumanRow(rawRow: number): number {
  return rawRow + 1;
}

function formatCellValue(value: string): string {
  return value.trim().length === 0 ? "(blank)" : value;
}
