/** Before/after comparison of verified fixes and the candidates that did not qualify. */
import { VerificationStrengthBadge } from "../components/primitives";
import { motionDurations, motionSprings } from "../motion";
import { formatLabel, formatPercent, humanizeProvenance, shortHash, strengthOf } from "../observatory";
import type { SelectedEvidence } from "../observatory";
import type { AnalyzeResponse, CandidateRepair, VerifiedFix } from "../types";
import { repairKey } from "../ui/helpers";
import { motion } from "motion/react";

export function RepairComparison({
  repairs,
  analysis,
  onSelect,
}: {
  repairs: VerifiedFix[];
  analysis: AnalyzeResponse;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  if (repairs.length === 0) {
    return null;
  }
  return (
    <section className="repair-comparison" aria-labelledby="repair-comparison-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Repair Comparison</p>
          <h2 id="repair-comparison-title">Verified cell changes</h2>
        </div>
        <span className="quiet-chip">{repairs.length} fixes</span>
      </div>
      <div className="repair-list">
        {repairs.map((fix) => (
          <motion.article
            className="repair-row"
            key={repairKey(fix)}
            layout
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            transition={motionSprings.soft}
          >
            <button type="button" className="repair-head" onClick={() => onSelect({ kind: "repair", id: repairKey(fix) })}>
              <span>
                Row {fix.row}, <code>{fix.column}</code>
              </span>
              <small>{fix.detector_id} - confidence {formatPercent(fix.confidence)} - source {shortHash(analysis.source.sha256)}</small>
            </button>
            <div className="repair-row__strength">
              <VerificationStrengthBadge strength={strengthOf(fix)} />
            </div>
            <div className="diff-grid">
              <motion.div
                className="diff-cell diff-cell--old"
                initial={{ opacity: 0.85, scale: 0.994 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.03, duration: motionDurations.fast }}
              >
                <span>Current</span>
                <code>{fix.old_value || "(empty)"}</code>
              </motion.div>
              <motion.div
                className="diff-cell diff-cell--new"
                initial={{ opacity: 0.85, scale: 0.994 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.08, duration: motionDurations.fast }}
              >
                <span>Proposed</span>
                <code>{fix.new_value || "(empty)"}</code>
              </motion.div>
            </div>
            <p>{fix.reason}</p>
            {fix.verifier_reason ? <p className="verifier-note">{fix.verifier_reason}</p> : null}
          </motion.article>
        ))}
      </div>
    </section>
  );
}

export function CandidateRepairList({ candidates }: { candidates: CandidateRepair[] }) {
  if (candidates.length === 0) {
    return null;
  }
  return (
    <section className="candidate-list" aria-labelledby="candidate-repairs-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Candidate Trail</p>
          <h2 id="candidate-repairs-title">Repairs considered</h2>
        </div>
        <span className="quiet-chip">{candidates.length} candidates</span>
      </div>
      {candidates.map((candidate) => (
        <article className="candidate-row" key={`${candidate.row}:${candidate.column}:${candidate.new_value}:${candidate.verifier_reason}`}>
          <strong>
            Row {candidate.row}, <code>{candidate.column}</code>
          </strong>
          <span>
              {formatLabel(candidate.detector_id)} - {formatLabel(candidate.operation)} -{" "}
              {humanizeProvenance(candidate.provenance)}
            </span>
          <div className="candidate-row__strength">
            <VerificationStrengthBadge strength={strengthOf(candidate)} />
          </div>
          <p>{candidate.verifier_reason}</p>
        </article>
      ))}
    </section>
  );
}
