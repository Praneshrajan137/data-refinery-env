/** Receipt summary and the local apply/revert handoff. */
import { CommandRow, Metric } from "../components/primitives";
import { motionDurations, panelVariants } from "../motion";
import {
  humanizeIndependentVerification,
  humanizeSafetyVerdict,
  humanizeVerifierVerdict,
} from "../observatory";
import type { AnalyzeResponse } from "../types";
import { motion } from "motion/react";

export function ReceiptSummary({ analysis }: { analysis: AnalyzeResponse }) {
  return (
    <motion.section
      className="receipt-summary"
      aria-label="Repair receipt summary"
      variants={panelVariants}
      initial="initial"
      animate="animate"
      exit="exit"
    >
      <div className="metric-grid metric-grid--four">
        <Metric label="Safety" value={humanizeSafetyVerdict(analysis.receipt.safety_verdict)} />
        <Metric label="Verifier" value={humanizeVerifierVerdict(analysis.receipt.verifier_verdict)} />
        <Metric
          label="Independent verify"
          value={humanizeIndependentVerification(analysis.receipt.independent_verification)}
        />
        <Metric label="Reversible" value={analysis.receipt.reversible ? "yes" : "no"} />
      </div>
      <p>{analysis.receipt.reason}</p>
    </motion.section>
  );
}

export function ReceiptHandoff({ analysis }: { analysis: AnalyzeResponse }) {
  return (
    <motion.section
      className="handoff-panel"
      aria-labelledby="handoff-title"
      variants={panelVariants}
      initial="initial"
      animate="animate"
      exit="exit"
      transition={{ delay: 0.04, duration: motionDurations.standard }}
    >
      <div>
        <p className="eyebrow">Handoff Capsule</p>
        <h2 id="handoff-title">Local transaction boundary</h2>
      </div>
      <div className="command-list">
        <CommandRow label="Dry run" command={analysis.apply_handoff.dry_run_command} delay={0.04} />
        <CommandRow label="Apply" command={analysis.apply_handoff.apply_command} delay={0.08} />
        <CommandRow label="Audit" command={analysis.apply_handoff.audit_command} delay={0.12} />
        <CommandRow label="Revert" command={analysis.apply_handoff.revert_command} delay={0.16} />
      </div>
      <p>{analysis.apply_handoff.note}</p>
    </motion.section>
  );
}
