/** Evidence tables and the selection-driven detail dock. */
import { ConfidenceBadge, EmptyState, RiskBadge, SeverityBadge } from "../components/primitives";
import { formatRows, problemToMessage } from "../csv";
import { motionDurations, panelVariants } from "../motion";
import { formatLabel, formatPercent, humanizeProvenance, humanizeSafetyVerdict, humanizeVerifierVerdict, parseUnsatCore, shortHash } from "../observatory";
import type { InstrumentTone, SelectedEvidence } from "../observatory";
import type { AnalyzeResponse, ConstraintCandidate, CsvPreview, IssueGroup, ProblemDetail, RepairReadiness, RiskLevel, Severity } from "../types";
import { failureKey, formatConstraintColumns, repairKey, toneClass, toneForSeverity } from "../ui/helpers";
import type { WorkflowStageView } from "../workflow";
import { ShieldCheck } from "lucide-react";
import { motion } from "motion/react";

export function CsvPreviewTable({ preview }: { preview: CsvPreview }) {
  return (
    <div className="table-frame" tabIndex={0} aria-label="CSV preview table">
      <table>
        <thead>
          <tr>
            {preview.columns.map((column) => (
              <th key={column} scope="col">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {preview.rows.map((row, index) => (
            <tr key={index}>
              {preview.columns.map((column) => (
                <td key={column}>{row[column]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {preview.truncated ? <p className="table-note">Showing the first five parsed rows.</p> : null}
    </div>
  );
}

export function RiskSummaryPanel({
  datasetLevel,
  readiness,
  reasons,
  label = "Risk reasons",
}: {
  datasetLevel: RiskLevel;
  readiness: RepairReadiness;
  reasons: string[];
  // Optional accessible name. Only one risk panel renders per page now -- pages compose it, and
  // a lens no longer smuggles a second copy in -- so this exists to name the panel for its
  // context, not to tell two identical landmarks apart.
  label?: string;
}) {
  return (
    <section className="risk-panel" aria-label={label}>
      <div className="risk-badge-row">
        <RiskBadge label="Dataset risk" value={datasetLevel} />
        <RiskBadge label="Repair readiness" value={readiness} />
      </div>
      <ul>
        {reasons.map((reason) => (
          <li key={reason}>{reason}</li>
        ))}
      </ul>
    </section>
  );
}

export function IssueTable({
  issues,
  onSelect,
}: {
  issues: IssueGroup[];
  onSelect: (selection: SelectedEvidence) => void;
}) {
  return (
    <div className="table-frame" tabIndex={0} aria-label="Grouped issue evidence">
      <table>
        <thead>
          <tr>
            <th scope="col">Column</th>
            <th scope="col">Issue type</th>
            <th scope="col">Severity</th>
            <th scope="col">Rows</th>
            <th scope="col">Count</th>
          </tr>
        </thead>
        <tbody>
          {issues.map((issue) => (
            <tr key={issue.key}>
              <td>
                <button type="button" className="cell-button" onClick={() => onSelect({ kind: "issue", id: issue.key })}>
                  <code>{issue.column}</code>
                </button>
              </td>
              <td>{issue.issue_type}</td>
              <td>
                <SeverityBadge severity={issue.severity} />
              </td>
              <td>{formatRows(issue.row_indices, issue.row_indices_truncated)}</td>
              <td>{issue.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ConstraintEvidenceTable({
  candidates,
  onSelect,
}: {
  candidates: ConstraintCandidate[];
  onSelect: (selection: SelectedEvidence) => void;
}) {
  return (
    <section className="evidence-section" aria-labelledby="constraint-review-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Assumptions</p>
          <h2 id="constraint-review-title">Constraint review</h2>
        </div>
        <span className="quiet-chip">{candidates.length} inferred</span>
      </div>
      {candidates.length === 0 ? (
        <EmptyState
          icon={<ShieldCheck aria-hidden="true" />}
          title="No inferred constraints"
          body="The schema inference pass did not emit reviewable candidates for this CSV."
        />
      ) : (
        <div className="table-frame" tabIndex={0} aria-label="Constraint review table">
          <table>
            <thead>
              <tr>
                <th scope="col">Kind</th>
                <th scope="col">Columns</th>
                <th scope="col">Confidence</th>
                <th scope="col">Decision</th>
                <th scope="col">Evidence</th>
              </tr>
            </thead>
            <tbody>
              {candidates.map((candidate) => (
                <tr key={candidate.candidate_id}>
                  <td>
                    <button
                      type="button"
                      className="cell-button"
                      onClick={() => onSelect({ kind: "constraint", id: candidate.candidate_id })}
                    >
                      <code>{formatLabel(candidate.kind)}</code>
                    </button>
                  </td>
                  <td>{formatConstraintColumns(candidate)}</td>
                  <td>
                    <ConfidenceBadge value={candidate.confidence} />
                  </td>
                  <td>{candidate.repair_supported ? candidate.decision : "unsupported"}</td>
                  <td>{candidate.evidence}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export function EvidenceDock({
  selectedEvidence,
  stages,
  analysis,
  issues,
  problem,
}: {
  selectedEvidence: SelectedEvidence | null;
  stages: WorkflowStageView[];
  analysis: AnalyzeResponse | null;
  issues: IssueGroup[];
  problem: ProblemDetail | null;
}) {
  const content = resolveDockContent(selectedEvidence, stages, analysis, issues, problem);
  return (
    <motion.aside
      className={`evidence-dock evidence-dock--${content.tone}`}
      aria-label="Evidence dock"
      layout
      variants={panelVariants}
      initial="initial"
      animate="animate"
      exit="exit"
    >
      <motion.div
        key={`${content.title}:${content.meta}:${content.tone}`}
        initial={{ opacity: 0, y: 4 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: motionDurations.standard, ease: "easeOut" }}
      >
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Evidence Dock</p>
            <h2>{content.title}</h2>
          </div>
          <span className="quiet-chip">{content.meta}</span>
        </div>
        <p>{content.detail}</p>
        {content.rows.length > 0 ? (
          <dl className="dock-facts">
            {content.rows.map((row) => (
              <div key={row.label}>
                <dt>{row.label}</dt>
                <dd>{row.value}</dd>
              </div>
            ))}
          </dl>
        ) : null}
      </motion.div>
    </motion.aside>
  );
}

export function resolveDockContent(
  selectedEvidence: SelectedEvidence | null,
  stages: WorkflowStageView[],
  analysis: AnalyzeResponse | null,
  issues: IssueGroup[],
  problem: ProblemDetail | null,
): {
  title: string;
  detail: string;
  meta: string;
  tone: InstrumentTone;
  rows: Array<{ label: string; value: string | number }>;
} {
  if (problem) {
    return {
      title: problem.title,
      detail: problemToMessage(problem),
      meta: String(problem.status),
      tone: "danger",
      rows: problem.error ? [{ label: "Error", value: String(problem.error) }] : [],
    };
  }
  if (selectedEvidence?.kind === "stage") {
    const stage = stages.find((candidate) => candidate.id === selectedEvidence.id);
    if (stage) {
      return {
        title: stage.label,
        detail: stage.summary || stage.description,
        meta: formatLabel(stage.status),
        tone: toneClass(stage.status),
        rows: [
          ...Object.entries(stage.counts ?? {}).map(([label, value]) => ({
            label: formatLabel(label),
            value: String(value),
          })),
          ...(stage.confidence === undefined ? [] : [{ label: "Confidence", value: formatPercent(stage.confidence) }]),
          ...(stage.uncertainty ? [{ label: "Uncertainty", value: stage.uncertainty }] : []),
        ],
      };
    }
  }
  if (analysis && selectedEvidence?.kind === "constraint") {
    const candidate = analysis.schema_inference.candidates.find((item) => item.candidate_id === selectedEvidence.id);
    if (candidate) {
      return {
        title: formatLabel(candidate.kind),
        detail: candidate.evidence,
        meta: candidate.repair_supported ? candidate.decision : "unsupported",
        tone: candidate.decision === "accepted" ? "verified" : "review",
        rows: [
          { label: "Columns", value: formatConstraintColumns(candidate) },
          { label: "Confidence", value: formatPercent(candidate.confidence) },
          { label: "Repair supported", value: candidate.repair_supported ? "yes" : "no" },
        ],
      };
    }
  }
  if (analysis && selectedEvidence?.kind === "issue") {
    const issue = issues.find((item) => item.key === selectedEvidence.id);
    if (issue) {
      return {
        title: formatLabel(issue.issue_type),
        detail: `Rows ${formatRows(issue.row_indices, issue.row_indices_truncated)} in ${issue.column}.`,
        meta: issue.severity,
        tone: toneForSeverity(issue.severity),
        rows: [
          { label: "Count", value: issue.count },
          { label: "Column", value: issue.column },
        ],
      };
    }
  }
  if (analysis && selectedEvidence?.kind === "repair") {
    const fix = analysis.repairs.find((item) => repairKey(item) === selectedEvidence.id);
    if (fix) {
      return {
        title: `Row ${fix.row}, ${fix.column}`,
        detail: fix.reason,
        meta: humanizeProvenance(fix.provenance),
        tone: "verified",
        rows: [
          { label: "Current", value: fix.old_value || "(empty)" },
          { label: "Proposed", value: fix.new_value || "(empty)" },
          { label: "Confidence", value: formatPercent(fix.confidence) },
        ],
      };
    }
  }
  if (analysis && selectedEvidence?.kind === "proof") {
    const obligation = analysis.receipt.proof_obligations.find((item) => item.obligation_id === selectedEvidence.id);
    if (obligation) {
      return {
        title: obligation.obligation_id,
        detail: obligation.reason,
        meta: obligation.status,
        tone: obligation.status === "accepted" ? "verified" : "review",
        rows: [
          { label: "Verifier", value: obligation.verifier },
          {
            label: "Blocked by",
            value:
              parseUnsatCore(obligation.unsat_core)
                .map((attribution) => attribution.kindLabel)
                .join(", ") || "nothing (no constraint was violated)",
          },
        ],
      };
    }
  }
  if (analysis && selectedEvidence?.kind === "failure") {
    const failure = analysis.verification.failures.find((item) => failureKey(item) === selectedEvidence.id);
    if (failure) {
      return {
        title: formatLabel(failure.issue_type),
        detail: failure.reason,
        meta: failure.status,
        tone: "danger",
        rows: [
          { label: "Row", value: failure.row },
          { label: "Column", value: failure.column },
          { label: "Attempts", value: failure.attempt_count },
        ],
      };
    }
  }
  if (analysis && selectedEvidence?.kind === "receipt") {
    return {
      title: "Receipt boundary",
      detail: analysis.receipt.reason,
      meta: analysis.receipt.mode,
      tone: analysis.receipt.safety_verdict === "allow" ? "verified" : "danger",
      rows: [
        { label: "Transaction", value: analysis.txn_journal.txn_id },
        { label: "Source hash", value: shortHash(analysis.receipt.source_sha256) },
        { label: "Verifier", value: humanizeVerifierVerdict(analysis.receipt.verifier_verdict) },
        { label: "Applied", value: analysis.receipt.applied ? "yes" : "no" },
      ],
    };
  }
  return {
    title: "Operating boundary",
    detail: "The hosted playground is stateless, dry-run only, and leaves apply/revert under local human control.",
    meta: "bounded",
    tone: "neutral",
    rows: analysis
      ? [
          { label: "Contract", value: analysis.meta.contract_version },
          { label: "Safety", value: humanizeSafetyVerdict(analysis.receipt.safety_verdict) },
          { label: "Verifier", value: humanizeVerifierVerdict(analysis.receipt.verifier_verdict) },
        ]
      : [],
  };
}
