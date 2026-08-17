/**
 * The five lenses.
 *
 * They are composed statically into pages rather than chosen by the user -- there is no tab
 * structure anywhere in the product -- which is why OverviewLens renders on both /run and
 * /evidence.
 */
import { ConstraintEvidenceTable, CsvPreviewTable, IssueTable, RiskSummaryPanel } from "./components/evidence";
import {
  DatasetBadge,
  EmptyState,
  EvidenceNote,
  LoadingState,
  Metric,
  VerificationStrengthLegend,
} from "./components/primitives";
import { ReceiptHandoff, ReceiptSummary } from "./components/receipt";
import { CandidateRepairList, RepairComparison } from "./components/repairs";
import { CertificatePanel, FailureList, HeldForReviewList } from "./components/trust";
import { motionDurations } from "./motion";
import { buildObservatoryView, humanizeVerifierVerdict, shortHash, strengthOf } from "./observatory";
import type { SelectedEvidence } from "./observatory";
import type { AnalyzeResponse, CsvPreview, DatasetInput, IssueGroup, Severity } from "./types";
import { downloadCertificate } from "./ui/helpers";
import type { SortKey, WorkState } from "./ui/helpers";
import { Activity, FileText, ShieldCheck, Wrench } from "lucide-react";
import { motion } from "motion/react";

export function OverviewLens({
  dataset,
  preview,
  analysis,
  observatory,
  onSelect,
}: {
  dataset: DatasetInput | null;
  preview: CsvPreview | null;
  analysis: AnalyzeResponse | null;
  observatory: ReturnType<typeof buildObservatoryView>;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  return (
    <div className="overview-lens">
      <section className={`run-posture run-posture--${observatory.runPosture.tone}`}>
        <div>
          <p className="eyebrow">Run Posture</p>
          <h2>{observatory.runPosture.title}</h2>
          <p>{observatory.runPosture.detail}</p>
        </div>
        <div className="metric-grid">
          {observatory.runPosture.metrics.map((metric) => (
            <Metric key={metric.label} label={metric.label} value={metric.value} />
          ))}
        </div>
      </section>

      <section className="dataset-panel" aria-labelledby="current-csv-title">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Source Preview</p>
            <h2 id="current-csv-title">Current CSV</h2>
          </div>
          <DatasetBadge dataset={dataset} />
        </div>
        {preview ? (
          <CsvPreviewTable preview={preview} />
        ) : (
          <EmptyState
            icon={<FileText aria-hidden="true" />}
            title="No dataset loaded"
            body="Choose a sample or upload a CSV to inspect rows before backend analysis."
          />
        )}
      </section>

      <section className="evidence-map" aria-labelledby="evidence-map-title">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Evidence Map</p>
            <h2 id="evidence-map-title">What the system knows</h2>
          </div>
          <span className="quiet-chip">{observatory.evidenceGroups.length} groups</span>
        </div>
        <div className="evidence-grid">
          {observatory.evidenceGroups.map((group) => (
            <button
              key={group.id}
              type="button"
              className={`evidence-tile evidence-tile--${group.tone}`}
              onClick={() => onSelect({ kind: group.id === "receipt" ? "receipt" : "stage", id: group.id })}
            >
              <span>{group.count}</span>
              <strong>{group.title}</strong>
              <p>{group.detail}</p>
            </button>
          ))}
        </div>
      </section>
      {/*
        The risk summary is NOT rendered here.
        It used to be, and RiskLens renders one too, so /evidence showed two identical panels
        and the accepted mitigation was to give them different accessible names -- labelling a
        duplicate rather than removing it. Shared panels are now composed by the PAGE, which can
        see what else is on it; a lens cannot. RunPage renders the risk summary explicitly
        because it has no RiskLens.
      */}
    </div>
  );
}

export function RiskLens({
  state,
  analysis,
  issues,
  filter,
  severityFilter,
  sortKey,
  onFilterChange,
  onSeverityFilterChange,
  onSortChange,
  onSelect,
}: {
  state: WorkState;
  analysis: AnalyzeResponse | null;
  issues: IssueGroup[];
  filter: string;
  severityFilter: Severity | "all";
  sortKey: SortKey;
  onFilterChange: (value: string) => void;
  onSeverityFilterChange: (value: Severity | "all") => void;
  onSortChange: (value: SortKey) => void;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  if (state === "loading") {
    return <LoadingState label="Analyzing CSV" />;
  }
  if (!analysis) {
    return (
      <EmptyState
        icon={<Activity aria-hidden="true" />}
        title="Analysis evidence appears here"
        body="Run Analyze to see risk, inferred constraints, verified repairs, and the dry-run receipt."
      />
    );
  }
  return (
    <div className="risk-lens">
      <div className="metric-grid metric-grid--four" aria-label="Risk summary">
        <Metric label="Rows" value={analysis.source.rows} />
        <Metric label="Columns" value={analysis.source.columns} />
        <Metric label="Issues" value={analysis.receipt.issues_count} />
        <Metric label="Pending constraints" value={analysis.risk_summary.pending_repair_supported_constraints} />
      </div>
      <RiskSummaryPanel
        datasetLevel={analysis.risk_summary.dataset_level}
        readiness={analysis.risk_summary.repair_readiness}
        reasons={analysis.risk_summary.reasons}
      />

      <div className="filter-row">
        <label>
          <span>Filter</span>
          <input
            type="search"
            value={filter}
            placeholder="Column or issue type"
            onChange={(event) => onFilterChange(event.target.value)}
          />
        </label>
        <label>
          <span>Severity</span>
          <select
            value={severityFilter}
            onChange={(event) => onSeverityFilterChange(event.target.value as Severity | "all")}
          >
            <option value="all">All severities</option>
            <option value="unsafe">Unsafe</option>
            <option value="review">Review</option>
            <option value="safe">Safe</option>
          </select>
        </label>
        <label>
          <span>Sort</span>
          <select value={sortKey} onChange={(event) => onSortChange(event.target.value as SortKey)}>
            <option value="severity">Severity</option>
            <option value="count">Count</option>
            <option value="column">Column</option>
          </select>
        </label>
      </div>

      {issues.length === 0 ? (
        <EmptyState
          icon={<ShieldCheck aria-hidden="true" />}
          title="No matching issues"
          body="Adjust the filters or analyze another dataset."
        />
      ) : (
        <IssueTable issues={issues} onSelect={onSelect} />
      )}

      <ConstraintEvidenceTable candidates={analysis.schema_inference.candidates} onSelect={onSelect} />
    </div>
  );
}

export function RepairsLens({
  state,
  analysis,
  dataset,
  onSelect,
}: {
  state: WorkState;
  analysis: AnalyzeResponse | null;
  dataset: DatasetInput | null;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  if (state === "loading") {
    return <LoadingState label="Verifying repair proposals" />;
  }
  if (!analysis) {
    return (
      <EmptyState
        icon={<Wrench aria-hidden="true" />}
        title="Verified repairs appear here"
        body={
          dataset
            ? "Run Analyze to inspect proposed changes, verifier evidence, and non-repairs."
            : "Load a sample or upload a CSV before requesting repair evidence."
        }
      />
    );
  }
  return (
    <div className="repairs-lens">
      <div className="metric-grid metric-grid--four" aria-label="Verification summary">
        <Metric label="Proven, would apply" value={analysis.repairs.filter((fix) => strengthOf(fix) === "proven").length} />
        <Metric label="Held for review" value={analysis.receipt.suggested_fixes?.length ?? 0} />
        <Metric label="Verifier" value={humanizeVerifierVerdict(analysis.verification.verifier_verdict)} />
        <Metric label="Attempted not fixed" value={analysis.verification.failures.length} />
      </div>
      <EvidenceNote
        title={analysis.repairs.length > 0 ? "Verified dry-run evidence" : "No verified repairs were proposed"}
        body={
          analysis.repairs.length > 0
            ? "Every listed fix passed the hosted safety and verifier gates. Its proof strength is shown per fix."
            : "The dry-run pipeline did not find a candidate that passed safety and verifier gates."
        }
      />
      <VerificationStrengthLegend />
      <RepairComparison repairs={analysis.repairs} analysis={analysis} onSelect={onSelect} />
      <HeldForReviewList items={analysis.receipt.suggested_fixes ?? []} />
      <CandidateRepairList candidates={analysis.receipt.candidate_repairs} />
      <FailureList failures={analysis.verification.failures} onSelect={onSelect} titleId="repair-failures-title" />
    </div>
  );
}

export function ReceiptLens({
  analysis,
  onSelect,
}: {
  analysis: AnalyzeResponse | null;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  if (!analysis) {
    return (
      <EmptyState
        icon={<ShieldCheck aria-hidden="true" />}
        title="No receipt yet"
        body="A dry-run receipt and local apply handoff are shown after analysis completes."
      />
    );
  }
  return (
    <div className="receipt-lens">
      <ReceiptSummary analysis={analysis} />
      <CertificatePanel
        certificate={analysis.certificate}
        independentVerification={analysis.receipt.independent_verification ?? "not_run"}
        auditCommand={analysis.apply_handoff.audit_command}
        onDownload={() => downloadCertificate(analysis)}
      />
      <ReceiptHandoff analysis={analysis} />
      <div className="hash-grid">
        <motion.button
          type="button"
          onClick={() => onSelect({ kind: "receipt", id: "source" })}
          initial={{ opacity: 0, y: 3 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.02, duration: motionDurations.fast }}
        >
          <span>Source hash</span>
          <code>{shortHash(analysis.receipt.source_sha256)}</code>
        </motion.button>
        <motion.button
          type="button"
          onClick={() => onSelect({ kind: "receipt", id: "patch" })}
          initial={{ opacity: 0, y: 3 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.05, duration: motionDurations.fast }}
        >
          <span>Patch plan</span>
          <code>{analysis.receipt.patch_plan_sha256 ? shortHash(analysis.receipt.patch_plan_sha256) : "none"}</code>
        </motion.button>
        <motion.button
          type="button"
          onClick={() => onSelect({ kind: "receipt", id: "constraints" })}
          initial={{ opacity: 0, y: 3 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.08, duration: motionDurations.fast }}
        >
          <span>Accepted constraints</span>
          <code>{analysis.receipt.accepted_constraint_ids.length}</code>
        </motion.button>
        <motion.button
          type="button"
          onClick={() => onSelect({ kind: "receipt", id: "txn" })}
          initial={{ opacity: 0, y: 3 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.11, duration: motionDurations.fast }}
        >
          <span>Transaction</span>
          <code>{analysis.txn_journal.txn_id}</code>
        </motion.button>
      </div>
      <ul className="limitations" aria-label="Playground limitations">
        {analysis.limitations.map((limitation) => (
          <li key={limitation}>{limitation}</li>
        ))}
      </ul>
      <div className="json-grid">
        <pre tabIndex={0} aria-label="Dry-run transaction journal">
          {JSON.stringify(analysis.txn_journal, null, 2)}
        </pre>
        <pre tabIndex={0} aria-label="Repair receipt">
          {JSON.stringify(analysis.receipt, null, 2)}
        </pre>
      </div>
    </div>
  );
}

export function RawEvidenceLens({
  analysis,
  evidenceText,
}: {
  analysis: AnalyzeResponse | null;
  evidenceText: string;
}) {
  if (!analysis || !evidenceText) {
    return (
      <EmptyState
        icon={<FileText aria-hidden="true" />}
        title="Raw evidence is unavailable"
        body="Run Analyze to generate the deterministic export payload."
      />
    );
  }
  return (
    <div className="raw-lens">
      <EvidenceNote
        title="Deterministic repair evidence"
        body="This payload includes source facts, assumptions, issues, repairs, verification, receipt, and local handoff."
      />
      <textarea aria-label="Copyable repair evidence" readOnly value={evidenceText} />
    </div>
  );
}
