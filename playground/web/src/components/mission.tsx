/** Run controls, dataset intake, and the six-step product loop. */
import { formatRows } from "../csv";
import { formatLabel, formatPercent, humanizeSafetyVerdict, humanizeVerifierVerdict, shortHash } from "../observatory";
import { SAFETY_REVERT_EXPLANATION, localCommands } from "../productLoop";
import type { PrimaryRepairMoment } from "../productLoop";
import type { ProductRouteId } from "../routes";
import type { AnalyzeResponse, BackendCapability, DatasetInput, RepairMode } from "../types";
import { SAMPLE_OPTIONS } from "../ui/helpers";
import type { WorkState } from "../ui/helpers";
import { AlertTriangle, CheckCircle2, ClipboardCopy, Database, Download, PauseCircle, Play, RefreshCw, Upload } from "lucide-react";
import type { ChangeEvent, RefObject } from "react";

export function MissionBar({
  dataset,
  busy,
  canRun,
  maxUploadBytes,
  capability,
  advanced,
  repairMode,
  allowEntityConsensus,
  backendState,
  streamingEnabled,
  acceptedConstraintIds,
  analysisState,
  hasEvidence,
  copyState,
  fileInputRef,
  onAdvancedChange,
  onRepairModeChange,
  onEntityConsensusChange,
  onChooseSample,
  onFileChange,
  onAnalyze,
  onRerun,
  onCancel,
  onCopy,
  onExport,
  onBackendRetry,
}: {
  dataset: DatasetInput | null;
  busy: boolean;
  canRun: boolean;
  maxUploadBytes: number;
  capability: BackendCapability | null;
  advanced: boolean;
  repairMode: RepairMode;
  allowEntityConsensus: boolean;
  backendState: WorkState;
  streamingEnabled: boolean;
  acceptedConstraintIds: string[];
  analysisState: WorkState;
  hasEvidence: boolean;
  copyState: "idle" | "copied" | "failed";
  fileInputRef: RefObject<HTMLInputElement | null>;
  onAdvancedChange: (next: boolean) => void;
  onRepairModeChange: (next: RepairMode) => void;
  onEntityConsensusChange: (next: boolean) => void;
  onChooseSample: (sampleName: string) => void | Promise<void>;
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void | Promise<void>;
  onAnalyze: () => void;
  onRerun: () => void;
  onCancel: () => void;
  onCopy: () => void;
  onExport: () => void;
  onBackendRetry: () => void;
}) {
  return (
    <section className="mission-bar" aria-label="DataForge mission bar">
      <div className="mission-identity">
        <span className="product-mark" aria-hidden="true">DF</span>
        <div>
          <p className="eyebrow">DataForge Run</p>
          <h1>CSV repair workbench</h1>
          <p>One CSV, one verified before/after, one exportable receipt.</p>
        </div>
      </div>

      <DatasetIntake
        dataset={dataset}
        busy={busy}
        fileInputRef={fileInputRef}
        onChooseSample={onChooseSample}
        onFileChange={onFileChange}
      />

      <div className="mission-controls">
        <div className="operating-marks" aria-label="Playground operating constraints">
          <span>Stateless dry run</span>
          <span>{streamingEnabled ? "Workflow stream" : "JSON fallback"}</span>
          <span>{Math.floor(maxUploadBytes / 1024)} KiB CSV cap</span>
          <BackendStatus state={backendState} capability={capability} onRetry={onBackendRetry} />
        </div>

        <label className="switch-row" htmlFor="advanced-mode">
          <span>
            <strong>Advanced</strong>
            <small>{capability?.advanced_available ? "Provider available" : "Unavailable"}</small>
          </span>
          <input
            id="advanced-mode"
            type="checkbox"
            role="switch"
            checked={advanced}
            disabled={busy || !capability?.advanced_available}
            onChange={(event) => onAdvancedChange(event.target.checked)}
          />
        </label>

        <label className="switch-row" htmlFor="agent-mode">
          <span>
            <strong>Agent</strong>
            <small>
              {capability?.agent_available
                ? capability.agent_provider === "azure"
                  ? "Frontier model (Azure), verified (dry run)"
                  : "Trained model, verified (dry run)"
                : "Unavailable"}
            </small>
          </span>
          <input
            id="agent-mode"
            type="checkbox"
            role="switch"
            checked={repairMode === "agent"}
            disabled={busy || !capability?.agent_available}
            onChange={(event) =>
              onRepairModeChange(event.target.checked ? "agent" : "deterministic")
            }
          />
        </label>

        <label className="switch-row" htmlFor="entity-consensus">
          <span>
            <strong>Cross-row consensus</strong>
            <small>
              {capability?.entity_consensus_available
                ? "Suggest fixes from an entity's sibling rows (review-only)"
                : "Unavailable"}
            </small>
          </span>
          <input
            id="entity-consensus"
            type="checkbox"
            role="switch"
            checked={allowEntityConsensus}
            disabled={busy || !capability?.entity_consensus_available}
            onChange={(event) => onEntityConsensusChange(event.target.checked)}
          />
        </label>

        <div className="run-actions">
          {analysisState === "loading" ? (
            <button className="danger-action" type="button" onClick={onCancel}>
              <PauseCircle aria-hidden="true" />
              Cancel run
            </button>
          ) : (
            <button className="primary-action" type="button" disabled={!canRun} onClick={onAnalyze}>
              <Play aria-hidden="true" />
              Analyze
            </button>
          )}
          <button
            className="secondary-action"
            type="button"
            disabled={!canRun || acceptedConstraintIds.length === 0}
            onClick={onRerun}
          >
            <RefreshCw aria-hidden="true" />
            Rerun with accepted constraints
          </button>
        </div>

        <div className="evidence-actions" aria-label="Evidence actions">
          <button className="icon-button" type="button" disabled={!hasEvidence} onClick={onCopy}>
            <ClipboardCopy aria-hidden="true" />
            {copyState === "copied" ? "Copied" : copyState === "failed" ? "Copy failed" : "Copy"}
          </button>
          <button className="icon-button" type="button" disabled={!hasEvidence} onClick={onExport}>
            <Download aria-hidden="true" />
            Export
          </button>
        </div>
      </div>
    </section>
  );
}

export function DatasetIntake({
  dataset,
  busy,
  fileInputRef,
  onChooseSample,
  onFileChange,
}: {
  dataset: DatasetInput | null;
  busy: boolean;
  fileInputRef: RefObject<HTMLInputElement | null>;
  onChooseSample: (sampleName: string) => void | Promise<void>;
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void | Promise<void>;
}) {
  return (
    <div className="mission-intake" aria-label="Dataset intake">
      <label className="file-intake" htmlFor="csv-upload">
        <Upload aria-hidden="true" />
        <span>
          <strong>Upload CSV</strong>
          <small>{dataset?.source === "upload" ? dataset.file.name : "Local preview only"}</small>
        </span>
        <input
          id="csv-upload"
          ref={fileInputRef}
          type="file"
          accept=".csv,text/csv"
          disabled={busy}
          onChange={onFileChange}
        />
      </label>
      <div className="sample-strip" aria-label="Sample datasets">
        {SAMPLE_OPTIONS.map((sample) => (
          <button
            className="sample-chip"
            type="button"
            key={sample.value}
            disabled={busy}
            onClick={() => void onChooseSample(sample.value)}
          >
            <Database aria-hidden="true" />
            <span>
              <strong>{sample.label}</strong>
              <small>{sample.detail}</small>
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

export function BackendStatus({
  state,
  capability,
  onRetry,
}: {
  state: WorkState;
  capability: BackendCapability | null;
  onRetry: () => void;
}) {
  if (state === "loading") {
    return (
      <span className="status-chip status-chip--active" role="status" aria-live="polite">
        <RefreshCw aria-hidden="true" />
        Warming backend
      </span>
    );
  }
  if (state === "error") {
    return (
      <button className="status-chip status-chip--danger" type="button" onClick={onRetry}>
        <AlertTriangle aria-hidden="true" />
        Backend unavailable
      </button>
    );
  }
  return (
    <span className="status-chip status-chip--verified" role="status" aria-live="polite">
      <CheckCircle2 aria-hidden="true" />
      {capability?.advanced_available ? "Ready with advanced" : "Ready"}
    </span>
  );
}

export function ProductLoopRail({
  dataset,
  analysis,
  primaryMoment,
}: {
  dataset: DatasetInput | null;
  analysis: AnalyzeResponse | null;
  primaryMoment: PrimaryRepairMoment | null;
}) {
  const steps = [
    {
      label: "Upload",
      detail: dataset ? dataset.file.name : "Choose Hospital or upload CSV",
      state: dataset ? "complete" : "active",
    },
    {
      label: "Profile",
      detail: analysis ? `${analysis.source.rows} rows, ${analysis.source.columns} columns` : "Infer schema and facts",
      state: analysis ? "complete" : dataset ? "active" : "pending",
    },
    {
      label: "Issues",
      detail: analysis ? `${analysis.receipt.issues_count} issue group(s)` : "Find risky cells",
      state: analysis ? (analysis.receipt.issues_count > 0 ? "review" : "complete") : "pending",
    },
    {
      label: "Repairs",
      detail:
        primaryMoment?.kind === "verified"
          ? `${primaryMoment.oldValue} -> ${primaryMoment.newValue}`
          : primaryMoment?.kind === "abstention"
            ? "Abstained safely"
            : "Review verified fixes",
      state:
        primaryMoment?.kind === "verified"
          ? "complete"
          : primaryMoment?.kind === "abstention"
            ? "review"
            : "pending",
    },
    {
      label: "Receipt",
      detail: analysis?.txn_journal.txn_id ?? "Export dry-run evidence",
      state: analysis ? "complete" : "pending",
    },
    {
      label: "Safety",
      detail: analysis
        ? `Safety ${humanizeSafetyVerdict(analysis.receipt.safety_verdict)}, verifier ${humanizeVerifierVerdict(analysis.receipt.verifier_verdict)}`
        : "Explain apply and revert",
      state: analysis ? "complete" : "pending",
    },
  ];

  return (
    <section className="product-loop-rail" aria-label="CSV repair loop">
      {steps.map((step, index) => (
        <div key={step.label} className={`loop-step loop-step--${step.state}`}>
          <span>{index + 1}</span>
          <strong>{step.label}</strong>
          <small>{step.detail}</small>
        </div>
      ))}
    </section>
  );
}

export function ProductLoopWorkbench({
  dataset,
  analysis,
  primaryMoment,
  hasEvidence,
  copyState,
  onCopy,
  onExport,
  onNavigate,
}: {
  dataset: DatasetInput | null;
  analysis: AnalyzeResponse | null;
  primaryMoment: PrimaryRepairMoment | null;
  hasEvidence: boolean;
  copyState: "idle" | "copied" | "failed";
  onCopy: () => void;
  onExport: () => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <section className="product-loop-workbench" aria-labelledby="product-loop-title">
      <div className="panel-heading product-loop-heading">
        <div>
          <p className="eyebrow">User-facing loop</p>
          <h2 id="product-loop-title">Upload CSV {"->"} profile {"->"} issues {"->"} verified repair {"->"} receipt {"->"} safe revert</h2>
        </div>
        <span className="quiet-chip">{analysis ? "receipt ready" : dataset ? "ready to analyze" : "waiting for CSV"}</span>
      </div>
      <div className="product-loop-grid">
        <ProfileSummary dataset={dataset} analysis={analysis} />
        <IssueReview analysis={analysis} onNavigate={onNavigate} />
        <VerifiedRepairReview analysis={analysis} primaryMoment={primaryMoment} onNavigate={onNavigate} />
        <ReceiptExport
          analysis={analysis}
          primaryMoment={primaryMoment}
          hasEvidence={hasEvidence}
          copyState={copyState}
          onCopy={onCopy}
          onExport={onExport}
          onNavigate={onNavigate}
        />
      </div>
      <SafetyRevertExplainer analysis={analysis} />
    </section>
  );
}

export function ProfileSummary({
  dataset,
  analysis,
}: {
  dataset: DatasetInput | null;
  analysis: AnalyzeResponse | null;
}) {
  return (
    <article className="loop-panel loop-panel--profile" aria-labelledby="profile-summary-title">
      <p className="eyebrow">Profile</p>
      <h3 id="profile-summary-title">Current CSV</h3>
      {analysis ? (
        <dl className="loop-facts">
          <div>
            <dt>File</dt>
            <dd>{analysis.source.name}</dd>
          </div>
          <div>
            <dt>Shape</dt>
            <dd>{analysis.source.rows} rows x {analysis.source.columns} columns</dd>
          </div>
          <div>
            <dt>Source hash</dt>
            <dd>{shortHash(analysis.source.sha256)}</dd>
          </div>
        </dl>
      ) : dataset ? (
        <dl className="loop-facts">
          <div>
            <dt>File</dt>
            <dd>{dataset.file.name}</dd>
          </div>
          <div>
            <dt>Preview</dt>
            <dd>{dataset.preview.rows.length} rows, {dataset.preview.columns.length} columns</dd>
          </div>
          <div>
            <dt>Mode</dt>
            <dd>local preview before backend profile</dd>
          </div>
        </dl>
      ) : (
        <p>Choose the Hospital sample or upload a CSV to begin the proof loop.</p>
      )}
    </article>
  );
}

export function IssueReview({
  analysis,
  onNavigate,
}: {
  analysis: AnalyzeResponse | null;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  const issue = analysis?.issues[0];
  return (
    <article className="loop-panel loop-panel--issues" aria-labelledby="issue-review-title">
      <p className="eyebrow">Issues</p>
      <h3 id="issue-review-title">{analysis ? `${analysis.receipt.issues_count} issue group(s)` : "Issue review waits for Analyze"}</h3>
      {analysis && issue ? (
        <>
          <dl className="loop-facts">
            <div>
              <dt>First issue</dt>
              <dd>{formatLabel(issue.issue_type)}</dd>
            </div>
            <div>
              <dt>Column</dt>
              <dd>{issue.column}</dd>
            </div>
            <div>
              <dt>Rows</dt>
              <dd>{formatRows(issue.row_indices.map((row) => row + 1), issue.row_indices_truncated)}</dd>
            </div>
          </dl>
          <button type="button" className="loop-link" onClick={() => onNavigate("evidence")}>
            Open issue evidence
          </button>
        </>
      ) : analysis ? (
        <p>No detector issue groups were reported for this CSV.</p>
      ) : (
        <p>DataForge profiles the table before proposing any repair.</p>
      )}
    </article>
  );
}

export function VerifiedRepairReview({
  analysis,
  primaryMoment,
  onNavigate,
}: {
  analysis: AnalyzeResponse | null;
  primaryMoment: PrimaryRepairMoment | null;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <article className="loop-panel loop-panel--repair" aria-labelledby="verified-repair-title">
      <p className="eyebrow">Verified repair review</p>
      <h3 id="verified-repair-title">{primaryMoment?.title ?? "Before/after appears after Analyze"}</h3>
      {primaryMoment?.kind === "verified" ? (
        <>
          <div className="primary-repair-note" role="note">
            <strong>{primaryMoment.note}</strong>
            <span>{primaryMoment.detectorId} - confidence {formatPercent(primaryMoment.confidence)}</span>
          </div>
          <div className="diff-grid primary-diff" aria-label="Primary repair before and after">
            <div className="diff-cell diff-cell--old">
              <span>Before</span>
              <code>{primaryMoment.oldValue || "(blank)"}</code>
            </div>
            <div className="diff-cell diff-cell--new">
              <span>After</span>
              <code>{primaryMoment.newValue || "(blank)"}</code>
            </div>
          </div>
          <dl className="loop-facts">
            <div>
              <dt>Verifier</dt>
              <dd>{primaryMoment.verifierVerdict}</dd>
            </div>
            <div>
              <dt>Safety</dt>
              <dd>{primaryMoment.safetyVerdict}</dd>
            </div>
            <div>
              <dt>Source hash</dt>
              <dd>{shortHash(primaryMoment.sourceSha256)}</dd>
            </div>
          </dl>
          <p>{primaryMoment.verifierReason}</p>
        </>
      ) : primaryMoment?.kind === "abstention" ? (
        <>
          <div className="primary-repair-note primary-repair-note--review" role="note">
            <strong>{primaryMoment.note}</strong>
            <span>{primaryMoment.status}</span>
          </div>
          <p>{primaryMoment.reason}</p>
        </>
      ) : analysis ? (
        <p>No verified repair was needed for this dry run.</p>
      ) : (
        <p>The clearest verified cell change will be highlighted here.</p>
      )}
      <button type="button" className="loop-link" onClick={() => onNavigate("repairs")} disabled={!analysis}>
        Open repair details
      </button>
    </article>
  );
}

export function ReceiptExport({
  analysis,
  primaryMoment,
  hasEvidence,
  copyState,
  onCopy,
  onExport,
  onNavigate,
}: {
  analysis: AnalyzeResponse | null;
  primaryMoment: PrimaryRepairMoment | null;
  hasEvidence: boolean;
  copyState: "idle" | "copied" | "failed";
  onCopy: () => void;
  onExport: () => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  const commands = analysis ? localCommands(analysis) : null;
  return (
    <article className="loop-panel loop-panel--receipt" aria-labelledby="receipt-export-title">
      <p className="eyebrow">Receipt</p>
      <h3 id="receipt-export-title">{analysis ? "Export dry-run receipt" : "Receipt waits for analysis"}</h3>
      <p>{primaryMoment?.note ?? "The receipt will include source facts, issues, repairs, verification, hashes, commands, and limitations."}</p>
      <div className="loop-actions" aria-label="Primary receipt actions">
        <button className="icon-button" type="button" disabled={!hasEvidence} onClick={onCopy}>
          <ClipboardCopy aria-hidden="true" />
          {copyState === "copied" ? "Copied" : copyState === "failed" ? "Copy failed" : "Copy"}
        </button>
        <button className="icon-button" type="button" disabled={!hasEvidence} onClick={onExport}>
          <Download aria-hidden="true" />
          Export
        </button>
      </div>
      {commands ? (
        <dl className="loop-facts">
          <div>
            <dt>Apply</dt>
            <dd><code>{commands.apply}</code></dd>
          </div>
          <div>
            <dt>Audit</dt>
            <dd><code>{commands.audit}</code></dd>
          </div>
          <div>
            <dt>Revert</dt>
            <dd><code>{commands.revert}</code></dd>
          </div>
        </dl>
      ) : null}
      <button type="button" className="loop-link" onClick={() => onNavigate("receipt")} disabled={!analysis}>
        Open full receipt
      </button>
    </article>
  );
}

export function SafetyRevertExplainer({ analysis }: { analysis: AnalyzeResponse | null }) {
  return (
    <section className="safety-revert-explainer" aria-labelledby="safety-revert-title">
      <div>
        <p className="eyebrow">Safety and revert</p>
        <h3 id="safety-revert-title">Why the hosted demo is safe to try</h3>
      </div>
      <ul>
        {SAFETY_REVERT_EXPLANATION.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
      {analysis ? (
        <div className="safety-hashes" aria-label="Receipt safety hashes">
          <span>source {shortHash(analysis.receipt.source_sha256)}</span>
          <span>patch {analysis.receipt.patch_plan_sha256 ? shortHash(analysis.receipt.patch_plan_sha256) : "none"}</span>
          <span>{analysis.receipt.applied ? "applied" : "not applied"}</span>
        </div>
      ) : null}
    </section>
  );
}
