import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  ClipboardCopy,
  Database,
  Download,
  FileText,
  RefreshCw,
  ShieldCheck,
  Upload,
  Wrench,
} from "lucide-react";
import {
  type ChangeEvent,
  type KeyboardEvent,
  type ReactNode,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { ApiProblemError, DataForgeClient } from "./api";
import { getRuntimeConfig } from "./config";
import {
  DEFAULT_MAX_UPLOAD_BYTES,
  buildEvidenceExport,
  formatRows,
  groupIssues,
  parseCsvPreview,
  problemToMessage,
  validateCsvFile,
} from "./csv";
import type {
  BackendCapability,
  CsvPreview,
  DatasetInput,
  IssueGroup,
  ProblemDetail,
  ProfileResponse,
  RepairResponse,
  Severity,
  VerifiedFix,
} from "./types";

const SAMPLE_OPTIONS = [
  { value: "hospital_10rows", label: "Hospital", detail: "Healthcare data" },
  { value: "flights_10rows", label: "Flights", detail: "Aviation data" },
  { value: "beers_10rows", label: "Beers", detail: "Consumer data" },
];

const TABS = [
  { id: "profile", label: "Profile" },
  { id: "repair", label: "Repair" },
  { id: "journal", label: "Journal" },
] as const;

type TabId = (typeof TABS)[number]["id"];
type WorkState = "idle" | "loading" | "ready" | "error";
type SortKey = "severity" | "count" | "column";

function App() {
  const runtimeConfig = useMemo(() => getRuntimeConfig(), []);
  const client = useMemo(
    () => new DataForgeClient(runtimeConfig.BACKEND_URL),
    [runtimeConfig.BACKEND_URL],
  );
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [capability, setCapability] = useState<BackendCapability | null>(null);
  const [backendState, setBackendState] = useState<WorkState>("loading");
  const [datasetState, setDatasetState] = useState<WorkState>("idle");
  const [dataset, setDataset] = useState<DatasetInput | null>(null);
  const [advanced, setAdvanced] = useState(false);
  const [activeTab, setActiveTab] = useState<TabId>("profile");
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [repair, setRepair] = useState<RepairResponse | null>(null);
  const [profileState, setProfileState] = useState<WorkState>("idle");
  const [repairState, setRepairState] = useState<WorkState>("idle");
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const [filter, setFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState<Severity | "all">("all");
  const [sortKey, setSortKey] = useState<SortKey>("severity");
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");

  const maxUploadBytes = capability?.max_upload_bytes ?? DEFAULT_MAX_UPLOAD_BYTES;
  const busy = datasetState === "loading" || profileState === "loading" || repairState === "loading";
  const canRun = backendState === "ready" && dataset !== null && !busy;
  const evidenceText = useMemo(
    () => (repair && dataset ? buildEvidenceExport(dataset.file.name, profile, repair) : ""),
    [dataset, profile, repair],
  );
  const groupedIssues = useMemo(() => groupIssues(profile?.issues ?? []), [profile]);
  const visibleIssues = useMemo(
    () => filterAndSortIssues(groupedIssues, filter, severityFilter, sortKey),
    [filter, groupedIssues, severityFilter, sortKey],
  );

  useEffect(() => {
    let cancelled = false;

    async function warmBackend() {
      setBackendState("loading");
      for (let attempt = 0; attempt < 6; attempt += 1) {
        try {
          const health = await client.health();
          if (!cancelled) {
            setCapability(health);
            setAdvanced((current) => current && health.advanced_available);
            setBackendState("ready");
          }
          return;
        } catch {
          await sleep(Math.min(750 * 2 ** attempt, 6_000));
        }
      }
      if (!cancelled) {
        setBackendState("error");
        setCapability(null);
      }
    }

    void warmBackend();
    return () => {
      cancelled = true;
    };
  }, [client]);

  async function adoptFile(file: File, source: DatasetInput["source"], sampleName?: string) {
    setDatasetState("loading");
    setProblem(null);

    const validation = validateCsvFile(file, maxUploadBytes);
    if (!validation.ok) {
      setDatasetState("error");
      setProblem(localProblem(validation.message ?? "The CSV file could not be accepted."));
      return;
    }

    try {
      const preview = parseCsvPreview(await file.text());
      setDataset({ file, source, sampleName, preview });
      setDatasetState("ready");
      setProfile(null);
      setRepair(null);
      setCopyState("idle");
      setProfileState("idle");
      setRepairState("idle");
      setActiveTab("profile");
    } catch (error) {
      setDatasetState("error");
      setProblem(localProblem(error instanceof Error ? error.message : "The CSV preview failed."));
    }
  }

  async function chooseSample(sampleName: string) {
    if (!sampleName || busy) {
      return;
    }
    try {
      const file = await client.sample(sampleName);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      await adoptFile(file, "sample", sampleName);
    } catch (error) {
      setDatasetState("error");
      setProblem(problemFromUnknown(error));
    }
  }

  async function handleFileChange(event: ChangeEvent<HTMLInputElement>) {
    const [file] = event.target.files ?? [];
    if (file) {
      await adoptFile(file, "upload");
    }
  }

  async function runProfile() {
    if (!dataset || !canRun) {
      return;
    }
    setProfileState("loading");
    setProblem(null);
    setActiveTab("profile");
    try {
      setProfile(await client.profile(dataset.file, advanced));
      setProfileState("ready");
    } catch (error) {
      const nextProblem = problemFromUnknown(error);
      setProfileState("error");
      setProblem(nextProblem);
      if (nextProblem.error === "advanced_mode_unavailable") {
        setAdvanced(false);
        setCapability((current) =>
          current ? { ...current, advanced_available: false } : current,
        );
      }
    }
  }

  async function runRepair() {
    if (!dataset || !canRun) {
      return;
    }
    setRepairState("loading");
    setProblem(null);
    setCopyState("idle");
    setActiveTab("repair");
    try {
      setRepair(await client.repair(dataset.file, advanced));
      setRepairState("ready");
    } catch (error) {
      const nextProblem = problemFromUnknown(error);
      setRepairState("error");
      setProblem(nextProblem);
      if (nextProblem.error === "advanced_mode_unavailable") {
        setAdvanced(false);
        setCapability((current) =>
          current ? { ...current, advanced_available: false } : current,
        );
      }
    }
  }

  async function copyEvidence() {
    if (!evidenceText) {
      return;
    }
    try {
      await navigator.clipboard.writeText(evidenceText);
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    }
  }

  function exportEvidence() {
    if (!evidenceText || !dataset) {
      return;
    }
    const url = URL.createObjectURL(new Blob([evidenceText], { type: "application/json" }));
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${dataset.file.name.replace(/\.csv$/i, "")}-dataforge-dry-run.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  function handleTabKeyDown(event: KeyboardEvent<HTMLButtonElement>) {
    const index = TABS.findIndex((tab) => tab.id === activeTab);
    if (event.key === "ArrowRight" || event.key === "ArrowLeft") {
      event.preventDefault();
      const direction = event.key === "ArrowRight" ? 1 : -1;
      setActiveTab(TABS[(index + direction + TABS.length) % TABS.length].id);
    }
    if (event.key === "Home") {
      event.preventDefault();
      setActiveTab(TABS[0].id);
    }
    if (event.key === "End") {
      event.preventDefault();
      setActiveTab(TABS[TABS.length - 1].id);
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar" aria-label="DataForge command bar">
        <div className="brand-lockup">
          <span className="product-mark" aria-hidden="true">DF</span>
          <div>
            <p className="eyebrow">DataForge Playground</p>
            <h1>Verified CSV repair workbench</h1>
          </div>
        </div>
        <div className="command-meta" aria-label="Playground operating constraints">
          <span>Stateless dry run</span>
          <span>{Math.floor(maxUploadBytes / 1024)} KiB CSV cap</span>
          <BackendStatus state={backendState} capability={capability} onRetry={() => window.location.reload()} />
        </div>
      </header>

      <main className="workspace" aria-busy={busy}>
        <section className="panel intake-panel" aria-labelledby="intake-title">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Input</p>
              <h2 id="intake-title">Dataset intake</h2>
            </div>
            <span className="limit-pill">{Math.floor(maxUploadBytes / 1024)} KiB limit</span>
          </div>

          <label className="file-drop" htmlFor="csv-upload">
            <Upload aria-hidden="true" />
            <span>
              <strong>Upload CSV</strong>
              <small>{dataset?.source === "upload" ? dataset.file.name : "No file selected"}</small>
            </span>
            <input
              id="csv-upload"
              ref={fileInputRef}
              type="file"
              accept=".csv,text/csv"
              disabled={busy}
              onChange={handleFileChange}
            />
          </label>

          <div className="sample-grid" aria-label="Sample datasets">
            {SAMPLE_OPTIONS.map((sample) => (
              <button
                className="sample-button"
                type="button"
                key={sample.value}
                disabled={busy}
                onClick={() => void chooseSample(sample.value)}
              >
                <Database aria-hidden="true" />
                <span>
                  <strong>{sample.label}</strong>
                  <small>{sample.detail}</small>
                </span>
              </button>
            ))}
          </div>

          <label className="switch-row" htmlFor="advanced-mode">
            <span>
              <strong>Advanced mode</strong>
              <small>
                {capability?.advanced_available
                  ? "Backend provider available"
                  : "Backend provider unavailable"}
              </small>
            </span>
            <input
              id="advanced-mode"
              type="checkbox"
              role="switch"
              checked={advanced}
              disabled={busy || !capability?.advanced_available}
              onChange={(event) => setAdvanced(event.target.checked)}
            />
          </label>

          <div className="action-row">
            <button className="primary-action" type="button" disabled={!canRun} onClick={() => void runProfile()}>
              <Activity aria-hidden="true" />
              Profile
            </button>
            <button className="secondary-action" type="button" disabled={!canRun} onClick={() => void runRepair()}>
              <Wrench aria-hidden="true" />
              Repair dry run
            </button>
          </div>
        </section>

        <section className="panel preview-panel" aria-labelledby="preview-title">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Preview</p>
              <h2 id="preview-title">Current CSV</h2>
            </div>
            <DatasetBadge dataset={dataset} />
          </div>
          {dataset ? (
            <CsvPreviewTable preview={dataset.preview} />
          ) : (
            <EmptyState
              icon={<FileText aria-hidden="true" />}
              title="No dataset loaded"
              body="Choose a sample or upload a CSV to inspect the first rows before sending it to the backend."
            />
          )}
        </section>

        <section className="panel results-panel" aria-labelledby="results-title">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Evidence</p>
              <h2 id="results-title">Results</h2>
            </div>
            {repair ? (
              <div className="evidence-actions">
                <button type="button" className="icon-button" onClick={() => void copyEvidence()}>
                  <ClipboardCopy aria-hidden="true" />
                  {copyState === "copied" ? "Copied" : copyState === "failed" ? "Copy failed" : "Copy"}
                </button>
                <button type="button" className="icon-button" onClick={exportEvidence}>
                  <Download aria-hidden="true" />
                  Export
                </button>
              </div>
            ) : null}
          </div>

          {problem ? <ProblemBanner problem={problem} /> : null}
          {copyState === "failed" && evidenceText ? (
            <CopyFallback evidenceText={evidenceText} />
          ) : null}

          <div className="tabs" role="tablist" aria-label="Result views">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                id={`tab-${tab.id}`}
                role="tab"
                type="button"
                aria-selected={activeTab === tab.id}
                aria-controls={`panel-${tab.id}`}
                tabIndex={activeTab === tab.id ? 0 : -1}
                onClick={() => setActiveTab(tab.id)}
                onKeyDown={handleTabKeyDown}
              >
                {tab.label}
              </button>
            ))}
          </div>

          <ResultPanel id="profile" activeTab={activeTab}>
            <ProfileView
              state={profileState}
              profile={profile}
              issues={visibleIssues}
              filter={filter}
              severityFilter={severityFilter}
              sortKey={sortKey}
              onFilterChange={setFilter}
              onSeverityFilterChange={setSeverityFilter}
              onSortChange={setSortKey}
            />
          </ResultPanel>
          <ResultPanel id="repair" activeTab={activeTab}>
            <RepairView state={repairState} repair={repair} dataset={dataset} />
          </ResultPanel>
          <ResultPanel id="journal" activeTab={activeTab}>
            <JournalView repair={repair} />
          </ResultPanel>
        </section>
      </main>
    </div>
  );
}

function BackendStatus({
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
      <div className="status-pill status-pill--loading" role="status" aria-live="polite">
        <RefreshCw aria-hidden="true" />
        Warming backend
      </div>
    );
  }
  if (state === "error") {
    return (
      <button className="status-pill status-pill--error" type="button" onClick={onRetry}>
        <AlertTriangle aria-hidden="true" />
        Backend unavailable
      </button>
    );
  }
  return (
    <div className="status-pill status-pill--ready" role="status" aria-live="polite">
      <CheckCircle2 aria-hidden="true" />
      {capability?.advanced_available ? "Ready with advanced mode" : "Ready"}
    </div>
  );
}

function DatasetBadge({ dataset }: { dataset: DatasetInput | null }) {
  if (!dataset) {
    return <span className="muted-pill">Waiting</span>;
  }
  return (
    <span className="muted-pill">
      {dataset.preview.rows.length} preview rows, {dataset.preview.columns.length} columns
    </span>
  );
}

function CsvPreviewTable({ preview }: { preview: CsvPreview }) {
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

function ResultPanel({
  id,
  activeTab,
  children,
}: {
  id: TabId;
  activeTab: TabId;
  children: ReactNode;
}) {
  const active = id === activeTab;
  return (
    <div
      id={`panel-${id}`}
      role="tabpanel"
      aria-labelledby={`tab-${id}`}
      hidden={!active}
      tabIndex={active ? 0 : -1}
      className="tab-panel"
    >
      {children}
    </div>
  );
}

function ProfileView({
  state,
  profile,
  issues,
  filter,
  severityFilter,
  sortKey,
  onFilterChange,
  onSeverityFilterChange,
  onSortChange,
}: {
  state: WorkState;
  profile: ProfileResponse | null;
  issues: IssueGroup[];
  filter: string;
  severityFilter: Severity | "all";
  sortKey: SortKey;
  onFilterChange: (value: string) => void;
  onSeverityFilterChange: (value: Severity | "all") => void;
  onSortChange: (value: SortKey) => void;
}) {
  if (state === "loading") {
    return <LoadingState label="Profiling CSV" />;
  }
  if (!profile) {
    return (
      <EmptyState
        icon={<Activity aria-hidden="true" />}
        title="Profile evidence appears here"
        body="Run profile to see grouped issue evidence, row counts, and severity."
      />
    );
  }
  return (
    <div className="result-stack">
      <div className="metric-strip" aria-label="Profile summary">
        <Metric label="Rows" value={profile.meta.rows} />
        <Metric label="Columns" value={profile.meta.columns} />
        <Metric label="Issues" value={profile.meta.total_issues} />
        <Metric label="Contract" value={profile.meta.contract_version} compact />
      </div>
      <EvidenceNote
        title={profile.meta.total_issues === 0 ? "No issues matched current detectors" : "Issue groups are detector evidence"}
        body={
          profile.meta.total_issues === 0
            ? "The current detector set did not flag this CSV. Review source context before treating the data as production-clean."
            : "Unsafe items should be reviewed first; review items are plausible repairs that still need context. Row indices are zero-based."
        }
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
          body="Adjust the filters or profile another dataset."
        />
      ) : (
        <IssueTable issues={issues} />
      )}
    </div>
  );
}

function IssueTable({ issues }: { issues: IssueGroup[] }) {
  return (
    <div className="table-frame" tabIndex={0} aria-label="Grouped profile issues">
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
                <code>{issue.column}</code>
              </td>
              <td>{issue.issue_type}</td>
              <td>
                <SeverityBadge severity={issue.severity} />
              </td>
              <td>{formatRows(issue.row_indices)}</td>
              <td>{issue.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RepairView({
  state,
  repair,
  dataset,
}: {
  state: WorkState;
  repair: RepairResponse | null;
  dataset: DatasetInput | null;
}) {
  if (state === "loading") {
    return <LoadingState label="Running dry repair" />;
  }
  if (!repair) {
    return (
      <EmptyState
        icon={<Wrench aria-hidden="true" />}
        title="Dry-run repairs appear here"
        body={
          dataset
            ? "Run repair dry run to inspect proposed changes, verifier evidence, and the transaction receipt."
            : "Load a sample or upload a CSV before requesting repair evidence."
        }
      />
    );
  }
  if (repair.fixes.length === 0) {
    return (
      <div className="result-stack">
        <EvidenceNote
          title="No verified repairs were proposed"
          body="This means the dry-run pipeline did not find a candidate that passed safety and verifier gates. It is not a proof that every value is correct."
        />
        {repair.receipt ? <ReceiptSummary repair={repair} /> : null}
      </div>
    );
  }
  return (
    <div className="result-stack">
      <EvidenceNote
        title="Verified dry-run evidence"
        body="Every listed fix passed the hosted safety and verifier gates. Nothing is applied in the browser; use the CLI for reversible local apply workflows."
      />
      {repair.receipt ? <ReceiptSummary repair={repair} /> : null}
      <div className="repair-list">
        {repair.fixes.map((fix) => (
          <RepairDiff key={`${fix.row}:${fix.column}:${fix.old_value}:${fix.new_value}`} fix={fix} />
        ))}
      </div>
    </div>
  );
}

function RepairDiff({ fix }: { fix: VerifiedFix }) {
  return (
    <article className="repair-row">
      <header>
        <div>
          <strong>
            Row {fix.row}, <code>{fix.column}</code>
          </strong>
          <small>
            {fix.detector_id} - confidence {Number(fix.confidence).toFixed(2)}
          </small>
        </div>
        <span className="provenance-pill">{fix.provenance}</span>
      </header>
      <div className="diff-grid">
        <div className="diff-cell diff-cell--old">
          <span>Current</span>
          <code>{fix.old_value || "(empty)"}</code>
        </div>
        <div className="diff-cell diff-cell--new">
          <span>Proposed</span>
          <code>{fix.new_value || "(empty)"}</code>
        </div>
      </div>
      <p>{fix.reason}</p>
      {fix.verifier_reason ? <p className="verifier-note">{fix.verifier_reason}</p> : null}
    </article>
  );
}

function JournalView({ repair }: { repair: RepairResponse | null }) {
  if (!repair?.txn_journal) {
    return (
      <EmptyState
        icon={<ShieldCheck aria-hidden="true" />}
        title="No journal yet"
        body="A dry-run transaction journal is shown after repair completes."
      />
    );
  }
  return (
    <div className="journal-grid">
      <Metric label="Transaction" value={repair.txn_journal.txn_id} compact />
      <Metric label="Fixes" value={repair.txn_journal.fixes_count} />
      <Metric label="Applied" value={repair.txn_journal.applied ? "yes" : "no"} />
      <Metric label="Source hash" value={repair.txn_journal.source_sha256.slice(0, 12)} compact />
      <pre tabIndex={0} aria-label="Dry-run transaction journal">
        {JSON.stringify(repair.txn_journal, null, 2)}
      </pre>
    </div>
  );
}

function ReceiptSummary({ repair }: { repair: RepairResponse }) {
  if (!repair.receipt) {
    return null;
  }
  return (
    <div className="receipt-grid" aria-label="Repair receipt summary">
      <Metric label="Safety" value={repair.receipt.safety_verdict} compact />
      <Metric label="Verifier" value={repair.receipt.verifier_verdict} compact />
      <Metric label="Issues" value={repair.receipt.issues_count} />
      <Metric label="Fixes" value={repair.receipt.fixes_count} />
      <p>{repair.receipt.reason}</p>
    </div>
  );
}

function EvidenceNote({ title, body }: { title: string; body: string }) {
  return (
    <div className="evidence-note">
      <ShieldCheck aria-hidden="true" />
      <div>
        <strong>{title}</strong>
        <p>{body}</p>
      </div>
    </div>
  );
}

function CopyFallback({ evidenceText }: { evidenceText: string }) {
  return (
    <div className="copy-fallback" role="status" aria-live="polite">
      <strong>Clipboard permission was blocked</strong>
      <p>Export still works. You can also select this evidence payload directly.</p>
      <textarea aria-label="Copyable repair evidence" readOnly value={evidenceText} />
    </div>
  );
}

function Metric({
  label,
  value,
  compact = false,
}: {
  label: string;
  value: string | number;
  compact?: boolean;
}) {
  return (
    <div className={compact ? "metric metric--compact" : "metric"}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function SeverityBadge({ severity }: { severity: Severity }) {
  return <span className={`severity severity--${severity}`}>{severity}</span>;
}

function ProblemBanner({ problem }: { problem: ProblemDetail }) {
  return (
    <div className="problem-banner" role="alert">
      <AlertTriangle aria-hidden="true" />
      <div>
        <strong>{problem.title}</strong>
        <p>{problemToMessage(problem)}</p>
      </div>
    </div>
  );
}

function LoadingState({ label }: { label: string }) {
  return (
    <div className="loading-state" role="status" aria-live="polite">
      <RefreshCw aria-hidden="true" />
      <span>{label}</span>
    </div>
  );
}

function EmptyState({
  icon,
  title,
  body,
}: {
  icon: ReactNode;
  title: string;
  body: string;
}) {
  return (
    <div className="empty-state">
      {icon}
      <strong>{title}</strong>
      <p>{body}</p>
    </div>
  );
}

function filterAndSortIssues(
  issues: IssueGroup[],
  filter: string,
  severity: Severity | "all",
  sortKey: SortKey,
) {
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
    return groupIssues([a, b])[0].key === a.key ? -1 : 1;
  });
}

function problemFromUnknown(error: unknown): ProblemDetail {
  if (error instanceof ApiProblemError) {
    return error.problem;
  }
  return localProblem(error instanceof Error ? error.message : "The request failed.");
}

function localProblem(message: string): ProblemDetail {
  return {
    type: "https://dataforge.local/problems/frontend_validation",
    title: "Dataset validation failed",
    status: 400,
    detail: message,
    error: "frontend_validation",
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

export default App;
