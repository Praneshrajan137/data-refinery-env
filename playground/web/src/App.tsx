import {
  Activity,
  AlertTriangle,
  BadgeCheck,
  BrainCircuit,
  CheckCircle2,
  CircleDot,
  ClipboardCopy,
  Database,
  Download,
  FileCheck2,
  FileText,
  PauseCircle,
  Play,
  RefreshCw,
  ShieldCheck,
  Upload,
  Wrench,
} from "lucide-react";
import { AnimatePresence, LayoutGroup, motion, useReducedMotion } from "motion/react";
import {
  type ChangeEvent,
  type ReactNode,
  type RefObject,
  useEffect,
  useMemo,
  useReducer,
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
import {
  buildGuardrailVerdict,
  buildObservatoryView,
  buildTrustVerdict,
  formatLabel,
  formatPercent,
  humanizeReviewReason,
  shortHash,
  strengthOf,
  type GuardrailVerdict,
  type InstrumentTone,
  type ReviewItem,
  type SelectedEvidence,
  type TrustVerdict,
} from "./observatory";
import {
  motionDurations,
  motionSprings,
  panelVariants,
  stageNodeVariants,
  variantsForIntensity,
  workflowStatusToAgentState,
  type MotionIntensity,
} from "./motion";
import {
  SAFETY_REVERT_EXPLANATION,
  localCommands,
  selectPrimaryRepairMoment,
  type PrimaryRepairMoment,
} from "./productLoop";
import {
  PRODUCT_ROUTES,
  routeById,
  routeFromPathname,
  type ProductRoute,
  type ProductRouteId,
} from "./routes";
import type {
  AgentSummary,
  AnalyzeResponse,
  BackendCapability,
  CandidateRepair,
  Certificate,
  ConstraintCandidate,
  CsvPreview,
  DatasetInput,
  IssueGroup,
  ProblemDetail,
  RepairFailure,
  RepairMode,
  RepairReadiness,
  RiskLevel,
  Severity,
  VerificationStrength,
  VerifiedFix,
  ExternalFix,
  VerifyFixesResponse,
  VerifyScenario,
  WorkflowEvent,
} from "./types";
import {
  createWorkflowState,
  workflowReducer,
  type WorkflowStageView,
} from "./workflow";

const SAMPLE_OPTIONS = [
  { value: "hospital_10rows", label: "Hospital", detail: "Rating 45.0 -> 4.5" },
  { value: "flights_10rows", label: "Flights", detail: "Aviation data" },
  { value: "beers_10rows", label: "Beers", detail: "Consumer data" },
];

type WorkState = "idle" | "loading" | "ready" | "error";
type SortKey = "severity" | "count" | "column";

function App() {
  const prefersReducedMotion = useReducedMotion();
  const motionIntensity: MotionIntensity = prefersReducedMotion ? "reduced" : "standard";
  const runtimeConfig = useMemo(() => getRuntimeConfig(), []);
  const client = useMemo(
    () => new DataForgeClient(runtimeConfig.BACKEND_URL),
    [runtimeConfig.BACKEND_URL],
  );
  const fileInputRef = useRef<HTMLInputElement>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const [capability, setCapability] = useState<BackendCapability | null>(null);
  const [backendState, setBackendState] = useState<WorkState>("loading");
  const [datasetState, setDatasetState] = useState<WorkState>("idle");
  const [dataset, setDataset] = useState<DatasetInput | null>(null);
  const [advanced, setAdvanced] = useState(false);
  const [repairMode, setRepairMode] = useState<RepairMode>("deterministic");
  const [route, setRoute] = useState<ProductRoute>(() => routeFromPathname(window.location.pathname));
  const [analysis, setAnalysis] = useState<AnalyzeResponse | null>(null);
  const [analysisState, setAnalysisState] = useState<WorkState>("idle");
  const [workflow, dispatchWorkflow] = useReducer(workflowReducer, undefined, createWorkflowState);
  const [acceptedConstraintIds, setAcceptedConstraintIds] = useState<string[]>([]);
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const [filter, setFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState<Severity | "all">("all");
  const [sortKey, setSortKey] = useState<SortKey>("severity");
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");
  const [selectedEvidence, setSelectedEvidence] = useState<SelectedEvidence | null>(null);

  const maxUploadBytes = capability?.max_upload_bytes ?? DEFAULT_MAX_UPLOAD_BYTES;
  const streamingEnabled =
    capability?.streaming_available === true &&
    capability.workflow_contract_version === "workflow_event_v1";
  const busy = datasetState === "loading" || analysisState === "loading";
  const canRun = backendState === "ready" && dataset !== null && !busy;
  const latestAnalysis = workflow.lastAnalysis ?? analysis;
  const evidenceText = useMemo(
    () => (latestAnalysis && dataset ? buildEvidenceExport(dataset.file.name, latestAnalysis) : ""),
    [latestAnalysis, dataset],
  );
  const groupedIssues = useMemo(() => groupIssues(latestAnalysis?.issues ?? []), [latestAnalysis]);
  const visibleIssues = useMemo(
    () => filterAndSortIssues(groupedIssues, filter, severityFilter, sortKey),
    [filter, groupedIssues, severityFilter, sortKey],
  );
  const observatory = useMemo(
    () =>
      buildObservatoryView({
        analysis: latestAnalysis,
        dataset,
        workflow,
        selectedConstraintIds: acceptedConstraintIds,
      }),
    [acceptedConstraintIds, dataset, latestAnalysis, workflow],
  );
  const primaryMoment = useMemo(
    () => (latestAnalysis ? selectPrimaryRepairMoment(latestAnalysis) : null),
    [latestAnalysis],
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
            setRepairMode((current) =>
              current === "agent" && !health.agent_available ? "deterministic" : current,
            );
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
      abortControllerRef.current?.abort();
    };
  }, [client]);

  useEffect(() => {
    function handlePopState() {
      setRoute(routeFromPathname(window.location.pathname));
    }
    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  function navigate(nextRouteId: ProductRouteId) {
    const nextRoute = routeById(nextRouteId);
    if (nextRoute.href !== window.location.pathname) {
      window.history.pushState({}, "", nextRoute.href);
    }
    setRoute(nextRoute);
  }

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
      setAnalysis(null);
      setAcceptedConstraintIds([]);
      setCopyState("idle");
      setAnalysisState("idle");
      navigate("run");
      setSelectedEvidence(null);
      dispatchWorkflow({ type: "reset" });
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

  async function runAnalyze(ids: string[]) {
    if (!dataset || !canRun) {
      return;
    }
    const controller = new AbortController();
    abortControllerRef.current = controller;
    setAnalysisState("loading");
    setProblem(null);
    setCopyState("idle");
    setSelectedEvidence({ kind: "stage", id: "intake" });
    dispatchWorkflow({ type: "start" });

    try {
      const nextAnalysis = streamingEnabled
        ? await client.analyzeStream(
            dataset.file,
            advanced,
            ids,
            {
              signal: controller.signal,
              onEvent: (event: WorkflowEvent) => dispatchWorkflow({ type: "event", event }),
            },
            repairMode,
          )
        : await client.analyze(dataset.file, advanced, ids, repairMode);

      if (!streamingEnabled) {
        dispatchWorkflow({ type: "analysis", analysis: nextAnalysis });
      }
      setAnalysis(nextAnalysis);
      setAcceptedConstraintIds(
        nextAnalysis.schema_inference.candidates
          .filter((candidate) => candidate.decision === "accepted")
          .map((candidate) => candidate.candidate_id),
      );
      setAnalysisState("ready");
      setSelectedEvidence({ kind: "receipt", id: nextAnalysis.receipt.txn_id ?? "receipt" });
    } catch (error) {
      if (isAbortError(error)) {
        dispatchWorkflow({ type: "cancel" });
        setAnalysisState(latestAnalysis ? "ready" : "idle");
        return;
      }
      const nextProblem = problemFromUnknown(error);
      setAnalysisState("error");
      setProblem(nextProblem);
      dispatchWorkflow({ type: "problem", problem: nextProblem });
      if (nextProblem.error === "advanced_mode_unavailable") {
        setAdvanced(false);
        setCapability((current) => (current ? { ...current, advanced_available: false } : current));
      }
      if (nextProblem.error === "agent_mode_unavailable") {
        setRepairMode("deterministic");
        setCapability((current) => (current ? { ...current, agent_available: false } : current));
      }
    } finally {
      if (abortControllerRef.current === controller) {
        abortControllerRef.current = null;
      }
    }
  }

  function cancelAnalyze() {
    abortControllerRef.current?.abort();
  }

  function toggleConstraint(candidateId: string, checked: boolean) {
    setAcceptedConstraintIds((current) => {
      if (checked) {
        return current.includes(candidateId) ? current : [...current, candidateId];
      }
      return current.filter((id) => id !== candidateId);
    });
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

  return (
    <ProductShell route={route} onNavigate={navigate}>
      <ProductPageHeader route={route} dataset={dataset} analysis={latestAnalysis} workflowStatus={workflow.status} />
      <LayoutGroup id="dataforge-observatory">
        <AnimatePresence mode="wait" initial={false}>
          <motion.div
            key={route.id}
            className="route-motion-frame"
            data-motion-route={route.id}
            variants={variantsForIntensity("route", motionIntensity)}
            initial="initial"
            animate="animate"
            exit="exit"
          >
            {route.id === "run" ? (
              <RunPage
                dataset={dataset}
                busy={busy}
                canRun={canRun}
                maxUploadBytes={maxUploadBytes}
                capability={capability}
                advanced={advanced}
                repairMode={repairMode}
                backendState={backendState}
                streamingEnabled={streamingEnabled}
                acceptedConstraintIds={acceptedConstraintIds}
                analysisState={analysisState}
                hasEvidence={Boolean(evidenceText)}
                evidenceText={evidenceText}
                copyState={copyState}
                fileInputRef={fileInputRef}
                problem={problem}
                latestAnalysis={latestAnalysis}
                primaryMoment={primaryMoment}
                observatory={observatory}
                onAdvancedChange={setAdvanced}
                onRepairModeChange={setRepairMode}
                onChooseSample={chooseSample}
                onFileChange={handleFileChange}
                onAnalyze={() => void runAnalyze([])}
                onRerun={() => void runAnalyze(acceptedConstraintIds)}
                onCancel={cancelAnalyze}
                onCopy={() => void copyEvidence()}
                onExport={exportEvidence}
                onBackendRetry={() => window.location.reload()}
                onNavigate={navigate}
                onSelect={setSelectedEvidence}
              />
            ) : null}
            {route.id === "atlas" ? (
              <AtlasPage
                stages={workflow.stages}
                runId={workflow.runId}
                status={workflow.status}
                selectedEvidence={selectedEvidence}
                reviewItems={observatory.reviewQueue}
                analysis={latestAnalysis}
                selectedConstraintIds={acceptedConstraintIds}
                canRerun={canRun && acceptedConstraintIds.length > 0}
                onToggleConstraint={toggleConstraint}
                onRerun={() => void runAnalyze(acceptedConstraintIds)}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {route.id === "evidence" ? (
              <EvidencePage
                dataset={dataset}
                preview={dataset?.preview ?? null}
                state={analysisState}
                analysis={latestAnalysis}
                observatory={observatory}
                issues={visibleIssues}
                filter={filter}
                severityFilter={severityFilter}
                sortKey={sortKey}
                problem={problem}
                selectedEvidence={selectedEvidence}
                allIssues={groupedIssues}
                onFilterChange={setFilter}
                onSeverityFilterChange={setSeverityFilter}
                onSortChange={setSortKey}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {route.id === "repairs" ? (
              <RepairsPage
                state={analysisState}
                analysis={latestAnalysis}
                dataset={dataset}
                selectedEvidence={selectedEvidence}
                issues={groupedIssues}
                problem={problem}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {route.id === "guardrail" ? (
              <GuardrailPage
                client={client}
                capability={capability}
                backendState={backendState}
                maxUploadBytes={maxUploadBytes}
                onBackendRetry={() => window.location.reload()}
                onNavigate={navigate}
              />
            ) : null}
            {route.id === "receipt" ? (
              <ReceiptPage
                analysis={latestAnalysis}
                evidenceText={evidenceText}
                copyState={copyState}
                selectedEvidence={selectedEvidence}
                stages={workflow.stages}
                issues={groupedIssues}
                problem={problem}
                onCopy={() => void copyEvidence()}
                onExport={exportEvidence}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {route.id === "system" ? (
              <SystemPage
                capability={capability}
                backendState={backendState}
                streamingEnabled={streamingEnabled}
                maxUploadBytes={maxUploadBytes}
                analysis={latestAnalysis}
              />
            ) : null}
          </motion.div>
        </AnimatePresence>
      </LayoutGroup>
    </ProductShell>
  );
}

function ProductShell({
  route,
  onNavigate,
  children,
}: {
  route: ProductRoute;
  onNavigate: (routeId: ProductRouteId) => void;
  children: ReactNode;
}) {
  return (
    <div className="product-shell">
      <aside className="product-nav" aria-label="DataForge product navigation">
        <div className="nav-lockup">
          <span className="product-mark" aria-hidden="true">DF</span>
          <div>
            <p className="eyebrow">DataForge</p>
            <strong>CSV Repair Loop</strong>
          </div>
        </div>
        <nav>
          {PRODUCT_ROUTES.map((item) => (
            <a
              key={item.id}
              href={item.href}
              aria-current={route.id === item.id ? "page" : undefined}
              className={route.id === item.id ? "product-nav-link product-nav-link--current" : "product-nav-link"}
              onClick={(event) => {
                event.preventDefault();
                onNavigate(item.id);
              }}
            >
              {route.id === item.id ? (
                <motion.i
                  className="nav-active-marker"
                  layoutId="nav-active-marker"
                  transition={motionSprings.layout}
                  aria-hidden="true"
                />
              ) : null}
              <span>{item.label}</span>
              <small>{item.title}</small>
            </a>
          ))}
        </nav>
      </aside>
      <div className="product-main">{children}</div>
    </div>
  );
}

function ProductPageHeader({
  route,
  dataset,
  analysis,
  workflowStatus,
}: {
  route: ProductRoute;
  dataset: DatasetInput | null;
  analysis: AnalyzeResponse | null;
  workflowStatus: string;
}) {
  return (
    <header className="page-hero" aria-label={`${route.title} page`}>
      <div>
        <p className="eyebrow">DataForge Playground</p>
        <h1>{route.title}</h1>
        <p>{route.description}</p>
      </div>
      <div className="page-signals" aria-label="Current run posture">
        <span>{dataset ? dataset.file.name : "No dataset"}</span>
        <span>{analysis ? analysis.meta.contract_version : "No receipt"}</span>
        <span>{formatLabel(workflowStatus)}</span>
      </div>
    </header>
  );
}

function RunPage({
  dataset,
  busy,
  canRun,
  maxUploadBytes,
  capability,
  advanced,
  repairMode,
  backendState,
  streamingEnabled,
  acceptedConstraintIds,
  analysisState,
  hasEvidence,
  evidenceText,
  copyState,
  fileInputRef,
  problem,
  latestAnalysis,
  primaryMoment,
  observatory,
  onAdvancedChange,
  onRepairModeChange,
  onChooseSample,
  onFileChange,
  onAnalyze,
  onRerun,
  onCancel,
  onCopy,
  onExport,
  onBackendRetry,
  onNavigate,
  onSelect,
}: {
  dataset: DatasetInput | null;
  busy: boolean;
  canRun: boolean;
  maxUploadBytes: number;
  capability: BackendCapability | null;
  advanced: boolean;
  repairMode: RepairMode;
  backendState: WorkState;
  streamingEnabled: boolean;
  acceptedConstraintIds: string[];
  analysisState: WorkState;
  hasEvidence: boolean;
  evidenceText: string;
  copyState: "idle" | "copied" | "failed";
  fileInputRef: RefObject<HTMLInputElement | null>;
  problem: ProblemDetail | null;
  latestAnalysis: AnalyzeResponse | null;
  primaryMoment: PrimaryRepairMoment | null;
  observatory: ReturnType<typeof buildObservatoryView>;
  onAdvancedChange: (next: boolean) => void;
  onRepairModeChange: (next: RepairMode) => void;
  onChooseSample: (sampleName: string) => void | Promise<void>;
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void | Promise<void>;
  onAnalyze: () => void;
  onRerun: () => void;
  onCancel: () => void;
  onCopy: () => void;
  onExport: () => void;
  onBackendRetry: () => void;
  onNavigate: (routeId: ProductRouteId) => void;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  return (
    <main className="route-page run-page" aria-busy={busy}>
      <MissionBar
        dataset={dataset}
        busy={busy}
        canRun={canRun}
        maxUploadBytes={maxUploadBytes}
        capability={capability}
        advanced={advanced}
        repairMode={repairMode}
        backendState={backendState}
        streamingEnabled={streamingEnabled}
        acceptedConstraintIds={acceptedConstraintIds}
        analysisState={analysisState}
        hasEvidence={hasEvidence}
        copyState={copyState}
        fileInputRef={fileInputRef}
        onAdvancedChange={onAdvancedChange}
        onRepairModeChange={onRepairModeChange}
        onChooseSample={onChooseSample}
        onFileChange={onFileChange}
        onAnalyze={onAnalyze}
        onRerun={onRerun}
        onCancel={onCancel}
        onCopy={onCopy}
        onExport={onExport}
        onBackendRetry={onBackendRetry}
      />
      <ProductLoopRail dataset={dataset} analysis={latestAnalysis} primaryMoment={primaryMoment} />
      {copyState === "failed" && evidenceText ? <CopyFallback evidenceText={evidenceText} /> : null}
      {problem ? <ProblemBanner problem={problem} /> : null}
      {latestAnalysis ? <TrustVerdictPanel verdict={buildTrustVerdict(latestAnalysis)} /> : null}
      {latestAnalysis?.agent ? <AgentSummaryPanel agent={latestAnalysis.agent} /> : null}
      <ProductLoopWorkbench
        dataset={dataset}
        analysis={latestAnalysis}
        primaryMoment={primaryMoment}
        hasEvidence={hasEvidence}
        copyState={copyState}
        onCopy={onCopy}
        onExport={onExport}
        onNavigate={onNavigate}
      />
      <OverviewLens
        dataset={dataset}
        preview={dataset?.preview ?? null}
        analysis={latestAnalysis}
        observatory={observatory}
        onSelect={onSelect}
      />
      <section className="route-actions" aria-label="Next pages">
        <button type="button" onClick={() => onNavigate("atlas")}>Open proof details</button>
        <button type="button" onClick={() => onNavigate("evidence")}>Open Evidence</button>
      </section>
    </main>
  );
}

function AtlasPage({
  stages,
  runId,
  status,
  selectedEvidence,
  reviewItems,
  analysis,
  selectedConstraintIds,
  canRerun,
  onToggleConstraint,
  onRerun,
  onSelect,
  onNavigate,
}: {
  stages: WorkflowStageView[];
  runId: string | null;
  status: string;
  selectedEvidence: SelectedEvidence | null;
  reviewItems: ReviewItem[];
  analysis: AnalyzeResponse | null;
  selectedConstraintIds: string[];
  canRerun: boolean;
  onToggleConstraint: (candidateId: string, checked: boolean) => void;
  onRerun: () => void;
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page atlas-page">
      <ProofAtlas stages={stages} runId={runId} status={status} selectedEvidence={selectedEvidence} onSelect={onSelect} />
      <ReviewQueue
        items={reviewItems}
        analysis={analysis}
        selectedConstraintIds={selectedConstraintIds}
        canRerun={canRerun}
        onToggleConstraint={onToggleConstraint}
        onRerun={onRerun}
        onSelect={onSelect}
      />
      {!analysis ? <EmptyPagePrompt title="No completed run yet" onNavigate={onNavigate} /> : null}
    </main>
  );
}

function EvidencePage({
  dataset,
  preview,
  state,
  analysis,
  observatory,
  issues,
  filter,
  severityFilter,
  sortKey,
  problem,
  selectedEvidence,
  allIssues,
  onFilterChange,
  onSeverityFilterChange,
  onSortChange,
  onSelect,
  onNavigate,
}: {
  dataset: DatasetInput | null;
  preview: CsvPreview | null;
  state: WorkState;
  analysis: AnalyzeResponse | null;
  observatory: ReturnType<typeof buildObservatoryView>;
  issues: IssueGroup[];
  filter: string;
  severityFilter: Severity | "all";
  sortKey: SortKey;
  problem: ProblemDetail | null;
  selectedEvidence: SelectedEvidence | null;
  allIssues: IssueGroup[];
  onFilterChange: (value: string) => void;
  onSeverityFilterChange: (value: Severity | "all") => void;
  onSortChange: (value: SortKey) => void;
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page split-page">
      <section className="workbench-plane">
        {problem ? <ProblemBanner problem={problem} /> : null}
        <OverviewLens dataset={dataset} preview={preview} analysis={analysis} observatory={observatory} onSelect={onSelect} />
        <RiskLens
          state={state}
          analysis={analysis}
          issues={issues}
          filter={filter}
          severityFilter={severityFilter}
          sortKey={sortKey}
          onFilterChange={onFilterChange}
          onSeverityFilterChange={onSeverityFilterChange}
          onSortChange={onSortChange}
          onSelect={onSelect}
        />
        {!analysis ? <EmptyPagePrompt title="Run analysis to unlock evidence" onNavigate={onNavigate} /> : null}
      </section>
      <EvidenceDock selectedEvidence={selectedEvidence} stages={[]} analysis={analysis} issues={allIssues} problem={problem} />
    </main>
  );
}

function RepairsPage({
  state,
  analysis,
  dataset,
  selectedEvidence,
  issues,
  problem,
  onSelect,
  onNavigate,
}: {
  state: WorkState;
  analysis: AnalyzeResponse | null;
  dataset: DatasetInput | null;
  selectedEvidence: SelectedEvidence | null;
  issues: IssueGroup[];
  problem: ProblemDetail | null;
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page split-page">
      <section className="workbench-plane">
        <RepairsLens state={state} analysis={analysis} dataset={dataset} onSelect={onSelect} />
        {!analysis ? <EmptyPagePrompt title="Run analysis to unlock repairs" onNavigate={onNavigate} /> : null}
      </section>
      <EvidenceDock selectedEvidence={selectedEvidence} stages={[]} analysis={analysis} issues={issues} problem={problem} />
    </main>
  );
}

function GuardrailPage({
  client,
  capability,
  backendState,
  maxUploadBytes,
  onBackendRetry,
  onNavigate,
}: {
  client: DataForgeClient;
  capability: BackendCapability | null;
  backendState: WorkState;
  maxUploadBytes: number;
  onBackendRetry: () => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [sampleName, setSampleName] = useState<string | null>(null);
  const [preview, setPreview] = useState<CsvPreview | null>(null);
  const [fixes, setFixes] = useState<ExternalFix[]>([]);
  const [proposer, setProposer] = useState("external-agent");
  const [acceptedConstraintIds, setAcceptedConstraintIds] = useState<string[]>([]);
  const [confirmEscalations, setConfirmEscalations] = useState(true);
  const [allowUnproven, setAllowUnproven] = useState(false);
  const [scenarioNote, setScenarioNote] = useState<string | null>(null);
  const [result, setResult] = useState<VerifyFixesResponse | null>(null);
  const [state, setState] = useState<WorkState>("idle");
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const guardrailFileInput = useRef<HTMLInputElement | null>(null);

  const busy = state === "loading";
  const backendReady = backendState === "ready";
  const canVerify = backendReady && !busy && file !== null && fixes.length > 0;

  function resetOutput() {
    setResult(null);
    setProblem(null);
  }

  async function chooseSample(value: string) {
    if (busy) {
      return;
    }
    resetOutput();
    setScenarioNote(null);
    setFixes([]);
    setAcceptedConstraintIds([]);
    try {
      const sampleFile = await client.sample(value);
      setFile(sampleFile);
      setSampleName(value);
      setPreview(parseCsvPreview(await sampleFile.text()));
    } catch (error) {
      setProblem(problemFromUnknown(error));
    }
  }

  async function onFileChange(event: ChangeEvent<HTMLInputElement>) {
    const chosen = event.target.files?.[0];
    if (!chosen) {
      return;
    }
    resetOutput();
    setScenarioNote(null);
    setFixes([]);
    setAcceptedConstraintIds([]);
    const validation = validateCsvFile(chosen, maxUploadBytes);
    if (!validation.ok) {
      setProblem(localProblem(validation.message ?? "The CSV file could not be accepted."));
      return;
    }
    try {
      setFile(chosen);
      setSampleName(null);
      setPreview(parseCsvPreview(await chosen.text()));
    } catch (error) {
      setProblem(problemFromUnknown(error));
    }
  }

  async function loadScenario() {
    if (!sampleName || busy) {
      return;
    }
    resetOutput();
    try {
      const scenario = await client.verifyScenario(sampleName);
      setFixes(scenario.fixes);
      setAcceptedConstraintIds(scenario.accepted_constraint_ids);
      setProposer(scenario.proposer);
      setScenarioNote(scenario.note);
    } catch (error) {
      setProblem(problemFromUnknown(error));
    }
  }

  function updateFix(index: number, patch: Partial<ExternalFix>) {
    setFixes((current) => current.map((fix, i) => (i === index ? { ...fix, ...patch } : fix)));
  }

  function addFix() {
    setFixes((current) => [...current, { row: 0, column: "", new_value: "" }]);
  }

  function removeFix(index: number) {
    setFixes((current) => current.filter((_, i) => i !== index));
  }

  async function verify() {
    if (!file || fixes.length === 0) {
      return;
    }
    setState("loading");
    setProblem(null);
    try {
      const response = await client.verifyFixes(file, fixes, {
        acceptedConstraintIds,
        proposer,
        confirmEscalations,
        allowUnproven,
      });
      setResult(response);
      setState("ready");
    } catch (error) {
      setResult(null);
      setState("error");
      setProblem(problemFromUnknown(error));
    }
  }

  const verdict = buildGuardrailVerdict(result);

  return (
    <main className="route-page guardrail-page">
      <header className="guardrail-hero">
        <p className="eyebrow">Agent guardrail</p>
        <h1>Verify an untrusted actor&apos;s proposed fixes</h1>
        <p>
          Propose cell edits as an agent, tool, or human would. DataForge proves the correct ones
          against an authoritative schema, holds or rejects the rest with an honest reason, and
          emits a certificate you can re-verify. Nothing is applied &mdash; this is a stateless dry
          run.
        </p>
      </header>

      {!backendReady ? (
        <BackendStatus state={backendState} capability={capability} onRetry={onBackendRetry} />
      ) : null}
      {problem ? <ProblemBanner problem={problem} /> : null}

      <section className="guardrail-setup" aria-label="Guardrail inputs">
        <div className="guardrail-intake">
          <p className="eyebrow">1. Choose a dataset</p>
          <div className="sample-chip-row" role="group" aria-label="Sample datasets">
            {SAMPLE_OPTIONS.map((sample) => (
              <button
                type="button"
                key={sample.value}
                className={
                  sampleName === sample.value ? "sample-chip sample-chip--active" : "sample-chip"
                }
                onClick={() => void chooseSample(sample.value)}
                disabled={busy}
              >
                <strong>{sample.label}</strong>
                <small>{sample.detail}</small>
              </button>
            ))}
          </div>
          <label className="guardrail-upload">
            <span>or upload a CSV</span>
            <input
              ref={guardrailFileInput}
              id="guardrail-csv-upload"
              type="file"
              accept=".csv,text/csv"
              onChange={(event) => void onFileChange(event)}
              disabled={busy}
            />
          </label>
          {preview ? (
            <p className="guardrail-dataset-note" role="status">
              Verifying against <code>{file?.name}</code> &mdash; {preview.columns.length} columns.
            </p>
          ) : (
            <EvidenceNote
              title="No dataset selected"
              body="Pick a sample or upload a CSV to verify proposed fixes against."
            />
          )}
        </div>

        <div className="guardrail-proposals">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">2. Propose fixes</p>
              <h2>Untrusted proposals</h2>
            </div>
            {sampleName ? (
              <button
                type="button"
                className="ghost-button"
                onClick={() => void loadScenario()}
                disabled={busy}
              >
                Load scripted agent batch
              </button>
            ) : null}
          </div>
          {scenarioNote ? <p className="guardrail-scenario-note">{scenarioNote}</p> : null}
          {acceptedConstraintIds.length > 0 ? (
            <p className="guardrail-schema-chip" role="status">
              <ShieldCheck aria-hidden="true" /> Authoritative schema:{" "}
              {acceptedConstraintIds.length} accepted constraint
              {acceptedConstraintIds.length === 1 ? "" : "s"}
            </p>
          ) : (
            <p className="guardrail-schema-chip guardrail-schema-chip--none">
              No authoritative schema &mdash; proposals can only be held, never proven.
            </p>
          )}

          {fixes.length > 0 ? (
            <ul className="fix-editor" aria-label="Proposed fixes">
              {fixes.map((fix, index) => (
                <li className="fix-editor__row" key={index}>
                  <label>
                    <span>Row</span>
                    <input
                      type="number"
                      min={0}
                      value={fix.row}
                      onChange={(event) =>
                        updateFix(index, { row: Number(event.target.value) || 0 })
                      }
                    />
                  </label>
                  <label>
                    <span>Column</span>
                    <input
                      type="text"
                      value={fix.column}
                      onChange={(event) => updateFix(index, { column: event.target.value })}
                    />
                  </label>
                  <label>
                    <span>New value</span>
                    <input
                      type="text"
                      value={fix.new_value}
                      onChange={(event) => updateFix(index, { new_value: event.target.value })}
                    />
                  </label>
                  <label>
                    <span>Expected old (optional)</span>
                    <input
                      type="text"
                      value={fix.expected_old_value ?? ""}
                      onChange={(event) =>
                        updateFix(index, { expected_old_value: event.target.value || null })
                      }
                    />
                  </label>
                  <button
                    type="button"
                    className="fix-editor__remove"
                    aria-label={`Remove fix ${index + 1}`}
                    onClick={() => removeFix(index)}
                  >
                    Remove
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <EvidenceNote
              title="No proposals yet"
              body="Add a fix or load the scripted agent batch to see the guardrail in action."
            />
          )}

          <div className="guardrail-controls">
            <button type="button" className="ghost-button" onClick={addFix} disabled={busy}>
              Add fix
            </button>
            <label className="guardrail-field">
              <span>Proposer</span>
              <input
                type="text"
                value={proposer}
                onChange={(event) => setProposer(event.target.value)}
              />
            </label>
            <label className="guardrail-toggle">
              <input
                type="checkbox"
                checked={confirmEscalations}
                onChange={(event) => setConfirmEscalations(event.target.checked)}
              />
              <span>Confirm escalations (dry run)</span>
            </label>
            <label className="guardrail-toggle">
              <input
                type="checkbox"
                checked={allowUnproven}
                onChange={(event) => setAllowUnproven(event.target.checked)}
              />
              <span>Allow unproven</span>
            </label>
          </div>

          <button
            type="button"
            className="primary-button guardrail-verify"
            onClick={() => void verify()}
            disabled={!canVerify}
          >
            <ShieldCheck aria-hidden="true" /> Verify proposed fixes
          </button>
        </div>
      </section>

      {state === "loading" ? <LoadingState label="Verifying proposed fixes" /> : null}

      {result ? (
        <>
          <GuardrailVerdictPanel verdict={verdict} />
          <WouldApplyList fixes={result.would_apply} />
          <HeldForReviewList items={result.receipt.suggested_fixes ?? []} />
          <CertificatePanel
            certificate={result.certificate}
            independentVerification={result.receipt.independent_verification ?? "not_run"}
            auditCommand={result.apply_handoff.audit_command}
            onDownload={() => downloadCertificate(result)}
          />
          <section className="route-actions" aria-label="Next pages">
            <button type="button" onClick={() => onNavigate("run")}>
              Back to the repair loop
            </button>
          </section>
        </>
      ) : null}
    </main>
  );
}

function GuardrailVerdictPanel({ verdict }: { verdict: GuardrailVerdict }) {
  if (verdict.level === "pending") {
    return null;
  }
  const independentAgreed = verdict.independentVerification === "agreed";
  return (
    <motion.section
      className={`trust-verdict trust-verdict--${verdict.level}`}
      aria-labelledby="guardrail-verdict-title"
      role="status"
      aria-live="polite"
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <header className="trust-verdict__head">
        <div className="trust-verdict__title">
          <ShieldCheck aria-hidden="true" />
          <div>
            <p className="eyebrow">Guardrail verdict</p>
            <h2 id="guardrail-verdict-title">{verdict.headline}</h2>
          </div>
        </div>
        <p className="trust-verdict__guarantee">{verdict.guaranteeLine}</p>
      </header>
      <dl className="trust-verdict__metrics">
        {verdict.metrics.map((metric) => (
          <div className={`trust-metric trust-metric--${metric.tone}`} key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>
              <span className="trust-metric__value">{metric.value}</span>
              <span className="trust-metric__hint">{metric.hint}</span>
            </dd>
          </div>
        ))}
      </dl>
      <p className="trust-verdict__foot">
        Independent verifier: <strong>{independentAgreed ? "agreed" : "not run"}</strong>. The
        hosted playground never mutates uploads; applying proven fixes is a local CLI workflow.
      </p>
    </motion.section>
  );
}

function WouldApplyList({ fixes }: { fixes: VerifiedFix[] }) {
  if (fixes.length === 0) {
    return (
      <EvidenceNote
        title="Nothing proven to apply"
        body="No proposal was verified against an authoritative schema, so none would auto-apply. Refusing to write unproven values is the correct behavior."
      />
    );
  }
  return (
    <section className="would-apply-list" aria-labelledby="would-apply-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Proven, would apply</p>
          <h2 id="would-apply-title">Verified external fixes</h2>
        </div>
        <span className="quiet-chip quiet-chip--ok">{fixes.length} proven</span>
      </div>
      {fixes.map((fix) => (
        <article className="would-apply-row" key={`${fix.row}:${fix.column}:${fix.new_value}`}>
          <div className="would-apply-row__head">
            <strong>
              Row {fix.row}, <code>{fix.column}</code>
            </strong>
            <VerificationStrengthBadge strength={strengthOf(fix)} />
          </div>
          <span className="would-apply-row__change">
            {fix.old_value || "(empty)"} &rarr; {fix.new_value || "(empty)"}
          </span>
          {fix.verifier_reason ? <p className="verifier-note">{fix.verifier_reason}</p> : null}
        </article>
      ))}
    </section>
  );
}

function ReceiptPage({
  analysis,
  evidenceText,
  copyState,
  selectedEvidence,
  stages,
  issues,
  problem,
  onCopy,
  onExport,
  onSelect,
  onNavigate,
}: {
  analysis: AnalyzeResponse | null;
  evidenceText: string;
  copyState: "idle" | "copied" | "failed";
  selectedEvidence: SelectedEvidence | null;
  stages: WorkflowStageView[];
  issues: IssueGroup[];
  problem: ProblemDetail | null;
  onCopy: () => void;
  onExport: () => void;
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page split-page">
      <section className="workbench-plane">
        <div className="receipt-toolbar">
          <button className="icon-button" type="button" disabled={!evidenceText} onClick={onCopy}>
            <ClipboardCopy aria-hidden="true" />
            {copyState === "copied" ? "Copied" : copyState === "failed" ? "Copy failed" : "Copy"}
          </button>
          <button className="icon-button" type="button" disabled={!evidenceText} onClick={onExport}>
            <Download aria-hidden="true" />
            Export
          </button>
        </div>
        <ReceiptLens analysis={analysis} onSelect={onSelect} />
        <RawEvidenceLens analysis={analysis} evidenceText={evidenceText} />
        {copyState === "failed" && evidenceText ? <CopyFallback evidenceText={evidenceText} /> : null}
        {!analysis ? <EmptyPagePrompt title="Run analysis to unlock receipt" onNavigate={onNavigate} /> : null}
      </section>
      <EvidenceDock selectedEvidence={selectedEvidence} stages={stages} analysis={analysis} issues={issues} problem={problem} />
    </main>
  );
}

function SystemPage({
  capability,
  backendState,
  streamingEnabled,
  maxUploadBytes,
  analysis,
}: {
  capability: BackendCapability | null;
  backendState: WorkState;
  streamingEnabled: boolean;
  maxUploadBytes: number;
  analysis: AnalyzeResponse | null;
}) {
  return (
    <main className="route-page system-page">
      <section className="system-grid">
        <Metric label="Backend" value={backendState} />
        <Metric label="Streaming" value={streamingEnabled ? "workflow_event_v1" : "JSON fallback"} />
        <Metric label="Upload cap" value={`${Math.floor(maxUploadBytes / 1024)} KiB`} />
        <Metric label="Advanced" value={capability?.advanced_available ? "available" : "unavailable"} />
        <Metric label="API version" value={capability?.api_version ?? analysis?.meta.api_version ?? "pending"} />
        <Metric label="Contract" value={analysis?.meta.contract_version ?? capability?.contract_version ?? "pending"} />
      </section>
      <section className="state-legend" aria-labelledby="state-legend-title">
        <div>
          <p className="eyebrow">Semantic State</p>
          <h2 id="state-legend-title">Proof intelligence legend</h2>
        </div>
        <div className="legend-grid">
          <span className="legend-item legend-item--command">Primary command</span>
          <span className="legend-item legend-item--active">Vermilion active</span>
          <span className="legend-item legend-item--info">Teal evidence</span>
          <span className="legend-item legend-item--verified">Viridian proof</span>
          <span className="legend-item legend-item--review">Brass review</span>
          <span className="legend-item legend-item--danger">Hematite danger</span>
          <span className="legend-item legend-item--agent">Ultraviolet agent</span>
          <span className="legend-item legend-item--selection">Selected evidence</span>
          <span className="legend-item legend-item--loading">Loading progress</span>
          <span className="legend-item legend-item--disabled">Disabled boundary</span>
          <span className="legend-item legend-item--verifying">Verifying in progress</span>
          <span className="legend-item legend-item--proposing">Proposing (plausibility)</span>
          <span className="legend-item legend-item--proven">Proven, applied</span>
          <span className="legend-item legend-item--held">Held for review</span>
          <span className="legend-item legend-item--rejected">Rejected</span>
          <span className="legend-item legend-item--asking">Needs a human</span>
          <span className="legend-item legend-item--done">Done</span>
          <span className="legend-item legend-item--idle">Idle</span>
        </div>
      </section>
      <section className="handoff-panel">
        <p className="eyebrow">Hosted Safety</p>
        <h2>Stateless dry-run contract</h2>
        <p>No browser storage, no frontend keys, no hosted apply/revert mutation, and no silent verifier bypass.</p>
      </section>
    </main>
  );
}

function EmptyPagePrompt({
  title,
  onNavigate,
}: {
  title: string;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <section className="empty-route">
      <FileText aria-hidden="true" />
      <strong>{title}</strong>
      <p>This page is part of the in-memory playground session. Load a dataset and run analysis from the command center.</p>
      <button type="button" className="primary-action" onClick={() => onNavigate("run")}>
        Open Run Page
      </button>
    </section>
  );
}

function MissionBar({
  dataset,
  busy,
  canRun,
  maxUploadBytes,
  capability,
  advanced,
  repairMode,
  backendState,
  streamingEnabled,
  acceptedConstraintIds,
  analysisState,
  hasEvidence,
  copyState,
  fileInputRef,
  onAdvancedChange,
  onRepairModeChange,
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
  backendState: WorkState;
  streamingEnabled: boolean;
  acceptedConstraintIds: string[];
  analysisState: WorkState;
  hasEvidence: boolean;
  copyState: "idle" | "copied" | "failed";
  fileInputRef: RefObject<HTMLInputElement | null>;
  onAdvancedChange: (next: boolean) => void;
  onRepairModeChange: (next: RepairMode) => void;
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

function DatasetIntake({
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

function ProductLoopRail({
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
      detail: analysis ? `${analysis.receipt.safety_verdict} safety, ${analysis.receipt.verifier_verdict} verifier` : "Explain apply and revert",
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

function ProductLoopWorkbench({
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

function ProfileSummary({
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

function IssueReview({
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

function VerifiedRepairReview({
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

function ReceiptExport({
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

function SafetyRevertExplainer({ analysis }: { analysis: AnalyzeResponse | null }) {
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

function ProofAtlas({
  stages,
  runId,
  status,
  selectedEvidence,
  onSelect,
}: {
  stages: WorkflowStageView[];
  runId: string | null;
  status: string;
  selectedEvidence: SelectedEvidence | null;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  return (
    <section className="proof-atlas" aria-labelledby="proof-atlas-title">
      <div className="atlas-heading">
        <div>
          <p className="eyebrow">Proof Atlas</p>
          <h2 id="proof-atlas-title">Live agent workflow</h2>
        </div>
        <span className={`run-state run-state--${status}`}>{formatLabel(status)}</span>
      </div>
      <ol className="atlas-track" aria-label="Workflow stages">
        {stages.map((stage, index) => (
          <StageNode
            key={stage.id}
            stage={stage}
            index={index}
            selected={selectedEvidence?.kind === "stage" && selectedEvidence.id === stage.id}
            onSelect={() => onSelect({ kind: "stage", id: stage.id })}
          />
        ))}
      </ol>
      <div className="atlas-footer">
        <span>{runId ? `run ${runId.slice(0, 12)}` : "no active run"}</span>
        <span>9-stage dry-run contract</span>
      </div>
    </section>
  );
}

function StageNode({
  stage,
  index,
  selected,
  onSelect,
}: {
  stage: WorkflowStageView;
  index: number;
  selected: boolean;
  onSelect: () => void;
}) {
  const tone = toneClass(stage.status);
  const agentState = workflowStatusToAgentState(stage.status);
  return (
    <motion.li
      className={`stage-node stage-node--${tone}`}
      data-agent-motion={agentState}
      data-workflow-status={stage.status}
      layout
      variants={stageNodeVariants}
      initial={false}
      animate={stage.status}
      transition={motionSprings.soft}
    >
      <button
        type="button"
        className="stage-node-button"
        aria-pressed={selected}
        onClick={onSelect}
      >
        <span className="stage-index">{String(index + 1).padStart(2, "0")}</span>
        <AnimatePresence mode="wait" initial={false}>
          <motion.span
            key={stage.status}
            className="stage-icon"
            aria-hidden="true"
            initial={{ opacity: 0, scale: 0.92 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.96 }}
            transition={{ duration: motionDurations.fast }}
          >
            {stage.status === "completed" ? (
              <CheckCircle2 />
            ) : stage.status === "blocked" || stage.status === "failed" ? (
              <AlertTriangle />
            ) : stage.status === "running" ? (
              <RefreshCw />
            ) : (
              <CircleDot />
            )}
          </motion.span>
        </AnimatePresence>
        <span className="stage-copy">
          <strong>{stage.label}</strong>
          <small>{formatLabel(stage.status)}</small>
        </span>
        {stage.requiresHuman ? <span className="human-dot">Review</span> : null}
      </button>
      <p>{stage.summary || stage.description}</p>
      <StageCounts counts={stage.counts} />
    </motion.li>
  );
}

function ReviewQueue({
  items,
  analysis,
  selectedConstraintIds,
  canRerun,
  onToggleConstraint,
  onRerun,
  onSelect,
}: {
  items: ReviewItem[];
  analysis: AnalyzeResponse | null;
  selectedConstraintIds: string[];
  canRerun: boolean;
  onToggleConstraint: (candidateId: string, checked: boolean) => void;
  onRerun: () => void;
  onSelect: (selection: SelectedEvidence) => void;
}) {
  return (
    <motion.aside
      className="review-queue"
      aria-label="Human review queue"
      layout
      variants={panelVariants}
      initial="initial"
      animate="animate"
      exit="exit"
    >
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Human Review</p>
          <h2>Decisions and boundaries</h2>
        </div>
        <span className="quiet-chip">{items.length} items</span>
      </div>

      <ConstraintReviewControls
        candidates={analysis?.schema_inference.candidates ?? []}
        selectedConstraintIds={selectedConstraintIds}
        onToggleConstraint={onToggleConstraint}
        onSelect={(candidateId) => onSelect({ kind: "constraint", id: candidateId })}
      />

      <div className="review-list">
        {items.map((item) => (
          <motion.button
            key={item.id}
            type="button"
            className={`review-item review-item--${item.tone}`}
            aria-label={`${formatLabel(item.kind)} ${item.title}`}
            onClick={() => onSelect(selectionFromReviewItem(item))}
            layout
            initial={{ opacity: 0, y: 3 }}
            animate={{ opacity: 1, y: 0 }}
            whileHover={{ y: -1 }}
            whileTap={{ scale: 0.99 }}
            transition={motionSprings.soft}
          >
            <span>{formatLabel(item.kind)}</span>
            <strong>{item.title}</strong>
            <small>{item.meta}</small>
            <p>{item.detail}</p>
          </motion.button>
        ))}
      </div>

      <motion.button
        className="queue-rerun"
        type="button"
        disabled={!canRerun}
        onClick={onRerun}
        whileTap={canRerun ? { scale: 0.99 } : undefined}
      >
        <RefreshCw aria-hidden="true" />
        Rerun with accepted constraints
      </motion.button>

      <div className="autonomy-boundary" aria-label="Autonomy boundary">
        <BrainCircuit aria-hidden="true" />
        <div>
          <strong>Hosted agency is bounded</strong>
          <p>Analyze, infer, propose, verify. Apply, audit, and revert stay local.</p>
        </div>
      </div>
    </motion.aside>
  );
}

function ConstraintReviewControls({
  candidates,
  selectedConstraintIds,
  onToggleConstraint,
  onSelect,
}: {
  candidates: ConstraintCandidate[];
  selectedConstraintIds: string[];
  onToggleConstraint: (candidateId: string, checked: boolean) => void;
  onSelect: (candidateId: string) => void;
}) {
  const supported = candidates.filter((candidate) => candidate.repair_supported);
  if (supported.length === 0) {
    return (
      <div className="constraint-controls">
        <strong>Accepted constraints</strong>
        <p>No repair-supported inferred constraints are waiting for this run.</p>
      </div>
    );
  }
  const selected = new Set(selectedConstraintIds);
  return (
    <div className="constraint-controls">
      <strong>Accepted constraints</strong>
      {supported.map((candidate) => {
        const checked = selected.has(candidate.candidate_id);
        return (
        <motion.label
          key={candidate.candidate_id}
          className="constraint-toggle"
          data-decision-state={checked ? "accepted" : "pending"}
          layout
          animate={checked ? { scale: 1.006 } : { scale: 1 }}
          transition={motionSprings.soft}
        >
          <input
            type="checkbox"
            aria-label={`Accept ${candidate.kind} constraint ${candidate.candidate_id}`}
            checked={checked}
            onChange={(event) => onToggleConstraint(candidate.candidate_id, event.target.checked)}
            onFocus={() => onSelect(candidate.candidate_id)}
          />
          <span>
            <b>{formatLabel(candidate.kind)}</b>
            <small>{formatConstraintColumns(candidate)} - {formatPercent(candidate.confidence)}</small>
          </span>
        </motion.label>
        );
      })}
    </div>
  );
}

function OverviewLens({
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

      {analysis ? (
        <RiskSummaryPanel
          datasetLevel={analysis.risk_summary.dataset_level}
          readiness={analysis.risk_summary.repair_readiness}
          reasons={analysis.risk_summary.reasons}
        />
      ) : null}
    </div>
  );
}

function RiskLens({
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

function RepairsLens({
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
        <Metric label="Verifier" value={analysis.verification.verifier_verdict} />
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
      <RepairComparison repairs={analysis.repairs} analysis={analysis} onSelect={onSelect} />
      <HeldForReviewList items={analysis.receipt.suggested_fixes ?? []} />
      <CandidateRepairList candidates={analysis.receipt.candidate_repairs} />
      <FailureList failures={analysis.verification.failures} onSelect={onSelect} titleId="repair-failures-title" />
    </div>
  );
}

function ReceiptLens({
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

function RawEvidenceLens({
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

function EvidenceDock({
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

function resolveDockContent(
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
        meta: fix.provenance,
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
          { label: "Unsat core", value: obligation.unsat_core.join(", ") || "none" },
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
        { label: "Verifier", value: analysis.receipt.verifier_verdict },
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
          { label: "Safety", value: analysis.receipt.safety_verdict },
          { label: "Verifier", value: analysis.receipt.verifier_verdict },
        ]
      : [],
  };
}

function DatasetBadge({ dataset }: { dataset: DatasetInput | null }) {
  if (!dataset) {
    return <span className="quiet-chip">Waiting</span>;
  }
  return (
    <span className="quiet-chip">
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

function RiskSummaryPanel({
  datasetLevel,
  readiness,
  reasons,
}: {
  datasetLevel: RiskLevel;
  readiness: RepairReadiness;
  reasons: string[];
}) {
  return (
    <section className="risk-panel" aria-label="Risk reasons">
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

function RiskBadge({ label, value }: { label: string; value: RiskLevel | RepairReadiness }) {
  return (
    <span className={`risk-badge risk-badge--${value}`}>
      <strong>{label}</strong>
      {formatLabel(value)}
    </span>
  );
}

function IssueTable({
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

function ConstraintEvidenceTable({
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

function RepairComparison({
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

function CandidateRepairList({ candidates }: { candidates: CandidateRepair[] }) {
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
          <span>{candidate.detector_id} - {candidate.operation} - {candidate.provenance}</span>
          <div className="candidate-row__strength">
            <VerificationStrengthBadge strength={strengthOf(candidate)} />
          </div>
          <p>{candidate.verifier_reason}</p>
        </article>
      ))}
    </section>
  );
}

function FailureList({
  failures,
  onSelect,
  titleId = "failures-title",
}: {
  failures: RepairFailure[];
  onSelect: (selection: SelectedEvidence) => void;
  titleId?: string;
}) {
  if (failures.length === 0) {
    return <EvidenceNote title="No repair abstentions" body="Every attempted repair either verified or no issue required a fix." />;
  }
  return (
    <section className="failure-list" aria-labelledby={titleId}>
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Abstentions</p>
          <h2 id={titleId}>Attempted but not fixed</h2>
        </div>
        <span className="quiet-chip">{failures.length} failures</span>
      </div>
      {failures.map((failure) => (
        <article className="failure-row" key={failureKey(failure)}>
          <button type="button" onClick={() => onSelect({ kind: "failure", id: failureKey(failure) })}>
            <strong>
              Row {failure.row}, <code>{failure.column}</code>
            </strong>
            <span>{failure.issue_type} - {failure.status} - attempts {failure.attempt_count}</span>
          </button>
          <p>{failure.reason}</p>
          {failure.unsat_core.length > 0 ? <code>{failure.unsat_core.join(", ")}</code> : null}
        </article>
      ))}
    </section>
  );
}

function ReceiptSummary({ analysis }: { analysis: AnalyzeResponse }) {
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
        <Metric label="Safety" value={analysis.receipt.safety_verdict} />
        <Metric label="Verifier" value={analysis.receipt.verifier_verdict} />
        <Metric
          label="Independent verify"
          value={analysis.receipt.independent_verification ?? "not_run"}
        />
        <Metric label="Reversible" value={analysis.receipt.reversible ? "yes" : "no"} />
      </div>
      <p>{analysis.receipt.reason}</p>
    </motion.section>
  );
}

function ReceiptHandoff({ analysis }: { analysis: AnalyzeResponse }) {
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

function CommandRow({ label, command, delay = 0 }: { label: string; command: string; delay?: number }) {
  return (
    <motion.div
      className="command-row"
      initial={{ opacity: 0, y: 3 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, duration: motionDurations.fast }}
    >
      <span>{label}</span>
      <code>{command}</code>
    </motion.div>
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

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function SeverityBadge({ severity }: { severity: Severity }) {
  return <span className={`severity severity--${severity}`}>{severity}</span>;
}

function ConfidenceBadge({ value }: { value: number }) {
  const bucket = value >= 0.85 ? "high" : value >= 0.65 ? "medium" : "low";
  return <span className={`confidence confidence--${bucket}`}>{formatPercent(value)}</span>;
}

function VerificationStrengthBadge({ strength }: { strength: VerificationStrength }) {
  const proven = strength === "proven";
  return (
    <span
      className={`strength-badge strength-badge--${proven ? "proven" : "plausibility"}${proven ? " df-motion-settle" : ""}`}
      title={
        proven
          ? "Proven: deterministic or verified against an authoritative schema. Safe to auto-apply."
          : "Plausibility-only: a model-proposed value with no authoritative schema. Never silently written."
      }
    >
      {proven ? <BadgeCheck aria-hidden="true" /> : <AlertTriangle aria-hidden="true" />}
      {proven ? "proven" : "plausible \u00b7 not written"}
    </span>
  );
}

function TrustVerdictPanel({ verdict }: { verdict: TrustVerdict }) {
  if (verdict.level === "pending") {
    return null;
  }
  const independentAgreed = verdict.independentVerification === "agreed";
  return (
    <motion.section
      className={`trust-verdict trust-verdict--${verdict.level}`}
      aria-labelledby="trust-verdict-title"
      role="status"
      aria-live="polite"
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <header className="trust-verdict__head">
        <div className="trust-verdict__title">
          <ShieldCheck aria-hidden="true" />
          <div>
            <p className="eyebrow">Trust verdict</p>
            <h2 id="trust-verdict-title">{verdict.headline}</h2>
          </div>
        </div>
        <p className="trust-verdict__guarantee">{verdict.guaranteeLine}</p>
      </header>
      <dl className="trust-verdict__metrics">
        {verdict.metrics.map((metric) => (
          <div className={`trust-metric trust-metric--${metric.tone}`} key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>
              <span className="trust-metric__value">{metric.value}</span>
              <span className="trust-metric__hint">{metric.hint}</span>
            </dd>
          </div>
        ))}
      </dl>
      <p className="trust-verdict__foot">
        {independentAgreed ? (
          <span className="corroborated-chip">
            <BadgeCheck aria-hidden="true" /> Independently verified
          </span>
        ) : (
          <span className="single-verifier-note">Single verifier</span>
        )}{" "}
        {independentAgreed
          ? "— two independently written verifiers agreed on the applied set."
          : "— the deterministic gate proved every applied change; a second cross-check was not required for this run."}
      </p>
    </motion.section>
  );
}

function CertificatePanel({
  certificate,
  independentVerification,
  auditCommand,
  onDownload,
}: {
  certificate: Certificate;
  independentVerification: string;
  auditCommand: string;
  onDownload: () => void;
}) {
  const passed = certificate.checks.filter((check) => check.ok).length;
  return (
    <motion.section
      className={`certificate-panel certificate-panel--${certificate.ok ? "ok" : "attention"}`}
      aria-labelledby="certificate-title"
      variants={panelVariants}
      initial="initial"
      animate="animate"
      exit="exit"
    >
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Portable trust certificate</p>
          <h2 id="certificate-title">
            {certificate.ok
              ? `Re-verified ${passed}/${certificate.checks.length} checks`
              : `Certificate reports ${certificate.checks.length - passed} unmet check(s)`}
          </h2>
        </div>
        <span className={`quiet-chip quiet-chip--${certificate.ok ? "ok" : "attention"}`}>
          {certificate.ok ? "self-verifies" : "review"}
        </span>
      </div>
      <p className="certificate-panel__lede">
        The receipt is self-contained: anyone holding your data and this certificate can re-check
        its trust invariants without re-running or trusting DataForge. This was re-verified
        server-side against your exact uploaded bytes.
      </p>
      <ul className="certificate-checks" aria-label="Certificate checks">
        {certificate.checks.map((check) => (
          <li
            className={`certificate-check certificate-check--${check.ok ? "ok" : "fail"}`}
            key={check.name}
          >
            {check.ok ? (
              <CheckCircle2 aria-hidden="true" />
            ) : (
              <AlertTriangle aria-hidden="true" />
            )}
            <div>
              <strong>{formatLabel(check.name)}</strong>
              <p>{check.detail}</p>
            </div>
          </li>
        ))}
      </ul>
      <div className="certificate-panel__actions">
        <button type="button" className="certificate-download" onClick={onDownload}>
          <Download aria-hidden="true" /> Download portable certificate
        </button>
        <p className="certificate-panel__reverify">
          {independentVerification === "agreed" ? (
            <span className="corroborated-chip">
              <BadgeCheck aria-hidden="true" /> Independently verified
            </span>
          ) : (
            <span className="single-verifier-note">Single verifier</span>
          )}{" "}
          Re-verify off this machine with <code>{auditCommand}</code>.
        </p>
      </div>
    </motion.section>
  );
}

function HeldForReviewList({ items }: { items: CandidateRepair[] }) {
  if (items.length === 0) {
    return null;
  }
  return (
    <section className="held-list" aria-labelledby="held-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Held for review</p>
          <h2 id="held-title">Proposals not proven safe to auto-apply</h2>
        </div>
        <span className="quiet-chip">{items.length} held</span>
      </div>
      <p className="held-list__lede">
        These were not written. Refusing to guess when a value cannot be proven from the data is a
        first-class, honest outcome — not a failure.
      </p>
      {items.map((item) => (
        <article
          className="held-row"
          key={`${item.row}:${item.column}:${item.new_value}:${item.review_reason ?? "held"}`}
        >
          <div className="held-row__head">
            <strong>
              Row {item.row}, <code>{item.column}</code>
            </strong>
            <VerificationStrengthBadge strength={strengthOf(item)} />
          </div>
          <span className="held-row__change">
            {item.old_value || "(empty)"} → {item.new_value || "(empty)"}
          </span>
          <p>{humanizeReviewReason(item.review_reason)}</p>
        </article>
      ))}
    </section>
  );
}

function agentTraceMotion(step: { action_type: string; accepted?: boolean | null }): string {
  const action = step.action_type.toUpperCase();
  if (action === "FIX") {
    return step.accepted === false ? "rejected" : "proven";
  }
  if (["FINALIZE", "DONE", "STOP", "FINISH", "COMPLETE"].includes(action)) {
    return "done";
  }
  if (["INSPECT_ROWS", "PATTERN_MATCH", "STAT_TEST", "HYPOTHESIS"].includes(action)) {
    return "verifying";
  }
  return "proposing";
}

function AgentSummaryPanel({ agent }: { agent: AgentSummary }) {
  return (
    <motion.section
      className="agent-summary"
      aria-label="Verified agent run"
      data-agent-motion="done"
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <header className="agent-summary__head">
        <div>
          <p className="eyebrow">Verified agent</p>
          <h3>{agent.policy_name}</h3>
        </div>
        <div className="agent-summary__metrics" role="group" aria-label="Agent run metrics">
          <Metric label="Steps" value={`${agent.steps_used}/${agent.max_steps}`} />
          <Metric label="Floor fixes" value={String(agent.floor_fix_count)} />
          <Metric label="Agent fixes" value={String(agent.agent_fix_count)} />
          <Metric label="Residual" value={String(agent.residual_count)} />
        </div>
      </header>
      <p className="agent-summary__reason">{agent.reason}</p>
      <p className="agent-summary__note">
        Agent proposals come from the {agent.policy_name} proposer and are each safety- and
        SMT-verified before display. Nothing is applied; this is a dry run, and a stronger
        proposer does not bypass the gate.
      </p>

      {agent.trace.length > 0 ? (
        <ol className="agent-trace" aria-label="Agent action trace">
          {agent.trace.map((step) => (
            <li
              key={step.step}
              className="agent-trace__step"
              data-agent-motion={agentTraceMotion(step)}
            >
              <span className="agent-trace__index">{step.step}</span>
              <span className="agent-trace__action">{step.action_type}</span>
              {step.accepted === true ? <span className="agent-trace__verdict agent-trace__verdict--ok">verified</span> : null}
              {step.accepted === false ? <span className="agent-trace__verdict agent-trace__verdict--rejected">rejected</span> : null}
              <span className="agent-trace__detail">{step.detail}</span>
            </li>
          ))}
        </ol>
      ) : null}

      {agent.agent_fixes.length > 0 ? (
        <div className="agent-summary__fixes">
          <h4>Agent-proposed verified fixes</h4>
          <ul>
            {agent.agent_fixes.map((fix) => (
              <li key={`${fix.row}:${fix.column}`}>
                <span className="agent-fix__cell">
                  row {fix.row} · {fix.column}
                </span>
                <span className="agent-fix__change">
                  {fix.old_value || "∅"} → {fix.new_value}
                </span>
                <VerificationStrengthBadge strength={strengthOf(fix)} />
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </motion.section>
  );
}

function ProblemBanner({ problem }: { problem: ProblemDetail }) {
  return (
    <motion.div
      className="problem-banner"
      role="alert"
      initial={{ opacity: 0, y: 3 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <AlertTriangle aria-hidden="true" />
      <div>
        <strong>{problem.title}</strong>
        <p>{problemToMessage(problem)}</p>
      </div>
    </motion.div>
  );
}

function LoadingState({ label }: { label: string }) {
  return (
    <motion.div
      className="loading-state"
      role="status"
      aria-live="polite"
      data-agent-motion="verifying"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: motionDurations.fast }}
    >
      <RefreshCw aria-hidden="true" />
      <span>{label}</span>
      <span className="loading-state__track" aria-hidden="true">
        <span className="loading-state__sweep df-motion-resolve" />
      </span>
    </motion.div>
  );
}

function EmptyState({ icon, title, body }: { icon: ReactNode; title: string; body: string }) {
  return (
    <motion.div
      className="empty-state"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: motionDurations.fast }}
    >
      {icon}
      <strong>{title}</strong>
      <p>{body}</p>
    </motion.div>
  );
}

function StageCounts({ counts }: { counts: Record<string, number | string | boolean> }) {
  const entries = Object.entries(counts);
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className="stage-counts">
      {entries.slice(0, 4).map(([key, value]) => (
        <span key={key}>
          {formatLabel(key)} {String(value)}
        </span>
      ))}
    </div>
  );
}

function filterAndSortIssues(
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

function formatConstraintColumns(candidate: ConstraintCandidate): string {
  const left = candidate.columns.join(", ");
  return candidate.dependent ? `${left} -> ${candidate.dependent}` : left;
}

function repairKey(fix: VerifiedFix): string {
  return `${fix.row}:${fix.column}:${fix.old_value}:${fix.new_value}`;
}

function downloadCertificate(analysis: {
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

function failureKey(failure: RepairFailure): string {
  return `${failure.row}:${failure.column}:${failure.issue_type}`;
}

function selectionFromReviewItem(item: ReviewItem): SelectedEvidence {
  if (item.kind === "constraint") {
    return { kind: "constraint", id: item.id };
  }
  if (item.kind === "failure") {
    return { kind: "failure", id: item.id };
  }
  return { kind: "receipt", id: item.id };
}

function toneClass(status: string): InstrumentTone {
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

function toneForSeverity(severity: Severity): InstrumentTone {
  if (severity === "unsafe") {
    return "danger";
  }
  if (severity === "review") {
    return "review";
  }
  return "verified";
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

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

export default App;
