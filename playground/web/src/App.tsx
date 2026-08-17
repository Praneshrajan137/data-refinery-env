/**
 * Application root.
 *
 * Owns all run state and the hand-rolled History router, and nothing else: every page,
 * lens and panel now lives in its own module. This file was 3,737 lines with 70 top-level
 * components in it, verified only through three Playwright specs.
 */
import { DataForgeClient } from "./api";
import { getRuntimeConfig } from "./config";
import { DEFAULT_MAX_UPLOAD_BYTES, buildEvidenceExport, groupIssues, parseCsvPreview, validateCsvFile } from "./csv";
import { variantsForIntensity } from "./motion";
import type { MotionIntensity } from "./motion";
import { buildObservatoryView } from "./observatory";
import type { SelectedEvidence } from "./observatory";
import { AtlasPage } from "./pages/AtlasPage";
import { EvidencePage } from "./pages/EvidencePage";
import { GuardrailPage } from "./pages/GuardrailPage";
import { ReceiptPage } from "./pages/ReceiptPage";
import { RepairsPage } from "./pages/RepairsPage";
import { RunPage } from "./pages/RunPage";
import { SystemPage } from "./pages/SystemPage";
import { selectPrimaryRepairMoment } from "./productLoop";
import { hrefWithRunQuery, isKnownRoutePath, parseRunQuery, routeById, routeFromPathname } from "./routes";
import type { ProductRoute, ProductRouteId, RunQueryState } from "./routes";
import { OfflineBanner } from "./components/primitives";
import { ProductPageHeader, ProductShell, UnknownRoute } from "./shell";
import type { AnalyzeResponse, BackendCapability, DatasetInput, ProblemDetail, RepairMode, Severity, WorkflowEvent } from "./types";
import { filterAndSortIssues, isAbortError, localProblem, problemFromUnknown, sleep } from "./ui/helpers";
import type { SortKey, WorkState } from "./ui/helpers";
import { createWorkflowState, workflowReducer } from "./workflow";
import { AnimatePresence, LayoutGroup, motion, useReducedMotion } from "motion/react";
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import type { ChangeEvent } from "react";

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
  const [allowEntityConsensus, setAllowEntityConsensus] = useState(false);
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
  // Connectivity was previously undetectable here: nothing in the app read navigator.onLine
  // or listened for online/offline, so losing the network produced a bare "Failed to fetch"
  // rendered under a CSV-validation title. Knowing we are offline lets the UI say so BEFORE a
  // request is attempted, and stops it blaming the backend for the device's problem.
  const [online, setOnline] = useState(() =>
    typeof navigator === "undefined" ? true : navigator.onLine !== false,
  );

  const maxUploadBytes = capability?.max_upload_bytes ?? DEFAULT_MAX_UPLOAD_BYTES;
  const streamingEnabled =
    capability?.streaming_available === true &&
    capability.workflow_contract_version === "workflow_event_v1";
  const busy = datasetState === "loading" || analysisState === "loading";
  // Offline is a precondition, not an error to discover after the fact: a run started with no
  // network can only fail, and failing loudly after a 20s wait is worse than not starting.
  const canRun = backendState === "ready" && dataset !== null && !busy && online;
  const latestAnalysis = workflow.lastAnalysis ?? analysis;
  const evidenceText = useMemo(
    () => (latestAnalysis && dataset ? buildEvidenceExport(dataset.file.name, latestAnalysis, workflow.staleAnalysis) : ""),
    [latestAnalysis, dataset, workflow.staleAnalysis],
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

  /**
   * Re-probes the backend without discarding anything.
   *
   * The "Backend unavailable" chip used to be wired to window.location.reload(). Reloading
   * this app destroys the run: the File handle, the parsed preview, the analysis, the receipt
   * and the selected evidence all live in component state and nothing is persisted, so the
   * recovery for a sleeping backend was to throw away the user's completed work. Since free
   * hosting sleeps when idle, that was the ordinary case rather than the rare one.
   *
   * `probeBackend` is a ref-stable callback used both by the mount effect and the chip.
   */
  const probeBackend = useCallback(
    async (isCancelled: () => boolean = () => false) => {
      setBackendState("loading");
      for (let attempt = 0; attempt < 6; attempt += 1) {
        try {
          const health = await client.health();
          if (!isCancelled()) {
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
      if (!isCancelled()) {
        setBackendState("error");
        setCapability(null);
      }
    },
    [client],
  );

  useEffect(() => {
    let cancelled = false;
    void probeBackend(() => cancelled);
    return () => {
      cancelled = true;
      abortControllerRef.current?.abort();
    };
  }, [probeBackend]);

  // Tracked separately from `route`, because routeFromPathname deliberately keeps falling back
  // to the run page; this only records whether the address the user arrived at was real.
  const [unknownPath, setUnknownPath] = useState<string | null>(() =>
    isKnownRoutePath(window.location.pathname) ? null : window.location.pathname,
  );

  useEffect(() => {
    function handlePopState() {
      setRoute(routeFromPathname(window.location.pathname));
      setUnknownPath(isKnownRoutePath(window.location.pathname) ? null : window.location.pathname);
    }
    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  // Connectivity was previously undetectable here: nothing in the app read navigator.onLine
  // or listened for online/offline, so losing the network produced a bare "Failed to fetch"
  // rendered under a CSV-validation title. Knowing we are offline lets the UI say so BEFORE a
  // request is attempted, and stops it blaming the backend for the device's problem.
  useEffect(() => {
    const goOnline = () => setOnline(true);
    const goOffline = () => setOnline(false);
    window.addEventListener("online", goOnline);
    window.addEventListener("offline", goOffline);
    return () => {
      window.removeEventListener("online", goOnline);
      window.removeEventListener("offline", goOffline);
    };
  }, []);

  /**
   * Keeps the URL describing the current run, so it can be shared and reloaded.
   *
   * Navigation preserves the run query rather than dropping it, which is what makes a link to
   * /playground/receipt land on an actual receipt instead of the empty prompt.
   */
  const runQuery = useMemo<RunQueryState>(
    () => ({
      sample: dataset?.source === "sample" ? dataset.sampleName : undefined,
      advanced: advanced || undefined,
      agent: repairMode === "agent" || undefined,
      consensus: allowEntityConsensus || undefined,
      constraints: acceptedConstraintIds.length > 0 ? acceptedConstraintIds : undefined,
    }),
    [acceptedConstraintIds, advanced, allowEntityConsensus, dataset, repairMode],
  );

  function navigate(nextRouteId: ProductRouteId) {
    const nextRoute = routeById(nextRouteId);
    const href = hrefWithRunQuery(nextRoute, runQuery);
    if (href !== `${window.location.pathname}${window.location.search}`) {
      window.history.pushState({}, "", href);
    }
    setRoute(nextRoute);
    setUnknownPath(null);
  }

  /**
   * The run state the page was OPENED with, captured during first render.
   *
   * Captured here, synchronously, rather than read inside the hydration effect, because the
   * URL-sync effect below runs first and would already have erased it: on mount there is no
   * dataset, so `runQuery` is empty, so the sync effect replaceState'd the query off the URL
   * before hydration ever looked. Two effects fighting over one URL -- the dataset silently
   * never loaded, and no unit test could see it.
   */
  const incomingQuery = useRef<RunQueryState>(parseRunQuery(window.location.search));

  /**
   * URL sync is disabled until the incoming link has been consumed, so that writing the
   * current state to the address bar cannot destroy the state we are still reading from it.
   */
  const [urlSyncEnabled, setUrlSyncEnabled] = useState(false);

  // Keeps the address bar current as toggles and accepted constraints change, using
  // replaceState so tweaking a switch does not fill the back button with history entries.
  useEffect(() => {
    if (!urlSyncEnabled) {
      return;
    }
    const href = hrefWithRunQuery(route, runQuery);
    if (href !== `${window.location.pathname}${window.location.search}`) {
      window.history.replaceState({}, "", href);
    }
  }, [route, runQuery, urlSyncEnabled]);

  const [shareState, setShareState] = useState<"idle" | "copied" | "failed">("idle");

  /**
   * A shared link is only offered when the run is actually reproducible from it: a sample
   * dataset. An uploaded CSV stays in the browser, so a link naming it would open to an empty
   * prompt for whoever received it, which is a worse outcome than offering no link at all.
   */
  const shareHref = useMemo(() => {
    if (dataset?.source !== "sample" || dataset.sampleName === undefined) {
      return null;
    }
    return `${window.location.origin}${hrefWithRunQuery(route, runQuery)}`;
  }, [dataset, route, runQuery]);

  async function copyShareLink() {
    if (!shareHref) {
      return;
    }
    try {
      await navigator.clipboard.writeText(shareHref);
      setShareState("copied");
    } catch {
      setShareState("failed");
    }
  }

  async function adoptFile(
    file: File,
    source: DatasetInput["source"],
    sampleName?: string,
    // A shared link naming both a sample and a route must land on the route it names.
    // Loading a dataset otherwise always jumps to /run, which would make
    // /playground/receipt?sample=... silently redirect away from the receipt.
    goToRun = true,
  ) {
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
      if (goToRun) {
        navigate("run");
      }
      setSelectedEvidence(null);
      dispatchWorkflow({ type: "reset" });
    } catch (error) {
      setDatasetState("error");
      setProblem(localProblem(error instanceof Error ? error.message : "The CSV preview failed."));
    }
  }

  async function chooseSample(sampleName: string, goToRun = true) {
    if (!sampleName || busy) {
      return;
    }
    try {
      const file = await client.sample(sampleName);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      await adoptFile(file, "sample", sampleName, goToRun);
    } catch (error) {
      setDatasetState("error");
      setProblem(problemFromUnknown(error));
    }
  }

  /**
   * Hydrates a shared link once the backend is reachable.
   *
   * Samples are fetched from the backend, so this cannot run at mount. It runs exactly once,
   * guarded by a ref rather than by state, so a later health re-probe does not re-apply the
   * original URL over whatever the user has since chosen.
   *
   * It does NOT call runAnalyze directly. `runAnalyze` reads `dataset` from its closure, and
   * immediately after `chooseSample` resolves that closure still sees `null`, so an inline call
   * bails silently -- which is precisely how the previous version of this feature came to load
   * a dataset and never analyse while a comment claimed otherwise. The run is therefore
   * requested as state and fired by the effect below, once React reports the dataset ready.
   */
  const sharedLinkApplied = useRef(false);
  const [pendingSharedRun, setPendingSharedRun] = useState<string[] | null>(null);
  useEffect(() => {
    if (backendState !== "ready" || sharedLinkApplied.current) {
      return;
    }
    sharedLinkApplied.current = true;
    const shared = incomingQuery.current;

    // Capability still governs: a link cannot switch on a mode this backend does not offer.
    if (shared.advanced && capability?.advanced_available) {
      setAdvanced(true);
    }
    if (shared.agent && capability?.agent_available) {
      setRepairMode("agent");
    }
    if (shared.consensus && capability?.entity_consensus_available) {
      setAllowEntityConsensus(true);
    }

    if (shared.sample === undefined) {
      if (shared.constraints !== undefined) {
        setAcceptedConstraintIds(shared.constraints);
      }
      setUrlSyncEnabled(true);
      return;
    }
    void (async () => {
      await chooseSample(shared.sample as string, false);
      // Applied after the dataset, because adopting one clears the accepted set.
      if (shared.constraints !== undefined) {
        setAcceptedConstraintIds(shared.constraints);
      }
      setPendingSharedRun(shared.constraints ?? []);
      setUrlSyncEnabled(true);
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- runs once, on first readiness.
  }, [backendState, capability]);

  // If the backend never comes up there is nothing to hydrate, so release the URL sync anyway
  // rather than leaving the address bar frozen for the rest of the session.
  useEffect(() => {
    if (backendState === "error") {
      setUrlSyncEnabled(true);
    }
  }, [backendState]);
  /**
   * Fires the analysis a shared link asked for, once the dataset it named is actually loaded.
   *
   * Restricted to sample datasets by construction: `pendingSharedRun` is only ever set by the
   * hydration path above, which requires `shared.sample`. A link cannot cause an upload, and it
   * cannot cause an analysis of anything the backend does not already host. The backend
   * rate-limits, so the exposure of a link that triggers one sample analysis is bounded.
   */
  useEffect(() => {
    if (pendingSharedRun === null || !canRun) {
      return;
    }
    const ids = pendingSharedRun;
    setPendingSharedRun(null);
    void runAnalyze(ids);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- runAnalyze is re-created per
    // render; depending on it would re-fire the run.
  }, [pendingSharedRun, canRun]);

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
            allowEntityConsensus,
          )
        : await client.analyze(
            dataset.file,
            advanced,
            ids,
            repairMode,
            allowEntityConsensus,
            controller.signal,
          );

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
      {/* The hero describes the CURRENT page, so it is suppressed at an unknown address --
          otherwise the header announced "CSV repair loop" and its description while the body
          said no such page exists, which is the header contradicting the content. */}
      {unknownPath === null ? (
        <ProductPageHeader route={route} dataset={dataset} analysis={latestAnalysis} workflowStatus={workflow.status} stale={workflow.staleAnalysis} />
      ) : null}
      {online ? null : <OfflineBanner />}
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
            {unknownPath !== null ? <UnknownRoute pathname={unknownPath} onNavigate={navigate} /> : null}
            {unknownPath === null && route.id === "run" ? (
              <RunPage
                dataset={dataset}
                busy={busy}
                canRun={canRun}
                maxUploadBytes={maxUploadBytes}
                capability={capability}
                advanced={advanced}
                repairMode={repairMode}
                allowEntityConsensus={allowEntityConsensus}
                backendState={backendState}
                streamingEnabled={streamingEnabled}
                acceptedConstraintIds={acceptedConstraintIds}
                analysisState={analysisState}
                hasEvidence={Boolean(evidenceText)}
                evidenceText={evidenceText}
                copyState={copyState}
                fileInputRef={fileInputRef}
                problem={problem}
                staleAnalysis={workflow.staleAnalysis}
                latestAnalysis={latestAnalysis}
                primaryMoment={primaryMoment}
                observatory={observatory}
                onAdvancedChange={setAdvanced}
                onRepairModeChange={setRepairMode}
                onEntityConsensusChange={setAllowEntityConsensus}
                onChooseSample={chooseSample}
                onFileChange={handleFileChange}
                onAnalyze={() => void runAnalyze([])}
                onRerun={() => void runAnalyze(acceptedConstraintIds)}
                onCancel={cancelAnalyze}
                onRetry={() => void runAnalyze(acceptedConstraintIds)}
                onCopy={() => void copyEvidence()}
                onExport={exportEvidence}
                onBackendRetry={() => void probeBackend()}
                onNavigate={navigate}
                onSelect={setSelectedEvidence}
              />
            ) : null}
            {unknownPath === null && route.id === "atlas" ? (
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
            {unknownPath === null && route.id === "evidence" ? (
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
                stages={workflow.stages}
                selectedEvidence={selectedEvidence}
                allIssues={groupedIssues}
                onFilterChange={setFilter}
                onSeverityFilterChange={setSeverityFilter}
                onSortChange={setSortKey}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {unknownPath === null && route.id === "repairs" ? (
              <RepairsPage
                state={analysisState}
                analysis={latestAnalysis}
                dataset={dataset}
                selectedEvidence={selectedEvidence}
                stages={workflow.stages}
                issues={groupedIssues}
                problem={problem}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {unknownPath === null && route.id === "guardrail" ? (
              <GuardrailPage
                client={client}
                capability={capability}
                backendState={backendState}
                maxUploadBytes={maxUploadBytes}
                onBackendRetry={() => void probeBackend()}
                onNavigate={navigate}
              />
            ) : null}
            {unknownPath === null && route.id === "receipt" ? (
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
                shareHref={shareHref}
                shareState={shareState}
                onShare={() => void copyShareLink()}
                onSelect={setSelectedEvidence}
                onNavigate={navigate}
              />
            ) : null}
            {unknownPath === null && route.id === "system" ? (
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

export default App;
