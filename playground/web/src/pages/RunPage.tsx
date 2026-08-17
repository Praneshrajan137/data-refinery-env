import { AgentSummaryPanel } from "../components/agent";
import { MissionBar, ProductLoopRail, ProductLoopWorkbench } from "../components/mission";
import { CopyFallback, ProblemBanner } from "../components/primitives";
import { TrustVerdictPanel } from "../components/trust";
import { RiskSummaryPanel } from "../components/evidence";
import { OverviewLens } from "../lenses";
import { buildObservatoryView, buildTrustVerdict } from "../observatory";
import type { SelectedEvidence } from "../observatory";
import type { PrimaryRepairMoment } from "../productLoop";
import type { ProductRouteId } from "../routes";
import type { AnalyzeResponse, BackendCapability, DatasetInput, ProblemDetail, RepairMode } from "../types";
import type { WorkState } from "../ui/helpers";
import type { ChangeEvent, RefObject } from "react";

export function RunPage({
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
  evidenceText,
  copyState,
  fileInputRef,
  problem,
  staleAnalysis,
  latestAnalysis,
  primaryMoment,
  observatory,
  onAdvancedChange,
  onRepairModeChange,
  onEntityConsensusChange,
  onChooseSample,
  onFileChange,
  onAnalyze,
  onRerun,
  onCancel,
  onRetry,
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
  allowEntityConsensus: boolean;
  backendState: WorkState;
  streamingEnabled: boolean;
  acceptedConstraintIds: string[];
  analysisState: WorkState;
  hasEvidence: boolean;
  evidenceText: string;
  copyState: "idle" | "copied" | "failed";
  fileInputRef: RefObject<HTMLInputElement | null>;
  problem: ProblemDetail | null;
  staleAnalysis: boolean;
  latestAnalysis: AnalyzeResponse | null;
  primaryMoment: PrimaryRepairMoment | null;
  observatory: ReturnType<typeof buildObservatoryView>;
  onAdvancedChange: (next: boolean) => void;
  onRepairModeChange: (next: RepairMode) => void;
  onEntityConsensusChange: (next: boolean) => void;
  onChooseSample: (sampleName: string) => void | Promise<void>;
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void | Promise<void>;
  onAnalyze: () => void;
  onRerun: () => void;
  onCancel: () => void;
  onRetry: () => void;
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
        allowEntityConsensus={allowEntityConsensus}
        backendState={backendState}
        streamingEnabled={streamingEnabled}
        acceptedConstraintIds={acceptedConstraintIds}
        analysisState={analysisState}
        hasEvidence={hasEvidence}
        copyState={copyState}
        fileInputRef={fileInputRef}
        onAdvancedChange={onAdvancedChange}
        onRepairModeChange={onRepairModeChange}
        onEntityConsensusChange={onEntityConsensusChange}
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
      {problem ? <ProblemBanner problem={problem} onRetry={canRun ? onRetry : undefined} /> : null}
      {latestAnalysis ? (
        <TrustVerdictPanel verdict={buildTrustVerdict(latestAnalysis)} stale={staleAnalysis} />
      ) : null}
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
      {/* Composed here rather than inside OverviewLens: this page has no RiskLens, so it owns
          the risk summary. /evidence does have one, which is why the lens no longer carries it. */}
      {latestAnalysis ? (
        <RiskSummaryPanel
          datasetLevel={latestAnalysis.risk_summary.dataset_level}
          readiness={latestAnalysis.risk_summary.repair_readiness}
          reasons={latestAnalysis.risk_summary.reasons}
          label="Run risk overview"
        />
      ) : null}
      <section className="route-actions" aria-label="Next pages">
        <button type="button" onClick={() => onNavigate("atlas")}>Open proof details</button>
        <button type="button" onClick={() => onNavigate("evidence")}>Open Evidence</button>
      </section>
    </main>
  );
}
