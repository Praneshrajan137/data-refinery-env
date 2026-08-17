import { EvidenceDock } from "../components/evidence";
import { ProblemBanner } from "../components/primitives";
import { OverviewLens, RiskLens } from "../lenses";
import { buildObservatoryView } from "../observatory";
import type { SelectedEvidence } from "../observatory";
import type { ProductRouteId } from "../routes";
import { EmptyPagePrompt } from "../shell";
import type { WorkflowStageView } from "../workflow";
import type { AnalyzeResponse, CsvPreview, DatasetInput, IssueGroup, ProblemDetail, Severity } from "../types";
import type { SortKey, WorkState } from "../ui/helpers";
import { ClaimDetail } from "../viz/ClaimDetail";
import { ConfidenceDistribution } from "../viz/ConfidenceDistribution";
import { DependencyGraph } from "../viz/DependencyGraph";
import { EvidenceOverview } from "../viz/EvidenceOverview";
import type { OverviewSelection } from "../viz/EvidenceOverview";
import { useState } from "react";

export function EvidencePage({
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
  stages,
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
  stages: WorkflowStageView[];
  selectedEvidence: SelectedEvidence | null;
  allIssues: IssueGroup[];
  onFilterChange: (value: string) => void;
  onSeverityFilterChange: (value: Severity | "all") => void;
  onSortChange: (value: SortKey) => void;
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  // Overview -> zoom -> details on demand. The selection lives here so the overview
  // stays stateless and the detail view is bounded by construction.
  const [zoom, setZoom] = useState<OverviewSelection | null>(null);
  return (
    <main className="route-page split-page">
      <section className="workbench-plane">
        {problem ? <ProblemBanner problem={problem} /> : null}
        <OverviewLens dataset={dataset} preview={preview} analysis={analysis} observatory={observatory} onSelect={onSelect} />
        <EvidenceOverview analysis={analysis} onZoom={setZoom} />
        <ClaimDetail analysis={analysis} selection={zoom} onClear={() => setZoom(null)} />
        <ConfidenceDistribution analysis={analysis} />
        <DependencyGraph analysis={analysis} />
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
      {/* stages must be the real workflow stages. This mount, and the one on /repairs, passed
          an empty array, while resolveDockContent looks a stage up in it -- and a stage
          selection is exactly what the evidence tiles in OverviewLens emit. Clicking an
          evidence tile therefore resolved to nothing and the dock silently fell through to
          generic content, on the two routes whose whole purpose is inspecting evidence. */}
      <EvidenceDock selectedEvidence={selectedEvidence} stages={stages} analysis={analysis} issues={allIssues} problem={problem} />
    </main>
  );
}
