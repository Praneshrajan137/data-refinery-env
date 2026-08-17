import { EvidenceDock } from "../components/evidence";
import { RepairsLens } from "../lenses";
import type { SelectedEvidence } from "../observatory";
import type { ProductRouteId } from "../routes";
import { EmptyPagePrompt } from "../shell";
import type { WorkflowStageView } from "../workflow";
import type { AnalyzeResponse, DatasetInput, IssueGroup, ProblemDetail } from "../types";
import type { WorkState } from "../ui/helpers";

export function RepairsPage({
  state,
  analysis,
  dataset,
  selectedEvidence,
  issues,
  problem,
  stages,
  onSelect,
  onNavigate,
}: {
  state: WorkState;
  analysis: AnalyzeResponse | null;
  dataset: DatasetInput | null;
  selectedEvidence: SelectedEvidence | null;
  issues: IssueGroup[];
  problem: ProblemDetail | null;
  stages: WorkflowStageView[];
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page split-page">
      <section className="workbench-plane">
        <RepairsLens state={state} analysis={analysis} dataset={dataset} onSelect={onSelect} />
        {!analysis ? <EmptyPagePrompt title="Run analysis to unlock repairs" onNavigate={onNavigate} /> : null}
      </section>
      <EvidenceDock selectedEvidence={selectedEvidence} stages={stages} analysis={analysis} issues={issues} problem={problem} />
    </main>
  );
}
