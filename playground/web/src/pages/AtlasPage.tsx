import { ProofAtlas, ReviewQueue } from "../components/atlas";
import type { ReviewItem, SelectedEvidence } from "../observatory";
import type { ProductRouteId } from "../routes";
import { EmptyPagePrompt } from "../shell";
import type { AnalyzeResponse } from "../types";
import type { WorkflowStageView } from "../workflow";

export function AtlasPage({
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
