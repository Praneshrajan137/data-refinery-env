/** The nine-stage workflow atlas and the constraint review queue beside it. */
import { StageCounts } from "../components/primitives";
import { motionDurations, motionSprings, panelVariants, stageNodeVariants, workflowStatusToAgentState } from "../motion";
import { formatLabel, formatPercent } from "../observatory";
import type { ReviewItem, SelectedEvidence } from "../observatory";
import type { AnalyzeResponse, ConstraintCandidate } from "../types";
import { formatConstraintColumns, selectionFromReviewItem, toneClass } from "../ui/helpers";
import type { WorkflowStageView } from "../workflow";
import { AlertTriangle, BrainCircuit, CheckCircle2, CircleDot, RefreshCw } from "lucide-react";
import { AnimatePresence, motion } from "motion/react";

export function ProofAtlas({
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

export function StageNode({
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

export function ReviewQueue({
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

export function ConstraintReviewControls({
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
