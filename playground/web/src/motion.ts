import type { Transition, Variants } from "motion/react";
import type { WorkflowEvent, WorkflowStatus } from "./types";

export type MotionIntensity = "standard" | "reduced";

export type AgentMotionState =
  | "thinking"
  | "acting"
  | "waiting"
  | "asking"
  | "uncertain"
  | "confident"
  | "completed"
  | "failed"
  | "interrupted"
  | "delegated"
  | "escalated"
  | "recovered";

export type MotionPreset =
  | "route"
  | "panel"
  | "stage"
  | "dock"
  | "review"
  | "repair"
  | "receipt"
  | "static";

export interface WorkflowMotionEvent {
  agentState: AgentMotionState;
  preset: MotionPreset;
  isLooping: boolean;
  attention: "none" | "low" | "medium" | "high";
}

export const motionDurations = {
  instant: 0.08,
  micro: 0.12,
  fast: 0.18,
  standard: 0.24,
  measured: 0.32,
  page: 0.36,
  max: 0.48,
} as const;

export const motionEasings = {
  standard: [0.2, 0, 0, 1],
  emphasized: [0.16, 1, 0.3, 1],
  exit: [0.4, 0, 1, 1],
  linear: "linear",
} as const;

export const motionSprings = {
  soft: { type: "spring", stiffness: 420, damping: 36, mass: 0.85 },
  snap: { type: "spring", stiffness: 620, damping: 42, mass: 0.7 },
  layout: { type: "spring", stiffness: 500, damping: 45, mass: 1 },
} as const satisfies Record<string, Transition>;

export const agentMotionStates: Record<AgentMotionState, WorkflowMotionEvent> = {
  thinking: { agentState: "thinking", preset: "stage", isLooping: true, attention: "low" },
  acting: { agentState: "acting", preset: "stage", isLooping: true, attention: "medium" },
  waiting: { agentState: "waiting", preset: "static", isLooping: false, attention: "none" },
  asking: { agentState: "asking", preset: "review", isLooping: false, attention: "medium" },
  uncertain: { agentState: "uncertain", preset: "review", isLooping: false, attention: "medium" },
  confident: { agentState: "confident", preset: "stage", isLooping: false, attention: "low" },
  completed: { agentState: "completed", preset: "stage", isLooping: false, attention: "low" },
  failed: { agentState: "failed", preset: "stage", isLooping: false, attention: "high" },
  interrupted: { agentState: "interrupted", preset: "review", isLooping: false, attention: "medium" },
  delegated: { agentState: "delegated", preset: "receipt", isLooping: false, attention: "low" },
  escalated: { agentState: "escalated", preset: "stage", isLooping: false, attention: "high" },
  recovered: { agentState: "recovered", preset: "stage", isLooping: false, attention: "low" },
};

export const routeVariants: Variants = {
  initial: { opacity: 0, y: 6 },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: motionDurations.page, ease: motionEasings.emphasized },
  },
  exit: {
    opacity: 0,
    y: -4,
    transition: { duration: motionDurations.fast, ease: motionEasings.exit },
  },
};

export const panelVariants: Variants = {
  initial: { opacity: 0, y: 4 },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: motionDurations.standard, ease: motionEasings.standard },
  },
  exit: {
    opacity: 0,
    y: 2,
    transition: { duration: motionDurations.micro, ease: motionEasings.exit },
  },
};

export const stageNodeVariants: Variants = {
  queued: { opacity: 0.88, scale: 1 },
  running: {
    opacity: 1,
    scale: [1, 1.012, 1],
    transition: { duration: motionDurations.max, ease: motionEasings.standard },
  },
  completed: {
    opacity: 1,
    scale: [1, 1.008, 1],
    transition: { duration: motionDurations.measured, ease: motionEasings.emphasized },
  },
  blocked: {
    opacity: 1,
    scale: 1,
    transition: { duration: motionDurations.fast, ease: motionEasings.standard },
  },
  failed: {
    opacity: 1,
    scale: [1, 0.992, 1],
    transition: motionSprings.snap,
  },
  cancelled: {
    opacity: 0.94,
    scale: 1,
    transition: { duration: motionDurations.fast, ease: motionEasings.standard },
  },
};

export const reducedRouteVariants: Variants = {
  initial: { opacity: 0 },
  animate: { opacity: 1, transition: { duration: motionDurations.instant } },
  exit: { opacity: 0, transition: { duration: motionDurations.instant } },
};

export const reducedPanelVariants: Variants = reducedRouteVariants;

export function variantsForIntensity(
  preset: Extract<MotionPreset, "route" | "panel">,
  intensity: MotionIntensity,
): Variants {
  if (intensity === "reduced") {
    return preset === "route" ? reducedRouteVariants : reducedPanelVariants;
  }
  return preset === "route" ? routeVariants : panelVariants;
}

export function workflowStatusToAgentState(status: WorkflowStatus): AgentMotionState {
  if (status === "running") {
    return "acting";
  }
  if (status === "completed") {
    return "completed";
  }
  if (status === "blocked") {
    return "asking";
  }
  if (status === "failed") {
    return "failed";
  }
  if (status === "cancelled") {
    return "interrupted";
  }
  return "waiting";
}

export function workflowEventToMotion(event: WorkflowEvent): WorkflowMotionEvent {
  const agentState = workflowStatusToAgentState(event.status);
  if (event.stage_id === "schema_inference" && event.status === "running") {
    return agentMotionStates.thinking;
  }
  if (event.stage_id === "smt_verifier" && event.status === "completed") {
    return agentMotionStates.confident;
  }
  if (event.stage_id === "dry_run_transaction" && event.status === "completed") {
    return agentMotionStates.delegated;
  }
  if (event.stage_id === "receipt" && event.status === "completed") {
    return agentMotionStates.recovered;
  }
  if (event.requires_human && event.status === "completed") {
    return agentMotionStates.uncertain;
  }
  return agentMotionStates[agentState];
}

export function reducedVariantHasNoSpatialTransform(variant: Variants): boolean {
  return Object.values(variant).every((value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return true;
    }
    return !("x" in value) && !("y" in value) && !("scale" in value);
  });
}
