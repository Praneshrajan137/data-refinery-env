import type { Transition, Variants } from "motion/react";
import type { WorkflowEvent, WorkflowStatus } from "./types";
import motionTokens from "./design/motion-tokens.json";

export type MotionIntensity = "standard" | "reduced";

// Legible, event-backed agent states. Each maps to a REAL pipeline/trace event
// and renders live (see agentStatePrimitive). The prior 12-state palette had 10
// states that were only ever drawn in a legend -- words nobody spoke -- and is
// retired here. See docs/design/perceptual-language.md section 4.1.
export type AgentMotionState =
  | "verifying"
  | "proposing"
  | "proven"
  | "held"
  | "rejected"
  | "asking"
  | "done"
  | "idle";

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

// --- Single source of truth -------------------------------------------------
// Durations, easings and springs are DERIVED from src/design/motion-tokens.json
// (milliseconds) so the TS (seconds) and CSS (ms) systems can never drift.
// generate_motion_system.mjs emits the matching CSS custom properties.
const ms = motionTokens.durationsMs;

export const motionDurations = {
  instant: ms.instant / 1000,
  micro: ms.micro / 1000,
  fast: ms.fast / 1000,
  standard: ms.standard / 1000,
  measured: ms.measured / 1000,
  page: ms.page / 1000,
  max: ms.max / 1000,
} as const;

const easingTuple = (name: keyof typeof motionTokens.easings): [number, number, number, number] => {
  const value = motionTokens.easings[name];
  return [value[0], value[1], value[2], value[3]];
};

export const motionEasings = {
  standard: easingTuple("standard"),
  emphasized: easingTuple("emphasized"),
  exit: easingTuple("exit"),
  linear: "linear",
} as const;

export const motionSprings = {
  soft: { type: "spring", ...motionTokens.springs.soft },
  snap: { type: "spring", ...motionTokens.springs.snap },
  layout: { type: "spring", ...motionTokens.springs.layout },
} as const satisfies Record<string, Transition>;

// --- Motion primitive grammar (earned salience) -----------------------------
// Each primitive answers one real causal question. See docs/design/perceptual-language.md.
export type MotionPrimitive =
  | "settle"
  | "hover"
  | "resolve"
  | "pause"
  | "recoil"
  | "still"
  | "downgrade";

export interface MotionPrimitiveSpec {
  rung: string;
  duration: string;
  easing: string;
  loop: boolean;
  answers: string;
}

export const motionPrimitives: Record<MotionPrimitive, MotionPrimitiveSpec> =
  motionTokens.primitives as Record<MotionPrimitive, MotionPrimitiveSpec>;

export const primitiveVariants: Record<MotionPrimitive, Variants> = {
  // proven and committed: converges decisively to rest
  settle: {
    initial: { opacity: 0.55, scale: 0.994 },
    animate: {
      opacity: 1,
      scale: 1,
      transition: { duration: motionDurations.measured, ease: motionEasings.emphasized },
    },
  },
  // uncommitted proposal: low amplitude, never resolves to rest
  hover: {
    initial: { opacity: 0.9, y: 0 },
    animate: {
      opacity: [0.9, 1, 0.9],
      y: [0, -1.5, 0],
      transition: { duration: motionDurations.max, ease: motionEasings.standard, repeat: Infinity },
    },
  },
  // verification in progress: determinate, honest, only while working
  resolve: {
    initial: { opacity: 0.5 },
    animate: {
      opacity: [0.5, 1, 0.5],
      transition: { duration: motionDurations.measured, ease: motionEasings.standard, repeat: Infinity },
    },
  },
  // held / abstained: motion arrested and held
  pause: {
    initial: { opacity: 1, y: -1 },
    animate: {
      opacity: 0.92,
      y: 0,
      transition: { duration: motionDurations.fast, ease: motionEasings.standard },
    },
  },
  // rejected / failed: one decisive counter-motion, no loop
  recoil: {
    initial: { x: 0 },
    animate: {
      x: [0, -3, 1, 0],
      transition: { duration: motionDurations.fast, ease: motionEasings.exit },
    },
  },
  // idle: no motion at all
  still: {
    initial: { opacity: 1 },
    animate: { opacity: 1 },
  },
  // drift relaxed a proof back to review
  downgrade: {
    initial: { opacity: 1 },
    animate: {
      opacity: [1, 0.68, 1],
      transition: { duration: motionDurations.page, ease: motionEasings.standard },
    },
  },
};

// Reduced-motion twins preserve MEANING, never merely delete movement, and never
// upgrade a rung: they collapse to an opacity-only static state.
export const reducedPrimitiveVariants: Record<MotionPrimitive, Variants> = {
  settle: { initial: { opacity: 0 }, animate: { opacity: 1, transition: { duration: motionDurations.instant } } },
  hover: { initial: { opacity: 0.92 }, animate: { opacity: 0.92 } },
  resolve: { initial: { opacity: 0.85 }, animate: { opacity: 0.85 } },
  pause: { initial: { opacity: 0.92 }, animate: { opacity: 0.92 } },
  recoil: { initial: { opacity: 1 }, animate: { opacity: 1 } },
  still: { initial: { opacity: 1 }, animate: { opacity: 1 } },
  downgrade: { initial: { opacity: 1 }, animate: { opacity: 1 } },
};

export function primitiveVariantsForIntensity(
  primitive: MotionPrimitive,
  intensity: MotionIntensity,
): Variants {
  return intensity === "reduced" ? reducedPrimitiveVariants[primitive] : primitiveVariants[primitive];
}

export const agentMotionStates: Record<AgentMotionState, WorkflowMotionEvent> = {
  verifying: { agentState: "verifying", preset: "stage", isLooping: true, attention: "low" },
  proposing: { agentState: "proposing", preset: "stage", isLooping: true, attention: "medium" },
  proven: { agentState: "proven", preset: "stage", isLooping: false, attention: "low" },
  held: { agentState: "held", preset: "review", isLooping: false, attention: "medium" },
  rejected: { agentState: "rejected", preset: "stage", isLooping: false, attention: "high" },
  asking: { agentState: "asking", preset: "review", isLooping: false, attention: "medium" },
  done: { agentState: "done", preset: "receipt", isLooping: false, attention: "low" },
  idle: { agentState: "idle", preset: "static", isLooping: false, attention: "none" },
};

// Each agent state's motion primitive -- its honest sentence's verb. Active work
// (verifying/proposing) loops; everything else is one-shot or still. Rendered off
// this so no state is legend-only.
export const agentStatePrimitive: Record<AgentMotionState, MotionPrimitive> = {
  verifying: "resolve",
  proposing: "hover",
  proven: "settle",
  held: "pause",
  rejected: "recoil",
  asking: "pause",
  done: "settle",
  idle: "still",
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
    return "verifying";
  }
  if (status === "completed") {
    return "done";
  }
  if (status === "blocked") {
    return "asking";
  }
  if (status === "failed") {
    return "rejected";
  }
  if (status === "cancelled") {
    return "held";
  }
  return "idle";
}

export function workflowEventToMotion(event: WorkflowEvent): WorkflowMotionEvent {
  const agentState = workflowStatusToAgentState(event.status);
  if (event.stage_id === "schema_inference" && event.status === "running") {
    return agentMotionStates.verifying;
  }
  if (event.stage_id === "smt_verifier" && event.status === "completed") {
    return agentMotionStates.proven;
  }
  if (event.stage_id === "dry_run_transaction" && event.status === "completed") {
    return agentMotionStates.proven;
  }
  if (event.stage_id === "receipt" && event.status === "completed") {
    return agentMotionStates.done;
  }
  if (event.requires_human && event.status === "completed") {
    return agentMotionStates.asking;
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
