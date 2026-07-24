import { describe, expect, it } from "vitest";
import {
  agentMotionStates,
  motionDurations,
  motionEasings,
  reducedRouteVariants,
  reducedVariantHasNoSpatialTransform,
  variantsForIntensity,
  workflowEventToMotion,
  workflowStatusToAgentState,
} from "./motion";
import type { WorkflowEvent } from "./types";

const agentStates = [
  "verifying",
  "proposing",
  "proven",
  "held",
  "rejected",
  "asking",
  "done",
  "idle",
] as const;

function event(stage_id: WorkflowEvent["stage_id"], status: WorkflowEvent["status"], requires_human = false): WorkflowEvent {
  return {
    schema_version: "workflow_event_v1",
    run_id: "run-test",
    sequence: 1,
    stage_id,
    status,
    summary: "test event",
    counts: {},
    requires_human,
  };
}

describe("motion system", () => {
  it("exports the locked temporal vocabulary", () => {
    expect(motionDurations).toMatchObject({
      instant: 0.08,
      micro: 0.12,
      fast: 0.18,
      standard: 0.24,
      measured: 0.32,
      page: 0.36,
      max: 0.48,
    });
    expect(motionEasings.standard).toEqual([0.2, 0, 0, 1]);
    expect(motionEasings.emphasized).toEqual([0.16, 1, 0.3, 1]);
    expect(motionEasings.exit).toEqual([0.4, 0, 1, 1]);
  });

  it("covers every agent-state motion contract", () => {
    for (const state of agentStates) {
      expect(agentMotionStates[state]).toMatchObject({ agentState: state });
      expect(agentMotionStates[state].attention).toMatch(/none|low|medium|high/);
    }
  });

  it("maps workflow status to visible agent states", () => {
    expect(workflowStatusToAgentState("queued")).toBe("idle");
    expect(workflowStatusToAgentState("running")).toBe("verifying");
    expect(workflowStatusToAgentState("completed")).toBe("done");
    expect(workflowStatusToAgentState("blocked")).toBe("asking");
    expect(workflowStatusToAgentState("failed")).toBe("rejected");
    expect(workflowStatusToAgentState("cancelled")).toBe("held");
  });

  it("derives specific motion states from workflow events", () => {
    expect(workflowEventToMotion(event("schema_inference", "running")).agentState).toBe("verifying");
    expect(workflowEventToMotion(event("constraint_review", "completed", true)).agentState).toBe("asking");
    expect(workflowEventToMotion(event("smt_verifier", "completed")).agentState).toBe("proven");
    expect(workflowEventToMotion(event("dry_run_transaction", "completed")).agentState).toBe("proven");
    expect(workflowEventToMotion(event("receipt", "completed")).agentState).toBe("done");
    expect(workflowEventToMotion(event("receipt", "failed")).agentState).toBe("rejected");
  });

  it("removes spatial transforms from reduced-motion variants", () => {
    expect(reducedVariantHasNoSpatialTransform(reducedRouteVariants)).toBe(true);
    expect(reducedVariantHasNoSpatialTransform(variantsForIntensity("route", "reduced"))).toBe(true);
    expect(reducedVariantHasNoSpatialTransform(variantsForIntensity("panel", "reduced"))).toBe(true);
  });

  it("keeps route transition variants available for standard motion", () => {
    const standardRoute = variantsForIntensity("route", "standard");
    expect(standardRoute.initial).toMatchObject({ opacity: 0, y: 6 });
    expect(standardRoute.animate).toMatchObject({ opacity: 1, y: 0 });
  });
});
