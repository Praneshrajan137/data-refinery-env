import { describe, expect, it } from "vitest";
import { buildDependencyGraphForTest as buildGraph } from "./DependencyGraph";
import type { ConstraintCandidate } from "../types";

function fd(
  from: string,
  to: string,
  partial: Partial<ConstraintCandidate> = {},
): ConstraintCandidate {
  return {
    candidate_id: `cnd-${from}-${to}`,
    kind: "functional_dependency",
    columns: [from],
    dependent: to,
    confidence: 0.97,
    evidence: "e",
    decision: "pending",
    repair_supported: true,
    ...partial,
  } as ConstraintCandidate;
}

describe("dependency graph layout is deterministic (L4)", () => {
  it("layers determinants before their dependents", () => {
    const graph = buildGraph([fd("a", "b"), fd("b", "c")]);
    const layerOf = new Map(graph.nodes.map((n) => [n.column, n.layer]));
    expect(layerOf.get("a")).toBe(0);
    expect(layerOf.get("b")).toBe(1);
    expect(layerOf.get("c")).toBe(2);
  });

  it("produces identical output for identical input, regardless of input order", () => {
    const forward = buildGraph([fd("a", "b"), fd("b", "c"), fd("a", "c")]);
    const shuffled = buildGraph([fd("a", "c"), fd("b", "c"), fd("a", "b")]);
    expect(JSON.stringify(forward.nodes)).toBe(JSON.stringify(shuffled.nodes));
  });

  it("terminates on a cyclic dependency set rather than hanging", () => {
    // FD mining does not guarantee acyclicity, so the layering must be capped.
    const graph = buildGraph([fd("a", "b"), fd("b", "a")]);
    expect(graph.nodes).toHaveLength(2);
    expect(graph.edges).toHaveLength(2);
    for (const node of graph.nodes) {
      expect(Number.isFinite(node.layer)).toBe(true);
    }
  });

  it("ignores non-dependency candidates", () => {
    const graph = buildGraph([
      fd("a", "b"),
      { ...fd("x", "y"), kind: "domain_bound", dependent: null } as ConstraintCandidate,
    ]);
    expect(graph.edges).toHaveLength(1);
  });

  it("distinguishes zero from not-measured (L3)", () => {
    expect(buildGraph([]).absence).toBe("not_measured");
    expect(
      buildGraph([{ ...fd("x", "y"), kind: "unique", dependent: null } as ConstraintCandidate])
        .absence,
    ).toBe("zero");
    expect(buildGraph([fd("a", "b")]).absence).toBeNull();
  });

  it("sizes the canvas from the layout rather than guessing", () => {
    const graph = buildGraph([fd("a", "b")]);
    expect(graph.width).toBeGreaterThan(0);
    expect(graph.height).toBeGreaterThan(0);
  });
});
