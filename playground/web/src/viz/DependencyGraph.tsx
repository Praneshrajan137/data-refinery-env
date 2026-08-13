import { useMemo } from "react";
import type { AnalyzeResponse, ConstraintCandidate } from "../types";

/**
 * The column dependency graph: which columns determine which.
 *
 * Rendered as SVG, not on the GPU, and in 2D, not 3D -- both deliberate:
 *
 * - **2D.** The pro-3D result for graph reading (stereo plus motion parallax
 *   raising the graph size in which path tracing stays accurate) applies to large
 *   graphs. Real tables here have 7 to 20 columns. At that size a deterministic
 *   layered layout in 2D is strictly easier to read, and 3D would add occlusion
 *   and a camera for nothing.
 * - **SVG.** A few dozen nodes need no GPU, and SVG keeps text selectable and
 *   crisp at any zoom. Reaching for the renderer here would be cost without
 *   benefit.
 * - **No force simulation.** L4 forbids motion with no referent event; a layout
 *   that jitters toward equilibrium is animation the data never asked for. This
 *   layout is deterministic and arrives composed.
 */

const NODE_WIDTH = 116;
const NODE_HEIGHT = 26;
const LAYER_GAP = 74;
const ROW_GAP = 12;
const PADDING = 12;

interface GraphEdge {
  from: string;
  to: string;
  confidence: number;
  decision: string;
  repairSupported: boolean;
  candidateId: string;
}

interface GraphNode {
  column: string;
  layer: number;
  index: number;
  x: number;
  y: number;
}

interface Graph {
  nodes: GraphNode[];
  edges: GraphEdge[];
  width: number;
  height: number;
  absence: "zero" | "not_measured" | null;
  absenceText: string;
}

function buildGraph(candidates: ConstraintCandidate[] | undefined): Graph {
  const fds = (candidates ?? []).filter(
    (candidate) => candidate.kind === "functional_dependency" && candidate.dependent,
  );

  const edges: GraphEdge[] = fds.map((candidate) => ({
    from: candidate.columns[0] ?? "",
    to: candidate.dependent as string,
    confidence: candidate.confidence,
    decision: candidate.decision,
    repairSupported: candidate.repair_supported,
    candidateId: candidate.candidate_id,
  }));

  if (edges.length === 0) {
    return {
      nodes: [],
      edges: [],
      width: 0,
      height: 0,
      absence: (candidates ?? []).length === 0 ? "not_measured" : "zero",
      absenceText:
        (candidates ?? []).length === 0
          ? "No constraints were inferred, so no dependencies could be found."
          : "Constraints were inferred, but none of them was a dependency between columns.",
    };
  }

  const columns = [...new Set(edges.flatMap((edge) => [edge.from, edge.to]))].sort();
  const incoming = new Map<string, string[]>();
  for (const column of columns) {
    incoming.set(column, []);
  }
  for (const edge of edges) {
    incoming.get(edge.to)?.push(edge.from);
  }

  // Longest-path layering, iteration-capped so a cyclic mined dependency set
  // cannot hang the layout. Mining does not guarantee acyclicity.
  const layer = new Map<string, number>(columns.map((column) => [column, 0]));
  for (let pass = 0; pass < columns.length; pass += 1) {
    let changed = false;
    for (const column of columns) {
      const parents = incoming.get(column) ?? [];
      const deepest = parents.reduce(
        (max, parent) => Math.max(max, (layer.get(parent) ?? 0) + 1),
        0,
      );
      if (deepest > (layer.get(column) ?? 0)) {
        layer.set(column, deepest);
        changed = true;
      }
    }
    if (!changed) {
      break;
    }
  }

  const byLayer = new Map<number, string[]>();
  for (const column of columns) {
    const index = layer.get(column) ?? 0;
    byLayer.set(index, [...(byLayer.get(index) ?? []), column]);
  }

  const nodes: GraphNode[] = [];
  let maxRows = 0;
  for (const [layerIndex, members] of [...byLayer.entries()].sort((a, b) => a[0] - b[0])) {
    // Alphabetical within a layer: identical input must give identical output.
    const sorted = [...members].sort();
    maxRows = Math.max(maxRows, sorted.length);
    sorted.forEach((column, index) => {
      nodes.push({
        column,
        layer: layerIndex,
        index,
        x: PADDING + layerIndex * (NODE_WIDTH + LAYER_GAP),
        y: PADDING + index * (NODE_HEIGHT + ROW_GAP),
      });
    });
  }

  const layerCount = byLayer.size;
  return {
    nodes,
    edges,
    width: PADDING * 2 + layerCount * NODE_WIDTH + Math.max(layerCount - 1, 0) * LAYER_GAP,
    height: PADDING * 2 + maxRows * NODE_HEIGHT + Math.max(maxRows - 1, 0) * ROW_GAP,
    absence: null,
    absenceText: "",
  };
}

export function DependencyGraph({ analysis }: { analysis: AnalyzeResponse | null }) {
  const graph = useMemo(
    () => buildGraph(analysis?.schema_inference?.candidates),
    [analysis?.schema_inference?.candidates],
  );

  if (analysis === null) {
    return null;
  }

  const acceptedCount = graph.edges.filter((edge) => edge.decision === "accepted").length;
  const label =
    graph.edges.length === 0
      ? `Column dependency graph: ${graph.absenceText}`
      : `Column dependency graph: ${graph.nodes.length} columns, ${graph.edges.length} inferred ` +
        `dependencies, ${acceptedCount} accepted as authoritative.`;

  const nodeAt = new Map(graph.nodes.map((node) => [node.column, node]));

  return (
    <section className="loop-panel dependency-graph" aria-label="Column dependency graph">
      <header>
        <h3>What determines what</h3>
        <p className="dependency-graph__note">
          Dependencies mined from this file. Accepting one makes it authoritative, which is
          what lets a fix be proven — and it is inferred from the same data it will then
          judge, so acceptance is a decision, not a formality.
        </p>
      </header>

      {graph.absence !== null ? (
        <p className={`evidence-surface__absence evidence-surface__absence--${graph.absence}`}>
          {graph.absenceText}
        </p>
      ) : (
        <div className="dependency-graph__frame">
          <svg
            role="img"
            aria-label={label}
            viewBox={`0 0 ${graph.width} ${graph.height}`}
            width={graph.width}
            height={graph.height}
            className="dependency-graph__svg"
          >
            <g className="dependency-graph__edges">
              {graph.edges.map((edge) => {
                const from = nodeAt.get(edge.from);
                const to = nodeAt.get(edge.to);
                if (!from || !to) {
                  return null;
                }
                const x1 = from.x + NODE_WIDTH;
                const y1 = from.y + NODE_HEIGHT / 2;
                const x2 = to.x;
                const y2 = to.y + NODE_HEIGHT / 2;
                const mid = (x1 + x2) / 2;
                return (
                  <path
                    key={edge.candidateId}
                    d={`M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`}
                    className={`dependency-graph__edge dependency-graph__edge--${edge.decision}`}
                    fill="none"
                  />
                );
              })}
            </g>
            <g className="dependency-graph__nodes">
              {graph.nodes.map((node) => (
                <g key={node.column} transform={`translate(${node.x}, ${node.y})`}>
                  <rect
                    width={NODE_WIDTH}
                    height={NODE_HEIGHT}
                    rx={4}
                    className="dependency-graph__node"
                  />
                  <text x={NODE_WIDTH / 2} y={NODE_HEIGHT / 2 + 4} textAnchor="middle">
                    {node.column}
                  </text>
                </g>
              ))}
            </g>
          </svg>
        </div>
      )}

      {graph.edges.length > 0 ? (
        <div className="table-frame" tabIndex={0}>
          <table>
            <caption className="visually-hidden">
              Inferred column dependencies, their confidence, and whether they were accepted
            </caption>
            <thead>
              <tr>
                <th scope="col">Determines</th>
                <th scope="col">Dependent</th>
                <th scope="col">Confidence</th>
                <th scope="col">Decision</th>
                <th scope="col">Repairs supported</th>
              </tr>
            </thead>
            <tbody>
              {[...graph.edges]
                .sort(
                  (a, b) =>
                    a.from.localeCompare(b.from) || a.to.localeCompare(b.to),
                )
                .map((edge) => (
                  <tr key={edge.candidateId}>
                    <th scope="row">{edge.from}</th>
                    <td>{edge.to}</td>
                    <td>{Math.round(edge.confidence * 100)}%</td>
                    <td>{edge.decision}</td>
                    <td>{edge.repairSupported ? "yes" : "no"}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  );
}

export { buildGraph as buildDependencyGraphForTest };
