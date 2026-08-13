import { useMemo } from "react";
import type { AnalyzeResponse } from "../types";
import { buildConfidenceDistribution } from "./confidence";

/**
 * What the confidence signal can and cannot do.
 *
 * This panel exists to prevent a specific wrong action. Seeing a per-cell
 * `confidence`, the natural move is to sort the review queue by it. On the measured
 * hospital queue with inferred functional dependencies that accomplishes almost
 * nothing, because the distribution is a spike. A spike is only visible as a
 * distribution, which is why this is a chart rather than a sentence.
 *
 * It draws NO threshold line, deliberately. `_DEFAULT_THRESHOLDS` maps issue types to
 * auto-apply thresholds and is tempting to overlay, but `partition_auto_apply` states
 * that "deterministic ones always auto-apply" -- those thresholds gate only LLM-
 * provenance fixes, and the playground's corrector policy is the disabled sentinel
 * throughout. A threshold line here would draw a gate that never fires.
 *
 * Bars encode count by LENGTH, which is L1's third channel. Not area, not intensity.
 */
export function ConfidenceDistribution({ analysis }: { analysis: AnalyzeResponse | null }) {
  const distribution = useMemo(
    () => buildConfidenceDistribution(analysis?.flagged_cells),
    [analysis?.flagged_cells],
  );

  if (analysis === null) {
    return null;
  }

  return (
    <section className="loop-panel confidence-distribution" aria-label="Detector confidence">
      <header>
        <h3>What detector confidence can tell you</h3>
        <p className="confidence-distribution__note">
          Confidence is the detector&rsquo;s own strength for a flagged cell. It is not a
          proof, and it is not what decided whether a change was written &mdash; provenance,
          verification strength and the safety verdict decide that.
        </p>
      </header>

      {distribution.absence !== null ? (
        <p
          className={`evidence-surface__absence evidence-surface__absence--${distribution.absence}`}
        >
          {distribution.absenceText}
        </p>
      ) : (
        <>
          <p className="confidence-distribution__finding">{distribution.finding}</p>
          <ul className="confidence-classes">
            {distribution.classes.map((entry) => {
              const peak = Math.max(...entry.bins.map((bin) => bin.count), 1);
              return (
                <li key={entry.issueType} className="confidence-class">
                  <p className="confidence-class__head">
                    <span className="confidence-class__name">
                      {entry.issueType.replace(/_/g, " ")}
                    </span>
                    <span className="confidence-class__meta">
                      {entry.count} cells, {entry.distinctValues} distinct value
                      {entry.distinctValues === 1 ? "" : "s"}
                      {entry.degenerate && entry.modeValue !== null
                        ? ` — ${Math.round(entry.modeShare * 100)}% at ${entry.modeValue}`
                        : ""}
                    </span>
                  </p>
                  <div
                    className="confidence-histogram"
                    role="img"
                    aria-label={histogramLabel(entry.issueType, entry)}
                  >
                    {entry.bins.map((bin) => (
                      <span
                        key={bin.from}
                        className="confidence-bin"
                        style={{ height: `${Math.round((bin.count / peak) * 100)}%` }}
                      />
                    ))}
                  </div>
                  {entry.degenerate ? (
                    <p className="confidence-class__verdict">
                      Near-degenerate: sorting this class by confidence would barely reorder
                      it.
                    </p>
                  ) : null}
                </li>
              );
            })}
          </ul>
          <p className="confidence-distribution__scope">
            No auto-apply threshold is drawn here. The per-class thresholds in the engine
            gate LLM-proposed values only; deterministic fixes auto-apply when proven,
            regardless of confidence. Drawing a threshold against these bars would show a
            gate that never fires on this run.
          </p>
        </>
      )}
    </section>
  );
}

function histogramLabel(
  issueType: string,
  entry: { count: number; distinctValues: number; modeValue: number | null; modeShare: number },
): string {
  const base =
    `Confidence distribution for ${issueType.replace(/_/g, " ")}: ${entry.count} cells across ` +
    `${entry.distinctValues} distinct confidence value${entry.distinctValues === 1 ? "" : "s"}`;
  if (entry.modeValue === null) {
    return `${base}.`;
  }
  return `${base}, with ${Math.round(entry.modeShare * 100)} percent at ${entry.modeValue}.`;
}
