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
 * Bars encode a PROPORTION by length on a COMMON scale, which is L1's third channel used
 * the way L1's justification requires. Cleveland and McGill's rank 1 is "position along a
 * common scale" and rank 2 is "position along identical, NON-ALIGNED scales" -- the two differ
 * precisely by whether the scale is shared. This histogram previously normalised every class
 * to its own peak (`Math.max(...entry.bins.map(b => b.count))`) with no axis, no ticks and no
 * labels, so bar heights were not comparable across classes and no reader could recover a
 * magnitude from one. A length with no scale is a shape.
 *
 * Every bar also carries its Clopper-Pearson interval, and the interval is the MARK rather
 * than a decoration on it. A bin is a count out of a known class total, so it is a binomial
 * proportion whose precision is computable, and drawing the point estimate alone would claim
 * a precision the data does not have. Error bars are avoided deliberately: they are
 * systematically misread, including a "within-the-bar" bias where values inside a bar read as
 * more likely than values outside it (Correll and Gleicher, "Error Bars Considered Harmful",
 * IEEE TVCG 20(12):2142-2151, 2014). The band IS the estimate's extent; the tick marks where
 * the point estimate falls inside it.
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
            {distribution.classes.map((entry) => (
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
                <div className="confidence-plot">
                  {/* The common scale, drawn. Without it the lengths are shapes. */}
                  <ul className="confidence-axis" aria-hidden="true">
                    {AXIS_TICKS.map((tick) => (
                      <li
                        key={tick}
                        className="confidence-axis__tick"
                        style={{ bottom: `${tick * 100}%` }}
                      >
                        <span className="confidence-axis__label">{Math.round(tick * 100)}%</span>
                      </li>
                    ))}
                  </ul>
                  <div
                    className="confidence-histogram"
                    role="img"
                    aria-label={histogramLabel(entry.issueType, entry)}
                  >
                    {entry.bins.map((bin) => (
                      <span key={bin.from} className="confidence-bin">
                        <span
                          className="confidence-bin__interval"
                          style={{
                            bottom: `${bin.lower * 100}%`,
                            height: `${Math.max(bin.upper - bin.lower, 0) * 100}%`,
                          }}
                        />
                        <span
                          className="confidence-bin__estimate"
                          style={{ bottom: `${bin.proportion * 100}%` }}
                        />
                      </span>
                    ))}
                  </div>
                </div>
                <p className="confidence-class__scale">
                  Share of this class per confidence bin, 0 to 100% on a common scale. The band
                  is the 95% Clopper-Pearson interval; the line is the observed share.
                </p>
                {entry.degenerate ? (
                  <p className="confidence-class__verdict">
                    Near-degenerate: sorting this class by confidence would barely reorder it.
                  </p>
                ) : null}
              </li>
            ))}
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

/** Quarter steps: enough to read a share against, few enough not to become the chart. */
const AXIS_TICKS = [0, 0.25, 0.5, 0.75, 1] as const;

function histogramLabel(
  issueType: string,
  entry: {
    count: number;
    distinctValues: number;
    modeValue: number | null;
    modeShare: number;
    bins: { from: number; proportion: number; lower: number; upper: number }[];
  },
): string {
  const base =
    `Confidence distribution for ${issueType.replace(/_/g, " ")}: ${entry.count} cells across ` +
    `${entry.distinctValues} distinct confidence value${entry.distinctValues === 1 ? "" : "s"}`;
  // The widest bin's interval, so a screen-reader user learns the precision too rather than
  // only the point estimates. Reading every bin aloud would be unusable.
  const widest = entry.bins.reduce(
    (worst, bin) => (bin.upper - bin.lower > worst.upper - worst.lower ? bin : worst),
    entry.bins[0],
  );
  const precision =
    widest === undefined
      ? ""
      : `. Widest 95% interval: ${Math.round(widest.lower * 100)} to ` +
        `${Math.round(widest.upper * 100)} percent, at confidence bin ${widest.from}`;
  if (entry.modeValue === null) {
    return `${base}${precision}.`;
  }
  return `${base}, with ${Math.round(entry.modeShare * 100)} percent at ${entry.modeValue}${precision}.`;
}
