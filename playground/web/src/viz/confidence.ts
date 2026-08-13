import type { FlaggedCells } from "../types";

/**
 * The confidence encoder: what the detector confidence signal can and cannot do.
 *
 * This exists to correct a misconception a user would otherwise form, which is a
 * legitimate reason for a chart: seeing a `confidence` field, the natural move is to
 * rank the review queue by it. On the measured hospital queue with inferred
 * functional dependencies that would accomplish nothing, because 10,261 of 10,373
 * flagged cells carry confidence exactly 0.95 (23 distinct values across the whole
 * queue, per eval/results/detector_queue_composition.json). The distribution is a
 * spike, and a spike is only visible as a distribution.
 *
 * IMPORTANT, and the reason this file draws no threshold line: `_DEFAULT_THRESHOLDS`
 * in dataforge/calibration.py maps issue types to auto-apply thresholds, and it is
 * tempting to plot confidence against them. Those thresholds gate only
 * `_LLM_PROVENANCE` fixes. `partition_auto_apply` states that "deterministic ones
 * always auto-apply", and the playground's corrector policy is the disabled 1.01
 * sentinel throughout. Plotting a threshold against detector confidence would
 * therefore draw a gate that never fires -- a false claim about what decided the
 * outcome. What actually decides it is provenance, verification strength, and the
 * safety verdict, all of which are already surfaced as text.
 */

export interface ConfidenceBin {
  /** Lower edge, inclusive. */
  from: number;
  /** Upper edge, exclusive except for the final bin. */
  to: number;
  count: number;
}

export interface ConfidenceClass {
  issueType: string;
  bins: ConfidenceBin[];
  count: number;
  distinctValues: number;
  /** The most common single value, and how much of the class sits on it. */
  modeValue: number | null;
  modeShare: number;
  /** True when one value holds most of the class, so ranking by it is futile. */
  degenerate: boolean;
}

export interface ConfidenceDistribution {
  classes: ConfidenceClass[];
  totalCells: number;
  absence: "zero" | "not_measured" | null;
  absenceText: string;
  /** Stated in the UI so the reading is not left to the viewer. */
  finding: string;
}

const BIN_COUNT = 10;
const DEGENERATE_SHARE = 0.5;

/**
 * Build the distribution from the SERVER-SIDE histogram.
 *
 * The histogram is not merely a smaller payload than raw per-cell confidences -- it
 * is the only correct source. `flagged.cells` is a bounded prefix ordered by
 * severity then descending confidence, so computing a distribution from it would be
 * systematically biased towards the high-severity, high-confidence tail. On the
 * measured hospital queue that prefix is 500 of 10,373 cells, deliberately chosen
 * from one end. A distribution over the whole population must be computed where the
 * whole population exists, which is the server.
 */
export function buildConfidenceDistribution(
  flagged: FlaggedCells | null | undefined,
): ConfidenceDistribution {
  if (flagged === null || flagged === undefined) {
    return {
      classes: [],
      totalCells: 0,
      absence: "not_measured",
      absenceText: "No per-cell confidences were published for this run.",
      finding: "",
    };
  }

  const histogram = flagged.confidence_histogram ?? [];
  if (histogram.length === 0) {
    const measured = flagged.total > 0;
    return {
      classes: [],
      totalCells: 0,
      absence: measured ? "not_measured" : "zero",
      absenceText: measured
        ? "This run flagged cells but published no confidence histogram."
        : "No cells were flagged, so there are no confidences to describe.",
      finding: "",
    };
  }

  const classes: ConfidenceClass[] = histogram
    .map((entry) => ({
      issueType: entry.issue_type,
      bins: entry.bins.map((bin) => ({
        from: bin.from_value,
        to: bin.to_value,
        count: bin.count,
      })),
      count: entry.count,
      distinctValues: entry.distinct_values,
      modeValue: entry.mode_value ?? null,
      modeShare: entry.mode_share,
      degenerate: entry.mode_share >= DEGENERATE_SHARE,
    }))
    .sort((a, b) => b.count - a.count || a.issueType.localeCompare(b.issueType));

  const degenerateClasses = classes.filter((entry) => entry.degenerate);
  const finding =
    degenerateClasses.length === 0
      ? "Confidence varies across this queue, so it carries some ordering information."
      : `Confidence is near-degenerate in ${degenerateClasses.length} of ${classes.length} ` +
        "detector classes: most cells share a single value, so ranking the review queue by " +
        "confidence would barely reorder it.";

  return {
    classes,
    // Population size from the histogram, not from the bounded detail prefix.
    totalCells: classes.reduce((sum, entry) => sum + entry.count, 0),
    absence: null,
    absenceText: "",
    finding,
  };
}
