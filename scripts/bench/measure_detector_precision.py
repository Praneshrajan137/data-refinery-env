"""Measure what the detector queue actually contains, per dataset and per issue type.

Every downstream claim in the API phase rests on the composition of this queue -- the
cost of human review, the value of triage, the density of positives available for
certification -- and none of it had been measured directly. This script does that, for
free, with no provider calls.

**Why the ground-truth objection does not apply.** A natural worry is that a low measured
precision is an artifact of incomplete annotation: detectors flagging real problems that
the benchmark never labelled. That cannot happen here. ``_compute_ground_truth`` in
``dataforge.datasets.real_world`` derives ground truth as ``dirty_text != clean_text``
over every cell, so it is **complete by construction relative to the clean reference**. A
flagged cell absent from ground truth is therefore a cell whose value *equals* the
reference -- a false positive with respect to that reference, not an unlabelled error.

The residual caveat, which this script records rather than hides: the clean reference is
itself curated, so a flagged cell matching it could still be a genuine quality issue the
curator chose to leave. That is a claim about the benchmark, not about the detectors, and
it is bounded by sampling actual false positives into the artifact for inspection.

**The regime distinction that makes or breaks every downstream claim.** The queue depends
entirely on whether inferred constraints are supplied to the detectors:

* **default** -- ``run_all_detectors(df)``, the shipped path.
* **inferred** -- ``run_all_detectors(df, schema=infer_schema(df).to_schema(
  include_inferred_constraints=True))``, which activates FD-violation detection from
  dependencies mined out of the *dirty* data.

Every paid experiment in the API phase (the arm sweep, the flagship, the review-gate probe,
the triage comparison) used the **inferred** regime, and its low precision was then reported
as a property of "the detector queue". This script measures both so that claim can be
scoped correctly. The project already documents why inferred constraints are hazardous --
see ``docs/trust/constraint-circularity.md`` -- which makes the distinction load-bearing
rather than academic.

Run::

    python scripts/bench/measure_detector_precision.py
"""

from __future__ import annotations

import json
import random
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors import run_all_detectors  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402
from dataforge.table import cell_value  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "detector_queue_composition.json"
_SCHEMA = "dataforge_detector_queue_composition_v1"
_DATASETS = ("hospital", "flights", "rayyan")
_FP_SAMPLE = 12
_SEED = 20260805


def _wilson_interval(successes: int, total: int, *, z: float = 1.96) -> list[float]:
    """Return a Wilson score interval for a proportion.

    Wilson rather than normal-approximation because these proportions sit near zero,
    where the normal interval produces negative lower bounds.
    """
    if total == 0:
        return [0.0, 0.0]
    phat = successes / total
    denom = 1.0 + z * z / total
    centre = (phat + z * z / (2 * total)) / denom
    half = z * ((phat * (1 - phat) / total + z * z / (4 * total * total)) ** 0.5) / denom
    return [round(max(0.0, centre - half), 6), round(min(1.0, centre + half), 6)]


def _per_type(issues: list[Any], truth: set[tuple[int, str]]) -> dict[str, Any]:
    """Break the queue down by issue type, since precision is wildly uneven across them."""
    by_type: dict[str, dict[str, Any]] = {}
    for issue in issues:
        entry = by_type.setdefault(issue.issue_type, {"flagged": 0, "true_errors": 0})
        entry["flagged"] += 1
        if (issue.row, issue.column) in truth:
            entry["true_errors"] += 1
    for entry in by_type.values():
        entry["precision"] = round(entry["true_errors"] / entry["flagged"], 6)
        entry["precision_ci95_wilson"] = _wilson_interval(entry["true_errors"], entry["flagged"])
    return dict(sorted(by_type.items(), key=lambda kv: -kv[1]["flagged"]))


def _measure(
    dataset: Any, issues: list[Any], truth: set[tuple[int, str]], rng: random.Random
) -> dict[str, Any]:
    """Summarise one queue: precision, review effort, and inspectable false positives."""
    true_positives = [i for i in issues if (i.row, i.column) in truth]
    false_positives = [i for i in issues if (i.row, i.column) not in truth]
    flagged = len(issues)
    # Sample real false positives so the "is the reference itself wrong?" caveat is
    # inspectable rather than rhetorical.
    sample = rng.sample(false_positives, min(_FP_SAMPLE, len(false_positives)))
    return {
        "flagged_cells": flagged,
        "true_errors_flagged": len(true_positives),
        "false_positives": len(false_positives),
        "precision": round(len(true_positives) / flagged, 6) if flagged else 0.0,
        "precision_ci95_wilson": _wilson_interval(len(true_positives), flagged),
        "recall": round(len(true_positives) / len(truth), 6) if truth else 0.0,
        "false_positive_rate_of_queue": (
            round(len(false_positives) / flagged, 6) if flagged else 0.0
        ),
        # Review effort per genuine error found, if a human works the queue unranked.
        # This is the number triage must beat to justify its cost.
        "cells_reviewed_per_true_error": (
            round(flagged / len(true_positives), 3) if true_positives else None
        ),
        "distinct_confidences": len({i.confidence for i in issues}),
        "most_common_confidences": Counter(i.confidence for i in issues).most_common(4),
        "by_issue_type": _per_type(issues, truth),
        "false_positive_examples": [
            {
                "row": issue.row,
                "column": issue.column,
                "issue_type": issue.issue_type,
                "flagged_value": str(cell_value(dataset.dirty_df, issue.row, issue.column)),
                "clean_reference_value": str(cell_value(dataset.clean_df, issue.row, issue.column)),
                "detector_confidence": issue.confidence,
                "detector_reason": issue.reason[:160],
            }
            for issue in sample
        ],
    }


def main() -> int:
    """Measure queue composition for every dataset, in both detector regimes."""
    rng = random.Random(_SEED)
    datasets: dict[str, Any] = {}

    for name in _DATASETS:
        dataset = load_real_world_dataset(name)
        truth = {(c.row, c.column) for c in dataset.ground_truth}
        inferred = infer_schema(dataset.dirty_df.copy(deep=True)).to_schema(
            include_inferred_constraints=True
        )
        regimes = {
            "default": run_all_detectors(dataset.dirty_df.copy(deep=True)),
            "inferred_constraints": run_all_detectors(
                dataset.dirty_df.copy(deep=True), schema=inferred
            ),
        }
        per_regime: dict[str, Any] = {}
        for regime, issues in regimes.items():
            per_regime[regime] = _measure(dataset, issues, truth, rng)
            entry = per_regime[regime]
            print(
                f"{name:9s} {regime:21s} flagged={entry['flagged_cells']:<6d} "
                f"true={entry['true_errors_flagged']:<5d} "
                f"precision={entry['precision']:.4f} "
                f"recall={entry['recall']:.4f} "
                f"review_per_error={entry['cells_reviewed_per_true_error']}"
            )
        datasets[name] = {
            "rows": int(len(dataset.dirty_df)),
            "ground_truth_cells": len(truth),
            "regimes": per_regime,
        }

    payload = {
        "schema": _SCHEMA,
        "question": (
            "What does the detector queue actually contain, and how much human effort "
            "does one genuine error cost when the queue is worked unranked?"
        ),
        "regime_note": (
            "'default' is the shipped run_all_detectors(df) path. 'inferred_constraints' "
            "additionally supplies a schema mined from the DIRTY data, enabling "
            "FD-violation detection. Every paid API-phase experiment used "
            "'inferred_constraints', so any precision claim from those runs is scoped to "
            "that regime and must not be stated as a property of the detectors generally. "
            "See docs/trust/constraint-circularity.md."
        ),
        "ground_truth_completeness": (
            "COMPLETE BY CONSTRUCTION relative to the clean reference: "
            "real_world._compute_ground_truth labels every cell where dirty != clean. A "
            "flagged cell absent from ground truth therefore matches the reference and is "
            "a false positive with respect to it, NOT an unlabelled error. Residual "
            "caveat: the reference is itself curated, so see false_positive_examples."
        ),
        "seed": _SEED,
        "datasets": datasets,
    }
    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"\nartifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
