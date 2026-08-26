"""Measure FD mining against COLUMN count, which nothing has ever done.

Why this script exists
----------------------
``dataforge/schema_inference.py::_fd_candidates`` is a bare nested loop over the column list, so it
is Theta(C^2 * R): C*(C-1) ordered pairs, each doing a full row pass to build determinant groups plus
a ``Counter`` over each group. It has **no column cap and no timeout**. The only early exits are
data-shaped -- a row floor of 5, a near-key fraction, a support-group minimum -- and the support and
confidence guards fire *after* the O(R) grouping pass has already run, so pruning does not change the
asymptotic cost.

The widest table this project has ever run it against is **hospital at 20 columns**. flights has 7,
rayyan 11, tax 15. The two 1200-column benchmarks are not a counterexample: each row there is a
single column record, so ``fd_violation`` is structurally ``not_applicable`` and the miner cannot run
on that corpus at all.

There is a second cost that matters more than the runtime, and it is also quadratic: the candidate
list is O(C^2), and **every candidate becomes a row a human must adjudicate** in
``constraints review``. Hospital's 20 columns yield 119 candidates. Nothing caps that.

What is measured
----------------
Wall-clock time and candidate count for ``infer_schema`` against synthetic tables of increasing
width, at a fixed row count. Synthetic because the question is about shape, not content: a real wide
corpus would confound width with everything else about the data.

The generator deliberately produces columns that SURVIVE the miner's determinant guards -- low
cardinality, non-constant, not near-unique -- because a table of unique ids would be rejected at the
near-key guard and would measure the guard rather than the loop. That choice makes this an
upper-bound-ish measurement on candidate count and a fair one on time.

Usage:
    python scripts/bench/measure_fd_mining_width.py --artifact eval/results/fd_mining_width.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd  # noqa: E402

from dataforge.schema_inference import infer_schema  # noqa: E402

#: Widths to measure. 20 is hospital, the widest table ever tested, so it anchors the curve to
#: something already published rather than starting in unmeasured territory.
WIDTHS: tuple[int, ...] = (10, 20, 40, 60, 80, 100)
#: Fixed row count. Small enough that a 100-column table completes, large enough to clear the
#: miner's 5-row floor and to give determinant groups real size.
ROWS = 500


def _synthetic(columns: int, rows: int, *, seed: int = 20260826) -> pd.DataFrame:
    """Build a table whose columns survive the miner's determinant guards.

    Each column takes one of a small number of distinct values, so it is neither constant (rejected
    at ``determinant_unique < 2``) nor near-unique (rejected at the 0.9 fraction). Values are derived
    deterministically from the row index and the column index, which also produces genuine
    approximate dependencies between columns whose periods divide one another -- the realistic case,
    and the one that generates candidates rather than early exits.
    """
    del seed  # deterministic by construction; recorded in the artifact for reproducibility
    data: dict[str, list[str]] = {}
    for index in range(columns):
        period = 2 + (index % 7)
        data[f"c{index:03d}"] = [f"v{row % period}" for row in range(rows)]
    return pd.DataFrame(data)


def measure() -> dict[str, Any]:
    """Time ``infer_schema`` at each width and report the scaling."""
    results: list[dict[str, Any]] = []
    for width in WIDTHS:
        frame = _synthetic(width, ROWS)
        start = time.perf_counter()
        inferred = infer_schema(frame)
        elapsed = time.perf_counter() - start
        fd_candidates = [c for c in inferred.candidates if c.kind == "functional_dependency"]
        results.append(
            {
                "columns": width,
                "rows": ROWS,
                "ordered_pairs": width * (width - 1),
                "seconds": round(elapsed, 4),
                "candidates_total": len(inferred.candidates),
                "candidates_functional_dependency": len(fd_candidates),
                # The review burden, which is the cost a human pays and the one nothing caps.
                "review_rows_a_human_would_adjudicate": len(inferred.candidates),
            }
        )

    baseline = next(r for r in results if r["columns"] == 20)
    widest = results[-1]
    return {
        "schema": "dataforge_fd_mining_width_v1",
        "note": (
            "Theta(C^2 * R) with no column cap and no timeout. The widest table this project has "
            "ever measured before this artifact is hospital at 20 columns. Synthetic tables, "
            "because the question is about shape rather than content."
        ),
        "measurements": results,
        "summary": {
            "hospital_width_seconds": baseline["seconds"],
            "widest_measured_columns": widest["columns"],
            "widest_measured_seconds": widest["seconds"],
            # Time ratio against the pair-count ratio. If the loop dominates, these track.
            "seconds_ratio_widest_over_20": (
                round(widest["seconds"] / baseline["seconds"], 2) if baseline["seconds"] else None
            ),
            "pairs_ratio_widest_over_20": round(
                widest["ordered_pairs"] / baseline["ordered_pairs"], 2
            ),
            "review_rows_at_widest": widest["review_rows_a_human_would_adjudicate"],
            "review_rows_ratio_widest_over_20": (
                round(
                    widest["review_rows_a_human_would_adjudicate"]
                    / baseline["review_rows_a_human_would_adjudicate"],
                    2,
                )
                if baseline["review_rows_a_human_would_adjudicate"]
                else None
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()

    payload = measure()
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"wrote {args.artifact}")
    print(f"  {'cols':>5}{'pairs':>8}{'seconds':>10}{'candidates':>12}")
    for row in payload["measurements"]:
        print(
            f"  {row['columns']:>5}{row['ordered_pairs']:>8}{row['seconds']:>10}"
            f"{row['candidates_total']:>12}"
        )
    summary = payload["summary"]
    print(
        f"  {summary['widest_measured_columns']} columns is "
        f"{summary['seconds_ratio_widest_over_20']}x the time and "
        f"{summary['review_rows_ratio_widest_over_20']}x the review rows of hospital's 20"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
