"""Measure the harness's per-flag cost as a function of table size, to price the tax arm.

Not a published-claim harness: it measures the MEASUREMENT's cost, not the product's behaviour. Kept
because the tax deviation had been justified twice with an estimate that was wrong by two orders of
magnitude, and an estimate is not admissible where a measurement is cheap.

Usage:
    python scripts/bench/measure_harness_cost.py --artifact eval/results/harness_cost.json
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors.base import Issue, Severity  # noqa: E402
from dataforge.repairers.fd_violation import FDViolationRepairer  # noqa: E402
from scripts.bench.measure_deductive_coverage import (  # noqa: E402
    _acting_group,
    _schema_for,
    discover_oracle_fds,
)

#: Row counts to price. tax is 200,000 rows; the point is to show the trend is linear per call and
#: therefore quadratic for the arm, not to guess a constant.
ROW_COUNTS = (5_000, 20_000, 50_000, 100_000, 200_000)

#: Enough calls to average out noise, few enough to stay cheap at 200,000 rows.
CALLS = 40

#: Fixed so the control is reproducible. Sampling matters here: the first N rows of a table are not a
#: random sample of its determinant groups, and per-flag cost depends on group size. A figure taken
#: only from ``head`` could be unrepresentative in either direction.
SEED = 20260826


def _time_propose(
    repairer: FDViolationRepairer,
    frame: Any,
    schema: Any,
    dependent: str,
    row_indices: list[int],
) -> float:
    """Average seconds per ``propose`` call over the given rows.

    A module-level function rather than a closure: closing over the loop's ``frame`` would bind late,
    which is the class of bug that silently measures the wrong table.
    """
    column_index = frame.columns.get_loc(dependent)
    started = time.perf_counter()
    for row in row_indices:
        issue = Issue(
            row=row,
            column=dependent,
            issue_type="fd_violation",
            severity=Severity.REVIEW,
            confidence=0.9,
            actual=str(frame.iat[row, column_index]),
            reason="cost measurement",
        )
        repairer.propose(issue, frame, schema, None)
    return (time.perf_counter() - started) / len(row_indices)


def measure(corpus: str, *, cache_root: Path | None) -> dict[str, Any]:
    """Time ``_acting_group`` per call against table size."""
    dataset = load_real_world_dataset(corpus, cache_root=cache_root)
    full = dataset.dirty_df
    columns = tuple(str(column) for column in full.columns)
    fds = discover_oracle_fds(dataset.clean_df, columns=columns)
    if not fds:
        raise SystemExit(f"{corpus} mines no oracle FDs; nothing to price")

    dependent = fds[0].dependent
    repairer = FDViolationRepairer(cache_dir=None, allow_llm=False)
    points: list[dict[str, Any]] = []
    for rows in ROW_COUNTS:
        if rows > full.shape[0]:
            continue
        frame = full.head(rows).reset_index(drop=True)
        schema = _schema_for(frame, fds)

        started = time.perf_counter()
        for index in range(CALLS):
            _acting_group(frame, index, fds, dependent)
        acting_per_call = (time.perf_counter() - started) / CALLS

        # The shipped repairer, called exactly as the arm calls it. Timed separately because the
        # attribution matters: a harness-side cost is mine to memoise, a repairer-side cost is a
        # property of the product under test.
        propose_per_call = _time_propose(repairer, frame, schema, dependent, list(range(CALLS)))
        # The control. If these diverge, the headline figure is an artifact of where I sampled.
        rng = random.Random(SEED)
        propose_random = _time_propose(
            repairer, frame, schema, dependent, rng.sample(range(rows), CALLS)
        )

        per_call = acting_per_call + propose_per_call
        points.append(
            {
                "rows": rows,
                "acting_group_seconds_per_call": round(acting_per_call, 6),
                "acting_group_ms_per_call": round(acting_per_call * 1000, 2),
                "propose_seconds_per_call": round(propose_per_call, 6),
                "propose_ms_per_call": round(propose_per_call * 1000, 2),
                "propose_ms_per_call_random_rows": round(propose_random * 1000, 2),
                "propose_seconds_per_call_random_rows": round(propose_random, 6),
                "seconds_per_flag": round(per_call, 6),
                "ms_per_flag": round(per_call * 1000, 2),
                "propose_share": round(propose_per_call / per_call, 4) if per_call else None,
            }
        )
        print(
            f"  {rows:>7,} rows: _acting_group {acting_per_call * 1000:7.2f} ms  "
            f"propose {propose_per_call * 1000:8.2f} ms  "
            f"(random rows {propose_random * 1000:8.2f} ms)  "
            f"total {per_call * 1000:8.2f} ms/flag",
            file=sys.stderr,
            flush=True,
        )

    largest = points[-1]
    return {
        "schema": "dataforge_harness_cost_v1",
        "corpus": corpus,
        "function": "_acting_group + FDViolationRepairer.propose",
        "calls_averaged": CALLS,
        "note": (
            "Both per-flag costs are linear in table size, so the arm is quadratic in table size. "
            "_acting_group builds a boolean mask over the ENTIRE frame per call; "
            "FDViolationRepairer.propose performs its own scan and dominates. The attribution is "
            "measured rather than inferred, because an inference from process CPU time attributed "
            "the whole cost to the harness and was wrong. This is the same shape of defect already "
            "recorded for FormatViolationRepairer._dominant_profile, which rescanned its column per "
            "flag and was solved with a harness-level memo whose equivalence was verified."
        ),
        "points": points,
        "largest": largest,
        "stable": {
            # Timings are not reproducible to the decimal: repeated runs of this script varied the
            # 200,000-row `propose` figure between roughly 1,950 and 2,210 ms. Only these coarsened
            # renderings are safe to bind to prose. Precision must not exceed reproducibility, and a
            # claim ledger built for deterministic counts will happily pin a decimal that noise moves.
            "propose_seconds_per_flag_rounded": round(largest["propose_seconds_per_call"]),
            "propose_share": largest["propose_share"],
            "tax_oracle_days_rounded": round(largest["seconds_per_flag"] * 164718 / 86400),
        },
        "projection": {
            "tax_oracle_flags": 164718,
            "hours_at_largest": round(largest["seconds_per_flag"] * 164718 / 3600, 1),
            "acting_group_only_hours": round(
                largest["acting_group_seconds_per_call"] * 164718 / 3600, 1
            ),
            "caveat": (
                "Excludes FDViolationDetector.detect and the replay phase, so it is a lower bound on "
                "the arm rather than a prediction of it."
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="tax")
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()

    payload = measure(args.corpus, cache_root=args.cache_root)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"wrote {args.artifact}")
    print(
        f"  {payload['largest']['rows']:,} rows: "
        f"{payload['largest']['seconds_per_flag'] * 1000:.2f} ms per flag "
        f"({payload['largest']['propose_share']:.0%} of it in propose)"
    )
    print(
        f"  lower bound on tax oracle write-exposure: "
        f"{payload['projection']['hours_at_largest']} hours"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
