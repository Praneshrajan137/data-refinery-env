"""Measure whether accepting constraints is ADDITIVE. It is not.

Why this script exists
----------------------
The mechanism behind the shipped-premise result is a conjunction: a mined dependency corrupts a cell
only when it is **false** AND the cell's determinant group holds **visible disagreement** for the
repairer to resolve. Two of the four dependencies that took hospital's corruption count from 86 to
116 are equally false and corrupted nothing, because their groups did not disagree.

That raises a question the review interface implicitly answers wrongly. ``constraints review`` asks a
human for N independent accept/reject decisions. If per-candidate harm composed, a reviewer could be
shown "this dependency would overwrite K correct cells" and reason about a budget. This script
measures whether it composes.

It does not. When several accepted dependencies could act on one cell, only one does -- cell-ownership
precedence in ``_acting_group`` picks the first match -- so overlapping dependencies MASK one another.
The sum over candidates measured alone is therefore much larger than the result of accepting them all.

What is measured
----------------
For each mined candidate, the four-way write exposure with a schema containing **that dependency
alone**; then the same measurement with **all** of them. Both through the vetted
:func:`_write_exposure`, never a reimplementation -- see the discipline note below.

A note on how this script was nearly wrong
------------------------------------------
The first attempt reimplemented the write loop inline. It omitted the no-change filter that
``_write_exposure`` applies, because ``_rule_choice``'s docstring says it returns values "before the
no-change check". Write counts came out **959 where the truth is 74** -- 6 to 13 times too high -- and
it nearly produced a published finding that writes are 95% no-ops, which was purely the bug. It was
caught because the number looked implausible, not by any gate.

The rule, recorded in ``PRODUCT.md``: a reimplementation of a measurement reproduces the defect the
vetted path exists to avoid. Hence the imports below rather than a local copy.

Usage:
    python scripts/bench/measure_constraint_additivity.py --artifact eval/results/constraint_additivity.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from scripts.bench.measure_deductive_coverage import (  # noqa: E402
    _fd_label,
    _schema_for,
    _write_exposure,
    fd_holds_on_clean,
    shipped_accept_all_fds,
)

#: The shipped decision rule. The counterfactual rules are measured elsewhere; mixing them in here
#: would confound the additivity question with the rule question.
RULE = "majority"


def measure(corpus: str, *, cache_root: Path | None) -> dict[str, Any]:
    """Measure per-candidate and combined write exposure for the shipped premise."""
    dataset = load_real_world_dataset(corpus, cache_root=cache_root)
    fds = shipped_accept_all_fds(dataset.dirty_df)

    per_candidate: list[dict[str, Any]] = []
    for fd in fds:
        exposure = _write_exposure(dataset, (fd,), _schema_for(dataset.dirty_df, (fd,)))
        rule = exposure["by_rule"][RULE]
        per_candidate.append(
            {
                "dependency": _fd_label(fd),
                "holds_on_clean": fd_holds_on_clean(dataset.clean_df, fd),
                "writes": rule["proposals"],
                "repaired_a_real_error": rule["repaired_a_real_error"],
                "corrupted_a_clean_cell": rule["corrupted_a_clean_cell"],
            }
        )

    combined_exposure = _write_exposure(dataset, fds, _schema_for(dataset.dirty_df, fds))
    combined = combined_exposure["by_rule"][RULE]

    isolated_corruption = sum(item["corrupted_a_clean_cell"] for item in per_candidate)
    isolated_writes = sum(item["writes"] for item in per_candidate)
    harmful_alone = [item for item in per_candidate if item["corrupted_a_clean_cell"] > 0]
    false_deps = [item for item in per_candidate if not item["holds_on_clean"]]

    ranked = sorted(per_candidate, key=lambda item: -item["corrupted_a_clean_cell"])
    cumulative = 0
    concentration: dict[str, float | None] = {}
    for index, item in enumerate(ranked, start=1):
        cumulative += item["corrupted_a_clean_cell"]
        if index in (1, 3, 5):
            concentration[f"top_{index}_share_of_isolated_corruption"] = (
                round(cumulative / isolated_corruption, 4) if isolated_corruption else None
            )

    return {
        "schema": "dataforge_constraint_additivity_v1",
        "corpus": corpus,
        "rule": RULE,
        "rows": int(dataset.dirty_df.shape[0]),
        "dirty_sha256": dataset.dirty_sha256,
        "note": (
            "Per-candidate write exposure measured with a schema containing that dependency ALONE, "
            "against the same measurement with all of them. Overlapping dependencies mask one "
            "another because only one acts per cell, so the sum over candidates exceeds the "
            "combined result. Pre-registration not required: this measures a property of the "
            "harness's own arithmetic, with no threshold and no decision attached."
        ),
        "per_candidate": ranked,
        "summary": {
            "candidates": len(per_candidate),
            "false_dependencies": len(false_deps),
            "harmful_in_isolation": len(harmful_alone),
            "isolated_corruption_sum": isolated_corruption,
            "combined_corruption": combined["corrupted_a_clean_cell"],
            "non_additivity_factor": (
                round(isolated_corruption / combined["corrupted_a_clean_cell"], 2)
                if combined["corrupted_a_clean_cell"]
                else None
            ),
            "isolated_writes_sum": isolated_writes,
            "combined_writes": combined["proposals"],
            "combined_repaired": combined["repaired_a_real_error"],
            **concentration,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="hospital")
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()

    payload = measure(args.corpus, cache_root=args.cache_root)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summary = payload["summary"]
    print(f"wrote {args.artifact}")
    print(
        f"  {summary['candidates']} candidates, {summary['false_dependencies']} false, "
        f"{summary['harmful_in_isolation']} harmful in isolation"
    )
    print(
        f"  corruption: {summary['isolated_corruption_sum']} summed alone vs "
        f"{summary['combined_corruption']} accepted together "
        f"= {summary['non_additivity_factor']}x non-additive"
    )
    print(
        f"  concentration: top 1 = {summary.get('top_1_share_of_isolated_corruption')}, "
        f"top 5 = {summary.get('top_5_share_of_isolated_corruption')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
