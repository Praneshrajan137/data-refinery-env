"""Label every mined functional dependency against ground truth, and evaluate two corrections.

Why this script exists
----------------------
``docs/trust/bypass-allowlist-evidence.md`` measured that premise quality, not the write gate,
determines corruption: **0** already-correct cells overwritten under a premise whose
dependencies all hold, **86** under the product's own mined premise, and all 25 sampled
corruptions traced to dependencies that are **false** on ground truth -- 23 of them to
``ZipCode -> HospitalName``.

The miner's own precision had never been measured. This script measures it on every
dirty/clean corpus, and evaluates the two corrections pre-registered in
``eval/preregistration/premise_quality.md`` as **counterfactuals on the mined candidate set**,
so the kill criterion can be applied before any product code changes.

The two corrections, neither of which introduces a free parameter
----------------------------------------------------------------
**C1, majority baseline.** ``confidence = 1 - g3`` measures how well ``X`` predicts ``Y``. The
trivial predictor that ignores ``X`` and always emits ``Y``'s modal value has error
``1 - majority_share(Y)``. A dependency that cannot beat that is evidence that ``Y`` is
skewed, not that a dependency exists. Require ``confidence > majority_share(dependent)``.

This generalises the rule the project's own bench oracle already applies -- a constant
dependent has ``majority_share == 1.0``, which no confidence can exceed -- rather than bolting
a new rule on. The product miner guards its *determinant* against triviality
(``_MAX_DETERMINANT_UNIQUE_FRACTION``, ``_MIN_FD_SUPPORT_GROUPS``) and has never guarded its
*dependent*.

**C2, tested denominator.** A violation can only occur inside a determinant group of two or
more rows; a singleton group is consistent with *any* dependent value and tests nothing. The
shipped formula divides violations by ``total_rows``, so every singleton inflates the score
without supplying evidence. Measure ``1 - violations / rows_in_multi_row_groups`` instead --
a correction to a denominator that was counting rows which cannot falsify the claim.

Ground truth
------------
A dependency is **true** when it holds with no exceptions on the CLEAN frame. That is a harsh
definition -- one violating cell in a thousand makes it false -- so ``fd_set_precision`` should
be read alongside the per-dependency table rather than on its own. It is nonetheless the right
label here, because the corrupting dependencies were not marginal: a zip code determining a
hospital name is wrong in kind, not by a cell.

``replication_mismatches`` must be zero. It recomputes each candidate's shipped confidence from
the frame and compares against the value the miner emitted, which is the check that the
counterfactuals are computed the same way the miner computes its own score.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402

#: The miner's retained absolute floor. Not tuned here; C2 is measured against the same value
#: the shipped code already uses, so the comparison isolates the denominator change.
_EMISSION_FLOOR = 0.9

#: Corpora with a row-aligned dirty/clean pair, so ground truth for a dependency exists.
CORPORA = ("hospital", "flights", "rayyan", "tax")


def _column_values(frame: pd.DataFrame, column: str) -> list[str]:
    return [str(value) for value in frame[column]]


def majority_share(frame: pd.DataFrame, column: str) -> float:
    """Return the fraction of rows holding the column's single most common value.

    This is the error floor of the trivial predictor that ignores the determinant entirely.
    """
    values = _column_values(frame, column)
    if not values:
        return 1.0
    return Counter(values).most_common(1)[0][1] / len(values)


def _groups(
    frame: pd.DataFrame, determinant: tuple[str, ...], dependent: str
) -> dict[str, list[str]]:
    """Group dependent values by the determinant key, mirroring the miner's grouping."""
    keys = [
        "\x1f".join(str(frame.iat[row, frame.columns.get_loc(col)]) for col in determinant)
        for row in range(len(frame))
    ]
    dependents = _column_values(frame, dependent)
    grouped: dict[str, list[str]] = defaultdict(list)
    for key, value in zip(keys, dependents, strict=True):
        grouped[key].append(value)
    return grouped


def dependency_statistics(
    frame: pd.DataFrame, determinant: tuple[str, ...], dependent: str
) -> dict[str, Any]:
    """Compute the shipped score and both counterfactual scores for one dependency."""
    total_rows = len(frame)
    grouped = _groups(frame, determinant, dependent)
    violations = sum(
        len(values) - Counter(values).most_common(1)[0][1] for values in grouped.values()
    )
    multi_row_groups = sum(1 for values in grouped.values() if len(values) >= 2)
    rows_in_multi_row_groups = sum(len(values) for values in grouped.values() if len(values) >= 2)

    shipped = round(1.0 - (violations / total_rows), 4) if total_rows else 0.0
    # C2: violations can only arise inside multi-row groups, so that is the denominator that
    # can actually falsify the claim. With no such group there is no evidence at all.
    tested = (
        round(1.0 - (violations / rows_in_multi_row_groups), 4) if rows_in_multi_row_groups else 0.0
    )
    return {
        "violations": violations,
        "groups": len(grouped),
        "multi_row_groups": multi_row_groups,
        "rows_in_multi_row_groups": rows_in_multi_row_groups,
        "tested_row_fraction": round(rows_in_multi_row_groups / total_rows, 4)
        if total_rows
        else 0.0,
        "shipped_confidence": shipped,
        "tested_confidence": tested,
    }


def fd_holds_on_clean(clean: pd.DataFrame, determinant: tuple[str, ...], dependent: str) -> bool:
    """Return whether the dependency holds with no exceptions on ground truth."""
    columns = set(clean.columns)
    if any(column not in columns for column in [*determinant, dependent]):
        return False
    grouped = clean.groupby(list(determinant), sort=False)[dependent]
    return int(grouped.nunique(dropna=False).max()) == 1


def measure(corpus: str, *, cache_root: Path | None) -> dict[str, Any]:
    """Label every mined dependency and score the incumbent against both counterfactuals."""
    dataset = load_real_world_dataset(corpus, cache_root=cache_root)
    dirty, clean = dataset.dirty_df, dataset.clean_df

    candidates = [
        candidate
        for candidate in infer_schema(dirty).candidates
        if candidate.kind == "functional_dependency" and candidate.dependent is not None
    ]

    replication_mismatches = 0
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        determinant = tuple(candidate.columns)
        dependent = str(candidate.dependent)
        stats = dependency_statistics(dirty, determinant, dependent)
        if stats["shipped_confidence"] != candidate.confidence:
            replication_mismatches += 1
        share = majority_share(dirty, dependent)
        rows.append(
            {
                "dependency": f"{' + '.join(determinant)} -> {dependent}",
                "holds_on_clean": fd_holds_on_clean(clean, determinant, dependent),
                "emitted_confidence": candidate.confidence,
                "majority_share_of_dependent": round(share, 4),
                # C1: must predict the dependent better than ignoring the determinant.
                "c1_beats_majority_baseline": candidate.confidence > share,
                # C2: the same floor the miner already applies, on the honest denominator.
                "c2_tested_confidence_clears_floor": stats["tested_confidence"] >= _EMISSION_FLOOR,
                **stats,
            }
        )

    return {
        "schema": "dataforge_premise_quality_v1",
        "corpus": corpus,
        "rows": int(len(dirty)),
        "dirty_sha256": dataset.dirty_sha256,
        "clean_sha256": dataset.clean_sha256,
        "emission_floor": _EMISSION_FLOOR,
        "replication_mismatches": replication_mismatches,
        "arms": _score_arms(rows),
        "dependencies": sorted(rows, key=lambda row: (row["holds_on_clean"], row["dependency"])),
    }


def _score_arms(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Score the incumbent and each counterfactual admission rule on the same candidates."""

    def _arm(name: str, admitted: list[dict[str, Any]]) -> dict[str, Any]:
        true_admitted = sum(1 for row in admitted if row["holds_on_clean"])
        false_admitted = len(admitted) - true_admitted
        return {
            "rule": name,
            "admitted": len(admitted),
            "admitted_true": true_admitted,
            "admitted_false": false_admitted,
            "fd_set_precision": round(true_admitted / len(admitted), 4) if admitted else None,
            "false_admitted_names": sorted(
                row["dependency"] for row in admitted if not row["holds_on_clean"]
            )[:25],
        }

    total_true = sum(1 for row in rows if row["holds_on_clean"])
    arms = {
        "incumbent": _arm("everything the miner emits today", rows),
        "c1_only": _arm(
            "C1: confidence > majority_share(dependent)",
            [row for row in rows if row["c1_beats_majority_baseline"]],
        ),
        "c2_only": _arm(
            "C2: tested_confidence >= floor",
            [row for row in rows if row["c2_tested_confidence_clears_floor"]],
        ),
        "c1_and_c2": _arm(
            "C1 and C2 together",
            [
                row
                for row in rows
                if row["c1_beats_majority_baseline"] and row["c2_tested_confidence_clears_floor"]
            ],
        ),
    }
    for arm in arms.values():
        # Recall over the true dependencies the miner found at all: the coverage cost of a rule.
        arm["true_dependencies_retained"] = (
            round(arm["admitted_true"] / total_true, 4) if total_true else None
        )
    arms["candidate_total"] = len(rows)  # type: ignore[assignment]
    arms["true_total"] = total_true  # type: ignore[assignment]
    return arms


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="hospital", choices=[*CORPORA])
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()

    payload = measure(args.corpus, cache_root=args.cache_root)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    arms = payload["arms"]
    print(
        f"corpus {payload['corpus']}  rows {payload['rows']}  "
        f"candidates {arms['candidate_total']}  true {arms['true_total']}  "
        f"replication_mismatches {payload['replication_mismatches']} (must be 0)"
    )
    print(f"  {'rule':<12}{'admitted':>9}{'true':>6}{'FALSE':>7}{'precision':>11}{'retained':>10}")
    for key in ("incumbent", "c1_only", "c2_only", "c1_and_c2"):
        arm = arms[key]
        print(
            f"  {key:<12}{arm['admitted']:>9}{arm['admitted_true']:>6}"
            f"{arm['admitted_false']:>7}{str(arm['fd_set_precision']):>11}"
            f"{str(arm['true_dependencies_retained']):>10}"
        )
    if arms["incumbent"]["false_admitted_names"]:
        print("  false dependencies the miner admits today:")
        for name in arms["incumbent"]["false_admitted_names"]:
            print(f"    {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
