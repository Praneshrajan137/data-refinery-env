"""Test H1: does any parameter-free function of the table separate true from false FDs?

Executes the H1 half of `eval/preregistration/premise_acquisition.md`. Read that document
before reading any number this produces -- in particular K1, which is the criterion that
makes H1 falsifiable rather than a posture.

## Why leave-one-table-out, and why it could not be done before

Four measures have been refused as gates, each after seeing the data. The objection to all of
them is identical: a threshold chosen on one corpus is indistinguishable from a memorised one.
`rwd` supplies ten annotated tables, which permits choosing a threshold on nine and testing it
on the tenth. That is validation rather than fitting, and it is the fold structure the
`DECISIONS.md` reversal criterion asked for in five separate entries.

**This harness deliberately gives the in-table hypothesis its best possible shot.** The
threshold is fitted openly, by maximising Youden's J on the training tables, because the point
is not to defend a refusal -- it is to find out whether one is warranted. If a measure survives
every fold, K1 fires, H1 is falsified, and the correct output is to ship that measure.

## What is imported rather than reimplemented

Candidate scoring comes from `measure_premise_quality_rwd`, which itself imports the measures
from `dataforge.premise_quality` -- the module the miner uses. Nothing here recomputes a
measure. `PRODUCT.md` records that a reimplementation of `_write_exposure` reported 959 writes
where the truth was 74; a shorter local loop is evidence against a measurement.

Read-only apart from one artifact under `eval/results/`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

MEASURES = ("mu_plus", "g3_prime", "confidence", "tested_confidence")
DEFAULT_INPUT = REPO / "eval" / "results" / "premise_quality_rwd.json"
DEFAULT_OUTPUT = REPO / "eval" / "results" / "premise_acquisition_h1.json"

# K5 in the pre-registration: below this many foldable tables, leave-one-table-out is not a
# validation and H1 must be reported as untested rather than confirmed.
K5_MIN_FOLDABLE_TABLES = 4


def _fit_threshold(scored: list[tuple[float, bool]]) -> dict[str, Any]:
    """Choose the threshold maximising Youden's J on the given (score, is_true) pairs.

    A gate admits a candidate when ``score > threshold``. J = TPR - FPR is used because it is
    symmetric in the two error types and needs no cost ratio, so nothing about the choice is
    tuned to make a particular conclusion come out. Ties break toward the LARGER threshold,
    which is the more conservative gate -- it admits fewer false dependencies, which is the
    direction a premise gate should err.

    Returns the threshold and the training-set behaviour, so a reader can see whether the fit
    was even good on the data it was fitted to.
    """
    positives = [score for score, is_true in scored if is_true]
    negatives = [score for score, is_true in scored if not is_true]
    if not positives or not negatives:
        return {"fitted": False, "reason": "a label class is empty in the training fold"}

    # Candidate thresholds: every observed score, plus one below the minimum so that
    # "admit everything" is reachable.
    candidates = sorted({score for score, _ in scored})
    candidates = [min(candidates) - 1.0, *candidates]

    best: tuple[float, float] | None = None
    for threshold in candidates:
        tpr = sum(1 for score in positives if score > threshold) / len(positives)
        fpr = sum(1 for score in negatives if score > threshold) / len(negatives)
        j = tpr - fpr
        if best is None or j > best[0] or (j == best[0] and threshold > best[1]):
            best = (j, threshold)

    assert best is not None
    j, threshold = best
    return {
        "fitted": True,
        "threshold": round(threshold, 6),
        "train_youden_j": round(j, 6),
        "train_true": len(positives),
        "train_false": len(negatives),
        "train_true_admitted": sum(1 for s in positives if s > threshold),
        "train_false_admitted": sum(1 for s in negatives if s > threshold),
    }


def _apply(threshold: float, scored: list[tuple[float, bool]]) -> dict[str, Any]:
    """Apply a gate ``score > threshold`` to a held-out table and report both error types."""
    positives = [score for score, is_true in scored if is_true]
    negatives = [score for score, is_true in scored if not is_true]
    discarded_true = [s for s in positives if s <= threshold]
    admitted_false = [s for s in negatives if s > threshold]
    return {
        "held_out_true": len(positives),
        "held_out_false": len(negatives),
        # The two numbers that decide the fold.
        "discarded_true": len(discarded_true),
        "admitted_false": len(admitted_false),
        # A fold is clean only when the gate loses no true dependency AND admits no false one.
        "separates": not discarded_true and not admitted_false,
        # Reported so a near-miss is visible rather than collapsed into a boolean.
        "min_true": round(min(positives), 6) if positives else None,
        "max_false": round(max(negatives), 6) if negatives else None,
    }


def main() -> int:
    """Run leave-one-table-out for every measure and emit the H1 verdict."""
    parser = argparse.ArgumentParser(description="Leave-one-table-out premise separation.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    if not args.input.exists():
        print(
            f"FAIL {args.input} is absent. Run scripts/bench/fetch_rwd_corpus.py then "
            f"scripts/bench/measure_premise_quality_rwd.py first.",
            file=sys.stderr,
        )
        return 1

    source = json.loads(args.input.read_text(encoding="utf-8"))
    tables = source["tables"]

    # (score, is_true) per measure per table, taken from the committed artifact.
    per_table: dict[str, dict[str, list[tuple[float, bool]]]] = {}
    for name, record in tables.items():
        if record.get("status") != "measured":
            continue
        rows = record["candidates"]
        has_true = any(row["annotated_true"] for row in rows)
        has_false = any(not row["annotated_true"] for row in rows)
        if not (has_true and has_false):
            # Not foldable: a single-class table can neither fit nor test.
            continue
        per_table[name] = {
            measure: [(float(row[measure]), bool(row["annotated_true"])) for row in rows]
            for measure in MEASURES
        }

    foldable = sorted(per_table)
    k5_satisfied = len(foldable) >= K5_MIN_FOLDABLE_TABLES

    results: dict[str, Any] = {}
    for measure in MEASURES:
        folds: dict[str, Any] = {}
        for held_out in foldable:
            train: list[tuple[float, bool]] = []
            for other in foldable:
                if other != held_out:
                    train.extend(per_table[other][measure])
            fit = _fit_threshold(train)
            if not fit.get("fitted"):
                folds[held_out] = {"status": "not_fitted", **fit}
                continue
            folds[held_out] = {
                "status": "tested",
                **fit,
                **_apply(float(fit["threshold"]), per_table[held_out][measure]),
            }

        tested = [f for f in folds.values() if f.get("status") == "tested"]
        clean = [f for f in tested if f["separates"]]
        lossless = [f for f in tested if f["discarded_true"] == 0]
        results[measure] = {
            "folds": folds,
            "folds_tested": len(tested),
            "folds_separating": len(clean),
            "folds_losing_no_true_dependency": len(lossless),
            "total_true_dependencies_discarded": sum(f["discarded_true"] for f in tested),
            # K1: H1 is falsified only by a measure that is clean on EVERY fold.
            "separates_on_every_fold": bool(tested) and len(clean) == len(tested),
        }

    falsifying = [m for m, r in results.items() if r["separates_on_every_fold"]]
    verdict = {
        "k5_min_foldable_tables": K5_MIN_FOLDABLE_TABLES,
        "foldable_tables": foldable,
        "foldable_table_count": len(foldable),
        "k5_satisfied": k5_satisfied,
        "measures_separating_on_every_fold": falsifying,
        # H1 says no measure does. K1 fires when one does.
        "h1_supported": k5_satisfied and not falsifying,
        "k1_fired": bool(falsifying),
        "status": (
            "untested_k5" if not k5_satisfied else ("k1_fired" if falsifying else "h1_supported")
        ),
    }

    payload = {
        "schema_version": "dataforge_premise_acquisition_h1_v1",
        "preregistration": "eval/preregistration/premise_acquisition.md",
        "source_artifact": str(args.input.relative_to(REPO)).replace("\\", "/"),
        "scope": source.get("scope", {}),
        "threshold_rule": (
            "maximise Youden's J (TPR - FPR) on the training tables; ties break toward the "
            "larger (more conservative) threshold. Fitted openly and deliberately: H1 is a "
            "claim that even a well-fitted threshold does not transfer."
        ),
        "gate_semantics": "a candidate is admitted when score > threshold",
        "verdict": verdict,
        "measures": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(f"Wrote {args.output}")
    print(f"Foldable tables: {len(foldable)} (K5 minimum {K5_MIN_FOLDABLE_TABLES})")
    for measure, record in results.items():
        print(
            f"  {measure:<18} clean folds {record['folds_separating']}/{record['folds_tested']}"
            f"  folds losing no true FD {record['folds_losing_no_true_dependency']}"
            f"  true FDs discarded {record['total_true_dependencies_discarded']}"
        )
    print(f"\nVERDICT: {verdict['status']}")
    if verdict["k1_fired"]:
        print(f"  K1 FIRED -- H1 falsified by: {falsifying}")
        print("  Ship that measure; C4 is unnecessary.")
    elif verdict["h1_supported"]:
        print("  No measure transfers across held-out tables. H1 stands.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
