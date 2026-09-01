"""Measure mu+ and g3' against the rwd human-annotated FD ground truth.

Executes `eval/preregistration/premise_quality_measure.md`. Read that document and its
AMENDMENT 1 before reading any number this produces.

The corpus is `rwd` from Parciak, Vandevoort, Neven, Peeters and Vansummeren, *Measuring
Approximate Functional Dependencies: a Comparative Study*, ICDE 2024 (arXiv:2312.06296),
MIT-licensed, Zenodo record 8098909. It supplies two things this repository has never had:

- `ground_truth.csv` -- 143 dependencies annotated TRUE by hand across 10 real tables.
- `included_candidates.csv` -- the 1,170-candidate universe those annotations were made
  against, so **negatives are the authors' published closed world rather than ours.** The
  pre-registration declared a closed-world assumption over our own miner's output as a
  weakness; this is strictly better and the amendment records it.

Nothing here reimplements a measure. `mu_plus` and `g3_prime` are imported from
`dataforge.premise_quality`, the module the miner uses, because
`PRODUCT.md` records that a reimplementation of `_write_exposure` reported 959 writes where
the truth was 74. A shorter local loop is evidence against a measurement.

Read-only: downloads nothing, and writes exactly one artifact under `eval/results/`.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from dataforge.premise_quality import g3_prime, mu_plus  # noqa: E402

CORPUS_DIR = REPO / ".benchmarks" / "rwd"
OUTPUT = REPO / "eval" / "results" / "premise_quality_rwd.json"


def _normalize_column(name: str) -> str:
    """Map a table header to the form the published candidate list uses.

    Necessary, and worth naming because a silent mismatch here would have been the worst
    possible failure: the first run scored **0 candidates** on both `adult.csv` and
    `hospital.csv` and reported that as a clean result rather than as an error. `hospital`
    is the decisive corpus for P1 and P2, so a missing-column mismatch that reads as "no
    candidates" would have quietly removed the load-bearing evidence.

    Two different mismatches, one rule. `adult.csv` has leading spaces in its header
    (`' workclass'` against candidate `'workclass'`); `hospital.csv` writes
    `'Provider Number(String)'` where the candidates say `'ProviderNumberString'`. Removing
    whitespace and parentheses reconciles both without a per-table special case.
    """
    return name.replace("(", "").replace(")", "").replace(" ", "").strip()


def _read_table(path: Path) -> tuple[list[str], list[list[str]]]:
    """Return (normalized header, rows) for a CSV, tolerating real-world encoding damage."""
    with path.open(encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        header = [_normalize_column(column) for column in next(reader)]
        rows = [row for row in reader if len(row) == len(header)]
    return header, rows


def _score_candidate(
    header: list[str], rows: list[list[str]], lhs: str, rhs: str
) -> dict[str, Any] | None:
    """Compute both measures for one candidate, or None when a column is absent."""
    if lhs not in header or rhs not in header:
        return None
    lhs_index, rhs_index = header.index(lhs), header.index(rhs)
    groups: dict[str, list[str]] = defaultdict(list)
    dependent_values: list[str] = []
    for row in rows:
        determinant_value, dependent_value = row[lhs_index], row[rhs_index]
        groups[determinant_value].append(dependent_value)
        dependent_values.append(dependent_value)
    total = len(dependent_values)
    if total < 2:
        return None
    violations = 0
    for group_values in groups.values():
        counts: dict[str, int] = defaultdict(int)
        for value in group_values:
            counts[value] += 1
        violations += len(group_values) - max(counts.values())
    multi_row_rows = sum(len(g) for g in groups.values() if len(g) >= 2)
    return {
        "lhs": lhs,
        "rhs": rhs,
        "rows": total,
        "determinant_distinct": len(groups),
        "violations": violations,
        # The shipped miner's two incumbents, for a like-for-like comparison.
        "confidence": round(1.0 - violations / total, 4),
        "tested_confidence": (
            round(1.0 - violations / multi_row_rows, 4) if multi_row_rows else 0.0
        ),
        "mu_plus": round(mu_plus(groups, dependent_values), 6),
        "g3_prime": round(g3_prime(groups), 6),
    }


def _separation(true_scores: list[float], false_scores: list[float]) -> dict[str, Any]:
    """Report whether a measure separates the two label classes, and by how much.

    `separates_at_zero` is the only decision-relevant field: it asks whether the
    pre-registered threshold of 0 -- not a fitted one -- puts every annotated-true candidate
    above every annotated-false one. `min_true` and `max_false` are reported so a reader can
    see how close the call was rather than trusting a boolean.
    """
    if not true_scores or not false_scores:
        return {"comparable": False, "reason": "one label class is empty"}
    min_true, max_false = min(true_scores), max(false_scores)
    return {
        "comparable": True,
        "n_true": len(true_scores),
        "n_false": len(false_scores),
        "min_true": round(min_true, 6),
        "max_false": round(max_false, 6),
        "median_true": round(statistics.median(true_scores), 6),
        "median_false": round(statistics.median(false_scores), 6),
        # Perfect separation by SOME threshold. Not the pre-registered claim.
        "separates_perfectly": min_true > max_false,
        # The pre-registered claim: does a threshold of exactly 0 do the work?
        "true_above_zero": sum(1 for s in true_scores if s > 0.0),
        "false_at_zero": sum(1 for s in false_scores if s <= 0.0),
        "false_above_zero": sum(1 for s in false_scores if s > 0.0),
    }


def main() -> int:
    """Score every published candidate whose table is present locally."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    gt_path = CORPUS_DIR / "ground_truth.csv"
    cand_path = CORPUS_DIR / "included_candidates.csv"
    if not gt_path.exists() or not cand_path.exists():
        print(
            f"FAIL rwd corpus not present under {CORPUS_DIR}. This script measures a "
            f"downloaded external corpus and deliberately does not fetch it itself.",
            file=sys.stderr,
        )
        return 1

    truth = {
        (r["table"], r["lhs"], r["rhs"]) for r in csv.DictReader(gt_path.open(encoding="utf-8"))
    }
    candidates = list(csv.DictReader(cand_path.open(encoding="utf-8")))

    by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidates:
        by_table[row["table"]].append(row)

    per_table: dict[str, Any] = {}
    for table_name, table_candidates in sorted(by_table.items()):
        table_path = CORPUS_DIR / table_name
        if not table_path.exists():
            per_table[table_name] = {"status": "table_not_downloaded"}
            continue
        header, rows = _read_table(table_path)
        scored: list[dict[str, Any]] = []
        unresolved: list[str] = []
        for row in table_candidates:
            lhs = _normalize_column(row["lhs"])
            rhs = _normalize_column(row["rhs"])
            record = _score_candidate(header, rows, lhs, rhs)
            if record is None:
                unresolved.append(f"{row['lhs']} -> {row['rhs']}")
                continue
            record["annotated_true"] = (table_name, row["lhs"], row["rhs"]) in truth
            record["published_g3"] = round(float(row["g3"]), 6) if row.get("g3") else None
            scored.append(record)
        measures = {
            name: _separation(
                [r[name] for r in scored if r["annotated_true"]],
                [r[name] for r in scored if not r["annotated_true"]],
            )
            for name in ("mu_plus", "g3_prime", "confidence", "tested_confidence")
        }
        per_table[table_name] = {
            "status": "measured",
            "rows": len(rows),
            "columns": len(header),
            "candidates_scored": len(scored),
            "annotated_true": sum(1 for r in scored if r["annotated_true"]),
            # Reported, not swallowed. A table where most candidates fail to resolve is a
            # naming mismatch masquerading as a measurement.
            "candidates_unresolved": len(unresolved),
            "unresolved_examples": unresolved[:5],
            "separation": measures,
            "candidates": scored,
        }

    payload = {
        "schema_version": "dataforge_premise_quality_rwd_v1",
        "preregistration": "eval/preregistration/premise_quality_measure.md",
        "corpus": {
            "name": "rwd",
            "source": "Parciak et al., ICDE 2024, arXiv:2312.06296",
            "repository": "UHasselt-DSI-Data-Systems-Lab/paper-afd-comparative-study",
            "zenodo": "8098909",
            "license": "MIT",
            "negatives": (
                "the authors' published included_candidates.csv universe minus "
                "ground_truth.csv, NOT a closed-world assumption over this repository's "
                "own miner output"
            ),
        },
        "measures_imported_from": "dataforge.premise_quality",
        "tables": per_table,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(f"Wrote {args.output}")
    for table_name, record in sorted(per_table.items()):
        if record.get("status") != "measured":
            print(f"  {table_name}: {record.get('status')}")
            continue
        print(
            f"  {table_name}: {record['candidates_scored']} candidates, "
            f"{record['annotated_true']} annotated true"
        )
        for name, sep in record["separation"].items():
            if not sep.get("comparable"):
                continue
            print(
                f"      {name:<18} min_true={sep['min_true']:<10} "
                f"max_false={sep['max_false']:<10} perfect={sep['separates_perfectly']} "
                f"false_above_zero={sep['false_above_zero']}/{sep['n_false']}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
