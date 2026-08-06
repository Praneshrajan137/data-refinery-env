"""Can a free, deterministic ranker match the paid LLM triager?

The API phase compared two *paid* scorers against each other and called the cheap control
a "free baseline". That control was the detector's own sort order over two coarse fields
(`severity`, `confidence`), and on hospital 10,261 of 10,373 cells share
``confidence = 0.95`` -- so its ROC-AUC of 0.488 measured a **near-constant feature**, not
the absence of free signal. On flights the same ordering scores 0.0201, which inverts to
~0.98 and beats the LLM's 0.514. Neither number licenses the conclusion that free methods
cannot rank.

This script builds the control that was missing: a logistic regression over per-cell
signals the detection pipeline **already computes and then discards** --

* per-cell detector agreement (how many detectors flagged it) and `tier`, both computed
  and thrown away by the merge in ``dataforge.detectors.run_all_detectors``;
* row- and column-level flag density;
* value frequency within its column, blankness, length, digit ratio;
* severity, confidence, and issue type.

**Evaluation is leave-one-dataset-out, never in-sample.** An in-sample number would
reproduce exactly the survivor bias this phase exists to remove, and would also hide the
only thing that matters in production: whether weights learned on other tables transfer to
a table the model has never seen. The gap between the in-sample and transfer scores is a
direct measurement of the transfer problem that conformal exchangeability also rests on.

Both detector regimes are evaluated, because ``eval/results/detector_queue_composition.json``
showed the flooded hospital queue that motivated paid triage exists only under inferred FD
constraints.

Free: no provider calls, no spend.

Run::

    python scripts/bench/compare_free_vs_llm_ranker.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.bench.ranking_metrics import precision_at_k, roc_auc  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors import default_detectors  # noqa: E402
from dataforge.detectors.base import Severity  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "free_vs_llm_ranker.json"
_SCHEMA = "dataforge_free_vs_llm_ranker_v1"
_DATASETS = ("hospital", "flights", "rayyan")

# Published LLM ranker ROC-AUC, for reference only (DECISIONS.md 2026-07-25 and
# eval/results/review_gate_probe.json). Measured under inferred constraints.
_LLM_REFERENCE = {"hospital": 0.9459, "flights": 0.514, "rayyan": 0.9545}

_SEVERITY_RANK = {Severity.UNSAFE: 2, Severity.REVIEW: 1, Severity.SAFE: 0}
_ESTABLISHED = {
    "missing_value",
    "type_mismatch",
    "format_violation",
    "outlier",
    "categorical_normalization",
}


def _severity_rank(severity: Any) -> int:
    """Map a severity to an ordinal, tolerating enum or string representations."""
    for member, rank in _SEVERITY_RANK.items():
        if severity == member or str(severity) == str(member):
            return rank
    return 0


def _collect(df: Any, schema: Any) -> dict[tuple[int, str], list[Any]]:
    """Return every issue per cell, WITHOUT the merge that discards agreement counts."""
    per_cell: dict[tuple[int, str], list[Any]] = {}
    for detector in default_detectors():
        for issue in detector.detect(df, schema):
            per_cell.setdefault((issue.row, issue.column), []).append(issue)
    return per_cell


def _features(
    df: Any, per_cell: dict[tuple[int, str], list[Any]]
) -> tuple[list[tuple[int, str]], np.ndarray, list[str]]:
    """Build the free feature matrix. Every column here costs zero provider calls."""
    row_flags = Counter(row for row, _ in per_cell)
    col_flags = Counter(column for _, column in per_cell)
    value_counts: dict[str, Counter[str]] = {}
    for column in df.columns:
        value_counts[column] = Counter(str(v) for v in df[column].tolist())

    issue_types = sorted({i.issue_type for issues in per_cell.values() for i in issues})
    names = [
        "detector_agreement",
        "tier_established",
        "severity_rank",
        "max_confidence",
        "mean_confidence",
        "row_flag_count",
        "col_flag_rate",
        "value_freq_norm",
        "value_is_blank",
        "value_len",
        "digit_ratio",
        "has_expected",
    ] + [f"type__{t}" for t in issue_types]

    keys: list[tuple[int, str]] = []
    rows: list[list[float]] = []
    total_rows = max(1, len(df))
    for (row, column), issues in per_cell.items():
        value = str(df[column].iloc[row])
        confidences = [i.confidence for i in issues]
        established = any(i.issue_type in _ESTABLISHED for i in issues)
        digits = sum(1 for ch in value if ch.isdigit())
        counts = value_counts[column]
        vector = [
            float(len(issues)),
            1.0 if established else 0.0,
            float(max(_severity_rank(i.severity) for i in issues)),
            float(max(confidences)),
            float(sum(confidences) / len(confidences)),
            float(row_flags[row]),
            float(col_flags[column]) / total_rows,
            float(counts[value]) / total_rows,
            1.0 if not value.strip() else 0.0,
            float(len(value)),
            float(digits) / max(1, len(value)),
            1.0 if any(i.expected is not None for i in issues) else 0.0,
        ]
        present = {i.issue_type for i in issues}
        vector.extend(1.0 if t in present else 0.0 for t in issue_types)
        keys.append((row, column))
        rows.append(vector)
    return keys, np.asarray(rows, dtype=float), names


def _align(matrix: np.ndarray, names: list[str], target: list[str]) -> np.ndarray:
    """Project a per-dataset matrix onto a shared feature space.

    Datasets surface different issue types, so the one-hot blocks differ. Transfer is only
    meaningful over the shared schema; missing columns become zeros.
    """
    index = {name: position for position, name in enumerate(names)}
    out = np.zeros((matrix.shape[0], len(target)), dtype=float)
    for position, name in enumerate(target):
        if name in index:
            out[:, position] = matrix[:, index[name]]
    return out


def _baseline_scores(
    per_cell: dict[tuple[int, str], list[Any]], keys: list[tuple[int, str]]
) -> list[float]:
    """Reproduce the existing 'free baseline': detector severity then confidence."""
    return [
        max(_severity_rank(i.severity) for i in per_cell[key])
        + max(i.confidence for i in per_cell[key])
        for key in keys
    ]


def _effort_curve(pairs: list[tuple[float, bool]]) -> list[dict[str, float]]:
    """Return the curve a user actually cares about: yield per unit of review effort.

    ROC-AUC is base-rate invariant, which makes it the right metric for *comparing
    scorers* and the wrong one for answering "if I review N cells, how many real errors do
    I find?". That question is base-rate dependent, so it must be computed at the natural
    rate on the full queue -- which is exactly what an enriched sample cannot give back.
    """
    total_positives = sum(1 for _s, ok in pairs if ok)
    if not pairs or not total_positives:
        return []
    ordered = sorted(pairs, key=lambda pair: -pair[0])
    curve: list[dict[str, float]] = []
    for fraction in (0.01, 0.05, 0.10, 0.20, 0.50):
        k = max(1, round(fraction * len(ordered)))
        found = sum(1 for _s, ok in ordered[:k] if ok)
        curve.append(
            {
                "effort_fraction": fraction,
                "cells_reviewed": k,
                "true_errors_found": found,
                "precision_at_k": round(found / k, 4),
                "recall_at_k": round(found / total_positives, 4),
                "cells_per_true_error": round(k / found, 3) if found else None,
            }
        )
    return curve


def main() -> int:
    """Fit and evaluate the free ranker in both regimes, leave-one-dataset-out."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    regimes: dict[str, Any] = {}
    for regime in ("default", "inferred_constraints"):
        prepared: dict[str, Any] = {}
        for name in _DATASETS:
            dataset = load_real_world_dataset(name)
            df = dataset.dirty_df.copy(deep=True)
            schema = None
            if regime == "inferred_constraints":
                schema = infer_schema(df.copy(deep=True)).to_schema(
                    include_inferred_constraints=True
                )
            per_cell = _collect(df, schema)
            keys, matrix, names = _features(df, per_cell)
            truth = {(c.row, c.column) for c in dataset.ground_truth}
            labels = np.asarray([1 if key in truth else 0 for key in keys], dtype=int)
            prepared[name] = {
                "keys": keys,
                "matrix": matrix,
                "names": names,
                "labels": labels,
                "baseline": _baseline_scores(per_cell, keys),
            }

        shared = sorted(set.intersection(*(set(p["names"]) for p in prepared.values())))
        results: dict[str, Any] = {}
        for held_out in _DATASETS:
            train = [n for n in _DATASETS if n != held_out]
            x_train = np.vstack(
                [_align(prepared[n]["matrix"], prepared[n]["names"], shared) for n in train]
            )
            y_train = np.concatenate([prepared[n]["labels"] for n in train])
            x_test = _align(prepared[held_out]["matrix"], prepared[held_out]["names"], shared)
            y_test = prepared[held_out]["labels"]

            if len(set(y_train)) < 2 or len(set(y_test)) < 2:
                results[held_out] = {"skipped": "degenerate labels"}
                continue

            scaler = StandardScaler().fit(x_train)
            model = LogisticRegression(max_iter=2000, class_weight="balanced")
            model.fit(scaler.transform(x_train), y_train)
            transfer = model.predict_proba(scaler.transform(x_test))[:, 1]

            # In-sample upper bound: fit and score the SAME dataset. Not a usable number --
            # its only purpose is to size the transfer gap.
            in_scaler = StandardScaler().fit(x_test)
            in_model = LogisticRegression(max_iter=2000, class_weight="balanced")
            in_model.fit(in_scaler.transform(x_test), y_test)
            in_sample = in_model.predict_proba(in_scaler.transform(x_test))[:, 1]

            pairs_transfer = [(float(s), bool(t)) for s, t in zip(transfer, y_test, strict=True)]
            pairs_in = [(float(s), bool(t)) for s, t in zip(in_sample, y_test, strict=True)]
            pairs_base = [
                (float(s), bool(t))
                for s, t in zip(prepared[held_out]["baseline"], y_test, strict=True)
            ]
            top = max(1, round(0.1 * len(pairs_transfer)))
            results[held_out] = {
                "n_cells": len(pairs_transfer),
                "positives": int(y_test.sum()),
                "free_transfer_roc_auc": round(roc_auc(pairs_transfer), 4),
                "free_in_sample_roc_auc": round(roc_auc(pairs_in), 4),
                "existing_baseline_roc_auc": round(roc_auc(pairs_base), 4),
                "llm_reference_roc_auc": _LLM_REFERENCE.get(held_out),
                "free_transfer_precision_at_top_10pct": round(
                    precision_at_k(pairs_transfer, top), 4
                ),
                "natural_precision": round(float(y_test.mean()), 4),
                "transfer_gap": round(roc_auc(pairs_in) - roc_auc(pairs_transfer), 4),
                # Natural-rate, full-queue effort curves: the user-facing metric.
                "effort_curve_free_transfer": _effort_curve(pairs_transfer),
                "effort_curve_existing_baseline": _effort_curve(pairs_base),
                "unranked_cells_per_true_error": (
                    round(len(pairs_transfer) / int(y_test.sum()), 3) if y_test.sum() else None
                ),
            }
            r = results[held_out]
            print(
                f"{regime:21s} {held_out:9s} n={r['n_cells']:<6d} pos={r['positives']:<5d} "
                f"free_transfer={r['free_transfer_roc_auc']:.4f} "
                f"free_in_sample={r['free_in_sample_roc_auc']:.4f} "
                f"old_baseline={r['existing_baseline_roc_auc']:.4f} "
                f"llm={r['llm_reference_roc_auc']}"
            )
        regimes[regime] = {"shared_features": shared, "leave_one_dataset_out": results}

    payload = {
        "schema": _SCHEMA,
        "question": (
            "Can a free deterministic ranker over signals the pipeline already computes "
            "match the paid LLM triager, and do its weights transfer across datasets?"
        ),
        "method": (
            "Logistic regression, class_weight=balanced, standardised features, evaluated "
            "LEAVE-ONE-DATASET-OUT. free_in_sample_roc_auc is reported only to size the "
            "transfer gap and must never be quoted as achievable performance."
        ),
        "llm_reference_note": (
            "llm_reference_roc_auc is quoted from DECISIONS.md 2026-07-25 and "
            "eval/results/review_gate_probe.json. Those runs used inferred constraints, "
            "top-200 candidate slices, and row caps of 1500 on flights/rayyan, so they are "
            "NOT strictly comparable to these full-queue numbers. Treat as orientation, "
            "not as a paired comparison."
        ),
        "regimes": regimes,
    }
    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"\nartifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
