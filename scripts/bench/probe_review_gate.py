"""Probe whether the review-triager auto-fire gate is derivable from free signals.

Motivation: the LLM review triager is a measured win on some datasets and useless
on others (`eval/results/llm_review_ranker_*.json`). Firing it unconditionally
spends money where it adds nothing; never firing it wastes a real capability. The
product rule we wanted was "fire only when the free detector-confidence ranking
carries no information", using a runtime-observable property of the confidence
distribution so no ground truth is needed.

This probe tests that hypothesis. It is FREE -- detectors only, no LLM calls --
and it is committed because the result is a **negative**: dispersion does not
predict whether the free baseline is informative, so no honest auto-fire gate
exists on these signals. The triager therefore ships as an explicit opt-in.

Usage::

    python scripts/bench/probe_review_gate.py
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.datasets.real_world import (  # noqa: E402
    load_real_world_dataset,
    sample_dataset_rows,
)
from dataforge.detectors import run_all_detectors  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "review_gate_probe.json"
_SCHEMA = "dataforge_review_gate_probe_v1"
_RESULTS = ROOT / "eval" / "results"

# Row caps keep the probe fast; detector confidence distributions are stable
# under head-sampling, and the probe's claim is about distribution SHAPE.
_DATASETS = (("hospital", None), ("flights", 1500), ("rayyan", 1500))


def normalized_entropy(values: list[float]) -> float:
    """Return Shannon entropy of the confidence distribution, scaled to [0, 1].

    Zero means degenerate (one confidence value, so ranking by it is impossible);
    one means maximally spread.
    """
    counts = Counter(round(v, 3) for v in values)
    n = len(values)
    if n == 0 or len(counts) <= 1:
        return 0.0
    entropy = -sum((c / n) * math.log(c / n) for c in counts.values())
    return entropy / math.log(len(counts))


def _std(values: list[float]) -> float:
    """Return the population standard deviation."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    return (sum((v - mean) ** 2 for v in values) / n) ** 0.5


def _measured_outcome(dataset: str) -> dict[str, Any] | None:
    """Return the committed ranker outcome for a dataset, if present."""
    path = _RESULTS / f"llm_review_ranker_{dataset}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    if not records:
        return None
    record = records[0]
    return {
        "llm_roc_auc": record.get("roc_auc"),
        "baseline_roc_auc": record.get("baseline_roc_auc"),
        "queue_precision_lift": record.get("ranking_queue_precision_lift"),
        "baseline_precision_at_k": record.get("baseline_precision_at_k"),
    }


def main() -> int:
    """Measure the candidate gate signals and record the verdict."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entropy-threshold", type=float, default=0.20)
    args = parser.parse_args()

    findings: dict[str, Any] = {}
    for name, cap in _DATASETS:
        dataset = load_real_world_dataset(name)
        if cap is not None:
            dataset = sample_dataset_rows(dataset, cap)
        df = dataset.dirty_df.copy(deep=True)
        schema = infer_schema(df).to_schema(include_inferred_constraints=True)
        issues = run_all_detectors(df, schema=schema)
        confidences = [issue.confidence for issue in issues]
        entropy = normalized_entropy(confidences)
        outcome = _measured_outcome(name)

        # The hypothesis under test: a degenerate (low-entropy) confidence
        # distribution means the free ranking is uninformative, so fire the LLM.
        gate_would_fire = entropy <= args.entropy_threshold
        llm_actually_helps = None
        if outcome and outcome["queue_precision_lift"] is not None:
            # Use queue-precision LIFT, not an ROC-AUC difference. flights has a
            # pathological baseline AUC of 0.020 (confidence is anti-correlated
            # with error), so an AUC delta would score it as a huge win when the
            # queue it produces is actually no better -- lift 0.84, i.e. worse.
            llm_actually_helps = float(outcome["queue_precision_lift"]) > 1.0

        findings[name] = {
            "queue_size": len(confidences),
            "distinct_confidences": len(Counter(round(c, 3) for c in confidences)),
            "normalized_entropy": round(entropy, 4),
            "confidence_std": round(_std(confidences), 4),
            "most_common": [
                [k, v] for k, v in Counter(round(c, 3) for c in confidences).most_common(4)
            ],
            "gate_would_fire": gate_would_fire,
            "llm_actually_helps": llm_actually_helps,
            "gate_correct": (
                None if llm_actually_helps is None else gate_would_fire == llm_actually_helps
            ),
            "measured_outcome": outcome,
        }

    wrong = sorted(k for k, v in findings.items() if v["gate_correct"] is False)
    verdict = "NO_GO" if wrong else "GO"
    payload = {
        "schema": _SCHEMA,
        "hypothesis": (
            "A runtime-observable property of the detector-confidence distribution "
            "(normalized entropy) predicts whether the free baseline ranking is "
            "informative, and can therefore gate paid LLM triage automatically."
        ),
        "entropy_threshold": args.entropy_threshold,
        "findings": findings,
        "mispredicted_datasets": wrong,
        "verdict": verdict,
        "conclusion": (
            "NO-GO. rayyan has a well-spread confidence distribution (entropy 0.64) yet a "
            "chance-level baseline (ROC-AUC 0.54) that the LLM beats decisively (0.96). "
            "Dispersion therefore does NOT predict baseline informativeness: that property "
            "depends on whether confidence CORRELATES with correctness, which requires ground "
            "truth the product does not have at runtime. Shipping this gate would silently "
            "withhold a ~50x queue-precision lift on rayyan-like data. The triager ships as "
            "an explicit user opt-in instead, with queue size surfaced as cost guidance "
            "rather than an automatic decision."
        ),
    }
    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(f"{'dataset':10s} {'queue':>7s} {'entropy':>8s}  fire?  helps?  correct?")
    for name, data in findings.items():
        print(
            f"{name:10s} {data['queue_size']:7d} {data['normalized_entropy']:8.3f}  "
            f"{str(data['gate_would_fire']):5s}  {str(data['llm_actually_helps']):5s}   "
            f"{data['gate_correct']}"
        )
    print(f"\nVerdict: {verdict}" + (f" (mispredicts {', '.join(wrong)})" if wrong else ""))
    print(f"Artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
