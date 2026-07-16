"""Fit the post-hoc calibration map on REAL corrector samples and measure the
Expected Calibration Error before vs after, on a disjoint test split.

This answers the question a bigger model / more reasoning could not (prior
sessions: gpt-5-mini corrector ECE ~0.84-0.96): does post-hoc isotonic/Platt
calibration make the corrector's confidence an HONEST probability?

Honest caveat baked into the artifact: isotonic calibration is monotone, so it
rescales confidence WITHOUT changing the ranking of proposals. It therefore
lowers ECE (honest probabilities) but does NOT change the conformal-certifiable
auto-apply coverage (which depends only on the ranking). Certified coverage is
reported before AND after to make that explicit.

Usage:
    python scripts/bench/calibrate_corrector.py \
        --samples-json eval/results/corrector_gpt5mini_hospital_min.json \
        --output-json eval/results/corrector_calibration.json
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from dataforge.bench.error_classes import expected_calibration_error
from dataforge.calibration import calibrated_conformal_corrector_policy
from dataforge.calibration_map import calibrate_samples_by_class, fit_calibration_map_by_class
from dataforge.conformal import certified_coverage_report, split_by_class

LabeledSample = tuple[float, bool]


def _pool_samples(records: list[dict[str, object]], key: str) -> dict[str, list[LabeledSample]]:
    """Pool per-class (confidence, was_correct) pairs across all records."""
    pooled: dict[str, list[LabeledSample]] = defaultdict(list)
    for record in records:
        by_class = record.get(key) or {}
        if not isinstance(by_class, dict):
            continue
        for error_class, samples in by_class.items():
            for pair in samples:
                confidence, was_correct = pair
                pooled[str(error_class)].append((float(confidence), bool(was_correct)))
    return dict(pooled)


def _overall_ece(samples_by_class: Mapping[str, Sequence[LabeledSample]]) -> float:
    """ECE over all pooled samples (flattened across classes)."""
    flat: list[LabeledSample] = [pair for samples in samples_by_class.values() for pair in samples]
    return expected_calibration_error([(c, b) for c, b in flat])


def build_calibration_report(
    samples_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    method: str = "isotonic",
    seed: int = 0,
    calib_fraction: float = 0.5,
    alpha: float = 0.05,
    delta: float = 0.05,
    min_support: int = 30,
) -> dict[str, object]:
    """Fit maps on the calibration split; measure ECE + certified coverage on test."""
    calib, test = split_by_class(samples_by_class, seed=seed, calib_fraction=calib_fraction)
    maps = fit_calibration_map_by_class(calib, method=method, min_support=1)  # any support to fit
    test_calibrated = calibrate_samples_by_class(maps, test)

    ece_before = _overall_ece(test)
    ece_after = _overall_ece(test_calibrated)

    # Certified coverage is a ranking property -> reported before AND after to show
    # calibration does not manufacture coverage (monotone rescale).
    coverage_before = certified_coverage_report(
        samples_by_class, alpha=alpha, delta=delta, min_support=min_support, seed=seed
    )
    calibrated_full = calibrate_samples_by_class(maps, samples_by_class)
    coverage_after = certified_coverage_report(
        calibrated_full, alpha=alpha, delta=delta, min_support=min_support, seed=seed
    )

    per_class: dict[str, dict[str, float | int]] = {}
    for error_class, test_samples in test.items():
        cal_samples = test_calibrated.get(error_class, [])
        per_class[error_class] = {
            "test_n": len(test_samples),
            "ece_before": expected_calibration_error([(c, b) for c, b in test_samples]),
            "ece_after": expected_calibration_error([(c, b) for c, b in cal_samples]),
            "calibration_method": maps[error_class].method,
        }

    return {
        "method": method,
        "seed": seed,
        "calib_fraction": calib_fraction,
        "alpha": alpha,
        "delta": delta,
        "min_support": min_support,
        "overall_ece_before": ece_before,
        "overall_ece_after": ece_after,
        "certified_coverage_before": coverage_before["overall_test_coverage"],
        "certified_coverage_after": coverage_after["overall_test_coverage"],
        "per_class": per_class,
        "maps": {cls: cmap.model_dump() for cls, cmap in maps.items()},
        "note": (
            "Post-hoc calibration lowers ECE (honest probabilities) but is monotone, so "
            "it does not change conformal-certifiable auto-apply coverage; both are shown."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples-json", type=Path, required=True)
    parser.add_argument(
        "--output-json", type=Path, default=Path("eval/results/corrector_calibration.json")
    )
    parser.add_argument("--method", default="isotonic")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args(argv)

    doc = json.loads(args.samples_json.read_text(encoding="utf-8"))
    records = doc.get("records", []) if isinstance(doc, dict) else []
    samples_by_class = _pool_samples(records, "calibration_samples_by_class")
    samples_by_type = _pool_samples(records, "calibration_samples_by_type")

    model = next((str(r.get("model")) for r in records if r.get("model")), "unknown")
    report = build_calibration_report(samples_by_class, method=args.method, seed=args.seed)

    # CLI-consumable, engine-ready block: certify per-ISSUE_TYPE thresholds on
    # calibrated scores (issue_type == CellFix.detector_id, the key the engine uses
    # at auto-apply time). Falls back to by_class keying if no by_type samples.
    policy_samples = samples_by_type or samples_by_class
    policy, maps = calibrated_conformal_corrector_policy(policy_samples, method=args.method)

    artifact = {
        "provenance": {
            "source_json": str(args.samples_json),
            "model": model,
            "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "n_samples_by_class": {k: len(v) for k, v in samples_by_class.items()},
            "n_samples_by_type": {k: len(v) for k, v in samples_by_type.items()},
        },
        "ece_report": report,
        "policy": policy.model_dump(),
        "maps": {issue_type: cmap.model_dump() for issue_type, cmap in maps.items()},
        "reference_confidences": {
            issue_type: [conf for conf, _ in samples]
            for issue_type, samples in policy_samples.items()
        },
        "note": (
            "policy + maps are keyed by issue_type for the engine auto-apply gate; "
            "reference_confidences are the raw calibration-split confidences the policy "
            "was fit on, used by the engine PSI drift guard to downgrade auto-apply when "
            "the live distribution drifts; ece_report demonstrates the ECE-wall break on "
            "the ground-truth-class split."
        ),
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    print(f"overall ECE {report['overall_ece_before']} -> {report['overall_ece_after']}")
    print(
        f"certified coverage {report['certified_coverage_before']} -> "
        f"{report['certified_coverage_after']}"
    )
    print(f"certified thresholds (by issue_type): {policy.auto_apply_thresholds}")
    print(f"wrote {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
