"""Lock the real-data calibration result: post-hoc calibration_map must reduce the
Expected Calibration Error of the REAL committed gpt-5-mini corrector samples on a
disjoint test split -- the empirical proof that post-hoc calibration fixes the ECE
wall a bigger model could not. Also pins the honest caveat: monotone calibration
does not manufacture conformal-certifiable auto-apply coverage.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.bench.calibrate_corrector import _pool_samples, build_calibration_report

_REAL_SAMPLES = (
    Path(__file__).resolve().parents[2]
    / "eval"
    / "results"
    / "corrector_gpt5mini_hospital_min.json"
)


def _pooled() -> dict[str, list[tuple[float, bool]]]:
    doc = json.loads(_REAL_SAMPLES.read_text(encoding="utf-8"))
    return _pool_samples(doc["records"], "calibration_samples_by_class")


class TestRealDataCalibrationBreaksECEWall:
    def test_ece_drops_on_disjoint_test_split(self) -> None:
        report = build_calibration_report(_pooled(), method="isotonic", seed=0)
        # The raw gpt-5-mini corrector is badly miscalibrated (measured ~0.84-0.90).
        assert report["overall_ece_before"] > 0.3
        # Post-hoc isotonic calibration substantially reduces ECE on held-out data.
        assert report["overall_ece_after"] < report["overall_ece_before"]
        assert report["overall_ece_after"] <= 0.2

    def test_calibration_does_not_manufacture_certified_coverage(self) -> None:
        # Monotone calibration preserves ranking, so conformal-certifiable coverage
        # is unchanged. The corrector precision is too low to certify => stays 0.
        report = build_calibration_report(_pooled(), method="isotonic", seed=0)
        assert report["certified_coverage_after"] == report["certified_coverage_before"]

    def test_platt_does_not_worsen_ece(self) -> None:
        # Platt (parametric logistic) needs both classes in the calibration split;
        # on this degenerate low-precision slice it may fall back to identity, which
        # is honest. It must never INCREASE ECE.
        report = build_calibration_report(_pooled(), method="platt", seed=0)
        assert report["overall_ece_after"] <= report["overall_ece_before"]
