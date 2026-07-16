"""Unit tests for dataforge.calibration_map - post-hoc probability calibration.

The corrector's confidence is a self-consistency agreement fraction with high
ECE. These tests pin the properties a calibration map must have to be trusted:
it is monotone (never re-ranks), it reduces ECE on miscalibrated input, it is
deterministic, it is leakage-free (fit on one split, applied to another), and it
degrades honestly to identity below a minimum support.
"""

from __future__ import annotations

import random

from dataforge.bench.error_classes import expected_calibration_error
from dataforge.calibration_map import (
    CalibrationMap,
    calibrate_samples_by_class,
    fit_calibration_map,
    fit_calibration_map_by_class,
    fit_isotonic,
    fit_platt,
)
from dataforge.conformal import LabeledSample


def _miscalibrated(n: int, seed: int = 0) -> list[LabeledSample]:
    """Overconfident generator: reported confidence c, true accuracy c**2.

    A proposal reporting confidence ``c`` is actually correct with probability
    ``c**2`` (systematically overconfident), so the raw scores have a large
    calibration gap that a good map should shrink.
    """
    rng = random.Random(seed)
    samples: list[LabeledSample] = []
    for _ in range(n):
        conf = round(rng.uniform(0.0, 1.0), 4)
        was_correct = rng.random() < conf**2
        samples.append((conf, was_correct))
    return samples


class TestIsotonicMonotonicity:
    def test_predictions_are_non_decreasing(self) -> None:
        calibration_map = fit_isotonic(_miscalibrated(400))
        grid = [i / 100 for i in range(101)]
        preds = [calibration_map.predict(x) for x in grid]
        assert all(b >= a - 1e-9 for a, b in zip(preds, preds[1:], strict=False))

    def test_predictions_stay_in_unit_interval(self) -> None:
        calibration_map = fit_isotonic(_miscalibrated(400, seed=3))
        for x in (-5.0, 0.0, 0.37, 1.0, 5.0):
            assert 0.0 <= calibration_map.predict(x) <= 1.0

    def test_empty_samples_fall_back_to_identity(self) -> None:
        calibration_map = fit_isotonic([])
        assert calibration_map.method == "identity"
        assert calibration_map.predict(0.42) == 0.42


class TestIsotonicReducesECE:
    def test_ece_drops_after_calibration(self) -> None:
        # Fit on calibration data, MEASURE ece on disjoint test data (leakage-free).
        calib = _miscalibrated(1500, seed=1)
        test = _miscalibrated(1500, seed=2)
        calibration_map = fit_isotonic(calib)

        raw_ece = expected_calibration_error(test)
        calibrated = [(calibration_map.predict(c), ok) for c, ok in test]
        calibrated_ece = expected_calibration_error(calibrated)

        assert raw_ece > 0.1  # the synthetic generator is badly miscalibrated
        assert calibrated_ece < raw_ece
        assert calibrated_ece < 0.05


class TestPlatt:
    def test_platt_reduces_ece_and_is_monotone(self) -> None:
        calib = _miscalibrated(1500, seed=5)
        test = _miscalibrated(1500, seed=6)
        calibration_map = fit_platt(calib)
        assert calibration_map.method == "platt"

        grid = [i / 100 for i in range(101)]
        preds = [calibration_map.predict(x) for x in grid]
        assert all(b >= a - 1e-9 for a, b in zip(preds, preds[1:], strict=False))

        raw_ece = expected_calibration_error(test)
        calibrated_ece = expected_calibration_error(
            [(calibration_map.predict(c), ok) for c, ok in test]
        )
        assert calibrated_ece < raw_ece

    def test_single_class_falls_back_to_identity(self) -> None:
        assert fit_platt([(0.9, True)] * 50).method == "identity"
        assert fit_platt([(0.3, False)] * 50).method == "identity"


class TestFitCalibrationMap:
    def test_below_min_support_is_identity(self) -> None:
        calibration_map = fit_calibration_map(_miscalibrated(10), min_support=30)
        assert calibration_map.method == "identity"

    def test_identity_method_is_identity(self) -> None:
        calibration_map = fit_calibration_map(_miscalibrated(200), method="identity")
        assert calibration_map.method == "identity"

    def test_default_method_is_isotonic(self) -> None:
        calibration_map = fit_calibration_map(_miscalibrated(200))
        assert calibration_map.method == "isotonic"


class TestDeterminismAndSerialization:
    def test_fit_is_deterministic(self) -> None:
        data = _miscalibrated(300, seed=9)
        a = fit_isotonic(data)
        b = fit_isotonic(data)
        assert a == b

    def test_round_trips_through_json(self) -> None:
        calibration_map = fit_isotonic(_miscalibrated(200))
        restored = CalibrationMap.model_validate_json(calibration_map.model_dump_json())
        assert restored == calibration_map
        assert restored.predict(0.5) == calibration_map.predict(0.5)


class TestPerClassCalibration:
    def test_low_support_class_is_identity_others_fit(self) -> None:
        samples_by_class = {
            "missing_value": _miscalibrated(200, seed=11),
            "value_format": _miscalibrated(5, seed=12),  # below support
        }
        maps = fit_calibration_map_by_class(samples_by_class, min_support=30)
        assert maps["missing_value"].method == "isotonic"
        assert maps["value_format"].method == "identity"

    def test_calibrate_samples_preserves_labels_and_count(self) -> None:
        samples_by_class = {"missing_value": _miscalibrated(120, seed=13)}
        maps = fit_calibration_map_by_class(samples_by_class, min_support=30)
        # Applying to a disjoint split (leakage-free) keeps labels and counts.
        test_by_class = {"missing_value": _miscalibrated(120, seed=14)}
        out = calibrate_samples_by_class(maps, test_by_class)
        assert len(out["missing_value"]) == 120
        assert [ok for _, ok in out["missing_value"]] == [
            ok for _, ok in test_by_class["missing_value"]
        ]
        assert all(0.0 <= c <= 1.0 for c, _ in out["missing_value"])

    def test_class_without_map_uses_identity(self) -> None:
        test_by_class = {"unseen": [(0.7, True), (0.2, False)]}
        out = calibrate_samples_by_class({}, test_by_class)
        assert out["unseen"] == [(0.7, True), (0.2, False)]


class TestIsotonicPoolingReferenceCase:
    def test_pava_pools_violators_into_monotone_fit(self) -> None:
        # Confidence 0.1 more accurate than 0.2 (a violation) must be pooled so
        # the fitted function is non-decreasing.
        samples: list[LabeledSample] = (
            [(0.1, True)] * 8
            + [(0.1, False)] * 2  # empirical 0.8
            + [(0.2, True)] * 2
            + [(0.2, False)] * 8  # empirical 0.2 (violates)
            + [(0.9, True)] * 9
            + [(0.9, False)] * 1  # empirical 0.9
        )
        calibration_map = fit_isotonic(samples)
        # The two low-confidence buckets pool to their combined mean (0.5).
        assert abs(calibration_map.predict(0.1) - 0.5) < 1e-9
        assert abs(calibration_map.predict(0.2) - 0.5) < 1e-9
        assert calibration_map.predict(0.9) > calibration_map.predict(0.2)
