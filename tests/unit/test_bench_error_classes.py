"""Unit tests for the per-error-class benchmark instrument."""

from __future__ import annotations

from dataforge.bench.core import BenchmarkRepair, SeedBenchmarkResult
from dataforge.bench.error_classes import (
    BENCH_ERROR_CLASSES,
    check_coverage_regression,
    class_coverage_matrix,
    classify_error_cell,
    expected_calibration_error,
    precision_at_auto_apply,
    score_repairs_by_class,
)
from dataforge.datasets.real_world import GroundTruthCell


class TestClassifier:
    def test_missing_value(self) -> None:
        assert classify_error_cell("", "42") == "missing_value"
        assert classify_error_cell("N/A", "Smith") == "missing_value"

    def test_numeric(self) -> None:
        assert classify_error_cell("1020.0", "102.0") == "numeric"
        assert classify_error_cell("5", "50") == "numeric"

    def test_datetime_format(self) -> None:
        assert classify_error_cell("2024/02/30", "2024-02-29") == "datetime_format"
        assert classify_error_cell("13/01/2024", "2024-01-13") == "datetime_format"

    def test_text_normalization_case_whitespace(self) -> None:
        assert classify_error_cell("  NewYork ", "newyork") == "text_normalization"
        assert classify_error_cell("Jon", "Jen") == "text_normalization"  # typo, dist 1

    def test_value_format_punctuation(self) -> None:
        # Same alphanumeric content, different formatting (phone).
        assert classify_error_cell("15551234567", "+1 (555) 123-4567") == "value_format"

    def test_every_class_is_canonical(self) -> None:
        for dirty, clean in [("", "x"), ("1", "2"), ("a", "completely different value")]:
            assert classify_error_cell(dirty, clean) in BENCH_ERROR_CLASSES


class TestPerClassScoring:
    def _gt(self) -> tuple[GroundTruthCell, ...]:
        return (
            GroundTruthCell(row=0, column="score", dirty_value="980", clean_value="98"),
            GroundTruthCell(row=1, column="score", dirty_value="1020", clean_value="102"),
            GroundTruthCell(row=2, column="name", dirty_value="", clean_value="Smith"),
        )

    def test_recall_is_per_class(self) -> None:
        # Fix only one of the two numeric decimal-shifts; miss the missing_value.
        repairs = [BenchmarkRepair(row=1, column="score", new_value="102", reason="x")]
        scores = score_repairs_by_class(self._gt(), repairs)
        assert scores["numeric"].support == 2
        assert scores["numeric"].tp == 1
        assert scores["numeric"].recall == 0.5
        assert scores["missing_value"].recall == 0.0

    def test_precision_on_class_counts_only_class_cells(self) -> None:
        # A correct numeric fix plus a spurious fix on a non-error cell.
        repairs = [
            BenchmarkRepair(row=1, column="score", new_value="102", reason="x"),
            BenchmarkRepair(row=9, column="score", new_value="999", reason="spurious"),
        ]
        scores = score_repairs_by_class(self._gt(), repairs)
        # Spurious cell is not class-attributable; numeric precision stays 1.0.
        assert scores["numeric"].precision_on_class == 1.0
        assert scores["numeric"].predicted_on_class == 1


class TestCalibration:
    def test_ece_zero_for_perfect_calibration(self) -> None:
        # Confidence 1.0 always correct, 0.0 always wrong -> perfectly calibrated.
        samples = [(1.0, True)] * 10 + [(0.0, False)] * 10
        assert expected_calibration_error(samples) == 0.0

    def test_ece_detects_overconfidence(self) -> None:
        samples = [(0.9, False)] * 10  # confident but always wrong
        assert expected_calibration_error(samples) > 0.5

    def test_precision_at_auto_apply(self) -> None:
        samples = [(True, True), (True, False), (False, True)]
        assert precision_at_auto_apply(samples) == 0.5

    def test_precision_at_auto_apply_vacuous(self) -> None:
        assert precision_at_auto_apply([(False, True), (False, False)]) == 1.0


class TestCoverageGate:
    def _record(self, by_class) -> SeedBenchmarkResult:  # noqa: ANN001
        return SeedBenchmarkResult(
            method="heuristic",
            dataset="flights",
            seed=0,
            status="ok",
            precision=0.0,
            recall=0.0,
            f1=0.0,
            tp=0,
            fp=0,
            fn=0,
            reproduction_command="dataforge bench --quick",
            by_class=by_class,
        )

    def test_matrix_and_gate_detect_zero_recall(self) -> None:
        scores = score_repairs_by_class(
            (GroundTruthCell(row=0, column="d", dirty_value="13/2024", clean_value="2024-01-13"),),
            [],  # no repairs -> datetime recall 0
        )
        record = self._record(scores)
        matrix = class_coverage_matrix([record])
        assert ("heuristic", "flights") in matrix
        passed, failures = check_coverage_regression(
            [record], {"heuristic/flights": {"datetime_format": 0.6}}
        )
        assert passed is False
        assert any("datetime_format" in f for f in failures)


class TestDetectionScoring:
    def test_detection_recall_credits_flagging_without_correct_value(self) -> None:
        gt = (
            GroundTruthCell(row=0, column="city", dirty_value="", clean_value="NY"),
            GroundTruthCell(row=1, column="city", dirty_value="", clean_value="LA"),
        )
        # Both cells detected, but no repairs proposed (value not derivable).
        scores = score_repairs_by_class(gt, [], detected_cells={(0, "city"), (1, "city")})
        mv = scores["missing_value"]
        assert mv.detection_recall == 1.0  # both flagged
        assert mv.recall == 0.0  # neither corrected
        assert mv.support == 2

    def test_detection_gate_floor(self) -> None:
        gt = (GroundTruthCell(row=0, column="city", dirty_value="", clean_value="NY"),)
        scores = score_repairs_by_class(gt, [], detected_cells=set())  # not detected
        record = SeedBenchmarkResult(
            method="heuristic",
            dataset="flights",
            seed=0,
            status="ok",
            precision=0.0,
            recall=0.0,
            f1=0.0,
            tp=0,
            fp=0,
            fn=0,
            reproduction_command="dataforge bench --quick",
            by_class=scores,
        )
        passed, failures = check_coverage_regression(
            [record], {"heuristic/flights": {"missing_value@detection": 0.9}}
        )
        assert passed is False
        assert any("missing_value@detection" in f for f in failures)


class TestCommittedThresholds:
    def test_coverage_floors_file_is_well_formed(self) -> None:
        import json
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        path = root / "eval" / "thresholds" / "coverage_floors.json"
        assert path.is_file(), "committed coverage floors file must exist"
        payload = json.loads(path.read_text(encoding="utf-8"))
        floors = payload["floors"]
        assert floors, "at least one floor must be defined"
        for key, class_floors in floors.items():
            method, sep, dataset = key.partition("/")
            assert sep and method and dataset, f"floor key must be method/dataset: {key}"
            for error_class, min_recall in class_floors.items():
                base_class = error_class.partition("@")[0]
                assert base_class in BENCH_ERROR_CLASSES, f"unknown class {error_class}"
                assert 0.0 <= float(min_recall) <= 1.0
