"""Unit tests for dataforge.detectors.decimal_shift.

Derived from SPEC_detectors.md Appendix A, Cases A.4–A.6.
Written FIRST (TDD red phase) before the implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.detectors.base import Severity
from dataforge.detectors.decimal_shift import DecimalShiftDetector


@pytest.fixture()
def detector() -> DecimalShiftDetector:
    """Fresh detector instance per test."""
    return DecimalShiftDetector()


class TestDecimalShiftSpecCases:
    """Spec Appendix A toy cases."""

    def test_a4_10x_outlier(self, detector: DecimalShiftDetector) -> None:
        """Case A.4: value 1020 is ~10x the median ~103."""
        df = pd.DataFrame({"price": [100.0, 105.0, 98.0, 1020.0, 103.0]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3
        assert issues[0].column == "price"
        assert issues[0].issue_type == "decimal_shift"
        assert issues[0].severity == Severity.REVIEW
        assert issues[0].confidence >= 0.8
        assert "1020" in issues[0].actual

    def test_a5_001x_outlier(self, detector: DecimalShiftDetector) -> None:
        """Case A.5: value 501 is ~0.01x the median ~49800."""
        df = pd.DataFrame({"salary": [50000, 48500, 52100, 501, 49800]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3
        assert issues[0].column == "salary"
        assert issues[0].issue_type == "decimal_shift"
        assert issues[0].confidence >= 0.8
        assert "501" in issues[0].actual

    def test_a6_no_outliers_uniform(self, detector: DecimalShiftDetector) -> None:
        """Case A.6: uniform column — no decimal shifts."""
        df = pd.DataFrame({"score": [88, 92, 85, 90, 87]})
        issues = detector.detect(df)
        assert issues == []


class TestDecimalShiftEdgeCases:
    """Edge cases beyond spec toy cases."""

    def test_too_few_values_no_issues(self, detector: DecimalShiftDetector) -> None:
        """Columns with fewer than 5 numeric values are skipped."""
        df = pd.DataFrame({"x": [100.0, 1000.0, 99.0]})
        issues = detector.detect(df)
        assert issues == []

    def test_all_same_values(self, detector: DecimalShiftDetector) -> None:
        """Column where all values are identical — no shift possible."""
        df = pd.DataFrame({"x": [50.0, 50.0, 50.0, 50.0, 50.0]})
        issues = detector.detect(df)
        assert issues == []

    def test_01x_shift(self, detector: DecimalShiftDetector) -> None:
        """A 0.1x shift: value is 10x too small."""
        df = pd.DataFrame({"weight": [70.0, 68.5, 72.1, 7.0, 69.8]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3
        assert "7.0" in issues[0].actual

    def test_100x_shift(self, detector: DecimalShiftDetector) -> None:
        """A 100x shift: value is 100x too large."""
        df = pd.DataFrame({"temp": [22.0, 21.5, 23.1, 2200.0, 22.8]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3

    def test_string_numeric_column(self, detector: DecimalShiftDetector) -> None:
        """String-typed column with numeric values — should still detect."""
        df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3

    def test_mixed_with_non_numeric_skipped(self, detector: DecimalShiftDetector) -> None:
        """Non-numeric values in column are skipped; numerics still checked."""
        df = pd.DataFrame({"val": ["100", "N/A", "98", "1020", "103", "105"]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3

    def test_negative_values(self, detector: DecimalShiftDetector) -> None:
        """Negative values — detect shifts in absolute magnitude."""
        df = pd.DataFrame({"delta": [-10.0, -12.0, -11.0, -110.0, -9.5]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 3

    def test_zero_median_handled(self, detector: DecimalShiftDetector) -> None:
        """Column with zero median should not crash or produce spurious issues."""
        df = pd.DataFrame({"x": [0, 0, 0, 0, 100]})
        # The 100 is an outlier but median is 0, so we can't compute ratio.
        # Should not crash.
        issues = detector.detect(df)
        # We don't assert count — just that it doesn't crash.
        assert isinstance(issues, list)

    def test_expected_value_suggested(self, detector: DecimalShiftDetector) -> None:
        """The expected field should contain the corrected value."""
        df = pd.DataFrame({"price": [100.0, 105.0, 98.0, 1020.0, 103.0]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].expected is not None
        # Expected should be ~102.0 (1020 / 10)
        expected_val = float(issues[0].expected)
        assert 90.0 <= expected_val <= 120.0
