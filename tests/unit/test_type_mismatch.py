"""Unit tests for dataforge.detectors.type_mismatch.

Derived from SPEC_detectors.md Appendix A, Cases A.1–A.3.
Written FIRST (TDD red phase) before the implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.detectors.base import Severity
from dataforge.detectors.type_mismatch import TypeMismatchDetector


@pytest.fixture()
def detector() -> TypeMismatchDetector:
    """Fresh detector instance per test."""
    return TypeMismatchDetector()


class TestTypeMismatchSpecCases:
    """Spec Appendix A toy cases."""

    def test_a1_numeric_value_in_string_column(self, detector: TypeMismatchDetector) -> None:
        """Case A.1: pure-numeric '12345' among string names."""
        df = pd.DataFrame({"name": ["Alice", "Bob", "12345", "Diana"]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 2
        assert issues[0].column == "name"
        assert issues[0].issue_type == "type_mismatch"
        assert issues[0].severity == Severity.REVIEW
        assert issues[0].confidence >= 0.7
        assert "12345" in issues[0].actual

    def test_a2_string_value_in_numeric_column(self, detector: TypeMismatchDetector) -> None:
        """Case A.2: 'N/A' in a predominantly numeric column."""
        df = pd.DataFrame({"age": ["25", "30", "N/A", "40"]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 2
        assert issues[0].column == "age"
        assert issues[0].issue_type == "type_mismatch"
        assert issues[0].severity == Severity.REVIEW
        assert issues[0].confidence >= 0.8
        assert "N/A" in issues[0].actual

    def test_a3_clean_column_no_issues(self, detector: TypeMismatchDetector) -> None:
        """Case A.3: all-numeric column produces no issues."""
        df = pd.DataFrame({"score": ["95", "87", "92", "78"]})
        issues = detector.detect(df)
        assert issues == []


class TestTypeMismatchEdgeCases:
    """Edge cases beyond spec toy cases."""

    def test_all_null_column_no_issues(self, detector: TypeMismatchDetector) -> None:
        """Column with all NaN/None should not produce issues."""
        df = pd.DataFrame({"x": [None, None, None]})
        issues = detector.detect(df)
        assert issues == []

    def test_single_value_column_no_issues(self, detector: TypeMismatchDetector) -> None:
        """A single-value column has no minority type to flag."""
        df = pd.DataFrame({"x": ["hello"]})
        issues = detector.detect(df)
        assert issues == []

    def test_multiple_non_numeric_in_numeric_column(self, detector: TypeMismatchDetector) -> None:
        """If half the column is non-numeric, it's ambiguous — not type_mismatch."""
        df = pd.DataFrame({"val": ["10", "abc", "20", "def"]})
        # 50/50 split is not clear enough for a mismatch flag
        issues = detector.detect(df)
        assert issues == []

    def test_mixed_with_clear_majority(self, detector: TypeMismatchDetector) -> None:
        """One non-numeric in many numerics: should flag."""
        df = pd.DataFrame({"amount": ["100", "200", "300", "400", "500", "six hundred", "700"]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 5
        assert "six hundred" in issues[0].actual

    def test_common_sentinel_values(self, detector: TypeMismatchDetector) -> None:
        """Common sentinels like 'NA', 'null', '-' in numeric columns."""
        df = pd.DataFrame({"temp": ["22.5", "23.1", "null", "21.8", "24.0"]})
        issues = detector.detect(df)
        assert len(issues) == 1
        assert issues[0].row == 2
        assert "null" in issues[0].actual

    def test_date_string_in_non_date_column(self, detector: TypeMismatchDetector) -> None:
        """A date-formatted string among plain strings is flagged."""
        df = pd.DataFrame({"label": ["red", "blue", "2024-01-15", "green", "yellow"]})
        issues = detector.detect(df)
        assert len(issues) >= 1
        date_issues = [i for i in issues if i.row == 2]
        assert len(date_issues) == 1

    def test_multi_column_independent(self, detector: TypeMismatchDetector) -> None:
        """Each column is checked independently."""
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "12345"],
                "score": ["95", "87", "N/A"],
            }
        )
        issues = detector.detect(df)
        assert len(issues) == 2
        cols = {i.column for i in issues}
        assert cols == {"name", "score"}
