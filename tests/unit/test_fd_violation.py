"""Unit tests for dataforge.detectors.fd_violation.

Derived from SPEC_detectors.md Appendix A, Cases A.7–A.8.
Written FIRST (TDD red phase) before the implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.detectors.base import (
    FunctionalDependency,
    Schema,
    Severity,
)
from dataforge.detectors.fd_violation import FDViolationDetector


@pytest.fixture()
def detector() -> FDViolationDetector:
    """Fresh detector instance per test."""
    return FDViolationDetector()


def _schema_with_fd(determinant: list[str], dependent: str) -> Schema:
    """Helper to build a Schema with a single FD."""
    return Schema(
        functional_dependencies=[FunctionalDependency(determinant=determinant, dependent=dependent)]
    )


class TestFDViolationSpecCases:
    """Spec Appendix A toy cases."""

    def test_a7_zip_city_violation(self, detector: FDViolationDetector) -> None:
        """Case A.7: zip '10001' maps to two different cities."""
        df = pd.DataFrame(
            {
                "zip_code": ["10001", "10001", "90210", "90210"],
                "city": ["New York", "Manhattan", "Beverly Hills", "Beverly Hills"],
            }
        )
        schema = _schema_with_fd(["zip_code"], "city")
        issues = detector.detect(df, schema)

        # Both rows 0 and 1 are in the violating group
        assert len(issues) == 2
        assert all(i.issue_type == "fd_violation" for i in issues)
        assert all(i.severity == Severity.UNSAFE for i in issues)
        assert all(i.column == "city" for i in issues)
        rows = {i.row for i in issues}
        assert rows == {0, 1}

    def test_a8_consistent_mapping_no_issues(self, detector: FDViolationDetector) -> None:
        """Case A.8: provider_id maps consistently to hospital."""
        df = pd.DataFrame(
            {
                "provider_id": ["P1", "P1", "P2", "P2"],
                "hospital": ["General", "General", "St. Mary", "St. Mary"],
            }
        )
        schema = _schema_with_fd(["provider_id"], "hospital")
        issues = detector.detect(df, schema)
        assert issues == []


class TestFDViolationEdgeCases:
    """Edge cases beyond spec toy cases."""

    def test_no_schema_returns_empty(self, detector: FDViolationDetector) -> None:
        """Without a schema, no FDs to check — return empty."""
        df = pd.DataFrame({"a": ["1", "1"], "b": ["x", "y"]})
        issues = detector.detect(df, schema=None)
        assert issues == []

    def test_schema_without_fds_returns_empty(self, detector: FDViolationDetector) -> None:
        """Schema exists but has no FDs declared."""
        df = pd.DataFrame({"a": ["1", "1"], "b": ["x", "y"]})
        schema = Schema(columns={"a": "str", "b": "str"})
        issues = detector.detect(df, schema)
        assert issues == []

    def test_single_row_group_trivially_satisfied(self, detector: FDViolationDetector) -> None:
        """Single-row groups trivially satisfy any FD."""
        df = pd.DataFrame(
            {
                "code": ["A", "B", "C"],
                "name": ["Alpha", "Beta", "Gamma"],
            }
        )
        schema = _schema_with_fd(["code"], "name")
        issues = detector.detect(df, schema)
        assert issues == []

    def test_multiple_fds_checked(self, detector: FDViolationDetector) -> None:
        """Multiple FDs are all checked independently."""
        df = pd.DataFrame(
            {
                "zip": ["10001", "10001", "90210"],
                "city": ["NY", "Manhattan", "LA"],
                "state": ["NY", "NY", "CA"],
            }
        )
        schema = Schema(
            functional_dependencies=[
                FunctionalDependency(determinant=["zip"], dependent="city"),
                FunctionalDependency(determinant=["zip"], dependent="state"),
            ]
        )
        issues = detector.detect(df, schema)

        # zip -> city: violated (rows 0, 1)
        city_issues = [i for i in issues if i.column == "city"]
        assert len(city_issues) == 2

        # zip -> state: satisfied (both "10001" rows have "NY")
        state_issues = [i for i in issues if i.column == "state"]
        assert len(state_issues) == 0

    def test_missing_column_in_dataframe_skipped(self, detector: FDViolationDetector) -> None:
        """If declared FD references a column not in the DataFrame, skip it."""
        df = pd.DataFrame({"a": ["1", "2"]})
        schema = _schema_with_fd(["a"], "nonexistent")
        issues = detector.detect(df, schema)
        assert issues == []

    def test_null_values_in_determinant(self, detector: FDViolationDetector) -> None:
        """Null values in determinant column — rows with nulls are excluded."""
        df = pd.DataFrame(
            {
                "zip": ["10001", None, "10001", "10001"],
                "city": ["NY", "Somewhere", "NY", "NY"],
            }
        )
        schema = _schema_with_fd(["zip"], "city")
        issues = detector.detect(df, schema)
        # "10001" consistently maps to "NY" — no violation
        assert issues == []

    def test_composite_determinant(self, detector: FDViolationDetector) -> None:
        """Composite determinant: (state, city) -> zip_code."""
        df = pd.DataFrame(
            {
                "state": ["NY", "NY", "NY", "CA"],
                "city": ["NYC", "NYC", "Albany", "LA"],
                "zip": ["10001", "10002", "12207", "90001"],
            }
        )
        schema = _schema_with_fd(["state", "city"], "zip")
        issues = detector.detect(df, schema)

        # (NY, NYC) -> {10001, 10002}: violation
        zip_issues = [i for i in issues if i.column == "zip"]
        assert len(zip_issues) == 2
        rows = {i.row for i in zip_issues}
        assert rows == {0, 1}

    def test_confidence_is_set(self, detector: FDViolationDetector) -> None:
        """FD violation confidence should be high (structural error)."""
        df = pd.DataFrame(
            {
                "code": ["A", "A"],
                "name": ["Alpha", "Beta"],
            }
        )
        schema = _schema_with_fd(["code"], "name")
        issues = detector.detect(df, schema)
        assert len(issues) == 2
        assert all(i.confidence >= 0.9 for i in issues)

    def test_reason_is_descriptive(self, detector: FDViolationDetector) -> None:
        """Reason string mentions the FD and the conflicting values."""
        df = pd.DataFrame(
            {
                "zip": ["10001", "10001"],
                "city": ["NY", "Manhattan"],
            }
        )
        schema = _schema_with_fd(["zip"], "city")
        issues = detector.detect(df, schema)
        assert len(issues) == 2
        for issue in issues:
            assert "zip" in issue.reason.lower() or "10001" in issue.reason
