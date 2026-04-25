"""Unit tests for dataforge.detectors.base — Issue, Severity, Schema, Detector protocol.

These tests are derived from SPEC_detectors.md Section 6.1–6.3.
Written FIRST (TDD red phase) before the implementation exists.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataforge.detectors.base import (
    AggregateDependency,
    Detector,
    DomainBound,
    FunctionalDependency,
    Issue,
    Schema,
    Severity,
)

# ── Severity enum ─────────────────────────────────────────────────────────


class TestSeverity:
    """Severity enum has exactly 3 members with correct ordering."""

    def test_has_three_members(self) -> None:
        assert len(Severity) == 3

    def test_members_are_safe_review_unsafe(self) -> None:
        assert Severity.SAFE.value == "safe"
        assert Severity.REVIEW.value == "review"
        assert Severity.UNSAFE.value == "unsafe"

    def test_ordering_safe_lt_review_lt_unsafe(self) -> None:
        """SAFE < REVIEW < UNSAFE for sorting (most severe first)."""
        assert Severity.SAFE < Severity.REVIEW < Severity.UNSAFE

    def test_ordering_le(self) -> None:
        """<= works for all combinations."""
        assert Severity.SAFE <= Severity.SAFE
        assert Severity.SAFE <= Severity.REVIEW
        assert not (Severity.UNSAFE <= Severity.SAFE)

    def test_ordering_gt(self) -> None:
        """> works for all combinations."""
        assert Severity.UNSAFE > Severity.REVIEW > Severity.SAFE
        assert not (Severity.SAFE > Severity.REVIEW)

    def test_ordering_ge(self) -> None:
        """>= works for all combinations."""
        assert Severity.UNSAFE >= Severity.UNSAFE
        assert Severity.UNSAFE >= Severity.REVIEW
        assert not (Severity.SAFE >= Severity.REVIEW)

    def test_ordering_with_non_severity_returns_not_implemented(self) -> None:
        """Comparing with non-Severity returns NotImplemented."""
        assert Severity.SAFE.__lt__(42) is NotImplemented
        assert Severity.SAFE.__le__(42) is NotImplemented
        assert Severity.SAFE.__gt__(42) is NotImplemented
        assert Severity.SAFE.__ge__(42) is NotImplemented

    def test_severity_from_string(self) -> None:
        assert Severity("safe") is Severity.SAFE
        assert Severity("review") is Severity.REVIEW
        assert Severity("unsafe") is Severity.UNSAFE


# ── Issue model ───────────────────────────────────────────────────────────


class TestIssue:
    """Issue Pydantic model validates fields correctly."""

    def test_valid_construction(self) -> None:
        issue = Issue(
            row=0,
            column="age",
            issue_type="type_mismatch",
            severity=Severity.REVIEW,
            confidence=0.9,
            actual="N/A",
            reason="Non-numeric value in numeric column",
        )
        assert issue.row == 0
        assert issue.column == "age"
        assert issue.issue_type == "type_mismatch"
        assert issue.severity == Severity.REVIEW
        assert issue.confidence == 0.9
        assert issue.expected is None
        assert issue.actual == "N/A"

    def test_valid_with_expected(self) -> None:
        issue = Issue(
            row=3,
            column="price",
            issue_type="decimal_shift",
            severity=Severity.REVIEW,
            confidence=0.85,
            expected="102.0",
            actual="1020.0",
            reason="Value is 10x the median",
        )
        assert issue.expected == "102.0"

    def test_invalid_issue_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Issue(
                row=0,
                column="a",
                issue_type="nonexistent_type",
                severity=Severity.SAFE,
                confidence=0.5,
                actual="x",
                reason="test",
            )

    def test_confidence_below_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Issue(
                row=0,
                column="a",
                issue_type="type_mismatch",
                severity=Severity.SAFE,
                confidence=-0.1,
                actual="x",
                reason="test",
            )

    def test_confidence_above_one_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Issue(
                row=0,
                column="a",
                issue_type="type_mismatch",
                severity=Severity.SAFE,
                confidence=1.1,
                actual="x",
                reason="test",
            )

    def test_negative_row_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Issue(
                row=-1,
                column="a",
                issue_type="type_mismatch",
                severity=Severity.SAFE,
                confidence=0.5,
                actual="x",
                reason="test",
            )

    def test_empty_column_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Issue(
                row=0,
                column="",
                issue_type="type_mismatch",
                severity=Severity.SAFE,
                confidence=0.5,
                actual="x",
                reason="test",
            )

    def test_empty_reason_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Issue(
                row=0,
                column="a",
                issue_type="type_mismatch",
                severity=Severity.SAFE,
                confidence=0.5,
                actual="x",
                reason="",
            )


# ── Schema model ──────────────────────────────────────────────────────────


class TestSchema:
    """Schema model parses column types and functional dependencies."""

    def test_minimal_schema(self) -> None:
        schema = Schema(columns={"a": "int", "b": "str"})
        assert schema.columns["a"] == "int"
        assert schema.functional_dependencies == ()

    def test_schema_with_fds(self) -> None:
        fd = FunctionalDependency(determinant=["zip_code"], dependent="city")
        schema = Schema(
            columns={"zip_code": "str", "city": "str"},
            functional_dependencies=[fd],
        )
        assert len(schema.functional_dependencies) == 1
        assert schema.functional_dependencies[0].determinant == ("zip_code",)
        assert schema.functional_dependencies[0].dependent == "city"

    def test_schema_with_week3_metadata(self) -> None:
        schema = Schema(
            columns={"amount": "float", "phone_number": "str"},
            pii_columns={"phone_number"},
            domain_bounds=[DomainBound(column="amount", min_value=0.0, max_value=5000.0)],
            aggregate_dependencies=[
                AggregateDependency(
                    source_column="amount",
                    aggregate="sum",
                    target_column="total_amount",
                    group_by=["order_id"],
                )
            ],
        )

        assert schema.pii_columns == frozenset({"phone_number"})
        assert schema.domain_bounds[0].column == "amount"
        assert schema.aggregate_dependencies[0].group_by == ("order_id",)

    def test_empty_determinant_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FunctionalDependency(determinant=[], dependent="city")

    def test_empty_dependent_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FunctionalDependency(determinant=["zip"], dependent="")


# ── Detector protocol ─────────────────────────────────────────────────────


class TestDetectorProtocol:
    """Detector protocol is structurally implementable."""

    def test_conforming_class_is_accepted(self) -> None:
        import pandas as pd

        class MyDetector:
            def detect(self, df: pd.DataFrame, schema: Schema | None = None) -> list[Issue]:
                return []

        detector: Detector = MyDetector()  # type: ignore[assignment]
        result = detector.detect(pd.DataFrame({"a": [1, 2, 3]}))
        assert result == []
