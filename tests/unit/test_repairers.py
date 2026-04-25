"""Unit tests for Week 2 repairers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd

from dataforge.detectors.base import FunctionalDependency, Issue, Schema, Severity
from dataforge.repairers.decimal_shift import DecimalShiftRepairer
from dataforge.repairers.fd_violation import FDViolationRepairer
from dataforge.repairers.type_mismatch import TypeMismatchRepairer


class TestDecimalShiftRepairer:
    """Deterministic decimal-shift repair behavior."""

    def test_uses_detector_expected_value(self) -> None:
        df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
        issue = Issue(
            row=3,
            column="amount",
            issue_type="decimal_shift",
            severity=Severity.REVIEW,
            confidence=0.9,
            expected="102",
            actual="1020",
            reason="10x too large",
        )

        proposed = DecimalShiftRepairer().propose(issue, df, schema=None)

        assert proposed is not None
        assert proposed.fix.old_value == "1020"
        assert proposed.fix.new_value == "102"
        assert proposed.fix.detector_id == "decimal_shift"
        assert proposed.provenance == "deterministic"

    def test_missing_expected_returns_none(self) -> None:
        df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
        issue = Issue(
            row=3,
            column="amount",
            issue_type="decimal_shift",
            severity=Severity.REVIEW,
            confidence=0.9,
            actual="1020",
            reason="10x too large",
        )

        assert DecimalShiftRepairer().propose(issue, df, schema=None) is None


class TestTypeMismatchRepairer:
    """Deterministic type-mismatch repair behavior."""

    def test_common_missing_sentinel_is_normalized_to_blank(self) -> None:
        df = pd.DataFrame(
            {"phone_number": ["2175550101", "3125550202", "not available", "6305551010"]}
        )
        issue = Issue(
            row=2,
            column="phone_number",
            issue_type="type_mismatch",
            severity=Severity.REVIEW,
            confidence=0.85,
            actual="not available",
            reason="Non-numeric value in numeric column",
        )

        proposed = TypeMismatchRepairer().propose(issue, df, schema=None)

        assert proposed is not None
        assert proposed.fix.old_value == "not available"
        assert proposed.fix.new_value == ""
        assert proposed.provenance == "deterministic"

    def test_numeric_value_in_string_column_returns_none(self) -> None:
        df = pd.DataFrame({"name": ["Alice", "Bob", "12345", "Diana"]})
        issue = Issue(
            row=2,
            column="name",
            issue_type="type_mismatch",
            severity=Severity.REVIEW,
            confidence=0.8,
            actual="12345",
            reason="Value '12345' looks numeric in predominantly string column 'name'",
        )

        assert TypeMismatchRepairer().propose(issue, df, schema=None) is None

    def test_sentinel_in_non_numeric_column_returns_none(self) -> None:
        df = pd.DataFrame({"status": ["active", "not available", "pending", "closed"]})
        issue = Issue(
            row=1,
            column="status",
            issue_type="type_mismatch",
            severity=Severity.REVIEW,
            confidence=0.8,
            actual="not available",
            reason="Non-numeric value in numeric column",
        )

        assert TypeMismatchRepairer().propose(issue, df, schema=None) is None


class TestFDViolationRepairer:
    """FD violation repair behavior, including cached LLM fallback."""

    def test_majority_rule_is_deterministic(self) -> None:
        df = pd.DataFrame({"code": ["A", "A", "A"], "name": ["Alpha", "Alpha", "Beta"]})
        schema = Schema(
            functional_dependencies=[FunctionalDependency(determinant=["code"], dependent="name")]
        )
        issue = Issue(
            row=2,
            column="name",
            issue_type="fd_violation",
            severity=Severity.UNSAFE,
            confidence=0.95,
            actual="Beta",
            reason="Functional dependency violated",
        )

        proposed = FDViolationRepairer(cache_dir=None, allow_llm=False).propose(issue, df, schema)

        assert proposed is not None
        assert proposed.fix.new_value == "Alpha"
        assert proposed.provenance == "deterministic"

    def test_tied_group_uses_cached_llm_on_second_run(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"code": ["A", "A"], "name": ["Alpha", "Beta"]})
        schema = Schema(
            functional_dependencies=[FunctionalDependency(determinant=["code"], dependent="name")]
        )
        issue = Issue(
            row=1,
            column="name",
            issue_type="fd_violation",
            severity=Severity.UNSAFE,
            confidence=0.95,
            actual="Beta",
            reason="Functional dependency violated",
        )
        repairer = FDViolationRepairer(
            cache_dir=tmp_path / ".dataforge" / "cache",
            allow_llm=True,
            model="gemini-2.0-flash",
        )
        mock_complete = AsyncMock(return_value='{"chosen_value": "Alpha"}')

        with patch("dataforge.repairers.fd_violation.complete", mock_complete):
            first = repairer.propose(issue, df, schema)
            second = repairer.propose(issue, df, schema)

        assert first is not None
        assert second is not None
        assert first.provenance == "llm_live"
        assert second.provenance == "llm_cache"
        assert first.fix.new_value == "Alpha"
        assert second.fix.new_value == "Alpha"
        assert mock_complete.await_count == 1

    def test_majority_value_for_same_row_returns_none(self) -> None:
        df = pd.DataFrame({"code": ["A", "A", "A"], "name": ["Alpha", "Alpha", "Beta"]})
        schema = Schema(
            functional_dependencies=[FunctionalDependency(determinant=["code"], dependent="name")]
        )
        issue = Issue(
            row=0,
            column="name",
            issue_type="fd_violation",
            severity=Severity.UNSAFE,
            confidence=0.95,
            actual="Alpha",
            reason="Functional dependency violated",
        )

        assert (
            FDViolationRepairer(cache_dir=None, allow_llm=False).propose(issue, df, schema) is None
        )

    def test_missing_fd_columns_return_none(self) -> None:
        df = pd.DataFrame({"code": ["A", "A"], "name": ["Alpha", "Beta"]})
        schema = Schema(
            functional_dependencies=[
                FunctionalDependency(determinant=["missing"], dependent="name")
            ]
        )
        issue = Issue(
            row=0,
            column="name",
            issue_type="fd_violation",
            severity=Severity.UNSAFE,
            confidence=0.95,
            actual="Alpha",
            reason="Functional dependency violated",
        )

        assert (
            FDViolationRepairer(cache_dir=None, allow_llm=False).propose(issue, df, schema) is None
        )

    def test_invalid_llm_json_returns_none(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"code": ["A", "A"], "name": ["Alpha", "Beta"]})
        schema = Schema(
            functional_dependencies=[FunctionalDependency(determinant=["code"], dependent="name")]
        )
        issue = Issue(
            row=1,
            column="name",
            issue_type="fd_violation",
            severity=Severity.UNSAFE,
            confidence=0.95,
            actual="Beta",
            reason="Functional dependency violated",
        )
        repairer = FDViolationRepairer(
            cache_dir=tmp_path / ".dataforge" / "cache",
            allow_llm=True,
            model="gemini-2.0-flash",
        )

        with patch("dataforge.repairers.fd_violation.complete", AsyncMock(return_value="not-json")):
            assert repairer.propose(issue, df, schema) is None

    def test_deterministic_repairers_never_call_provider(self) -> None:
        df_decimal = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
        decimal_issue = Issue(
            row=3,
            column="amount",
            issue_type="decimal_shift",
            severity=Severity.REVIEW,
            confidence=0.9,
            expected="102",
            actual="1020",
            reason="10x too large",
        )
        df_type = pd.DataFrame(
            {"phone_number": ["2175550101", "3125550202", "not available", "6305551010"]}
        )
        type_issue = Issue(
            row=2,
            column="phone_number",
            issue_type="type_mismatch",
            severity=Severity.REVIEW,
            confidence=0.85,
            actual="not available",
            reason="Non-numeric value in numeric column",
        )

        with patch("dataforge.agent.providers.complete", new=AsyncMock()) as mock_complete:
            DecimalShiftRepairer().propose(decimal_issue, df_decimal, schema=None)
            TypeMismatchRepairer().propose(type_issue, df_type, schema=None)

        assert mock_complete.await_count == 0
