"""Unit tests for repairer registry helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import dataforge.repairers as repairers
from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix


def _issue(*, issue_type: str, row: int, column: str, actual: str, reason: str) -> Issue:
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,  # type: ignore[arg-type]
        severity=Severity.REVIEW,
        confidence=0.9,
        actual=actual,
        reason=reason,
    )


def _fix(*, row: int, column: str, old_value: str, new_value: str, detector_id: str) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=row,
            column=column,
            old_value=old_value,
            new_value=new_value,
            detector_id=detector_id,
        ),
        reason="candidate",
        confidence=0.9,
        provenance="deterministic",
    )


class _NullRepairer:
    def propose(
        self,
        issue: Issue,
        df: pd.DataFrame,
        schema: Schema | None,
        retry_context: object | None = None,
    ) -> None:
        del issue, df, schema, retry_context


class _EchoRepairer:
    def propose(
        self,
        issue: Issue,
        df: pd.DataFrame,
        schema: Schema | None,
        retry_context: object | None = None,
    ) -> ProposedFix:
        del df, schema, retry_context
        return _fix(
            row=issue.row,
            column=issue.column,
            old_value=issue.actual,
            new_value="clean",
            detector_id=issue.issue_type,
        )


class TestRepairerRegistry:
    """Coverage for registry-level helper behavior."""

    def test_propose_fixes_skips_missing_none_and_duplicate_repairs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setattr(
            repairers,
            "build_repairers",
            lambda **_: {
                "decimal_shift": _NullRepairer(),
                "fd_violation": _EchoRepairer(),
            },
        )
        df = pd.DataFrame({"amount": ["bad"]})
        issues = [
            _issue(
                issue_type="type_mismatch",
                row=0,
                column="amount",
                actual="bad",
                reason="missing repairer",
            ),
            _issue(
                issue_type="decimal_shift",
                row=0,
                column="amount",
                actual="bad",
                reason="repairer returns none",
            ),
            _issue(
                issue_type="fd_violation",
                row=0,
                column="amount",
                actual="bad",
                reason="first candidate",
            ),
            _issue(
                issue_type="fd_violation",
                row=0,
                column="amount",
                actual="bad",
                reason="duplicate candidate",
            ),
        ]

        fixes = repairers.propose_fixes(
            issues,
            df,
            Schema(columns={"amount": "str"}),
            cache_dir=tmp_path,
        )

        assert len(fixes) == 1
        assert fixes[0].fix.new_value == "clean"
