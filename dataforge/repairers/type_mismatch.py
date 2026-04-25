"""Deterministic repairer for conservative type-mismatch fixes."""

from __future__ import annotations

import re

import pandas as pd

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RetryContext
from dataforge.transactions.txn import CellFix

_NUMERIC_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
_MISSING_SENTINELS = frozenset(
    {"n/a", "na", "null", "none", "nan", "not available", "unknown", "-", ""}
)


def _looks_numeric(value: str) -> bool:
    """Return whether a string parses like a number."""
    return bool(_NUMERIC_RE.match(value.strip()))


def _is_predominantly_numeric(series: pd.Series) -> bool:
    """Return whether the non-empty series is mostly numeric strings."""
    normalized = [str(value).strip() for value in series if str(value).strip()]
    if not normalized:
        return False
    numeric_count = sum(1 for value in normalized if _looks_numeric(value))
    return numeric_count / len(normalized) >= 0.65


class TypeMismatchRepairer:
    """Repair a narrow, deterministic subset of type-mismatch issues."""

    def propose(
        self,
        issue: Issue,
        df: pd.DataFrame,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Normalize common numeric-column sentinel values to blank cells."""
        del schema, retry_context
        if issue.issue_type != "type_mismatch":
            return None
        if issue.row >= len(df.index) or issue.column not in df.columns:
            return None

        old_value = str(df.at[issue.row, issue.column])
        normalized_old = old_value.strip().lower()
        if normalized_old not in _MISSING_SENTINELS:
            return None
        if not _is_predominantly_numeric(df[issue.column]):
            return None
        if old_value == "":
            return None

        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value="",
                detector_id="type_mismatch",
            ),
            reason=(
                f"Normalize sentinel value '{old_value}' to a blank cell in "
                f"predominantly numeric column '{issue.column}'."
            ),
            confidence=issue.confidence,
            provenance="deterministic",
        )
