"""Repairer for missing values via functional-dependency-derivable fills only.

A missing value cannot be invented. This repairer fills a missing cell only
when its value is *derivable* from a declared functional dependency: if some FD
``determinant -> this_column`` holds and another row shares the same determinant
values with a known (non-missing) value in this column, that value is proposed.
Otherwise it abstains (returns ``None``) - the missing value stays
detection-only. Every proposal still passes the SMT verifier and constitution.
"""

from __future__ import annotations

from typing import Any

from dataforge.detectors.base import Issue, Schema
from dataforge.detectors.missing_value import is_missing
from dataforge.repairers.base import ProposedFix, RetryContext
from dataforge.table import TableLike, cell_value, column_names, row_count
from dataforge.transactions.txn import CellFix


class MissingValueRepairer:
    """Fills missing values only when a functional dependency makes them derivable."""

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Propose an FD-derived fill, or abstain when the value is not derivable."""
        del retry_context
        if issue.issue_type != "missing_value" or schema is None:
            return None

        fds = getattr(schema, "functional_dependencies", None)
        if not fds:
            return None

        old_value = cell_value(df, issue.row, issue.column)
        columns = set(column_names(df))
        derived = self._derive_from_fds(df, issue.row, issue.column, fds, columns)
        if derived is None or is_missing(derived) or derived == old_value:
            return None

        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value=derived,
                detector_id="missing_value",
                operation="update",
            ),
            reason=(
                f"Filled missing value in '{issue.column}' from a functional "
                f"dependency match -> '{derived}'."
            ),
            confidence=issue.confidence,
            provenance="deterministic",
        )

    def _derive_from_fds(
        self,
        df: TableLike,
        row: int,
        column: str,
        fds: Any,
        columns: set[str],
    ) -> str | None:
        """Return a uniquely FD-derived value for the cell, or None."""
        candidates: set[str] = set()
        for fd in fds:
            dependent = str(getattr(fd, "dependent", ""))
            determinant = [str(c) for c in getattr(fd, "determinant", [])]
            if dependent != column or not determinant:
                continue
            if any(col not in columns for col in determinant):
                continue
            key = self._row_key(df, row, determinant)
            if key is None:
                continue  # determinant itself has a missing value; cannot match
            match = self._lookup(df, determinant, key, column)
            if match is not None:
                candidates.add(match)
        # Only fill when every matching FD agrees on a single value.
        return next(iter(candidates)) if len(candidates) == 1 else None

    @staticmethod
    def _row_key(df: TableLike, row: int, determinant: list[str]) -> tuple[str, ...] | None:
        """Return the determinant key for a row, or None if any part is missing."""
        values: list[str] = []
        for col in determinant:
            value = cell_value(df, row, col)
            if is_missing(value):
                return None
            values.append(value)
        return tuple(values)

    @staticmethod
    def _lookup(
        df: TableLike, determinant: list[str], key: tuple[str, ...], column: str
    ) -> str | None:
        """Find a non-missing dependent value for another row with the same key."""
        found: set[str] = set()
        for other in range(row_count(df)):
            if tuple(cell_value(df, other, col) for col in determinant) != key:
                continue
            value = cell_value(df, other, column)
            if not is_missing(value):
                found.add(value)
        return next(iter(found)) if len(found) == 1 else None
