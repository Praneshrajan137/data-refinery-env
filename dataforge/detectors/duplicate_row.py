"""Detector for exact duplicate rows.

Flags rows that are byte-for-byte duplicates of an earlier row. Each duplicate
occurrence after the first is reported (on the first column) as a
``duplicate_row`` issue for review. It is detection-only: removing rows is a
destructive operation the safety constitution forbids auto-applying, so no
repairer is registered - duplicates are surfaced, never silently deleted.

The detector is pure: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, row_count


class DuplicateRowDetector:
    """Flags exact duplicate rows (detection-only; never auto-deletes).

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"a": ["1", "2", "1"], "b": ["x", "y", "x"]})
        >>> issues = DuplicateRowDetector().detect(df)
        >>> issues[0].row
        2
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect exact duplicate rows."""
        columns = column_names(df)
        if not columns:
            return []
        first_column = columns[0]
        seen: dict[tuple[str, ...], int] = {}
        issues: list[Issue] = []
        for row_idx in range(row_count(df)):
            signature = tuple(str(df.at[row_idx, col]) for col in columns)
            original = seen.get(signature)
            if original is None:
                seen[signature] = row_idx
                continue
            issues.append(
                Issue(
                    row=row_idx,
                    column=first_column,
                    issue_type="duplicate_row",
                    severity=Severity.REVIEW,
                    confidence=0.95,
                    actual=signature[0],
                    reason=(
                        f"Row {row_idx} is an exact duplicate of row {original}; "
                        "review before removing (deletion is never auto-applied)."
                    ),
                )
            )
        return issues
