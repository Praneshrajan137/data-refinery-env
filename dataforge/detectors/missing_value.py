"""Detector for missing values in predominantly-populated columns.

A blank or sentinel cell is only an *error* when the column is otherwise
populated (a mostly-empty column is legitimately sparse, not broken). This
detector flags missing/sentinel cells in columns whose populated rate clears a
threshold, or that a schema declares non-null.

Missing-value *correction* is intentionally limited: a correct value generally
cannot be invented. The companion repairer fills a value only when it is
derivable from a declared functional dependency; otherwise the issue is
detection-only (surfaced for review, never silently filled).

The detector is pure: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

# Values that read as missing.
_MISSING_SENTINELS = frozenset(
    {"", "n/a", "na", "null", "none", "nan", "nil", "-", "unknown", "not available", "?"}
)
# Minimum non-missing fraction for a column to be considered "populated".
_POPULATED_THRESHOLD = 0.5
# Minimum rows for the populated-rate estimate to be meaningful.
_MIN_VALUES = 8


def is_missing(value: str) -> bool:
    """Return whether a value reads as missing or a null sentinel."""
    return value.strip().lower() in _MISSING_SENTINELS


class MissingValueDetector:
    """Flags missing/sentinel cells in columns that are predominantly populated.

    Example:
        >>> import pandas as pd
        >>> detector = MissingValueDetector()
        >>> df = pd.DataFrame({"city": ["NY", "LA", "", "SF", "BOS", "DC", "LA", "NY"]})
        >>> issues = detector.detect(df)
        >>> issues[0].row
        2
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect missing-value issues across predominantly-populated columns."""
        non_null_columns = self._schema_non_null_columns(schema)
        issues: list[Issue] = []
        for col_name in column_names(df):
            issues.extend(self._check_column(df, str(col_name), non_null_columns))
        return issues

    @staticmethod
    def _schema_non_null_columns(schema: Schema | None) -> frozenset[str]:
        """Return columns a schema declares non-null, if any."""
        if schema is None:
            return frozenset()
        columns = getattr(schema, "not_null_columns", None) or getattr(schema, "non_null", None)
        if not columns:
            return frozenset()
        return frozenset(str(c) for c in columns)

    def _check_column(
        self, df: TableLike, col_name: str, non_null_columns: frozenset[str]
    ) -> list[Issue]:
        """Flag missing cells in one column when the column is populated."""
        values = [str(v) for v in column_values(df, col_name)]
        if len(values) < _MIN_VALUES:
            return []

        missing_rows = [i for i, v in enumerate(values) if is_missing(v)]
        if not missing_rows:
            return []
        populated = len(values) - len(missing_rows)
        populated_rate = populated / len(values)

        declared_non_null = col_name in non_null_columns
        if not declared_non_null and populated_rate < _POPULATED_THRESHOLD:
            return []  # legitimately sparse column

        confidence = 0.95 if declared_non_null else round(min(0.9, 0.4 + populated_rate / 2), 2)
        issues: list[Issue] = []
        for row_idx in missing_rows:
            issues.append(
                Issue(
                    row=row_idx,
                    column=col_name,
                    issue_type="missing_value",
                    severity=Severity.REVIEW,
                    confidence=confidence,
                    expected=None,
                    actual=values[row_idx],
                    reason=(
                        f"Missing value in column '{col_name}', which is "
                        f"{populated_rate:.0%} populated."
                    ),
                )
            )
        return issues
