"""Detector for numeric outliers beyond power-of-10 decimal shifts.

The decimal-shift detector catches values that are a clean power-of-10 multiple
of the column median. This detector catches the *other* numeric anomalies - a
value far from the column's robust center by a modified z-score - and reports
them for review. It is detection-only: a robust outlier flag does not imply a
derivable correct value, so no repairer is registered for it.

The detector is pure: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

from statistics import median

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

_MIN_VALUES = 12
# Modified z-score (MAD-based) threshold. 3.5 is the Iglewicz-Hoaglin recommendation.
_MAD_THRESHOLD = 3.5


def _parse_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "").strip())
    except (TypeError, ValueError):
        return None


class OutlierDetector:
    """Flags numeric values far from a column's robust center (detection-only).

    Uses the median absolute deviation (MAD) modified z-score, which is robust
    to the very outliers it is detecting. Power-of-10 shifts are intentionally
    left to the decimal-shift detector (which owns those cells at tier 0).

    Example:
        >>> import pandas as pd
        >>> col = [str(x) for x in [10, 11, 9, 12, 10, 11, 13, 9, 10, 12, 11, 4200]]
        >>> issues = OutlierDetector().detect(pd.DataFrame({"v": col}))
        >>> issues[0].row
        11
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect numeric outliers across numeric columns."""
        issues: list[Issue] = []
        for col_name in column_names(df):
            issues.extend(self._check_column(df, str(col_name)))
        return issues

    def _check_column(self, df: TableLike, col_name: str) -> list[Issue]:
        """Flag MAD-outlier values in one numeric column."""
        parsed: list[tuple[int, float]] = []
        total = 0
        for row_idx, raw in enumerate(column_values(df, col_name)):
            text = str(raw).strip()
            if not text:
                continue
            total += 1
            value = _parse_float(text)
            if value is not None:
                parsed.append((row_idx, value))

        # Require a predominantly numeric column with enough values.
        if len(parsed) < _MIN_VALUES or len(parsed) < 0.9 * total:
            return []

        values = [v for _, v in parsed]
        center = median(values)
        deviations = [abs(v - center) for v in values]
        mad = median(deviations)
        if mad == 0:
            return []  # degenerate spread; do not flag

        issues: list[Issue] = []
        for row_idx, value in parsed:
            modified_z = 0.6745 * (value - center) / mad
            if abs(modified_z) <= _MAD_THRESHOLD:
                continue
            confidence = round(min(0.9, 0.6 + (abs(modified_z) - _MAD_THRESHOLD) / 20), 2)
            issues.append(
                Issue(
                    row=row_idx,
                    column=col_name,
                    issue_type="outlier",
                    severity=Severity.REVIEW,
                    confidence=confidence,
                    actual=str(value),
                    reason=(
                        f"Value {value:g} is a robust outlier in column '{col_name}' "
                        f"(modified z-score {modified_z:.1f}, median {center:g})."
                    ),
                )
            )
        return issues
