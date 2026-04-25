"""Detector for decimal-shift anomalies in numeric columns.

Identifies values that are exact powers-of-10 multiples (10x, 100x, 0.1x,
0.01x, etc.) of the column's central tendency.  This is the canonical
"decimal point was moved" data-entry error pattern.

The detector is **pure**: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from dataforge.detectors.base import Issue, Schema, Severity

if TYPE_CHECKING:
    pass

# Minimum non-null numeric values required for meaningful statistics.
_MIN_COLUMN_SIZE = 5

# Powers of 10 to check.  Positive = value is N× too large;
# negative = value is N× too small.
_SHIFT_POWERS = (-3, -2, -1, 1, 2, 3)

# How close ratio must be to a power of 10 (in log10 space).
# 0.15 means we accept ratios within 10^±0.15 ≈ 0.71× – 1.41× of the
# exact power.  Tight enough to avoid false positives on natural variance.
_LOG_TOLERANCE = 0.15


def _try_float(value: object) -> float | None:
    """Attempt to parse a value as float, returning None on failure.

    Args:
        value: Any value (string, int, float, None, …).

    Returns:
        The float value or None if parsing fails.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


class DecimalShiftDetector:
    """Detects values that are power-of-10 multiples of the column distribution.

    For each numeric column, computes the median and checks every value
    to see if ``value / median`` is close to 10^k for k in {-3, -2, -1,
    1, 2, 3}.  Flagged values get an ``expected`` field with the corrected
    value (``value / 10^k``).

    Requires at least 5 non-null numeric values per column.  Columns with
    zero or near-zero median are handled gracefully.

    Example:
        >>> import pandas as pd
        >>> detector = DecimalShiftDetector()
        >>> df = pd.DataFrame({"price": [100.0, 105.0, 98.0, 1020.0, 103.0]})
        >>> issues = detector.detect(df)
        >>> issues[0].row
        3
    """

    def detect(self, df: pd.DataFrame, schema: Schema | None = None) -> list[Issue]:
        """Detect decimal-shift issues in the DataFrame.

        Args:
            df: The input DataFrame to analyze.
            schema: Optional declared schema (unused by this detector).

        Returns:
            A list of Issue objects for values that appear to be shifted
            by a power of 10 relative to the column distribution.
        """
        issues: list[Issue] = []

        for col_name in df.columns:
            col_issues = self._check_column(df, str(col_name))
            issues.extend(col_issues)

        return issues

    def _check_column(self, df: pd.DataFrame, col_name: str) -> list[Issue]:
        """Check a single column for decimal-shift outliers.

        Args:
            df: The DataFrame containing the column.
            col_name: Name of the column to check.

        Returns:
            Issues found in this column.
        """
        # Parse all values to float, keeping track of original indices.
        parsed: list[tuple[int, float, str]] = []
        for row_idx, val in enumerate(df[col_name].tolist()):
            fval = _try_float(val)
            if fval is not None:
                parsed.append((row_idx, fval, str(val)))

        if len(parsed) < _MIN_COLUMN_SIZE:
            return []

        values = np.array([v for _, v, _ in parsed])
        median = float(np.median(values))

        # If median is zero or very close, we cannot compute meaningful ratios.
        if abs(median) < 1e-10:
            return []

        issues: list[Issue] = []
        for row_idx, fval, str_val in parsed:
            if abs(fval) < 1e-10:
                continue

            ratio = fval / median
            if abs(ratio) < 1e-10:
                continue

            log_ratio = math.log10(abs(ratio))

            best_power: int | None = None
            best_distance = float("inf")

            for power in _SHIFT_POWERS:
                distance = abs(log_ratio - power)
                if distance < _LOG_TOLERANCE and distance < best_distance:
                    best_distance = distance
                    best_power = power

            if best_power is not None:
                correction_factor = 10.0**best_power
                expected_val = fval / correction_factor

                # Confidence: closer to exact power → higher confidence.
                confidence = round(min(0.95, max(0.70, 1.0 - best_distance * 2.0)), 2)

                if best_power > 0:
                    reason = (
                        f"Value {fval:g} in column '{col_name}' appears to be "
                        f"~{int(correction_factor)}x the typical value "
                        f"(median ~{median:g})"
                    )
                else:
                    reason = (
                        f"Value {fval:g} in column '{col_name}' appears to be "
                        f"~{1.0 / correction_factor:g}x too small compared to "
                        f"the typical value (median ~{median:g})"
                    )

                issues.append(
                    Issue(
                        row=row_idx,
                        column=col_name,
                        issue_type="decimal_shift",
                        severity=Severity.REVIEW,
                        confidence=confidence,
                        expected=f"{expected_val:g}",
                        actual=str_val.strip(),
                        reason=reason,
                    )
                )

        return issues
