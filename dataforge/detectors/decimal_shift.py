"""Detector for decimal-shift anomalies in numeric columns.

Identifies values that are exact powers-of-10 multiples (10x, 100x, 0.1x,
0.01x, etc.) of the column's central tendency.  This is the canonical
"decimal point was moved" data-entry error pattern.

**The rule is only sound on columns of NARROW dynamic range, and it now enforces
that rather than assuming it.** A fixed 10x offset from the median is evidence of a
misplaced decimal point only if 10x is unusual *for this column*. In a column whose
own values already span an order of magnitude, a value 10x from the median is
perfectly ordinary, and flagging it is a false positive.

That precondition was known and written down -- in the wrong place. The corruption
oracle (``tests/property/test_no_corruption_invariant.py``) generates its clean
numeric columns "clustered (low variance) so no correct cell is a decimal-shift
outlier", which made its no-false-positive invariant a property of the *fixture*
instead of the *code*. Real warehouse columns are not clustered: measured log-IQR is
0.44 dex for ``orders.o_totalprice``, 0.47 for ``lineitem.l_extendedprice``, 0.62 for
``customer.c_acctbal`` and 0.48 for ``QUERY_HISTORY.total_elapsed_time`` -- so a 1-dex
shift sits only 1.6-2.3 IQR units out and is not an outlier at all.

Measured cost of the missing check (2026-08-22, error-free data, so every flag is a
false positive by construction):

===============================================  ==========  =========
column                                           rewrites    rate
===============================================  ==========  =========
``lineitem.l_extendedprice``                         212,358     3.54%
``orders.o_totalprice``                               41,685     2.78%
``customer.c_acctbal``                                 9,385     6.26%
``QUERY_HISTORY.total_elapsed_time`` (real)             4,167     9.86%
===============================================  ==========  =========

Adding the dispersion gate below removes 98.1% of those (267,595 -> 5,141), and
**loses no true positives**: this detector found zero real errors on hospital (39
flags), flights (92) and rayyan (112).

The detector is **pure**: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

import math
from statistics import median

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

# Minimum non-null numeric values required for meaningful statistics.
#
# Left at 5. A first version of this fix raised it to 20 on the reasoning that five points
# cannot support a distributional claim about spread. That reasoning is sound but it was
# not *measured* -- every false-positive measurement behind this module used columns of
# 150,000 to 6,000,000 rows, where the minimum is irrelevant -- and it broke eight of this
# detector's own spec tests. Shipping an unmeasured threshold change bundled with a
# measured one would make the evidence for the whole fix harder to audit.
#
# It is also unnecessary: the dispersion gate below is safe at n=5 in both directions. A
# tightly-clustered 5-value column yields a small log-IQR, so a genuine 10x shift still
# clears the gate; a wide 5-value column yields a large one, so the gate abstains. The
# noise in a 5-point quartile estimate makes the gate *conservative*, not permissive.
_MIN_COLUMN_SIZE = 5

# Powers of 10 to check.  Positive = value is Nx too large;
# negative = value is Nx too small.
_SHIFT_POWERS = (-3, -2, -1, 1, 2, 3)

# How close ratio must be to a power of 10 (in log10 space).
# 0.15 means we accept ratios within 10^+-0.15 ~= 0.71x - 1.41x of the
# exact power.
_LOG_TOLERANCE = 0.15

# How many log-space inter-quartile ranges a value must sit from the median before a
# power-of-10 offset counts as anomalous rather than ordinary.
#
# This is the whole fix. Without it the rule asks "is this value ~10^k from the
# median?", which is true of a large slice of any wide-range column. With it the rule
# asks "is this value ~10^k from the median AND genuinely far out for this column?"
#
# 3.0 is the measured operating point: it removed 100% of the false positives on
# ``l_extendedprice``, 99.1% on ``total_elapsed_time`` and ~90% on ``o_totalprice`` and
# ``c_acctbal``. It is deliberately the same order as the conventional 3-sigma /
# 1.5-IQR family rather than fitted to make a number look good, and injected 10x errors
# on genuinely clustered columns clear it comfortably -- which is why the corruption
# oracle still passes.
_MIN_LOG_IQR_DISTANCE = 3.0

# Floor on the log-IQR used in the gate. A perfectly constant column has log-IQR 0,
# which would make the gate vacuous (any offset is infinitely many IQRs out). This
# floor keeps the gate meaningful there while staying far below any real column's
# spread, so it never weakens the check on the columns that matter.
_MIN_LOG_IQR = 0.02


def _log_iqr(values: list[float]) -> float:
    """Return the inter-quartile range of ``log10(|v|)``, floored at ``_MIN_LOG_IQR``.

    Log space, because a decimal shift is multiplicative: the question "is 10x unusual
    here?" is a question about ratios, and only log space makes it a question about
    distance. Inter-quartile rather than standard deviation because the sample may
    itself contain the shifted values this detector exists to find, and a variance
    estimate would be inflated by them.
    """
    logs = sorted(math.log10(abs(v)) for v in values if abs(v) > 1e-10)
    if len(logs) < 4:
        return _MIN_LOG_IQR
    q1 = logs[len(logs) // 4]
    q3 = logs[(3 * len(logs)) // 4]
    return max(_MIN_LOG_IQR, q3 - q1)


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

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect decimal-shift issues in the DataFrame.

        Args:
            df: The input DataFrame to analyze.
            schema: Optional declared schema (unused by this detector).

        Returns:
            A list of Issue objects for values that appear to be shifted
            by a power of 10 relative to the column distribution.
        """
        issues: list[Issue] = []

        for col_name in column_names(df):
            col_issues = self._check_column(df, str(col_name))
            issues.extend(col_issues)

        return issues

    def _check_column(self, df: TableLike, col_name: str) -> list[Issue]:
        """Check a single column for decimal-shift outliers.

        Args:
            df: The DataFrame containing the column.
            col_name: Name of the column to check.

        Returns:
            Issues found in this column.
        """
        # Parse all values to float, keeping track of original indices.
        parsed: list[tuple[int, float, str]] = []
        for row_idx, val in enumerate(column_values(df, col_name)):
            fval = _try_float(val)
            if fval is not None:
                parsed.append((row_idx, fval, str(val)))

        if len(parsed) < _MIN_COLUMN_SIZE:
            return []

        center = float(median([v for _, v, _ in parsed]))

        # If median is zero or very close, we cannot compute meaningful ratios.
        if abs(center) < 1e-10:
            return []

        # How wide is this column, multiplicatively? A power-of-10 offset is only
        # anomalous when it is large relative to the column's own spread.
        log_iqr = _log_iqr([v for _, v, _ in parsed])

        issues: list[Issue] = []
        for row_idx, fval, str_val in parsed:
            if abs(fval) < 1e-10:
                continue

            ratio = fval / center
            if abs(ratio) < 1e-10:
                continue

            log_ratio = math.log10(abs(ratio))

            # THE DISPERSION GATE. Skip values that are a power of ten from the median
            # but still ordinary for this column. Without this the detector rewrote
            # 263,428 correct monetary values on error-free TPC-H and 9.86% of real
            # query-history durations; see the module docstring for the measurements.
            if abs(log_ratio) <= _MIN_LOG_IQR_DISTANCE * log_iqr:
                continue

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
                        f"(median ~{center:g})"
                    )
                else:
                    reason = (
                        f"Value {fval:g} in column '{col_name}' appears to be "
                        f"~{1.0 / correction_factor:g}x too small compared to "
                        f"the typical value (median ~{center:g})"
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
