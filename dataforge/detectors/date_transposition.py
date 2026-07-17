"""Detector for transposed date components (Y/M/D written where M/D/YY is meant).

A measured, genuinely-inferable-but-not-provable error family (rayyan
``article_jcreated_at``): a date whose components are rotated, e.g. the dirty
value ``"4/2/15"`` is the year-first ``Y/M/D`` form of the intended
``"2/15/04"`` (``M/D/YY``). The exact fix is a deterministic left-rotation of the
three components -- verified to reproduce the clean value on 722/722 measured
cells. So the *correction* is exact.

Detection, however, is NOT provable: every such cell is ALSO a syntactically
valid ``M/D/YY`` date, so no in-table rule can decide with certainty that a given
cell is transposed rather than a legitimately different date (measured best
precision ~0.94, and the transposed form is even the column majority). Because a
wrong auto-fix here would corrupt a valid date, this detector is **detection-only
and never registered with a repairer** -- by construction there is no write path.
It carries the exact rotation in ``Issue.expected`` so the engine can surface it
as an *unverified, human-review* suggestion (never auto-applied). Tier 1,
strictly additive; it only fires inside columns that are predominantly 3-part
numeric slash-dates, so it cannot touch non-date columns.
"""

from __future__ import annotations

import re

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

_THREE_PART_DATE = re.compile(r"^\s*\d{1,4}/\d{1,2}/\d{1,4}\s*$")
# A column is treated as a date column only when most non-empty cells look like
# a 3-part numeric slash-date; below this the detector stays silent.
_DATE_COLUMN_FRACTION = 0.5
# Detection is inherently uncertain here (see module docstring); the confidence
# reflects that this is a review suggestion, not a provable correction.
_SUGGESTION_CONFIDENCE = 0.5


def _parts(value: str) -> tuple[int, int, int] | None:
    """Return the three integer components of a numeric ``a/b/c`` date, else None."""
    text = value.strip()
    if not _THREE_PART_DATE.match(text):
        return None
    chunks = text.split("/")
    if len(chunks) != 3:
        return None
    try:
        first, second, third = (int(chunk) for chunk in chunks)
    except ValueError:
        return None
    return first, second, third


def _valid_mdy(parts: tuple[int, int, int]) -> bool:
    """Valid as month/day/year: month in 1..12, day in 1..31."""
    month, day, _year = parts
    return 1 <= month <= 12 and 1 <= day <= 31


def _valid_ymd(parts: tuple[int, int, int]) -> bool:
    """Valid as year/month/day: month in 1..12, day in 1..31."""
    _year, month, day = parts
    return 1 <= month <= 12 and 1 <= day <= 31


def rotate_ymd_to_mdy(value: str) -> str | None:
    """Rotate a ``Y/M/D`` date left into canonical ``M/D/YY`` (year zero-padded).

    ``(p0, p1, p2) -> f"{p1}/{p2}/{p0:02d}"``. Returns ``None`` when the value is
    not three numeric ``/``-separated parts. Deterministic and idempotent-safe:
    the detector only calls it on cells it decides are transposition candidates.
    """
    parts = _parts(value)
    if parts is None:
        return None
    first, second, third = parts
    return f"{second}/{third}/{first:02d}"


class DateTranspositionDetector:
    """Flags dates that appear component-transposed (``Y/M/D`` vs ``M/D/YY``).

    Detection-only: emits an issue carrying the exact rotation in ``expected``,
    but is never paired with a repairer, so it cannot auto-apply. The engine
    surfaces it as an unverified review suggestion.

    Example:
        >>> import pandas as pd
        >>> detector = DateTranspositionDetector()
        >>> df = pd.DataFrame({"created": ["4/2/15", "12/1/06", "1/13/01"]})
        >>> [(i.actual, i.expected) for i in detector.detect(df)]
        [('4/2/15', '2/15/04'), ('12/1/06', '1/6/12')]
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect transposition candidates in predominantly-date columns."""
        del schema
        issues: list[Issue] = []
        for col_name in column_names(df):
            name = str(col_name)
            values = [str(v) for v in column_values(df, name)]
            if not self._is_date_column(values):
                continue
            for row_index, value in enumerate(values):
                parts = _parts(value)
                if parts is None:
                    continue
                # Ambiguous transposition candidate: valid both ways, and the
                # rotation actually changes the value.
                if not (_valid_mdy(parts) and _valid_ymd(parts)):
                    continue
                rotated = rotate_ymd_to_mdy(value)
                if rotated is None or rotated == value.strip():
                    continue
                issues.append(
                    Issue(
                        row=row_index,
                        column=name,
                        issue_type="date_transposition",
                        severity=Severity.REVIEW,
                        confidence=_SUGGESTION_CONFIDENCE,
                        expected=rotated,
                        actual=value.strip(),
                        reason=(
                            f"Value {value.strip()!r} in column '{name}' is ambiguous: it is "
                            f"already a valid M/D/YY date (keep as-is), but may also be a Y/M/D "
                            f"transposition whose canonical M/D/YY form is '{rotated}'. Both "
                            f"readings are valid dates, so no in-table rule can decide - surfaced "
                            f"for human review at confidence 0.5, never auto-applied."
                        ),
                    )
                )
        return issues

    @staticmethod
    def _is_date_column(values: list[str]) -> bool:
        """True when most non-empty cells look like 3-part numeric slash-dates."""
        non_empty = [v for v in values if v.strip()]
        if not non_empty:
            return False
        date_like = sum(1 for v in non_empty if _THREE_PART_DATE.match(v.strip()))
        return date_like / len(non_empty) >= _DATE_COLUMN_FRACTION
