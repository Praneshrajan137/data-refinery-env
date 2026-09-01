"""Detector for time values buried in date/timezone cruft.

A frequently-undetected, genuinely-inferable error family (measured on flights):
a valid clock time wrapped in extra content -- a leading date
(``"12/02/2011 6:55 a.m."``) or a trailing timezone/parenthetical
(``"9:05 a.m. (-00:00)"``) -- whose correct value is the embedded time. Unlike
the flights ``act_dep_time`` value errors (a different, non-inferable time), the
correct value here IS present in the cell, so this slice is on the *detectable*
side of the honest frontier.

Emitted as ``time_format_cruft``, its own issue type since 2026-09-01. It previously
shared ``format_violation`` with :mod:`dataforge.detectors.format_violation`, which was a
write-safety hazard rather than untidiness: that detector's ``Issue.expected`` holds a shape
MASK (``"9999-99-99"``) while this one's holds a substitutable VALUE, and ``Issue`` carries no
detector identity apart from its issue type. While the two shared an id, routing this
detector's exact value into the suggestion path would have routed masks with it. Separate ids
are what made the safe half routable, so the value below is now surfaced as an unverified
review suggestion instead of being computed and discarded.

Still detection-only: no repairer is registered for this id, so it cannot auto-apply and
cannot change correction F1. Tier 1, strictly additive. The rule requires a clock-time token
plus date/timezone residue, so it never fires on columns without such times (measured:
0 false positives over 4584 correct cells in the affected columns).
"""

from __future__ import annotations

import re

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

_TIME = re.compile(r"\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.)", re.IGNORECASE)
_DATE = re.compile(r"\d{1,2}/\d{1,2}/\d{2,4}")


def _embedded_time_with_cruft(value: str) -> str | None:
    """Return the embedded clock time when the cell is a time plus date/tz cruft."""
    text = value.strip()
    match = _TIME.search(text)
    if match is None:
        return None
    residue = (text[: match.start()] + text[match.end() :]).strip()
    if not residue:
        return None  # a clean time, nothing to flag
    if _DATE.search(residue) or "(" in residue or "-" in residue:
        return match.group(0).strip()
    return None


class TimeFormatCruftDetector:
    """Flags a valid clock time wrapped in a leading date or trailing timezone.

    Example:
        >>> import pandas as pd
        >>> detector = TimeFormatCruftDetector()
        >>> df = pd.DataFrame({"t": ["12/02/2011 6:55 a.m.", "6:55 a.m."]})
        >>> [i.row for i in detector.detect(df)]
        [0]
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect time-with-cruft format issues across all columns."""
        issues: list[Issue] = []
        for col_name in column_names(df):
            name = str(col_name)
            for row_index, value in enumerate(column_values(df, name)):
                clean_time = _embedded_time_with_cruft(str(value))
                if clean_time is None:
                    continue
                issues.append(
                    Issue(
                        row=row_index,
                        column=name,
                        issue_type="time_format_cruft",
                        severity=Severity.REVIEW,
                        confidence=0.8,
                        expected=clean_time,
                        actual=str(value).strip(),
                        reason=(
                            f"Value {str(value).strip()!r} in column '{name}' wraps a valid "
                            f"time '{clean_time}' in extra date/timezone text."
                        ),
                    )
                )
        return issues
