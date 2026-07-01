"""Detector for format/pattern violations in structured columns.

Many real-world errors are *format* errors: a date in ``DD/MM/YYYY`` inside a
column of ISO ``YYYY-MM-DD`` dates, a zip code with a missing leading zero, a
mis-punctuated phone number. No single hand-written rule catches these across
datasets, so this detector learns each column's dominant value *shape* and
flags the minority shapes.

Precision guard (the reason this is safe to add broadly): the detector only
considers *structured* columns - those whose dominant shape contains a digit or
an ``@`` (dates, codes, zips, phones, emails). Free-text/prose columns (names,
addresses, descriptions) have no single dominant shape and are never flagged,
which is where naive format detectors generate false positives.

The detector is pure: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

from collections import Counter

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

# Minimum non-empty values for a column to be eligible.
_MIN_VALUES = 8
# Dominant shape must cover at least this fraction of values.
_DOMINANCE_THRESHOLD = 0.85
# Skip columns with too many distinct shapes (free text / high-cardinality).
_MAX_DISTINCT_SHAPES = 8


def value_shape(value: str) -> str:
    """Return the length-aware structural skeleton of a value.

    Each digit becomes ``9`` and each letter becomes ``A`` (length-preserving);
    other characters (separators, punctuation) are kept literally. This captures
    both separator format and field width, so fixed-width codes and dates align
    while free text fragments into many distinct shapes (and is skipped):

        "2024-01-13" -> "9999-99-99"
        "13/01/2024" -> "99/99/9999"
        "02134"      -> "99999"
        "2134"       -> "9999"
        "john@x.com" -> "AAAA@A.AAA"
    """
    return "".join("9" if ch.isdigit() else "A" if ch.isalpha() else ch for ch in value)


def _is_structured_shape(shape: str) -> bool:
    """Return whether a shape is structured enough to flag minorities against.

    Structured = contains a digit run or an email ``@``. Pure-word shapes like
    ``"A A"`` (names) are free text and are deliberately excluded.
    """
    return "9" in shape or "@" in shape


class FormatViolationDetector:
    """Flags values whose structural shape conflicts with the column's dominant shape.

    Example:
        >>> import pandas as pd
        >>> detector = FormatViolationDetector()
        >>> dates = ["2024-01-%02d" % d for d in range(1, 20)] + ["13/01/2024"]
        >>> df = pd.DataFrame({"d": dates})
        >>> issues = detector.detect(df)
        >>> issues[0].actual
        '13/01/2024'
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect format-violation issues across structured columns."""
        issues: list[Issue] = []
        for col_name in column_names(df):
            issues.extend(self._check_column(df, str(col_name)))
        return issues

    def _check_column(self, df: TableLike, col_name: str) -> list[Issue]:
        """Flag minority-shape values in one column, with precision guards."""
        entries: list[tuple[int, str, str]] = []
        for row_idx, raw in enumerate(column_values(df, col_name)):
            if raw is None:
                continue
            value = str(raw).strip()
            if not value:
                continue
            entries.append((row_idx, value, value_shape(value)))

        if len(entries) < _MIN_VALUES:
            return []

        shape_counts = Counter(shape for _, _, shape in entries)
        if len(shape_counts) > _MAX_DISTINCT_SHAPES:
            return []  # free text / high-cardinality column

        dominant_shape, dominant_count = shape_counts.most_common(1)[0]
        total = len(entries)
        dominance = dominant_count / total
        if dominance < _DOMINANCE_THRESHOLD:
            return []
        if not _is_structured_shape(dominant_shape):
            return []  # dominant shape is prose; do not flag

        confidence = round(min(0.95, 0.5 + dominance / 2.0), 2)
        issues: list[Issue] = []
        for row_idx, value, shape in entries:
            if shape == dominant_shape:
                continue
            issues.append(
                Issue(
                    row=row_idx,
                    column=col_name,
                    issue_type="format_violation",
                    severity=Severity.REVIEW,
                    confidence=confidence,
                    expected=dominant_shape,
                    actual=value,
                    reason=(
                        f"Value '{value}' has shape '{shape}' but column '{col_name}' is "
                        f"dominated by shape '{dominant_shape}' ({dominance:.0%})."
                    ),
                )
            )
        return issues
