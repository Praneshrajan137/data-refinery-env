"""Detector for type-mismatch anomalies in tabular data.

Identifies values whose inferred type conflicts with the dominant type of
their column. Examples: a pure-numeric string ``"12345"`` in a column of
person names, or ``"N/A"`` in a column where 90% of values parse as float.

The detector is **pure**: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pandas as pd

from dataforge.detectors.base import Issue, Schema, Severity

if TYPE_CHECKING:
    pass

# Compiled regexes for type inference.
_NUMERIC_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
_DATE_RE = re.compile(
    r"^\d{4}[-/]\d{2}[-/]\d{2}"  # YYYY-MM-DD or YYYY/MM/DD prefix
    r"([ T]\d{2}:\d{2}(:\d{2})?)?$"  # optional time component
)

# Minimum ratio of the dominant type for a column to be considered typed.
# Below this, the column is genuinely mixed and we don't flag individuals.
_DOMINANCE_THRESHOLD = 0.65


def _classify_value(value: str) -> str:
    """Classify a single string value into a type category.

    Args:
        value: The string value to classify.

    Returns:
        One of ``"numeric"``, ``"date"``, or ``"string"``.
    """
    stripped = value.strip()
    if _NUMERIC_RE.match(stripped):
        return "numeric"
    if _DATE_RE.match(stripped):
        return "date"
    return "string"


class TypeMismatchDetector:
    """Detects values whose type conflicts with the column's dominant type.

    For each column, the detector infers the dominant type by classifying
    every non-null value as numeric, date, or string. Values belonging to
    the minority type are flagged as ``type_mismatch`` issues.

    A column must have a clear dominant type (>= 65% of values) for any
    flags to be raised. Ambiguous 50/50 splits are not flagged.

    Example:
        >>> import pandas as pd
        >>> detector = TypeMismatchDetector()
        >>> df = pd.DataFrame({"age": ["25", "30", "N/A", "40"]})
        >>> issues = detector.detect(df)
        >>> len(issues)
        1
        >>> issues[0].actual
        'N/A'
    """

    def detect(self, df: pd.DataFrame, schema: Schema | None = None) -> list[Issue]:
        """Detect type-mismatch issues in the DataFrame.

        Args:
            df: The input DataFrame to analyze. Values are expected to be
                strings (loaded with ``dtype=str``).
            schema: Optional declared schema (not used by this detector yet;
                reserved for future type-declaration awareness).

        Returns:
            A list of Issue objects for each cell whose type conflicts with
            the column's dominant type.
        """
        issues: list[Issue] = []

        for col_name in df.columns:
            col_issues = self._check_column(df, str(col_name))
            issues.extend(col_issues)

        return issues

    def _check_column(self, df: pd.DataFrame, col_name: str) -> list[Issue]:
        """Check a single column for type mismatches.

        Args:
            df: The DataFrame containing the column.
            col_name: Name of the column to check.

        Returns:
            Issues found in this column.
        """
        series = df[col_name]

        # Collect (index, value, type) for non-null entries.
        classified: list[tuple[int, str, str]] = []
        for row_idx, val in enumerate(series.tolist()):
            if pd.isna(val):
                continue
            str_val = str(val).strip()
            if not str_val:
                continue
            classified.append((row_idx, str_val, _classify_value(str_val)))

        if len(classified) < 2:
            return []

        # Count type frequencies.
        type_counts: dict[str, int] = {}
        for _, _, vtype in classified:
            type_counts[vtype] = type_counts.get(vtype, 0) + 1

        total = len(classified)
        dominant_type = max(type_counts, key=lambda t: type_counts[t])
        dominant_count = type_counts[dominant_type]
        dominance_ratio = dominant_count / total

        if dominance_ratio < _DOMINANCE_THRESHOLD:
            return []

        # Flag minority-type values.
        issues: list[Issue] = []
        for row_idx, str_val, vtype in classified:
            if vtype == dominant_type:
                continue

            # Scale confidence: dominance of 0.65 -> 0.70, 0.75 -> 0.85, 1.0 -> 0.95.
            # A single minority value in a strongly typed column is a strong signal.
            minority_count = total - dominant_count
            scarcity_boost = max(0.0, 0.10 * (1.0 - minority_count / total))
            confidence = min(0.95, dominance_ratio + scarcity_boost)
            reason = self._build_reason(str_val, vtype, dominant_type, col_name)

            issues.append(
                Issue(
                    row=row_idx,
                    column=col_name,
                    issue_type="type_mismatch",
                    severity=Severity.REVIEW,
                    confidence=round(confidence, 2),
                    actual=str_val,
                    reason=reason,
                )
            )

        return issues

    @staticmethod
    def _build_reason(value: str, value_type: str, dominant_type: str, col_name: str) -> str:
        """Build a human-readable reason string.

        Args:
            value: The actual cell value.
            value_type: Inferred type of the value.
            dominant_type: Dominant type of the column.
            col_name: Column name for context.

        Returns:
            A descriptive reason string.
        """
        if dominant_type == "numeric" and value_type == "string":
            return f"Value '{value}' is non-numeric in predominantly numeric column '{col_name}'"
        if dominant_type == "string" and value_type == "numeric":
            return f"Value '{value}' looks numeric in predominantly string column '{col_name}'"
        if dominant_type == "string" and value_type == "date":
            return f"Value '{value}' looks like a date in predominantly string column '{col_name}'"
        return (
            f"Value '{value}' (type: {value_type}) conflicts with "
            f"dominant type '{dominant_type}' in column '{col_name}'"
        )
