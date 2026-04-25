"""DataForge detector package — pure data-quality issue detection.

This package provides the detector infrastructure and three Week 1 detectors:

- :class:`TypeMismatchDetector` — numeric/string/date type conflicts.
- :class:`DecimalShiftDetector` — power-of-10 outliers in numeric columns.
- :class:`FDViolationDetector` — rows violating declared functional dependencies.

Use :func:`run_all_detectors` to run all detectors and get a merged,
deduplicated, severity-sorted issue list.
"""

from __future__ import annotations

import pandas as pd

from dataforge.detectors.base import Detector, Issue, Schema, Severity
from dataforge.detectors.decimal_shift import DecimalShiftDetector
from dataforge.detectors.fd_violation import FDViolationDetector
from dataforge.detectors.type_mismatch import TypeMismatchDetector

__all__ = [
    "DecimalShiftDetector",
    "FDViolationDetector",
    "Issue",
    "Schema",
    "Severity",
    "TypeMismatchDetector",
    "run_all_detectors",
]

# Severity sort key: UNSAFE first, then REVIEW, then SAFE.
_SEVERITY_ORDER = {Severity.UNSAFE: 0, Severity.REVIEW: 1, Severity.SAFE: 2}


def run_all_detectors(df: pd.DataFrame, schema: Schema | None = None) -> list[Issue]:
    """Run all registered detectors and return a merged, sorted issue list.

    Issues are deduplicated by (row, column, issue_type) and sorted by
    severity (UNSAFE first) then confidence (highest first).

    Args:
        df: The input DataFrame to analyze.
        schema: Optional declared schema with column types and constraints.

    Returns:
        A list of Issue objects from all detectors, sorted by severity
        then confidence descending.

    Example:
        >>> import pandas as pd
        >>> from dataforge.detectors import run_all_detectors
        >>> df = pd.DataFrame({"age": ["25", "30", "N/A", "40"]})
        >>> issues = run_all_detectors(df)
        >>> len(issues)
        1
    """
    detectors: list[Detector] = [
        TypeMismatchDetector(),
        DecimalShiftDetector(),
        FDViolationDetector(),
    ]

    all_issues: list[Issue] = []
    for detector in detectors:
        all_issues.extend(detector.detect(df, schema))

    # Deduplicate by (row, column, issue_type).
    seen: set[tuple[int, str, str]] = set()
    unique: list[Issue] = []
    for issue in all_issues:
        key = (issue.row, issue.column, issue.issue_type)
        if key not in seen:
            seen.add(key)
            unique.append(issue)

    # Sort: UNSAFE first, then REVIEW, then SAFE; within same severity,
    # highest confidence first.
    unique.sort(key=lambda i: (_SEVERITY_ORDER[i.severity], -i.confidence))

    return unique
