"""DataForge detector package — pure data-quality issue detection.

This package provides the detector ensemble infrastructure. Heterogeneous base
detectors each target one error family; :func:`run_all_detectors` runs the
ensemble and returns one issue per cell (highest-confidence detector wins),
severity-sorted. No single strategy generalizes across datasets, so coverage
comes from the breadth of the ensemble, not any one detector.

Use :func:`run_all_detectors` to run the ensemble and get a merged,
per-cell-deduplicated, severity-sorted issue list.
"""

from __future__ import annotations

from dataforge.detectors.base import ALL_ISSUE_TYPES, Detector, Issue, Schema, Severity
from dataforge.detectors.categorical_normalization import CategoricalNormalizationDetector
from dataforge.detectors.date_transposition import DateTranspositionDetector
from dataforge.detectors.decimal_shift import DecimalShiftDetector
from dataforge.detectors.duplicate_row import DuplicateRowDetector
from dataforge.detectors.entity_consensus import EntityConsensusDetector
from dataforge.detectors.fd_violation import FDViolationDetector
from dataforge.detectors.format_violation import FormatViolationDetector
from dataforge.detectors.missing_value import MissingValueDetector
from dataforge.detectors.outlier import OutlierDetector
from dataforge.detectors.semantic_domain import (
    PatternSDC,
    SDCLoadResult,
    SemanticDomainDetector,
    load_pattern_sdcs,
    parse_pattern_sdcs,
)
from dataforge.detectors.time_format_cruft import TimeFormatCruftDetector
from dataforge.detectors.type_mismatch import TypeMismatchDetector
from dataforge.table import TableLike

__all__ = [
    "ALL_ISSUE_TYPES",
    "CategoricalNormalizationDetector",
    "DateTranspositionDetector",
    "DecimalShiftDetector",
    "DuplicateRowDetector",
    "EntityConsensusDetector",
    "FDViolationDetector",
    "FormatViolationDetector",
    "Issue",
    "MissingValueDetector",
    "OutlierDetector",
    "PatternSDC",
    "SDCLoadResult",
    "Schema",
    "SemanticDomainDetector",
    "Severity",
    "TimeFormatCruftDetector",
    "TypeMismatchDetector",
    "default_detectors",
    "load_pattern_sdcs",
    "parse_pattern_sdcs",
    "run_all_detectors",
]

# Severity sort key: UNSAFE first, then REVIEW, then SAFE.
_SEVERITY_ORDER = {Severity.UNSAFE: 0, Severity.REVIEW: 1, Severity.SAFE: 2}

# Tier 0 = established, high-precision detectors that own their cells. Tier 1 =
# newer/broader detectors that are strictly additive (they only claim cells no
# tier-0 detector flagged), guaranteeing they cannot regress the proven floor.
_ESTABLISHED_ISSUE_TYPES = frozenset({"type_mismatch", "decimal_shift", "fd_violation"})


def default_detectors() -> list[Detector]:
    """Return the default detector ensemble in registration order.

    New base detectors are appended here. Order only affects tie-breaking when
    two detectors flag the same cell with equal confidence and severity.

    :class:`SemanticDomainDetector` is deliberately **absent**: it needs a fetched,
    hash-verified SDC artifact, and this ensemble must stay offline and dependency-free.
    Construct it explicitly with ``load_pattern_sdcs()``.
    """
    return [
        TypeMismatchDetector(),
        DecimalShiftDetector(),
        FDViolationDetector(),
        FormatViolationDetector(),
        MissingValueDetector(),
        CategoricalNormalizationDetector(),
        OutlierDetector(),
        DuplicateRowDetector(),
        TimeFormatCruftDetector(),
        DateTranspositionDetector(),
        EntityConsensusDetector(),
    ]


def run_all_detectors(df: TableLike, schema: Schema | None = None) -> list[Issue]:
    """Run the detector ensemble and return one issue per cell, severity-sorted.

    Each cell is reported at most once. When multiple detectors flag the same
    cell, the issue is kept by precedence: most severe first (UNSAFE > REVIEW >
    SAFE), then highest confidence, then detector registration order. This
    preserves the established high-precision detectors' precedence (e.g. an
    UNSAFE fd_violation always wins its cell), so a newly added REVIEW-level
    detector is strictly additive - it can only claim cells no higher-precedence
    detector flagged, and can never displace or regress an existing repair.

    Args:
        df: The input table to analyze.
        schema: Optional declared schema with column types and constraints.

    Returns:
        A list of Issue objects, one per flagged cell, sorted by severity
        (UNSAFE first) then confidence descending.

    Example:
        >>> import pandas as pd
        >>> from dataforge.detectors import run_all_detectors
        >>> df = pd.DataFrame({"age": ["25", "30", "N/A", "40"]})
        >>> issues = run_all_detectors(df)
        >>> len(issues)
        1
    """
    # Keep, per cell, the highest-precedence issue: most severe, then highest
    # confidence, then earliest detector. Precise detectors thus retain their
    # cells and new detectors only fill the gaps.
    best: dict[tuple[int, str], tuple[tuple[int, int, float, int], Issue]] = {}
    for order, detector in enumerate(default_detectors()):
        for issue in detector.detect(df, schema):
            key = (issue.row, issue.column)
            tier = 0 if issue.issue_type in _ESTABLISHED_ISSUE_TYPES else 1
            rank = (tier, _SEVERITY_ORDER[issue.severity], -issue.confidence, order)
            current = best.get(key)
            if current is None or rank < current[0]:
                best[key] = (rank, issue)

    unique = [entry[1] for entry in best.values()]
    unique.sort(key=lambda i: (_SEVERITY_ORDER[i.severity], -i.confidence, i.row, i.column))
    return unique
