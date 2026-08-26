"""Core models and protocol for the DataForge detector subsystem."""

from __future__ import annotations

import enum
from typing import Final, Literal, Protocol, get_args

from pydantic import BaseModel, Field

from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS
from dataforge.table import TableLike
from dataforge.verifier.schema import (
    AcceptedValues,
    AggregateDependency,
    DomainBound,
    FunctionalDependency,
    RegexConstraint,
    RelationshipConstraint,
    Schema,
)

__all__ = [
    "ALL_ISSUE_TYPES",
    "AggregateDependency",
    "AcceptedValues",
    "Detector",
    "DomainBound",
    "FunctionalDependency",
    "Issue",
    "IssueTypeLiteral",
    "RegexConstraint",
    "RelationshipConstraint",
    "Schema",
    "Severity",
]


class Severity(enum.Enum):
    """Three-tier severity for data-quality issues.

    Ordering: SAFE < REVIEW < UNSAFE (higher = more severe).

    - SAFE: likely benign; can be auto-applied in bulk without human review.
    - REVIEW: ambiguous; should appear in the profile table for human triage.
    - UNSAFE: structural error; blocks automated repair without explicit approval.

    See DECISIONS.md entry "Issue severity tiers" for the rationale behind
    choosing exactly 3 levels.
    """

    SAFE = "safe"
    REVIEW = "review"
    UNSAFE = "unsafe"

    def __lt__(self, other: object) -> bool:
        """Enable ordering so SAFE < REVIEW < UNSAFE."""
        if not isinstance(other, Severity):
            return NotImplemented
        order = {Severity.SAFE: 0, Severity.REVIEW: 1, Severity.UNSAFE: 2}
        return order[self] < order[other]

    def __le__(self, other: object) -> bool:
        """Enable ordering so SAFE <= REVIEW <= UNSAFE."""
        if not isinstance(other, Severity):
            return NotImplemented
        order = {Severity.SAFE: 0, Severity.REVIEW: 1, Severity.UNSAFE: 2}
        return order[self] <= order[other]

    def __gt__(self, other: object) -> bool:
        """Enable ordering so UNSAFE > REVIEW > SAFE."""
        if not isinstance(other, Severity):
            return NotImplemented
        order = {Severity.SAFE: 0, Severity.REVIEW: 1, Severity.UNSAFE: 2}
        return order[self] > order[other]

    def __ge__(self, other: object) -> bool:
        """Enable ordering so UNSAFE >= REVIEW >= SAFE."""
        if not isinstance(other, Severity):
            return NotImplemented
        order = {Severity.SAFE: 0, Severity.REVIEW: 1, Severity.UNSAFE: 2}
        return order[self] >= order[other]


# Closed vocabulary of issue types. Extend this Literal as new detectors ship.
# Detection-only classes are allowed: emitting an issue type with no registered
# repairer degrades gracefully to detection-only (the issue is reported but not
# auto-fixed), which is the honest detection-vs-correction split.
IssueTypeLiteral = Literal[
    "type_mismatch",
    "decimal_shift",
    "fd_violation",
    "missing_value",
    "format_violation",
    "categorical_normalization",
    "outlier",
    "duplicate_row",
    "date_transposition",
    "entity_consensus",
    # Detection-only, and structurally so: there is no `semantic_domain_violation`
    # repairer and the id is deliberately absent from CONSTRAINT_CHECKABLE_DETECTORS, so
    # it has no write path on any surface. See dataforge/detectors/semantic_domain.py for
    # why an externally learned constraint must still be advisory.
    "semantic_domain_violation",
]

# The issue-type universe, DERIVED from the Literal above rather than restated.
#
# Why this exists, dated 2026-08-26. `scripts/ci/readme_truth.py` polices claims of write
# authority. It imported `CONSTRAINT_CHECKABLE_DETECTORS` from source of truth -- and then
# hardcoded the *population* it policed as an eight-name set literal. `IssueTypeLiteral` had
# since grown to eleven, so `date_transposition`, `entity_consensus` and
# `semantic_domain_violation` were invisible to that gate in both directions: a document could
# assert any of them auto-applies and CI would pass. `README.md` said "Eight detector families"
# for the same reason, and the doc and the gate AGREED WITH EACH OTHER while both disagreed
# with this file -- two mutually-consistent wrong artifacts reading as verification.
#
# The general rule, which is why this constant lives here and not in the checker: a gate that
# hardcodes any part of the universe it polices can only detect changes to the part it derives.
# Freezing the population is invisible precisely because the frozen literal was correct on the
# day it was written. Derive the population; never restate it.
ALL_ISSUE_TYPES: Final[frozenset[str]] = frozenset(get_args(IssueTypeLiteral))

# Structural invariant, asserted at import. An allowlist entry naming an issue type no detector
# can emit is a write permission for nothing -- or, worse, a typo that silently disables a
# permission someone measured and earned.
if not CONSTRAINT_CHECKABLE_DETECTORS <= ALL_ISSUE_TYPES:  # pragma: no cover - import guard
    raise AssertionError(
        "CONSTRAINT_CHECKABLE_DETECTORS contains issue types absent from IssueTypeLiteral: "
        f"{sorted(CONSTRAINT_CHECKABLE_DETECTORS - ALL_ISSUE_TYPES)}"
    )


class Issue(BaseModel):
    """A single data-quality finding at a specific (row, column) location.

    Args:
        row: Zero-indexed row number in the DataFrame.
        column: Column name where the issue was detected.
        issue_type: Machine-readable issue category (closed vocabulary).
        severity: Three-tier severity classification.
        confidence: Detector's confidence in the finding (0.0 to 1.0).
        expected: What the value should be (if known); None for detection-only.
        actual: The actual value found in the cell.
        reason: Human-readable explanation of the issue.

    Example:
        >>> issue = Issue(
        ...     row=3, column="price", issue_type="decimal_shift",
        ...     severity=Severity.REVIEW, confidence=0.92,
        ...     expected="102.0", actual="1020.0",
        ...     reason="Value 1020.0 appears to be ~10x the typical value",
        ... )
    """

    row: int = Field(ge=0, description="Zero-indexed row number")
    column: str = Field(min_length=1, description="Column name")
    issue_type: IssueTypeLiteral = Field(description="Machine-readable issue category")
    severity: Severity = Field(description="Three-tier severity")
    confidence: float = Field(ge=0.0, le=1.0, description="Detector confidence")
    expected: str | None = Field(default=None, description="Expected value (if known)")
    actual: str = Field(description="Actual value found in the cell")
    reason: str = Field(min_length=1, description="Human-readable explanation")

    model_config = {"frozen": True}


class Detector(Protocol):
    """Structural protocol that every detector must implement.

    A detector is a pure function over tabular data: it receives a table
    and an optional Schema, and returns a list of Issue objects. No LLM calls,
    no disk I/O, no side effects.

    Example:
        >>> class MyDetector:
        ...     def detect(
        ...         self, df: TableLike, schema: Schema | None = None
        ...     ) -> list[Issue]:
        ...         return []
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect data-quality issues in the given DataFrame.

        Args:
            df: The input table to analyze.
            schema: Optional declared schema with column types and constraints.

        Returns:
            A list of Issue objects describing detected anomalies.
        """
        ...  # pragma: no cover
