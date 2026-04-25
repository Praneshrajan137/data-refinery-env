"""Core models and protocol for the DataForge detector subsystem."""

from __future__ import annotations

import enum
from typing import Literal, Protocol

import pandas as pd
from pydantic import BaseModel, Field

from dataforge.verifier.schema import (
    AggregateDependency,
    DomainBound,
    FunctionalDependency,
    Schema,
)

__all__ = [
    "AggregateDependency",
    "Detector",
    "DomainBound",
    "FunctionalDependency",
    "Issue",
    "IssueTypeLiteral",
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
IssueTypeLiteral = Literal["type_mismatch", "decimal_shift", "fd_violation"]


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

    A detector is a pure function over tabular data: it receives a DataFrame
    and an optional Schema, and returns a list of Issue objects. No LLM calls,
    no disk I/O, no side effects.

    Example:
        >>> class MyDetector:
        ...     def detect(
        ...         self, df: pd.DataFrame, schema: Schema | None = None
        ...     ) -> list[Issue]:
        ...         return []
    """

    def detect(self, df: pd.DataFrame, schema: Schema | None = None) -> list[Issue]:
        """Detect data-quality issues in the given DataFrame.

        Args:
            df: The input DataFrame to analyze.
            schema: Optional declared schema with column types and constraints.

        Returns:
            A list of Issue objects describing detected anomalies.
        """
        ...  # pragma: no cover
