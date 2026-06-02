"""Repair proposal and result models."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RepairConfidence(float, Enum):
    """Confidence levels for repair proposals."""

    VERY_LOW = 0.2
    LOW = 0.4
    MEDIUM = 0.6
    HIGH = 0.8
    VERY_HIGH = 0.95


@dataclass(frozen=True)
class ProposedFix:
    """
    Immutable repair proposal for a data quality issue.

    Attributes:
        row: Row index to repair
        column: Column to repair
        original_value: Original problematic value
        proposed_value: Suggested replacement value
        confidence: Confidence in this fix (0.0-1.0)
        reason: Explanation of why this fix is proposed
        repair_type: Type of repair strategy used
        cost: Cost metric for applying this repair
        provenance: Information about how fix was derived
    """

    row: int
    column: str
    original_value: Any
    proposed_value: Any
    confidence: float
    reason: str
    repair_type: str
    cost: float = 0.0
    provenance: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return string representation of proposed fix."""
        return (
            f"Repair row {self.row}, col '{self.column}': "
            f"{self.original_value!r} → {self.proposed_value!r} "
            f"(confidence: {self.confidence:.0%})"
        )


@dataclass(frozen=True)
class RepairResult:
    """
    Result of applying a repair.

    Attributes:
        success: Whether repair was successfully applied
        row: Row that was repaired
        column: Column that was repaired
        original_value: Original value before repair
        new_value: New value after repair
        verified: Whether repair was verified by constraint solver
        error: Error message if repair failed
    """

    success: bool
    row: int
    column: str
    original_value: Any
    new_value: Any
    verified: bool = False
    error: str = ""

    def __str__(self) -> str:
        """Return string representation of repair result."""
        status = "✓" if self.success else "✗"
        verified_str = " (verified)" if self.verified else ""
        return (
            f"{status} Row {self.row}, col '{self.column}': "
            f"{self.original_value!r} → {self.new_value!r}{verified_str}"
        )
