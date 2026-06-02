"""Issue detection models."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class IssueType(str, Enum):
    """Types of data quality issues."""

    TYPE_MISMATCH = "type_mismatch"
    DECIMAL_SHIFT = "decimal_shift"
    FUNCTIONAL_DEPENDENCY_VIOLATION = "fd_violation"
    CONSTRAINT_VIOLATION = "constraint_violation"
    MISSING_VALUE = "missing_value"
    OUTLIER = "outlier"
    DUPLICATE = "duplicate"


class IssueSeverity(str, Enum):
    """Severity levels for issues."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True)
class Issue:
    """
    Immutable data quality issue.

    Attributes:
        issue_type: Type of issue detected
        severity: Severity level
        row: Row index (0-based) where issue occurs
        column: Column name where issue occurs
        value: Actual value that triggered the issue
        expected: Expected value or type
        message: Human-readable description
        context: Additional context (dict of relevant info)
        detector: Name of detector that found this issue
    """

    issue_type: IssueType
    severity: IssueSeverity
    row: int
    column: str
    value: Any
    expected: Any
    message: str
    detector: str
    context: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return string representation of issue."""
        return (
            f"{self.issue_type.value} at row {self.row}, column '{self.column}': "
            f"{self.message}"
        )
