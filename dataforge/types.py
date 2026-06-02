"""Type definitions and protocols for DataForge."""

from pathlib import Path
from typing import Any, Protocol, TypeVar, Union

# Basic type aliases
TableData = list[dict[str, Any]]  # List of row dicts
CSVPath = Union[str, Path]
SchemePath = Union[str, Path]
AuditPath = Union[str, Path]

# Numeric types
Number = Union[int, float]
Numeric = TypeVar("Numeric", int, float)

# Configuration types
ConfigDict = dict[str, Any]

# Callable types for detectors and repairers
T = TypeVar("T")
U = TypeVar("U")


class Detector(Protocol):
    """Protocol for data quality detectors."""

    name: str
    """Name of detector."""

    def detect(self, data: TableData, schema: Any) -> list[Any]:
        """Detect issues in data according to schema."""
        ...


class Repairer(Protocol):
    """Protocol for repair strategies."""

    name: str
    """Name of repairer."""

    def repair(self, data: TableData, issues: list[Any]) -> list[Any]:
        """Propose repairs for detected issues."""
        ...


class Formatter(Protocol):
    """Protocol for output formatters."""

    def format(self, data: Any, **kwargs: Any) -> str:
        """Format data for output."""
        ...


# Result types
class Result(Protocol[T]):
    """Generic result type for error handling."""

    def is_ok(self) -> bool:
        """Check if result represents success."""
        ...

    def is_err(self) -> bool:
        """Check if result represents failure."""
        ...

    def unwrap(self) -> T:
        """Extract value or raise error."""
        ...
