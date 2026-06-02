"""Schema and constraint models."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ColumnType(str, Enum):
    """Supported column data types."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    MIXED = "mixed"


@dataclass(frozen=True)
class Constraint:
    """
    Schema constraint for data validation.

    Attributes:
        name: Constraint identifier
        type: Constraint type (e.g., 'unique', 'not_null', 'functional_dependency')
        columns: Columns involved in constraint
        expression: Optional constraint expression or parameters
    """

    name: str
    type: str
    columns: tuple[str, ...]
    expression: Optional[str] = None

    def __str__(self) -> str:
        """Return string representation of constraint."""
        cols = ", ".join(self.columns)
        return f"{self.type}({cols})"


@dataclass(frozen=True)
class Column:
    """
    Column definition with inferred type and constraints.

    Attributes:
        name: Column name
        type: Inferred column type
        nullable: Whether column can contain null values
        constraints: Column-specific constraints
        sample_values: Sample values for reference
        cardinality: Number of unique values
    """

    name: str
    type: ColumnType
    nullable: bool = True
    constraints: tuple[Constraint, ...] = field(default_factory=tuple)
    sample_values: tuple[Any, ...] = field(default_factory=tuple)
    cardinality: int = 0

    def __str__(self) -> str:
        """Return string representation of column."""
        nullable_str = " NULL" if self.nullable else " NOT NULL"
        return f"{self.name}: {self.type.value}{nullable_str}"


@dataclass(frozen=True)
class Schema:
    """
    Complete schema definition for a CSV file.

    Attributes:
        columns: Dictionary mapping column names to Column definitions
        constraints: Table-level constraints
        inferred: Whether schema was inferred from data
        confidence: Confidence in inferred schema (0.0-1.0)
    """

    columns: dict[str, Column]
    constraints: tuple[Constraint, ...] = field(default_factory=tuple)
    inferred: bool = True
    confidence: float = 0.0

    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        return self.columns.get(name)

    def has_column(self, name: str) -> bool:
        """Check if schema has a column."""
        return name in self.columns

    def __str__(self) -> str:
        """Return string representation of schema."""
        cols_str = ", ".join(str(col) for col in self.columns.values())
        return f"Schema({cols_str})"
