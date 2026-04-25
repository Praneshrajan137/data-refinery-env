"""Canonical schema models shared by detectors, safety, and the verifier."""

from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

AggregateLiteral = Literal["sum", "avg"]

_CONFIG = ConfigDict(frozen=True)


@dataclass(config=_CONFIG, kw_only=True)
class FunctionalDependency:
    """Declared functional dependency: determinant columns -> dependent column."""

    determinant: tuple[str, ...] = Field(min_length=1)
    dependent: str = Field(min_length=1)


@dataclass(config=_CONFIG, kw_only=True)
class DomainBound:
    """Numeric min/max bounds for a column."""

    column: str = Field(min_length=1)
    min_value: float | None = None
    max_value: float | None = None
    inclusive_min: bool = True
    inclusive_max: bool = True


@dataclass(config=_CONFIG, kw_only=True)
class AggregateDependency:
    """Metadata describing a source column used in an aggregate elsewhere."""

    source_column: str = Field(min_length=1)
    target_column: str = Field(min_length=1)
    aggregate: AggregateLiteral
    group_by: tuple[str, ...] = Field(default_factory=tuple)


@dataclass(config=_CONFIG, kw_only=True)
class Schema:
    """Optional declared schema for a dataset."""

    columns: dict[str, str] = Field(default_factory=dict)
    functional_dependencies: tuple[FunctionalDependency, ...] = Field(default_factory=tuple)
    pii_columns: frozenset[str] = Field(default_factory=frozenset)
    domain_bounds: tuple[DomainBound, ...] = Field(default_factory=tuple)
    aggregate_dependencies: tuple[AggregateDependency, ...] = Field(default_factory=tuple)

    def column_type(self, column: str) -> str | None:
        """Return the declared type for a column, if any."""
        return self.columns.get(column)

    def domain_bounds_for(self, column: str) -> tuple[DomainBound, ...]:
        """Return all domain bounds declared for the given column."""
        return tuple(bound for bound in self.domain_bounds if bound.column == column)

    def aggregate_dependencies_for(self, column: str) -> tuple[AggregateDependency, ...]:
        """Return aggregate dependencies where the column is the source input."""
        return tuple(
            dependency
            for dependency in self.aggregate_dependencies
            if dependency.source_column == column
        )
