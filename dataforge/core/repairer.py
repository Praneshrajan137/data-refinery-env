"""Repairer base class and registry."""

from abc import ABC, abstractmethod
from typing import Optional

from dataforge.models import Issue, ProposedFix, Schema
from dataforge.types import TableData


class Repairer(ABC):
    """Base class for repair strategies."""

    name: str
    """Name identifier for this repairer."""

    description: str = "Data repair strategy"
    """Description of repair approach."""

    @abstractmethod
    def repair(
        self, data: TableData, issues: list[Issue], schema: Optional[Schema] = None
    ) -> list[ProposedFix]:
        """
        Propose repairs for detected issues.

        Args:
            data: List of row dictionaries
            issues: List of detected issues to repair
            schema: Optional schema for context

        Returns:
            List of proposed fixes
        """
        pass

    def __call__(
        self, data: TableData, issues: list[Issue], schema: Optional[Schema] = None
    ) -> list[ProposedFix]:
        """Allow repairer to be called directly."""
        return self.repair(data, issues, schema)


class RepairerRegistry:
    """Registry for managing repairers."""

    _repairers: dict[str, Repairer] = {}

    @classmethod
    def register(cls, repairer: Repairer) -> None:
        """Register a repairer."""
        cls._repairers[repairer.name] = repairer

    @classmethod
    def unregister(cls, name: str) -> None:
        """Unregister a repairer by name."""
        if name in cls._repairers:
            del cls._repairers[name]

    @classmethod
    def get(cls, name: str) -> Optional[Repairer]:
        """Get repairer by name."""
        return cls._repairers.get(name)

    @classmethod
    def get_all(cls) -> dict[str, Repairer]:
        """Get all registered repairers."""
        return cls._repairers.copy()

    @classmethod
    def get_names(cls) -> list[str]:
        """Get names of all registered repairers."""
        return list(cls._repairers.keys())

    @classmethod
    def repair_all(
        cls,
        data: TableData,
        issues: list[Issue],
        schema: Optional[Schema] = None,
        enabled: Optional[list[str]] = None,
    ) -> dict[str, list[ProposedFix]]:
        """
        Run all repairers (or only enabled ones).

        Args:
            data: Data to repair
            issues: Issues to address
            schema: Optional schema
            enabled: List of repairer names to run (None = run all)

        Returns:
            Dict mapping repairer name to list of proposed fixes
        """
        repairers = cls._repairers
        if enabled:
            repairers = {k: v for k, v in repairers.items() if k in enabled}

        results = {}
        for name, repairer in repairers.items():
            try:
                results[name] = repairer.repair(data, issues, schema)
            except Exception as e:
                # Log error but don't fail entire repair process
                results[name] = []

        return results
