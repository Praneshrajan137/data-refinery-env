"""Detector base class and registry."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from dataforge.models import Issue, Schema
from dataforge.types import TableData


class Detector(ABC):
    """Base class for data quality detectors."""

    name: str
    """Name identifier for this detector."""

    description: str = "Data quality detector"
    """Description of what this detector identifies."""

    @abstractmethod
    def detect(
        self, data: TableData, schema: Optional[Schema] = None
    ) -> list[Issue]:
        """
        Detect issues in the data.

        Args:
            data: List of row dictionaries
            schema: Schema to validate against (optional)

        Returns:
            List of detected issues
        """
        pass

    def __call__(
        self, data: TableData, schema: Optional[Schema] = None
    ) -> list[Issue]:
        """Allow detector to be called directly."""
        return self.detect(data, schema)


class DetectorRegistry:
    """Registry for managing detectors."""

    _detectors: dict[str, Detector] = {}

    @classmethod
    def register(cls, detector: Detector) -> None:
        """Register a detector."""
        cls._detectors[detector.name] = detector

    @classmethod
    def unregister(cls, name: str) -> None:
        """Unregister a detector by name."""
        if name in cls._detectors:
            del cls._detectors[name]

    @classmethod
    def get(cls, name: str) -> Optional[Detector]:
        """Get detector by name."""
        return cls._detectors.get(name)

    @classmethod
    def get_all(cls) -> dict[str, Detector]:
        """Get all registered detectors."""
        return cls._detectors.copy()

    @classmethod
    def get_names(cls) -> list[str]:
        """Get names of all registered detectors."""
        return list(cls._detectors.keys())

    @classmethod
    def detect_all(
        cls, data: TableData, schema: Optional[Schema] = None, enabled: Optional[list[str]] = None
    ) -> dict[str, list[Issue]]:
        """
        Run all detectors (or only enabled ones).

        Args:
            data: Data to analyze
            schema: Optional schema
            enabled: List of detector names to run (None = run all)

        Returns:
            Dict mapping detector name to list of issues
        """
        detectors = cls._detectors
        if enabled:
            detectors = {k: v for k, v in detectors.items() if k in enabled}

        results = {}
        for name, detector in detectors.items():
            try:
                results[name] = detector.detect(data, schema)
            except Exception as e:
                # Log error but don't fail entire detection
                results[name] = []

        return results
