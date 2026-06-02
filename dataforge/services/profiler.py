"""Data profiling service."""

from dataclasses import dataclass
from typing import Optional

from dataforge.config import DataForgeConfig
from dataforge.core import DetectorRegistry
from dataforge.io import CSVReader
from dataforge.models import Issue, Schema
from dataforge.types import CSVPath, TableData


@dataclass
class ProfileResult:
    """Result of data profiling."""

    data: TableData
    """The data that was profiled."""

    schema: Optional[Schema] = None
    """Inferred schema for the data."""

    issues: dict[str, list[Issue]] = None
    """Issues found grouped by detector name."""

    total_issues: int = 0
    """Total number of issues found."""

    row_count: int = 0
    """Number of rows in data."""

    column_count: int = 0
    """Number of columns in data."""

    def __post_init__(self):
        """Initialize default values."""
        if self.issues is None:
            self.issues = {}
        self.row_count = len(self.data)
        if self.data:
            self.column_count = len(self.data[0])
        self.total_issues = sum(len(issues) for issues in self.issues.values())

    def get_all_issues(self) -> list[Issue]:
        """Get all issues as flat list."""
        return [issue for issues in self.issues.values() for issue in issues]

    def get_issues_by_severity(self, severity: str) -> list[Issue]:
        """Get issues filtered by severity."""
        return [
            issue for issue in self.get_all_issues() if issue.severity.value == severity
        ]


class ProfileService:
    """Service for profiling data quality."""

    def __init__(self, config: Optional[DataForgeConfig] = None):
        """
        Initialize profiler service.

        Args:
            config: DataForge configuration
        """
        self.config = config or DataForgeConfig()
        self.csv_reader = CSVReader(
            encoding=self.config.encoding,
            delimiter=self.config.delimiter,
            quote_char=self.config.quote_char,
        )

    def profile_file(self, csv_path: CSVPath, schema: Optional[Schema] = None) -> ProfileResult:
        """
        Profile a CSV file for data quality issues.

        Args:
            csv_path: Path to CSV file
            schema: Optional schema for validation

        Returns:
            ProfileResult with issues found
        """
        # Read data
        data = self.csv_reader.read(csv_path)

        # Infer schema if not provided
        if schema is None:
            from dataforge.io import SchemaStore
            schema = SchemaStore.infer_from_data(data)

        # Run detectors
        issues = DetectorRegistry.detect_all(
            data, schema, enabled=self.config.detectors.enabled_detectors
        )

        # Filter by confidence threshold
        filtered_issues = {}
        for detector_name, detector_issues in issues.items():
            filtered_issues[detector_name] = [
                issue
                for issue in detector_issues
                if self._should_include_issue(issue)
            ]

        return ProfileResult(
            data=data,
            schema=schema,
            issues=filtered_issues,
        )

    def profile_data(
        self, data: TableData, schema: Optional[Schema] = None
    ) -> ProfileResult:
        """
        Profile in-memory data for quality issues.

        Args:
            data: List of row dictionaries
            schema: Optional schema for validation

        Returns:
            ProfileResult with issues found
        """
        # Infer schema if not provided
        if schema is None:
            from dataforge.io import SchemaStore
            schema = SchemaStore.infer_from_data(data)

        # Run detectors
        issues = DetectorRegistry.detect_all(
            data, schema, enabled=self.config.detectors.enabled_detectors
        )

        # Filter by confidence threshold
        filtered_issues = {}
        for detector_name, detector_issues in issues.items():
            filtered_issues[detector_name] = [
                issue
                for issue in detector_issues
                if self._should_include_issue(issue)
            ]

        return ProfileResult(
            data=data,
            schema=schema,
            issues=filtered_issues,
        )

    def _should_include_issue(self, issue: Issue) -> bool:
        """Check if issue should be included based on configuration."""
        # In production, could add more filtering logic here
        return True
