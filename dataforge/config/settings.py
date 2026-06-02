"""Configuration models for DataForge."""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class OutputFormat(str, Enum):
    """Supported output formats."""

    TABLE = "table"
    JSON = "json"
    CSV = "csv"
    YAML = "yaml"


class LogLevel(str, Enum):
    """Logging levels."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class DetectorConfig:
    """Configuration for detectors."""

    enabled_detectors: list[str] = field(default_factory=lambda: [
        "type_mismatch",
        "decimal_shift",
        "fd_violation",
    ])
    """List of detectors to enable."""

    sample_size: Optional[int] = None
    """Sample size for detection (None = analyze all)."""

    confidence_threshold: float = 0.5
    """Minimum confidence for issue detection."""

    type_inference_samples: int = 1000
    """Samples to use for type inference."""


@dataclass
class RepairerConfig:
    """Configuration for repairers."""

    enabled_repairers: list[str] = field(default_factory=lambda: [
        "type_mismatch",
        "decimal_shift",
        "fd_violation",
    ])
    """List of repairers to enable."""

    repair_confidence_threshold: float = 0.7
    """Minimum confidence to apply repair automatically."""

    max_changes_per_row: int = 5
    """Maximum repairs allowed per row."""

    prefer_user_input: bool = True
    """Prefer user input over automatic repairs."""


@dataclass
class VerifierConfig:
    """Configuration for verification."""

    enabled: bool = True
    """Whether to verify repairs with Z3."""

    timeout_seconds: int = 30
    """Z3 solver timeout."""

    use_incremental: bool = True
    """Use incremental Z3 solving."""


@dataclass
class OutputConfig:
    """Configuration for output formatting."""

    format: OutputFormat = OutputFormat.TABLE
    """Output format."""

    colors_enabled: bool = True
    """Whether to use colored output."""

    verbose: bool = False
    """Verbose output."""

    show_context: bool = True
    """Show error context details."""


@dataclass
class DataForgeConfig:
    """Central configuration for DataForge."""

    # Input/output
    input_csv: Optional[str] = None
    """Path to input CSV file."""

    output_csv: Optional[str] = None
    """Path to output CSV file."""

    audit_log: Optional[str] = None
    """Path to audit log file."""

    schema_file: Optional[str] = None
    """Path to schema definition file."""

    # Processing
    encoding: str = "utf-8"
    """CSV file encoding."""

    delimiter: str = ","
    """CSV delimiter."""

    quote_char: str = '"'
    """CSV quote character."""

    # Detection and repair
    detectors: DetectorConfig = field(default_factory=DetectorConfig)
    """Detector configuration."""

    repairers: RepairerConfig = field(default_factory=RepairerConfig)
    """Repairer configuration."""

    verifier: VerifierConfig = field(default_factory=VerifierConfig)
    """Verification configuration."""

    # Output
    output: OutputConfig = field(default_factory=OutputConfig)
    """Output configuration."""

    # Logging
    log_level: LogLevel = LogLevel.INFO
    """Logging level."""

    debug_mode: bool = False
    """Enable debug mode."""

    # Custom options
    custom: dict[str, Any] = field(default_factory=dict)
    """Custom configuration options."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataForgeConfig":
        """Create config from dictionary."""
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "input_csv": self.input_csv,
            "output_csv": self.output_csv,
            "audit_log": self.audit_log,
            "schema_file": self.schema_file,
            "encoding": self.encoding,
            "delimiter": self.delimiter,
            "quote_char": self.quote_char,
            "log_level": self.log_level.value,
            "debug_mode": self.debug_mode,
        }

    def __str__(self) -> str:
        """Return string representation of config."""
        return f"DataForgeConfig(input={self.input_csv}, output={self.output_csv})"
