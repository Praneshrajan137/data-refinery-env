"""DataForge custom exception hierarchy with structured error context."""

from typing import Any, Optional


class DataForgeError(Exception):
    """Base exception for all DataForge errors."""

    def __init__(
        self,
        message: str,
        context: Optional[dict[str, Any]] = None,
        suggestion: Optional[str] = None,
    ):
        """
        Initialize DataForgeError.

        Args:
            message: User-friendly error message
            context: Additional context about the error (location, values, etc.)
            suggestion: Recovery suggestion for the user
        """
        self.message = message
        self.context = context or {}
        self.suggestion = suggestion
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format complete error message with context and suggestion."""
        parts = [self.message]
        if self.context:
            context_str = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
            parts.append(f"[{context_str}]")
        if self.suggestion:
            parts.append(f"→ {self.suggestion}")
        return " ".join(parts)


class IOError(DataForgeError):
    """Raised for file I/O and parsing errors."""

    pass


class SchemaError(DataForgeError):
    """Raised for schema-related errors (inference, validation, etc.)."""

    pass


class DetectionError(DataForgeError):
    """Raised when detection of issues fails."""

    pass


class RepairError(DataForgeError):
    """Raised when repair proposal or application fails."""

    pass


class VerificationError(DataForgeError):
    """Raised when repair verification fails."""

    pass


class ConfigError(DataForgeError):
    """Raised for configuration errors."""

    pass


class TransactionError(DataForgeError):
    """Raised for transaction-related errors (audit trail, revert, etc.)."""

    pass


class ValidationError(DataForgeError):
    """Raised when data validation fails."""

    pass
