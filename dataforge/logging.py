"""Structured logging for DataForge."""

import logging
from typing import Optional

from dataforge.config import LogLevel


def setup_logging(level: LogLevel = LogLevel.INFO, debug: bool = False) -> None:
    """
    Set up structured logging.

    Args:
        level: Logging level
        debug: Enable debug mode
    """
    log_level = logging.DEBUG if debug else getattr(logging, level.value.upper())

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)
