"""Utility functions for DataForge."""

from pathlib import Path
from typing import Any, Optional

from dataforge.exceptions import DataForgeError


def ensure_path_exists(path: str, create_parent: bool = False) -> Path:
    """
    Ensure a path is valid and exists.

    Args:
        path: File path
        create_parent: Whether to create parent directories

    Returns:
        Path object

    Raises:
        DataForgeError: If path is invalid
    """
    p = Path(path)

    if create_parent:
        p.parent.mkdir(parents=True, exist_ok=True)

    return p


def safe_read_value(obj: dict, key: str, default: Any = None) -> Any:
    """Safely read dictionary value with default."""
    return obj.get(key, default)


def format_size(size: int) -> str:
    """Format byte size as human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format float as percentage string."""
    return f"{value * 100:.{decimals}f}%"


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Flatten nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
