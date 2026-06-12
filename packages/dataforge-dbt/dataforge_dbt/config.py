"""Configuration loading for the DataForge dbt integration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

Mode = Literal["dry_run", "apply", "refuse"]

_VALID_MODES: set[str] = {"dry_run", "apply", "refuse"}


class DataForgeDbtConfigError(ValueError):
    """Raised when the DataForge dbt integration configuration is invalid.

    Args:
        message: User-facing description of the invalid configuration.
    """


@dataclass(frozen=True)
class DataForgeDbtConfig:
    """Runtime configuration for a DataForge dbt hook invocation.

    Args:
        mode: Hook behavior: log only, write a transaction artifact, or fail on unsafe issues.
        target_path: dbt target directory used for DataForge transaction artifacts.
        profile_name: Optional dbt profile name used to find a scoped integration block.

    Example:
        >>> config = DataForgeDbtConfig(mode="dry_run", target_path=Path("target"))
        >>> config.transaction_dir
        PosixPath('target/dataforge_txns')
    """

    mode: Mode
    target_path: Path
    profile_name: str | None = None
    row_identity_columns: tuple[str, ...] = ()

    @property
    def transaction_dir(self) -> Path:
        """Return the target directory for DataForge dbt transaction artifacts.

        Returns:
            Path under the dbt target directory where transaction JSONL files are written.
        """
        return self.target_path / "dataforge_txns"


def parse_mode(value: str) -> Mode:
    """Validate and normalize a DataForge dbt mode value.

    Args:
        value: Raw mode string supplied by a dbt macro or integration block.

    Returns:
        A validated mode literal.

    Raises:
        DataForgeDbtConfigError: If the mode is not one of dry_run, apply, or refuse.
    """
    normalized = value.strip().lower()
    if normalized not in _VALID_MODES:
        raise DataForgeDbtConfigError(
            "DataForge dbt mode must be one of 'dry_run', 'apply', or 'refuse'."
        )
    return normalized  # type: ignore[return-value]


def load_config(
    *,
    mode: str,
    target_path: Path,
    profiles_path: Path | None = None,
    profile_name: str | None = None,
) -> DataForgeDbtConfig:
    """Load DataForge dbt configuration from arguments and optional profiles.yml.

    Args:
        mode: Mode supplied by the dbt macro invocation.
        target_path: dbt target directory.
        profiles_path: Optional path to dbt profiles.yml.
        profile_name: Optional profile name whose ``dataforge`` block should be read.

    Returns:
        Parsed immutable runtime configuration.

    Raises:
        DataForgeDbtConfigError: If the profile block or mode is invalid.
    """
    profile_block = _read_profile_block(profiles_path=profiles_path, profile_name=profile_name)
    configured_mode = mode
    configured_target = (
        Path(str(profile_block.get("target_path", target_path))) if profile_block else target_path
    )
    env_target = os.environ.get("DATAFORGE_DBT_TARGET_PATH")
    resolved_target = Path(env_target) if env_target else configured_target
    return DataForgeDbtConfig(
        mode=parse_mode(configured_mode),
        target_path=resolved_target,
        profile_name=profile_name,
        row_identity_columns=_parse_row_identity_columns(profile_block),
    )


def _parse_row_identity_columns(profile_block: dict[str, Any]) -> tuple[str, ...]:
    """Return optional row identity columns from the profile integration block."""
    raw = profile_block.get("row_identity_columns", profile_block.get("row_id"))
    if raw is None:
        return ()
    if isinstance(raw, str):
        return tuple(part.strip() for part in raw.split(",") if part.strip())
    if isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        return tuple(item.strip() for item in raw if item.strip())
    raise DataForgeDbtConfigError(
        "dataforge.row_identity_columns must be a string or list of strings."
    )


def _read_profile_block(
    *,
    profiles_path: Path | None,
    profile_name: str | None,
) -> dict[str, Any]:
    """Read the optional ``dataforge`` integration block from profiles.yml.

    Args:
        profiles_path: Optional dbt profiles.yml path.
        profile_name: Optional dbt profile name.

    Returns:
        The parsed dataforge block, or an empty dict when no block exists.

    Raises:
        DataForgeDbtConfigError: If profiles.yml is malformed.
    """
    if profiles_path is None or profile_name is None or not profiles_path.exists():
        return {}

    try:
        loaded = yaml.safe_load(profiles_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise DataForgeDbtConfigError(f"Could not parse dbt profiles.yml: {exc}") from exc
    except OSError as exc:
        raise DataForgeDbtConfigError(f"Could not read dbt profiles.yml: {exc}") from exc

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise DataForgeDbtConfigError("dbt profiles.yml must contain a mapping at the top level.")

    profile = loaded.get(profile_name)
    if profile is None:
        return {}
    if not isinstance(profile, dict):
        raise DataForgeDbtConfigError(f"dbt profile '{profile_name}' must be a mapping.")

    block = profile.get("dataforge", {})
    if block is None:
        return {}
    if not isinstance(block, dict):
        raise DataForgeDbtConfigError(
            f"dbt profile '{profile_name}' has a dataforge block that is not a mapping."
        )
    return block
