# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Data Quality Validation & Cleaning Pipeline — public package API.

This is the canonical import surface for the data_quality_env package.
All public types, enums, constants, and the client class are re-exported
here for convenient access::

    from data_quality_env import (
        DataQualityAction,
        DataQualityObservation,
        DataQualityState,
        DataQualityEnv,
        IssueType,
        FixType,
    )

The ``DataQualityEnv`` client may be ``None`` if openenv-core's
``EnvClient`` and ``websocket-client`` are both unavailable.  Downstream
code should guard accordingly.
"""

from .models import (
    DataQualityAction,
    DataQualityObservation,
    DataQualityState,
    IssueType,
    FixType,
    ActionResult,
    RemainingHint,
    MAX_INSPECT_ROWS,
    MAX_INSPECT_COLUMNS,
    DEFAULT_MAX_STEPS,
)

try:
    from .client import DataQualityEnv
except ImportError:
    DataQualityEnv = None  # type: ignore[assignment,misc]

__all__ = [
    # Models
    "DataQualityAction",
    "DataQualityObservation",
    "DataQualityState",
    # Enums
    "IssueType",
    "FixType",
    "ActionResult",
    "RemainingHint",
    # Constants
    "MAX_INSPECT_ROWS",
    "MAX_INSPECT_COLUMNS",
    "DEFAULT_MAX_STEPS",
    # Client (may be None if dependencies missing)
    "DataQualityEnv",
]
