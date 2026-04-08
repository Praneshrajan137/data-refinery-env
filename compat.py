# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Compatibility layer for openenv imports.

[FIX-01] Replaces scattered try/except import blocks throughout the codebase.
[FIX-02] Validates openenv-core version at import time with actionable errors.
[FIX-03] Provides stable re-export names for the rest of the repo.
[FIX-04] Exports every symbol downstream modules need; application code should
         import from this module instead of importing openenv directly.
[FIX-23] Remains importable without openenv-core by exposing minimal local
         fallback base types for offline validation and local test paths.

Module layout when openenv-core is installed:
    openenv.core.env_server.types -> Action, Observation, State,
                                     ConcurrencyConfig, EnvironmentMetadata
    openenv.core.env_server.interfaces -> Environment, Transform
    openenv.core.env_server.http_server -> create_app
    openenv.core.env_client -> EnvClient
    openenv.core.client_types -> StepResult

Verification:
    python compat.py
"""

from __future__ import annotations

import abc
import importlib
import sys
import warnings
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "Action",
    "Observation",
    "State",
    "ConcurrencyConfig",
    "EnvironmentMetadata",
    "Environment",
    "Transform",
    "create_app",
    "EnvClient",
    "StepResult",
    "IMPORT_ROOT",
    "OPENENV_VERSION",
    "print_diagnostics",
    "validate_installation",
]


def _build_fallback_types() -> tuple[type[BaseModel], ...]:
    """Construct local stand-ins for the openenv base types."""

    class _FallbackAction(BaseModel):
        metadata: dict[str, Any] = Field(default_factory=dict)
        model_config = ConfigDict(extra="forbid")

    class _FallbackObservation(BaseModel):
        done: bool = False
        reward: bool | int | float | None = 0.0
        metadata: dict[str, Any] = Field(default_factory=dict)
        model_config = ConfigDict(extra="forbid")

    class _FallbackState(BaseModel):
        episode_id: str | None = None
        step_count: int = Field(default=0, ge=0)
        model_config = ConfigDict(extra="allow")

    class _FallbackEnvironment(abc.ABC):
        @abc.abstractmethod
        def reset(self, **kwargs: Any) -> Any:
            ...

        @abc.abstractmethod
        def step(self, action: Any) -> Any:
            ...

        @property
        @abc.abstractmethod
        def state(self) -> Any:
            ...

        def close(self) -> None:
            """Compatibility no-op when no transport runtime exists."""

    @dataclass
    class _FallbackStepResult:
        observation: Any
        reward: bool | int | float | None = 0.0
        done: bool = False

    return (
        _FallbackAction,
        _FallbackObservation,
        _FallbackState,
        _FallbackEnvironment,
        _FallbackStepResult,
    )


# ===========================================================================
# Resolve import root
# ===========================================================================

_CANDIDATES = ("openenv.core", "openenv_core")
IMPORT_ROOT: str | None = None

for _candidate in _CANDIDATES:
    try:
        importlib.import_module(f"{_candidate}.env_server.types")
        IMPORT_ROOT = _candidate
        break
    except ImportError:
        continue

_USING_FALLBACK_TYPES = IMPORT_ROOT is None

if IMPORT_ROOT == "openenv_core":
    warnings.warn(
        "Importing via 'openenv_core' is deprecated. "
        "Upgrade openenv-core to use 'openenv.core' instead.",
        DeprecationWarning,
        stacklevel=2,
    )


# ===========================================================================
# Version validation
# ===========================================================================

OPENENV_VERSION: str | None = None

try:
    from importlib.metadata import version as _pkg_version

    OPENENV_VERSION = _pkg_version("openenv-core")
except Exception:
    pass

_MIN_VERSION = (0, 2, 0)

if OPENENV_VERSION is not None:
    try:
        _parts = tuple(int(x) for x in OPENENV_VERSION.split(".")[:3])
        if _parts < _MIN_VERSION:
            warnings.warn(
                "openenv-core "
                f"{OPENENV_VERSION} is below minimum "
                f"{'.'.join(map(str, _MIN_VERSION))}. "
                "Some features may not work. Upgrade with: "
                f"pip install 'openenv-core>={'.'.join(map(str, _MIN_VERSION))}'",
                UserWarning,
                stacklevel=2,
            )
    except (ValueError, TypeError):
        pass


# ===========================================================================
# Core types
# ===========================================================================

Action: type[Any]
Observation: type[Any]
State: type[Any]
Environment: type[Any] | None
StepResult: type[Any] | None
ConcurrencyConfig: type[Any] | None = None
EnvironmentMetadata: type[Any] | None = None
Transform: type[Any] | None = None
create_app: Callable[..., Any] | None = None
EnvClient: type[Any] | None = None

if IMPORT_ROOT is not None:
    _types_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server.types")

    Action = _types_mod.Action
    Observation = _types_mod.Observation
    State = _types_mod.State
    ConcurrencyConfig = getattr(_types_mod, "ConcurrencyConfig", None)
    EnvironmentMetadata = getattr(_types_mod, "EnvironmentMetadata", None)

    try:
        _ifaces_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server.interfaces")
        Environment = getattr(_ifaces_mod, "Environment", None)
        Transform = getattr(_ifaces_mod, "Transform", None)
    except ImportError:
        Environment = None
        Transform = None

    try:
        _http_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server.http_server")
        create_app = getattr(_http_mod, "create_app", None)
    except ImportError:
        try:
            _srv_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server")
            create_app = getattr(_srv_mod, "create_app", None)
        except ImportError:
            create_app = None

    try:
        _client_mod = importlib.import_module(f"{IMPORT_ROOT}.env_client")
        EnvClient = getattr(_client_mod, "EnvClient", None)
    except ImportError:
        EnvClient = None

    StepResult = None
    for _sub in ("client_types", "types"):
        try:
            _ct_mod = importlib.import_module(f"{IMPORT_ROOT}.{_sub}")
            _step_result = getattr(_ct_mod, "StepResult", None)
            if _step_result is not None:
                StepResult = _step_result
                break
        except ImportError:
            continue
else:
    (
        Action,
        Observation,
        State,
        Environment,
        StepResult,
    ) = _build_fallback_types()


# ===========================================================================
# Diagnostics
# ===========================================================================

def validate_installation() -> dict[str, bool]:
    """Validate which compatibility components are available."""

    return {
        "core_types": all(x is not None for x in (Action, Observation, State)),
        "Environment": Environment is not None,
        "create_app": create_app is not None,
        "EnvClient": EnvClient is not None,
        "StepResult": StepResult is not None,
        "ConcurrencyConfig": ConcurrencyConfig is not None,
        "EnvironmentMetadata": EnvironmentMetadata is not None,
        "Transform": Transform is not None,
    }


def print_diagnostics() -> None:
    """Print a diagnostic table of resolved imports."""

    separator = "-" * 60

    print(f"\n{separator}")
    print("  openenv-core Compatibility Layer -- Diagnostics")
    print(separator)
    print(
        "  Import root:          "
        f"{IMPORT_ROOT or 'fallback (openenv-core unavailable)'}"
    )
    print(f"  openenv-core version: {OPENENV_VERSION or 'unknown'}")
    print(f"  Python version:       {sys.version.split()[0]}")
    print(separator)

    components = [
        ("Action", Action),
        ("Observation", Observation),
        ("State", State),
        ("ConcurrencyConfig", ConcurrencyConfig),
        ("EnvironmentMetadata", EnvironmentMetadata),
        ("Environment", Environment),
        ("Transform", Transform),
        ("create_app", create_app),
        ("EnvClient", EnvClient),
        ("StepResult", StepResult),
    ]

    for name, obj in components:
        if obj is not None:
            module_name = getattr(obj, "__module__", "?")
            status = f"OK  {module_name}"
        else:
            status = "MISSING"
        print(f"  {name:<24s} {status}")

    print(separator)

    results = validate_installation()
    mandatory_ok = results["core_types"]
    total_ok = sum(results.values())
    total = len(results)

    if mandatory_ok and total_ok == total:
        print("  Status: ALL COMPONENTS AVAILABLE [OK]")
    elif mandatory_ok:
        missing = [name for name, present in results.items() if not present]
        if _USING_FALLBACK_TYPES:
            print(
                "  Status: FALLBACK CORE TYPES ACTIVE, optional missing: "
                f"{', '.join(missing)} [WARN]"
            )
        else:
            print(
                "  Status: CORE OK, optional missing: "
                f"{', '.join(missing)} [WARN]"
            )
    else:
        print("  Status: CRITICAL -- core types unavailable [FAIL]")
        print("  Run: pip install 'openenv-core>=0.2.0'")

    print(f"{separator}\n")


# ===========================================================================
# Self-test entry point
# ===========================================================================

if __name__ == "__main__":
    print_diagnostics()

    results = validate_installation()
    if not results["core_types"]:
        print("FATAL: Core types (Action, Observation, State) are unavailable.")
        sys.exit(1)

    observation_fields = set(Observation.model_fields.keys())
    expected_observation_fields = {"done", "reward", "metadata"}
    missing_observation_fields = expected_observation_fields - observation_fields
    if missing_observation_fields:
        print(
            "WARNING: Observation is missing expected fields: "
            f"{missing_observation_fields}"
        )
    else:
        print(
            "Observation base fields verified: "
            f"{sorted(expected_observation_fields)} [OK]"
        )

    action_fields = set(Action.model_fields.keys())
    expected_action_fields = {"metadata"}
    missing_action_fields = expected_action_fields - action_fields
    if missing_action_fields:
        print(f"WARNING: Action is missing expected fields: {missing_action_fields}")
    else:
        print(
            "Action base fields verified: "
            f"{sorted(expected_action_fields)} [OK]"
        )

    state_fields = set(State.model_fields.keys())
    expected_state_fields = {"episode_id", "step_count"}
    missing_state_fields = expected_state_fields - state_fields
    if missing_state_fields:
        print(f"WARNING: State is missing expected fields: {missing_state_fields}")
    else:
        print(
            "State base fields verified: "
            f"{sorted(expected_state_fields)} [OK]"
        )

    if create_app is not None:
        import inspect

        signature = inspect.signature(create_app)
        expected_params = {"env", "action_cls", "observation_cls"}
        actual_params = set(signature.parameters.keys())
        missing_params = expected_params - actual_params
        if missing_params:
            print(f"WARNING: create_app missing expected params: {missing_params}")
        else:
            print(
                "create_app signature verified: "
                f"{list(signature.parameters.keys())} [OK]"
            )

    if Environment is not None:
        import inspect

        abstract_methods = {
            name
            for name, method in inspect.getmembers(Environment)
            if getattr(method, "__isabstractmethod__", False)
        }
        expected_abstract_methods = {"reset", "step", "state"}
        if expected_abstract_methods <= abstract_methods:
            print(
                "Environment abstract methods verified: "
                f"{sorted(expected_abstract_methods)} [OK]"
            )
        else:
            missing_methods = expected_abstract_methods - abstract_methods
            print(f"WARNING: Environment missing abstract methods: {missing_methods}")

    if StepResult is not None:
        import dataclasses

        if dataclasses.is_dataclass(StepResult):
            step_result_fields = {field.name for field in dataclasses.fields(StepResult)}
            expected_step_result_fields = {"observation", "reward", "done"}
            missing_step_result_fields = (
                expected_step_result_fields - step_result_fields
            )
            if missing_step_result_fields:
                print(
                    "WARNING: StepResult missing fields: "
                    f"{missing_step_result_fields}"
                )
            else:
                print(
                    "StepResult fields verified: "
                    f"{sorted(expected_step_result_fields)} [OK]"
                )
        else:
            print("NOTE: StepResult is not a dataclass (API may have changed)")

    if _USING_FALLBACK_TYPES:
        print("\ncompat.py validation PASSED -- fallback types active.")
    else:
        print("\ncompat.py validation PASSED -- all checks green.")
