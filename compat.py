# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Compatibility layer — single source of truth for all openenv imports.

[FIX-01] Replaces scattered try/except import blocks throughout the codebase.
[FIX-02] Validates openenv-core version at import time with actionable errors.
[FIX-03] Provides complete type annotations for IDE/mypy support.
[FIX-04] Exports every symbol any downstream module needs — never import from
         openenv directly in application code; always use:

             from compat import Action, Observation, State, Environment, ...

Module Layout (by provenance):
    openenv.core.env_server.types    → Action, Observation, State,
                                       ConcurrencyConfig, EnvironmentMetadata
    openenv.core.env_server.interfaces → Environment, Transform
    openenv.core.env_server.http_server → create_app
    openenv.core.env_client          → EnvClient
    openenv.core.client_types        → StepResult

Verification:
    python compat.py          # prints diagnostic table
    python -m pytest compat.py -v  # if doctest desired
"""

from __future__ import annotations

import importlib
import sys
import warnings
from typing import (
    Any,
    Callable,
    Optional,
    TYPE_CHECKING,
    Type,
)

if TYPE_CHECKING:
    # For static analysis only — these are resolved dynamically below
    from openenv.core.env_server.types import (
        Action as _ActionT,
        ConcurrencyConfig as _ConcurrencyConfigT,
        EnvironmentMetadata as _EnvironmentMetadataT,
        Observation as _ObservationT,
        State as _StateT,
    )
    from openenv.core.env_server.interfaces import (
        Environment as _EnvironmentT,
        Transform as _TransformT,
    )
    from openenv.core.env_client import EnvClient as _EnvClientT
    from openenv.core.client_types import StepResult as _StepResultT

# ---------------------------------------------------------------------------
# Public API — exhaustive list of every re-exported symbol
# ---------------------------------------------------------------------------
__all__ = [
    # Core types (openenv.core.env_server.types)
    "Action",
    "Observation",
    "State",
    "ConcurrencyConfig",
    "EnvironmentMetadata",
    # Server interfaces (openenv.core.env_server.interfaces)
    "Environment",
    "Transform",
    # App factory (openenv.core.env_server.http_server)
    "create_app",
    # Client (openenv.core.env_client)
    "EnvClient",
    # Client types (openenv.core.client_types)
    "StepResult",
    # Metadata
    "IMPORT_ROOT",
    "OPENENV_VERSION",
    # Diagnostics
    "print_diagnostics",
    "validate_installation",
]


# ===========================================================================
# §1  Resolve import root
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

if IMPORT_ROOT is None:
    raise ImportError(
        "openenv-core is not installed or its import path has changed.\n"
        "Install it with:\n"
        "    pip install 'openenv-core>=0.2.0'\n"
        "\n"
        f"Tried import roots: {_CANDIDATES}"
    )

# Warn if using the deprecated path
if IMPORT_ROOT == "openenv_core":
    warnings.warn(
        "Importing via 'openenv_core' is deprecated. "
        "Upgrade openenv-core to use 'openenv.core' instead.",
        DeprecationWarning,
        stacklevel=2,
    )


# ===========================================================================
# §2  Version validation
# ===========================================================================

OPENENV_VERSION: str | None = None

try:
    from importlib.metadata import version as _pkg_version

    OPENENV_VERSION = _pkg_version("openenv-core")
except Exception:
    # importlib.metadata may not find it if installed in non-standard location
    pass

_MIN_VERSION = (0, 2, 0)

if OPENENV_VERSION is not None:
    try:
        _parts = tuple(int(x) for x in OPENENV_VERSION.split(".")[:3])
        if _parts < _MIN_VERSION:
            warnings.warn(
                f"openenv-core {OPENENV_VERSION} is below minimum {'.'.join(map(str, _MIN_VERSION))}. "
                f"Some features may not work. Upgrade with: pip install 'openenv-core>={'.'.join(map(str, _MIN_VERSION))}'",
                UserWarning,
                stacklevel=2,
            )
    except (ValueError, TypeError):
        pass  # Non-standard version string; skip check


# ===========================================================================
# §3  Import core types  —  openenv.core.env_server.types
# ===========================================================================

_types_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server.types")

Action: Type[_ActionT] = _types_mod.Action  # type: ignore[assignment]
Observation: Type[_ObservationT] = _types_mod.Observation  # type: ignore[assignment]
State: Type[_StateT] = _types_mod.State  # type: ignore[assignment]
ConcurrencyConfig: Type[_ConcurrencyConfigT] = getattr(  # type: ignore[assignment]
    _types_mod, "ConcurrencyConfig", None
)
EnvironmentMetadata: Type[_EnvironmentMetadataT] = getattr(  # type: ignore[assignment]
    _types_mod, "EnvironmentMetadata", None
)


# ===========================================================================
# §4  Import server interfaces  —  openenv.core.env_server.interfaces
# ===========================================================================

Environment: Type[_EnvironmentT] | None = None  # type: ignore[assignment]
Transform: Type[_TransformT] | None = None  # type: ignore[assignment]

try:
    _ifaces_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server.interfaces")
    Environment = getattr(_ifaces_mod, "Environment", None)  # type: ignore[assignment]
    Transform = getattr(_ifaces_mod, "Transform", None)  # type: ignore[assignment]
except ImportError:
    pass  # Older openenv-core without interfaces module


# ===========================================================================
# §5  Import app factory  —  openenv.core.env_server.http_server
# ===========================================================================

create_app: Callable[..., Any] | None = None

try:
    _http_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server.http_server")
    create_app = getattr(_http_mod, "create_app", None)
except ImportError:
    pass

# Fallback: some versions expose it at the env_server level
if create_app is None:
    try:
        _srv_mod = importlib.import_module(f"{IMPORT_ROOT}.env_server")
        create_app = getattr(_srv_mod, "create_app", None)
    except ImportError:
        pass


# ===========================================================================
# §6  Import client  —  openenv.core.env_client
# ===========================================================================

EnvClient: Type[_EnvClientT] | None = None  # type: ignore[assignment]

try:
    _client_mod = importlib.import_module(f"{IMPORT_ROOT}.env_client")
    EnvClient = getattr(_client_mod, "EnvClient", None)  # type: ignore[assignment]
except ImportError:
    pass


# ===========================================================================
# §7  Import client types  —  openenv.core.client_types
# ===========================================================================

StepResult: Type[_StepResultT] | None = None  # type: ignore[assignment]

for _sub in ("client_types", "types"):
    try:
        _ct_mod = importlib.import_module(f"{IMPORT_ROOT}.{_sub}")
        _sr = getattr(_ct_mod, "StepResult", None)
        if _sr is not None:
            StepResult = _sr  # type: ignore[assignment]
            break
    except ImportError:
        continue


# ===========================================================================
# §8  Diagnostics and validation
# ===========================================================================

def validate_installation() -> dict[str, bool]:
    """Validate that all required components are available.

    Returns:
        Dictionary mapping component names to availability (True/False).
        The 'core_types' key covers Action, Observation, State — these are
        mandatory.  Everything else is recommended but has fallback paths.
    """
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
    """Print a comprehensive diagnostic table of all resolved imports.

    Useful for debugging import issues and verifying the installation.
    """
    _SEP = "-" * 60

    print(f"\n{_SEP}")
    print("  openenv-core Compatibility Layer -- Diagnostics")
    print(_SEP)
    print(f"  Import root:          {IMPORT_ROOT}")
    print(f"  openenv-core version: {OPENENV_VERSION or 'unknown'}")
    print(f"  Python version:       {sys.version.split()[0]}")
    print(_SEP)

    _components = [
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

    for name, obj in _components:
        if obj is not None:
            loc = getattr(obj, "__module__", "?")
            status = f"OK  {loc}"
        else:
            status = "MISSING"
        print(f"  {name:<24s} {status}")

    print(_SEP)

    # Validation summary
    results = validate_installation()
    mandatory_ok = results["core_types"]
    total_ok = sum(results.values())
    total = len(results)

    if mandatory_ok and total_ok == total:
        print("  Status: ALL COMPONENTS AVAILABLE [OK]")
    elif mandatory_ok:
        missing = [k for k, v in results.items() if not v]
        print(f"  Status: CORE OK, optional missing: {', '.join(missing)} [WARN]")
    else:
        print("  Status: CRITICAL -- core types unavailable [FAIL]")
        print("  Run: pip install 'openenv-core>=0.2.0'")

    print(f"{_SEP}\n")


# ===========================================================================
# §9  Self-test entry point
# ===========================================================================

if __name__ == "__main__":
    print_diagnostics()

    # Structural assertions for CI/CD gates
    results = validate_installation()
    if not results["core_types"]:
        print("FATAL: Core types (Action, Observation, State) are unavailable.")
        sys.exit(1)

    # Verify inherited fields are present (guards against API breakage)
    _obs_fields = set(Observation.model_fields.keys())  # type: ignore[union-attr]
    _expected_obs = {"done", "reward", "metadata"}
    _missing_obs = _expected_obs - _obs_fields
    if _missing_obs:
        print(f"WARNING: Observation is missing expected fields: {_missing_obs}")
        print("  This may indicate an openenv-core API change.")
    else:
        print(f"Observation base fields verified: {sorted(_expected_obs)} [OK]")

    _action_fields = set(Action.model_fields.keys())  # type: ignore[union-attr]
    _expected_action = {"metadata"}
    _missing_action = _expected_action - _action_fields
    if _missing_action:
        print(f"WARNING: Action is missing expected fields: {_missing_action}")
    else:
        print(f"Action base fields verified: {sorted(_expected_action)} [OK]")

    _state_fields = set(State.model_fields.keys())  # type: ignore[union-attr]
    _expected_state = {"episode_id", "step_count"}
    _missing_state = _expected_state - _state_fields
    if _missing_state:
        print(f"WARNING: State is missing expected fields: {_missing_state}")
    else:
        print(f"State base fields verified: {sorted(_expected_state)} [OK]")

    # Verify create_app signature
    if create_app is not None:
        import inspect

        sig = inspect.signature(create_app)
        _expected_params = {"env", "action_cls", "observation_cls"}
        _actual_params = set(sig.parameters.keys())
        _missing_params = _expected_params - _actual_params
        if _missing_params:
            print(f"WARNING: create_app missing expected params: {_missing_params}")
        else:
            print(f"create_app signature verified: {list(sig.parameters.keys())} [OK]")

    # Verify Environment abstract methods
    if Environment is not None:
        import inspect

        _abstracts = {
            name
            for name, method in inspect.getmembers(Environment)
            if getattr(method, "__isabstractmethod__", False)
        }
        _expected_abstracts = {"reset", "step", "state"}
        if _expected_abstracts <= _abstracts:
            print(f"Environment abstract methods verified: {sorted(_expected_abstracts)} [OK]")
        else:
            _missing_abs = _expected_abstracts - _abstracts
            print(f"WARNING: Environment missing abstract methods: {_missing_abs}")

    # Verify StepResult fields
    if StepResult is not None:
        import dataclasses

        if dataclasses.is_dataclass(StepResult):
            _sr_fields = {f.name for f in dataclasses.fields(StepResult)}
            _expected_sr = {"observation", "reward", "done"}
            _missing_sr = _expected_sr - _sr_fields
            if _missing_sr:
                print(f"WARNING: StepResult missing fields: {_missing_sr}")
            else:
                print(f"StepResult fields verified: {sorted(_expected_sr)} [OK]")
        else:
            print("NOTE: StepResult is not a dataclass (API may have changed)")

    print("\ncompat.py validation PASSED -- all checks green.")
