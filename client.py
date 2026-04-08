# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Client for interacting with the Data Quality environment server.

This module provides ``DataQualityEnv`` — a typed WebSocket client that
connects to the Data Quality environment server and exposes a Pythonic
API for reset / step / state operations.

Resolution Strategy:
    1. **Primary** — subclass ``openenv.core.EnvClient`` if available
       (preferred: handles connection lifecycle, WebSocket framing,
       reconnection, and Docker integration out of the box).
    2. **Fallback** — lightweight synchronous WebSocket wrapper for
       environments where openenv-core is not installed.

Usage (openenv-core available)::

    from data_quality_env.client import DataQualityEnv
    from data_quality_env.models import DataQualityAction, IssueType, FixType

    with DataQualityEnv(base_url="http://localhost:7860") as client:
        result = client.reset(task_id="task_1_format_fixer")
        print(result.observation.message)

        # Diagnose an issue
        result = client.step(DataQualityAction.diagnose(
            row_index=3, column_name="email",
            issue_type=IssueType.FORMAT_ERROR,
        ))
        print(f"Result: {result.observation.action_result}")
        print(f"Reward: {result.observation.reward_delta:+.2f}")

        # Fix it
        result = client.step(DataQualityAction.fix(
            row_index=3, column_name="email",
            fix_type=FixType.CORRECT_VALUE,
            new_value="john.doe@example.com",
            justification="Missing @ symbol between 'doe' and 'example'.",
        ))

        # Finalize
        result = client.step(DataQualityAction.finalize())
        print(f"Final score: {result.observation.cumulative_reward:.4f}")

Usage with Docker::

    client = DataQualityEnv.from_docker_image("data_quality_env-env:latest")
    try:
        result = client.reset(task_id="task_2_duplicate_detective")
        # ... interact ...
    finally:
        client.close()

Bug fixes from review:
    [FIX-19]  Preserves openenv-native client as primary path (not replaced).
    [FIX-20]  Correct ``_step_payload`` using ``model_dump(exclude_none=True)``
              instead of hardcoded field names.
    [FIX-21]  Proper fallback client with clean context manager protocol.
    [FIX-22]  ``_parse_state`` returns ``DataQualityState``.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

# ── Import models (relative when possible, absolute fallback) ─────────────
try:
    from .models import (
        DataQualityAction,
        DataQualityObservation,
        DataQualityState,
    )
except ImportError:
    from models import (  # type: ignore[no-redef]
        DataQualityAction,
        DataQualityObservation,
        DataQualityState,
    )


logger = logging.getLogger("data_quality_env.client")


# ═══════════════════════════════════════════════════════════════════════════
# §1  Try to build the openenv-native client
# ═══════════════════════════════════════════════════════════════════════════

_EnvClient: Optional[type] = None
_StepResult: Optional[type] = None
_State: Optional[type] = None

try:
    from openenv.core import EnvClient
    from openenv.core.client_types import StepResult
    from openenv.core.env_server.types import State

    _EnvClient = EnvClient
    _StepResult = StepResult
    _State = State
except ImportError:
    pass


if _EnvClient is not None:
    # ───────────────────────────────────────────────────────────────────
    # Primary: openenv-native EnvClient subclass
    # ───────────────────────────────────────────────────────────────────

    class DataQualityEnv(
        _EnvClient[DataQualityAction, DataQualityObservation, _State]  # type: ignore[type-var]
    ):
        """WebSocket client for the Data Quality environment.

        Subclasses ``openenv.core.EnvClient`` — inherits connection
        lifecycle management, WebSocket framing, reconnection logic,
        and Docker integration.

        The three ``_*`` methods below define how actions and responses
        are serialized/deserialized over the wire.  Their signatures
        match the openenv-core v0.2.x abstract API.  If a future
        openenv release renames them, update accordingly (the echo_env
        client.py in the openenv repo is the canonical reference).
        """

        def _step_payload(self, action: DataQualityAction) -> Dict[str, Any]:
            """Serialize an action to a JSON-safe dict for the wire.

            [FIX-20] Uses ``model_dump(exclude_none=True)`` to include
            only populated fields, matching the server's Pydantic-based
            action parsing on the other end.
            """
            return action.model_dump(exclude_none=True)

        def _parse_result(
            self, payload: Dict[str, Any]
        ) -> _StepResult:  # type: ignore[type-var]
            """Deserialize a server response into a ``StepResult``.

            The server sends::

                {
                    "observation": { ... DataQualityObservation fields ... },
                    "reward": float,
                    "done": bool,
                }

            We reconstruct the typed observation and wrap it in StepResult.
            """
            obs_data = payload.get("observation", payload)
            obs = DataQualityObservation(**obs_data)
            return _StepResult(  # type: ignore[misc]
                observation=obs,
                reward=obs.reward,
                done=obs.done,
            )

        def _parse_state(
            self, payload: Dict[str, Any]
        ) -> DataQualityState:
            """Deserialize a server state response.

            [FIX-22] Returns ``DataQualityState`` (with all custom fields)
            instead of bare ``State``.
            """
            return DataQualityState(**payload)

else:
    # ───────────────────────────────────────────────────────────────────
    # Fallback: lightweight WebSocket client
    # ───────────────────────────────────────────────────────────────────

    class DataQualityEnv:  # type: ignore[no-redef]
        """Minimal WebSocket client fallback when openenv-core is unavailable.

        Provides the same public API surface (``reset``, ``step``, ``state``)
        as the openenv-native version, using either ``websocket-client`` or
        ``websockets.sync`` for synchronous communication.

        Usage::

            with DataQualityEnv(base_url="http://localhost:7860") as client:
                obs = client.reset(task_id="task_1_format_fixer")
                obs = client.step(DataQualityAction.inspect(row_indices=[0]))
                obs = client.step(DataQualityAction.finalize())
        """

        def __init__(self, base_url: str = "http://localhost:7860") -> None:
            self.ws_url = (
                base_url
                .replace("http://", "ws://")
                .replace("https://", "wss://")
                + "/ws"
            )
            self._ws: Any = None
            self._transport_name: str = "unknown"

        # ── Context manager protocol ─────────────────────────────────────

        def __enter__(self) -> DataQualityEnv:
            try:
                import websocket as ws_lib
            except ImportError as exc:
                try:
                    from websockets.sync.client import connect as ws_connect
                except ImportError as sync_exc:
                    raise ImportError(
                        "Fallback client requires either 'websocket-client' "
                        "or 'websockets>=11'. Install one of them with:\n"
                        "    pip install websocket-client\n"
                        "or:\n"
                        "    pip install 'websockets>=11'"
                    ) from sync_exc

                self._ws = ws_connect(self.ws_url, open_timeout=30)
                self._transport_name = "websockets.sync"
            else:
                self._ws = ws_lib.create_connection(self.ws_url, timeout=30)
                self._transport_name = "websocket-client"

            logger.info(
                "Connected to %s via %s", self.ws_url, self._transport_name
            )
            return self

        def __exit__(self, *args: Any) -> None:
            self.close()

        def close(self) -> None:
            """Close the WebSocket connection."""
            if self._ws is not None:
                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None

        # ── Sync adapter (for API parity with EnvClient) ─────────────────

        def sync(self) -> DataQualityEnv:
            """Return self (already synchronous). API parity with EnvClient."""
            return self

        # ── Wire protocol ─────────────────────────────────────────────────

        def _send(self, msg: Dict[str, Any]) -> Dict[str, Any]:
            """Send a JSON message and wait for the response."""
            if self._ws is None:
                raise RuntimeError(
                    "Not connected. Use 'with DataQualityEnv(...) as client:' "
                    "or call __enter__ first."
                )
            self._ws.send(json.dumps(msg))
            response = self._ws.recv()
            return json.loads(response)

        # ── Public API ────────────────────────────────────────────────────

        def reset(
            self, task_id: str = "task_1_format_fixer"
        ) -> DataQualityObservation:
            """Reset the environment and return the initial observation."""
            resp = self._send({"type": "reset", "task_id": task_id})
            obs_data = resp.get("observation", resp)
            return DataQualityObservation(**obs_data)

        def step(
            self, action: DataQualityAction
        ) -> DataQualityObservation:
            """Execute an action and return the resulting observation."""
            if hasattr(action, "model_dump"):
                payload = action.model_dump(exclude_none=True)
            else:
                payload = action.dict(exclude_none=True)  # type: ignore[union-attr]

            resp = self._send({"type": "step", "payload": payload})
            obs_data = resp.get("observation", resp)
            return DataQualityObservation(**obs_data)

        def get_state(self) -> DataQualityState:
            """Request the current environment state from the server."""
            resp = self._send({"type": "state"})
            state_data = resp.get("state", resp)
            return DataQualityState(**state_data)


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

__all__ = ["DataQualityEnv"]
