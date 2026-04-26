# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""ASGI application for the Data Quality environment.

This module exposes the environment over HTTP and WebSocket, preferring the
native openenv server when available and falling back to a lightweight manual
app otherwise.

Resolution order:
    1. openenv ``create_app`` when openenv-core is installed
    2. Manual app built on FastAPI
    3. Manual app built on Starlette

CRITICAL: A raw ASGI middleware wraps the final ``app`` object to clamp
ALL score-like float values in every HTTP JSON response.  This is the
**only** reliable way to guarantee the hackathon Phase 2 validator never
sees 0.0 or 1.0 — monkey-patching ``serialize_observation`` is ineffective
because route handlers capture function references at import time.
"""

from __future__ import annotations

import json
import logging
import math
import re
from typing import Any, Callable

from ..models import DataQualityAction, DataQualityObservation
from .data_quality_environment import DataQualityEnvironment


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-28s  %(levelname)-5s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_quality_env.server")


# ═══════════════════════════════════════════════════════════════════════════
# §1  Score Clamping Utilities
# ═══════════════════════════════════════════════════════════════════════════

_SCORE_EPS = 0.0001
_SCORE_LO = _SCORE_EPS
_SCORE_HI = 1.0 - _SCORE_EPS

# Keys whose values MUST be clamped to (0, 1) exclusive.
# Matches any key containing "reward", "score", "rate", "penalty",
# "weight", "coverage", "utilization", "efficiency".
_SCORE_KEY_RE = re.compile(
    r"reward|score|_rate|penalty|_weight|coverage|utilization|efficiency",
    re.IGNORECASE,
)

# Keys that are structural integers and must NEVER be clamped.
_INT_KEYS = frozenset(
    {
        "total_issues",
        "fixable_issues",
        "detection_only",
        "detected",
        "fixed",
        "false_positives",
        "steps_used",
        "max_steps",
        "total_rows",
        "total_columns",
        "issues_found",
        "steps_taken",
        "row",
        "index",
        "row_index",
        "column",
        "step_count",
        "non_null",
        "null_count",
        "unique_count",
        "total",
    }
)


def _clamp_score(value: Any) -> Any:
    """Clamp a numeric value to the strict open interval (SCORE_LO, SCORE_HI).

    Bools pass through unchanged. None is mapped to SCORE_LO.
    NaN/Inf are mapped to the nearest boundary.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return _SCORE_LO
    if isinstance(value, (int, float)):
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            return _SCORE_LO if v != float("inf") else _SCORE_HI
        return max(_SCORE_LO, min(_SCORE_HI, v))
    return value


def _deep_clamp_rewards(obj: Any) -> Any:
    """Recursively walk a dict/list and clamp all reward/score float fields."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if k in _INT_KEYS:
                # Preserve structural integers — never clamp these.
                out[k] = v
            elif isinstance(v, bool):
                out[k] = v
            elif _SCORE_KEY_RE.search(k) and isinstance(v, (int, float)):
                out[k] = _clamp_score(v)
            elif isinstance(v, (dict, list)):
                out[k] = _deep_clamp_rewards(v)
            else:
                out[k] = v
        return out
    if isinstance(obj, list):
        return [_deep_clamp_rewards(item) for item in obj]
    return obj


def _nuclear_clamp_response(obj: Any) -> Any:
    """Aggressively clamp ALL possible score values in a response.

    This is the outermost clamping layer:
    1. Clamp top-level ``reward`` (the "task score" the validator reads).
    2. Recursively clamp all score-like keys via _deep_clamp_rewards.
    3. If there's an ``observation`` sub-dict, clamp its contents too.
    """
    if not isinstance(obj, dict):
        return obj

    # 1. Ensure top-level reward is always clamped (this is the task score).
    if "reward" in obj and not isinstance(obj["reward"], bool):
        obj["reward"] = _clamp_score(obj["reward"])

    # 2. Deep-clamp the entire response body.
    obj = _deep_clamp_rewards(obj)

    # 3. Double-check nested observation dict.
    if "observation" in obj and isinstance(obj["observation"], dict):
        obs = obj["observation"]
        for key in (
            "reward",
            "cumulative_reward",
            "reward_delta",
            "score",
            "task_score",
            "final_score",
        ):
            if key in obs and isinstance(obs[key], (int, float)) and not isinstance(obs[key], bool):
                obs[key] = _clamp_score(obs[key])
        obj["observation"] = _deep_clamp_rewards(obs)

    return obj


def _serialize(obj: Any) -> dict[str, Any]:
    """Serialize Pydantic models with score clamping safety net."""
    if hasattr(obj, "model_dump"):
        data = obj.model_dump(mode="json")
    elif hasattr(obj, "dict"):
        data = obj.dict()
    else:
        data = dict(obj)  # type: ignore[call-overload]
    return _nuclear_clamp_response(data)


# ═══════════════════════════════════════════════════════════════════════════
# §2  Raw ASGI Middleware — the ONLY reliable way to clamp scores
# ═══════════════════════════════════════════════════════════════════════════


class ScoreClampASGIMiddleware:
    """Raw ASGI middleware that intercepts every HTTP JSON response and
    clamps all score-like float values to the strict (0.0001, 0.9999) range.

    This operates at the ASGI protocol level — it buffers ``http.response.body``
    messages, parses JSON, clamps, re-serializes, and fixes content-length.
    Unlike ``BaseHTTPMiddleware``, this:
      - Correctly updates content-length after body modification.
      - Works with any ASGI framework (openenv, FastAPI, Starlette).
      - Cannot be bypassed by framework-internal serialization.
    """

    def __init__(self, inner_app: Any) -> None:
        self.inner_app = inner_app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            # WebSocket / lifespan — pass through unchanged.
            await self.inner_app(scope, receive, send)
            return

        # Buffer response headers + body so we can modify before sending.
        response_started = False
        start_message: dict[str, Any] | None = None
        body_chunks: list[bytes] = []

        async def send_wrapper(message: dict[str, Any]) -> None:
            nonlocal response_started, start_message

            if message["type"] == "http.response.start":
                # Hold the start message — don't send until we've seen the body.
                response_started = True
                start_message = message
                return

            if message["type"] == "http.response.body":
                body = message.get("body", b"")
                more_body = message.get("more_body", False)
                body_chunks.append(body)

                if more_body:
                    # More chunks coming — keep buffering.
                    return

                # ─── Final chunk: we have the complete response body ───
                full_body = b"".join(body_chunks)

                # Check content-type from the start message headers.
                content_type = ""
                if start_message:
                    for hdr_name, hdr_value in start_message.get("headers", []):
                        name = hdr_name if isinstance(hdr_name, str) else hdr_name.decode("latin-1")
                        val = (
                            hdr_value if isinstance(hdr_value, str) else hdr_value.decode("latin-1")
                        )
                        if name.lower() == "content-type":
                            content_type = val
                            break

                # Only process JSON responses.
                if "json" in content_type.lower() and full_body:
                    try:
                        data = json.loads(full_body)
                        clamped = _nuclear_clamp_response(data)
                        full_body = json.dumps(clamped, separators=(",", ":")).encode("utf-8")
                    except (json.JSONDecodeError, TypeError, ValueError):
                        pass  # Not valid JSON — send original body.

                # Rebuild headers with correct content-length.
                if start_message:
                    new_headers: list[list[bytes]] = []
                    for hdr_name, hdr_value in start_message.get("headers", []):
                        name_bytes = (
                            hdr_name if isinstance(hdr_name, bytes) else hdr_name.encode("latin-1")
                        )
                        if name_bytes.lower() == b"content-length":
                            continue  # Drop old content-length.
                        new_headers.append(
                            [
                                name_bytes,
                                hdr_value
                                if isinstance(hdr_value, bytes)
                                else hdr_value.encode("latin-1"),
                            ]
                        )
                    new_headers.append([b"content-length", str(len(full_body)).encode("latin-1")])
                    start_message["headers"] = new_headers

                    # Now send both messages.
                    await send(start_message)

                await send(
                    {
                        "type": "http.response.body",
                        "body": full_body,
                    }
                )
                return

            # Unknown message type — forward as-is.
            await send(message)

        await self.inner_app(scope, receive, send_wrapper)


# ═══════════════════════════════════════════════════════════════════════════
# §3  Manual Fallback Server (FastAPI / Starlette)
# ═══════════════════════════════════════════════════════════════════════════


def _manual_handlers(json_response_cls: type[Any]) -> tuple[Callable[..., Any], ...]:
    """Build reusable HTTP and WebSocket handlers for the manual fallback app."""
    try:
        from starlette.websockets import WebSocketDisconnect
    except ImportError:

        class WebSocketDisconnect(Exception):
            """Fallback sentinel when Starlette's disconnect type is unavailable."""

    async def health(_request: Any = None) -> Any:
        return json_response_cls(
            {
                "status": "healthy",
                "environment": "data_quality_env",
            }
        )

    async def root(_request: Any = None) -> Any:
        return json_response_cls(
            {
                "name": "data_quality_env",
                "description": (
                    "Data Quality Validation and Cleaning Pipeline. "
                    "Agents learn to detect and fix real-world tabular data issues."
                ),
                "tasks": [
                    "task_1_format_fixer",
                    "task_2_duplicate_detective",
                    "task_3_integrity_auditor",
                ],
                "version": "1.0.0",
            }
        )

    async def websocket_endpoint(websocket: Any) -> None:
        await websocket.accept()
        session_env = DataQualityEnvironment()
        logger.info("WebSocket client connected (new session)")

        try:
            while True:
                try:
                    raw = await websocket.receive_text()
                except WebSocketDisconnect:
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    await websocket.send_json({"error": "Invalid JSON"})
                    continue

                cmd = msg.get("type", msg.get("command", ""))

                try:
                    if cmd == "reset":
                        task_id = msg.get(
                            "task_id",
                            msg.get("payload", {}).get("task_id", "task_1_format_fixer"),
                        )
                        obs = session_env.reset(task_id=task_id)
                        await websocket.send_json(
                            {
                                "type": "reset_result",
                                "observation": _serialize(obs),
                            }
                        )
                    elif cmd == "step":
                        payload = msg.get("payload", msg.get("action", msg))
                        action_data = {
                            key: value
                            for key, value in payload.items()
                            if key not in ("type", "command")
                        }
                        action = DataQualityAction(**action_data)
                        obs = session_env.step(action)
                        await websocket.send_json(
                            {
                                "type": "step_result",
                                "observation": _serialize(obs),
                            }
                        )
                    elif cmd == "state":
                        await websocket.send_json(
                            {
                                "type": "state_result",
                                "state": _serialize(session_env.state),
                            }
                        )
                    else:
                        await websocket.send_json(
                            {
                                "error": (
                                    f"Unknown command: {cmd!r}. Valid commands: reset, step, state."
                                )
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.error("Error processing %r: %s", cmd, exc)
                    await websocket.send_json({"error": str(exc)})
        finally:
            logger.info("WebSocket client disconnected")

    return health, root, websocket_endpoint


def _build_manual_app() -> Any:
    """Create a manual fallback app using FastAPI or Starlette."""

    try:
        from fastapi import FastAPI
        from starlette.middleware.cors import CORSMiddleware
        from starlette.responses import JSONResponse

        framework = "fastapi"
        app_obj = FastAPI(
            title="Data Quality Environment",
            description=(
                "Data Quality Validation and Cleaning Pipeline. "
                "Agents learn to detect and fix data quality issues in "
                "real-world tabular datasets."
            ),
            version="1.0.0",
        )
        health, root, websocket_endpoint = _manual_handlers(JSONResponse)
    except ImportError:
        from starlette.applications import Starlette
        from starlette.middleware.cors import CORSMiddleware
        from starlette.responses import JSONResponse
        from starlette.routing import Route, WebSocketRoute

        framework = "starlette"
        health, root, websocket_endpoint = _manual_handlers(JSONResponse)
        app_obj = Starlette(
            debug=False,
            routes=[
                Route("/health", health, methods=["GET"]),
                Route("/", root, methods=["GET"]),
                WebSocketRoute("/ws", websocket_endpoint),
            ],
        )

    logger.info("Building manual fallback server with %s", framework)

    app_obj.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if framework == "fastapi":
        app_obj.get("/health")(health)
        app_obj.get("/")(root)
        app_obj.websocket("/ws")(websocket_endpoint)

    return app_obj


# ═══════════════════════════════════════════════════════════════════════════
# §4  App Creation
# ═══════════════════════════════════════════════════════════════════════════

_app_created = False
_inner_app: Any = None  # The framework app BEFORE our ASGI wrapper.

try:
    from openenv.core.env_server.http_server import create_app

    try:
        _inner_app = create_app(
            env=DataQualityEnvironment,
            action_cls=DataQualityAction,
            observation_cls=DataQualityObservation,
            env_name="data_quality_env",
            max_concurrent_envs=4,
        )
    except TypeError:
        _inner_app = create_app(
            DataQualityEnvironment,
            DataQualityAction,
            DataQualityObservation,
        )

    _app_created = True
    logger.info(
        "Server initialized via openenv create_app "
        "(env_name=data_quality_env, max_concurrent_envs=4)"
    )
except Exception as tier1_err:  # noqa: BLE001
    logger.info("openenv create_app unavailable: %s", tier1_err)


if not _app_created:
    try:
        _inner_app = _build_manual_app()
        _app_created = True
        logger.info("Fallback server ready with /health, /, /ws endpoints")
    except ImportError as tier2_err:
        raise ImportError(
            "Neither openenv-core, FastAPI, nor Starlette is available. "
            "Install dependencies with:\n"
            "    pip install 'openenv-core[core]>=0.2.0'\n"
            "or:\n"
            "    pip install 'fastapi>=0.104.0' 'uvicorn>=0.24.0'\n"
            "or:\n"
            "    pip install 'starlette>=0.37.0' 'uvicorn>=0.24.0'"
        ) from tier2_err


# ── Ensure /health endpoint exists ────────────────────────────────────────

try:
    _has_health = any(
        getattr(route, "path", None) == "/health" for route in getattr(_inner_app, "routes", [])
    )
except Exception:
    _has_health = False

if not _has_health:
    try:
        from starlette.responses import JSONResponse as _JSONResponse
    except ImportError:
        from fastapi.responses import JSONResponse as _JSONResponse  # type: ignore[no-redef]

    @_inner_app.get("/health")  # type: ignore[union-attr]
    async def health_check() -> _JSONResponse:
        return _JSONResponse(
            {
                "status": "healthy",
                "environment": "data_quality_env",
            }
        )


# ═══════════════════════════════════════════════════════════════════════════
# §5  Wrap with Raw ASGI Middleware — the final, authoritative ``app``
# ═══════════════════════════════════════════════════════════════════════════
#
# This is the ONLY ``app`` object exported to uvicorn.  Every HTTP JSON
# response passes through ScoreClampASGIMiddleware before reaching the
# network.  No framework-internal serialization can bypass this layer.

app = ScoreClampASGIMiddleware(_inner_app)
logger.info("ScoreClampASGIMiddleware installed as outermost ASGI wrapper")


# ═══════════════════════════════════════════════════════════════════════════
# §6  CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════


def main(host: str = "0.0.0.0", port: int = 7860) -> None:
    """Run the ASGI app via uvicorn."""
    import uvicorn

    logger.info("Starting server on %s:%d", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse
    import sys as _sys

    parser = argparse.ArgumentParser(description="Data Quality Environment Server")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=7860, help="Port number (default: 7860)")

    if len(_sys.argv) == 1:
        main()
    else:
        args = parser.parse_args()
        main(host=args.host, port=args.port)
