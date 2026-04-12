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
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-28s  %(levelname)-5s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_quality_env.server")

try:
    from ..models import DataQualityAction, DataQualityObservation
    from .data_quality_environment import DataQualityEnvironment
except ImportError:
    from models import DataQualityAction, DataQualityObservation  # type: ignore[no-redef]
    from server.data_quality_environment import DataQualityEnvironment  # type: ignore[no-redef]


_SCORE_EPS = 0.0001  # Hackathon validator requires scores strictly in (0, 1)


def _clamp_reward(value: Any) -> Any:
    """Ensure a reward value is strictly in (0, 1)."""
    if isinstance(value, (int, float)) and value is not None:
        return max(_SCORE_EPS, min(1.0 - _SCORE_EPS, float(value)))
    return value


def _serialize(obj: Any) -> dict[str, Any]:
    """Serialize Pydantic models with score clamping safety net."""
    if hasattr(obj, "model_dump"):
        data = obj.model_dump(mode="json")
    elif hasattr(obj, "dict"):
        data = obj.dict()
    else:
        data = dict(obj)  # type: ignore[call-overload]
    # Belt-and-suspenders: clamp reward fields in the serialized output.
    # The hackathon validator rejects 0.0 and 1.0 in ANY reward field.
    for key in ("reward", "cumulative_reward", "reward_delta"):
        if key in data and isinstance(data[key], (int, float)):
            data[key] = _clamp_reward(data[key])
    return data


_default_env: DataQualityEnvironment | None = None


def _get_or_create_env() -> DataQualityEnvironment:
    """Return the shared HTTP environment, creating one if needed."""
    global _default_env
    if _default_env is None:
        _default_env = DataQualityEnvironment()
    return _default_env


def _manual_handlers(json_response_cls: type[Any]) -> tuple[Callable[..., Any], ...]:
    """Build reusable HTTP and WebSocket handlers for the manual fallback app."""
    try:
        from starlette.websockets import WebSocketDisconnect
    except ImportError:
        class WebSocketDisconnect(Exception):
            """Fallback sentinel when Starlette's disconnect type is unavailable."""

    async def health(_request: Any = None) -> Any:
        return json_response_cls({
            "status": "healthy",
            "environment": "data_quality_env",
        })

    async def root(_request: Any = None) -> Any:
        return json_response_cls({
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
        })

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
                            msg.get("payload", {}).get(
                                "task_id", "task_1_format_fixer"
                            ),
                        )
                        obs = session_env.reset(task_id=task_id)
                        await websocket.send_json({
                            "type": "reset_result",
                            "observation": _serialize(obs),
                        })
                    elif cmd == "step":
                        payload = msg.get("payload", msg.get("action", msg))
                        action_data = {
                            key: value
                            for key, value in payload.items()
                            if key not in ("type", "command")
                        }
                        action = DataQualityAction(**action_data)
                        obs = session_env.step(action)
                        await websocket.send_json({
                            "type": "step_result",
                            "observation": _serialize(obs),
                        })
                    elif cmd == "state":
                        await websocket.send_json({
                            "type": "state_result",
                            "state": _serialize(session_env.state),
                        })
                    else:
                        await websocket.send_json({
                            "error": (
                                f"Unknown command: {cmd!r}. "
                                "Valid commands: reset, step, state."
                            )
                        })
                except Exception as exc:  # noqa: BLE001
                    logger.error("Error processing %r: %s", cmd, exc)
                    await websocket.send_json({"error": str(exc)})
        finally:
            logger.info("WebSocket client disconnected")

    async def reset_http(request: Any) -> Any:
        """POST /reset — create a new environment session and reset to a task."""
        global _default_env
        try:
            body = await request.json()
        except Exception:
            body = {}
        task_id = body.get("task_id", "task_1_format_fixer")
        _default_env = DataQualityEnvironment()
        obs = _default_env.reset(task_id=task_id)
        return json_response_cls({"observation": _serialize(obs)})

    async def step_http(request: Any) -> Any:
        """POST /step — submit an action and receive the next observation."""
        env = _get_or_create_env()
        try:
            body = await request.json()
        except Exception:
            return json_response_cls({"error": "Invalid JSON body"}, status_code=400)
        # Accept both {"action": {...}} (openenv protocol) and flat fields
        action_data = body.get("action", body)
        action_data = {
            k: v for k, v in action_data.items() if k not in ("type", "command")
        }
        action = DataQualityAction(**action_data)
        obs = env.step(action)
        return json_response_cls({"observation": _serialize(obs)})

    async def state_http(request: Any = None) -> Any:
        """GET /state — return current environment state."""
        env = _get_or_create_env()
        return json_response_cls({"state": _serialize(env.state)})

    return health, root, websocket_endpoint, reset_http, step_http, state_http


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
        health, root, websocket_endpoint, reset_http, step_http, state_http = _manual_handlers(JSONResponse)
    except ImportError:
        from starlette.applications import Starlette
        from starlette.middleware.cors import CORSMiddleware
        from starlette.responses import JSONResponse
        from starlette.routing import Route, WebSocketRoute

        framework = "starlette"
        health, root, websocket_endpoint, reset_http, step_http, state_http = _manual_handlers(JSONResponse)
        app_obj = Starlette(
            debug=False,
            routes=[
                Route("/health", health, methods=["GET"]),
                Route("/", root, methods=["GET"]),
                Route("/reset", reset_http, methods=["POST"]),
                Route("/step", step_http, methods=["POST"]),
                Route("/state", state_http, methods=["GET"]),
                WebSocketRoute("/ws", websocket_endpoint),
            ],
        )

    logger.info(
        "Building manual fallback server with %s", framework
    )

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
        app_obj.post("/reset")(reset_http)
        app_obj.post("/step")(step_http)
        app_obj.get("/state")(state_http)
        app_obj.websocket("/ws")(websocket_endpoint)

    return app_obj


_app_created = False
_inner_app: Any = None  # The actual ASGI app (before middleware wrapping)

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
        logger.info("Fallback server ready with /health, /, /reset, /step, /state, /ws endpoints")
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


# ── Register fallback routes on _inner_app BEFORE wrapping with middleware ──

try:
    _has_health = any(
        getattr(route, "path", None) == "/health"
        for route in getattr(_inner_app, "routes", [])
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
        return _JSONResponse({
            "status": "healthy",
            "environment": "data_quality_env",
        })


# ── Ensure /reset, /step, /state HTTP endpoints exist (defense-in-depth) ──
try:
    _has_reset = any(
        getattr(route, "path", None) == "/reset"
        for route in getattr(_inner_app, "routes", [])
    )
except Exception:
    _has_reset = False

if not _has_reset:
    try:
        from starlette.responses import JSONResponse as _ResetJSONResponse
    except ImportError:
        from fastapi.responses import JSONResponse as _ResetJSONResponse  # type: ignore[no-redef]

    @_inner_app.post("/reset")  # type: ignore[union-attr]
    async def _fallback_reset(request: Any) -> _ResetJSONResponse:
        global _default_env
        try:
            body = await request.json()
        except Exception:
            body = {}
        task_id = body.get("task_id", "task_1_format_fixer")
        _default_env = DataQualityEnvironment()
        obs = _default_env.reset(task_id=task_id)
        return _ResetJSONResponse({"observation": _serialize(obs)})

    @_inner_app.post("/step")  # type: ignore[union-attr]
    async def _fallback_step(request: Any) -> _ResetJSONResponse:
        env = _get_or_create_env()
        try:
            body = await request.json()
        except Exception:
            return _ResetJSONResponse({"error": "Invalid JSON body"}, status_code=400)
        # Accept both {"action": {...}} (openenv protocol) and flat fields
        action_data = body.get("action", body)
        action_data = {
            k: v for k, v in action_data.items() if k not in ("type", "command")
        }
        action = DataQualityAction(**action_data)
        obs = env.step(action)
        return _ResetJSONResponse({"observation": _serialize(obs)})

    @_inner_app.get("/state")  # type: ignore[union-attr]
    async def _fallback_state() -> _ResetJSONResponse:
        env = _get_or_create_env()
        return _ResetJSONResponse({"state": _serialize(env.state)})

    logger.info("Added fallback /reset, /step, /state HTTP endpoints")


# ── Nuclear safety net: ASGI middleware to clamp ALL reward fields ──────
# This intercepts every JSON response (from any code path, including
# openenv's create_app) and clamps reward/score fields to (0.0001, 0.9999).

def _clamp_scores_in_dict(d: dict) -> dict:
    """Recursively clamp all reward-like fields in a dict to (0.0001, 0.9999).
    
    Also handles the openenv create_app case where the top-level ``reward``
    is ``None`` — replaces it with the observation's ``cumulative_reward``
    or ``_SCORE_EPS`` as a fallback.
    """
    _KEYS = {"reward", "cumulative_reward", "reward_delta"}
    for key, value in d.items():
        if key in _KEYS:
            if value is None:
                # openenv framework sets top-level reward=None; replace with
                # cumulative_reward from nested observation or SCORE_MIN
                obs = d.get("observation", {})
                if isinstance(obs, dict) and "cumulative_reward" in obs:
                    fallback = obs["cumulative_reward"]
                    if isinstance(fallback, (int, float)) and not isinstance(fallback, bool):
                        d[key] = max(_SCORE_EPS, min(1.0 - _SCORE_EPS, float(fallback)))
                    else:
                        d[key] = _SCORE_EPS
                else:
                    d[key] = _SCORE_EPS
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                d[key] = max(_SCORE_EPS, min(1.0 - _SCORE_EPS, float(value)))
        elif isinstance(value, dict):
            _clamp_scores_in_dict(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _clamp_scores_in_dict(item)
    return d


class _ScoreClampMiddleware:
    """ASGI middleware that clamps reward fields in ALL JSON responses."""

    def __init__(self, app_inner: Any) -> None:
        self.app = app_inner

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        if scope["type"] == "websocket":
            # For WebSockets, intercept send messages
            async def _ws_send(message: dict) -> None:
                if message.get("type") == "websocket.send":
                    text = message.get("text")
                    if text:
                        try:
                            data = json.loads(text)
                            if isinstance(data, dict):
                                _clamp_scores_in_dict(data)
                                message = {**message, "text": json.dumps(data)}
                        except (json.JSONDecodeError, TypeError):
                            pass
                await send(message)
            await self.app(scope, receive, _ws_send)
            return

        # HTTP: collect response body, clamp scores, re-send
        response_headers: list = []
        response_status: int = 200
        body_parts: list[bytes] = []

        async def _http_send(message: dict) -> None:
            nonlocal response_headers, response_status
            if message["type"] == "http.response.start":
                response_status = message.get("status", 200)
                response_headers = list(message.get("headers", []))
                # Don't send start yet — we need to potentially modify body
            elif message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))
                if not message.get("more_body", False):
                    # All body received — process it
                    full_body = b"".join(body_parts)
                    content_type = ""
                    new_headers = []
                    for hname, hval in response_headers:
                        if hname.lower() == b"content-type":
                            content_type = hval.decode("utf-8", errors="replace")
                        new_headers.append((hname, hval))

                    if "json" in content_type:
                        try:
                            data = json.loads(full_body)
                            if isinstance(data, dict):
                                _clamp_scores_in_dict(data)
                                full_body = json.dumps(data).encode("utf-8")
                                # Update content-length
                                new_headers = [
                                    (h, v) for h, v in new_headers
                                    if h.lower() != b"content-length"
                                ]
                                new_headers.append(
                                    (b"content-length", str(len(full_body)).encode())
                                )
                        except (json.JSONDecodeError, TypeError):
                            pass

                    await send({
                        "type": "http.response.start",
                        "status": response_status,
                        "headers": new_headers,
                    })
                    await send({
                        "type": "http.response.body",
                        "body": full_body,
                    })

        await self.app(scope, receive, _http_send)


# Wrap the inner app with score-clamping middleware as the FINAL step
app = _ScoreClampMiddleware(_inner_app)  # type: ignore[assignment]
logger.info("Score-clamping ASGI middleware installed")


def main(host: str = "0.0.0.0", port: int = 7860) -> None:
    """Run the ASGI app via uvicorn."""
    import uvicorn

    logger.info("Starting server on %s:%d", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse
    import sys as _sys

    parser = argparse.ArgumentParser(
        description="Data Quality Environment Server"
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Bind address"
    )
    parser.add_argument(
        "--port", type=int, default=7860, help="Port number (default: 7860)"
    )

    if len(_sys.argv) == 1:
        main()
    else:
        args = parser.parse_args()
        main(host=args.host, port=args.port)
