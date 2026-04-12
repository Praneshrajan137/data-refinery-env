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


_SCORE_EPS = 0.0001
_SCORE_LO = _SCORE_EPS
_SCORE_HI = 1.0 - _SCORE_EPS

_REWARD_KEYS = frozenset({
    "reward", "cumulative_reward", "reward_delta",
    "score", "task_score", "final_score",
})


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
        import math
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            return _SCORE_LO if v != float("inf") else _SCORE_HI
        return max(_SCORE_LO, min(_SCORE_HI, v))
    return value


def _deep_clamp_rewards(obj: Any) -> Any:
    """Recursively walk a dict/list and clamp all reward-like float fields."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if k in _REWARD_KEYS and not isinstance(v, bool):
                out[k] = _clamp_score(v)
            elif isinstance(v, (dict, list)):
                out[k] = _deep_clamp_rewards(v)
            else:
                out[k] = v
        return out
    if isinstance(obj, list):
        return [_deep_clamp_rewards(item) for item in obj]
    return obj


def _serialize(obj: Any) -> dict[str, Any]:
    """Serialize Pydantic models with score clamping safety net."""
    if hasattr(obj, "model_dump"):
        data = obj.model_dump(mode="json")
    elif hasattr(obj, "dict"):
        data = obj.dict()
    else:
        data = dict(obj)  # type: ignore[call-overload]
    return _deep_clamp_rewards(data)


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
        app_obj.websocket("/ws")(websocket_endpoint)

    return app_obj


_app_created = False

try:
    from openenv.core.env_server.http_server import create_app

    try:
        app = create_app(
            env=DataQualityEnvironment,
            action_cls=DataQualityAction,
            observation_cls=DataQualityObservation,
            env_name="data_quality_env",
            max_concurrent_envs=4,
        )
    except TypeError:
        app = create_app(
            DataQualityEnvironment,
            DataQualityAction,
            DataQualityObservation,
        )

    # ── Monkey-patch serialize_observation in the openenv framework ────
    # The framework's serialize_observation calls model_dump(exclude=...)
    # and then reads observation.reward directly.  In some Pydantic v2
    # builds the model_serializer(mode="wrap") output is re-filtered by
    # the exclude set, stripping our re-injected reward.  This patch
    # ensures all reward-like values are clamped at the wire boundary.
    _patch_applied = False
    for _mod_path in (
        "openenv.core.env_server.http_server",
        "openenv_core.env_server.http_server",
    ):
        if _patch_applied:
            break
        try:
            import importlib
            _hs_mod = importlib.import_module(_mod_path)

            _original_serialize_obs = _hs_mod.serialize_observation

            def _clamped_serialize_observation(observation):  # type: ignore[no-untyped-def]
                result = _original_serialize_obs(observation)

                # 1. Clamp top-level reward (the primary "task score")
                if "reward" in result:
                    result["reward"] = _clamp_score(result["reward"])
                else:
                    result["reward"] = _clamp_score(
                        getattr(observation, "reward", None)
                    )

                # 2. Ensure done is always present
                if "done" not in result:
                    result["done"] = getattr(observation, "done", False)

                # 3. Deep-clamp all reward-like fields inside the obs dict
                obs_dict = result.get("observation")
                if isinstance(obs_dict, dict):
                    result["observation"] = _deep_clamp_rewards(obs_dict)

                return result

            _hs_mod.serialize_observation = _clamped_serialize_observation
            _patch_applied = True
            logger.info(
                "Monkey-patched serialize_observation via %s with reward clamping",
                _mod_path,
            )
        except Exception as patch_err:
            logger.warning(
                "Could not monkey-patch via %s: %s", _mod_path, patch_err
            )

    if not _patch_applied:
        logger.error(
            "CRITICAL: serialize_observation patch failed on ALL import roots — "
            "scores may leak 0.0/1.0 on the wire"
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
        app = _build_manual_app()
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


try:
    _has_health = any(
        getattr(route, "path", None) == "/health"
        for route in getattr(app, "routes", [])
    )
except Exception:
    _has_health = False

if not _has_health:
    try:
        from starlette.responses import JSONResponse as _JSONResponse
    except ImportError:
        from fastapi.responses import JSONResponse as _JSONResponse  # type: ignore[no-redef]

    @app.get("/health")  # type: ignore[union-attr]
    async def health_check() -> _JSONResponse:
        return _JSONResponse({
            "status": "healthy",
            "environment": "data_quality_env",
        })


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
