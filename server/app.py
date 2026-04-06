# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""FastAPI application for the Data Quality environment.

This module creates an HTTP + WebSocket server that exposes the
``DataQualityEnvironment`` for agent interaction, compatible with
the openenv ``EnvClient`` protocol.

Server Resolution Strategy (three tiers, first successful wins):
    1. ``create_app`` from openenv-core (canonical, handles WS/HTTP/schema)
    2. Manual FastAPI + WebSocket fallback (when openenv-core is unavailable)

Endpoints (Tier 1 — via ``create_app``):
    - POST /reset      — reset the environment
    - POST /step       — execute an action
    - GET  /state      — current state
    - GET  /schema     — action/observation JSON schemas
    - GET  /health     — health check
    - WS   /ws         — persistent WebSocket sessions

Endpoints (Tier 2 — manual fallback):
    - GET  /           — environment metadata
    - GET  /health     — health check
    - WS   /ws         — WebSocket endpoint (reset/step/state commands)

Usage::

    # Development (with auto-reload):
    uvicorn server.app:app --reload --host 0.0.0.0 --port 7860

    # Production:
    uvicorn server.app:app --host 0.0.0.0 --port 7860 --workers 4

    # Direct execution:
    python -m server.app
    python -m server.app --port 8001

Bug fixes from review:
    [FIX-08]  Full fallback server when openenv's ``create_app`` is unavailable.
    [FIX-12]  Removed nonexistent ``create_web_interface_app`` reference.
    [FIX-13]  Proper import patterns — no ``sys.path`` hacking.
    [FIX-14]  Pass ``env_name`` and ``max_concurrent_envs`` to ``create_app``.
    [FIX-17]  Per-session environment instances in fallback (no shared state).
"""

from __future__ import annotations

import json
import logging
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-28s  %(levelname)-5s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_quality_env.server")


# ── Imports ───────────────────────────────────────────────────────────────
# [FIX-13] Use the same import pattern as the existing working app.py:
# relative imports when running as a package, absolute fallback for direct
# execution.

try:
    from ..models import DataQualityAction, DataQualityObservation
    from .data_quality_environment import DataQualityEnvironment
except ImportError:
    from models import DataQualityAction, DataQualityObservation  # type: ignore[no-redef]
    from server.data_quality_environment import DataQualityEnvironment  # type: ignore[no-redef]


# ═══════════════════════════════════════════════════════════════════════════
# §1  Tier 1: openenv ``create_app``
# ═══════════════════════════════════════════════════════════════════════════

_app_created = False

try:
    from openenv.core.env_server.http_server import create_app

    # [FIX-14] Defensive kwargs — openenv-core versions differ in whether
    # create_app accepts positional-only or keyword-only arguments.
    # Try kwargs first (canonical), fall back to positional (legacy).
    try:
        app = create_app(
            env=DataQualityEnvironment,
            action_cls=DataQualityAction,
            observation_cls=DataQualityObservation,
            env_name="data_quality_env",
            max_concurrent_envs=4,
        )
    except TypeError:
        # Fallback: some versions only accept positional args
        app = create_app(
            DataQualityEnvironment,
            DataQualityAction,
            DataQualityObservation,
        )

    _app_created = True
    logger.info(
        "Server initialized via openenv create_app "
        "(env_name=data_quality_env, max_concurrent_envs=4)"
    )

except Exception as tier1_err:
    logger.warning("openenv create_app unavailable: %s", tier1_err)


# ═══════════════════════════════════════════════════════════════════════════
# §2  Tier 2: Manual FastAPI + WebSocket fallback
# ═══════════════════════════════════════════════════════════════════════════

if not _app_created:
    try:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.middleware.cors import CORSMiddleware
        from fastapi.responses import JSONResponse

        logger.info(
            "Building manual fallback server (openenv create_app unavailable)"
        )

        app = FastAPI(
            title="Data Quality Environment",
            description=(
                "Data Quality Validation & Cleaning Pipeline — "
                "agents learn to detect and fix data quality issues "
                "in tabular datasets."
            ),
            version="1.0.0",
        )

        # CORS for development
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # ── REST endpoints ────────────────────────────────────────────────

        @app.get("/health")
        async def health() -> JSONResponse:
            return JSONResponse({
                "status": "healthy",
                "environment": "data_quality_env",
            })

        @app.get("/")
        async def root() -> JSONResponse:
            return JSONResponse({
                "name": "data_quality_env",
                "description": (
                    "Data Quality Validation & Cleaning Pipeline — "
                    "agents learn to detect and fix data quality issues "
                    "in tabular datasets."
                ),
                "tasks": [
                    "task_1_format_fixer",
                    "task_2_duplicate_detective",
                    "task_3_integrity_auditor",
                ],
                "version": "1.0.0",
            })

        # ── WebSocket endpoint ────────────────────────────────────────────
        # [FIX-17] Each connection gets its own environment instance.

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            # Per-session environment instance (no shared state)
            session_env = DataQualityEnvironment()
            logger.info("WebSocket client connected (new session)")

            try:
                while True:
                    raw = await websocket.receive_text()

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
                            payload = msg.get(
                                "payload", msg.get("action", msg)
                            )
                            # Strip protocol keys before constructing action
                            action_data = {
                                k: v
                                for k, v in payload.items()
                                if k not in ("type", "command")
                            }
                            action = DataQualityAction(**action_data)
                            obs = session_env.step(action)
                            await websocket.send_json({
                                "type": "step_result",
                                "observation": _serialize(obs),
                            })

                        elif cmd == "state":
                            st = session_env.state
                            await websocket.send_json({
                                "type": "state_result",
                                "state": _serialize(st),
                            })

                        else:
                            await websocket.send_json({
                                "error": f"Unknown command: {cmd!r}. "
                                "Valid: reset, step, state.",
                            })

                    except Exception as exc:
                        logger.error("Error processing '%s': %s", cmd, exc)
                        await websocket.send_json({"error": str(exc)})

            except WebSocketDisconnect:
                logger.info("WebSocket client disconnected")

        _app_created = True
        logger.info("Fallback server ready with /health, /, /ws endpoints")

    except ImportError as tier2_err:
        raise ImportError(
            "Neither openenv-core nor FastAPI is available. "
            "Install dependencies with:\n"
            "    pip install 'openenv-core[core]>=0.2.0'\n"
            "  or:\n"
            "    pip install 'fastapi>=0.104.0' 'uvicorn>=0.24.0'\n"
        ) from tier2_err


# ═══════════════════════════════════════════════════════════════════════════
# §3  Ensure /health exists regardless of tier
# ═══════════════════════════════════════════════════════════════════════════

# Guard: some create_app implementations may not include /health.
try:
    _has_health = any(
        getattr(r, "path", None) == "/health"
        for r in getattr(app, "routes", [])
    )
except Exception:
    _has_health = False

if not _has_health:
    from fastapi.responses import JSONResponse as _JR

    @app.get("/health")  # type: ignore[union-attr]
    async def health_check() -> _JR:
        return _JR({"status": "healthy", "environment": "data_quality_env"})


# ═══════════════════════════════════════════════════════════════════════════
# §4  Serialization Helper
# ═══════════════════════════════════════════════════════════════════════════

def _serialize(obj: Any) -> dict:
    """Serialize a Pydantic model to a JSON-safe dict.

    Handles both Pydantic v2 (``model_dump``) and v1 (``dict``) APIs,
    with enum values converted to their string representations.
    """
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if hasattr(obj, "dict"):
        return obj.dict()
    return dict(obj)  # type: ignore[call-overload]


# ═══════════════════════════════════════════════════════════════════════════
# §5  CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════

def main(host: str = "0.0.0.0", port: int = 7860) -> None:
    """Entry point for direct execution.

    Supports both ``uv run`` and ``python -m`` invocation::

        uv run --project . server
        uv run --project . server --port 8001
        python -m data_quality_env.server.app
        python -m data_quality_env.server.app --port 8001

    For production, use uvicorn directly with multiple workers::

        uvicorn data_quality_env.server.app:app --workers 4
    """
    import uvicorn

    logger.info("Starting server on %s:%d", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Data Quality Environment Server"
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Bind address"
    )
    parser.add_argument(
        "--port", type=int, default=7860, help="Port number (default: 7860)"
    )
    args = parser.parse_args()
    main(host=args.host, port=args.port)
