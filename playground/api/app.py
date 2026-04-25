"""Stateless FastAPI backend for the hosted DataForge playground.

The Week 5 playground is intentionally split across two free-tier hosts:

- Cloudflare Pages serves the static frontend.
- Hugging Face Spaces serves this API-only backend.

All uploaded data is processed in memory or under a per-request temporary
directory and is discarded before the request completes.
"""

import io
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp

from dataforge.cli.repair import _propose_repairs
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Severity
from dataforge.repairers.base import ProposedFix
from dataforge.safety import SafetyFilter, SafetyVerdict
from dataforge.transactions.log import (
    append_created_transaction,
    sha256_bytes,
    snapshot_path_for,
)
from dataforge.transactions.txn import RepairTransaction, generate_txn_id

logger = logging.getLogger("playground.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

MAX_UPLOAD_BYTES = 1_048_576
SAMPLES_DIR = Path(__file__).resolve().parent / "samples"
ALLOWED_SAMPLES = {"hospital_10rows", "flights_10rows", "beers_10rows"}


class SizeCapMiddleware(BaseHTTPMiddleware):
    """Reject requests whose declared Content-Length exceeds the upload cap."""

    def __init__(self, app: ASGIApp, max_bytes: int = MAX_UPLOAD_BYTES) -> None:
        super().__init__(app)
        self.max_bytes = max_bytes

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Check Content-Length before any request body is read."""
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                length = int(content_length)
            except ValueError:
                return JSONResponse(status_code=400, content={"error": "invalid_content_length"})
            if length > self.max_bytes:
                logger.warning(
                    "Rejected request: Content-Length %d exceeds max %d",
                    length,
                    self.max_bytes,
                )
                return JSONResponse(
                    status_code=413,
                    content={"error": "file_too_large", "max_bytes": self.max_bytes},
                )
        return await call_next(request)


limiter = Limiter(key_func=get_remote_address)


def _advanced_available() -> bool:
    """Return whether at least one backend LLM provider is configured."""
    return bool(os.environ.get("GROQ_API_KEY") or os.environ.get("GEMINI_API_KEY"))


def _build_cors_origins() -> list[str]:
    """Build the explicit CORS allowlist from the environment."""
    env_origins = os.environ.get("DATAFORGE_PLAYGROUND_ORIGINS", "")
    return [origin.strip() for origin in env_origins.split(",") if origin.strip()]


def _build_cors_origin_regex() -> str:
    """Build the regex allowlist for Pages and optional localhost development."""
    patterns = [r"https://.*\.pages\.dev"]
    if os.environ.get("DATAFORGE_PLAYGROUND_DEV") == "1":
        patterns.append(r"http://(?:localhost|127(?:\.\d{1,3}){3})(?::\d+)?")
    return "^(" + "|".join(patterns) + ")$"


app = FastAPI(
    title="DataForge Playground API",
    description="Stateless backend for the hosted DataForge playground.",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url=None,
)
app.add_middleware(SizeCapMiddleware, max_bytes=MAX_UPLOAD_BYTES)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_build_cors_origins(),
    allow_origin_regex=_build_cors_origin_regex(),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def _rate_limit_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """Return a machine-readable 429 response."""
    return JSONResponse(
        status_code=429,
        content={"error": "rate_limit_exceeded", "detail": str(exc.detail)},
    )


async def _read_upload(file: UploadFile) -> bytes:
    """Read an uploaded file with a defensive hard cap."""
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail={"error": "file_too_large", "max_bytes": MAX_UPLOAD_BYTES},
        )
    return data


def _csv_to_df(data: bytes) -> pd.DataFrame:
    """Parse CSV bytes into a string-preserving DataFrame."""
    return pd.read_csv(
        io.BytesIO(data),
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )


def _severity_to_str(severity: Severity) -> str:
    """Convert a Severity enum into the JSON response value."""
    return severity.value


def _issues_to_response(
    issues: list[Issue],
    df: pd.DataFrame,
    *,
    advanced_requested: bool,
) -> dict[str, Any]:
    """Format detected issues into the public playground JSON contract."""
    grouped: dict[tuple[str, str, str], list[int]] = {}
    for issue in issues:
        key = (issue.column, issue.issue_type, _severity_to_str(issue.severity))
        grouped.setdefault(key, []).append(issue.row)

    payload_issues: list[dict[str, Any]] = []
    for (column, issue_type, severity), row_indices in grouped.items():
        unique_rows = sorted(set(row_indices))
        payload_issues.append(
            {
                "column": column,
                "issue_type": issue_type,
                "severity": severity,
                "row_indices": unique_rows,
                "count": len(unique_rows),
            }
        )

    return {
        "issues": payload_issues,
        "meta": {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "total_issues": len(issues),
            "advanced_requested": advanced_requested,
        },
    }


def _fixes_to_response(
    fixes: list[ProposedFix],
    transaction: RepairTransaction,
    *,
    source_name: str,
) -> dict[str, Any]:
    """Format accepted repair proposals plus a redacted transaction journal."""
    payload_fixes: list[dict[str, Any]] = []
    for proposed_fix in fixes:
        payload_fixes.append(
            {
                "row": proposed_fix.fix.row,
                "column": proposed_fix.fix.column,
                "old_value": proposed_fix.fix.old_value,
                "new_value": proposed_fix.fix.new_value,
                "detector_id": proposed_fix.fix.detector_id,
                "reason": proposed_fix.reason,
                "confidence": proposed_fix.confidence,
                "provenance": proposed_fix.provenance,
            }
        )

    return {
        "fixes": payload_fixes,
        "txn_journal": {
            "txn_id": transaction.txn_id,
            "created_at": transaction.created_at.isoformat(),
            "source_name": source_name,
            "source_sha256": transaction.source_sha256,
            "fixes_count": len(transaction.fixes),
            "applied": transaction.applied,
            "events": [{"event_type": "created"}],
            "note": (
                "Playground is stateless. This journal is ephemeral and discarded "
                "after the response. Install the CLI to apply and revert repairs."
            ),
        },
    }


def _require_advanced_mode(advanced_requested: bool) -> None:
    """Reject advanced mode requests unless a provider key is configured."""
    if advanced_requested and not _advanced_available():
        raise HTTPException(status_code=400, detail={"error": "advanced_mode_unavailable"})


def _write_ephemeral_transaction(
    *,
    upload_path: Path,
    source_bytes: bytes,
    fixes: list[ProposedFix],
) -> RepairTransaction:
    """Persist an ephemeral dry-run transaction under the temporary request directory."""
    txn_id = generate_txn_id()
    snapshot_path = snapshot_path_for(upload_path, txn_id)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(source_bytes)

    transaction = RepairTransaction(
        txn_id=txn_id,
        created_at=datetime.now(UTC),
        source_path=str(upload_path.resolve()),
        source_sha256=sha256_bytes(source_bytes),
        source_snapshot_path=str(snapshot_path.resolve()),
        fixes=[proposed_fix.fix for proposed_fix in fixes],
        applied=False,
    )
    append_created_transaction(transaction)
    return transaction


def _run_repair_pipeline(
    *,
    upload_name: str,
    source_bytes: bytes,
    allow_llm: bool,
) -> tuple[list[ProposedFix], RepairTransaction]:
    """Run the real dry-run repair pipeline inside a temporary workspace."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_root = Path(tmpdir)
        upload_path = temp_root / upload_name
        upload_path.write_bytes(source_bytes)

        working_df = _csv_to_df(source_bytes)
        issues = run_all_detectors(working_df, schema=None)
        accepted_fixes, _attempt_groups = _propose_repairs(
            issues,
            upload_path,
            working_df.copy(deep=True),
            None,
            allow_llm=allow_llm,
            model="gemini-2.0-flash",
            allow_pii=False,
            confirm_pii=False,
            confirm_escalations=False,
            interactive=False,
        )

        batch_safety = SafetyFilter().evaluate_batch(accepted_fixes)
        if batch_safety.verdict != SafetyVerdict.ALLOW:
            raise RuntimeError(f"Batch safety rejected dry-run fixes: {batch_safety.reason}")

        transaction = _write_ephemeral_transaction(
            upload_path=upload_path,
            source_bytes=source_bytes,
            fixes=accepted_fixes,
        )
        return accepted_fixes, transaction


@app.get("/")
async def root() -> dict[str, Any]:
    """Return service metadata for humans and uptime probes."""
    return {
        "service": "DataForge Playground API",
        "status": "ok",
        "docs_url": "/api/docs",
        "frontend_hosting": "cloudflare_pages",
    }


@app.get("/api/health")
async def health() -> dict[str, Any]:
    """Return backend readiness plus UI-facing capability metadata."""
    return {
        "status": "ok",
        "advanced_available": _advanced_available(),
        "max_upload_bytes": MAX_UPLOAD_BYTES,
    }


@app.get("/api/samples/{name}")
async def get_sample(name: str) -> StreamingResponse:
    """Return a bundled sample CSV by name."""
    if name not in ALLOWED_SAMPLES:
        raise HTTPException(
            status_code=404,
            detail={"error": "sample_not_found", "available": sorted(ALLOWED_SAMPLES)},
        )

    csv_path = SAMPLES_DIR / f"{name}.csv"
    if not csv_path.exists():
        logger.error("Sample file missing on disk: %s", csv_path)
        raise HTTPException(status_code=500, detail={"error": "sample_file_missing"})

    return StreamingResponse(
        io.BytesIO(csv_path.read_bytes()),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{name}.csv"'},
    )


@app.post("/api/profile")
@limiter.limit("10/minute")
async def profile(request: Request, file: UploadFile) -> dict[str, Any]:
    """Profile an uploaded CSV and return the detected issues."""
    advanced_requested = request.query_params.get("advanced", "false").lower() == "true"
    _require_advanced_mode(advanced_requested)

    source_bytes = await _read_upload(file)
    upload_name = Path(file.filename or "upload.csv").name
    logger.info(
        "Profile request: filename=%s bytes=%d advanced=%s",
        upload_name,
        len(source_bytes),
        advanced_requested,
    )

    try:
        df = _csv_to_df(source_bytes)
        issues = run_all_detectors(df, schema=None)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Profile endpoint failed")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "profile_failed",
                "message": "The profile pipeline could not complete safely.",
            },
        ) from exc

    return _issues_to_response(issues, df, advanced_requested=advanced_requested)


@app.post("/api/repair")
@limiter.limit("10/minute")
async def repair(request: Request, file: UploadFile) -> dict[str, Any]:
    """Return dry-run repair proposals plus an ephemeral transaction journal."""
    dry_run = request.query_params.get("dry_run", "true").lower() == "true"
    advanced_requested = request.query_params.get("advanced", "false").lower() == "true"

    if not dry_run:
        raise HTTPException(status_code=400, detail={"error": "apply_not_supported"})
    _require_advanced_mode(advanced_requested)

    source_bytes = await _read_upload(file)
    upload_name = Path(file.filename or "upload.csv").name
    logger.info(
        "Repair request: filename=%s bytes=%d advanced=%s",
        upload_name,
        len(source_bytes),
        advanced_requested,
    )

    try:
        fixes, transaction = _run_repair_pipeline(
            upload_name=upload_name,
            source_bytes=source_bytes,
            allow_llm=advanced_requested,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Repair endpoint failed")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "repair_failed",
                "message": "The repair pipeline could not complete safely.",
            },
        ) from exc

    return _fixes_to_response(fixes, transaction, source_name=upload_name)
