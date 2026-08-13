"""Stateless FastAPI backend for the hosted DataForge playground.

The hosted playground is intentionally split across two free-tier hosts:

- Cloudflare Workers Static Assets serves the static frontend.
- Hugging Face Spaces serves this API-only backend.

All uploaded data is processed in memory or under a per-request temporary
directory and is discarded before the request completes.
"""

import asyncio
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import time
import uuid
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from threading import Lock
from typing import Any, Literal, Protocol, TypeVar, cast

import pandas as pd
from fastapi import FastAPI, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pandas.errors import EmptyDataError, ParserError
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp

from dataforge import (
    CONTRACT_VERSION,
    ExternalFix,
    Issue,
    RepairPipelineRequest,
    RepairTransaction,
    Severity,
    VerifiedFix,
    VerifyAndApplyRequest,
    run_all_detectors,
    run_repair_pipeline,
    verify_and_apply,
)
from dataforge.agent import AgentRepairRequest, run_agent_repair
from dataforge.agent.policy import PolicyUnavailableError
from dataforge.certificate import verify_certificate
from dataforge.http.problem import problem_exception_handler, problem_response
from dataforge.observability import configure_fastapi_observability
from dataforge.schema_inference import (
    REPAIR_SUPPORTED_CONSTRAINT_KINDS,
    build_constraint_review_artifact,
    dump_constraint_review_artifact,
    infer_schema,
    update_constraint_review_artifact,
)


class FallbackRateLimitExceededError(Exception):
    """Fallback exception shape matching slowapi's detail attribute."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


try:
    _slowapi_module = import_module("slowapi")
    _slowapi_errors = import_module("slowapi.errors")
    _slowapi_util = import_module("slowapi.util")
    _SlowapiLimiter: Any | None = _slowapi_module.Limiter
    _SlowapiRateLimitExceeded: type[Exception] | None = _slowapi_errors.RateLimitExceeded
    get_remote_address = cast(Callable[[Request], str], _slowapi_util.get_remote_address)

    SLOWAPI_AVAILABLE = True
except ModuleNotFoundError:
    _SlowapiLimiter = None
    _SlowapiRateLimitExceeded = None
    SLOWAPI_AVAILABLE = False

    def get_remote_address(request: Request) -> str:
        """Return the client host for fallback rate-limit keys."""
        return request.client.host if request.client else "unknown"


_CallableT = TypeVar("_CallableT", bound=Callable[..., Any])


class _StorageLike(Protocol):
    """Minimal storage protocol used by tests and fallback middleware."""

    def reset(self) -> None: ...


class _LimiterLike(Protocol):
    """Minimal limiter protocol shared by slowapi and the fallback."""

    _storage: _StorageLike

    def limit(self, limit_value: str) -> Callable[[_CallableT], _CallableT]: ...


class _FallbackStorage:
    """Small in-memory windowed counter used when slowapi is unavailable."""

    def __init__(self) -> None:
        self._hits: dict[tuple[str, str], list[float]] = defaultdict(list)

    def reset(self) -> None:
        """Clear all fallback counters."""
        self._hits.clear()

    def allow(self, key: tuple[str, str], *, limit: int, window_seconds: float) -> bool:
        """Record a hit and return whether it fits inside the window."""
        now = time.monotonic()
        hits = [seen for seen in self._hits[key] if now - seen < window_seconds]
        hits.append(now)
        self._hits[key] = hits
        return len(hits) <= limit


class _FallbackLimiter:
    """Decorator-compatible fallback limiter."""

    def __init__(self) -> None:
        self._storage: _StorageLike = _FallbackStorage()

    def limit(self, _limit_value: str) -> Callable[[_CallableT], _CallableT]:
        """Return an identity decorator; middleware enforces the limit."""

        def decorator(func: _CallableT) -> _CallableT:
            return func

        return decorator


_RateLimitExceeded: type[Exception] = (
    _SlowapiRateLimitExceeded
    if _SlowapiRateLimitExceeded is not None
    else FallbackRateLimitExceededError
)

logger = logging.getLogger("playground.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


def _positive_int_env(name: str, default: int) -> int:
    """Return a positive integer env override, falling back safely."""
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


MAX_UPLOAD_BYTES = _positive_int_env("DATAFORGE_PLAYGROUND_MAX_UPLOAD_BYTES", 1_048_576)
MAX_MULTIPART_OVERHEAD_BYTES = 16_384
MAX_UPLOAD_ROWS = _positive_int_env("DATAFORGE_PLAYGROUND_MAX_ROWS", 10_000)
MAX_UPLOAD_COLUMNS = _positive_int_env("DATAFORGE_PLAYGROUND_MAX_COLUMNS", 128)
MAX_UPLOAD_CELLS = _positive_int_env("DATAFORGE_PLAYGROUND_MAX_CELLS", 200_000)
REQUEST_TIMEOUT_SECONDS = _positive_int_env("DATAFORGE_PLAYGROUND_TIMEOUT_SECONDS", 20)
AGENT_REQUEST_TIMEOUT_SECONDS = _positive_int_env("DATAFORGE_PLAYGROUND_AGENT_TIMEOUT_SECONDS", 120)
PLAYGROUND_AGENT_MAX_STEPS = _positive_int_env("DATAFORGE_PLAYGROUND_AGENT_MAX_STEPS", 8)
FLAGGED_CELL_DETAIL_LIMIT = _positive_int_env("DATAFORGE_PLAYGROUND_FLAGGED_CELL_DETAIL_LIMIT", 500)
ISSUE_ROW_DISPLAY_LIMIT = _positive_int_env("DATAFORGE_PLAYGROUND_ISSUE_ROW_DISPLAY_LIMIT", 50)
MAX_EXTERNAL_FIXES = _positive_int_env("DATAFORGE_PLAYGROUND_MAX_EXTERNAL_FIXES", 200)
SAMPLES_DIR = Path(__file__).resolve().parent / "samples"
SLOWAPI_CONFIG = Path(__file__).resolve().parent / "slowapi.env"
ALLOWED_SAMPLES = {"hospital_10rows", "flights_10rows", "beers_10rows"}
ACCEPTED_UPLOAD_TYPES = {"", "text/csv", "text/plain", "application/vnd.ms-excel"}
OTEL_ENABLED_VALUES = {"1", "true", "yes", "on"}
PLAYGROUND_REPAIR_MODES = {"deterministic", "agent"}


RiskLevel = Literal["none", "low", "medium", "high"]
RepairReadiness = Literal["no_action", "verified", "partial", "blocked"]
ConstraintDecision = Literal["pending", "accepted", "rejected"]
WorkflowStageId = Literal[
    "intake",
    "schema_inference",
    "constraint_review",
    "detectors",
    "repair_candidates",
    "safety_gate",
    "smt_verifier",
    "dry_run_transaction",
    "receipt",
]
WorkflowStatus = Literal["queued", "running", "completed", "blocked", "failed", "cancelled"]
WORKFLOW_CONTRACT_VERSION: Literal["workflow_event_v1"] = "workflow_event_v1"
WORKFLOW_STAGE_IDS: tuple[WorkflowStageId, ...] = (
    "intake",
    "schema_inference",
    "constraint_review",
    "detectors",
    "repair_candidates",
    "safety_gate",
    "smt_verifier",
    "dry_run_transaction",
    "receipt",
)


class LimitPayload(BaseModel):
    """Processing limits exposed to playground clients."""

    max_upload_bytes: int
    max_rows: int
    max_columns: int
    max_cells: int


class LatencyMetrics(BaseModel):
    """Rolling latency snapshot."""

    window_size: int
    p50: float
    p95: float
    max: float


class MetricsSnapshot(BaseModel):
    """Small in-process metrics response for free-tier health checks."""

    requests_total: int
    responses_4xx: int
    responses_5xx: int
    error_rate: float
    latency_ms: LatencyMetrics
    routes: dict[str, int]


class RootResponse(BaseModel):
    """Stable API root response."""

    service: str
    status: Literal["ok"]
    api_version: str
    contract_version: str
    docs_url: str
    frontend_hosting: str


class HealthResponse(BaseModel):
    """Backend readiness and UI capability metadata."""

    service: str
    status: Literal["ok"]
    advanced_available: bool
    agent_available: bool
    entity_consensus_available: bool
    agent_policy: str
    agent_provider: str | None
    verify_available: bool
    agent_max_steps: int
    max_upload_bytes: int
    streaming_available: bool
    workflow_contract_version: Literal["workflow_event_v1"]
    api_version: str
    contract_version: str
    build_sha: str
    server_time_utc: str
    environment: str
    limits: LimitPayload
    cors_configured: bool
    otel_enabled: bool
    otel_instrumented: bool
    metrics: MetricsSnapshot


class SourceView(BaseModel):
    """Uploaded source facts safe to return to the browser."""

    name: str
    size_bytes: int
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: int
    columns: int
    column_names: list[str]


class IssueView(BaseModel):
    """Grouped detector evidence for one issue type/column/severity."""

    column: str
    issue_type: str
    severity: Literal["safe", "review", "unsafe"]
    row_indices: list[int]
    row_indices_truncated: bool = False
    count: int


class FlaggedCellView(BaseModel):
    """One flagged cell, individually addressable.

    ``IssueView`` groups by (column, issue_type, severity) and caps ``row_indices``
    at ``ISSUE_ROW_DISPLAY_LIMIT``, which destroys the per-cell record: on the
    measured hospital queue with inferred functional dependencies a single group
    holds roughly a thousand rows, so a client rebuilding cells from
    ``row_indices`` would show about fifty of them and look complete.

    It also discarded ``Issue.confidence`` (the only per-cell detector strength for
    cells with no proposed fix), ``Issue.actual``/``expected``, and
    ``Issue.reason`` -- which is where the detector's own numbers live, such as the
    modified z-score, the column median, and the populated fraction.
    """

    row: int
    column: str
    issue_type: str
    severity: Literal["safe", "review", "unsafe"]
    confidence: float
    actual: str
    expected: str | None = None
    reason: str


class FlaggedCellIndexView(BaseModel):
    """Every flagged cell's POSITION, in a columnar form.

    The map needs only coordinates: which column, which row. Sending full records
    for that is roughly twenty times larger than necessary -- at ~350 bytes per
    record, the measured 10,373-cell hospital queue is about 3.6 MB of JSON from a
    1 MiB upload. Two parallel integer arrays carry the same information for the
    map in ~15 bytes per cell.

    Rung is deliberately absent: an aggregated band may not carry one, and the
    outcome lists (repairs, suggested_fixes, failures) are small and already
    complete, so a client derives per-cell strength from those instead.
    """

    column_indices: list[int]
    rows: list[int]


class ConfidenceBinView(BaseModel):
    """One histogram bucket of detector confidence."""

    from_value: float
    to_value: float
    count: int


class ConfidenceClassView(BaseModel):
    """Detector-confidence shape for one issue type.

    Sent as a histogram rather than as thousands of floats. It also carries the mode
    and its share, because the load-bearing fact about this signal is how degenerate
    it is: in the measured hospital FD regime, 10,261 of 10,373 cells share one value.
    """

    issue_type: str
    bins: list[ConfidenceBinView]
    count: int
    distinct_values: int
    mode_value: float | None = None
    mode_share: float = 0.0


class FlaggedCellsView(BaseModel):
    """The per-cell channel, with its coverage stated rather than implied.

    Three parts, each sized to its job:

    * ``index`` -- every flagged cell's position, columnar and complete.
    * ``cells`` -- full records for a bounded, severity-ordered prefix, for the
      detail view. Ordering is severity, then descending detector confidence, then
      position: the detector ensemble's own ordering. It is NOT a triage score and
      carries no claim about how likely each cell is to be a genuine error.
    * ``confidence_histogram`` -- the confidence distribution per issue type.
    """

    index: FlaggedCellIndexView
    cells: list[FlaggedCellView]
    confidence_histogram: list[ConfidenceClassView] = Field(default_factory=list)
    total: int
    truncated: bool
    note: str


class ReviewRankedCellView(BaseModel):
    """One cell of an opt-in human-review ordering.

    Empty unless the caller supplied a ``review_ranker``. The playground never
    supplies one: it is an LLM scorer, so firing it per request would spend money
    on every analysis, and the auto-fire gate is a measured NO-GO
    (DECISIONS.md 2026-08-04). Surfaced so a CLI or library caller that DID opt in
    can render its ranking here.
    """

    row: int
    column: str
    triage_score: float
    reason: str


class ConstraintCandidateView(BaseModel):
    """Reviewable inferred constraint shown in the proof loop."""

    candidate_id: str
    kind: str
    columns: list[str]
    dependent: str | None = None
    inferred_type: str | None = None
    pattern: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    confidence: float
    evidence: str
    decision: ConstraintDecision
    repair_supported: bool


class SchemaInferenceView(BaseModel):
    """Ephemeral schema-inference evidence for the uploaded source."""

    schema_version: Literal["constraint_review_v1"]
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int
    candidates: list[ConstraintCandidateView]


class RiskSummary(BaseModel):
    """Categorical, evidence-derived risk summary."""

    dataset_level: RiskLevel
    repair_readiness: RepairReadiness
    severity_counts: dict[Literal["safe", "review", "unsafe"], int]
    pending_repair_supported_constraints: int
    reasons: list[str]


class VerifiedFixView(BaseModel):
    """Verified dry-run cell repair."""

    row: int
    column: str
    old_value: str
    new_value: str
    detector_id: str
    reason: str
    confidence: float
    provenance: str
    verifier_reason: str | None = None
    verification_strength: str | None = None
    review_reason: str | None = None


class RepairFailureView(BaseModel):
    """Issue-level repair attempt that did not produce a verified fix."""

    row: int
    column: str
    issue_type: str
    status: str
    reason: str
    attempt_count: int
    unsat_core: list[str] = Field(default_factory=list)


class RootCauseView(BaseModel):
    """Issue diagnosis carried in the public repair receipt."""

    row: int
    column: str
    issue_type: str
    category: str
    confidence: float
    reason: str


class CandidateRepairView(BaseModel):
    """Candidate repair carried in the public repair receipt."""

    row: int
    column: str
    old_value: str
    new_value: str
    detector_id: str
    operation: str
    reason: str
    confidence: float
    provenance: str
    verifier_reason: str
    verification_strength: str | None = None
    review_reason: str | None = None


class ProofObligationView(BaseModel):
    """Proof or safety obligation carried in the public repair receipt."""

    obligation_id: str
    verifier: str
    status: str
    reason: str
    unsat_core: list[str] = Field(default_factory=list)


class RepairJournalView(BaseModel):
    """Redacted dry-run transaction journal."""

    txn_id: str
    created_at: str
    source_name: str
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    fixes_count: int
    applied: bool
    events: list[dict[str, str]]
    note: str


class RepairReceiptView(BaseModel):
    """Stable repair receipt surfaced to browser clients."""

    schema_version: str
    receipt_version: str
    contract_version: str
    mode: str
    applied: bool
    reversible: bool
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    post_sha256: str | None = None
    txn_id: str | None = None
    safety_verdict: str
    verifier_verdict: str
    independent_verification: str = "not_run"
    issues_count: int
    fixes_count: int
    candidate_provenance: list[str]
    root_causes: list[RootCauseView] = Field(default_factory=list)
    candidate_repairs: list[CandidateRepairView] = Field(default_factory=list)
    applied_fixes: list[VerifiedFixView] = Field(default_factory=list)
    suggested_fixes: list[CandidateRepairView] = Field(default_factory=list)
    proof_obligations: list[ProofObligationView] = Field(default_factory=list)
    accepted_constraint_ids: list[str]
    constraints_artifact_sha256: str | None = None
    patch_plan_sha256: str | None = None
    revert_command: str | None = None
    review_ranking: list[ReviewRankedCellView] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    reason: str


class VerificationSummary(BaseModel):
    """Safety, verifier, and abstention evidence for the run."""

    safety_verdict: str
    verifier_verdict: str
    accepted_constraint_ids: list[str]
    failures: list[RepairFailureView]
    abstentions: list[str]
    failure_reasons: list[str]


class CertificateCheckView(BaseModel):
    """One independently re-checked claim from the trust certificate."""

    name: str
    ok: bool
    detail: str


class CertificateView(BaseModel):
    """Independent re-verification of the run's portable trust certificate.

    The receipt is a self-contained certificate: anyone holding the source data
    and this receipt can re-check its trust invariants (cryptographic identity,
    verifier acceptance, and proof-honesty of the auto-applied set) without
    re-running DataForge. This block reports that check, computed server-side
    against the exact uploaded bytes. For a stateless dry run it confirms the
    receipt describes this data and that no unproven write was auto-applied.
    """

    ok: bool
    checks: list[CertificateCheckView] = Field(default_factory=list)


class ApplyHandoff(BaseModel):
    """Local CLI handoff for reversible apply outside the hosted playground."""

    source_name: str
    dry_run_command: str
    apply_command: str
    audit_command: str
    revert_command: str
    note: str


class ResponseMeta(BaseModel):
    """Shared API response metadata."""

    api_version: str
    contract_version: str


class ProfileMeta(ResponseMeta):
    """Profile compatibility response metadata."""

    rows: int
    columns: int
    column_names: list[str]
    total_issues: int
    advanced_requested: bool


class ProfileResponse(BaseModel):
    """Compatibility response for POST /api/profile."""

    issues: list[IssueView]
    meta: ProfileMeta


class RepairResponse(BaseModel):
    """Compatibility response for POST /api/repair."""

    fixes: list[VerifiedFixView]
    txn_journal: RepairJournalView
    receipt: RepairReceiptView
    meta: ResponseMeta
    failures: list[RepairFailureView] = Field(default_factory=list)


class AgentTraceStepView(BaseModel):
    """One step of the verified agent's audit trace, safe for the browser."""

    step: int
    action_type: str
    accepted: bool | None = None
    detail: str


class AgentSummaryView(BaseModel):
    """Verified-agent run summary attached to an analysis in agent mode.

    The agent is a remote fine-tuned 0.5B model driving the same deterministic
    floor plus safety constitution and SMT verifier as the default path. It
    never applies data; ``agent_fixes`` are the residual fixes the model
    proposed beyond the deterministic floor, each independently verified.
    """

    policy_name: str
    steps_used: int
    max_steps: int
    floor_fix_count: int
    agent_fix_count: int
    residual_count: int
    reason: str
    agent_txn_id: str | None = None
    agent_fixes: list[VerifiedFixView] = Field(default_factory=list)
    # Honest abstention in agent mode. The engine computes this and it was being
    # dropped here, so the browser could see what the agent WOULD write but never
    # what it declined to write -- exactly the asymmetry this product exists to
    # avoid presenting.
    held_fixes: list[VerifiedFixView] = Field(default_factory=list)
    trace: list[AgentTraceStepView] = Field(default_factory=list)


class AnalyzeResponse(BaseModel):
    """Primary Playground proof-loop response."""

    source: SourceView
    schema_inference: SchemaInferenceView
    risk_summary: RiskSummary
    issues: list[IssueView]
    flagged_cells: FlaggedCellsView
    repairs: list[VerifiedFixView]
    verification: VerificationSummary
    certificate: CertificateView
    txn_journal: RepairJournalView
    receipt: RepairReceiptView
    apply_handoff: ApplyHandoff
    limitations: list[str]
    agent: AgentSummaryView | None = None
    meta: ResponseMeta


class ExternalFixInput(BaseModel):
    """One externally-proposed cell edit submitted to the guardrail.

    Mirrors ``dataforge.ExternalFix``. ``expected_old_value`` is an optional
    compare-and-set precondition: when present, a proposal is rejected as stale
    if the current cell differs (preventing a lost update).
    """

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    new_value: str
    expected_old_value: str | None = None


class VerifyScenarioView(BaseModel):
    """A curated 'untrusted agent' batch for one bundled sample.

    Pairs a mixed batch of proposals with the accepted constraint id(s) that make
    the schema-consistent ones provable, so one click shows the full
    proven / held / rejected split against an authoritative schema.
    """

    name: str
    proposer: str
    fixes: list[ExternalFixInput]
    accepted_constraint_ids: list[str]
    note: str


class VerifyFixesResponse(BaseModel):
    """Guardrail verdict for externally-proposed fixes (dry-run only).

    ``would_apply`` are the proposals that were *proven* (verified against an
    authoritative schema) and would auto-apply on ``dataforge verify-apply
    --apply``; ``receipt.suggested_fixes`` are held or rejected, each with an
    honest ``review_reason``; ``certificate`` independently re-verifies the
    receipt against the exact uploaded bytes. The hosted playground never mutates
    uploads, so nothing is written here.
    """

    source: SourceView
    proposer: str
    proposed_count: int
    authoritative_schema: bool
    would_apply: list[VerifiedFixView]
    receipt: RepairReceiptView
    verification: VerificationSummary
    certificate: CertificateView
    apply_handoff: ApplyHandoff
    limitations: list[str]
    meta: ResponseMeta


class WorkflowEvent(BaseModel):
    """Line-delimited workflow event for the agentic supervision cockpit."""

    schema_version: Literal["workflow_event_v1"] = WORKFLOW_CONTRACT_VERSION
    run_id: str
    sequence: int
    stage_id: WorkflowStageId
    status: WorkflowStatus
    summary: str
    started_at: str | None = None
    completed_at: str | None = None
    counts: dict[str, int | float | str | bool] = Field(default_factory=dict)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    uncertainty: str | None = None
    requires_human: bool = False
    analysis: AnalyzeResponse | None = None
    problem: dict[str, Any] | None = None


class _RequestMetrics:
    """Tiny in-process request counters for free-tier health reporting."""

    def __init__(self, window_size: int = 200) -> None:
        self._lock = Lock()
        self._window_size = window_size
        self._latencies_ms: deque[float] = deque(maxlen=window_size)
        self._requests_total = 0
        self._responses_4xx = 0
        self._responses_5xx = 0
        self._routes: dict[str, int] = defaultdict(int)

    def record(self, *, method: str, path: str, status_code: int, duration_ms: float) -> None:
        """Record one completed request."""
        with self._lock:
            self._requests_total += 1
            self._latencies_ms.append(duration_ms)
            self._routes[f"{method} {path}"] += 1
            if 400 <= status_code < 500:
                self._responses_4xx += 1
            elif status_code >= 500:
                self._responses_5xx += 1

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-safe metrics snapshot."""
        with self._lock:
            latencies = sorted(self._latencies_ms)
            total = self._requests_total
            responses_5xx = self._responses_5xx
            return {
                "requests_total": total,
                "responses_4xx": self._responses_4xx,
                "responses_5xx": responses_5xx,
                "error_rate": round(responses_5xx / total, 4) if total else 0.0,
                "latency_ms": {
                    "window_size": len(latencies),
                    "p50": _percentile(latencies, 0.50),
                    "p95": _percentile(latencies, 0.95),
                    "max": round(latencies[-1], 2) if latencies else 0.0,
                },
                "routes": dict(sorted(self._routes.items())),
            }


def _percentile(values: list[float], percentile: float) -> float:
    """Return a nearest-rank percentile for a small rolling window."""
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, int(round(percentile * (len(values) - 1)))))
    return round(values[index], 2)


request_metrics = _RequestMetrics()


def _request_id(request: Request) -> str | None:
    """Return the current request id when request middleware has assigned one."""
    request_id = getattr(request.state, "dataforge_request_id", None)
    return request_id if isinstance(request_id, str) and request_id else None


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach request IDs, duration headers, and lightweight metrics."""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.dataforge_request_id = request_id
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = (time.perf_counter() - started) * 1000
            request_metrics.record(
                method=request.method,
                path=request.url.path,
                status_code=500,
                duration_ms=duration_ms,
            )
            logger.exception(
                "Playground request crashed",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": round(duration_ms, 2),
                },
            )
            raise

        duration_ms = (time.perf_counter() - started) * 1000
        request_metrics.record(
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        response.headers["X-DataForge-Request-Id"] = request_id
        response.headers["X-DataForge-Duration-Ms"] = f"{duration_ms:.2f}"
        logger.info(
            "Playground request completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round(duration_ms, 2),
            },
        )
        return response


class SizeCapMiddleware(BaseHTTPMiddleware):
    """Reject requests whose declared Content-Length cannot contain a valid upload."""

    def __init__(
        self,
        app: ASGIApp,
        max_file_bytes: int = MAX_UPLOAD_BYTES,
        max_multipart_overhead_bytes: int = MAX_MULTIPART_OVERHEAD_BYTES,
    ) -> None:
        super().__init__(app)
        self.max_file_bytes = max_file_bytes
        self.max_body_bytes = max_file_bytes + max_multipart_overhead_bytes

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
                return problem_response(
                    status=400,
                    type_="https://dataforge.local/problems/invalid_content_length",
                    title="Invalid Content Length",
                    detail="The Content-Length header must be an integer.",
                    instance=str(request.url.path),
                    error="invalid_content_length",
                    request_id=_request_id(request),
                )
            if length > self.max_body_bytes:
                logger.warning(
                    "Rejected request: Content-Length %d exceeds max body %d",
                    length,
                    self.max_body_bytes,
                )
                return problem_response(
                    status=413,
                    type_="https://dataforge.local/problems/file_too_large",
                    title="File Too Large",
                    detail="The uploaded request body exceeds the playground limit.",
                    instance=str(request.url.path),
                    error="file_too_large",
                    max_bytes=self.max_file_bytes,
                    request_id=_request_id(request),
                )
        return await call_next(request)


class FallbackRateLimitMiddleware(BaseHTTPMiddleware):
    """Enforce the playground POST limit when slowapi is not installed."""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Apply a 10/minute in-memory fallback to mutating playground endpoints."""
        if request.method == "POST" and request.url.path in {
            "/api/analyze",
            "/api/analyze/stream",
            "/api/profile",
            "/api/repair",
        }:
            storage = limiter._storage
            key = (get_remote_address(request), request.url.path)
            if isinstance(storage, _FallbackStorage) and not storage.allow(
                key,
                limit=10,
                window_seconds=60.0,
            ):
                return problem_response(
                    status=429,
                    type_="https://dataforge.local/problems/rate_limit_exceeded",
                    title="Rate Limit Exceeded",
                    detail="10 per 1 minute",
                    instance=str(request.url.path),
                    headers={"Retry-After": "60"},
                    error="rate_limit_exceeded",
                    retry_after=60,
                    request_id=_request_id(request),
                )
        return await call_next(request)


class OriginGuardMiddleware(BaseHTTPMiddleware):
    """Reject browser requests from origins outside the configured allowlist."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        allow_origins: list[str],
        allow_origin_regex: str | None,
    ) -> None:
        super().__init__(app)
        self._allow_origins = frozenset(allow_origins)
        self._allow_origin_pattern = (
            re.compile(allow_origin_regex) if allow_origin_regex is not None else None
        )

    def _allowed(self, origin: str) -> bool:
        if origin in self._allow_origins:
            return True
        return bool(
            self._allow_origin_pattern is not None and self._allow_origin_pattern.fullmatch(origin)
        )

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Deny disallowed browser origins before endpoint handlers run."""
        origin = request.headers.get("origin")
        if origin and not self._allowed(origin):
            return problem_response(
                status=403,
                type_="https://dataforge.local/problems/origin_not_allowed",
                title="Origin Not Allowed",
                detail="This playground backend only accepts browser requests from configured frontend origins.",
                instance=str(request.url.path),
                error="origin_not_allowed",
                request_id=_request_id(request),
            )
        return await call_next(request)


if _SlowapiLimiter is not None:
    limiter: _LimiterLike = cast(
        _LimiterLike,
        _SlowapiLimiter(key_func=get_remote_address, config_filename=str(SLOWAPI_CONFIG)),
    )
else:
    limiter = _FallbackLimiter()


def _advanced_available() -> bool:
    """Return whether at least one backend LLM provider is configured."""
    return bool(
        os.environ.get("GROQ_API_KEY")
        or os.environ.get("GEMINI_API_KEY")
        or os.environ.get("AZURE_API_KEY")
    )


def _resolve_agent_policy() -> tuple[str, str | None]:
    """Resolve the agent policy backend and optional provider (least surprise).

    Precedence: an explicit ``DATAFORGE_PLAYGROUND_AGENT_POLICY`` always wins
    (e.g. ``hosted:azure`` or ``remote``). Otherwise, to avoid silently switching
    an existing deployment, a configured remote Space (``DATAFORGE_REMOTE_MODEL_URL``)
    keeps using ``remote``; a first-party Azure deployment (``AZURE_API_KEY``,
    e.g. gpt-5.6-sol) is used only when no remote Space is configured. To showcase
    the frontier proposer on a box that also has a remote Space, set the explicit
    override. When nothing is configured this returns ``("remote", None)`` so
    ``run_agent_repair`` fails fast into a 400.
    """
    override = os.environ.get("DATAFORGE_PLAYGROUND_AGENT_POLICY", "").strip()
    if override:
        kind, _, provider = override.partition(":")
        return (kind.strip() or "remote"), (provider.strip() or None)
    if os.environ.get("DATAFORGE_REMOTE_MODEL_URL", "").strip():
        return "remote", None
    if os.environ.get("AZURE_API_KEY", "").strip():
        return "hosted", "azure"
    return "remote", None


def _agent_policy_label(kind: str, provider: str | None) -> str:
    """Render a stable, UI-facing label for a resolved agent policy."""
    return f"{kind}:{provider}" if provider else kind


def _agent_available() -> bool:
    """Return whether a verified agent proposer backend is configured.

    Agent mode drives a proposer whose every fix is safety- and SMT-verified
    before display. It is available when either a first-party Azure deployment
    (``AZURE_API_KEY``, e.g. gpt-5.6-sol) or the remote trained-model Space
    (``DATAFORGE_REMOTE_MODEL_URL``) is configured; otherwise the playground
    degrades gracefully by not offering the mode (it never errors on the default
    path).
    """
    return bool(
        os.environ.get("DATAFORGE_REMOTE_MODEL_URL", "").strip()
        or os.environ.get("AZURE_API_KEY", "").strip()
    )


def _build_cors_origins() -> list[str]:
    """Build the explicit CORS allowlist from the environment."""
    env_origins = os.environ.get("DATAFORGE_PLAYGROUND_ORIGINS", "")
    return [origin.strip() for origin in env_origins.split(",") if origin.strip()]


def _build_cors_origin_regex() -> str | None:
    """Build the regex allowlist for local development only."""
    patterns: list[str] = []
    if os.environ.get("DATAFORGE_PLAYGROUND_DEV") == "1":
        patterns.append(r"http://(?:localhost|127(?:\.\d{1,3}){3})(?::\d+)?")
    if not patterns:
        return None
    return "^(" + "|".join(patterns) + ")$"


CORS_ORIGINS = _build_cors_origins()
CORS_ORIGIN_REGEX = _build_cors_origin_regex()


app = FastAPI(
    title="DataForge Playground API",
    description="Stateless backend for the hosted DataForge playground.",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url=None,
)
app.add_middleware(
    SizeCapMiddleware,
    max_file_bytes=MAX_UPLOAD_BYTES,
    max_multipart_overhead_bytes=MAX_MULTIPART_OVERHEAD_BYTES,
)
if not SLOWAPI_AVAILABLE:
    app.add_middleware(FallbackRateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_origin_regex=CORS_ORIGIN_REGEX,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)
app.add_middleware(
    OriginGuardMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_origin_regex=CORS_ORIGIN_REGEX,
)
app.add_middleware(RequestContextMiddleware)
app.state.limiter = limiter
app.add_exception_handler(HTTPException, problem_exception_handler)
OTEL_INSTRUMENTED = configure_fastapi_observability(app, service_name="dataforge-playground-api")


@app.exception_handler(_RateLimitExceeded)
async def _rate_limit_handler(request: Request, exc: Exception) -> JSONResponse:
    """Return a machine-readable 429 response."""
    detail = str(getattr(exc, "detail", str(exc)))
    return problem_response(
        status=429,
        type_="https://dataforge.local/problems/rate_limit_exceeded",
        title="Rate Limit Exceeded",
        detail=detail,
        instance=str(request.url.path),
        headers={"Retry-After": "60"},
        error="rate_limit_exceeded",
        retry_after=60,
        request_id=_request_id(request),
    )


def _upload_problem(
    *,
    status_code: int,
    error: str,
    message: str,
    **extensions: Any,
) -> HTTPException:
    """Build an HTTPException that normalizes to problem+json."""
    return HTTPException(
        status_code=status_code,
        detail={"error": error, "message": message, **extensions},
    )


def _validate_upload_file(file: UploadFile) -> None:
    """Reject clearly unsupported upload metadata before reading bytes."""
    upload_name = Path(file.filename or "upload.csv").name
    content_type = (file.content_type or "").split(";", maxsplit=1)[0].strip().lower()
    if not upload_name.lower().endswith(".csv") and content_type not in ACCEPTED_UPLOAD_TYPES:
        raise _upload_problem(
            status_code=415,
            error="unsupported_file_type",
            message="Upload a CSV file with a .csv extension or text/csv content type.",
            accepted_types=sorted(ACCEPTED_UPLOAD_TYPES - {""}),
        )


async def _read_upload(file: UploadFile) -> bytes:
    """Read an uploaded file with a defensive hard cap."""
    _validate_upload_file(file)
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise _upload_problem(
            status_code=413,
            error="file_too_large",
            message="The uploaded CSV is larger than the hosted playground limit.",
            max_bytes=MAX_UPLOAD_BYTES,
        )
    if len(data) == 0:
        raise _upload_problem(
            status_code=400,
            error="empty_csv",
            message="CSV must include a header row and at least one data row.",
        )
    return data


def _csv_to_df(data: bytes) -> pd.DataFrame:
    """Parse CSV bytes into a string-preserving DataFrame."""
    try:
        df = pd.read_csv(
            io.BytesIO(data),
            dtype=str,
            keep_default_na=False,
            na_filter=False,
        )
    except EmptyDataError as exc:
        raise _upload_problem(
            status_code=400,
            error="empty_csv",
            message="CSV must include a header row and at least one data row.",
        ) from exc
    except ParserError as exc:
        raise _upload_problem(
            status_code=400,
            error="invalid_csv",
            message="CSV could not be parsed. Check quoting, delimiters, and row structure.",
        ) from exc

    if len(df.columns) == 0 or len(df) == 0:
        raise _upload_problem(
            status_code=400,
            error="empty_csv",
            message="CSV must include a header row and at least one data row.",
        )
    _enforce_dataframe_limits(df)
    return df


def _enforce_dataframe_limits(df: pd.DataFrame) -> None:
    """Apply hosted playground row, column, and cell limits after parsing."""
    row_total = len(df)
    column_total = len(df.columns)
    cell_total = row_total * column_total
    if row_total > MAX_UPLOAD_ROWS:
        raise _upload_problem(
            status_code=413,
            error="too_many_rows",
            message="The uploaded CSV has more rows than the hosted playground allows.",
            max_rows=MAX_UPLOAD_ROWS,
            observed_rows=row_total,
        )
    if column_total > MAX_UPLOAD_COLUMNS:
        raise _upload_problem(
            status_code=413,
            error="too_many_columns",
            message="The uploaded CSV has more columns than the hosted playground allows.",
            max_columns=MAX_UPLOAD_COLUMNS,
            observed_columns=column_total,
        )
    if cell_total > MAX_UPLOAD_CELLS:
        raise _upload_problem(
            status_code=413,
            error="too_many_cells",
            message="The uploaded CSV has too many cells for the hosted playground.",
            max_cells=MAX_UPLOAD_CELLS,
            observed_cells=cell_total,
        )


def _severity_to_str(severity: Severity) -> str:
    """Convert a Severity enum into the JSON response value."""
    return severity.value


def _issue_views(issues: list[Issue]) -> list[IssueView]:
    """Group detector issues and cap display row lists deterministically."""
    grouped: dict[tuple[str, str, str], list[int]] = {}
    for issue in issues:
        key = (issue.column, issue.issue_type, _severity_to_str(issue.severity))
        grouped.setdefault(key, []).append(issue.row)

    severity_order = {"unsafe": 0, "review": 1, "safe": 2}
    payload_issues: list[IssueView] = []
    for (column, issue_type, severity), row_indices in grouped.items():
        unique_rows = sorted(set(row_indices))
        displayed_rows = unique_rows[:ISSUE_ROW_DISPLAY_LIMIT]
        payload_issues.append(
            IssueView(
                column=column,
                issue_type=issue_type,
                severity=cast(Literal["safe", "review", "unsafe"], severity),
                row_indices=displayed_rows,
                row_indices_truncated=len(unique_rows) > len(displayed_rows),
                count=len(unique_rows),
            )
        )
    payload_issues.sort(
        key=lambda issue: (severity_order[issue.severity], -issue.count, issue.column)
    )
    return payload_issues


def _confidence_histogram(issues: list[Issue]) -> list[ConfidenceClassView]:
    """Summarise detector confidence per issue type as a histogram.

    Sent instead of thousands of raw floats, and carrying the mode share because the
    honest finding about this signal is how little it varies.
    """
    bin_count = 10
    grouped: dict[str, list[float]] = {}
    for issue in issues:
        grouped.setdefault(issue.issue_type, []).append(issue.confidence)

    classes: list[ConfidenceClassView] = []
    for issue_type, values in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        counts = [0] * bin_count
        frequency: dict[float, int] = {}
        for value in values:
            clamped = min(0.999999, max(0.0, value))
            counts[int(clamped * bin_count)] += 1
            frequency[value] = frequency.get(value, 0) + 1
        mode_value: float | None = None
        mode_count = 0
        for value, count in frequency.items():
            if count > mode_count:
                mode_count = count
                mode_value = value
        classes.append(
            ConfidenceClassView(
                issue_type=issue_type,
                bins=[
                    ConfidenceBinView(
                        from_value=index / bin_count,
                        to_value=(index + 1) / bin_count,
                        count=counts[index],
                    )
                    for index in range(bin_count)
                ],
                count=len(values),
                distinct_values=len(frequency),
                mode_value=mode_value,
                mode_share=(mode_count / len(values)) if values else 0.0,
            )
        )
    return classes


def _flagged_cells_view(issues: list[Issue], column_names: list[str]) -> FlaggedCellsView:
    """Build the per-cell channel, stating its own coverage.

    One record per detected cell, deduped on (row, column) because the detector
    ensemble already resolves one issue per cell. Positions travel in a columnar
    index so the map is complete without a multi-megabyte payload; full records
    travel only for a bounded, severity-ordered prefix.
    """
    severity_order = {"unsafe": 0, "review": 1, "safe": 2}
    by_cell: dict[tuple[int, str], Issue] = {}
    for issue in issues:
        by_cell.setdefault((issue.row, issue.column), issue)

    ordered = sorted(
        by_cell.values(),
        key=lambda issue: (
            severity_order[_severity_to_str(issue.severity)],
            -issue.confidence,
            issue.row,
            issue.column,
        ),
    )
    total = len(ordered)
    detail = ordered[:FLAGGED_CELL_DETAIL_LIMIT]
    truncated = total > len(detail)

    column_position = {name: index for index, name in enumerate(column_names)}
    index_columns: list[int] = []
    index_rows: list[int] = []
    for issue in ordered:
        position = column_position.get(issue.column)
        if position is None:
            continue
        index_columns.append(position)
        index_rows.append(issue.row)

    if total == 0:
        note = "No cells were flagged. This is a measured result, not a missing one."
    elif truncated:
        note = (
            f"All {total} flagged cells are located on the map. Full details are listed for "
            f"the first {len(detail)}, ordered by severity then detector confidence."
        )
    else:
        note = f"All {total} flagged cells are located and individually listed."

    return FlaggedCellsView(
        index=FlaggedCellIndexView(column_indices=index_columns, rows=index_rows),
        cells=[
            FlaggedCellView(
                row=issue.row,
                column=issue.column,
                issue_type=issue.issue_type,
                severity=cast(
                    Literal["safe", "review", "unsafe"], _severity_to_str(issue.severity)
                ),
                confidence=issue.confidence,
                actual=issue.actual,
                expected=issue.expected,
                reason=issue.reason,
            )
            for issue in detail
        ],
        confidence_histogram=_confidence_histogram(list(by_cell.values())),
        total=total,
        truncated=truncated,
        note=note,
    )


def _profile_response(
    issues: list[Issue],
    df: pd.DataFrame,
    *,
    advanced_requested: bool,
) -> ProfileResponse:
    """Format profile evidence into the compatibility response contract."""
    return ProfileResponse(
        issues=_issue_views(issues),
        meta=ProfileMeta(
            rows=len(df),
            columns=len(df.columns),
            column_names=list(df.columns),
            total_issues=len(issues),
            advanced_requested=advanced_requested,
            api_version=app.version,
            contract_version=CONTRACT_VERSION,
        ),
    )


def _fix_views(fixes: list[VerifiedFix]) -> list[VerifiedFixView]:
    """Return public verified-fix views."""
    return [
        VerifiedFixView(
            row=fix.row,
            column=fix.column,
            old_value=fix.old_value,
            new_value=fix.new_value,
            detector_id=fix.detector_id,
            reason=fix.reason,
            confidence=fix.confidence,
            provenance=fix.provenance,
            verifier_reason=fix.verifier_reason,
            verification_strength=fix.verification_strength,
            review_reason=fix.review_reason,
        )
        for fix in fixes
    ]


def _failure_views(failures: list[Any]) -> list[RepairFailureView]:
    """Return public attempted-but-not-fixed views."""
    return [
        RepairFailureView(
            row=failure.row,
            column=failure.column,
            issue_type=failure.issue_type,
            status=failure.status,
            reason=failure.reason,
            attempt_count=failure.attempt_count,
            unsat_core=list(failure.unsat_core),
        )
        for failure in failures
    ]


def _journal_view(transaction: RepairTransaction, *, source_name: str) -> RepairJournalView:
    """Format a redacted ephemeral transaction journal."""
    return RepairJournalView(
        txn_id=transaction.txn_id,
        created_at=transaction.created_at.isoformat(),
        source_name=source_name,
        source_sha256=transaction.source_sha256,
        fixes_count=len(transaction.fixes),
        applied=transaction.applied,
        events=[{"event_type": "created"}],
        note=(
            "Playground is stateless. This journal is ephemeral and discarded "
            "after the response. Install the CLI to apply and revert repairs."
        ),
    )


def _receipt_view(receipt: Any) -> RepairReceiptView:
    """Format the engine repair receipt for browser clients."""
    return RepairReceiptView(
        schema_version=receipt.schema_version,
        receipt_version=receipt.receipt_version,
        contract_version=receipt.contract_version,
        mode=receipt.mode,
        applied=receipt.applied,
        reversible=receipt.reversible,
        source_sha256=receipt.source_sha256,
        post_sha256=receipt.post_sha256,
        txn_id=receipt.txn_id,
        safety_verdict=receipt.safety_verdict,
        verifier_verdict=receipt.verifier_verdict,
        independent_verification=receipt.independent_verification,
        issues_count=receipt.issues_count,
        fixes_count=receipt.fixes_count,
        candidate_provenance=list(receipt.candidate_provenance),
        root_causes=[
            RootCauseView(**root_cause.model_dump()) for root_cause in receipt.root_causes
        ],
        candidate_repairs=[
            CandidateRepairView(**candidate.model_dump()) for candidate in receipt.candidate_repairs
        ],
        applied_fixes=_fix_views(list(receipt.applied_fixes)),
        suggested_fixes=[
            CandidateRepairView(**candidate.model_dump()) for candidate in receipt.suggested_fixes
        ],
        proof_obligations=[
            ProofObligationView(
                **{
                    **obligation.model_dump(),
                    "unsat_core": list(obligation.unsat_core),
                }
            )
            for obligation in receipt.proof_obligations
        ],
        accepted_constraint_ids=list(receipt.accepted_constraint_ids),
        constraints_artifact_sha256=receipt.constraints_artifact_sha256,
        patch_plan_sha256=receipt.patch_plan_sha256,
        revert_command=receipt.revert_command,
        review_ranking=[
            ReviewRankedCellView(
                row=cell.row,
                column=cell.column,
                triage_score=cell.triage_score,
                reason=cell.reason,
            )
            for cell in getattr(receipt, "review_ranking", [])
        ],
        limitations=list(receipt.limitations),
        reason=receipt.reason,
    )


def _certificate_view(receipt: Any, *, source_bytes: bytes) -> CertificateView:
    """Independently re-verify the receipt as a portable trust certificate.

    Uses the pure, dependency-free :func:`verify_certificate` against the exact
    uploaded bytes, so the browser can show that the receipt re-verifies without
    trusting the server. Fail-open on the check itself is not possible here: any
    failed invariant is reported truthfully via ``ok=False``.
    """
    verification = verify_certificate(
        receipt.model_dump(mode="json"),
        data_bytes=source_bytes,
    )
    return CertificateView(
        ok=verification.ok,
        checks=[
            CertificateCheckView(name=check.name, ok=check.ok, detail=check.detail)
            for check in verification.checks
        ],
    )


def _candidate_views(artifact: Any) -> list[ConstraintCandidateView]:
    """Format reviewable inferred constraints for the browser."""
    candidates: list[ConstraintCandidateView] = []
    for reviewed in artifact.candidates:
        candidate = reviewed.candidate
        candidates.append(
            ConstraintCandidateView(
                candidate_id=reviewed.candidate_id,
                kind=candidate.kind,
                columns=list(candidate.columns),
                dependent=candidate.dependent,
                inferred_type=candidate.inferred_type,
                pattern=candidate.pattern,
                min_value=candidate.min_value,
                max_value=candidate.max_value,
                confidence=candidate.confidence,
                evidence=candidate.evidence,
                decision=reviewed.decision,
                repair_supported=candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS,
            )
        )
    return candidates


def _parse_external_fixes(raw: str | None) -> list[ExternalFix]:
    """Parse the JSON form field describing externally-proposed cell fixes.

    Accepts either a JSON array or an object with a ``fixes`` array of
    ``{row, column, new_value, expected_old_value?}`` objects, matching the CLI
    and MCP contract.
    """
    if raw is None or not raw.strip():
        raise _upload_problem(
            status_code=400,
            error="empty_fixes",
            message="Provide at least one proposed fix as a JSON array of "
            "{row, column, new_value, expected_old_value?} objects.",
        )
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise _upload_problem(
            status_code=400,
            error="invalid_fixes",
            message="fixes must be valid JSON.",
        ) from exc
    if isinstance(payload, dict) and "fixes" in payload:
        payload = payload["fixes"]
    if not isinstance(payload, list) or not payload:
        raise _upload_problem(
            status_code=400,
            error="empty_fixes",
            message="fixes must be a non-empty JSON array of proposed cell edits.",
        )
    if len(payload) > MAX_EXTERNAL_FIXES:
        raise _upload_problem(
            status_code=400,
            error="too_many_fixes",
            message=f"A batch may propose at most {MAX_EXTERNAL_FIXES} fixes.",
            max_fixes=MAX_EXTERNAL_FIXES,
        )
    fixes: list[ExternalFix] = []
    for index, spec in enumerate(payload):
        if not isinstance(spec, dict):
            raise _upload_problem(
                status_code=400,
                error="invalid_fixes",
                message=f"Fix #{index} is not a JSON object.",
            )
        try:
            fixes.append(
                ExternalFix(
                    row=int(spec["row"]),
                    column=str(spec["column"]),
                    new_value=str(spec["new_value"]),
                    expected_old_value=(
                        None
                        if spec.get("expected_old_value") is None
                        else str(spec["expected_old_value"])
                    ),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _upload_problem(
                status_code=400,
                error="invalid_fixes",
                message=f"Fix #{index} must have integer row, string column, and a new_value.",
            ) from exc
    return fixes


def _parse_accepted_constraint_ids(raw: str | None) -> list[str]:
    """Parse the JSON form field for accepted inferred constraints."""
    if raw is None or not raw.strip():
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise _upload_problem(
            status_code=400,
            error="invalid_accepted_constraint_ids",
            message="accepted_constraint_ids must be a JSON array of candidate ids.",
        ) from exc
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise _upload_problem(
            status_code=400,
            error="invalid_accepted_constraint_ids",
            message="accepted_constraint_ids must be a JSON array of candidate ids.",
        )

    accepted: list[str] = []
    seen: set[str] = set()
    for candidate_id in payload:
        if candidate_id in seen:
            continue
        accepted.append(candidate_id)
        seen.add(candidate_id)
    return accepted


def _artifact_with_accepted_ids(artifact: Any, accepted_ids: list[str]) -> Any:
    """Return a reviewed artifact with only the submitted ids accepted."""
    known_ids = {candidate.candidate_id for candidate in artifact.candidates}
    unknown_ids = sorted(set(accepted_ids) - known_ids)
    if unknown_ids:
        raise _upload_problem(
            status_code=400,
            error="unknown_constraint_id",
            message="One or more accepted constraint ids were not produced for this CSV.",
            unknown_ids=unknown_ids,
        )
    return update_constraint_review_artifact(artifact, accept_ids=accepted_ids)


def _risk_summary(
    *,
    issues: list[Issue],
    fixes: list[VerifiedFix],
    failures: list[RepairFailureView],
    receipt: RepairReceiptView,
    candidate_views: list[ConstraintCandidateView],
) -> RiskSummary:
    """Build a categorical risk summary without calibrated accuracy claims."""
    severity_counts: dict[Literal["safe", "review", "unsafe"], int] = {
        "safe": 0,
        "review": 0,
        "unsafe": 0,
    }
    for issue in issues:
        if issue.severity.value == "safe":
            severity_counts["safe"] += 1
        elif issue.severity.value == "review":
            severity_counts["review"] += 1
        elif issue.severity.value == "unsafe":
            severity_counts["unsafe"] += 1

    pending_supported = sum(
        1
        for candidate in candidate_views
        if candidate.repair_supported and candidate.decision == "pending"
    )
    reasons: list[str] = []
    if severity_counts["unsafe"]:
        reasons.append(f"{severity_counts['unsafe']} unsafe issue(s) require review.")
    if severity_counts["review"]:
        reasons.append(f"{severity_counts['review']} review-level issue(s) were detected.")
    if failures:
        reasons.append(f"{len(failures)} issue(s) were attempted but not verified as repairs.")
    if pending_supported:
        reasons.append(
            f"{pending_supported} repair-supported inferred constraint(s) remain pending."
        )
    if not reasons:
        reasons.append("No current detector findings were reported for this CSV.")

    if severity_counts["unsafe"] or failures or receipt.verifier_verdict in {"reject", "unknown"}:
        dataset_level: RiskLevel = "high"
    elif severity_counts["review"] or pending_supported:
        dataset_level = "medium"
    elif severity_counts["safe"]:
        dataset_level = "low"
    else:
        dataset_level = "none"

    if not issues and not fixes and not failures:
        readiness: RepairReadiness = "no_action"
    elif (
        fixes
        and not failures
        and receipt.safety_verdict == "allow"
        and receipt.verifier_verdict == "accept"
    ):
        readiness = "verified"
    elif fixes:
        readiness = "partial"
    else:
        readiness = "blocked"

    return RiskSummary(
        dataset_level=dataset_level,
        repair_readiness=readiness,
        severity_counts=severity_counts,
        pending_repair_supported_constraints=pending_supported,
        reasons=reasons,
    )


def _apply_handoff(source_name: str, receipt: RepairReceiptView) -> ApplyHandoff:
    """Build a local CLI handoff without enabling hosted mutation."""
    source_ref = f"path/to/{source_name}"
    dry_run_command = f"dataforge repair {source_ref} --dry-run"
    apply_command = f"dataforge repair {source_ref} --apply"
    if receipt.accepted_constraint_ids:
        dry_run_command = f"dataforge repair {source_ref} --constraints constraints.json --dry-run"
        apply_command = f"dataforge repair {source_ref} --constraints constraints.json --apply"
    txn_ref = receipt.txn_id or "<txn-id>"
    return ApplyHandoff(
        source_name=source_name,
        dry_run_command=dry_run_command,
        apply_command=apply_command,
        audit_command=f"dataforge audit {txn_ref}",
        revert_command=f"dataforge revert {txn_ref}",
        note=(
            "The hosted playground never mutates uploads. Apply and byte-for-byte revert "
            "are local CLI transaction workflows."
        ),
    )


def _verify_apply_handoff(source_name: str, receipt: RepairReceiptView) -> ApplyHandoff:
    """Build the local ``verify-apply`` CLI handoff for externally-proposed fixes."""
    source_ref = f"path/to/{source_name}"
    constraints = " --constraints constraints.json" if receipt.accepted_constraint_ids else ""
    dry_run_command = (
        f"dataforge verify-apply {source_ref} --fixes fixes.json{constraints} --dry-run"
    )
    apply_command = (
        f"dataforge verify-apply {source_ref} --fixes fixes.json{constraints} "
        "--apply --confirm-escalations"
    )
    txn_ref = receipt.txn_id or "<txn-id>"
    return ApplyHandoff(
        source_name=source_name,
        dry_run_command=dry_run_command,
        apply_command=apply_command,
        audit_command=f"dataforge audit {txn_ref}",
        revert_command=f"dataforge revert {txn_ref}",
        note=(
            "The hosted playground verifies external fixes in a stateless dry run and never "
            "mutates uploads. Applying proven fixes and byte-for-byte revert are local CLI "
            "transaction workflows."
        ),
    )


def _require_advanced_mode(advanced_requested: bool) -> None:
    """Reject advanced mode requests unless a provider key is configured."""
    if advanced_requested and not _advanced_available():
        raise HTTPException(status_code=400, detail={"error": "advanced_mode_unavailable"})


def _analyze_upload(
    *,
    upload_name: str,
    source_bytes: bytes,
    accepted_constraint_ids: list[str],
    allow_llm: bool,
    allow_entity_consensus: bool = False,
) -> AnalyzeResponse:
    """Run the proof-loop analysis pipeline inside a temporary workspace."""
    df = _csv_to_df(source_bytes)
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_root = Path(tmpdir)
        upload_path = temp_root / upload_name
        upload_path.write_bytes(source_bytes)

        inference = infer_schema(df)
        pending_artifact = build_constraint_review_artifact(
            inference,
            source_path=upload_path,
            source_sha256=source_sha256,
        )
        constraints_artifact = _artifact_with_accepted_ids(
            pending_artifact, accepted_constraint_ids
        )
        constraints_payload = dump_constraint_review_artifact(constraints_artifact).encode("utf-8")
        constraints_sha256 = hashlib.sha256(constraints_payload).hexdigest()

        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=upload_path,
                mode="dry_run",
                schema=None,
                create_dry_run_transaction=True,
                allow_llm=allow_llm,
                allow_entity_consensus=allow_entity_consensus,
                constraints=constraints_artifact,
                constraints_artifact_sha256=constraints_sha256,
            )
        )
        if result.transaction is None:
            raise RuntimeError(result.receipt.reason)

    candidates = _candidate_views(constraints_artifact)
    receipt = _receipt_view(result.receipt)
    failures = _failure_views(result.failures)
    repairs = _fix_views(result.fixes)
    return AnalyzeResponse(
        source=SourceView(
            name=upload_name,
            size_bytes=len(source_bytes),
            sha256=source_sha256,
            rows=len(df),
            columns=len(df.columns),
            column_names=list(df.columns),
        ),
        schema_inference=SchemaInferenceView(
            schema_version=constraints_artifact.schema_version,
            source_sha256=constraints_artifact.source_sha256,
            row_count=constraints_artifact.row_count,
            candidates=candidates,
        ),
        risk_summary=_risk_summary(
            issues=result.issues,
            fixes=result.fixes,
            failures=failures,
            receipt=receipt,
            candidate_views=candidates,
        ),
        issues=_issue_views(result.issues),
        flagged_cells=_flagged_cells_view(result.issues, list(df.columns)),
        repairs=repairs,
        verification=VerificationSummary(
            safety_verdict=receipt.safety_verdict,
            verifier_verdict=receipt.verifier_verdict,
            accepted_constraint_ids=receipt.accepted_constraint_ids,
            failures=failures,
            abstentions=list(result.receipt.abstentions),
            failure_reasons=list(result.receipt.failure_reasons),
        ),
        certificate=_certificate_view(result.receipt, source_bytes=source_bytes),
        txn_journal=_journal_view(result.transaction, source_name=upload_name),
        receipt=receipt,
        apply_handoff=_apply_handoff(upload_name, receipt),
        limitations=[
            "Hosted analysis is stateless and dry-run only.",
            "Inferred constraints are pending unless explicitly accepted for this run.",
            "Current detectors cover type mismatches, decimal shifts, and functional dependencies.",
        ],
        meta=ResponseMeta(api_version=app.version, contract_version=CONTRACT_VERSION),
    )


def _agent_analyze_upload(
    *,
    upload_name: str,
    source_bytes: bytes,
    accepted_constraint_ids: list[str],
) -> AnalyzeResponse:
    """Run the verified multi-step agent over the remote trained model.

    The deterministic proof-loop response is produced first (it supplies the
    verified schema, detectors, safety/SMT receipt, and dry-run journal shown in
    the cockpit). The real :func:`run_agent_repair` loop then runs against the
    remote model policy; its residual, individually verified fixes and full
    audit trace are attached as an :class:`AgentSummaryView`. Nothing is applied.
    """
    base = _analyze_upload(
        upload_name=upload_name,
        source_bytes=source_bytes,
        accepted_constraint_ids=accepted_constraint_ids,
        allow_llm=False,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        upload_path = Path(tmpdir) / upload_name
        upload_path.write_bytes(source_bytes)
        policy_kind, policy_provider = _resolve_agent_policy()
        try:
            result = run_agent_repair(
                AgentRepairRequest(
                    source_path=upload_path,
                    mode="dry_run",
                    policy=policy_kind,
                    provider=policy_provider,
                    max_steps=PLAYGROUND_AGENT_MAX_STEPS,
                )
            )
        except PolicyUnavailableError as exc:
            raise _upload_problem(
                status_code=400,
                error="agent_mode_unavailable",
                message=(
                    "Agent mode requires a configured verified proposer "
                    "(set AZURE_API_KEY for a first-party Azure deployment, "
                    "or DATAFORGE_REMOTE_MODEL_URL for the remote model)."
                ),
            ) from exc

    agent_only_fixes = [fix for fix in result.fixes if fix.provenance != "deterministic"]
    base.agent = AgentSummaryView(
        policy_name=result.policy_name,
        steps_used=result.steps_used,
        max_steps=result.max_steps,
        floor_fix_count=result.floor_fix_count,
        agent_fix_count=result.agent_fix_count,
        residual_count=result.residual_count,
        reason=result.reason,
        agent_txn_id=result.txn_id,
        agent_fixes=_fix_views(agent_only_fixes),
        held_fixes=_fix_views(list(getattr(result, "held_fixes", []))),
        trace=[
            AgentTraceStepView(
                step=record.step,
                action_type=record.action_type,
                accepted=record.accepted,
                detail=record.detail,
            )
            for record in result.trace
        ],
    )
    base.limitations = [
        *base.limitations,
        f"Agent proposals come from the '{result.policy_name}' policy; each proposed "
        "fix is still safety- and SMT-verified before display, and nothing is applied "
        "(a stronger proposer does not bypass the gate).",
    ]
    return base


def _verify_fixes_upload(
    *,
    upload_name: str,
    source_bytes: bytes,
    fixes: list[ExternalFix],
    accepted_constraint_ids: list[str],
    proposer: str,
    confirm_escalations: bool,
    allow_unproven: bool,
) -> VerifyFixesResponse:
    """Verify externally-proposed fixes through the shared gate (stateless dry run).

    Mirrors ``_analyze_upload``'s temp-workspace pattern but routes through
    ``verify_and_apply``: an untrusted actor proposes edits, and only the
    schema-proven ones are reported as would-apply while the rest are held or
    rejected with honest reasons. Nothing is written; ``mode`` is always dry_run.
    """
    df = _csv_to_df(source_bytes)
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    with tempfile.TemporaryDirectory() as tmpdir:
        upload_path = Path(tmpdir) / upload_name
        upload_path.write_bytes(source_bytes)

        inference = infer_schema(df)
        pending_artifact = build_constraint_review_artifact(
            inference,
            source_path=upload_path,
            source_sha256=source_sha256,
        )
        constraints_artifact = _artifact_with_accepted_ids(
            pending_artifact, accepted_constraint_ids
        )

        result = verify_and_apply(
            VerifyAndApplyRequest(
                source_path=upload_path,
                fixes=fixes,
                mode="dry_run",
                constraints=constraints_artifact,
                proposer=proposer,
                confirm_escalations=confirm_escalations,
                allow_unproven_autoapply=allow_unproven,
            )
        )

    receipt = _receipt_view(result.receipt)
    failures = _failure_views(result.failures)
    return VerifyFixesResponse(
        source=SourceView(
            name=upload_name,
            size_bytes=len(source_bytes),
            sha256=source_sha256,
            rows=len(df),
            columns=len(df.columns),
            column_names=list(df.columns),
        ),
        proposer=proposer,
        proposed_count=len(fixes),
        authoritative_schema=bool(accepted_constraint_ids),
        would_apply=_fix_views(result.fixes),
        receipt=receipt,
        verification=VerificationSummary(
            safety_verdict=receipt.safety_verdict,
            verifier_verdict=receipt.verifier_verdict,
            accepted_constraint_ids=receipt.accepted_constraint_ids,
            failures=failures,
            abstentions=list(result.receipt.abstentions),
            failure_reasons=list(result.receipt.failure_reasons),
        ),
        certificate=_certificate_view(result.receipt, source_bytes=source_bytes),
        apply_handoff=_verify_apply_handoff(upload_name, receipt),
        limitations=[
            "Hosted verification is stateless and dry-run only; no upload is ever mutated.",
            "External values are untrusted: a fix is proven (would auto-apply) only against an "
            "authoritative schema (accepted constraints); otherwise it is held for review.",
            "Every external write escalates the unconfirmed-write safety rule; applying proven "
            "fixes and byte-for-byte revert are local CLI workflows.",
        ],
        meta=ResponseMeta(api_version=app.version, contract_version=CONTRACT_VERSION),
    )


_VERIFY_SCENARIOS: dict[str, dict[str, Any]] = {
    "hospital_10rows": {
        "proposer": "triage-agent",
        "accept": [("column_type", "er_wait_time"), ("column_type", "rating")],
        "fixes": [
            {"row": 0, "column": "er_wait_time", "new_value": "30"},
            {"row": 1, "column": "rating", "new_value": "abc"},
            {"row": 2, "column": "rating", "new_value": "4.0", "expected_old_value": "WRONG"},
            {"row": 0, "column": "ghost_column", "new_value": "x"},
        ],
        "note": (
            "A triage agent proposed four edits. Only the correctly-typed edit is proven and "
            "would apply; a type-corrupting value, a stale edit, and an edit to a non-existent "
            "column are each blocked with an honest reason."
        ),
    },
    "beers_10rows": {
        "proposer": "catalog-agent",
        "accept": [("column_type", "brewery_id"), ("column_type", "id")],
        "fixes": [
            {"row": 0, "column": "brewery_id", "new_value": "172"},
            {"row": 1, "column": "id", "new_value": "abc"},
            {"row": 2, "column": "brewery_id", "new_value": "300", "expected_old_value": "WRONG"},
            {"row": 0, "column": "ghost_column", "new_value": "x"},
        ],
        "note": (
            "A catalog agent proposed four edits. Only the correctly-typed edit is proven and "
            "would apply; a type-corrupting value, a stale edit, and an edit to a non-existent "
            "column are each blocked with an honest reason."
        ),
    },
    "flights_10rows": {
        "proposer": "ingest-agent",
        "accept": [("column_type", "tuple_id")],
        "fixes": [
            {"row": 0, "column": "tuple_id", "new_value": "104"},
            {"row": 1, "column": "tuple_id", "new_value": "abc"},
            {"row": 2, "column": "tuple_id", "new_value": "999", "expected_old_value": "WRONG"},
            {"row": 0, "column": "ghost_column", "new_value": "x"},
        ],
        "note": (
            "An ingest agent proposed four edits. Only the correctly-typed edit is proven and "
            "would apply; a type-corrupting value, a stale edit, and an edit to a non-existent "
            "column are each blocked with an honest reason."
        ),
    },
}


def _build_verify_scenario(name: str) -> VerifyScenarioView:
    """Build a curated untrusted-agent batch for one bundled sample.

    Resolves the accepted constraint id(s) that authorize the correct edit
    server-side (candidate ids are content-derived), so the scripted batch
    reliably yields a proven / held / rejected split against an authoritative
    schema.
    """
    if name not in ALLOWED_SAMPLES:
        raise HTTPException(
            status_code=404,
            detail={"error": "sample_not_found", "available": sorted(ALLOWED_SAMPLES)},
        )
    spec = _VERIFY_SCENARIOS.get(name)
    if spec is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "scenario_not_found", "available": sorted(_VERIFY_SCENARIOS)},
        )
    csv_path = SAMPLES_DIR / f"{name}.csv"
    if not csv_path.exists():
        logger.error("Scenario sample file missing on disk: %s", csv_path)
        raise HTTPException(status_code=500, detail={"error": "sample_file_missing"})

    source_bytes = csv_path.read_bytes()
    df = _csv_to_df(source_bytes)
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    artifact = build_constraint_review_artifact(
        infer_schema(df), source_path=csv_path, source_sha256=source_sha256
    )
    want: set[tuple[str, str]] = set(spec["accept"])
    accepted_ids = [
        reviewed.candidate_id
        for reviewed in artifact.candidates
        if (reviewed.candidate.kind, reviewed.candidate.columns[0]) in want
    ]
    return VerifyScenarioView(
        name=name,
        proposer=str(spec["proposer"]),
        fixes=[ExternalFixInput(**fix) for fix in spec["fixes"]],
        accepted_constraint_ids=accepted_ids,
        note=str(spec["note"]),
    )


def _normalize_repair_mode(raw: str | None) -> str:
    """Validate the requested repair mode and enforce agent availability."""
    mode = (raw or "deterministic").strip().lower()
    if mode not in PLAYGROUND_REPAIR_MODES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_repair_mode",
                "allowed": sorted(PLAYGROUND_REPAIR_MODES),
            },
        )
    if mode == "agent" and not _agent_available():
        raise HTTPException(
            status_code=400,
            detail={
                "error": "agent_mode_unavailable",
                "message": (
                    "Agent mode requires a configured remote model "
                    "(set DATAFORGE_REMOTE_MODEL_URL)."
                ),
            },
        )
    return mode


def _build_analysis(
    *,
    upload_name: str,
    source_bytes: bytes,
    accepted_constraint_ids: list[str],
    advanced_requested: bool,
    repair_mode: str,
    allow_entity_consensus: bool = False,
) -> AnalyzeResponse:
    """Dispatch to the deterministic or verified-agent analysis path."""
    if repair_mode == "agent":
        return _agent_analyze_upload(
            upload_name=upload_name,
            source_bytes=source_bytes,
            accepted_constraint_ids=accepted_constraint_ids,
        )
    return _analyze_upload(
        upload_name=upload_name,
        source_bytes=source_bytes,
        accepted_constraint_ids=accepted_constraint_ids,
        allow_llm=advanced_requested,
        allow_entity_consensus=allow_entity_consensus,
    )


_ResultT = TypeVar("_ResultT")


async def _run_with_timeout(
    label: str,
    func: Callable[[], _ResultT],
    *,
    timeout_seconds: int | None = None,
) -> _ResultT:
    """Run a blocking pipeline step with a public timeout failure mode."""
    effective_timeout = REQUEST_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(func),
            timeout=float(effective_timeout),
        )
    except TimeoutError as exc:
        logger.warning("%s timed out after %s seconds", label, effective_timeout)
        raise _upload_problem(
            status_code=504,
            error="request_timeout",
            message="The playground backend timed out before completing the request.",
            timeout_seconds=effective_timeout,
        ) from exc


def _utc_now_text() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def _workflow_event(
    *,
    run_id: str,
    sequence: int,
    stage_id: WorkflowStageId,
    status: WorkflowStatus,
    summary: str,
    started_at: str | None = None,
    completed_at: str | None = None,
    counts: dict[str, int | float | str | bool] | None = None,
    confidence: float | None = None,
    uncertainty: str | None = None,
    requires_human: bool = False,
    analysis: AnalyzeResponse | None = None,
    problem: dict[str, Any] | None = None,
) -> WorkflowEvent:
    """Create one stable workflow event for NDJSON streaming."""
    event_started_at = started_at or _utc_now_text()
    return WorkflowEvent(
        run_id=run_id,
        sequence=sequence,
        stage_id=stage_id,
        status=status,
        summary=summary,
        started_at=event_started_at,
        completed_at=completed_at,
        counts=counts or {},
        confidence=confidence,
        uncertainty=uncertainty,
        requires_human=requires_human,
        analysis=analysis,
        problem=problem,
    )


def _event_line(event: WorkflowEvent) -> str:
    """Serialize a workflow event as one NDJSON line."""
    payload = event.model_dump(mode="json", exclude_none=True)
    if event.analysis is not None:
        payload["analysis"] = event.analysis.model_dump(mode="json")
    return f"{json.dumps(payload, separators=(',', ':'))}\n"


def _problem_payload_from_exception(exc: HTTPException) -> dict[str, Any]:
    """Convert a public HTTPException into a stream-safe problem payload."""
    detail: dict[str, Any] = exc.detail if isinstance(exc.detail, dict) else {}
    message = (
        str(detail.get("message") or detail.get("detail") or exc.detail)
        if detail or exc.detail
        else "The analysis pipeline could not complete safely."
    )
    error = str(detail.get("error") or f"http_{exc.status_code}")
    return {
        **detail,
        "type": str(detail.get("type") or f"https://dataforge.local/problems/{error}"),
        "title": str(detail.get("title") or error.replace("_", " ").title()),
        "status": exc.status_code,
        "detail": message,
        "error": error,
    }


def _completed_workflow_events(run_id: str, analysis: AnalyzeResponse) -> list[WorkflowEvent]:
    """Build completed supervision-stage events from the final proof-loop response."""
    now = _utc_now_text()
    pending_supported = analysis.risk_summary.pending_repair_supported_constraints
    proof_count = len(analysis.receipt.proof_obligations)
    safety_failed = analysis.receipt.safety_verdict not in {"allow", "accept", "accepted"}
    verifier_failed = analysis.receipt.verifier_verdict in {"reject", "unknown"}
    transaction_ready = bool(analysis.txn_journal.txn_id) and analysis.txn_journal.applied is False
    receipt_counts: dict[str, int | float | str | bool] = {
        "issues": analysis.receipt.issues_count,
        "fixes": analysis.receipt.fixes_count,
        "limitations": len(analysis.limitations),
    }
    if analysis.agent is not None:
        receipt_counts["agent_fixes"] = analysis.agent.agent_fix_count
        receipt_counts["floor_fixes"] = analysis.agent.floor_fix_count
        receipt_counts["agent_steps_used"] = analysis.agent.steps_used
        receipt_counts["agent_residual"] = analysis.agent.residual_count
    events: list[WorkflowEvent] = [
        _workflow_event(
            run_id=run_id,
            sequence=3,
            stage_id="schema_inference",
            status="completed",
            summary=(
                f"Inferred {len(analysis.schema_inference.candidates)} reviewable "
                "constraint candidate(s)."
            ),
            completed_at=now,
            counts={
                "candidates": len(analysis.schema_inference.candidates),
                "repair_supported_pending": pending_supported,
            },
            confidence=_average_candidate_confidence(analysis.schema_inference.candidates),
            uncertainty="Inference is advisory until accepted for the current run.",
            requires_human=pending_supported > 0,
        ),
        _workflow_event(
            run_id=run_id,
            sequence=4,
            stage_id="constraint_review",
            status="completed",
            summary=(
                f"{len(analysis.receipt.accepted_constraint_ids)} accepted constraint(s) "
                "were used for repair semantics."
            ),
            completed_at=now,
            counts={
                "accepted": len(analysis.receipt.accepted_constraint_ids),
                "pending_supported": pending_supported,
            },
            requires_human=pending_supported > 0,
        ),
        _workflow_event(
            run_id=run_id,
            sequence=5,
            stage_id="detectors",
            status="completed",
            summary=f"Detected {len(analysis.issues)} issue group(s) across the uploaded CSV.",
            completed_at=now,
            counts={
                "issues": len(analysis.issues),
                "safe": analysis.risk_summary.severity_counts.get("safe", 0),
                "review": analysis.risk_summary.severity_counts.get("review", 0),
                "unsafe": analysis.risk_summary.severity_counts.get("unsafe", 0),
            },
            uncertainty="Severity is categorical and evidence-derived, not an accuracy score.",
            requires_human=analysis.risk_summary.severity_counts.get("unsafe", 0) > 0,
        ),
        _workflow_event(
            run_id=run_id,
            sequence=6,
            stage_id="repair_candidates",
            status="completed",
            summary=(
                f"Produced {len(analysis.receipt.candidate_repairs)} candidate repair(s); "
                f"{len(analysis.repairs)} became verified fix(es)."
            ),
            completed_at=now,
            counts={
                "candidate_repairs": len(analysis.receipt.candidate_repairs),
                "verified_fixes": len(analysis.repairs),
                "failures": len(analysis.verification.failures),
            },
            confidence=_average_fix_confidence(analysis.repairs),
            requires_human=len(analysis.verification.failures) > 0,
        ),
        _workflow_event(
            run_id=run_id,
            sequence=7,
            stage_id="safety_gate",
            status="blocked" if safety_failed else "completed",
            summary=f"Safety gate returned {analysis.receipt.safety_verdict}.",
            completed_at=now,
            counts={"proof_obligations": proof_count},
            requires_human=safety_failed,
        ),
        _workflow_event(
            run_id=run_id,
            sequence=8,
            stage_id="smt_verifier",
            status="blocked" if verifier_failed else "completed",
            summary=f"SMT verifier returned {analysis.receipt.verifier_verdict}.",
            completed_at=now,
            counts={
                "proof_obligations": proof_count,
                "abstentions": len(analysis.verification.abstentions),
            },
            requires_human=verifier_failed or bool(analysis.verification.abstentions),
        ),
        _workflow_event(
            run_id=run_id,
            sequence=9,
            stage_id="dry_run_transaction",
            status="completed" if transaction_ready else "blocked",
            summary=(
                f"Created dry-run transaction {analysis.txn_journal.txn_id}; "
                "no uploaded data was mutated."
            ),
            completed_at=now,
            counts={
                "fixes": analysis.txn_journal.fixes_count,
                "applied": analysis.txn_journal.applied,
            },
        ),
        _workflow_event(
            run_id=run_id,
            sequence=10,
            stage_id="receipt",
            status="completed",
            summary=analysis.receipt.reason,
            completed_at=now,
            counts=receipt_counts,
            requires_human=(
                pending_supported > 0
                or len(analysis.verification.failures) > 0
                or verifier_failed
                or safety_failed
            ),
            analysis=analysis,
        ),
    ]
    return events


def _average_candidate_confidence(candidates: list[ConstraintCandidateView]) -> float | None:
    """Return a rounded average confidence for inferred constraints."""
    if not candidates:
        return None
    return round(sum(candidate.confidence for candidate in candidates) / len(candidates), 4)


def _average_fix_confidence(fixes: list[VerifiedFixView]) -> float | None:
    """Return a rounded average confidence for verified fixes."""
    if not fixes:
        return None
    return round(sum(fix.confidence for fix in fixes) / len(fixes), 4)


def _profile_upload(source_bytes: bytes, *, advanced_requested: bool) -> ProfileResponse:
    """Parse and profile a CSV upload in a worker thread."""
    df = _csv_to_df(source_bytes)
    issues = run_all_detectors(df, schema=None)
    return _profile_response(issues, df, advanced_requested=advanced_requested)


def _repair_response_from_analyze(analysis: AnalyzeResponse) -> RepairResponse:
    """Project the proof-loop response into the legacy repair response."""
    return RepairResponse(
        fixes=analysis.repairs,
        txn_journal=analysis.txn_journal,
        receipt=analysis.receipt,
        failures=analysis.verification.failures,
        meta=analysis.meta,
    )


def _profile_response_from_analyze(
    analysis: AnalyzeResponse, *, advanced_requested: bool
) -> ProfileResponse:
    """Project the proof-loop response into the legacy profile response."""
    return ProfileResponse(
        issues=analysis.issues,
        meta=ProfileMeta(
            rows=analysis.source.rows,
            columns=analysis.source.columns,
            column_names=analysis.source.column_names,
            total_issues=analysis.receipt.issues_count,
            advanced_requested=advanced_requested,
            api_version=analysis.meta.api_version,
            contract_version=analysis.meta.contract_version,
        ),
    )


def _limits_payload() -> LimitPayload:
    """Return processing limits exposed to the frontend and monitors."""
    return LimitPayload(
        max_upload_bytes=MAX_UPLOAD_BYTES,
        max_rows=MAX_UPLOAD_ROWS,
        max_columns=MAX_UPLOAD_COLUMNS,
        max_cells=MAX_UPLOAD_CELLS,
    )


def _environment_name() -> str:
    """Return a non-secret deployment environment label."""
    configured = os.environ.get("DATAFORGE_ENV") or os.environ.get("DATAFORGE_PLAYGROUND_ENV")
    if configured:
        return configured
    return "development" if os.environ.get("DATAFORGE_PLAYGROUND_DEV") == "1" else "production"


@app.get("/", response_model=RootResponse)
async def root() -> RootResponse:
    """Return service metadata for humans and uptime probes."""
    return RootResponse(
        service="DataForge Playground API",
        status="ok",
        api_version=app.version,
        contract_version=CONTRACT_VERSION,
        docs_url="/api/docs",
        frontend_hosting="cloudflare_static_assets",
    )


@app.get("/api/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Return backend readiness plus UI-facing capability metadata."""
    return HealthResponse(
        service="DataForge Playground API",
        status="ok",
        advanced_available=_advanced_available(),
        agent_available=_agent_available(),
        entity_consensus_available=True,
        agent_policy=_agent_policy_label(*_resolve_agent_policy()),
        agent_provider=_resolve_agent_policy()[1],
        verify_available=True,
        agent_max_steps=PLAYGROUND_AGENT_MAX_STEPS,
        max_upload_bytes=MAX_UPLOAD_BYTES,
        streaming_available=True,
        workflow_contract_version=WORKFLOW_CONTRACT_VERSION,
        api_version=app.version,
        contract_version=CONTRACT_VERSION,
        build_sha=os.environ.get("DATAFORGE_BUILD_SHA")
        or os.environ.get("GITHUB_SHA")
        or "unknown",
        server_time_utc=datetime.now(UTC).isoformat(),
        environment=_environment_name(),
        limits=_limits_payload(),
        cors_configured=bool(CORS_ORIGINS or CORS_ORIGIN_REGEX),
        otel_enabled=os.environ.get("DATAFORGE_OTEL_ENABLED", "").strip().lower()
        in OTEL_ENABLED_VALUES,
        otel_instrumented=OTEL_INSTRUMENTED,
        metrics=MetricsSnapshot.model_validate(request_metrics.snapshot()),
    )


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


@app.post("/api/analyze", response_model=AnalyzeResponse)
@limiter.limit("10/minute")
async def analyze(
    request: Request,
    file: UploadFile,
    accepted_constraint_ids: str | None = Form(default=None),
    repair_mode: str = Form(default="deterministic"),
    allow_entity_consensus: bool = Form(default=False),
) -> AnalyzeResponse:
    """Analyze an uploaded CSV through profile, constraint review, and dry-run repair."""
    advanced_requested = request.query_params.get("advanced", "false").lower() == "true"
    _require_advanced_mode(advanced_requested)
    mode = _normalize_repair_mode(repair_mode)

    source_bytes = await _read_upload(file)
    upload_name = Path(file.filename or "upload.csv").name
    logger.info(
        "Analyze request: filename=%s bytes=%d advanced=%s mode=%s",
        upload_name,
        len(source_bytes),
        advanced_requested,
        mode,
    )
    accepted_ids = _parse_accepted_constraint_ids(accepted_constraint_ids)
    timeout_seconds = AGENT_REQUEST_TIMEOUT_SECONDS if mode == "agent" else REQUEST_TIMEOUT_SECONDS

    try:
        return await _run_with_timeout(
            "analyze",
            lambda: _build_analysis(
                upload_name=upload_name,
                source_bytes=source_bytes,
                accepted_constraint_ids=accepted_ids,
                advanced_requested=advanced_requested,
                repair_mode=mode,
                allow_entity_consensus=allow_entity_consensus,
            ),
            timeout_seconds=timeout_seconds,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Analyze endpoint failed")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "analyze_failed",
                "message": "The analysis pipeline could not complete safely.",
            },
        ) from exc


@app.post("/api/analyze/stream")
@limiter.limit("10/minute")
async def analyze_stream(
    request: Request,
    file: UploadFile,
    accepted_constraint_ids: str | None = Form(default=None),
    repair_mode: str = Form(default="deterministic"),
    allow_entity_consensus: bool = Form(default=False),
) -> StreamingResponse:
    """Stream the analyze proof loop as NDJSON workflow events."""
    advanced_requested = request.query_params.get("advanced", "false").lower() == "true"
    mode = _normalize_repair_mode(repair_mode)
    source_bytes = await _read_upload(file)
    upload_name = Path(file.filename or "upload.csv").name
    accepted_ids = _parse_accepted_constraint_ids(accepted_constraint_ids)
    run_id = uuid.uuid4().hex
    timeout_seconds = AGENT_REQUEST_TIMEOUT_SECONDS if mode == "agent" else REQUEST_TIMEOUT_SECONDS

    async def event_stream() -> AsyncIterator[str]:
        started_at = _utc_now_text()
        yield _event_line(
            _workflow_event(
                run_id=run_id,
                sequence=0,
                stage_id="intake",
                status="running",
                summary="Reading CSV upload and establishing the dry-run boundary.",
                started_at=started_at,
                counts={
                    "bytes": len(source_bytes),
                    "advanced": advanced_requested,
                    "repair_mode": mode,
                },
            )
        )
        await asyncio.sleep(0)
        try:
            _require_advanced_mode(advanced_requested)
            yield _event_line(
                _workflow_event(
                    run_id=run_id,
                    sequence=1,
                    stage_id="intake",
                    status="completed",
                    summary=f"Accepted {upload_name} for stateless dry-run analysis.",
                    started_at=started_at,
                    completed_at=_utc_now_text(),
                    counts={"bytes": len(source_bytes), "repair_mode": mode},
                )
            )
            yield _event_line(
                _workflow_event(
                    run_id=run_id,
                    sequence=2,
                    stage_id="schema_inference",
                    status="running",
                    summary="Inferring schema assumptions before repair semantics are applied.",
                    started_at=_utc_now_text(),
                )
            )
            await asyncio.sleep(0)
            analysis = await _run_with_timeout(
                "analyze_stream",
                lambda: _build_analysis(
                    upload_name=upload_name,
                    source_bytes=source_bytes,
                    accepted_constraint_ids=accepted_ids,
                    advanced_requested=advanced_requested,
                    repair_mode=mode,
                    allow_entity_consensus=allow_entity_consensus,
                ),
                timeout_seconds=timeout_seconds,
            )
            for event in _completed_workflow_events(run_id, analysis):
                yield _event_line(event)
                await asyncio.sleep(0)
        except HTTPException as exc:
            yield _event_line(
                _workflow_event(
                    run_id=run_id,
                    sequence=99,
                    stage_id="receipt",
                    status="failed",
                    summary="The analysis workflow stopped before a verified receipt was produced.",
                    completed_at=_utc_now_text(),
                    problem=_problem_payload_from_exception(exc),
                    requires_human=True,
                )
            )
        except Exception:
            logger.exception("Analyze stream endpoint failed")
            yield _event_line(
                _workflow_event(
                    run_id=run_id,
                    sequence=99,
                    stage_id="receipt",
                    status="failed",
                    summary="The analysis workflow could not complete safely.",
                    completed_at=_utc_now_text(),
                    problem={
                        "type": "https://dataforge.local/problems/analyze_failed",
                        "title": "Analyze Failed",
                        "status": 500,
                        "detail": "The analysis pipeline could not complete safely.",
                        "error": "analyze_failed",
                    },
                    requires_human=True,
                )
            )

    return StreamingResponse(
        event_stream(),
        media_type="application/x-ndjson",
        headers={
            "X-DataForge-Workflow-Contract": WORKFLOW_CONTRACT_VERSION,
            "X-DataForge-Workflow-Run-Id": run_id,
        },
    )


@app.post("/api/verify-fixes", response_model=VerifyFixesResponse)
@limiter.limit("10/minute")
async def verify_fixes(
    request: Request,
    file: UploadFile,
    fixes: str = Form(...),
    accepted_constraint_ids: str | None = Form(default=None),
    proposer: str = Form(default="external-agent"),
    confirm_escalations: bool = Form(default=True),
    allow_unproven: bool = Form(default=False),
) -> VerifyFixesResponse:
    """Verify externally-proposed cell fixes through the shared prove gate (dry run).

    The guardrail-for-agents surface: an untrusted actor proposes edits and only
    the schema-proven ones are reported as would-apply; the rest are held or
    rejected with honest reasons, and the receipt re-verifies as a certificate.
    """
    source_bytes = await _read_upload(file)
    upload_name = Path(file.filename or "upload.csv").name
    external_fixes = _parse_external_fixes(fixes)
    accepted_ids = _parse_accepted_constraint_ids(accepted_constraint_ids)
    clean_proposer = (proposer or "external-agent").strip()[:120] or "external-agent"
    logger.info(
        "Verify-fixes request: filename=%s bytes=%d proposed=%d proposer=%s confirm=%s",
        upload_name,
        len(source_bytes),
        len(external_fixes),
        clean_proposer,
        confirm_escalations,
    )

    try:
        return await _run_with_timeout(
            "verify-fixes",
            lambda: _verify_fixes_upload(
                upload_name=upload_name,
                source_bytes=source_bytes,
                fixes=external_fixes,
                accepted_constraint_ids=accepted_ids,
                proposer=clean_proposer,
                confirm_escalations=confirm_escalations,
                allow_unproven=allow_unproven,
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Verify-fixes endpoint failed")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "verify_fixes_failed",
                "message": "The verification pipeline could not complete safely.",
            },
        ) from exc


@app.get("/api/verify-scenarios/{name}", response_model=VerifyScenarioView)
async def get_verify_scenario(name: str) -> VerifyScenarioView:
    """Return a curated 'untrusted agent' batch for a bundled sample.

    The batch pairs a realistic mix of correct / corrupting / stale / invalid
    proposals with the accepted constraint id(s) that authorize the correct ones,
    so a single click demonstrates the full proven / held / rejected split
    against an authoritative schema.
    """
    return _build_verify_scenario(name)


@app.post("/api/profile", response_model=ProfileResponse)
@limiter.limit("10/minute")
async def profile(request: Request, file: UploadFile) -> ProfileResponse:
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
        return await _run_with_timeout(
            "profile",
            lambda: _profile_upload(source_bytes, advanced_requested=advanced_requested),
        )
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


@app.post("/api/repair", response_model=RepairResponse)
@limiter.limit("10/minute")
async def repair(request: Request, file: UploadFile) -> RepairResponse:
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
        analysis = await _run_with_timeout(
            "repair",
            lambda: _analyze_upload(
                upload_name=upload_name,
                source_bytes=source_bytes,
                accepted_constraint_ids=[],
                allow_llm=advanced_requested,
            ),
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

    return _repair_response_from_analyze(analysis)
