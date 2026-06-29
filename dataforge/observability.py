"""Optional OpenTelemetry hooks for DataForge backend surfaces."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from importlib import import_module
from typing import Any, Literal

_SENSITIVE_ATTR_FRAGMENTS = ("authorization", "cookie", "token", "key", "secret", "password")
RepairTelemetryStage = Literal[
    "detect",
    "propose",
    "safety_gate",
    "smt_verify",
    "transaction_create",
    "transaction_apply",
    "receipt",
    "revert",
]
REPAIR_TELEMETRY_SPANS: dict[str, str] = {
    "detect": "dataforge.repair.detect",
    "propose": "dataforge.repair.propose",
    "safety_gate": "dataforge.repair.safety_gate",
    "smt_verify": "dataforge.repair.smt_verify",
    "transaction_create": "dataforge.repair.transaction_create",
    "transaction_apply": "dataforge.repair.transaction_apply",
    "receipt": "dataforge.repair.receipt",
    "revert": "dataforge.repair.revert",
}
REPAIR_TELEMETRY_STAGES = frozenset(REPAIR_TELEMETRY_SPANS)


def _otel_enabled() -> bool:
    """Return whether optional OpenTelemetry instrumentation is enabled."""
    return os.environ.get("DATAFORGE_OTEL_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _safe_attrs(attributes: dict[str, Any]) -> dict[str, str | int | float | bool]:
    """Keep only scalar, non-sensitive telemetry attributes."""
    safe: dict[str, str | int | float | bool] = {}
    for key, value in attributes.items():
        lowered = key.lower()
        if any(fragment in lowered for fragment in _SENSITIVE_ATTR_FRAGMENTS):
            continue
        if lowered in {"row_values", "rows", "payload", "source_bytes", "csv"}:
            continue
        if isinstance(value, str | int | float | bool):
            safe[key] = value
    return safe


def configure_fastapi_observability(app: Any, *, service_name: str) -> bool:
    """Instrument a FastAPI app when OpenTelemetry is explicitly enabled."""
    if not _otel_enabled():
        return False
    try:
        fastapi_instrumentation = import_module("opentelemetry.instrumentation.fastapi")
        trace_module = import_module("opentelemetry.trace")
    except ImportError:
        return False

    app.state.dataforge_service_name = service_name
    fastapi_instrumentation.FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=trace_module.get_tracer_provider(),
        excluded_urls="/api/docs,/docs,/redoc,/openapi.json",
    )
    return True


def _span_name(stage: str) -> str:
    """Return the span name for a public repair telemetry stage."""
    if stage in REPAIR_TELEMETRY_SPANS:
        return REPAIR_TELEMETRY_SPANS[stage]
    return stage


@contextmanager
def repair_stage_span(stage: RepairTelemetryStage | str, **attributes: Any) -> Iterator[None]:
    """Create a repair-stage span when OpenTelemetry is available."""
    if not _otel_enabled():
        with nullcontext():
            yield
        return

    try:
        trace_module = import_module("opentelemetry.trace")
    except ImportError:
        with nullcontext():
            yield
        return

    tracer = trace_module.get_tracer("dataforge.repair")
    with tracer.start_as_current_span(_span_name(stage)) as span:
        if stage in REPAIR_TELEMETRY_SPANS:
            span.set_attribute("dataforge.repair.stage", stage)
        for key, value in _safe_attrs(attributes).items():
            span.set_attribute(key, value)
        yield
