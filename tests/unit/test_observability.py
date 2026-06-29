"""Tests for optional DataForge observability hooks."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from dataforge import observability


def test_observability_is_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("DATAFORGE_OTEL_ENABLED", raising=False)
    app = SimpleNamespace(state=SimpleNamespace())

    assert observability.configure_fastapi_observability(app, service_name="test") is False
    with observability.repair_stage_span("stage", rows=[{"secret": "value"}]):
        pass


def test_observability_handles_missing_otel(monkeypatch) -> None:
    monkeypatch.setenv("DATAFORGE_OTEL_ENABLED", "1")

    def missing_import(_name: str) -> Any:
        raise ImportError("missing")

    monkeypatch.setattr(observability, "import_module", missing_import)
    app = SimpleNamespace(state=SimpleNamespace())

    assert observability.configure_fastapi_observability(app, service_name="test") is False
    with observability.repair_stage_span("stage", token="secret"):
        pass


def test_observability_instruments_and_redacts(monkeypatch) -> None:
    monkeypatch.setenv("DATAFORGE_OTEL_ENABLED", "1")
    calls: list[tuple[str, Any]] = []

    class FakeInstrumentor:
        @staticmethod
        def instrument_app(app: Any, **kwargs: Any) -> None:
            calls.append(("instrument_app", app))
            calls.append(("tracer_provider", kwargs["tracer_provider"]))

    class FakeSpan:
        def __enter__(self) -> FakeSpan:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def set_attribute(self, key: str, value: str | int | float | bool) -> None:
            calls.append((key, value))

    class FakeTracer:
        def start_as_current_span(self, stage: str) -> FakeSpan:
            calls.append(("span", stage))
            return FakeSpan()

    def fake_import(name: str) -> Any:
        if name == "opentelemetry.instrumentation.fastapi":
            return SimpleNamespace(FastAPIInstrumentor=FakeInstrumentor)
        if name == "opentelemetry.trace":
            return SimpleNamespace(
                get_tracer_provider=lambda: "provider",
                get_tracer=lambda _name: FakeTracer(),
            )
        raise ImportError(name)

    monkeypatch.setattr(observability, "import_module", fake_import)
    app = SimpleNamespace(state=SimpleNamespace())

    assert observability.configure_fastapi_observability(app, service_name="test") is True
    with observability.repair_stage_span(
        "detect",
        row_count=3,
        authorization="Bearer secret",
        row_values=["sensitive"],
        source_bytes=b"id,name\n1,Alice\n",
        api_key="secret",
        payload={"raw": "dataset"},
    ):
        pass

    assert app.state.dataforge_service_name == "test"
    assert ("instrument_app", app) in calls
    assert ("tracer_provider", "provider") in calls
    assert ("span", "dataforge.repair.detect") in calls
    assert ("dataforge.repair.stage", "detect") in calls
    assert ("row_count", 3) in calls
    assert not any(
        key in {"authorization", "row_values", "source_bytes", "api_key", "payload"}
        for key, _value in calls
    )


def test_repair_telemetry_vocabulary_is_frozen() -> None:
    """Repair lifecycle telemetry uses the documented public stage names."""
    assert {
        "detect",
        "propose",
        "safety_gate",
        "smt_verify",
        "transaction_create",
        "transaction_apply",
        "receipt",
        "revert",
    } == observability.REPAIR_TELEMETRY_STAGES
