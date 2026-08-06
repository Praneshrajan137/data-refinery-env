"""The product path is metered and capped.

``dataforge repair --agent`` previously made unbounded billable calls:
``providers.complete`` returned a bare string, so there was no usage to account
for and no cap to enforce. The runbook admitted it ("No per-call USD guard --
the product ``providers.complete`` path is unguarded"). These tests are the lock
on that hole staying closed.

Every test is offline: ``httpx.AsyncClient`` is replaced with a fake.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from types import TracebackType
from typing import Any

import httpx
import pytest

from dataforge.agent import providers
from dataforge.spend import CostCapExceededError

_MESSAGES: list[providers.Message] = [{"role": "user", "content": "hi"}]


class _FakeResponse:
    """A minimal stand-in for an httpx response."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        """No-op: these fakes always represent a 200."""

    def json(self) -> dict[str, Any]:
        """Return the canned response body."""
        return self._payload


class _FakeAsyncClient:
    """Async-context-manager stand-in that returns a canned payload."""

    def __init__(self, payload: dict[str, Any], **_: object) -> None:
        self._payload = payload
        self.calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> _FakeAsyncClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        """Record the request and return the canned response."""
        self.calls.append({"url": url, **kwargs})
        return _FakeResponse(self._payload)


def _azure_payload(
    *, prompt: int = 1000, completion: int = 1000, reasoning: int = 0
) -> dict[str, Any]:
    """Build an Azure/OpenAI-shaped response body with usage."""
    return {
        "choices": [{"message": {"content": "answer"}}],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "completion_tokens_details": {"reasoning_tokens": reasoning},
        },
    }


@pytest.fixture(autouse=True)
def _azure_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Configure a metered Azure provider and reset the process-wide meter."""
    monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "azure")
    monkeypatch.setenv("AZURE_API_KEY", "test-key")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://example.openai.azure.com")
    monkeypatch.setenv("DATAFORGE_AZURE_MODEL", "test-deployment")
    monkeypatch.delenv("DATAFORGE_MAX_USD", raising=False)
    monkeypatch.delenv("DATAFORGE_AZURE_MAX_USD", raising=False)
    providers.reset_spend_meter()
    yield
    providers.reset_spend_meter()


def _install_client(
    monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any]
) -> list[_FakeAsyncClient]:
    """Patch httpx.AsyncClient to return ``payload``; return created clients."""
    created: list[_FakeAsyncClient] = []

    def factory(**kwargs: object) -> _FakeAsyncClient:
        client = _FakeAsyncClient(payload, **kwargs)
        created.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    return created


class TestUsageIsReturned:
    """The provider layer now surfaces what a call actually cost."""

    def test_complete_with_usage_reports_tokens(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_client(monkeypatch, _azure_payload(prompt=120, completion=45, reasoning=30))
        result = asyncio.run(providers.complete_with_usage(_MESSAGES))
        assert result.text == "answer"
        assert result.provider == "azure"
        assert result.model == "test-deployment"
        assert result.usage.prompt_tokens == 120
        assert result.usage.completion_tokens == 45
        # Reasoning tokens were previously invisible even though they are billed.
        assert result.usage.reasoning_tokens == 30
        assert result.usage.present is True

    def test_missing_usage_is_flagged_not_silently_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_client(monkeypatch, {"choices": [{"message": {"content": "answer"}}]})
        result = asyncio.run(providers.complete_with_usage(_MESSAGES))
        assert result.usage.present is False
        assert result.usage.prompt_tokens == 0

    def test_complete_still_returns_a_plain_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Back-compat: every existing caller of `complete` is unaffected.
        _install_client(monkeypatch, _azure_payload())
        assert asyncio.run(providers.complete(_MESSAGES)) == "answer"


class TestProductPathIsCapped:
    """The hole the runbook documented is closed."""

    def test_spend_accumulates_across_calls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_client(monkeypatch, _azure_payload(prompt=1000, completion=1000))
        asyncio.run(providers.complete(_MESSAGES))
        asyncio.run(providers.complete(_MESSAGES))
        meter = providers.spend_meter()
        assert meter is not None
        assert meter.calls == 2
        # 1000/1k*0.005 + 1000/1k*0.015 = $0.020 per call.
        assert meter.cumulative_usd == pytest.approx(0.040)

    def test_global_cap_hard_stops_the_agent_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATAFORGE_MAX_USD", "0.05")
        providers.reset_spend_meter()
        _install_client(monkeypatch, _azure_payload(prompt=1000, completion=1000))
        asyncio.run(providers.complete(_MESSAGES))  # $0.020
        asyncio.run(providers.complete(_MESSAGES))  # $0.040
        with pytest.raises(CostCapExceededError, match="azure spend guard tripped"):
            asyncio.run(providers.complete(_MESSAGES))  # $0.060

    def test_provider_specific_cap_overrides_global(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATAFORGE_MAX_USD", "0.001")
        monkeypatch.setenv("DATAFORGE_AZURE_MAX_USD", "10")
        providers.reset_spend_meter()
        _install_client(monkeypatch, _azure_payload(prompt=1000, completion=1000))
        asyncio.run(providers.complete(_MESSAGES))
        meter = providers.spend_meter()
        assert meter is not None
        assert meter.max_usd == 10.0

    def test_no_cap_configured_means_no_refusal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Metering must not become a behavior change for users who set no cap.
        _install_client(monkeypatch, _azure_payload(prompt=100_000, completion=100_000))
        for _ in range(3):
            asyncio.run(providers.complete(_MESSAGES))
        meter = providers.spend_meter()
        assert meter is not None
        assert meter.max_usd is None
        assert meter.calls == 3


class TestResponseFormatPlumbing:
    """Structured Outputs can reach the wire (used by the constrained corrector)."""

    def test_response_format_is_forwarded_to_azure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        created = _install_client(monkeypatch, _azure_payload())
        spec: dict[str, object] = {
            "type": "json_schema",
            "json_schema": {"name": "fix", "strict": True},
        }
        asyncio.run(providers.complete_with_usage(_MESSAGES, response_format=spec))
        assert created[0].calls[0]["json"]["response_format"] == spec

    def test_response_format_is_absent_when_not_requested(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        created = _install_client(monkeypatch, _azure_payload())
        asyncio.run(providers.complete_with_usage(_MESSAGES))
        assert "response_format" not in created[0].calls[0]["json"]


class TestRetryPolicy:
    """Retry the failures that are transient; never the ones that are not."""

    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    def test_transient_statuses_are_retryable(self, status: int) -> None:
        exc = httpx.HTTPStatusError(
            "boom",
            request=httpx.Request("POST", "https://example.com"),
            response=httpx.Response(status),
        )
        assert providers._is_retryable(exc) is True

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    def test_client_errors_are_not_retried(self, status: int) -> None:
        # The old policy retried every HTTPStatusError, tripling the latency of a
        # misconfiguration that could never succeed.
        exc = httpx.HTTPStatusError(
            "boom",
            request=httpx.Request("POST", "https://example.com"),
            response=httpx.Response(status),
        )
        assert providers._is_retryable(exc) is False

    def test_timeouts_are_retryable(self) -> None:
        # The old policy did NOT retry timeouts, which is the documented failure
        # mode that killed earlier paid runs against reasoning deployments.
        assert providers._is_retryable(httpx.TimeoutException("slow")) is True
