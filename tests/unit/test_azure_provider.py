"""Unit tests for the Azure OpenAI provider in dataforge.agent.providers.

Azure OpenAI (first-party, "sold directly by Azure") is billed against the
subscription, so it works on free-trial credit. It uses the OpenAI-compatible
chat/completions surface addressed by *deployment name*, authenticated with an
``api-key`` header and an ``api-version`` query parameter.

GPT-5 / reasoning deployments reject ``temperature != 1`` and require
``max_completion_tokens`` (not ``max_tokens``), so this provider omits
temperature by default and always sends ``max_completion_tokens``.

No real API calls are made — httpx is mocked.
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataforge.agent.providers import (
    Message,
    ProviderError,
    complete,
    get_provider_name,
    resolve_model,
)

_ENDPOINT = "https://my-res.openai.azure.com"
_DEPLOYMENT = "gpt-5.5"


def _make_mock_response(json_data: dict[str, object]) -> MagicMock:
    """Create a mock httpx.Response returning the given JSON body."""
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


def _mock_async_client(response: MagicMock) -> AsyncMock:
    """Build a mocked httpx.AsyncClient whose ``post`` returns ``response``."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.post.return_value = response
    return mock_client


class TestAzureAutodetectAndResolution:
    """Provider autodetection and model resolution for Azure."""

    def test_azure_autodetected_from_key(self) -> None:
        """With only AZURE_API_KEY set, autodetect the azure provider."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ["AZURE_API_KEY"] = "test-key"
            assert get_provider_name() == "azure"

    def test_explicit_provider_wins_over_autodetect(self) -> None:
        """DATAFORGE_LLM_PROVIDER=azure selects azure explicitly."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "azure"}):
            assert get_provider_name() == "azure"

    def test_resolve_model_reads_env(self) -> None:
        """resolve_model('azure') reads DATAFORGE_AZURE_MODEL (deployment name)."""
        with patch.dict(os.environ, {"DATAFORGE_AZURE_MODEL": "gpt-5.5"}, clear=True):
            assert resolve_model("azure") == "gpt-5.5"

    def test_resolve_model_has_no_builtin_default(self) -> None:
        """Azure has no built-in default: the deployment name is user-specific."""
        with patch.dict(os.environ, {}, clear=True):
            assert resolve_model("azure") == ""


class TestAzureProvider:
    """The _complete_azure path via the public complete() dispatch."""

    def _env(self, **extra: str) -> dict[str, str]:
        base = {
            "DATAFORGE_LLM_PROVIDER": "azure",
            "AZURE_API_KEY": "test-key",
            "AZURE_OPENAI_ENDPOINT": _ENDPOINT,
            "DATAFORGE_AZURE_MODEL": _DEPLOYMENT,
            "AZURE_OPENAI_API_VERSION": "2025-04-01-preview",
        }
        base.update(extra)
        return base

    def test_azure_builds_deployment_url_and_parses_content(self) -> None:
        """Azure posts to the deployment chat/completions URL and parses content."""
        response = _make_mock_response({"choices": [{"message": {"content": "azure says hi"}}]})
        mock_client = _mock_async_client(response)
        with (
            patch.dict(os.environ, self._env(), clear=True),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            messages: list[Message] = [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "hi"},
            ]
            result = asyncio.run(complete(messages))

        assert result == "azure says hi"
        call_url = mock_client.post.call_args[0][0]
        assert call_url == (f"{_ENDPOINT}/openai/deployments/{_DEPLOYMENT}/chat/completions")
        params = mock_client.post.call_args.kwargs["params"]
        assert params["api-version"] == "2025-04-01-preview"
        headers = mock_client.post.call_args.kwargs["headers"]
        assert headers["api-key"] == "test-key"
        # System stays inline as a message role (OpenAI-compatible surface).
        payload = mock_client.post.call_args.kwargs["json"]
        assert payload["messages"][0] == {"role": "system", "content": "You are helpful"}

    def test_azure_uses_max_completion_tokens_and_omits_temperature(self) -> None:
        """GPT-5 rejects temperature!=1; default payload omits it and uses the
        modern max_completion_tokens field."""
        response = _make_mock_response({"choices": [{"message": {"content": "ok"}}]})
        mock_client = _mock_async_client(response)
        with (
            patch.dict(os.environ, self._env(), clear=True),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}], temperature=0.0))

        payload = mock_client.post.call_args.kwargs["json"]
        assert "max_completion_tokens" in payload
        assert "max_tokens" not in payload
        assert "temperature" not in payload

    def test_azure_sends_temperature_when_opted_in(self) -> None:
        """DATAFORGE_AZURE_SEND_TEMPERATURE=1 opts a non-reasoning deployment
        back into sending temperature."""
        response = _make_mock_response({"choices": [{"message": {"content": "ok"}}]})
        mock_client = _mock_async_client(response)
        with (
            patch.dict(
                os.environ,
                self._env(DATAFORGE_AZURE_SEND_TEMPERATURE="1"),
                clear=True,
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}], temperature=0.4))

        payload = mock_client.post.call_args.kwargs["json"]
        assert payload["temperature"] == 0.4

    def test_azure_missing_api_key(self) -> None:
        """Azure without AZURE_API_KEY raises ProviderError."""
        with (
            patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "azure"}, clear=True),
            pytest.raises(ProviderError, match="AZURE_API_KEY"),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))

    def test_azure_missing_endpoint(self) -> None:
        """Azure without AZURE_OPENAI_ENDPOINT raises a clear ProviderError."""
        env = {
            "DATAFORGE_LLM_PROVIDER": "azure",
            "AZURE_API_KEY": "test-key",
            "DATAFORGE_AZURE_MODEL": _DEPLOYMENT,
        }
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ProviderError, match="AZURE_OPENAI_ENDPOINT"),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))

    def test_azure_missing_deployment(self) -> None:
        """Azure without a deployment name raises a clear ProviderError."""
        env = {
            "DATAFORGE_LLM_PROVIDER": "azure",
            "AZURE_API_KEY": "test-key",
            "AZURE_OPENAI_ENDPOINT": _ENDPOINT,
        }
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ProviderError, match="deployment"),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))

    def test_azure_strips_trailing_slash_on_endpoint(self) -> None:
        """A trailing slash on the endpoint does not produce a doubled slash."""
        response = _make_mock_response({"choices": [{"message": {"content": "ok"}}]})
        mock_client = _mock_async_client(response)
        with (
            patch.dict(
                os.environ,
                self._env(AZURE_OPENAI_ENDPOINT=f"{_ENDPOINT}/"),
                clear=True,
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))
        call_url = mock_client.post.call_args[0][0]
        assert "//openai" not in call_url.replace("https://", "")


class TestAzureMarketplaceHonestyGuard:
    """BDD honesty scenario: Claude/Anthropic are Marketplace SaaS on Foundry
    and are blocked on free-trial credit. Requesting one via the first-party
    Azure OpenAI path must fail fast with an actionable message."""

    @pytest.mark.parametrize(
        "deployment",
        ["claude-sonnet-5", "claude-opus-4-8", "my-sonnet-deploy"],
    )
    def test_anthropic_model_on_azure_openai_path_fails_fast(self, deployment: str) -> None:
        env = {
            "DATAFORGE_LLM_PROVIDER": "azure",
            "AZURE_API_KEY": "test-key",
            "AZURE_OPENAI_ENDPOINT": _ENDPOINT,
            "DATAFORGE_AZURE_MODEL": deployment,
        }
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ProviderError, match="pay-as-you-go"),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))
