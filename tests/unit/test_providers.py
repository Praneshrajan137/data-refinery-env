"""Unit tests for dataforge.agent.providers — multi-provider LLM client stub.

Tests dispatch logic, error handling, and message validation.
No actual API calls are made.
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
)


class TestProviderDispatch:
    """Provider selection from DATAFORGE_LLM_PROVIDER env var."""

    def test_default_provider_is_groq(self) -> None:
        """Without env var, default to groq."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DATAFORGE_LLM_PROVIDER", None)
            assert get_provider_name() == "groq"

    def test_env_var_selects_provider(self) -> None:
        """DATAFORGE_LLM_PROVIDER selects the provider."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "gemini"}):
            assert get_provider_name() == "gemini"

    def test_env_var_case_insensitive(self) -> None:
        """Provider name is lowercased."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "GROQ"}):
            assert get_provider_name() == "groq"


class TestUnsupportedProviders:
    """Unimplemented providers raise NotImplementedError."""

    @pytest.mark.parametrize("provider", ["cerebras", "openrouter", "hf", "cloudflare"])
    def test_unimplemented_raises(self, provider: str) -> None:
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": provider}):
            messages: list[Message] = [{"role": "user", "content": "hello"}]
            with pytest.raises(NotImplementedError):
                asyncio.run(complete(messages))


class TestMessageValidation:
    """Message format validation."""

    def test_message_type_structure(self) -> None:
        """Messages must have role and content."""
        msg: Message = {"role": "user", "content": "test"}
        assert msg["role"] == "user"
        assert msg["content"] == "test"


def _make_mock_response(json_data: dict[str, object]) -> MagicMock:
    """Create a mock httpx.Response with the given JSON data.

    Args:
        json_data: The JSON payload the mock response should return.

    Returns:
        A MagicMock configured to mimic an httpx.Response.
    """
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


class TestGroqProvider:
    """Groq provider — mocked HTTP calls."""

    def test_groq_calls_correct_endpoint(self) -> None:
        """Groq provider calls api.groq.com."""
        response = _make_mock_response({"choices": [{"message": {"content": "hello back"}}]})

        # Mock the async context manager and post method.
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {"DATAFORGE_LLM_PROVIDER": "groq", "GROQ_API_KEY": "test-key"},
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            result = asyncio.run(complete(messages))
            assert result == "hello back"
            mock_client.post.assert_called_once()
            call_url = mock_client.post.call_args[0][0]
            assert "groq" in call_url


class TestGeminiProvider:
    """Gemini provider — mocked HTTP calls."""

    def test_gemini_calls_correct_endpoint(self) -> None:
        """Gemini provider calls generativelanguage.googleapis.com."""
        response = _make_mock_response(
            {"candidates": [{"content": {"parts": [{"text": "gemini says hi"}]}}]}
        )

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {"DATAFORGE_LLM_PROVIDER": "gemini", "GEMINI_API_KEY": "test-key"},
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            result = asyncio.run(complete(messages))
            assert result == "gemini says hi"
            mock_client.post.assert_called_once()
            call_url = mock_client.post.call_args[0][0]
            assert "googleapis" in call_url


class TestProviderErrors:
    """Error paths in provider dispatch."""

    def test_groq_missing_api_key(self) -> None:
        """Groq without GROQ_API_KEY raises ProviderError."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "groq"}, clear=True):
            os.environ.pop("GROQ_API_KEY", None)
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            with pytest.raises(ProviderError, match="GROQ_API_KEY"):
                asyncio.run(complete(messages))

    def test_gemini_missing_api_key(self) -> None:
        """Gemini without GEMINI_API_KEY raises ProviderError."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "gemini"}, clear=True):
            os.environ.pop("GEMINI_API_KEY", None)
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            with pytest.raises(ProviderError, match="GEMINI_API_KEY"):
                asyncio.run(complete(messages))

    def test_unknown_provider_raises(self) -> None:
        """Totally unknown provider raises NotImplementedError."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "nonexistent"}):
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            with pytest.raises(NotImplementedError, match="Unknown provider"):
                asyncio.run(complete(messages))

    def test_provider_error_has_provider_name(self) -> None:
        """ProviderError stores the provider name."""
        err = ProviderError("groq", "connection timeout")
        assert err.provider == "groq"
        assert "groq" in str(err)
        assert "connection timeout" in str(err)

    def test_gemini_system_message_conversion(self) -> None:
        """Gemini converts system messages to systemInstruction."""
        response = _make_mock_response({"candidates": [{"content": {"parts": [{"text": "ok"}]}}]})

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {"DATAFORGE_LLM_PROVIDER": "gemini", "GEMINI_API_KEY": "test-key"},
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            messages: list[Message] = [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "hi"},
            ]
            result = asyncio.run(complete(messages))
            assert result == "ok"
            # Verify the payload includes systemInstruction.
            call_kwargs = mock_client.post.call_args
            payload = (
                call_kwargs[1]["json"]
                if "json" in call_kwargs[1]
                else call_kwargs.kwargs.get("json")
            )
            assert "systemInstruction" in payload
