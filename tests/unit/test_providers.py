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
    resolve_model,
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

    def test_bedrock_autodetected_from_bearer_token(self) -> None:
        """With only AWS_BEARER_TOKEN_BEDROCK set, autodetect bedrock."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ["AWS_BEARER_TOKEN_BEDROCK"] = "test-key"
            assert get_provider_name() == "bedrock"


class TestModelResolution:
    """resolve_model() and DATAFORGE_<PROVIDER>_MODEL env fallback (BYOM)."""

    def test_resolve_model_reads_env_per_provider(self) -> None:
        with patch.dict(os.environ, {"DATAFORGE_GROQ_MODEL": "groq-x"}, clear=True):
            assert resolve_model("groq") == "groq-x"
        with patch.dict(os.environ, {"DATAFORGE_GEMINI_MODEL": "gem-x"}, clear=True):
            assert resolve_model("gemini") == "gem-x"

    def test_resolve_model_falls_back_to_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert resolve_model("groq") == "llama-3.1-70b-versatile"
            assert resolve_model("gemini") == "gemini-2.0-flash"
            assert resolve_model("bedrock") == ""

    def test_groq_complete_honors_env_model(self) -> None:
        """complete() with no explicit model uses DATAFORGE_GROQ_MODEL."""
        response = _make_mock_response({"choices": [{"message": {"content": "ok"}}]})
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {
                    "DATAFORGE_LLM_PROVIDER": "groq",
                    "GROQ_API_KEY": "test-key",
                    "DATAFORGE_GROQ_MODEL": "my-custom-groq",
                },
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))

        assert mock_client.post.call_args.kwargs["json"]["model"] == "my-custom-groq"

    def test_gemini_complete_honors_env_model(self) -> None:
        """complete() with no explicit model uses DATAFORGE_GEMINI_MODEL in the URL."""
        response = _make_mock_response({"candidates": [{"content": {"parts": [{"text": "ok"}]}}]})
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {
                    "DATAFORGE_LLM_PROVIDER": "gemini",
                    "GEMINI_API_KEY": "test-key",
                    "DATAFORGE_GEMINI_MODEL": "gemini-3.1-flash-lite-preview",
                },
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}]))

        assert "gemini-3.1-flash-lite-preview" in mock_client.post.call_args[0][0]

    def test_explicit_model_overrides_env(self) -> None:
        """An explicitly passed model wins over the env var."""
        response = _make_mock_response({"choices": [{"message": {"content": "ok"}}]})
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {
                    "DATAFORGE_LLM_PROVIDER": "groq",
                    "GROQ_API_KEY": "test-key",
                    "DATAFORGE_GROQ_MODEL": "env-model",
                },
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            asyncio.run(complete([{"role": "user", "content": "hi"}], model="explicit-model"))

        assert mock_client.post.call_args.kwargs["json"]["model"] == "explicit-model"


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


class TestBedrockProvider:
    """Bedrock provider — mocked Converse HTTP calls."""

    def test_bedrock_calls_converse_endpoint_and_parses_output(self) -> None:
        """Bedrock posts a Converse payload and parses output.message.content."""
        response = _make_mock_response(
            {"output": {"message": {"content": [{"text": "bedrock says hi"}]}}}
        )

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.post.return_value = response

        with (
            patch.dict(
                os.environ,
                {
                    "DATAFORGE_LLM_PROVIDER": "bedrock",
                    "AWS_BEARER_TOKEN_BEDROCK": "test-key",
                    "DATAFORGE_BEDROCK_MODEL": "us.anthropic.claude-sonnet-5-test-v1:0",
                    "AWS_REGION": "us-east-1",
                },
            ),
            patch("dataforge.agent.providers.httpx.AsyncClient", return_value=mock_client),
        ):
            messages: list[Message] = [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "hi"},
            ]
            result = asyncio.run(complete(messages))

        assert result == "bedrock says hi"
        mock_client.post.assert_called_once()
        call_url = mock_client.post.call_args[0][0]
        assert call_url == (
            "https://bedrock-runtime.us-east-1.amazonaws.com/"
            "model/us.anthropic.claude-sonnet-5-test-v1:0/converse"
        )
        payload = mock_client.post.call_args.kwargs["json"]
        # System prompt is hoisted to a top-level field, not a message role.
        assert payload["system"] == [{"text": "You are helpful"}]
        assert payload["messages"] == [{"role": "user", "content": [{"text": "hi"}]}]
        assert "maxTokens" in payload["inferenceConfig"]
        headers = mock_client.post.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer test-key"

    def test_bedrock_missing_api_key(self) -> None:
        """Bedrock without AWS_BEARER_TOKEN_BEDROCK raises ProviderError."""
        with patch.dict(os.environ, {"DATAFORGE_LLM_PROVIDER": "bedrock"}, clear=True):
            os.environ.pop("AWS_BEARER_TOKEN_BEDROCK", None)
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            with pytest.raises(ProviderError, match="AWS_BEARER_TOKEN_BEDROCK"):
                asyncio.run(complete(messages))

    def test_bedrock_missing_model(self) -> None:
        """Bedrock without a configured model raises ProviderError."""
        with patch.dict(
            os.environ,
            {"DATAFORGE_LLM_PROVIDER": "bedrock", "AWS_BEARER_TOKEN_BEDROCK": "test-key"},
            clear=True,
        ):
            os.environ.pop("DATAFORGE_BEDROCK_MODEL", None)
            messages: list[Message] = [{"role": "user", "content": "hi"}]
            with pytest.raises(ProviderError, match="model"):
                asyncio.run(complete(messages))
