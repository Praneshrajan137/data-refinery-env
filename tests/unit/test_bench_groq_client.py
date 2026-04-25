"""Unit tests for the benchmark-local Groq client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataforge.bench.groq_client import GroqBenchClient


def _mock_response(payload: dict[str, object]) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status = MagicMock()
    return response


class TestGroqBenchClient:
    """Groq benchmark client behavior with mocked HTTP responses."""

    def test_complete_parses_content_and_usage(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response(
            {
                "choices": [{"message": {"content": '{"repairs": []}'}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 5},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            completion = GroqBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.text == '{"repairs": []}'
        assert completion.prompt_tokens == 12
        assert completion.completion_tokens == 5
        assert completion.warnings == ()

    def test_complete_warns_when_usage_is_missing(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response(
            {"choices": [{"message": {"content": '{"repairs": []}'}}]}
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            completion = GroqBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.prompt_tokens == 0
        assert completion.completion_tokens == 0
        assert completion.warnings == ("missing_usage_payload",)

    def test_complete_raises_on_unexpected_payload(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response({"choices": []})

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ValueError, match="Unexpected Groq response payload"),
        ):
            GroqBenchClient(api_key="test").complete([{"role": "user", "content": "hi"}])
