"""Unit tests for the benchmark-local Groq client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from dataforge.bench.groq_client import (
    AzureBenchClient,
    BedrockBenchClient,
    CerebrasBenchClient,
    CostCapExceededError,
    GeminiBenchClient,
    GrokBenchClient,
    GroqBenchClient,
    ProviderRateLimitError,
    ProviderRequestError,
    _is_rate_limit_error,
)


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
        assert mock_client.post.call_args.kwargs["json"]["max_tokens"] == 512
        assert mock_client.post.call_args.args[0] == (
            "https://api.groq.com/openai/v1/chat/completions"
        )

    def test_cerebras_client_uses_cerebras_endpoint_and_model(self) -> None:
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
            completion = CerebrasBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.text == '{"repairs": []}'
        assert mock_client.post.call_args.args[0] == ("https://api.cerebras.ai/v1/chat/completions")
        assert mock_client.post.call_args.kwargs["json"]["model"] == (
            "qwen-3-235b-a22b-instruct-2507"
        )

    def test_grok_client_uses_x_ai_endpoint_and_default_model(self) -> None:
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
            completion = GrokBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.text == '{"repairs": []}'
        assert mock_client.post.call_args.args[0] == "https://api.x.ai/v1/chat/completions"
        assert mock_client.post.call_args.kwargs["json"]["model"] == "grok-4.5"

    def test_grok_cost_guard_trips_when_over_budget(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response(
            {
                "choices": [{"message": {"content": '{"repairs": []}'}}],
                "usage": {"prompt_tokens": 1_000_000, "completion_tokens": 1_000_000},
            }
        )

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(CostCapExceededError, match="grok spend guard"),
        ):
            GrokBenchClient(api_key="test", max_usd=1.0).complete(
                [{"role": "user", "content": "hi"}]
            )

    def test_groq_client_has_no_cost_guard_by_default(self) -> None:
        # Groq/Cerebras keep the guard OFF (behavior unchanged): a huge usage
        # payload does not raise and cumulative_usd stays 0.
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response(
            {
                "choices": [{"message": {"content": '{"repairs": []}'}}],
                "usage": {"prompt_tokens": 1_000_000, "completion_tokens": 1_000_000},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            client = GroqBenchClient(api_key="test")
            client.complete([{"role": "user", "content": "hi"}])

        assert client.cumulative_usd == 0.0

    def test_gemini_client_uses_generate_content_payload(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response(
            {
                "candidates": [{"content": {"parts": [{"text": '{"repairs": []}'}]}}],
                "usageMetadata": {"promptTokenCount": 12, "candidatesTokenCount": 5},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            completion = GeminiBenchClient(api_key="test").complete(
                [
                    {"role": "system", "content": "sys"},
                    {"role": "user", "content": "hi"},
                    {"role": "assistant", "content": "{}"},
                ]
            )

        assert completion.text == '{"repairs": []}'
        assert completion.prompt_tokens == 12
        assert completion.completion_tokens == 5
        assert mock_client.post.call_args.args[0] == (
            "https://generativelanguage.googleapis.com/v1beta/"
            "models/gemini-3.1-pro-preview:generateContent"
        )
        request_json = mock_client.post.call_args.kwargs["json"]
        assert request_json["systemInstruction"]["parts"][0]["text"] == "sys"
        assert request_json["contents"][1]["role"] == "model"

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

    def test_complete_honors_retry_after_on_rate_limit(self) -> None:
        request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        rate_limited_response = httpx.Response(
            429,
            headers={"retry-after": "7"},
            request=request,
        )
        rate_limited_error = httpx.HTTPStatusError(
            "rate limited",
            request=request,
            response=rate_limited_response,
        )
        mock_response = _mock_response(
            {
                "choices": [{"message": {"content": '{"repairs": []}'}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 5},
            }
        )
        mock_response.raise_for_status.side_effect = [rate_limited_error, None]
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = mock_response

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            patch("dataforge.bench.groq_client.time.sleep") as sleep,
        ):
            completion = GroqBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.text == '{"repairs": []}'
        sleep.assert_called_once_with(7.0)

    def test_complete_raises_when_retry_after_exceeds_cap(self) -> None:
        request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        rate_limited_response = httpx.Response(
            429,
            headers={"retry-after": "3356"},
            text='{"error":{"message":"wait"}}',
            request=request,
        )
        rate_limited_error = httpx.HTTPStatusError(
            "rate limited",
            request=request,
            response=rate_limited_response,
        )
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = rate_limited_error
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = mock_response

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            patch("dataforge.bench.groq_client.time.sleep") as sleep,
            pytest.raises(ProviderRateLimitError, match="exceeds cap"),
        ):
            GroqBenchClient(api_key="test", max_retry_after_s=120).complete(
                [{"role": "user", "content": "hi"}]
            )

        sleep.assert_not_called()

    def test_complete_raises_on_unexpected_payload(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = _mock_response({"choices": []})

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ValueError, match="Unexpected groq response payload"),
        ):
            GroqBenchClient(api_key="test").complete([{"role": "user", "content": "hi"}])

    def test_complete_raises_clear_timeout_error(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.side_effect = httpx.TimeoutException("slow")

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(TimeoutError, match="timed out after 3.0 seconds"),
        ):
            GroqBenchClient(api_key="test", timeout_s=3).complete(
                [{"role": "user", "content": "hi"}]
            )

    def test_complete_raises_provider_request_error_with_response_body(self) -> None:
        request = httpx.Request("POST", "https://api.cerebras.ai/v1/chat/completions")
        response = httpx.Response(
            400,
            text='{"message":"context length exceeded"}',
            request=request,
        )
        bad_request = httpx.HTTPStatusError("bad request", request=request, response=response)
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = bad_request
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.return_value = mock_response

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ProviderRequestError, match="context length exceeded"),
        ):
            CerebrasBenchClient(api_key="test", model="llama3.1-8b").complete(
                [{"role": "user", "content": "large prompt"}]
            )

    def test_rate_limit_helper_and_spacing_sleep(self) -> None:
        request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        rate_limited = httpx.HTTPStatusError(
            "rate limited",
            request=request,
            response=httpx.Response(429, request=request),
        )
        unavailable = httpx.HTTPStatusError(
            "unavailable",
            request=request,
            response=httpx.Response(503, request=request),
        )

        with patch("dataforge.bench.groq_client.httpx.Client"):
            client = GroqBenchClient(api_key="test", min_interval_s=5)
        client._last_success_at = 10.0

        with (
            patch("dataforge.bench.groq_client.time.monotonic", return_value=12.0),
            patch("dataforge.bench.groq_client.time.sleep") as sleep,
        ):
            client._respect_spacing()

        assert _is_rate_limit_error(rate_limited) is True
        assert _is_rate_limit_error(unavailable) is False
        sleep.assert_called_once_with(3.0)

    def test_retryable_503_final_attempt_raises_provider_error(self) -> None:
        request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        response = httpx.Response(503, text="temporarily down", request=request)
        unavailable = httpx.HTTPStatusError("unavailable", request=request, response=response)
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = unavailable
        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            patch("dataforge.bench.groq_client.time.sleep") as sleep,
            pytest.raises(ProviderRequestError, match="temporarily down"),
        ):
            GroqBenchClient(api_key="test", max_retries=1).complete(
                [{"role": "user", "content": "hi"}]
            )

        sleep.assert_not_called()

    def test_invalid_retry_after_uses_fallback_delay(self) -> None:
        request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        rate_limited_response = httpx.Response(
            429,
            headers={"retry-after": "not-a-number"},
            request=request,
        )
        rate_limited_error = httpx.HTTPStatusError(
            "rate limited",
            request=request,
            response=rate_limited_response,
        )
        success = _mock_response(
            {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }
        )
        rate_limit_then_success = MagicMock()
        rate_limit_then_success.raise_for_status.side_effect = [rate_limited_error, None]
        mock_client = MagicMock()
        mock_client.post.side_effect = [rate_limit_then_success, success]

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            patch("dataforge.bench.groq_client.time.sleep") as sleep,
        ):
            completion = GroqBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.text == "ok"
        sleep.assert_called_once_with(2.0)

    def test_gemini_temperature_is_configurable(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "candidates": [{"content": {"parts": [{"text": "ok"}]}}],
                "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            GeminiBenchClient(api_key="test", temperature=0.4).complete(
                [{"role": "user", "content": "hi"}]
            )

        request_json = mock_client.post.call_args.kwargs["json"]
        assert request_json["generationConfig"]["temperature"] == 0.4

    def test_gemini_missing_usage_and_unexpected_payload(self) -> None:
        missing_usage_client = MagicMock()
        missing_usage_client.post.return_value = _mock_response(
            {"candidates": [{"content": {"parts": [{"text": "ok"}, {"text": "!"}]}}]}
        )
        bad_payload_client = MagicMock()
        bad_payload_client.post.return_value = _mock_response({"candidates": []})

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=missing_usage_client):
            completion = GeminiBenchClient(api_key="test").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.text == "ok!"
        assert completion.warnings == ("missing_usage_payload",)

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=bad_payload_client),
            pytest.raises(ValueError, match="Unexpected gemini response payload"),
        ):
            GeminiBenchClient(api_key="test").complete([{"role": "user", "content": "hi"}])


class TestBedrockBenchClient:
    """Bedrock benchmark client behavior with mocked Converse responses."""

    def test_bedrock_client_uses_converse_payload_and_usage(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "output": {"message": {"content": [{"text": '{"repairs": []}'}]}},
                "usage": {"inputTokens": 12, "outputTokens": 5, "totalTokens": 17},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            completion = BedrockBenchClient(
                api_key="test", model="us.anthropic.claude-sonnet-5-test-v1:0"
            ).complete(
                [
                    {"role": "system", "content": "sys"},
                    {"role": "user", "content": "hi"},
                    {"role": "assistant", "content": "{}"},
                ]
            )

        assert completion.text == '{"repairs": []}'
        assert completion.prompt_tokens == 12
        assert completion.completion_tokens == 5
        assert completion.warnings == ()
        assert mock_client.post.call_args.args[0] == (
            "https://bedrock-runtime.us-east-1.amazonaws.com/"
            "model/us.anthropic.claude-sonnet-5-test-v1:0/converse"
        )
        request_json = mock_client.post.call_args.kwargs["json"]
        assert request_json["system"] == [{"text": "sys"}]
        assert request_json["messages"][0] == {"role": "user", "content": [{"text": "hi"}]}
        assert request_json["messages"][1] == {"role": "assistant", "content": [{"text": "{}"}]}
        assert request_json["inferenceConfig"]["maxTokens"] == 512

    def test_bedrock_client_respects_region(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "output": {"message": {"content": [{"text": "ok"}]}},
                "usage": {"inputTokens": 1, "outputTokens": 1},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            BedrockBenchClient(api_key="test", model="m", region="us-west-2").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert mock_client.post.call_args.args[0] == (
            "https://bedrock-runtime.us-west-2.amazonaws.com/model/m/converse"
        )

    def test_bedrock_warns_when_usage_missing(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {"output": {"message": {"content": [{"text": "ok"}]}}}
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            completion = BedrockBenchClient(api_key="test", model="m").complete(
                [{"role": "user", "content": "hi"}]
            )

        assert completion.prompt_tokens == 0
        assert completion.completion_tokens == 0
        assert completion.warnings == ("missing_usage_payload",)

    def test_bedrock_raises_on_unexpected_payload(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response({"output": {}})

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ValueError, match="Unexpected bedrock response payload"),
        ):
            BedrockBenchClient(api_key="test", model="m").complete(
                [{"role": "user", "content": "hi"}]
            )

    def test_bedrock_raises_provider_request_error_with_body(self) -> None:
        request = httpx.Request(
            "POST", "https://bedrock-runtime.us-east-1.amazonaws.com/model/m/converse"
        )
        response = httpx.Response(400, text='{"message":"model not found"}', request=request)
        bad_request = httpx.HTTPStatusError("bad request", request=request, response=response)
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = bad_request
        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ProviderRequestError, match="model not found"),
        ):
            BedrockBenchClient(api_key="test", model="m").complete(
                [{"role": "user", "content": "hi"}]
            )

    def test_bedrock_cost_guard_hard_stops_when_cap_exceeded(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "output": {"message": {"content": [{"text": "ok"}]}},
                "usage": {"inputTokens": 1000, "outputTokens": 1000},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            # Each call costs 1000/1000*0.003 + 1000/1000*0.015 = $0.018.
            client = BedrockBenchClient(
                api_key="test",
                model="m",
                max_usd=0.05,
                usd_per_1k_input=0.003,
                usd_per_1k_output=0.015,
            )
            # First two calls stay under the $0.05 cap ($0.018, $0.036).
            client.complete([{"role": "user", "content": "hi"}])
            client.complete([{"role": "user", "content": "hi"}])
            # Third call crosses $0.05 ($0.054) and must hard-stop.
            with pytest.raises(CostCapExceededError, match="spend guard tripped"):
                client.complete([{"role": "user", "content": "hi"}])

        assert client.cumulative_usd == pytest.approx(0.054)

    def test_bedrock_temperature_is_configurable(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "output": {"message": {"content": [{"text": "ok"}]}},
                "usage": {"inputTokens": 1, "outputTokens": 1},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            BedrockBenchClient(api_key="test", model="m", temperature=0.4).complete(
                [{"role": "user", "content": "hi"}]
            )

        request_json = mock_client.post.call_args.kwargs["json"]
        assert request_json["inferenceConfig"]["temperature"] == 0.4

    def test_bedrock_temperature_defaults_to_zero(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "output": {"message": {"content": [{"text": "ok"}]}},
                "usage": {"inputTokens": 1, "outputTokens": 1},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            BedrockBenchClient(api_key="test", model="m").complete(
                [{"role": "user", "content": "hi"}]
            )

        request_json = mock_client.post.call_args.kwargs["json"]
        assert request_json["inferenceConfig"]["temperature"] == 0.0

    def test_bedrock_no_cost_guard_by_default(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {
                "output": {"message": {"content": [{"text": "ok"}]}},
                "usage": {"inputTokens": 100000, "outputTokens": 100000},
            }
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            client = BedrockBenchClient(api_key="test", model="m")
            # No cap configured: even a huge call does not raise.
            completion = client.complete([{"role": "user", "content": "hi"}])

        assert completion.text == "ok"
        assert client.cumulative_usd > 0.0


class TestAzureBenchClient:
    """Azure OpenAI benchmark client behavior with mocked chat/completions."""

    def _resp(self, text: str, *, prompt: int = 10, completion: int = 4) -> MagicMock:
        return _mock_response(
            {
                "choices": [{"message": {"content": text}}],
                "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
            }
        )

    def test_azure_builds_deployment_url_with_api_version_and_key_header(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = self._resp('{"repairs": []}')

        with patch(
            "dataforge.bench.groq_client.httpx.Client", return_value=mock_client
        ) as mock_client_cls:
            client = AzureBenchClient(
                api_key="secret",
                model="gpt-5.5",
                endpoint="https://my-res.openai.azure.com/",
                api_version="2025-04-01-preview",
            )
            completion = client.complete(
                [
                    {"role": "system", "content": "sys"},
                    {"role": "user", "content": "hi"},
                ]
            )

        assert completion.text == '{"repairs": []}'
        assert completion.prompt_tokens == 10
        assert completion.completion_tokens == 4
        # Deployment-name URL, trailing slash on endpoint normalized.
        assert mock_client.post.call_args.args[0] == (
            "https://my-res.openai.azure.com/openai/deployments/gpt-5.5/chat/completions"
        )
        assert mock_client.post.call_args.kwargs["params"]["api-version"] == "2025-04-01-preview"
        # api-key header auth (constructed at __init__).
        assert mock_client_cls.call_args.kwargs["headers"]["api-key"] == "secret"
        payload = mock_client.post.call_args.kwargs["json"]
        # System stays inline (OpenAI-compatible), modern token field, no temperature.
        assert payload["messages"][0] == {"role": "system", "content": "sys"}
        assert "max_completion_tokens" in payload
        assert "max_tokens" not in payload
        assert "temperature" not in payload

    def test_azure_sends_temperature_only_when_opted_in(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = self._resp("ok")

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            AzureBenchClient(
                api_key="k",
                model="m",
                endpoint="https://e.openai.azure.com",
                send_temperature=True,
                temperature=0.4,
            ).complete([{"role": "user", "content": "hi"}])

        assert mock_client.post.call_args.kwargs["json"]["temperature"] == 0.4

    def test_azure_sends_reasoning_effort_when_configured(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = self._resp("ok")

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            AzureBenchClient(
                api_key="k",
                model="gpt-5-mini",
                endpoint="https://e.openai.azure.com",
                reasoning_effort="low",
            ).complete([{"role": "user", "content": "hi"}])

        assert mock_client.post.call_args.kwargs["json"]["reasoning_effort"] == "low"

    def test_azure_warns_when_usage_missing(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response(
            {"choices": [{"message": {"content": "ok"}}]}
        )

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            completion = AzureBenchClient(
                api_key="k", model="m", endpoint="https://e.openai.azure.com"
            ).complete([{"role": "user", "content": "hi"}])

        assert completion.warnings == ("missing_usage_payload",)

    def test_azure_raises_on_unexpected_payload(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = _mock_response({"choices": []})

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ValueError, match="Unexpected azure response payload"),
        ):
            AzureBenchClient(
                api_key="k", model="m", endpoint="https://e.openai.azure.com"
            ).complete([{"role": "user", "content": "hi"}])

    def test_azure_raises_provider_request_error_with_body(self) -> None:
        request = httpx.Request("POST", "https://e.openai.azure.com/x")
        response = httpx.Response(400, text='{"error":"bad deployment"}', request=request)
        bad_request = httpx.HTTPStatusError("bad", request=request, response=response)
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = bad_request
        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        with (
            patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client),
            pytest.raises(ProviderRequestError, match="bad deployment"),
        ):
            AzureBenchClient(
                api_key="k", model="m", endpoint="https://e.openai.azure.com"
            ).complete([{"role": "user", "content": "hi"}])

    def test_azure_cost_guard_hard_stops_when_cap_exceeded(self) -> None:
        mock_client = MagicMock()
        mock_client.post.return_value = self._resp("ok", prompt=1000, completion=1000)

        with patch("dataforge.bench.groq_client.httpx.Client", return_value=mock_client):
            # Each call costs 1000/1000*0.003 + 1000/1000*0.015 = $0.018.
            client = AzureBenchClient(
                api_key="k",
                model="m",
                endpoint="https://e.openai.azure.com",
                max_usd=0.05,
                usd_per_1k_input=0.003,
                usd_per_1k_output=0.015,
            )
            client.complete([{"role": "user", "content": "hi"}])
            client.complete([{"role": "user", "content": "hi"}])
            with pytest.raises(CostCapExceededError, match="Azure spend guard tripped"):
                client.complete([{"role": "user", "content": "hi"}])

        assert client.cumulative_usd == pytest.approx(0.054)
