"""Minimal OpenAI-compatible clients for benchmark-only LLM baselines."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Protocol, cast

import httpx


class ProviderRequestError(RuntimeError):
    """Raised when a provider rejects a benchmark request payload."""


class ProviderRateLimitError(ProviderRequestError):
    """Raised when a provider asks us to wait longer than the configured cap."""


class CostCapExceededError(RuntimeError):
    """Raised when cumulative estimated spend crosses the configured USD cap.

    This is a hard stop: once raised, no further billable calls are made. The
    estimate uses conservative (high) per-token prices so the guard trips early
    rather than late.
    """


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Return whether an exception is an HTTP 429 response."""
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429


def _is_retryable_provider_error(exc: BaseException) -> bool:
    """Return whether an HTTP error is worth retrying for teacher collection."""
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {429, 503}


def _retry_after_s(exc: httpx.HTTPStatusError, *, fallback_s: float) -> float:
    """Return provider retry-after delay when present."""
    raw_retry_after = exc.response.headers.get("retry-after")
    if raw_retry_after is None:
        return fallback_s
    try:
        return max(float(raw_retry_after), fallback_s)
    except ValueError:
        return fallback_s


@dataclass(frozen=True, kw_only=True)
class GroqCompletion:
    """Completion payload plus conservative usage accounting."""

    text: str
    prompt_tokens: int
    completion_tokens: int
    warnings: tuple[str, ...]


class BenchLLMClient(Protocol):
    """Structural interface shared by all benchmark LLM clients."""

    @property
    def model(self) -> str:
        """Return the configured provider model name."""
        ...

    @property
    def provider(self) -> str:
        """Return the provider identifier."""
        ...

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Send one benchmark completion request to the provider."""
        ...


class OpenAICompatBenchClient:
    """Sequential OpenAI-compatible client with fixed 429 retry and spacing."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        endpoint: str,
        provider: str,
        min_interval_s: float = 2.0,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
        usd_per_1k_input: float | None = None,
        usd_per_1k_output: float | None = None,
        max_usd: float | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._endpoint = endpoint
        self._provider = provider
        self._min_interval_s = min_interval_s
        self._max_tokens = max_tokens
        self._max_retries = max_retries
        self._max_retry_after_s = max_retry_after_s
        self._timeout_s = timeout_s
        self._usd_per_1k_input = usd_per_1k_input
        self._usd_per_1k_output = usd_per_1k_output
        self._max_usd = max_usd
        self._cumulative_usd = 0.0
        self._last_success_at: float | None = None
        self._client = httpx.Client(
            timeout=self._timeout_s,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )

    @property
    def model(self) -> str:
        """Return the configured provider model name."""
        return self._model

    @property
    def provider(self) -> str:
        """Return the configured provider identifier."""
        return self._provider

    @property
    def cumulative_usd(self) -> float:
        """Return the cumulative estimated spend so far (0 when guard is off)."""
        return self._cumulative_usd

    def _respect_spacing(self) -> None:
        """Sleep long enough to keep requests sequential with a fixed gap."""
        if self._last_success_at is None:
            return
        elapsed = time.monotonic() - self._last_success_at
        remaining = self._min_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _post(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Issue the underlying chat-completions request."""
        payload = {
            "model": self._model,
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": self._max_tokens,
        }
        last_rate_limit_error: httpx.HTTPStatusError | None = None
        for attempt in range(self._max_retries):
            response: httpx.Response | None = None
            try:
                response = self._client.post(
                    self._endpoint,
                    json=payload,
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if not _is_retryable_provider_error(exc) or attempt == self._max_retries - 1:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRequestError(
                        f"{self._provider} request rejected with HTTP "
                        f"{exc.response.status_code}: {body}"
                    ) from exc
                last_rate_limit_error = exc
                retry_s = _retry_after_s(exc, fallback_s=2.0 * (attempt + 1))
                if retry_s > self._max_retry_after_s:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRateLimitError(
                        f"{self._provider} rate limit retry-after {retry_s:.2f}s "
                        f"exceeds cap {self._max_retry_after_s:.2f}s: {body}"
                    ) from exc
                logging.getLogger("dataforge.bench.groq_client").warning(
                    "%s_rate_limit attempt=%d retry_after_s=%.2f",
                    self._provider,
                    attempt + 1,
                    retry_s,
                )
                time.sleep(retry_s)
                continue
            except httpx.TimeoutException as exc:
                raise TimeoutError(
                    f"{self._provider} request timed out after {self._timeout_s:.1f} seconds."
                ) from exc
            return dict(response.json())
        if last_rate_limit_error is not None:
            raise last_rate_limit_error
        raise RuntimeError(f"{self._provider} request failed without a response.")

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Send one benchmark completion request to the configured provider."""
        self._respect_spacing()
        payload = self._post(messages)
        self._last_success_at = time.monotonic()

        warnings: list[str] = []
        usage = payload.get("usage", {})
        prompt_tokens = int(usage.get("prompt_tokens", 0)) if isinstance(usage, dict) else 0
        completion_tokens = int(usage.get("completion_tokens", 0)) if isinstance(usage, dict) else 0
        if not usage:
            warnings.append("missing_usage_payload")
            logging.getLogger("dataforge.bench.groq_client").warning(
                "%s_missing_usage_payload", self._provider
            )

        try:
            choices = cast(list[dict[str, object]], payload["choices"])
            message = cast(dict[str, object], choices[0]["message"])
            content = str(message["content"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(
                f"Unexpected {self._provider} response payload: {json.dumps(payload)}"
            ) from exc
        self._enforce_cost_guard(prompt_tokens, completion_tokens)
        return GroqCompletion(
            text=content,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            warnings=tuple(warnings),
        )

    def _enforce_cost_guard(self, prompt_tokens: int, completion_tokens: int) -> None:
        """Accumulate estimated spend and hard-stop if it crosses the USD cap.

        A no-op when no per-token prices are configured (the default), so Groq
        and Cerebras behavior is unchanged.
        """
        if self._usd_per_1k_input is None or self._usd_per_1k_output is None:
            return
        self._cumulative_usd += (prompt_tokens / 1000.0) * self._usd_per_1k_input + (
            completion_tokens / 1000.0
        ) * self._usd_per_1k_output
        if self._max_usd is not None and self._cumulative_usd > self._max_usd:
            raise CostCapExceededError(
                f"{self._provider} spend guard tripped: estimated "
                f"${self._cumulative_usd:.4f} exceeds cap ${self._max_usd:.2f}. "
                "No further calls will be made."
            )


class GroqBenchClient(OpenAICompatBenchClient):
    """Sequential Groq client with fixed 429 retry and spacing."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "llama-3.3-70b-versatile",
        min_interval_s: float = 2.0,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            endpoint="https://api.groq.com/openai/v1/chat/completions",
            provider="groq",
            min_interval_s=min_interval_s,
            max_tokens=max_tokens,
            max_retries=max_retries,
            max_retry_after_s=max_retry_after_s,
            timeout_s=timeout_s,
        )


class CerebrasBenchClient(OpenAICompatBenchClient):
    """Sequential Cerebras client with fixed 429 retry and spacing."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "qwen-3-235b-a22b-instruct-2507",
        min_interval_s: float = 0.5,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            endpoint="https://api.cerebras.ai/v1/chat/completions",
            provider="cerebras",
            min_interval_s=min_interval_s,
            max_tokens=max_tokens,
            max_retries=max_retries,
            max_retry_after_s=max_retry_after_s,
            timeout_s=timeout_s,
        )


class GrokBenchClient(OpenAICompatBenchClient):
    """Sequential xAI Grok client (OpenAI-compatible) with a USD cost guard.

    NOTE: "grok" (xAI) is not the repo's "groq" (Groq Inc.) provider. Grok is
    metered ($/token), so the USD guard is on by default with conservative
    xAI list prices; override via the runner env.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "grok-4.5",
        min_interval_s: float = 0.5,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
        usd_per_1k_input: float = 0.002,
        usd_per_1k_output: float = 0.006,
        max_usd: float | None = 15.0,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            endpoint="https://api.x.ai/v1/chat/completions",
            provider="grok",
            min_interval_s=min_interval_s,
            max_tokens=max_tokens,
            max_retries=max_retries,
            max_retry_after_s=max_retry_after_s,
            timeout_s=timeout_s,
            usd_per_1k_input=usd_per_1k_input,
            usd_per_1k_output=usd_per_1k_output,
            max_usd=max_usd,
        )


class GeminiBenchClient:
    """Sequential Gemini client adapted to the benchmark completion interface."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gemini-3.1-pro-preview",
        min_interval_s: float = 2.0,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
        temperature: float = 0.0,
    ) -> None:
        self._api_key = api_key
        self._model = model.removeprefix("models/")
        self._min_interval_s = min_interval_s
        self._max_tokens = max_tokens
        self._max_retries = max_retries
        self._max_retry_after_s = max_retry_after_s
        self._timeout_s = timeout_s
        self._temperature = temperature
        self._last_success_at: float | None = None
        self._client = httpx.Client(
            timeout=self._timeout_s,
            headers={"Content-Type": "application/json"},
        )

    @property
    def model(self) -> str:
        """Return the configured Gemini model name."""
        return self._model

    @property
    def provider(self) -> str:
        """Return the provider identifier."""
        return "gemini"

    def _respect_spacing(self) -> None:
        """Sleep long enough to keep requests sequential with a fixed gap."""
        if self._last_success_at is None:
            return
        elapsed = time.monotonic() - self._last_success_at
        remaining = self._min_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _payload(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Convert OpenAI-style chat messages to Gemini generateContent payload."""
        system_texts: list[str] = []
        contents: list[dict[str, object]] = []
        for message in messages:
            role = message.get("role", "user")
            content = message.get("content", "")
            if role == "system":
                system_texts.append(content)
                continue
            gemini_role = "model" if role == "assistant" else "user"
            contents.append({"role": gemini_role, "parts": [{"text": content}]})

        payload: dict[str, object] = {
            "contents": contents,
            "generationConfig": {
                "temperature": self._temperature,
                "maxOutputTokens": self._max_tokens,
            },
        }
        if system_texts:
            payload["systemInstruction"] = {
                "parts": [{"text": "\n\n".join(system_texts)}],
            }
        return payload

    def _post(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Issue the underlying Gemini generateContent request."""
        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent"
        )
        last_rate_limit_error: httpx.HTTPStatusError | None = None
        for attempt in range(self._max_retries):
            response: httpx.Response | None = None
            try:
                response = self._client.post(
                    endpoint,
                    params={"key": self._api_key},
                    json=self._payload(messages),
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if not _is_retryable_provider_error(exc) or attempt == self._max_retries - 1:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRequestError(
                        f"gemini request rejected with HTTP {exc.response.status_code}: {body}"
                    ) from exc
                last_rate_limit_error = exc
                retry_s = _retry_after_s(exc, fallback_s=2.0 * (attempt + 1))
                if retry_s > self._max_retry_after_s:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRateLimitError(
                        f"gemini rate limit retry-after {retry_s:.2f}s "
                        f"exceeds cap {self._max_retry_after_s:.2f}s: {body}"
                    ) from exc
                logging.getLogger("dataforge.bench.groq_client").warning(
                    "gemini_rate_limit attempt=%d retry_after_s=%.2f",
                    attempt + 1,
                    retry_s,
                )
                time.sleep(retry_s)
                continue
            except httpx.TimeoutException as exc:
                raise TimeoutError(
                    f"gemini request timed out after {self._timeout_s:.1f} seconds."
                ) from exc
            return dict(response.json())
        if last_rate_limit_error is not None:
            raise last_rate_limit_error
        raise RuntimeError("gemini request failed without a response.")

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Send one benchmark completion request to Gemini."""
        self._respect_spacing()
        payload = self._post(messages)
        self._last_success_at = time.monotonic()

        warnings: list[str] = []
        usage = payload.get("usageMetadata", {})
        prompt_tokens = int(usage.get("promptTokenCount", 0)) if isinstance(usage, dict) else 0
        completion_tokens = (
            int(usage.get("candidatesTokenCount", 0)) if isinstance(usage, dict) else 0
        )
        if not usage:
            warnings.append("missing_usage_payload")
            logging.getLogger("dataforge.bench.groq_client").warning("gemini_missing_usage_payload")

        try:
            candidates = cast(list[dict[str, object]], payload["candidates"])
            content = cast(dict[str, object], candidates[0]["content"])
            parts = cast(list[dict[str, object]], content["parts"])
            text = "".join(str(part.get("text", "")) for part in parts)
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"Unexpected gemini response payload: {json.dumps(payload)}") from exc
        return GroqCompletion(
            text=text,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            warnings=tuple(warnings),
        )


class AzureBenchClient:
    """Sequential Azure OpenAI client adapted to the benchmark interface.

    Azure OpenAI is first-party ("sold directly by Azure"), so it is billed
    against the subscription and works on free-trial credit. It is
    OpenAI-compatible (system stays a message role) but the model is addressed
    by *deployment name* in the URL, auth is an ``api-key`` header, and the
    ``api-version`` is a query parameter. GPT-5 / reasoning deployments reject
    ``temperature != 1`` and require ``max_completion_tokens``; temperature is
    therefore only sent when ``send_temperature`` is True.

    A hard USD cost guard (mirroring the Bedrock client) prevents a bounded run
    from ever overspending the finite trial credit.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        endpoint: str,
        api_version: str = "2025-04-01-preview",
        min_interval_s: float = 1.0,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
        max_usd: float | None = None,
        usd_per_1k_input: float = 0.005,
        usd_per_1k_output: float = 0.015,
        send_temperature: bool = False,
        temperature: float = 0.0,
        reasoning_effort: str | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._endpoint = endpoint.rstrip("/")
        self._api_version = api_version
        self._min_interval_s = min_interval_s
        self._max_tokens = max_tokens
        self._max_retries = max_retries
        self._max_retry_after_s = max_retry_after_s
        self._timeout_s = timeout_s
        self._max_usd = max_usd
        self._usd_per_1k_input = usd_per_1k_input
        self._usd_per_1k_output = usd_per_1k_output
        self._send_temperature = send_temperature
        self._temperature = temperature
        self._reasoning_effort = reasoning_effort
        self._cumulative_usd = 0.0
        self._last_success_at: float | None = None
        self._client = httpx.Client(
            timeout=self._timeout_s,
            headers={"api-key": self._api_key, "Content-Type": "application/json"},
        )

    @property
    def cumulative_usd(self) -> float:
        """Return the running estimated spend across all completed calls."""
        return self._cumulative_usd

    @property
    def model(self) -> str:
        """Return the configured Azure deployment name."""
        return self._model

    @property
    def provider(self) -> str:
        """Return the provider identifier."""
        return "azure"

    def _respect_spacing(self) -> None:
        """Sleep long enough to keep requests sequential with a fixed gap."""
        if self._last_success_at is None:
            return
        elapsed = time.monotonic() - self._last_success_at
        remaining = self._min_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _payload(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Build an Azure OpenAI chat/completions request payload."""
        payload: dict[str, object] = {
            "messages": list(messages),
            "max_completion_tokens": self._max_tokens,
        }
        if self._reasoning_effort:
            payload["reasoning_effort"] = self._reasoning_effort
        if self._send_temperature:
            payload["temperature"] = self._temperature
        return payload

    def _post(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Issue the underlying Azure OpenAI chat/completions request."""
        endpoint = f"{self._endpoint}/openai/deployments/{self._model}/chat/completions"
        last_rate_limit_error: httpx.HTTPStatusError | None = None
        for attempt in range(self._max_retries):
            response: httpx.Response | None = None
            try:
                response = self._client.post(
                    endpoint,
                    params={"api-version": self._api_version},
                    json=self._payload(messages),
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if not _is_retryable_provider_error(exc) or attempt == self._max_retries - 1:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRequestError(
                        f"azure request rejected with HTTP {exc.response.status_code}: {body}"
                    ) from exc
                last_rate_limit_error = exc
                retry_s = _retry_after_s(exc, fallback_s=2.0 * (attempt + 1))
                if retry_s > self._max_retry_after_s:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRateLimitError(
                        f"azure rate limit retry-after {retry_s:.2f}s "
                        f"exceeds cap {self._max_retry_after_s:.2f}s: {body}"
                    ) from exc
                logging.getLogger("dataforge.bench.groq_client").warning(
                    "azure_rate_limit attempt=%d retry_after_s=%.2f",
                    attempt + 1,
                    retry_s,
                )
                time.sleep(retry_s)
                continue
            except httpx.TimeoutException as exc:
                raise TimeoutError(
                    f"azure request timed out after {self._timeout_s:.1f} seconds."
                ) from exc
            return dict(response.json())
        if last_rate_limit_error is not None:
            raise last_rate_limit_error
        raise RuntimeError("azure request failed without a response.")

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Send one benchmark completion request to Azure OpenAI."""
        self._respect_spacing()
        payload = self._post(messages)
        self._last_success_at = time.monotonic()

        warnings: list[str] = []
        usage = payload.get("usage", {})
        prompt_tokens = int(usage.get("prompt_tokens", 0)) if isinstance(usage, dict) else 0
        completion_tokens = int(usage.get("completion_tokens", 0)) if isinstance(usage, dict) else 0
        if not usage:
            warnings.append("missing_usage_payload")
            logging.getLogger("dataforge.bench.groq_client").warning("azure_missing_usage_payload")

        try:
            choices = cast(list[dict[str, object]], payload["choices"])
            message = cast(dict[str, object], choices[0]["message"])
            text = str(message["content"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"Unexpected azure response payload: {json.dumps(payload)}") from exc

        # Accumulate a conservative cost estimate and hard-stop if it crosses the
        # configured USD cap, so a bounded run can never overspend trial credit.
        self._cumulative_usd += (
            prompt_tokens / 1000.0 * self._usd_per_1k_input
            + completion_tokens / 1000.0 * self._usd_per_1k_output
        )
        if self._max_usd is not None and self._cumulative_usd > self._max_usd:
            raise CostCapExceededError(
                f"Azure spend guard tripped: estimated ${self._cumulative_usd:.4f} "
                f"exceeds cap ${self._max_usd:.2f}. No further calls will be made."
            )

        return GroqCompletion(
            text=text,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            warnings=tuple(warnings),
        )


class BedrockBenchClient:
    """Sequential Amazon Bedrock client adapted to the benchmark interface.

    Uses the bearer-token Converse API (not SigV4, not OpenAI-compatible):
    the system prompt is a top-level field, message content is a list of
    typed blocks, and inference parameters live under ``inferenceConfig``.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        region: str = "us-east-1",
        min_interval_s: float = 2.0,
        max_tokens: int = 512,
        max_retries: int = 5,
        max_retry_after_s: float = 120.0,
        timeout_s: float = 60.0,
        max_usd: float | None = None,
        usd_per_1k_input: float = 0.003,
        usd_per_1k_output: float = 0.015,
        temperature: float = 0.0,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._region = region
        self._min_interval_s = min_interval_s
        self._max_tokens = max_tokens
        self._max_retries = max_retries
        self._max_retry_after_s = max_retry_after_s
        self._timeout_s = timeout_s
        self._max_usd = max_usd
        self._usd_per_1k_input = usd_per_1k_input
        self._usd_per_1k_output = usd_per_1k_output
        self._temperature = temperature
        self._cumulative_usd = 0.0
        self._last_success_at: float | None = None
        self._client = httpx.Client(
            timeout=self._timeout_s,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )

    @property
    def cumulative_usd(self) -> float:
        """Return the running estimated spend across all completed calls."""
        return self._cumulative_usd

    @property
    def model(self) -> str:
        """Return the configured Bedrock model / inference-profile ID."""
        return self._model

    @property
    def provider(self) -> str:
        """Return the provider identifier."""
        return "bedrock"

    def _respect_spacing(self) -> None:
        """Sleep long enough to keep requests sequential with a fixed gap."""
        if self._last_success_at is None:
            return
        elapsed = time.monotonic() - self._last_success_at
        remaining = self._min_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _payload(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Convert OpenAI-style chat messages to a Converse request payload."""
        system_blocks: list[dict[str, str]] = []
        converse_messages: list[dict[str, object]] = []
        for message in messages:
            role = message.get("role", "user")
            content = message.get("content", "")
            if role == "system":
                system_blocks.append({"text": content})
                continue
            converse_messages.append({"role": role, "content": [{"text": content}]})

        payload: dict[str, object] = {
            "messages": converse_messages,
            "inferenceConfig": {
                "temperature": self._temperature,
                "maxTokens": self._max_tokens,
            },
        }
        if system_blocks:
            payload["system"] = system_blocks
        return payload

    def _post(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Issue the underlying Bedrock Converse request."""
        endpoint = (
            f"https://bedrock-runtime.{self._region}.amazonaws.com/model/{self._model}/converse"
        )
        last_rate_limit_error: httpx.HTTPStatusError | None = None
        for attempt in range(self._max_retries):
            response: httpx.Response | None = None
            try:
                response = self._client.post(endpoint, json=self._payload(messages))
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if not _is_retryable_provider_error(exc) or attempt == self._max_retries - 1:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRequestError(
                        f"bedrock request rejected with HTTP {exc.response.status_code}: {body}"
                    ) from exc
                last_rate_limit_error = exc
                retry_s = _retry_after_s(exc, fallback_s=2.0 * (attempt + 1))
                if retry_s > self._max_retry_after_s:
                    body = exc.response.text[:500].replace("\n", " ")
                    raise ProviderRateLimitError(
                        f"bedrock rate limit retry-after {retry_s:.2f}s "
                        f"exceeds cap {self._max_retry_after_s:.2f}s: {body}"
                    ) from exc
                logging.getLogger("dataforge.bench.groq_client").warning(
                    "bedrock_rate_limit attempt=%d retry_after_s=%.2f",
                    attempt + 1,
                    retry_s,
                )
                time.sleep(retry_s)
                continue
            except httpx.TimeoutException as exc:
                raise TimeoutError(
                    f"bedrock request timed out after {self._timeout_s:.1f} seconds."
                ) from exc
            return dict(response.json())
        if last_rate_limit_error is not None:
            raise last_rate_limit_error
        raise RuntimeError("bedrock request failed without a response.")

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Send one benchmark completion request to Bedrock."""
        self._respect_spacing()
        payload = self._post(messages)
        self._last_success_at = time.monotonic()

        warnings: list[str] = []
        usage = payload.get("usage", {})
        prompt_tokens = int(usage.get("inputTokens", 0)) if isinstance(usage, dict) else 0
        completion_tokens = int(usage.get("outputTokens", 0)) if isinstance(usage, dict) else 0
        if not usage:
            warnings.append("missing_usage_payload")
            logging.getLogger("dataforge.bench.groq_client").warning(
                "bedrock_missing_usage_payload"
            )

        try:
            output = cast(dict[str, object], payload["output"])
            message = cast(dict[str, object], output["message"])
            content = cast(list[dict[str, object]], message["content"])
            text = "".join(str(block.get("text", "")) for block in content)
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"Unexpected bedrock response payload: {json.dumps(payload)}") from exc

        # Accumulate a conservative cost estimate and hard-stop if it crosses
        # the configured USD cap, so a bounded run can never overspend.
        self._cumulative_usd += (
            prompt_tokens / 1000.0 * self._usd_per_1k_input
            + completion_tokens / 1000.0 * self._usd_per_1k_output
        )
        if self._max_usd is not None and self._cumulative_usd > self._max_usd:
            raise CostCapExceededError(
                f"Bedrock spend guard tripped: estimated ${self._cumulative_usd:.4f} "
                f"exceeds cap ${self._max_usd:.2f}. No further calls will be made."
            )

        return GroqCompletion(
            text=text,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            warnings=tuple(warnings),
        )
