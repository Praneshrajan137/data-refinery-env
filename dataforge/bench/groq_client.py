"""Minimal Groq client for benchmark-only LLM baselines."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import cast

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Return whether an exception is a Groq 429 response."""
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429


@dataclass(frozen=True, kw_only=True)
class GroqCompletion:
    """Completion payload plus conservative usage accounting."""

    text: str
    prompt_tokens: int
    completion_tokens: int
    warnings: tuple[str, ...]


class GroqBenchClient:
    """Sequential Groq client with fixed 429 retry and spacing."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "llama-3.3-70b-versatile",
        min_interval_s: float = 2.0,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._min_interval_s = min_interval_s
        self._last_success_at: float | None = None

    @property
    def model(self) -> str:
        """Return the configured Groq model name."""
        return self._model

    def _respect_spacing(self) -> None:
        """Sleep long enough to keep requests sequential with a fixed gap."""
        if self._last_success_at is None:
            return
        elapsed = time.monotonic() - self._last_success_at
        remaining = self._min_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)

    @retry(
        retry=retry_if_exception(_is_rate_limit_error),
        wait=wait_fixed(2),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _post(self, messages: list[dict[str, str]]) -> dict[str, object]:
        """Issue the underlying Groq chat-completions request."""
        payload = {
            "model": self._model,
            "messages": messages,
            "temperature": 0.0,
        }
        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
            )
            response.raise_for_status()
        return dict(response.json())

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Send one benchmark completion request to Groq."""
        self._respect_spacing()
        payload = self._post(messages)
        self._last_success_at = time.monotonic()

        warnings: list[str] = []
        usage = payload.get("usage", {})
        prompt_tokens = int(usage.get("prompt_tokens", 0)) if isinstance(usage, dict) else 0
        completion_tokens = int(usage.get("completion_tokens", 0)) if isinstance(usage, dict) else 0
        if not usage:
            warnings.append("missing_usage_payload")

        try:
            choices = cast(list[dict[str, object]], payload["choices"])
            message = cast(dict[str, object], choices[0]["message"])
            content = str(message["content"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"Unexpected Groq response payload: {json.dumps(payload)}") from exc
        return GroqCompletion(
            text=content,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            warnings=tuple(warnings),
        )
