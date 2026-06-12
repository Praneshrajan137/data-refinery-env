"""Gemini Flash adapter for data-quality repair evaluation.

Google Gemini free tier (as of 2026-05-01): 15 RPM, 1,500 RPD,
1 million tokens/minute on Gemini 2.0 Flash. Quota unit = fraction
of the daily request allocation consumed per API call.

Gemini uses a non-OpenAI-compatible REST API, so this adapter overrides
the payload construction and response parsing from ChatProviderAgent.
"""

from __future__ import annotations

import logging
from typing import Any

from dataforge_evals.agents.base import AgentRunResult, Task, Usage
from dataforge_evals.agents.provider_base import ChatProviderAgent, ProviderError

logger = logging.getLogger(__name__)

# Gemini free-tier daily request limit (as of 2026-05-01).
_GEMINI_FREE_DAILY_REQUESTS = 1_500


class GeminiFlashAgent(ChatProviderAgent):
    """Gemini Flash adapter for data-quality repair evaluation.

    Uses the Gemini generateContent REST API (not OpenAI-compatible).
    Overrides payload construction and response parsing to match
    Gemini's content/parts structure.

    Attributes:
        name: CLI identifier ``"gemini-flash"``.
        provider: ``"gemini"``.
    """

    name = "gemini-flash"
    provider = "gemini"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gemini-2.0-flash",
        http_timeout_s: float = 15.0,
    ) -> None:
        """Initialize the Gemini adapter.

        Args:
            api_key: Google AI API key (``GEMINI_API_KEY``).
            model: Gemini model identifier.
            http_timeout_s: Per-request HTTP timeout in seconds.
        """
        super().__init__(api_key=api_key, model=model, http_timeout_s=http_timeout_s)

    @property
    def endpoint(self) -> str:
        """Return the Gemini generateContent endpoint with embedded API key."""
        return (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self._model}:generateContent?key={self._api_key}"
        )

    def headers(self) -> dict[str, str]:
        """Return Gemini HTTP headers (API key is in the URL, not a header)."""
        return {"Content-Type": "application/json"}

    def quota_units(self, *, calls: int, prompt_tokens: int, completion_tokens: int) -> float:
        """Gemini quota: fraction of daily request allocation.

        Args:
            calls: Number of HTTP requests made.
            prompt_tokens: Prompt tokens (tracked but not primary quota dimension).
            completion_tokens: Completion tokens (tracked but not primary quota dimension).

        Returns:
            Fraction of free-tier daily request quota consumed.
        """
        return round(calls / _GEMINI_FREE_DAILY_REQUESTS, 6)

    def run(self, task: Task) -> AgentRunResult:
        """Run Gemini Flash and return proposed fixes plus usage.

        Constructs a Gemini-native payload from the OpenAI-compatible
        base payload, then parses the Gemini-specific response structure.

        Args:
            task: The evaluation task containing dirty data.

        Returns:
            AgentRunResult with fixes, usage accounting, and model identifier.
        """
        base_payload = self.payload(task)
        messages = base_payload["messages"]
        if not isinstance(messages, list):
            raise ProviderError("Gemini payload messages must be a list", provider="gemini")
        prompt = "\n\n".join(
            str(message.get("content", "")) for message in messages if isinstance(message, dict)
        )
        gemini_payload: dict[str, object] = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0},
        }
        response = self._post_with_backoff(gemini_payload)
        text, usage = self._extract_gemini_text_and_usage(response)
        return AgentRunResult(
            fixes=self._parse_fixes(text), usage=usage, steps=1, model=self._model
        )

    def _extract_gemini_text_and_usage(self, response: dict[str, object]) -> tuple[str, Usage]:
        """Extract text and usage from the Gemini generateContent response.

        Args:
            response: Parsed JSON response from Gemini.

        Returns:
            Tuple of (completion text, Usage accounting).

        Raises:
            ProviderError: If the response structure is unexpected.
        """
        try:
            candidates: Any = response["candidates"]
            if not isinstance(candidates, list):
                raise TypeError("candidates is not a list")
            first: Any = candidates[0]
            if not isinstance(first, dict):
                raise TypeError("candidate is not a dict")
            content: Any = first["content"]
            if not isinstance(content, dict):
                raise TypeError("content is not a dict")
            parts: Any = content["parts"]
            if not isinstance(parts, list):
                raise TypeError("parts is not a list")
            first_part: Any = parts[0]
            if not isinstance(first_part, dict):
                raise TypeError("part is not a dict")
            text = str(first_part["text"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderError("Unexpected Gemini response payload", provider="gemini") from exc

        usage_payload: Any = response.get("usageMetadata", {})
        prompt_tokens = (
            int(usage_payload.get("promptTokenCount", 0)) if isinstance(usage_payload, dict) else 0
        )
        completion_tokens = (
            int(usage_payload.get("candidatesTokenCount", 0))
            if isinstance(usage_payload, dict)
            else 0
        )
        usage = Usage(
            calls=1,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            quota_units=self.quota_units(
                calls=1, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens
            ),
        )
        return text, usage
