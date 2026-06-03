"""Groq-hosted Llama adapter for data-quality repair evaluation.

Groq free tier (as of 2026-05-01): 30 requests/minute, 14,400 requests/day,
6,000 tokens/minute on Llama 3.3 70B. Quota unit = fraction of the daily
request allocation consumed per API call.
"""

from __future__ import annotations

from dataforge_evals.agents.provider_base import ChatProviderAgent

# Groq free-tier daily request limit for Llama 3.3 70B (as of 2026-05-01).
_GROQ_FREE_DAILY_REQUESTS = 14_400


class GroqLlamaAgent(ChatProviderAgent):
    """Groq-hosted Llama adapter for data-quality repair evaluation.

    Uses the Groq OpenAI-compatible endpoint with Llama 3.3 70B Versatile.
    Quota units are normalized against the free-tier daily request allocation.

    Attributes:
        name: CLI identifier ``"groq-llama-70b"``.
        provider: ``"groq"``.
    """

    name = "groq-llama-70b"
    provider = "groq"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "llama-3.3-70b-versatile",
        http_timeout_s: float = 15.0,
    ) -> None:
        """Initialize the Groq adapter.

        Args:
            api_key: Groq API key (``GROQ_API_KEY``).
            model: Groq model identifier.
            http_timeout_s: Per-request HTTP timeout in seconds.
        """
        super().__init__(api_key=api_key, model=model, http_timeout_s=http_timeout_s)

    @property
    def endpoint(self) -> str:
        """Return the Groq OpenAI-compatible endpoint."""
        return "https://api.groq.com/openai/v1/chat/completions"

    def headers(self) -> dict[str, str]:
        """Return Groq authorization headers."""
        return {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}

    def quota_units(self, *, calls: int, prompt_tokens: int, completion_tokens: int) -> float:
        """Groq quota: fraction of daily request allocation.

        Args:
            calls: Number of HTTP requests made.
            prompt_tokens: Prompt tokens (tracked but not primary quota dimension).
            completion_tokens: Completion tokens (tracked but not primary quota dimension).

        Returns:
            Fraction of free-tier daily request quota consumed.
        """
        return round(calls / _GROQ_FREE_DAILY_REQUESTS, 6)
