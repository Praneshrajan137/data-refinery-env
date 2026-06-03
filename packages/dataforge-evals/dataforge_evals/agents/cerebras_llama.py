"""Cerebras-hosted Llama adapter for data-quality repair evaluation.

Cerebras free tier (as of 2026-05-01): 30 RPM, 1,000 RPD on Llama 3.1 70B.
Quota unit = fraction of the daily request allocation consumed per API call.
"""

from __future__ import annotations

from dataforge_evals.agents.provider_base import ChatProviderAgent

# Cerebras free-tier daily request limit (as of 2026-05-01).
_CEREBRAS_FREE_DAILY_REQUESTS = 1_000


class CerebrasLlamaAgent(ChatProviderAgent):
    """Cerebras-hosted Llama adapter for data-quality repair evaluation.

    Uses the Cerebras OpenAI-compatible endpoint with Llama 3.1 70B.
    Quota units are normalized against the free-tier daily request allocation.

    Attributes:
        name: CLI identifier ``"cerebras-llama"``.
        provider: ``"cerebras"``.
    """

    name = "cerebras-llama"
    provider = "cerebras"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "llama3.1-70b",
        http_timeout_s: float = 15.0,
    ) -> None:
        """Initialize the Cerebras adapter.

        Args:
            api_key: Cerebras API key (``CEREBRAS_API_KEY``).
            model: Cerebras model identifier.
            http_timeout_s: Per-request HTTP timeout in seconds.
        """
        super().__init__(api_key=api_key, model=model, http_timeout_s=http_timeout_s)

    @property
    def endpoint(self) -> str:
        """Return the Cerebras OpenAI-compatible endpoint."""
        return "https://api.cerebras.ai/v1/chat/completions"

    def headers(self) -> dict[str, str]:
        """Return Cerebras authorization headers."""
        return {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}

    def quota_units(self, *, calls: int, prompt_tokens: int, completion_tokens: int) -> float:
        """Cerebras quota: fraction of daily request allocation.

        Args:
            calls: Number of HTTP requests made.
            prompt_tokens: Prompt tokens (tracked but not primary quota dimension).
            completion_tokens: Completion tokens (tracked but not primary quota dimension).

        Returns:
            Fraction of free-tier daily request quota consumed.
        """
        return round(calls / _CEREBRAS_FREE_DAILY_REQUESTS, 6)
