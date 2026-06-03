"""OpenRouter adapter for data-quality repair evaluation.

OpenRouter free tier (as of 2026-05-01): varies by model; some free models
available. Quota unit = fraction of a nominal 1000-request daily budget,
since OpenRouter's free tier is credit-based and model-dependent.
"""

from __future__ import annotations

from dataforge_evals.agents.provider_base import ChatProviderAgent

# Nominal daily request budget for OpenRouter quota normalization.
_OPENROUTER_NOMINAL_DAILY_REQUESTS = 1_000


class OpenRouterAgent(ChatProviderAgent):
    """OpenRouter adapter for data-quality repair evaluation.

    Uses the OpenRouter OpenAI-compatible endpoint with configurable model.
    Default model: ``meta-llama/llama-3.3-70b-instruct``.

    Attributes:
        name: CLI identifier ``"openrouter"``.
        provider: ``"openrouter"``.
    """

    name = "openrouter"
    provider = "openrouter"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "meta-llama/llama-3.3-70b-instruct",
        http_timeout_s: float = 15.0,
    ) -> None:
        """Initialize the OpenRouter adapter.

        Args:
            api_key: OpenRouter API key (``OPENROUTER_API_KEY``).
            model: OpenRouter model identifier.
            http_timeout_s: Per-request HTTP timeout in seconds.
        """
        super().__init__(api_key=api_key, model=model, http_timeout_s=http_timeout_s)

    @property
    def endpoint(self) -> str:
        """Return the OpenRouter chat-completions endpoint."""
        return "https://openrouter.ai/api/v1/chat/completions"

    def headers(self) -> dict[str, str]:
        """Return OpenRouter authorization headers."""
        return {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}

    def quota_units(self, *, calls: int, prompt_tokens: int, completion_tokens: int) -> float:
        """OpenRouter quota: fraction of nominal daily request budget.

        Args:
            calls: Number of HTTP requests made.
            prompt_tokens: Prompt tokens (tracked for audit).
            completion_tokens: Completion tokens (tracked for audit).

        Returns:
            Fraction of nominal daily budget consumed.
        """
        return round(calls / _OPENROUTER_NOMINAL_DAILY_REQUESTS, 6)
