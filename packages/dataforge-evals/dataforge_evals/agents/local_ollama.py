"""Local Ollama adapter for air-gapped or unlimited local evaluation.

Ollama runs locally and has no rate limits or quota. Quota units are
always zero, enabling unlimited evaluation without external dependencies.
"""

from __future__ import annotations

from dataforge_evals.agents.provider_base import ChatProviderAgent


class LocalOllamaAgent(ChatProviderAgent):
    """Local Ollama adapter for air-gapped or unlimited local evaluation.

    Connects to a local Ollama instance running an OpenAI-compatible
    endpoint. Requires Ollama to be running on ``localhost:11434``.

    Attributes:
        name: CLI identifier ``"local-ollama"``.
        provider: ``"ollama"``.
    """

    name = "local-ollama"
    provider = "ollama"

    def __init__(
        self,
        *,
        api_key: str = "ollama",
        model: str = "llama3.1:8b",
        http_timeout_s: float = 15.0,
    ) -> None:
        """Initialize the Ollama adapter.

        Args:
            api_key: Placeholder key (Ollama does not require authentication).
            model: Ollama model tag.
            http_timeout_s: Per-request HTTP timeout in seconds.
        """
        super().__init__(api_key=api_key, model=model, http_timeout_s=http_timeout_s)

    @property
    def endpoint(self) -> str:
        """Return the local Ollama OpenAI-compatible endpoint."""
        return "http://localhost:11434/v1/chat/completions"

    def headers(self) -> dict[str, str]:
        """Return Ollama-compatible headers (no authentication required)."""
        return {"Content-Type": "application/json"}

    def quota_units(self, *, calls: int, prompt_tokens: int, completion_tokens: int) -> float:
        """Ollama quota: always zero (local, no rate limits).

        Args:
            calls: Number of HTTP requests made.
            prompt_tokens: Prompt tokens (tracked for audit).
            completion_tokens: Completion tokens (tracked for audit).

        Returns:
            Always 0.0.
        """
        return 0.0
