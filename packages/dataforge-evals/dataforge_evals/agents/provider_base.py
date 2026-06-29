"""Base class for chat-completion provider adapters.

Implements the shared prompt/parse boundary, provider-specific quota
accounting, and HTTP retry logic that all hosted provider adapters inherit.
"""

from __future__ import annotations

import json
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Any, cast

import httpx

from dataforge_evals.agents.base import AgentRunResult, AgentTask, AgentTaskInput, Fix, Usage

logger = logging.getLogger(__name__)

# Pattern to extract JSON from markdown code fences (common LLM output format)
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?\s*```", re.DOTALL)


class ProviderError(RuntimeError):
    """Raised when a provider adapter cannot complete a request.

    Attributes:
        provider: The provider that failed (e.g. ``"groq"``, ``"gemini"``).
    """

    def __init__(self, message: str, *, provider: str = "unknown") -> None:
        super().__init__(f"[{provider}] {message}")
        self.provider = provider


class ChatProviderAgent(ABC):
    """Base class for chat-completion data-quality agent adapters.

    Subclasses must implement ``endpoint`` and ``headers``. The default
    ``run`` method posts an OpenAI-compatible chat-completion request,
    retries on HTTP 429 with exponential backoff, and parses the response
    into ``Fix`` objects.

    Override ``quota_units`` to provide provider-specific free-tier
    normalization instead of the default formula.

    Attributes:
        name: CLI-facing agent identifier.
        provider: Provider name for diagnostics and logging.
    """

    name: str
    provider: str

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        max_retries: int = 5,
        http_timeout_s: float = 15.0,
    ) -> None:
        """Initialize the provider adapter.

        Args:
            api_key: Provider API key for authentication.
            model: Provider model identifier (e.g. ``"llama-3.3-70b-versatile"``).
            max_retries: Maximum retry attempts on HTTP 429.
            http_timeout_s: Per-request HTTP timeout in seconds.
        """
        self._api_key = api_key
        self._model = model
        self._max_retries = max_retries
        self._http_timeout_s = http_timeout_s

    @property
    def model(self) -> str:
        """Return the configured provider model identifier."""
        return self._model

    @property
    @abstractmethod
    def endpoint(self) -> str:
        """Return the provider chat-completions endpoint URL."""

    @abstractmethod
    def headers(self) -> dict[str, str]:
        """Return provider-specific HTTP headers including authentication."""

    def payload(self, task: AgentTask) -> dict[str, object]:
        """Build an OpenAI-compatible chat-completions payload.

        Args:
            task: The evaluation task containing dirty data.

        Returns:
            JSON-serializable payload dictionary.
        """
        return {
            "model": self._model,
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You repair tabular data by proposing exact cell replacements. "
                        "Rows are zero-based and columns must exactly match one of the allowed "
                        "column names. Emit only cells that are visibly incorrect from the "
                        "provided dirty records; do not invent broad changes. Return strict JSON "
                        'only in this object shape: {"action":"submit_repairs","repairs":['
                        '{"row":0,"column":"Column","new_value":"value","reason":"why"}]}. '
                        "Use finish with an empty repairs list when no cells should be changed. "
                        "Do not wrap the JSON in markdown code fences."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "dataset": task.name,
                            "metadata": task.metadata,
                            "columns": task.canonical_columns,
                            "records": task.dirty_df.to_dict(orient="records"),
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }

    def quota_units(self, *, calls: int, prompt_tokens: int, completion_tokens: int) -> float:
        """Compute provider-normalized free-tier quota units.

        Override in subclasses for provider-specific normalization.
        The default formula uses a generic rate/token approximation.

        Args:
            calls: Number of HTTP requests made.
            prompt_tokens: Total prompt/input tokens consumed.
            completion_tokens: Total completion/output tokens consumed.

        Returns:
            Normalized quota fraction as a float in [0.0, ...].
        """
        request_fraction = calls / 1000 if calls else 0.0
        token_fraction = (
            (prompt_tokens + completion_tokens) / 100_000
            if (prompt_tokens or completion_tokens)
            else 0.0
        )
        return round(max(request_fraction, token_fraction), 4)

    def _post_with_backoff(self, payload: dict[str, object]) -> dict[str, object]:
        """Post a request, retrying 429s with exponential backoff for this provider only.

        Does NOT fall back to another provider - fallback would contaminate
        the comparison across a multi-agent evaluation.

        Args:
            payload: JSON-serializable request payload.

        Returns:
            Parsed JSON response dictionary.

        Raises:
            ProviderError: After exhausting all retry attempts.
            httpx.HTTPStatusError: On non-429 HTTP errors.
        """
        delay = 2.0
        with httpx.Client(timeout=self._http_timeout_s) as client:
            for attempt in range(self._max_retries):
                try:
                    response = client.post(self.endpoint, json=payload, headers=self.headers())
                except httpx.TimeoutException as exc:
                    raise ProviderError(
                        f"request timed out after {self._http_timeout_s:.1f} seconds",
                        provider=self.provider,
                    ) from exc
                if response.status_code != 429:
                    response.raise_for_status()
                    return dict(response.json())
                if attempt == self._max_retries - 1:
                    response.raise_for_status()
                logger.warning(
                    "HTTP 429 from %s - waiting %.0f seconds for quota reset (attempt %d/%d)",
                    self.provider,
                    delay,
                    attempt + 1,
                    self._max_retries,
                )
                time.sleep(delay)
                delay *= 2
        raise ProviderError(
            f"request exhausted {self._max_retries} retries",
            provider=self.provider,
        )

    def _extract_text_and_usage(self, response: dict[str, object]) -> tuple[str, Usage]:
        """Extract completion text and usage fields from an OpenAI-compatible response.

        Args:
            response: Parsed JSON response from the provider.

        Returns:
            Tuple of (completion text, Usage accounting).

        Raises:
            ProviderError: If the response payload is malformed.
        """
        try:
            choices = cast(list[dict[str, Any]], response["choices"])
            message = cast(dict[str, Any], choices[0]["message"])
            text = str(message["content"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderError(
                "Unexpected response payload structure",
                provider=self.provider,
            ) from exc
        usage_payload = response.get("usage", {})
        prompt_tokens = (
            int(usage_payload.get("prompt_tokens", 0)) if isinstance(usage_payload, dict) else 0
        )
        completion_tokens = (
            int(usage_payload.get("completion_tokens", 0)) if isinstance(usage_payload, dict) else 0
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

    def _extract_json_text(self, text: str) -> str:
        """Extract JSON from LLM output, stripping markdown code fences if present.

        Args:
            text: Raw completion text from the provider.

        Returns:
            Clean JSON string ready for parsing.
        """
        stripped = text.strip()
        match = _JSON_FENCE_RE.search(stripped)
        if match:
            return match.group(1).strip()
        return stripped

    def _parse_fixes(self, text: str) -> list[Fix]:
        """Parse provider JSON output into fix objects.

        Handles markdown code fences (common LLM output pattern) and
        validates each fix object individually, skipping malformed entries
        with a logged warning rather than failing the entire run.

        Args:
            text: Raw completion text from the provider.

        Returns:
            List of validated Fix objects.

        Raises:
            ProviderError: If the output cannot be parsed as JSON at all.
        """
        clean_text = self._extract_json_text(text)
        try:
            raw = json.loads(clean_text)
        except json.JSONDecodeError as exc:
            raise ProviderError(
                f"Provider returned non-JSON fixes: {clean_text[:200]}",
                provider=self.provider,
            ) from exc
        if isinstance(raw, dict):
            raw = raw.get("repairs", [])
        if not isinstance(raw, list):
            raise ProviderError(
                "Provider JSON must be a list of fix objects",
                provider=self.provider,
            )
        fixes: list[Fix] = []
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                logger.warning("Skipping non-dict fix at index %d from %s", index, self.provider)
                continue
            try:
                fixes.append(
                    Fix(
                        row=int(item["row"]),
                        column=str(item["column"]),
                        new_value=str(item["new_value"]),
                        reason=str(item.get("reason", "provider proposal")),
                    )
                )
            except (KeyError, ValueError, TypeError) as exc:
                logger.warning(
                    "Skipping malformed fix at index %d from %s: %s",
                    index,
                    self.provider,
                    exc,
                )
        return fixes

    def run(self, task: AgentTaskInput) -> AgentRunResult:
        """Run the provider adapter and return proposed fixes plus usage.

        Args:
            task: The evaluation task containing dirty data.

        Returns:
            AgentRunResult with fixes, usage accounting, and model identifier.
        """
        response = self._post_with_backoff(self.payload(task))
        text, usage = self._extract_text_and_usage(response)
        fixes = self._parse_fixes(text)
        return AgentRunResult(fixes=fixes, usage=usage, steps=1, model=self._model)
