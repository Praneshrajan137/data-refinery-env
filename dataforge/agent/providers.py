"""Multi-provider LLM client for DataForge.

Reads ``DATAFORGE_LLM_PROVIDER`` from the environment and dispatches to the
matching provider.  Implemented providers are **groq**, **gemini**, and
**bedrock** (Amazon Bedrock Converse API, bearer-token auth); other providers
raise ``NotImplementedError``.

No LLM calls are made by detectors — this module is for the agent loop
(Week 2+) and is stubbed here to establish the interface.

The interface is:
    ``async def complete(messages, model, temperature) -> str``
"""

from __future__ import annotations

import os
from typing import Literal, TypedDict

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# ── Message type ──────────────────────────────────────────────────────────


class Message(TypedDict):
    """A single chat message.

    Args:
        role: The speaker role — ``"system"``, ``"user"``, or ``"assistant"``.
        content: The text content of the message.
    """

    role: Literal["system", "user", "assistant"]
    content: str


# ── Exceptions ────────────────────────────────────────────────────────────


class ProviderError(Exception):
    """Raised when an LLM provider call fails after retries.

    Args:
        provider: The provider name that failed.
        message: Description of the failure.
    """

    def __init__(self, provider: str, message: str) -> None:
        self.provider = provider
        super().__init__(f"[{provider}] {message}")


# ── Provider dispatch ─────────────────────────────────────────────────────

_SUPPORTED_PROVIDERS = frozenset(
    {"groq", "gemini", "bedrock", "cerebras", "openrouter", "hf", "cloudflare"}
)


def get_provider_name() -> str:
    """Read the active provider from the environment.

    Returns:
        The lowercased provider name from ``DATAFORGE_LLM_PROVIDER``.
        When no explicit provider is configured, prefer a provider whose
        credential is present in the environment.

    Example:
        >>> import os
        >>> os.environ["DATAFORGE_LLM_PROVIDER"] = "gemini"
        >>> get_provider_name()
        'gemini'
    """
    configured = os.environ.get("DATAFORGE_LLM_PROVIDER")
    if configured:
        return configured.lower()
    if os.environ.get("GROQ_API_KEY"):
        return "groq"
    if os.environ.get("GEMINI_API_KEY"):
        return "gemini"
    if os.environ.get("AWS_BEARER_TOKEN_BEDROCK"):
        return "bedrock"
    return "groq"


async def complete(
    messages: list[Message],
    *,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Send a chat completion request to the active LLM provider.

    Args:
        messages: List of chat messages forming the conversation.
        model: Optional model override. If None, uses the provider default.
        temperature: Sampling temperature (0.0 = deterministic).

    Returns:
        The assistant's response text.

    Raises:
        NotImplementedError: If the provider is not yet implemented.
        ProviderError: If the API call fails after retries.

    Example:
        >>> import asyncio
        >>> msgs = [{"role": "user", "content": "What is 2+2?"}]
        >>> # result = asyncio.run(complete(msgs))  # requires API key
    """
    provider = get_provider_name()

    if provider == "groq":
        return await _complete_groq(messages, model=model, temperature=temperature)
    if provider == "gemini":
        return await _complete_gemini(messages, model=model, temperature=temperature)
    if provider == "bedrock":
        return await _complete_bedrock(messages, model=model, temperature=temperature)

    if provider in _SUPPORTED_PROVIDERS:
        raise NotImplementedError(
            f"Provider '{provider}' is planned but not yet implemented. "
            f"Use 'groq', 'gemini', or 'bedrock'."
        )

    raise NotImplementedError(
        f"Unknown provider '{provider}'. Supported: {sorted(_SUPPORTED_PROVIDERS)}"
    )


# ── Groq provider ────────────────────────────────────────────────────────

_GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
_GROQ_DEFAULT_MODEL = "llama-3.1-70b-versatile"


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def _complete_groq(
    messages: list[Message],
    *,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Call Groq's OpenAI-compatible chat completions API.

    Args:
        messages: Chat messages.
        model: Model name (defaults to llama-3.1-70b-versatile).
        temperature: Sampling temperature.

    Returns:
        The assistant's response text.

    Raises:
        ProviderError: If the response is malformed.
    """
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise ProviderError("groq", "GROQ_API_KEY environment variable not set")

    payload = {
        "model": model or _GROQ_DEFAULT_MODEL,
        "messages": [dict(m) for m in messages],
        "temperature": temperature,
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            _GROQ_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()

    data = response.json()
    try:
        return str(data["choices"][0]["message"]["content"])
    except (KeyError, IndexError) as exc:
        raise ProviderError("groq", f"Unexpected response format: {data}") from exc


# ── Gemini provider ──────────────────────────────────────────────────────

_GEMINI_URL_TEMPLATE = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
)
_GEMINI_DEFAULT_MODEL = "gemini-2.0-flash"


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def _complete_gemini(
    messages: list[Message],
    *,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Call Google's Gemini generativeLanguage API.

    Args:
        messages: Chat messages (converted to Gemini's content format).
        model: Model name (defaults to gemini-2.0-flash).
        temperature: Sampling temperature.

    Returns:
        The assistant's response text.

    Raises:
        ProviderError: If the response is malformed.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ProviderError("gemini", "GEMINI_API_KEY environment variable not set")

    model_name = model or _GEMINI_DEFAULT_MODEL
    url = _GEMINI_URL_TEMPLATE.format(model=model_name)

    # Convert OpenAI-style messages to Gemini format.
    contents: list[dict[str, object]] = []
    system_instruction: str | None = None
    for msg in messages:
        if msg["role"] == "system":
            system_instruction = msg["content"]
        else:
            role = "user" if msg["role"] == "user" else "model"
            contents.append(
                {
                    "role": role,
                    "parts": [{"text": msg["content"]}],
                }
            )

    payload: dict[str, object] = {
        "contents": contents,
        "generationConfig": {"temperature": temperature},
    }
    if system_instruction:
        payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            url,
            json=payload,
            params={"key": api_key},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

    data = response.json()
    try:
        return str(data["candidates"][0]["content"]["parts"][0]["text"])
    except (KeyError, IndexError) as exc:
        raise ProviderError("gemini", f"Unexpected response format: {data}") from exc


# ── Bedrock provider ─────────────────────────────────────────────────────

_BEDROCK_URL_TEMPLATE = "https://bedrock-runtime.{region}.amazonaws.com/model/{model}/converse"
_BEDROCK_DEFAULT_REGION = "us-east-1"
_BEDROCK_MAX_TOKENS = 256


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def _complete_bedrock(
    messages: list[Message],
    *,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Call Amazon Bedrock's Converse API using a bearer-token API key.

    Bedrock is not OpenAI-compatible: the ``system`` prompt is a top-level
    field (not a message role), each message's content is a list of typed
    blocks, and inference parameters live under ``inferenceConfig``.

    Args:
        messages: Chat messages (converted to Converse format).
        model: Bedrock model / inference-profile ID. Falls back to
            ``DATAFORGE_BEDROCK_MODEL`` when None.
        temperature: Sampling temperature.

    Returns:
        The assistant's response text.

    Raises:
        ProviderError: If the key/model is missing or the response is malformed.
    """
    api_key = os.environ.get("AWS_BEARER_TOKEN_BEDROCK", "")
    if not api_key:
        raise ProviderError("bedrock", "AWS_BEARER_TOKEN_BEDROCK environment variable not set")

    model_name = model or os.environ.get("DATAFORGE_BEDROCK_MODEL", "")
    if not model_name:
        raise ProviderError(
            "bedrock",
            "No model configured. Set DATAFORGE_BEDROCK_MODEL or pass model=.",
        )

    region = os.environ.get("AWS_REGION", _BEDROCK_DEFAULT_REGION)
    url = _BEDROCK_URL_TEMPLATE.format(region=region, model=model_name)

    # Convert OpenAI-style messages to Converse format: system is top-level.
    system_blocks: list[dict[str, str]] = []
    converse_messages: list[dict[str, object]] = []
    for msg in messages:
        if msg["role"] == "system":
            system_blocks.append({"text": msg["content"]})
        else:
            converse_messages.append({"role": msg["role"], "content": [{"text": msg["content"]}]})

    payload: dict[str, object] = {
        "messages": converse_messages,
        "inferenceConfig": {"temperature": temperature, "maxTokens": _BEDROCK_MAX_TOKENS},
    }
    if system_blocks:
        payload["system"] = system_blocks

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()

    data = response.json()
    try:
        return str(data["output"]["message"]["content"][0]["text"])
    except (KeyError, IndexError) as exc:
        raise ProviderError("bedrock", f"Unexpected response format: {data}") from exc
