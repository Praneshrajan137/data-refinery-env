"""Multi-provider LLM client for DataForge.

Reads ``DATAFORGE_LLM_PROVIDER`` from the environment and dispatches to the
matching provider.  Implemented providers are **groq**, **gemini**, **bedrock**
(Amazon Bedrock Converse API, bearer-token auth), and **azure** (Azure OpenAI,
first-party, ``api-key`` auth); other providers raise ``NotImplementedError``.

Azure OpenAI is "sold directly by Azure" (first-party), so it is billed against
the subscription and works on free-trial credit. Anthropic Claude on Foundry is
a third-party Marketplace SaaS offer that requires pay-as-you-go billing and a
different (Anthropic Messages) endpoint; requesting a Claude model through this
first-party path fails fast with an actionable message.

No LLM calls are made by detectors — this module is for the agent loop
(Week 2+) and is stubbed here to establish the interface.

The interface is:
    ``async def complete(messages, model, temperature) -> str``
"""

from __future__ import annotations

import os
from dataclasses import dataclass
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


def _env_truthy(name: str) -> bool:
    """Return whether an env var is set to a truthy value (1/true/yes/on)."""
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


_SUPPORTED_PROVIDERS = frozenset(
    {"groq", "gemini", "bedrock", "azure", "grok", "cerebras", "openrouter", "hf", "cloudflare"}
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
    if os.environ.get("AZURE_API_KEY"):
        return "azure"
    if os.environ.get("XAI_API_KEY"):
        return "grok"
    if os.environ.get("CEREBRAS_API_KEY"):
        return "cerebras"
    return "groq"


# Environment overrides for the per-provider model, mirroring the bench runner
# naming so a user can bring their own model with one variable.
_PROVIDER_MODEL_ENV = {
    "groq": "DATAFORGE_GROQ_MODEL",
    "gemini": "DATAFORGE_GEMINI_MODEL",
    "bedrock": "DATAFORGE_BEDROCK_MODEL",
    "azure": "DATAFORGE_AZURE_MODEL",
    "grok": "DATAFORGE_GROK_MODEL",
    "cerebras": "DATAFORGE_CEREBRAS_MODEL",
}


def resolve_model(provider: str | None = None) -> str:
    """Resolve the effective model id for a provider from env, else its default.

    This is the single source of truth for "which model" when no explicit model
    is passed: it lets a user set ``DATAFORGE_<PROVIDER>_MODEL`` and have it apply
    everywhere in the product (agent policy and LLM repairers), not just the bench.

    Args:
        provider: Provider name; defaults to the active provider from the env.

    Returns:
        The model id from ``DATAFORGE_<PROVIDER>_MODEL`` if set, otherwise the
        provider's built-in default (empty string for bedrock, which has no default).
    """
    name = (provider or get_provider_name()).strip().lower()
    env_var = _PROVIDER_MODEL_ENV.get(name)
    override = os.environ.get(env_var, "").strip() if env_var else ""
    if override:
        return override
    if name in _OPENAI_COMPAT_CONFIG:
        return _OPENAI_COMPAT_CONFIG[name].default_model
    if name == "gemini":
        return _GEMINI_DEFAULT_MODEL
    if name in ("bedrock", "azure"):
        # No built-in default: the Bedrock model ID / Azure deployment name is
        # account-specific and must be provided by the user.
        return ""
    return _GROQ_DEFAULT_MODEL


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

    if provider in _OPENAI_COMPAT_CONFIG:
        return await _complete_openai_compat(
            messages, provider=provider, model=model, temperature=temperature
        )
    if provider == "gemini":
        return await _complete_gemini(messages, model=model, temperature=temperature)
    if provider == "bedrock":
        return await _complete_bedrock(messages, model=model, temperature=temperature)
    if provider == "azure":
        return await _complete_azure(messages, model=model, temperature=temperature)

    if provider in _SUPPORTED_PROVIDERS:
        raise NotImplementedError(
            f"Provider '{provider}' is planned but not yet implemented. "
            f"Use 'groq', 'grok', 'cerebras', 'gemini', 'bedrock', or 'azure'."
        )

    raise NotImplementedError(
        f"Unknown provider '{provider}'. Supported: {sorted(_SUPPORTED_PROVIDERS)}"
    )


# ── OpenAI-compatible providers (groq, grok/xAI, cerebras) ────────────────
#
# These providers share one wire format: a Bearer-token POST to a
# /chat/completions endpoint with {model, messages, temperature}. A single
# generic path serves all of them; adding another is one config entry.

_GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
_GROQ_DEFAULT_MODEL = "llama-3.1-70b-versatile"


@dataclass(frozen=True, slots=True)
class _OpenAICompatConfig:
    """Wire config for one OpenAI-compatible provider."""

    url: str
    api_key_env: str
    default_model: str


_OPENAI_COMPAT_CONFIG: dict[str, _OpenAICompatConfig] = {
    "groq": _OpenAICompatConfig(
        url=_GROQ_URL, api_key_env="GROQ_API_KEY", default_model=_GROQ_DEFAULT_MODEL
    ),
    # xAI Grok is OpenAI-compatible (base https://api.x.ai/v1, Bearer auth).
    # NOTE: "grok" (xAI model) is NOT the repo's "groq" provider (Groq Inc.).
    "grok": _OpenAICompatConfig(
        url="https://api.x.ai/v1/chat/completions",
        api_key_env="XAI_API_KEY",
        default_model="grok-4.5",
    ),
    "cerebras": _OpenAICompatConfig(
        url="https://api.cerebras.ai/v1/chat/completions",
        api_key_env="CEREBRAS_API_KEY",
        default_model="qwen-3-235b-a22b-instruct-2507",
    ),
}


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def _complete_openai_compat(
    messages: list[Message],
    *,
    provider: str,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Call any OpenAI-compatible chat completions API (groq, grok, cerebras).

    Args:
        messages: Chat messages.
        provider: One of the keys in ``_OPENAI_COMPAT_CONFIG``.
        model: Model name (defaults to the provider's configured default).
        temperature: Sampling temperature.

    Returns:
        The assistant's response text.

    Raises:
        ProviderError: If the API key is missing or the response is malformed.
    """
    config = _OPENAI_COMPAT_CONFIG[provider]
    api_key = os.environ.get(config.api_key_env, "")
    if not api_key:
        raise ProviderError(provider, f"{config.api_key_env} environment variable not set")

    payload = {
        "model": model or resolve_model(provider),
        "messages": [dict(m) for m in messages],
        "temperature": temperature,
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            config.url,
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
        raise ProviderError(provider, f"Unexpected response format: {data}") from exc


async def _complete_groq(
    messages: list[Message],
    *,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Backward-compatible Groq entry point; delegates to the generic path."""
    return await _complete_openai_compat(
        messages, provider="groq", model=model, temperature=temperature
    )


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

    model_name = model or resolve_model("gemini")
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


# ── Azure OpenAI provider ────────────────────────────────────────────────

_AZURE_DEFAULT_API_VERSION = "2025-04-01-preview"
_AZURE_MAX_TOKENS = 512
_AZURE_DEFAULT_TIMEOUT_S = 60.0


def _azure_max_tokens() -> int:
    """Return the Azure completion-token budget from env, else the default.

    GPT-5 / reasoning deployments spend hidden reasoning tokens against this
    budget, so a too-small cap yields empty content. ``DATAFORGE_AZURE_MAX_TOKENS``
    lets callers raise it (the runbook sets 2048).
    """
    raw = os.environ.get("DATAFORGE_AZURE_MAX_TOKENS", "").strip()
    if not raw:
        return _AZURE_MAX_TOKENS
    try:
        value = int(raw)
    except ValueError:
        return _AZURE_MAX_TOKENS
    return value if value > 0 else _AZURE_MAX_TOKENS


def _azure_timeout_s() -> float:
    """Return the Azure request timeout (seconds) from env, else the default.

    Long teacher/corrector runs against reasoning deployments can exceed the
    60s default on a slow chunk; ``DATAFORGE_AZURE_TIMEOUT_S`` lets callers raise
    it so a single slow request does not abort the run. The bench client already
    honours this env var; the agent path now matches it.
    """
    raw = os.environ.get("DATAFORGE_AZURE_TIMEOUT_S", "").strip()
    if not raw:
        return _AZURE_DEFAULT_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        return _AZURE_DEFAULT_TIMEOUT_S
    return value if value > 0 else _AZURE_DEFAULT_TIMEOUT_S


# Anthropic Claude on Microsoft Foundry is a third-party Marketplace SaaS offer:
# it requires a pay-as-you-go subscription (blocked on free-trial / credit-only
# accounts) and uses the Anthropic Messages API, not this first-party Azure
# OpenAI chat/completions surface. Fail fast with an honest message rather than
# emit a confusing 400/404 from the wrong endpoint.
_AZURE_MARKETPLACE_MARKERS = ("claude", "sonnet", "opus", "haiku")


def _azure_marketplace_guard(model_name: str) -> None:
    """Reject Anthropic/Marketplace models on the first-party Azure OpenAI path."""
    lowered = model_name.lower()
    if any(marker in lowered for marker in _AZURE_MARKETPLACE_MARKERS):
        raise ProviderError(
            "azure",
            f"Model '{model_name}' looks like an Anthropic Claude model. On "
            "Microsoft Foundry, Claude is a third-party Marketplace offer that "
            "requires a pay-as-you-go Azure subscription (it is unavailable on "
            "free-trial or credit-only accounts) and is served via the Anthropic "
            "Messages API, not this first-party Azure OpenAI path. Deploy a "
            "first-party Azure OpenAI model (e.g. a GPT-5 family deployment) and "
            "set DATAFORGE_AZURE_MODEL to its deployment name, or upgrade to "
            "pay-as-you-go to use Claude.",
        )


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def _complete_azure(
    messages: list[Message],
    *,
    model: str | None = None,
    temperature: float = 0.0,
) -> str:
    """Call an Azure OpenAI deployment's chat/completions API.

    Azure OpenAI is OpenAI-compatible (system stays a message role), but the
    model is addressed by *deployment name* in the URL path, authentication is
    via an ``api-key`` header, and the ``api-version`` is a query parameter.

    GPT-5 / reasoning deployments reject ``temperature != 1`` and require
    ``max_completion_tokens`` instead of ``max_tokens``. To stay compatible by
    default, temperature is omitted unless ``DATAFORGE_AZURE_SEND_TEMPERATURE``
    is truthy, and ``max_completion_tokens`` is always used.

    Args:
        messages: Chat messages (sent as-is; system is a normal role).
        model: Azure deployment name. Falls back to ``DATAFORGE_AZURE_MODEL``.
        temperature: Sampling temperature (only sent when opted in).

    Returns:
        The assistant's response text.

    Raises:
        ProviderError: If the key/endpoint/deployment is missing, the model is a
            Marketplace (Anthropic) model, or the response is malformed.
    """
    api_key = os.environ.get("AZURE_API_KEY", "")
    if not api_key:
        raise ProviderError("azure", "AZURE_API_KEY environment variable not set")

    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "").strip()
    if not endpoint:
        raise ProviderError(
            "azure",
            "AZURE_OPENAI_ENDPOINT is not set. Set it to your Azure OpenAI "
            "resource endpoint, e.g. https://<resource>.openai.azure.com.",
        )

    model_name = model or os.environ.get("DATAFORGE_AZURE_MODEL", "")
    if not model_name:
        raise ProviderError(
            "azure",
            "No Azure deployment configured. Set DATAFORGE_AZURE_MODEL to your "
            "deployment name or pass model=.",
        )
    _azure_marketplace_guard(model_name)

    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", _AZURE_DEFAULT_API_VERSION)
    url = f"{endpoint.rstrip('/')}/openai/deployments/{model_name}/chat/completions"

    payload: dict[str, object] = {
        "messages": [dict(m) for m in messages],
        "max_completion_tokens": _azure_max_tokens(),
    }
    effort = os.environ.get("DATAFORGE_AZURE_REASONING_EFFORT", "").strip().lower()
    if effort:
        payload["reasoning_effort"] = effort
    if _env_truthy("DATAFORGE_AZURE_SEND_TEMPERATURE"):
        payload["temperature"] = temperature

    async with httpx.AsyncClient(timeout=_azure_timeout_s()) as client:
        response = await client.post(
            url,
            json=payload,
            params={"api-version": api_version},
            headers={"api-key": api_key, "Content-Type": "application/json"},
        )
        response.raise_for_status()

    data = response.json()
    try:
        return str(data["choices"][0]["message"]["content"])
    except (KeyError, IndexError) as exc:
        raise ProviderError("azure", f"Unexpected response format: {data}") from exc
