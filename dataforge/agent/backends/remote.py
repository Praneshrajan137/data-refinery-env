"""Remote model backend for the DataForge verified agent (torch-free).

Drives a hosted Gradio ZeroGPU Space (see ``playground-model/app.py``) over
HTTP, one round-trip per agent step, and exposes the same synchronous
completion signature the hosted and local backends use. This lets a CPU-only
deployment (the playground API) run the real multi-step agent loop against the
trained checkpoint without importing ``torch`` or ``transformers``: the model
lives on the Space, while the safety constitution and SMT verifier run locally
on the caller.

The transport speaks Gradio's REST protocol directly (submit -> poll the
Server-Sent-Events stream) using ``httpx`` (already a core dependency), so no
``gradio_client`` install is required. The Space's ``generate`` endpoint takes
``(messages_json, temperature, max_new_tokens)`` and returns the assistant text.

Environment variables:
    DATAFORGE_REMOTE_MODEL_URL            Base URL of the hosted model Space
                                          (required; e.g. an HF Space URL).
    DATAFORGE_REMOTE_MODEL_TOKEN          Optional bearer token for private Spaces.
    DATAFORGE_REMOTE_MODEL_TIMEOUT        Per-call timeout in seconds (default 60).
    DATAFORGE_REMOTE_MODEL_MAX_NEW_TOKENS Generation cap sent per call (default 384).
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Sequence
from typing import Any

from dataforge.agent.providers import Message

__all__ = [
    "RemoteBackendUnavailableError",
    "RemoteCompletionError",
    "build_remote_completion",
]

# Gradio REST prefixes: Gradio 5 serves under /gradio_api, Gradio 4 under /call.
_GRADIO_PREFIXES = ("/gradio_api/call/", "/call/")
_GENERATE_API = "generate"


class RemoteBackendUnavailableError(RuntimeError):
    """Raised at construction when the remote backend is not configured."""


class RemoteCompletionError(RuntimeError):
    """Raised at call time when a remote completion fails or is malformed."""


def _submit(client: Any, base_url: str, api_name: str, data: list[object]) -> str:
    """POST the call and return the SSE stream URL for its event id."""
    body = {"data": data}
    last_error: str = "no endpoint matched"
    for prefix in _GRADIO_PREFIXES:
        url = f"{base_url}{prefix}{api_name}"
        response = client.post(url, json=body)
        if response.status_code == 404:
            last_error = f"404 at {url}"
            continue
        response.raise_for_status()
        payload = response.json()
        event_id = payload.get("event_id") or payload.get("hash")
        if not event_id:
            raise RemoteCompletionError(f"no event id in submit response: {payload!r}")
        return f"{url}/{event_id}"
    raise RemoteCompletionError(f"{api_name} endpoint not found ({last_error})")


def _parse_sse(text: str) -> str:
    """Extract the completion string from a Gradio SSE response body."""
    event: str | None = None
    last_data: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r")
        if line.startswith("event:"):
            event = line[len("event:") :].strip()
        elif line.startswith("data:"):
            data = line[len("data:") :].strip()
            if event == "error":
                raise RemoteCompletionError(f"remote model reported an error: {data}")
            last_data = data
    if last_data is None:
        raise RemoteCompletionError("remote model returned no data")
    try:
        parsed = json.loads(last_data)
    except json.JSONDecodeError as exc:
        raise RemoteCompletionError(f"unparseable remote response: {last_data!r}") from exc
    if isinstance(parsed, list) and parsed:
        return str(parsed[0])
    if isinstance(parsed, str):
        return parsed
    raise RemoteCompletionError(f"unexpected remote payload shape: {parsed!r}")


def build_remote_completion(
    model: str | None = None,
) -> Callable[[Sequence[Message], str | None, float], str]:
    """Build a synchronous completion callable backed by a hosted model Space.

    Args:
        model: Accepted for signature parity but ignored -- the Space serves a
            fixed checkpoint chosen by its own configuration.

    Returns:
        A callable ``(messages, model_name, temperature) -> str`` compatible
        with :data:`dataforge.agent.policy.CompletionFn`.

    Raises:
        RemoteBackendUnavailableError: If ``DATAFORGE_REMOTE_MODEL_URL`` is unset.
    """
    del model  # The remote Space owns model selection.
    base_url = os.environ.get("DATAFORGE_REMOTE_MODEL_URL", "").strip().rstrip("/")
    if not base_url:
        raise RemoteBackendUnavailableError("DATAFORGE_REMOTE_MODEL_URL is not set")

    token = os.environ.get("DATAFORGE_REMOTE_MODEL_TOKEN", "").strip()
    timeout = float(os.environ.get("DATAFORGE_REMOTE_MODEL_TIMEOUT", "60") or "60")
    max_new_tokens = int(os.environ.get("DATAFORGE_REMOTE_MODEL_MAX_NEW_TOKENS", "384") or "384")
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    def _complete(messages: Sequence[Message], _model_name: str | None, temperature: float) -> str:
        import httpx

        chat = [{"role": message["role"], "content": message["content"]} for message in messages]
        data: list[object] = [json.dumps(chat), float(temperature), max_new_tokens]
        try:
            with httpx.Client(timeout=timeout, headers=headers) as client:
                stream_url = _submit(client, base_url, _GENERATE_API, data)
                response = client.get(stream_url)
                response.raise_for_status()
                return _parse_sse(response.text)
        except httpx.HTTPError as exc:
            raise RemoteCompletionError(f"remote model request failed: {exc}") from exc

    return _complete
