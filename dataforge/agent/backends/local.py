"""Local model backend for the DataForge verified agent (offline by default).

Loads a fine-tuned causal LM (the DataForge GRPO/SFT Qwen checkpoint by
default) with transformers and exposes the same synchronous completion
signature the hosted provider client uses, so a single agent loop drives both
local and hosted policies.

This module is intentionally import-light: heavy dependencies (``torch``,
``transformers``) are imported lazily inside :func:`build_local_completion`,
and the model is loaded once and reused. If the dependencies or the model are
unavailable the loader raises, and the policy factory degrades to the
deterministic policy rather than failing the run.

Environment variables:
    DATAFORGE_AGENT_MODEL   Model id or local path (default: the DataForge
                            0.5B GRPO checkpoint).
    DATAFORGE_AGENT_DEVICE  ``cpu`` / ``cuda`` / ``auto`` (default: ``auto``).
    DATAFORGE_AGENT_MAX_NEW_TOKENS  Generation cap (default: 256).
    HF_HUB_OFFLINE          Honoured by transformers for fully offline use.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from typing import Any

from dataforge.agent.providers import Message

__all__ = ["DEFAULT_LOCAL_MODEL", "build_local_completion"]

DEFAULT_LOCAL_MODEL = "praneshrajan15/DataForge-0.5B-GRPO"

_MODEL_CACHE: dict[str, tuple[Any, Any, str]] = {}


def _resolve_model_id(model: str | None) -> str:
    """Resolve the model id from the argument, env var, or default."""
    if model:
        return model
    return (
        os.environ.get("DATAFORGE_AGENT_MODEL", DEFAULT_LOCAL_MODEL).strip() or DEFAULT_LOCAL_MODEL
    )


def _resolve_device(requested: str | None) -> str:
    """Pick a torch device, honouring DATAFORGE_AGENT_DEVICE then availability."""
    import torch

    choice = (requested or os.environ.get("DATAFORGE_AGENT_DEVICE", "auto")).strip().lower()
    if choice in {"cpu", "cuda"}:
        return choice
    return "cuda" if torch.cuda.is_available() else "cpu"


def _load_model(model_id: str) -> tuple[Any, Any, str]:
    """Load and cache the tokenizer/model, returning (tokenizer, model, device)."""
    cached = _MODEL_CACHE.get(model_id)
    if cached is not None:
        return cached

    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    device = _resolve_device(None)
    tokenizer: Any = AutoTokenizer.from_pretrained(model_id)
    dtype = torch.float16 if device == "cuda" else torch.float32
    model: Any = AutoModelForCausalLM.from_pretrained(model_id, torch_dtype=dtype)
    model.to(device)
    model.eval()

    _MODEL_CACHE[model_id] = (tokenizer, model, device)
    return tokenizer, model, device


def build_local_completion(
    model: str | None = None,
) -> Callable[[Sequence[Message], str | None, float], str]:
    """Build a synchronous completion callable backed by a local model.

    Args:
        model: Optional model id/path override.

    Returns:
        A callable ``(messages, model_name, temperature) -> str`` compatible
        with :data:`dataforge.agent.policy.CompletionFn`.

    Raises:
        ImportError: If transformers/torch are not installed.
        Exception: If the model cannot be loaded (missing weights, offline).
    """
    model_id = _resolve_model_id(model)
    tokenizer, loaded_model, device = _load_model(model_id)

    import torch

    max_new_tokens = int(os.environ.get("DATAFORGE_AGENT_MAX_NEW_TOKENS", "256"))

    def _complete(messages: Sequence[Message], _model_name: str | None, temperature: float) -> str:
        chat = [{"role": m["role"], "content": m["content"]} for m in messages]
        prompt = tokenizer.apply_chat_template(chat, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(prompt, return_tensors="pt").to(device)
        do_sample = temperature > 0.0
        with torch.no_grad():
            output_ids = loaded_model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=do_sample,
                temperature=temperature if do_sample else None,
                pad_token_id=tokenizer.pad_token_id or tokenizer.eos_token_id,
            )
        generated = output_ids[0][inputs["input_ids"].shape[1] :]
        decoded: str = tokenizer.decode(generated, skip_special_tokens=True)
        return decoded

    return _complete
