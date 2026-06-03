"""Local Hugging Face Transformers adapter for dataforge-evals."""

from __future__ import annotations

import os
from typing import Any, cast

from dataforge_evals.agents.base import AgentRunResult, Fix, Task, Usage
from dataforge_evals.agents.provider_base import ProviderError
from dataforge_evals.repair_contract import parse_repair_action, render_repair_messages

DEFAULT_HF_MODEL_ID = "Praneshrajan15/DataForge-0.5B-SFT"
LOCAL_INFERENCE_ENV = "DATAFORGE_ALLOW_LOCAL_MODEL_INFERENCE"


def resolve_default_model_id() -> str:
    """Resolve the default HF model id for local SFT evaluation."""
    configured = os.environ.get("DATAFORGE_EVAL_MODEL", "").strip()
    if configured:
        return configured
    token = os.environ.get("HF_TOKEN", "").strip()
    if token:
        try:
            from huggingface_hub import HfApi  # type: ignore[import-not-found]

            whoami = HfApi(token=token).whoami(token=token)
            name = whoami.get("name") if isinstance(whoami, dict) else None
            if isinstance(name, str) and name:
                return f"{name}/DataForge-0.5B-SFT"
        except Exception:
            pass
    return DEFAULT_HF_MODEL_ID


class HfLocalAgent:
    """Evaluate a local or Hub-hosted causal LM through Transformers.

    The model and tokenizer are loaded lazily inside ``run`` so the adapter can
    be constructed by the CLI without immediately downloading weights. This
    also keeps the object pickle-light for the harness worker process.
    """

    name = "hf-local"

    def __init__(
        self,
        *,
        model_id: str | None = None,
        max_new_tokens: int = 384,
        device: str = "auto",
        tokenizer: Any | None = None,
        model: Any | None = None,
    ) -> None:
        """Initialize the local HF adapter."""
        if max_new_tokens < 1:
            raise ValueError("max_new_tokens must be >= 1")
        self.model_id = model_id or resolve_default_model_id()
        self.max_new_tokens = max_new_tokens
        self.device = device
        self._tokenizer = tokenizer
        self._model = model

    def _messages(self, task: Task) -> list[dict[str, str]]:
        """Build the chat prompt using the same repair contract as provider adapters."""
        target_rows = []
        for row_index, (_, row) in enumerate(task.dirty_df.iterrows()):
            rendered_row = {"_row": str(row_index)}
            for column in task.canonical_columns:
                rendered_row[str(column)] = str(row[column])
            target_rows.append(rendered_row)
        return render_repair_messages(
            schema_summary={
                "dataset": task.name,
                "columns": list(task.canonical_columns),
                "rows": len(task.dirty_df.index),
                "split": "eval",
            },
            target_rows=target_rows,
            context_rows=[],
            allowed_columns=task.canonical_columns,
            metadata=task.metadata,
            repairs=None,
        )

    def _load(self) -> tuple[Any, Any]:
        """Load tokenizer and model lazily."""
        if self._tokenizer is not None and self._model is not None:
            return self._tokenizer, self._model
        if os.environ.get(LOCAL_INFERENCE_ENV) != "1":
            raise ProviderError(
                "Local Hugging Face model inference is disabled for this workspace. "
                f"Set {LOCAL_INFERENCE_ENV}=1 only in an approved remote runtime.",
                provider="hf-local",
            )
        try:
            import torch  # type: ignore[import-not-found]
            from transformers import (  # type: ignore[import-not-found]
                AutoModelForCausalLM,
                AutoTokenizer,
            )
        except ImportError as exc:
            raise ProviderError(
                "hf-local requires transformers and torch. Install dataforge_07_evals[hf].",
                provider="hf-local",
            ) from exc

        token = os.environ.get("HF_TOKEN") or None
        tokenizer = AutoTokenizer.from_pretrained(self.model_id, token=token)
        dtype = torch.float32 if self.device == "cpu" else torch.float16
        model_kwargs: dict[str, Any] = {"torch_dtype": dtype, "token": token}
        if self.device == "auto":
            model_kwargs["device_map"] = "auto"
        model = cast(Any, AutoModelForCausalLM.from_pretrained(self.model_id, **model_kwargs))
        if self.device != "auto":
            model = model.to(self.device)
        model.eval()
        self._tokenizer = tokenizer
        self._model = model
        return tokenizer, model

    def _prompt(self, tokenizer: Any, messages: list[dict[str, str]]) -> str:
        """Serialize chat messages using the tokenizer chat template when available."""
        if hasattr(tokenizer, "apply_chat_template"):
            return str(
                tokenizer.apply_chat_template(
                    messages,
                    tokenize=False,
                    add_generation_prompt=True,
                )
            )
        return "\n\n".join(f"{message['role']}: {message['content']}" for message in messages)

    def _complete(self, prompt: str) -> tuple[str, Usage]:
        """Generate completion text and local usage accounting."""
        if self._model is not None and hasattr(self._model, "complete"):
            text = str(self._model.complete(prompt))
            return text, Usage(
                calls=0, prompt_tokens=len(prompt.split()), completion_tokens=len(text.split())
            )

        tokenizer, model = self._load()
        try:
            import torch
        except ImportError as exc:  # pragma: no cover - guarded by _load
            raise ProviderError("torch is required for hf-local.", provider="hf-local") from exc

        inputs = tokenizer(prompt, return_tensors="pt")
        if self.device != "auto":
            inputs = {
                key: value.to(self.device) if hasattr(value, "to") else value
                for key, value in inputs.items()
            }
        with torch.no_grad():
            output_ids = model.generate(
                **inputs,
                max_new_tokens=self.max_new_tokens,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
            )
        input_length = int(inputs["input_ids"].shape[-1])
        generated = output_ids[0][input_length:]
        text = str(tokenizer.decode(generated, skip_special_tokens=True))
        usage = Usage(
            calls=0,
            prompt_tokens=input_length,
            completion_tokens=int(generated.shape[-1]),
            quota_units=0.0,
        )
        return text, usage

    def _parse_fixes(self, text: str) -> list[Fix]:
        """Parse model JSON output into validated fixes."""
        parsed = parse_repair_action(text)
        if not parsed.ok or parsed.action is None:
            if parsed.error_kind == "truncated_json":
                raise ProviderError(
                    f"HF model returned truncated JSON fixes: {text[:200]}",
                    provider="hf-local",
                )
            if parsed.error_kind == "parse_failure":
                raise ProviderError(
                    f"HF model returned non-JSON fixes: {text[:200]}",
                    provider="hf-local",
                )
            raise ProviderError(
                f"HF model returned invalid repair JSON: {parsed.error_message}",
                provider="hf-local",
            )
        return [
            Fix(
                row=repair.row,
                column=repair.column,
                new_value=repair.new_value,
                reason=repair.reason if repair.reason != "repair proposal" else "hf-local proposal",
            )
            for repair in parsed.action.repairs
        ]

    def run(self, task: Task) -> AgentRunResult:
        """Run local generation and return proposed fixes plus usage."""
        tokenizer, _model = self._load() if self._model is None else (self._tokenizer, self._model)
        prompt = self._prompt(tokenizer, self._messages(task))
        text, usage = self._complete(prompt)
        return AgentRunResult(
            fixes=self._parse_fixes(text),
            usage=usage,
            steps=1,
            model=self.model_id,
        )
