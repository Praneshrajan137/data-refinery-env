"""Gradio ZeroGPU Space for the DataForge-0.5B checkpoint.

This Space serves two audiences from one loaded checkpoint:

* a **human demo** (`Detect + propose fixes`) that takes a CSV snippet and shows
  what the model proposes, and
* a **stable programmatic API** (`generate`, `health`) that the DataForge
  playground drives, one GPU round-trip per agent step, through the torch-free
  remote policy. The API contract is deliberately small and version-stable:
  `generate(messages_json, temperature, max_new_tokens) -> assistant text`.

The checkpoint defaults to the verified GRPO model
(`Praneshrajan15/DataForge-0.5B-GRPO`); override with `DATAFORGE_SPACE_MODEL_ID`.
Nothing here applies repairs, stores data, or bypasses the DataForge safety and
SMT verification path -- those run on the caller (the playground API or CLI).
"""

from __future__ import annotations

import csv
import io
import json
import os
from collections.abc import Callable
from typing import Any

import gradio as gr

try:
    import spaces
except ImportError:  # pragma: no cover - local development fallback

    class _SpacesFallback:
        """Compatibility shim for non-Space local runs."""

        @staticmethod
        def GPU(  # noqa: N802 - mirrors the Hugging Face spaces API.
            *args: object,
            **kwargs: object,
        ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            """Return an identity decorator when the HF `spaces` package is absent."""
            del args, kwargs

            def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                return func

            return decorator

    spaces = _SpacesFallback()


MODEL_ID = os.environ.get("DATAFORGE_SPACE_MODEL_ID", "Praneshrajan15/DataForge-0.5B-GRPO")
MAX_ROWS = 50
MAX_NEW_TOKENS_CAP = 512
MAX_MESSAGES = 32
MAX_MESSAGE_CHARS = 8000
EXAMPLE_SNIPPETS = [
    "id,amount,department\n1,100,cardiology\n2,105,cardiology\n3,1020,cardiology",
    "id,email,zip\n1,ana@example.com,02139\n2,bob@example.com,2139\n3,chen@example.com,02139",
    "id,room,ward\n1,12A,north\n2,12A,north\n3,99Z,south",
]
TABLE_HEADERS = [
    "status",
    "row",
    "column",
    "issue_type",
    "old_value",
    "new_value",
    "confidence",
    "reason",
]
SYSTEM_PROMPT = (
    "You are DataForge-0.5B. Given a CSV snippet, return JSON only. "
    "Use either a list of repair objects or {'fixes': [...]} with keys row, "
    "column, issue_type, old_value, new_value, confidence, reason. If no repair "
    "is justified, return an empty list."
)

# Loaded once per Space process (populated inside the first GPU call, where CUDA
# is available on ZeroGPU) so multi-step agent loops reuse weights instead of
# re-instantiating the model on every round-trip.
_MODEL_CACHE: dict[str, Any] = {}


def _table_row(
    *,
    status: str,
    row: str = "",
    column: str = "",
    issue_type: str = "",
    old_value: str = "",
    new_value: str = "",
    confidence: str = "",
    reason: str = "",
) -> list[str]:
    """Build one stable output-table row."""
    return [status, row, column, issue_type, old_value, new_value, confidence, reason]


def parse_csv_snippet(csv_snippet: str) -> tuple[bool, str, list[dict[str, str]]]:
    """Parse and validate a CSV snippet submitted to the demo.

    Args:
        csv_snippet: Raw CSV text from the Gradio textbox.

    Returns:
        Tuple of `(ok, message, rows)`. When `ok` is false, `message` is safe to
        show in the UI and `rows` is empty.
    """
    if not csv_snippet.strip():
        return False, "Paste a CSV snippet with a header row and up to 50 data rows.", []

    try:
        reader = csv.DictReader(io.StringIO(csv_snippet))
        if reader.fieldnames is None or not any(name for name in reader.fieldnames):
            return False, "CSV must include a header row.", []
        rows = [dict(row) for row in reader]
    except csv.Error as exc:
        return False, f"CSV could not be parsed: {exc}", []

    if not rows:
        return False, "CSV must include at least one data row.", []
    if len(rows) > MAX_ROWS:
        return False, f"CSV snippet has {len(rows)} rows; the demo accepts at most {MAX_ROWS}.", []
    return True, "CSV accepted.", rows


def _json_candidates(text: str) -> list[Any]:
    """Return JSON payload candidates parsed from a model response."""
    stripped = text.strip()
    candidates: list[Any] = []
    for candidate in (stripped, _extract_json_block(stripped)):
        if not candidate:
            continue
        try:
            candidates.append(json.loads(candidate))
        except json.JSONDecodeError:
            continue
    return candidates


def _extract_json_block(text: str) -> str | None:
    """Extract the outermost JSON-looking block from model text."""
    starts = [index for index in (text.find("["), text.find("{")) if index >= 0]
    if not starts:
        return None
    start = min(starts)
    end = max(text.rfind("]"), text.rfind("}"))
    if end <= start:
        return None
    return text[start : end + 1]


def parse_model_output(model_text: str) -> list[list[str]]:
    """Normalize model output into stable table rows."""
    for payload in _json_candidates(model_text):
        raw_items: Any
        if isinstance(payload, dict):
            raw_items = payload.get("fixes", payload.get("issues", []))
        else:
            raw_items = payload
        if not isinstance(raw_items, list):
            continue
        rows: list[list[str]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            rows.append(
                _table_row(
                    status="proposed",
                    row=str(item.get("row", "")),
                    column=str(item.get("column", "")),
                    issue_type=str(item.get("issue_type", item.get("detector_id", ""))),
                    old_value=str(item.get("old_value", item.get("actual", ""))),
                    new_value=str(item.get("new_value", item.get("expected", ""))),
                    confidence=str(item.get("confidence", "")),
                    reason=str(item.get("reason", "")),
                )
            )
        return rows or [_table_row(status="ok", reason="The model returned no proposed fixes.")]
    preview = model_text.strip().replace("\n", " ")
    if len(preview) > 240:
        preview = preview[:237] + "..."
    return [_table_row(status="raw", reason=preview or "The model returned an empty response.")]


def _coerce_messages(messages_json: str) -> list[dict[str, str]]:
    """Validate and normalize a chat payload for the `generate` API.

    Accepts a JSON array of `{"role", "content"}` objects, a `{"messages": [...]}`
    wrapper, or a bare string (treated as a single user turn). Roles are clamped
    to the chat set and content is length-capped so a single call cannot exhaust
    the GPU budget.
    """
    raw = messages_json.strip()
    if not raw:
        raise ValueError("messages payload is empty")
    try:
        parsed: Any = json.loads(raw)
    except json.JSONDecodeError:
        parsed = [{"role": "user", "content": raw}]
    if isinstance(parsed, dict):
        parsed = parsed.get("messages", [parsed])
    if not isinstance(parsed, list) or not parsed:
        raise ValueError("messages must be a non-empty list")
    if len(parsed) > MAX_MESSAGES:
        raise ValueError(f"too many messages ({len(parsed)} > {MAX_MESSAGES})")
    out: list[dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise ValueError("each message must be a JSON object")
        role = str(item.get("role", "user"))
        if role not in {"system", "user", "assistant"}:
            role = "user"
        content = str(item.get("content", ""))
        if len(content) > MAX_MESSAGE_CHARS:
            content = content[:MAX_MESSAGE_CHARS]
        out.append({"role": role, "content": content})
    return out


def _load_model() -> tuple[Any, Any]:
    """Load (and cache) the tokenizer and model for this Space process."""
    if "model" not in _MODEL_CACHE:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        model_kwargs: dict[str, Any] = {}
        if torch.cuda.is_available():
            model_kwargs["torch_dtype"] = torch.float16
        model = AutoModelForCausalLM.from_pretrained(MODEL_ID, **model_kwargs)
        _MODEL_CACHE["tokenizer"] = tokenizer
        _MODEL_CACHE["model"] = model
    return _MODEL_CACHE["tokenizer"], _MODEL_CACHE["model"]


def _run_chat(
    messages: list[dict[str, str]],
    *,
    temperature: float,
    max_new_tokens: int,
) -> str:
    """Run a chat completion against the loaded checkpoint and return the text."""
    import torch

    tokenizer, model = _load_model()
    if torch.cuda.is_available():
        model = model.to("cuda")
    device = next(model.parameters()).device

    try:
        input_ids = tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
        ).to(device)
    except Exception:
        prompt = (
            "\n".join(f"{message['role']}: {message['content']}" for message in messages)
            + "\nassistant:"
        )
        input_ids = tokenizer(prompt, return_tensors="pt").input_ids.to(device)

    gen_kwargs: dict[str, Any] = {
        "max_new_tokens": max_new_tokens,
        "pad_token_id": tokenizer.eos_token_id,
    }
    if temperature and temperature > 0:
        gen_kwargs["do_sample"] = True
        gen_kwargs["temperature"] = temperature
    else:
        gen_kwargs["do_sample"] = False

    outputs = model.generate(input_ids=input_ids, **gen_kwargs)
    generated = outputs[0][input_ids.shape[-1] :]
    text = tokenizer.decode(generated, skip_special_tokens=True)

    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return str(text)


def _generate_model_text(csv_snippet: str) -> str:
    """Run the checkpoint on a CSV snippet for the human demo path."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"CSV:\n{csv_snippet.strip()}\n\nJSON:"},
    ]
    return _run_chat(messages, temperature=0.0, max_new_tokens=384)


@spaces.GPU(duration=60)
def detect_and_propose(csv_snippet: str) -> list[list[str]]:
    """Detect data-quality issues and propose fixes for a CSV snippet."""
    ok, message, _rows = parse_csv_snippet(csv_snippet)
    if not ok:
        return [_table_row(status="error", reason=message)]
    try:
        model_text = _generate_model_text(csv_snippet)
    except Exception as exc:
        return [_table_row(status="error", reason=f"Model inference failed: {exc}")]
    return parse_model_output(model_text)


def detect_and_propose_with_status(csv_snippet: str) -> tuple[list[list[str]], str]:
    """Return model proposals plus an honest demo-status message."""
    rows = detect_and_propose(csv_snippet)
    first_status = rows[0][0] if rows else "raw"
    if first_status == "error":
        return rows, "Input rejected or inference failed. The verified playground path remains Profile -> Repair -> Verify -> Revert."
    if first_status == "raw":
        return rows, "The checkpoint returned unstructured text. Treat this as research output, not a verified repair."
    if first_status == "ok":
        return rows, "The checkpoint proposed no fixes for this snippet."
    return rows, f"Experimental checkpoint returned {len(rows)} proposed fix row(s). Verify repairs with the CLI or playground API before trusting them."


@spaces.GPU(duration=60)
def generate(
    messages_json: str,
    temperature: float = 0.0,
    max_new_tokens: float = 384,
) -> str:
    """Stable chat-completion endpoint driven by the DataForge agent loop.

    Args:
        messages_json: JSON array of `{"role", "content"}` chat messages (or a
            bare string treated as a single user turn).
        temperature: Sampling temperature; `<= 0` selects greedy decoding so the
            agent's deterministic floor stays reproducible.
        max_new_tokens: Requested generation cap, clamped to `MAX_NEW_TOKENS_CAP`.

    Returns:
        The assistant text completion with the chat scaffolding removed.

    Raises:
        gr.Error: If the payload is invalid or inference fails, so remote callers
            observe a clear transport-level error and can degrade gracefully.
    """
    try:
        messages = _coerce_messages(str(messages_json))
    except ValueError as exc:
        raise gr.Error(f"invalid messages payload: {exc}") from exc
    capped = max(1, min(int(max_new_tokens), MAX_NEW_TOKENS_CAP))
    try:
        return _run_chat(messages, temperature=float(temperature), max_new_tokens=capped)
    except Exception as exc:  # pragma: no cover - surfaced to the remote caller
        raise gr.Error(f"inference failed: {exc}") from exc


def health() -> str:
    """Return a JSON capability descriptor for the remote policy (no GPU)."""
    return json.dumps(
        {
            "status": "ok",
            "model_id": MODEL_ID,
            "max_new_tokens_cap": MAX_NEW_TOKENS_CAP,
            "max_messages": MAX_MESSAGES,
            "api": ["generate", "health"],
        }
    )


with gr.Blocks(title="DataForge 0.5B") as demo:
    gr.Markdown(
        """
# DataForge 0.5B (GRPO)

Experimental model demo for short CSV snippets, serving the verified GRPO
checkpoint. This Space shows what the checkpoint proposes and exposes a stable
`generate` API for the DataForge playground agent; it does not apply repairs,
store data, or replace the verified DataForge workflow.

**Use the product path for evidence:** Profile -> Repair -> Verify -> Revert
in the CLI or playground. Safety filtering and SMT verification run on the
caller, not here. This model surface is intentionally bounded to 50 rows, one
queued inference at a time, and research-grade outputs (GRPO correction F1 is
low; treat proposals as unverified until the caller checks them).
"""
    )
    with gr.Row():
        with gr.Column(scale=2):
            csv_input = gr.Textbox(
                label="CSV snippet",
                lines=14,
                max_lines=20,
                placeholder="id,amount\n1,100\n2,105\n3,1020",
            )
            gr.Examples(
                examples=EXAMPLE_SNIPPETS,
                inputs=csv_input,
                label="Audited examples",
            )
            run_button = gr.Button("Detect + propose fixes", variant="primary")
        with gr.Column(scale=3):
            output = gr.Dataframe(
                headers=TABLE_HEADERS,
                datatype=["str"] * len(TABLE_HEADERS),
                row_count=1,
                column_count=len(TABLE_HEADERS),
                label="Model output",
            )
            status_output = gr.Markdown("Waiting for a CSV snippet.")
    run_button.click(
        detect_and_propose_with_status,
        inputs=csv_input,
        outputs=[output, status_output],
        show_progress="full",
        concurrency_limit=1,
    )

    with gr.Accordion("Agent API (programmatic)", open=False):
        gr.Markdown(
            "These endpoints back the DataForge playground agent. `generate` "
            "takes a JSON chat payload and returns the assistant text; `health` "
            "reports the served model id and caps. They are stable API names; "
            "the UI controls below are for manual inspection only."
        )
        messages_input = gr.Textbox(
            label="messages (JSON)",
            lines=6,
            value='[{"role": "user", "content": "Return an empty JSON list: []"}]',
        )
        with gr.Row():
            temperature_input = gr.Number(label="temperature", value=0.0)
            max_new_tokens_input = gr.Number(label="max_new_tokens", value=384)
        generate_button = gr.Button("generate")
        generate_output = gr.Textbox(label="completion", lines=6)
        generate_button.click(
            generate,
            inputs=[messages_input, temperature_input, max_new_tokens_input],
            outputs=generate_output,
            api_name="generate",
            concurrency_limit=1,
        )
        health_button = gr.Button("health")
        health_output = gr.Textbox(label="health", lines=3)
        health_button.click(health, inputs=None, outputs=health_output, api_name="health")

demo.queue(max_size=8, default_concurrency_limit=1)


if __name__ == "__main__":
    demo.launch()
