---
title: DataForge 0.5B GRPO
sdk: gradio
app_file: app.py
license: apache-2.0
models:
  - Praneshrajan15/DataForge-0.5B-GRPO
  - Praneshrajan15/DataForge-0.5B-SFT
tags:
  - data-quality
  - tabular-data
  - gradio
  - zerogpu
---

# DataForge 0.5B (GRPO)

This Space serves `Praneshrajan15/DataForge-0.5B-GRPO`, the GRPO checkpoint from
the DataForge tabular-repair training path (override with the
`DATAFORGE_SPACE_MODEL_ID` Space variable). It powers two surfaces from one
loaded checkpoint:

1. **Human demo** -- paste a CSV snippet (header row, up to 50 data rows) and run
   **Detect + propose fixes**. The model returns proposed issue/fix rows when it
   can parse the task.
2. **Programmatic agent API** -- the DataForge playground drives this Space one
   GPU round-trip per agent step through a torch-free remote policy.

The checkpoint is research-grade evidence that the DataForge training, merge,
evaluation, and publish path works. Its correction F1 is low; it is **not** a
production quality claim. Safety filtering and SMT verification run on the
caller (the playground API or CLI), never inside this Space.

## Programmatic API

Two stable, version-pinned endpoints (see the "Agent API" accordion in the UI):

- `generate(messages_json, temperature, max_new_tokens) -> completion text`
  where `messages_json` is a JSON array of `{"role", "content"}` chat turns.
  `temperature <= 0` selects greedy decoding; `max_new_tokens` is clamped to a
  fixed cap. Invalid payloads and inference failures surface as a Gradio error
  so remote callers can degrade gracefully.
- `health() -> JSON` reporting the served `model_id` and caps.

## ZeroGPU setup

Create a Hugging Face Space with the Gradio SDK and select ZeroGPU in the Space
settings. Hugging Face's current ZeroGPU documentation describes Gradio-only
dynamic GPU allocation backed by shared RTX Pro 6000 Blackwell capacity. Queue
priority and daily quota depend on the visitor's account tier, so public demo
and agent calls can occasionally wait or fail when quota is exhausted.

The Space loads model weights from the Hugging Face Hub with `from_pretrained()`
and caches them for the process so multi-step agent loops reuse the weights.
Model weights, generated caches, and user CSV snippets are not committed to this
repository.

## Limitations

- Inputs are capped at 50 rows (demo) and a fixed message/token budget (API).
- The model may emit malformed JSON or propose incorrect fixes.
- Do not use this demo for autonomous production data modification.
- Run real DataForge repairs through the CLI, MCP server, or playground so
  safety, verification, and transaction logging remain in the loop.
