---
title: DataForge Playground
emoji: 📊
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: false
license: apache-2.0
short_description: Upload a CSV, profile and dry-run-repair it in your browser.
---

# DataForge Playground

API backend for the hosted DataForge Playground. The browser UI is deployed to
Cloudflare Pages; this Space serves the stateless CSV profiling and dry-run
repair endpoints.

**What it does:**

- **Profile**: Detects type mismatches, decimal shifts, and functional
  dependency violations using heuristic detectors.
- **Repair (Dry Run)**: Proposes fixes through the full Safety → Verifier →
  Transaction pipeline, returning an ephemeral transaction journal.

**What it does NOT do:**

- No data is persisted. Your file is processed in memory and discarded.
- No cookies, no analytics of file contents.
- No LLM calls by default (opt-in only, requires a configured key).

## Run locally instead

```bash
python -m pip install -e ".[dev]"
pip install -r playground/api/requirements.txt
uvicorn playground.api.app:app --reload --port 7860
```

## Source

- Main repository: [github.com/Praneshrajan15/data-quality-env](https://github.com/Praneshrajan15/data-quality-env)
- Spec: `specs/SPEC_playground.md`
- License: Apache-2.0
