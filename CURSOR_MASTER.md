# DataForge — The Cursor AI Context & Prompt Pack

The single source of truth that a disciplined coding agent (Cursor / Claude / any IDE agent) uses to build DataForge end-to-end — and that a senior reviewer uses to decide in ten minutes whether the author knows what they are doing.

Every line in this document is either (a) context the agent must internalize, (b) a constraint it must obey, or (c) a prompt it will execute. Distribution, career narrative, and marketing copy are intentionally absent — they are the author's responsibility, not the agent's.

**Target audience this document serves, in order of priority:**

1. The coding agent (which reads it on every session).
2. A hire-bar staff engineer at Anthropic / Databricks / HuggingFace / dbt Labs reviewing the repo for 10 minutes.
3. The author's future self, six months from now, re-reading before a major change.

**How to use this file:**

1. Drop it into your repo at `CURSOR_MASTER.md`.
2. Copy the `.cursor/rules/dataforge.md` block (Section 1) into `.cursor/rules/dataforge.md`.
3. Copy the `CLAUDE.md` block (Section 2.5) into the repo root.
4. Read `META_CONTEXT.md` in full — especially Section 0.6 (Pre-mortem) and Section 0.7 (Honest competitive positioning) — before writing a single line of code.
5. Work through the 13-week plan (Section 5).
6. Before every public release, run the ship rubric in Section 7 AND the pre-mortem checklist in Section 7.5.

**Document version:** v2.0 (2026-04-18).

**Epistemic status.** Several claims in this document are load-bearing facts about external systems (free-tier limits, model release dates, library versions). Those facts drift. Every such claim is dated "as of April 2026"; when a number here disagrees with the provider's current documentation, the provider wins and this doc is updated in the same PR.

---

## Section 0 — Meta-context

See `META_CONTEXT.md` — the definitive, expanded meta-context document covering: project identity (0.1), target users (0.2), quality bar with ship/judgment/safety/product signals (0.3), ten non-negotiables (0.4), Cursor anti-patterns (0.5), ten pre-mortem failure modes with mitigations (0.6), and honest competitive positioning (0.7). Read it in full before writing any code.

---

## Section 1 — The .cursor/rules/dataforge.md file

See `.cursor/rules/dataforge.md` — placed verbatim from the spec.

---

## Section 2 — Project-level context files

### 2.1 README.md skeleton

The shipped public README is `README.md` (kept honest and minimal for Week 0). `README_DATAFORGE.md` is reference skeleton material describing the target structure for a future release — do not promote its aspirational claims into `README.md` until the corresponding features ship.

### 2.2 ARCHITECTURE.md skeleton

See `ARCHITECTURE.md` — layered structure (L1–L6), dependency justification table, and data-flow walkthrough.

### 2.3 DECISIONS.md skeleton

See `DECISIONS.md` — Cursor is responsible for appending an entry every time a non-trivial choice is made. Minimum: one entry per week.

### 2.4 specs/SPEC_TEMPLATE.md

See `specs/SPEC_TEMPLATE.md` — every module's spec is a fork of this template. Do not start code without it.

### 2.5 CLAUDE.md (living knowledge base for Cursor)

See `CLAUDE.md` — Cursor reads this at session start. Append to it every time you learn something the hard way (a failing test that took an hour to diagnose, a subtle API gotcha, a performance cliff).

---

## Section 3 — Canonical file structure

See `FILE_STRUCTURE.md` — the full target directory tree. Cursor: create this tree on Day 0.

Sister repos (separate GitHub repos, separate PyPI packages — create after the CLI launches):

- `dataforge-evals` — standalone evaluation harness.
- `dataforge-agent-patterns` — reusable agent primitives library.
- `dataforge-dbt` — dbt adapter.
- `dataforge-airbyte` — Airbyte source connector.
- `dataforge-mcp` — MCP server wrapping the CLI.

---

## Section 4 — Engineering principles operationalized

### 4.1 pyproject.toml

See `pyproject.toml` — the authoritative package configuration. Free-tier-friendly dependency set with justified alternatives.

### 4.2 Makefile

See `Makefile` — one-command dev loop: `make lint`, `make format`, `make type`, `make test`, `make test-mapped`, `make coverage`, `make bench`, `make mutation`, `make clean`. Uses `ruff` for both linting and formatting (no `black` dependency).

### 4.3 test_map.json + scripts/test_mapped.py

See `test_map.json` — AST-derived dependency map (source file → relevant tests). TDAD-driven (Rehan 2026). Grow this as modules ship — Cursor must update it in the same PR as the source file.

See `scripts/test_mapped.py` — runs only the tests mapped to a given source file.

### 4.4 .github/workflows/ci.yml

See `.github/workflows/ci.yml` — lint + type + test on every PR, regression gate job. Free for public repos.

### 4.5 ralph.sh and prompt.template.md

See `ralph.sh` — autonomous Cursor iteration loop. Use when you want to let Cursor grind through a spec's task list without supervision.

See `prompt.template.md` — source template for the Ralph prompt. `$SPEC_PATH` is injected at runtime.

### 4.6 The eight-step Cursor pipeline

Use this mentally for every change:

1. Read the spec. If no spec, write it first (see `specs/SPEC_TEMPLATE.md`).
2. Copy Appendix A toy cases into the test file. These are your failing tests.
3. Run `make test-mapped FILE=<source>` — confirm RED.
4. Implement the minimum to go GREEN.
5. Run `make lint && make type && make coverage`.
6. Run `pytest tests/regression/ -x` — must stay GREEN.
7. Commit with Conventional Commit + a `DECISIONS.md` entry if any non-obvious choice was made.
8. Append to `progress.md` what you learned (gotchas, performance discoveries, API quirks).

If any of steps 3–6 fail for the wrong reason, stop and diagnose. Do not paper over with `try/except` or test skips.
