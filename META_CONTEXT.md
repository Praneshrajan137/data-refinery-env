# Section 0 — Meta-context (what Cursor must know before writing a single line)

## 0.1 What DataForge is (the one-line spec)

DataForge is an open-source AI agent that autonomously detects, diagnoses, and safely repairs data-quality bugs in tabular data (CSVs, warehouse tables, dbt models), with SMT-verified repairs, a constitutional safety layer, and reversible transactions. It ships as:

- A **CLI** (`dataforge profile | repair | revert | watch | bench`) — the hero product.
- A **hosted playground** at `dataforge.dev/playground` — 10-second "try it now".
- An **OpenEnv-compatible RL environment** — the training substrate that makes the agent learnable.
- An **evaluation harness** (`dataforge-evals`) — a standalone package others can use.
- A **family of open post-trained models** (0.5B → 7B) on Hugging Face, trained with SFT → GRPO → GiGPO.
- **Integrations**: `dataforge-dbt`, `dataforge-airbyte`, `dataforge-databricks`, an MCP server.
- An **agent-patterns library** (`dataforge-agent-patterns`) — generic agentic primitives extracted from DataForge, usable in any agent project.

---

## 0.2 Who it is for (the actual users, not marketing personas)

- **Priya**, data engineer at a Series-B SaaS company with a messy Snowflake/dbt stack. Runs `dataforge profile` on a problematic model and expects readable output in < 5 seconds.
- **Marcus**, staff engineer at an AI company whose training pipeline is corrupted by upstream CSVs. Wants `pip install dataforge` and `dataforge repair broken.csv --dry-run` in under five minutes.
- **Shreya**, applied-AI PM at a data platform. Reads `DECISIONS.md` and the architecture post to decide whether to build a similar feature in-house.
- **The agent itself** — a downstream user. Calls DataForge as an MCP tool from Claude/Cursor/Windsurf. Needs concise tool signatures, safe defaults, reversibility.

---

## 0.3 The quality target (binary, testable)

A hire-bar senior engineer at Anthropic / HuggingFace / Databricks / dbt Labs / Monte Carlo, looking at this repo for 10 minutes on a Tuesday, reaches one of the following three conclusions — and the project succeeds only at the first:

1. **"Who built this? Let's talk to them."** ← success.
2. **"Interesting ideas, but over-scoped and under-built."** ← failure mode A (ambition without ship).
3. **"Another resume project dressed up in agent vocabulary."** ← failure mode B (taste deficit).

A reviewer reaches judgment 1 (and not 2 or 3) because of a **conjunction** of signals, not any single one. All must be true:

### Ship signals

- `pip install dataforge && dataforge profile fixtures/hospital_10rows.csv` works on a fresh Python 3.11 or 3.12 env in under 30 seconds (once published to PyPI; during development, use `pip install -e ".[dev]"` from the repo).
- The README's first screen has a benchmark table with real, reproducible numbers — not TBD and not marketing copy.
- At least one integration package (`dataforge-dbt`, `dataforge-mcp`, or `dataforge-evals`) is live on PyPI with a non-trivial integration test that runs in CI.

### Judgment signals

- `DECISIONS.md` has ≥ 10 substantive entries, each with genuine alternatives considered (not two strawmen and a foregone conclusion) and reversal criteria (what evidence would flip the decision).
- The repo contains at least one RFC or spec that was **superseded** — and the superseded document is still committed, marked `Status: Superseded by RFC-NNN`. This proves the author reasons in public, not just retrofits.
- The `specs/QUESTIONS.md` file is not empty. It contains real open questions the author flagged rather than guessed through.

### Safety-engineering signals

- Every fix the agent proposes passes through Safety Filter → SMT verifier → reversible transaction log, visible in the code and traceable via the audit log (Section 6.6).
- The adversarial test suite is **real**, not performative — it contains prompts the author wrote, not just a generic jailbreak template.
- `dataforge revert <txn_id>` demonstrably restores byte-for-byte pre-state, verified by hash comparison in a property test.

### Product-thinking signals (the ones most often missing from "portfolio projects")

- The README has a "When DataForge is the wrong tool" section (Section 0.7). Unconfident projects never include this; confident ones always do.
- The project has at least one named external user acknowledged in `CONTRIBUTORS.md` or the README — someone outside the author's head who used it and either filed an issue or said something quotable. A design partner is the highest-value signal in the repo, worth more than any number of stars.
- The roadmap (RFCs) shows product judgment about what **not** to build — explicit deferrals with reasoning, not a greatest-hits list.

### Anti-signal checklist

If **any** of these are present, the reviewer reaches judgment 2 or 3 instead:

- README claims features that aren't shipped ("Supports BigQuery" when the BigQuery code does not exist).
- Benchmark cells show suspiciously round numbers with no linked script.
- Commit history shows a single "initial commit" dump of 10K lines — the project wasn't built in public, it was rehearsed in private.
- LLM-prose tells: "Absolutely!", sparkle emojis, "revolutionary", "game-changing", "In this document we will explore…".
- Every file has a docstring but no file has a test.

---

## 0.4 The non-negotiables (Cursor: obey these absolutely)

1. **Never regress existing passing tests.** The regression suite currently contains a single import smoke test (the ~389-assertion target grows as modules ship in Weeks 1–13). These tests are **stable**, not frozen: before merging any change, all existing tests must still pass. If you believe a test encodes a stale or wrong assumption, the process is: (a) write a spec entry proposing the change, (b) open a PR that updates the spec and the test together, (c) require explicit reviewer approval. Tests are artifacts that can be wrong; stable means "changing these has a high, auditable bar," not "these are sacred."
2. **Every public function has a type hint and a docstring.** No exceptions. `mypy --strict` must pass for `dataforge/*`.
3. **Every proposed data fix passes through Safety Filter → SMT verifier → Transaction log.** This is the core safety invariant. Never bypass.
4. **No browser storage APIs** (`localStorage`, `sessionStorage`) anywhere. If state must persist in the playground, use FastAPI backend + signed cookies.
5. **No fabricated benchmark numbers.** Every number in `README.md` and blog posts must be reproducible from a committed script. If you don't have a number yet, write `TBD (see scripts/bench/…)` — never invent.
6. **No browser-run LLM calls that expose an API key.** The playground calls a backend; the backend holds the key.
7. **No copying of training data from commercial APIs into the repo.** Only publicly licensed fixtures may be committed.
8. **Write the spec before the code.** For every module of substance, `specs/SPEC_<module>.md` must exist, be reviewed, and be committed before the implementation is merged.
9. **Write the failing test before the implementation.** Red → green → refactor. The test embodies the spec's acceptance criteria.
10. **Commit messages are Conventional Commits** (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`). Short subject (≤ 72 chars) + body explaining why, not just what.

---

## 0.5 What Cursor must NOT do

- Do not refactor working modules "for style" without a spec entry justifying it.
- Do not add dependencies without appending to `ARCHITECTURE.md § Dependencies` with a one-sentence justification.
- Do not generate placeholder markdown that looks like documentation but isn't. If a section doesn't have real content yet, write `TBD — tracked in issue #N` and move on.
- Do not mock or stub where an integration test is required. A test that passes against a mock is not evidence the integration works.
- Do not edit `tests/regression/test_env.py` (the regression smoke test baseline) without first opening a PR that updates the corresponding spec and documents WHY the test was wrong. This file is **stable**, not frozen — changes require the spec-first discipline from Section 0.4, rule 1. The default assumption is that the test is right and your code is wrong.
- Do not write README claims in the first person plural ("we built") before there's a we. Use "DataForge does X" language.
- Do not use LLM emoji decorations, sparkle emojis, or "Absolutely!" preambles in any committed file.

---

## 0.6 Pre-mortem — how this project dies (inversion analysis)

Before writing code, reason in reverse. Imagine it's Day 91, the project shipped, and it failed to produce the outcome in Section 0.3. What killed it? The following are the realistic failure modes ranked by probability × severity. For each, the paired mitigation is the constraint the agent must obey throughout the build — not a fix applied at the end.

### F1. Scope fragmentation (highest risk, most common kill reason)

Seven deliverables (CLI, playground, OpenEnv, evals, agent-patterns, three integrations, a model family) in 13 weeks on free compute, by one person, is fiction at face value. The likely outcome without discipline is seven half-finished artifacts, none convincing. **Mitigation:** Section 5.0 ships an explicit P1 / P2 / P3 tag on every deliverable and a named cut order. If behind at the Week-6 checkpoint, P3 is already deleted from the roadmap and P2 is next. The P1 bundle (CLI + reversible repair + safety gate + one SOTA-comparison benchmark table + one integration) is the minimum shippable project. Everything else is upside.

### F2. Synthetic-benchmark ceiling

If the benchmark table is computed entirely from procedurally corrupted CSVs, a reviewer's first instinct — correctly — is "this measures what your generator emits, not what real data engineers face." **Mitigation:** Hospital / Flights / Beers (real-world, externally sourced, widely cited) must be in the primary benchmark table before any synthetic results. Synthetic results go in a clearly-labelled secondary table in the docs, never the README. No seeded-and-solved-by-the-same-author benchmarks in public comms.

### F3. No design partner — building in a vacuum

A repo that no one outside the author's head has used is an assertion, not evidence. **Mitigation:** by end of Week 4, three named design partners have each run `dataforge profile` against their own data and filed at least one issue or sent a one-paragraph reaction. Their handles go in `CONTRIBUTORS.md` (with permission). If Week 4 arrives with zero design partners, the project pauses and user-validation is the only Week-5 activity. See Section 5.5.

### F4. The safety pipeline adds latency that users won't accept

Three gates (Safety → SMT → Transaction log) sound responsible and feel slow. If `dataforge repair data.csv --dry-run` on a 10K-row CSV takes 12 seconds, the demo is dead on arrival regardless of correctness. **Mitigation:** hard budget of p95 < 2 seconds end-to-end on 10K rows for the dry-run command. Benchmark continuously in `tests/benchmarks/bench_pipeline.py`. If we cannot meet this budget, we ship an "unsafe-but-fast" path with a loud banner, not a safe-and-slow path no one uses.

### F5. Trained models fail to outperform zero-shot LLMs on the SOTA comparison

A 0.5B–1.5B model trained on Kaggle free tier may not beat Claude / Gemini zero-shot. If the headline benchmark has DataForge-1.5B-GRPO below Gemini 2.5 Flash, the training narrative collapses. **Mitigation:** the headline of the project is the CLI + safety + reversibility, not the trained model. The trained model is framed as "what you get on-prem, air-gapped, free" — a different axis than zero-shot frontier performance. The README's benchmark table shows both columns (F1 and free-tier cost per cleaned 1K cells). If the trained model loses on F1 but wins on cost by >10×, that's still a story worth telling. If it loses on both, we publish an honest negative-result blog post — which is itself a high-signal artifact.

### F6. Agent calls a commercial API in a way that leaks a key or data

The single most career-damaging failure mode: a browser-visible API key, or user CSV data posted to a third-party LLM without consent. **Mitigation:** Section 0.4 rules 4 and 6 are hard constraints. The playground backend holds all keys; the CLI refuses to send data to any LLM without an explicit `--allow-llm` flag and prints the destination + redaction policy on every invocation. The MCP server applies the same rule — no silent outbound data.

### F7. Drift between README claims and shipped code

The README is a contract. Every time a feature is described in the README but not yet shipped, the project loses credibility compound-daily with every visitor. **Mitigation:** CI includes a README-integrity check (`scripts/ci/readme_truth.py`) that asserts every `dataforge <subcommand>` the README demonstrates actually exists in `dataforge.cli` and that every filename referenced resolves. Adversarial reading of the README against the repo is on the ship rubric (Section 7).

### F8. Safety-predicate DSL is a smoke screen

If the "constitution" YAML uses free-form Python strings parsed with `eval()` or a near-equivalent, the entire safety story collapses on inspection. A reviewer will try to escape the sandbox and will succeed. **Mitigation:** the DSL is named-callable-only — YAML references registered Python predicates by `id` with typed arguments. Zero string-eval code paths. See Section 6.1 for the corrected design.

### F9. Overfitting to the Cursor/Claude hype cycle

"AI agent that…" is saturated. A reviewer who has seen 200 such READMEs skims past the agent claim and looks for substance. **Mitigation:** lead with the boring, legible win: "reversible, verified repairs for tabular data." The agent architecture is *how* we do it, not *what* we are. Every public doc passes the "would this land as a Hacker News front page if we strip the word 'agent'?" test.

### F10. The author burns out in Week 7 and disappears

Thirteen weeks of disciplined public building is harder than it reads. If the author loses momentum and the repo goes 3 weeks without a commit, the project is functionally dead in reviewer eyes. **Mitigation:** the P1 bundle is achievable in ~6–7 weeks if P2/P3 are ruthlessly cut. The 13-week plan has slack built in. Maintain a visible streak via `progress.md`; if momentum drops, cut scope, don't cut transparency.

**Usage:** these ten failure modes are the row headings of the pre-mortem checklist in Section 7.5, which is run before every public release. A release with any unchecked pre-mortem item does not ship.

---

## 0.7 Honest competitive positioning — what DataForge is NOT

Reviewers trust projects that are legibly aware of their context. The space is crowded; not naming the neighbors is a confidence tell in the wrong direction.

### Tools in the same neighborhood (named, specifically, with their strengths)

- **Great Expectations** (open source, ~10K GitHub stars, de facto standard). Strength: declarative expectations, wide integration surface, mature community. Weakness for DataForge's use case: expectations are written by humans and check, not discover and repair. DataForge is complementary — the repair layer that could sit downstream of a GE suite.
- **Soda** (commercial + OSS Soda Core). Strength: SQL-native checks, team features, scheduling. Weakness for DataForge's use case: detection-only by design; no repair; no safety-verified fix proposal layer.
- **Monte Carlo** (commercial, enterprise observability). Strength: lineage, anomaly detection, SLA tracking across warehouses. Weakness for DataForge's use case: priced for enterprise; not for a lone data engineer on a $0 budget; no repair.
- **Cleanlab** (open source, 10K+ stars, ML-native). Strength: label errors in labeled ML datasets, confident-learning theory. Weakness for DataForge's use case: focuses on labeled training data, not warehouse / CSV / dbt-model quality; different axis.
- **dbt-expectations, dbt-utils.test_\***. Strength: lives inside the dbt stack everyone already has. Weakness for DataForge's use case: declarative tests only; no agent layer; no automatic fix proposals.
- **HoloClean / Raha / Baran** (academic). Strengths: benchmark baselines, published results. Weakness: research artifacts, not production tools — no PyPI install of a maintained package a data engineer uses in 2026.
- **Cocoon** (Claude 3.5 Sonnet, VLDB 2024). Strength: first serious LLM-based data cleaner; human-in-the-loop. Weakness for DataForge's use case: single-pass LLM reasoning, no constitutional safety, no SMT verification, no reversibility.

### Where DataForge is differentiated (the one-paragraph pitch a reviewer can steelman)

DataForge is the only tool in the above set that (a) proposes executable repairs (not just detects issues), (b) passes every proposed repair through a constitutional safety filter + SMT verifier before any disk write, (c) logs every applied change as a reversible transaction, and (d) ships a free-tier-trained model family for air-gapped use. Any one of these exists elsewhere. The conjunction is the product.

### Where DataForge is the WRONG tool (put this paragraph in the README)

Do not use DataForge if your data is (a) streaming / unbounded — DataForge is batch-oriented; (b) > 100 million rows — the SMT verifier has linear-in-schema cost but not sublinear-in-data cost; (c) in a regulated environment where every fix must be human-authored (healthcare billing, SOX, EU AI Act high-risk) — DataForge proposes fixes an agent generated, which is precisely what your compliance officer needs to review and likely reject; (d) under a strict SLA where a 30-second profile is already too slow; (e) already well-served by Great Expectations suites your team has maintained for years — adding DataForge is a solution in search of a problem. Naming these cases up front earns trust for everything else.

### What DataForge explicitly does not claim

- Not a data catalog. Use DataHub, Amundsen, OpenMetadata.
- Not lineage. Use OpenLineage, Marquez, dbt's DAG.
- Not a full observability platform. Use Monte Carlo, Elementary, or Soda.
- Not a warehouse. DuckDB is the query engine; you bring the warehouse.
- Not a replacement for your CI — DataForge runs *in* your CI, against your dbt models.
