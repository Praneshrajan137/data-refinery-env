# CLAUDE.md - DataForge Living Knowledge Base

This file accumulates gotchas, decisions, and context that should survive across
Cursor, Claude, and Codex sessions. Append new discoveries to the bottom with
the date.

## Project Conventions

- **Read [PRODUCT.md](PRODUCT.md) first.** It is the canonical constitution
  (purpose, philosophy, first principles, honesty doctrine, vision, mission, and
  the safety invariant). Every session's work must uphold it; when in doubt about
  intent or a claim's wording, PRODUCT.md wins.
- **DATASET SCOPE RULE (must follow every session):** The `beers` dataset is
  EXCLUDED from the project. Never add, benchmark, train on, calibrate against,
  set coverage floors for, or document `beers` in any forward-looking work.
  For the remaining RAHA datasets, do NOT rank by a fixed priority — they are one
  canonical suite of equal provenance that differ by ERROR PROFILE, not quality.
  Choose datasets by the CAPABILITY a given change is meant to prove:
  `hospital` is the flagship dataset. Its 0.7926 heuristic correction F1 is a HISTORICAL
  RECORD, not a live regression floor — corrected 2026-08-27. That figure was measured with
  a deterministic stack that still had `type_mismatch` and `decimal_shift` in the auto-apply
  set; both were removed on measurement (`decimal_shift` would have rewritten 263,428
  already-correct values on a fourth corpus), so no current configuration can produce it and
  `dataforge bench --quick` does not. Treating it as a floor inverted the incentive: the only
  way to "hold" it is to re-admit the two detectors that were removed for corrupting data.
  What replaces it is per-detector unconditional write measurement, including how many
  already-correct cells each detector would overwrite, in
  [docs/trust/bypass-allowlist-evidence.md](docs/trust/bypass-allowlist-evidence.md).
  When the number is quoted at all, PRODUCT.md requires describing it as *competitive with /
  in the range of* the Raha+Baran baseline under our scoring, never as a SOTA win: the two
  numbers are not measured under an identical protocol. Also use
  `tax` for provable
  FD/rule-violation repair at scale; `rayyan` for datetime/format canonicalization;
  `flights` for the not-inferable-in-table frontier. Never prioritize `tax` or
  `rayyan` for accuracy work on an UNMEASURED baseline — measure first (`tax` needs
  a scale-aware/sampled bench; `rayyan` needs a full correction run). Frozen
  historical artifacts (past SFT/GRPO training curricula, archived `eval/results/`
  run snapshots, released-model tokenizer vocab) may still mention `beers` because
  that is a historical fact and must NOT be rewritten. New work simply must not use
  `beers`.
- Python 3.11 / 3.12. `pyproject.toml` pins `requires-python = ">=3.11,<3.13"`.
- The top-level `dataforge/` package exports the product API. Root-level legacy
  wrappers exist only for compatibility.
- CLI commands live in `dataforge/cli/` and are registered in
  `dataforge/cli/__init__.py`.
- Rich is used for user-facing CLI output. Do not use `print()` in library code.
- `data_quality_env/` (the legacy hackathon package) and the ~20 loose root-level `.py`
  shims are DELETED as of 2026-08-27. Do not reintroduce either: `tests/regression/test_env.py`
  asserts the name is not importable and that the repo root carries no loose modules.
  The repo DIRECTORY is still called `data_quality_env`, so a grep hit is usually the path,
  not the package.

## Known Gotchas

- `pandas.read_csv(..., dtype=str)` is the safest default for messy CSVs. Pandas
  type inference can lose precision on monetary or identifier-like values.
- Z3 `Real` variables are mathematical reals, not IEEE-754 floats. Use `FP` only
  when actual floating-point behavior matters.
- TRL v1+ manages `remove_unused_columns` internally in `GRPOConfig`; do not
  hand-set it from older tutorials.
- Week 12 GRPO configs intentionally use `prompt_token_budget`, not
  `max_prompt_length`; pass `max_prompt_length` to TRL only if the installed
  `GRPOConfig` signature exposes it.
- On Windows-style runtimes, import `GRPOTrainer` under `PYTHONUTF8=1` to avoid
  TRL chat-template decode failures.
- `causal-learn` PC does not accept NaN values. Impute or drop missing values
  before discovery.
- OpenEnv's current primary API is `reset()`, `step()`, and `state()`. The local
  server also exposes `close()` for compatibility.

## 2026-05-15 Notes

- The environment action space is now eight actions. `ROOT_CAUSE` is read-only
  and returns analyzer-backed root indices; it does not authorize repairs.
- `R_ROOT_CAUSE` is a small dense bonus and only applies when task metadata
  exposes root labels.
- `dataforge-mcp/` is a nested standalone package. Keep MCP transport
  dependencies out of core `dataforge`.
- The SFT oracle workflow reserves held-out rows before chunking. Held-out rows
  must not appear in target rows, context rows, normalization candidates, fixes,
  or messages.
- The published 0.5B SFT checkpoint is smoke-release evidence, not a quality
  milestone. Do not describe it as deployment-ready unless verifier metrics
  show a real held-out gain and the docs are updated with that evidence.
- The Gradio model demo is separate from the CSV playground. It caps inputs at
  50 parsed data rows and may return malformed or incorrect model output.
- Hugging Face ZeroGPU is selected in Space settings. Do not document unsupported
  README frontmatter keys for hardware selection.
- GRPO reward scoring must remain local/stateless. Do not call the mutable
  OpenEnv HTTP singleton from parallel rollout rewards unless a future stateless
  scoring endpoint is specified.

## Performance Notes

- Detector pass on a 10k-row CSV should finish in under 2 seconds.
- SMT verification can become expensive if FDs are expanded into concrete row
  pairs. Prefer symbolic constraints where possible.
- Rich tables are slow for large output. Summarize or paginate beyond a few
  hundred rows.

## Dated Notes, Within A Budget

This section is append-only, but the file is not unbounded. **CLAUDE.md is injected as
instructions into every editor session**, so a line added here changes how every future
session behaves, and volatile detail added here goes stale in the one place a session is
most likely to trust it. `tests/unit/test_claude_md_scope.py` enforces a size budget and
refuses environment-scoped content, because between 2026-08-30 and 2026-09-01 this file
grew from 104 to 259 lines with cloud-sandbox operating detail that had nothing to do with
local work. That material now lives in `docs/automation/README.md`, which nothing
auto-loads.

Before appending here, ask whether the note is a **durable repository convention**. If it
is scoped to one environment, one deployment, or one investigation, it belongs in a file
nothing auto-loads.

## 2026-07-12 Notes

- **`beers` removed from the active project (dataset-scope rule).** Per the DATASET
  SCOPE RULE above, `beers` was removed from the live, forward-looking surfaces:
  `dataforge/datasets/registry.py` (registry entry), `dataforge/cli/bench.py`
  (default dataset expansion), `dataforge/release/model_family.py` (required/eval
  dataset lists), `eval/thresholds/coverage_floors.json` (the `heuristic/beers`
  floor block), and the README benchmark docs. Live bench tests were updated to
  hospital+flights. Frozen curricula (`training/grpo_config.py`, `expert_v*`
  trajectories) and archived `eval/results/` snapshots were intentionally left
  untouched — they record what past runs actually did. Dataset selection for new
  work is capability-based (see the DATASET SCOPE RULE above): `hospital` is the
  flagship/regression anchor; `tax`/`rayyan`/`flights` are chosen by the capability
  a change proves, measured before prioritized.

## 2026-09-08 Notes

- **`393 repairs / 0 corruptions` is the ORACLE arm, never a declared premise.** It comes from
  `discover_oracle_fds`, which admits a dependency only if it holds on the **clean** frame; that
  harness says "No user has this. It is the ceiling." Five sites had credited it to a user's
  `--schema`, including `dataforge repair --help`. Before quoting any premise number, name the arm:
  **oracle** = admitted by ground truth, **mined** = from the dirty frame, **declared** = authored
  by a user. Ground truth may GRADE a declared premise; the moment it FILTERS one, the arm is an
  oracle wearing a different label.
- **There is no demonstrated end-to-end correction capability on hospital.** Measured through the
  shipped write path, a declared premise writes **0** cells and the oracle ceiling reaches F1
  **0.1918**, against the proposal-stage **0.8352**
  ([docs/trust/declared-premise-capability.md](docs/trust/declared-premise-capability.md)). So
  0.8352 is a proposal-stage tripwire only, and the gap is **not** a premise-quality problem: a
  13-dependency premise every member of which ground truth admits also writes zero. Do not tell
  users that declaring a schema recovers the capability C4 gives up.
- Hashing a committed file for a kill criterion? Hash **normalised text**, not bytes.
  `core.autocrlf=true` leaves CRLF in a Windows worktree while git stores LF, so a byte hash
  differs between here and Linux CI and fires the criterion for the wrong reason.

## 2026-09-08 Notes, second entry

- **RETRACTED from the entry above: "no demonstrated end-to-end correction capability on hospital".**
  That was measured with the shipped default only. The declared premise''s zero was a refusal on
  **VOLUME**, not evidence: `repair.py` discards an entire batch rewriting more than 100 cells, and
  discards it **silently** into neither `result.fixes` nor `receipt.suggested_fixes`. With
  `--confirm-escalations` the same premise corrects **152** of 509 errors at precision **1.0000**
  with **zero** corruptions, end-to-end **F1 0.4599**
  ([docs/trust/fd-repair-yield-mechanism.md](docs/trust/fd-repair-yield-mechanism.md)). Always quote
  it with the flag; the same premise is 0.0000 without it. The oracle premise never looked better
  than the declared one — it looked **smaller** (54 cells is under the cap, 152 is not).
- **When a pipeline arm reports zero, record `receipt.safety_verdict` and `receipt.reason` before
  concluding anything.** Five instruments reported a bare zero and none reported why, because they
  recorded `writes`/`tp`/`fp` only. A pipeline-stage measurement that does not record *why the
  pipeline refused* is an incomplete instrument. This is the third variant of the one defect class
  this repo keeps finding.
- The differential verifier checks a candidate against the dependencies **touching the fixed
  column** (`direct.py:129-133`, `smt.py:245-248`), global over **rows** — **not** against the whole
  schema, whatever `fd_violation.py` used to say. And its z3 leg is not the bottleneck: it returned
  `accept` on all 160 proposals the Direct leg proved.
