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

## Append-Only From Here Onward

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

## 2026-08-30 Notes

- **A scheduled Cortex Code automation now reviews this repo daily.** It is a
  Snowflake AGENT TASK in `USER$PRANESH07.PUBLIC`, firing daily at 12:30 in
  `Asia/Kolkata`, authenticating to GitHub through the Snowflake secret
  `INTEGRATIONS.PUBLIC.GITHUB_PAT` (`TYPE = PASSWORD`, `USERNAME = 'git'`). The
  fire runs in a Snowflake-managed sandbox at `/workspace`: it has NO local
  filesystem and no access to a developer checkout, so its prompt must clone
  `https://github.com/Praneshrajan137/dataforge.git` itself. A prompt that says
  "analyze this project" without cloning is a silent no-op — that was the
  original defect.
- **The automation writes exactly one file: `docs/automation/daily-review.md`.**
  That file is MACHINE-GENERATED and NON-AUTHORITATIVE. It is not evidence, and
  it does not carry the standing of anything under `docs/trust/`. Do not cite it
  as a source, and do not promote a number out of it without measuring that
  number yourself.
- **The automation pushes to `automation/daily-review-<YYYY-MM-DD>`, never to
  `main`, and never creates a tag.** This is a deliberate containment boundary,
  not a stylistic preference. `.github/workflows/ci.yml` and `docs.yml` both
  trigger on `push: branches: [main]`, so an unattended daily commit to `main`
  would run full CI every day and could leave `main` red with no observer. A
  push to any other branch triggers no workflow. The tag prohibition matters
  because every `publish-*.yml` triggers on `tags: v*` — a bot tag would ship to
  PyPI. (`sync-to-hf.yml` is `workflow_dispatch` only, so the Space is safe from
  a stray push either way.) If you ever repoint this automation at `main`, you
  are re-opening both holes.
- **The automation is forbidden from emitting a number it did not measure in
  that run.** This is the non-obvious one. `scripts/ci/docs_truth.py` is an
  ALLOWLIST over `docs/quantitative_claims.yaml`: it verifies that registered
  claims still match their artifacts, so it cannot see a number in a document it
  does not know about. The failure mode for a new machine-written doc is
  therefore SILENCE, not a red gate — unverified figures would pass CI
  untouched. That is the exact defect `docs_truth.py` exists to prevent, so the
  prompt bans unmeasured numbers and requires open questions instead.
- **The automation must not modify `PRODUCT.md`, `DECISIONS.md`, `CLAUDE.md`,
  anything under `docs/trust/`, or `docs/quantitative_claims.yaml`.** Those are
  the constitution, the decision record, this file, the evidence corpus, and the
  claim registry. Nothing unattended edits them.
- **Debugging a fire:** `cortex automation doctor <name>` gives state and
  `thread_id`; `cortex conversations transcript <thread_id>` shows what actually
  ran. Prefer the transcript for the dangerous case — state `SUCCEEDED` while the
  side effect never happened. The prompt ends with a `DAILY_REVIEW_OK …` /
  `DAILY_REVIEW_FAILED:<reason>` status line so a vacuous success is
  distinguishable from a real one.
- **Gotcha, unresolved as of this writing:** `cortex automation list` fails on
  this machine with "Could not confirm whether automations are enabled for this
  account (the Cortex Agent endpoint was unreachable)." While that persists the
  CLI cannot create, test, or inspect automations, and the Snowsight Automations
  dialog is the only path. Consequence: the skill's recommended one-shot test
  before scheduling is unavailable, so a newly saved automation is UNVERIFIED
  until its first real fire. Confirm it exists with `SHOW TASKS IN SCHEMA
  "USER$PRANESH07".PUBLIC` rather than trusting that the dialog accepted it.
- **A successful `ALTER GIT REPOSITORY … FETCH` proves the PAT can READ, not
  PUSH.** Fetch and push are different scopes. If a fire fails at delivery with
  the objects otherwise healthy, insufficient PAT scope is the first suspect.
