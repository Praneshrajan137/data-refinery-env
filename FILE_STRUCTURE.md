# DataForge15 File Structure

Last updated: 2026-05-15.

This file documents the current repository shape plus planned areas that are
intentionally not shipped yet.

## Current Canonical Tree

```text
data_quality_env/
  dataforge/                 Core package.
    agent/                   Typed tool actions, scratchpad, provider clients.
    bench/                   Real-world benchmark runner and reports.
    causal/                  Causal DAG and root-cause analyzer.
    cli/                     Typer commands: profile, repair, revert, bench.
    datasets/                Embedded and real-world dataset loaders.
    detectors/               type_mismatch, decimal_shift, fd_violation.
    env/                     OpenEnv-compatible environment and HTTP server.
    integrations/            Early adapter placeholders.
    repairers/               Deterministic repair proposal generators.
    safety/                  Constitution compiler and safety filter.
    transactions/            Append-only journals, snapshots, revert.
    ui/                      Rich rendering helpers.
    verifier/                SMT verifier and explanations.
    calibration.py           Abstention policy (auto-apply/review thresholds).
    calibration_map.py       Post-hoc isotonic/Platt probability calibration.
    conformal.py             Distribution-free certified auto-apply + PSI drift.
  dataforge-mcp/             Standalone MCP source package and tests; planned PyPI name is dataforge_07_mcp.
  playground/
    api/                     Hugging Face Docker Space backend.
    web/                     Cloudflare Workers Static Assets frontend.
  playground-model/          Separate Gradio Space for 0.5B SFT demo.
  scripts/
    bench/                   Benchmark generation scripts.
    ci/                      README truth checker.
    data/                    SFT trajectory collection/build/validation.
    model/                   SFT release verification and dataset card upload.
    playground/              Space staging and deployment verification.
  specs/                     Specs and open questions.
  tests/                     Unit, integration, regression, property, benchmark,
                             adversarial, and fixture coverage.
  training/                  SFT/GRPO configs, Kaggle notebooks, dataset/model docs.
```

Root documentation and project files:

```text
README.md
ARCHITECTURE.md
DECISIONS.md
CHANGELOG.md
CLAUDE.md
CONTRIBUTING.md
CURSOR_MASTER.md
FILE_STRUCTURE.md
META_CONTEXT.md
README_DATAFORGE.md
REWARD_DESIGN.md
SECURITY.md
Makefile
pyproject.toml
test_map.json
uv.lock
```

## Generated Or Local-Only Areas

These directories are not canonical documentation sources and should not be
edited by hand as part of a docs refresh:

```text
.hf-space-repo/
.hf-space-stage/
.hf-space-stage-plan/
.mypy_cache/
.pytest_cache/
.ruff_cache/
.venv/
data/sft_traj/*.jsonl
logs/
```

## Planned Or Aspirational Areas

The following product surfaces remain future work unless a spec says otherwise:

- `packages/dataforge-evals`: monorepo source for `dataforge_07_evals`.
- `packages/dataforge-dbt`: monorepo source for `dataforge_07_dbt`.
- `packages/dataforge-agent-patterns`: monorepo source for
  `dataforge_07_agent_patterns`.
- `dataforge-airbyte`
- warehouse-native adapters
- published `dataforge_07_evals`, `dataforge_07_dbt`, and
  `dataforge_07_agent_patterns` package evidence
- production model family beyond the current verified SFT/GRPO release evidence
- hosted product domain and docs site

Do not promote planned areas into `README.md` as shipped features until the
corresponding package, tests, and release evidence exist.
