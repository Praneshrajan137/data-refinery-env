# DataForge — Canonical File Structure (Section 3)
#
# This file documents the target directory tree.
# Cursor: create this tree on Day 0.
#
# dataforge/                              # The main repo. Public, Apache-2.0.
# ├── .cursor/
# │   └── rules/
# │       └── dataforge.md                # (from Section 1)
# ├── .github/
# │   └── workflows/
# │       ├── ci.yml                      # Lint + type + test on every PR
# │       ├── bench.yml                   # Weekly benchmarks on main
# │       └── release.yml                 # Tag → build → PyPI + HF release
# ├── dataforge/                          # The installable Python package
# │   ├── __init__.py                     # Exports the public API
# │   ├── cli/                            # Typer-based CLI (one module per subcommand)
# │   │   ├── __init__.py
# │   │   ├── profile.py
# │   │   ├── repair.py
# │   │   ├── revert.py
# │   │   ├── watch.py
# │   │   └── bench.py
# │   ├── detectors/                      # Pure detector functions. No LLM calls.
# │   │   ├── __init__.py
# │   │   ├── base.py                     # Detector protocol
# │   │   ├── type_mismatch.py
# │   │   ├── decimal_shift.py
# │   │   ├── fd_violation.py
# │   │   ├── pii_leakage.py
# │   │   ├── outlier.py
# │   │   ├── encoding_error.py
# │   │   └── ...                         # 18 total, one per detector class
# │   ├── repairers/                      # Fix-proposal generators
# │   │   ├── __init__.py
# │   │   ├── base.py
# │   │   └── ...                         # one per detector type
# │   ├── agent/                          # The agent loop (LLM or trained model)
# │   │   ├── __init__.py
# │   │   ├── loop.py                     # Observe → plan → tool-use → fix → verify
# │   │   ├── scratchpad.py               # In-episode hypothesis tracking
# │   │   ├── tool_actions.py             # SQL_QUERY, STAT_TEST, PATTERN_MATCH, etc.
# │   │   ├── providers.py                # LLM provider abstraction
# │   │   └── prompts/                    # Versioned prompt templates
# │   │       ├── system_v1.md
# │   │       └── tool_use_v1.md
# │   ├── safety/                         # Constitutional safety layer
# │   │   ├── __init__.py
# │   │   ├── constitution.py             # YAML parser + compiler
# │   │   ├── filter.py                   # ALLOW/ESCALATE/DENY verdicts
# │   │   ├── refusal.py                  # Graceful refusal objective for RL
# │   │   └── adversarial/                # Red-team prompt corpus
# │   │       ├── jailbreak.yaml
# │   │       ├── pii_extraction.yaml
# │   │       └── role_manipulation.yaml
# │   ├── verifier/                       # SMT verification (Z3)
# │   │   ├── __init__.py
# │   │   ├── smt.py                      # Schema → Z3 compilation
# │   │   ├── schema.py                   # Schema definition + FD miner
# │   │   └── explain.py                  # Unsat core → natural-language explanation
# │   ├── causal/                         # Root-cause analyzer
# │   │   ├── __init__.py
# │   │   ├── dag.py                      # Causal DAG construction
# │   │   ├── pc.py                       # PC algorithm with FD priors
# │   │   └── root_cause.py               # Minimal root-set identification
# │   ├── transactions/                   # Reversible transaction log
# │   │   ├── __init__.py
# │   │   ├── txn.py                      # RepairTransaction dataclass
# │   │   ├── log.py                      # Append-only log writer/reader
# │   │   └── revert.py                   # Byte-for-byte restore
# │   ├── env/                            # OpenEnv-compatible RL environment
# │   │   ├── __init__.py
# │   │   ├── environment.py              # step() / reset() / close()
# │   │   ├── observation.py              # Observation builder
# │   │   ├── reward.py                   # Reward engine (dense + terminal)
# │   │   └── grader.py                   # Ground-truth comparison
# │   ├── engine/                         # Corruption / BYOD engine
# │   │   ├── __init__.py
# │   │   ├── profiler.py                 # Auto-discover types, FDs, constraints
# │   │   ├── synthesizer.py              # Compositional corruption grammar
# │   │   ├── fd_discovery.py             # FastFDs-style FD miner
# │   │   └── cascade.py                  # Cascading-error composition
# │   ├── datasets/                       # Dataset loaders
# │   │   ├── __init__.py
# │   │   ├── builtin/                    # Small bundled fixtures (< 100KB each)
# │   │   ├── real_world.py               # Hospital / Flights / Beers loader
# │   │   └── registry.py                 # Canonical dataset metadata
# │   ├── integrations/                   # Stubs for external adapters
# │   │   ├── __init__.py
# │   │   ├── dbt.py
# │   │   └── airbyte.py
# │   └── ui/                             # Rich-based terminal UI
# │       ├── __init__.py
# │       ├── profile_view.py
# │       ├── repair_diff.py
# │       └── components.py
# ├── specs/                              # Spec-driven development
# │   ├── SPEC_TEMPLATE.md
# │   ├── SPEC_detectors.md
# │   ├── SPEC_agent_loop.md
# │   ├── SPEC_safety.md
# │   ├── SPEC_smt_verifier.md
# │   ├── SPEC_causal_root_cause.md
# │   ├── SPEC_transactions.md
# │   ├── SPEC_env.md
# │   ├── SPEC_cli.md
# │   ├── SPEC_playground.md
# │   └── QUESTIONS.md
# ├── tests/
# │   ├── unit/
# │   ├── integration/
# │   ├── regression/
# │   │   └── test_env.py                 # Regression smoke test (placeholder; grows as modules ship)
# │   ├── property/
# │   ├── benchmarks/
# │   ├── adversarial/
# │   └── fixtures/
# ├── scripts/
# │   ├── test_mapped.py
# │   ├── bench/
# │   │   ├── run_sota_comparison.py
# │   │   ├── run_agent_comparison.py
# │   │   └── generate_report.py
# │   ├── data/
# │   │   ├── download_benchmarks.py
# │   │   └── collect_sft_trajectories.py
# │   └── figures/
# │       ├── build_all_figures.py
# │       └── learning_curve.py
# ├── training/
# │   ├── configs/
# │   │   ├── sft_05b.yaml
# │   │   ├── grpo_05b.yaml
# │   │   └── gigpo_15b.yaml
# │   ├── kaggle/
# │   ├── colab/
# │   └── hf_space/
# ├── docs/
# │   ├── mkdocs.yml
# │   ├── docs/
# │   │   ├── index.md
# │   │   ├── quickstart.md
# │   │   ├── detectors.md
# │   │   ├── safety.md
# │   │   └── architecture.md
# │   └── assets/
# ├── playground/
# │   ├── web/
# │   │   ├── index.html
# │   │   ├── app.js
# │   │   └── style.css
# │   └── api/
# │       ├── app.py
# │       ├── Dockerfile
# │       └── requirements.txt
# ├── .github_templates/
# ├── pyproject.toml
# ├── Makefile
# ├── test_map.json
# ├── ralph.sh
# ├── prompt.template.md
# ├── progress.md
# ├── README.md
# ├── ARCHITECTURE.md
# ├── DECISIONS.md
# ├── CLAUDE.md
# ├── SECURITY.md
# ├── CONTRIBUTING.md
# ├── CHANGELOG.md
# ├── LICENSE
# └── CURSOR_MASTER.md
#
# Sister repos (separate GitHub repos, separate PyPI packages —
# create after the CLI launches):
#
# dataforge-evals — standalone evaluation harness.
# dataforge-agent-patterns — reusable agent primitives library.
# dataforge-dbt — dbt adapter.
# dataforge-airbyte — Airbyte source connector.
# dataforge-mcp — MCP server wrapping the CLI.
