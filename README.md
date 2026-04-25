# DataForge

DataForge currently ships a real Week 3 CLI for CSV profiling and repair.

This repository now includes shipped detectors, deterministic repairers,
constitutional safety gating, SMT-backed structural verification, reversible
transaction logs, and real-world benchmark infrastructure. The hosted
playground, warehouse integrations, and trained model family remain future
work.

## Current Status

- `dataforge profile`, `dataforge repair`, `dataforge revert`, and `dataforge bench`
- Three shipped detectors: `type_mismatch`, `decimal_shift`, `fd_violation`
- Three shipped repairers with safety + verifier gating in the apply path
- Reversible transaction logs with byte-identical revert via source snapshots
- Benchmark/report generation infrastructure for Hospital / Flights / Beers
- `Makefile` targets for setup, lint, type-checking, and tests
- CI plus unit / integration / property / adversarial coverage

## Benchmark Results

<!-- BENCH:START -->
Generated from `eval/results/agent_comparison.json`.

| Method | Precision | Recall | F1 | Avg Steps | Quota Units |
| --- | --- | --- | --- | --- | --- |
| heuristic | 0.0000 | 0.0000 | 0.0000 | 134.33 | 0.0000 |
| llm_react | Skipped | Skipped | Skipped | Skipped | Skipped |
| llm_zeroshot | Skipped | Skipped | Skipped | Skipped | Skipped |
| random | 0.0038 | 0.0003 | 0.0005 | 150.33 | 0.0000 |

See `BENCHMARK_REPORT.md` for per-dataset tables, error bars, and citation-only SOTA rows.

Skipped methods in this run: DATAFORGE_LLM_PROVIDER must be set to groq.
<!-- BENCH:END -->

## Local Setup

```bash
make setup
make lint
make type
make test
```

Verification works on Linux, macOS, or Windows (with Git Bash as the
shell substrate for GNU Make). Requires Python 3.11 or 3.12
(`requires-python = ">=3.11,<3.13"`).

### Windows-specific setup

```powershell
# Install Python 3.12 and GNU Make if not present
winget install -e --id Python.Python.3.12
winget install -e --id ezwinports.make

# Create and activate a project venv
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies and verify
python -m pip install -e ".[all]"
make lint && make type && make test
```

Git for Windows provides the Bash implementation the Makefile uses on Windows.
Do not rely on `C:\Windows\System32\bash.exe` (WSL).

## Environment Variables

Future provider keys belong in a root `.env` file that is gitignored and meant
to be loaded with `python-dotenv`.

- `GROQ_API_KEY`
- `GEMINI_API_KEY`
- `CEREBRAS_API_KEY`
- `OPENROUTER_API_KEY`
- `HF_TOKEN`

## Repository Docs

- [.cursor/rules/dataforge.md](.cursor/rules/dataforge.md) — always-applied rules
- [ARCHITECTURE.md](ARCHITECTURE.md) — system diagram and dependency justification
- [DECISIONS.md](DECISIONS.md) — technical decision log
- [CONTRIBUTING.md](CONTRIBUTING.md) — workflow and code standards
- [CLAUDE.md](CLAUDE.md) — living knowledge base for Cursor sessions
- [CURSOR_MASTER.md](CURSOR_MASTER.md) — full context and prompt pack
- [META_CONTEXT.md](META_CONTEXT.md) — meta-context (read before writing code)
- [FILE_STRUCTURE.md](FILE_STRUCTURE.md) — canonical target directory tree
- [SECURITY.md](SECURITY.md) — vulnerability reporting policy
- [specs/SPEC_TEMPLATE.md](specs/SPEC_TEMPLATE.md) — spec template for new modules

## License

Apache-2.0. See [LICENSE](LICENSE).
