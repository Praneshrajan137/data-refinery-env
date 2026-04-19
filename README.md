# DataForge

DataForge is in Week 0 scaffold state.

This repository currently ships the DataForge monorepo skeleton, packaging,
developer tooling, CI workflow, and a single smoke test. It does **not** yet
ship substantive detectors, repairers, agent orchestration, safety gates, SMT
verification, or reversible transactions.

## Current Status

- `dataforge/` package scaffold with a minimal Typer entrypoint
- `Makefile` targets for setup, lint, type-checking, and tests
- CI that validates the empty skeleton on Linux
- A placeholder regression smoke test that verifies `import dataforge`

## Local Setup

```bash
make setup
make lint
make type
make test
```

Week 0 verification works on Linux, macOS, or Windows (with Git Bash as the
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
