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

Week 0 verification is designed for Linux or WSL/Git Bash with Python 3.11 or
3.12.

## Environment Variables

Future provider keys belong in a root `.env` file that is gitignored and meant
to be loaded with `python-dotenv`.

- `GROQ_API_KEY`
- `GEMINI_API_KEY`
- `CEREBRAS_API_KEY`
- `OPENROUTER_API_KEY`
- `HF_TOKEN`

## Repository Docs

- [.cursor/rules/dataforge.md](.cursor/rules/dataforge.md)
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [DECISIONS.md](DECISIONS.md)
- [specs/SPEC_TEMPLATE.md](specs/SPEC_TEMPLATE.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
