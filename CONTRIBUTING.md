# Contributing To DataForge

Thank you for considering a contribution to DataForge. This document explains
the process and standards for the current alpha repository.

## Before You Start

1. Read `.cursor/rules/dataforge.md`.
2. Read `CLAUDE.md`.
3. Check the relevant spec under `specs/`.
4. Check `specs/QUESTIONS.md` for unresolved design questions.

## Workflow

1. Create a branch from `main`.
2. Find or write the spec for your change in `specs/SPEC_<module>.md`.
3. Write the failing test first for behavior changes.
4. Implement the smallest change that satisfies the spec.
5. Run the relevant gates:

```bash
make lint
make type
make test-mapped FILE=<your_source_file>
python -m pytest tests/regression/ -x
```

6. Update documentation if public behavior, commands, interfaces, or release
   evidence changed.
7. Commit with a Conventional Commit message.

## Code Standards

- Python 3.11 / 3.12 (`requires-python = ">=3.11,<3.13"`).
- Type hints on every public parameter and return value.
- Useful Google-style docstrings for public functions, classes, and modules.
- `dataforge/` is the canonical product package.
- `data_quality_env/` is a compatibility package; do not make the repository
  root importable.
- No `print()` in library code; use `logging`. CLI output uses `rich`.
- No global mutable state and no silent catch-all exceptions.
- No TODO/FIXME comments in merged code.

## Documentation Standards

- `README.md` is the public truth source.
- Do not claim unshipped integrations, hosted domains, or model-quality wins.
- Generated Hugging Face staging mirrors are deployment artifacts, not canonical
  docs.
- Metrics must point to a committed script, result artifact, or verifier output.

## Adding A Dependency

Before adding a dependency to `pyproject.toml`, justify it in
`ARCHITECTURE.md`. If it belongs only to MCP, playground, or model demo code,
keep it scoped to that package/surface instead of core runtime dependencies.

## Reporting Bugs

Open a GitHub issue with:

1. What you expected.
2. What happened, including the traceback.
3. The command and version (`dataforge --version`).
4. A minimal reproducer if possible.

## Conduct

Be respectful, constructive, and specific. The standard is a useful code review
at work: direct, kind, and grounded in evidence.
