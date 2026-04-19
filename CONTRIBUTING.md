# Contributing to DataForge

Thank you for considering a contribution to DataForge. This document
explains the process and standards.

## Before you start

1. **Read** `.cursor/rules/dataforge.md` — the always-applied rules.
2. **Read** `CLAUDE.md` — gotchas and conventions.
3. **Check** `specs/QUESTIONS.md` — your question may already be there.

## Workflow

1. **Fork** the repo and create a branch from `main`.
2. **Find (or write) the spec** for your change in `specs/SPEC_<module>.md`.
   If no spec exists, open an issue proposing one before writing code.
3. **Write the failing test first.** See `specs/SPEC_TEMPLATE.md § Appendix A`
   for the test-case format.
4. **Implement** the minimum code to make the test pass.
5. **Run the gates:**
   ```bash
   make lint && make type && make test-mapped FILE=<your_source_file>
   pytest tests/regression/ -x
   ```
6. **Commit** with a [Conventional Commit](https://www.conventionalcommits.org/)
   message (≤ 72 char subject).
7. **Open a PR** against `main`. The PR description should reference the spec
   and explain *why*, not just *what*.

## Code standards

- Python 3.11 / 3.12 (`requires-python = ">=3.11,<3.13"`). Use modern syntax (`dict | None`, not `Optional[Dict]`).
- Type hints on every parameter and return value. `mypy --strict` must pass.
- Google-style docstrings with one-line summary, Args, Returns, Raises.
- No `print()` in library code — use `logging`. CLI output uses `rich`.
- No global mutable state. No silent catch-all exceptions.
- No TODO/FIXME in merged code. Open an issue instead.

## Adding a dependency

Before adding any dependency to `pyproject.toml`, you must:

1. Justify it in one sentence in `ARCHITECTURE.md § Dependencies`.
2. If you cannot justify it in one sentence, you don't need it.

## Tests

- **Never delete or weaken an existing test.** If a test is wrong: open an
  issue, update the spec, update the test — in that order.
- **Never skip a flaky test.** Diagnose and fix it.
- Coverage target: ≥ 90% line, ≥ 80% branch.
- Mutation score target: ≥ 85%.

## Reporting bugs

Open a GitHub issue with:

1. What you expected to happen.
2. What actually happened (include the full traceback).
3. The command and version (`dataforge --version`).
4. A minimal reproducer if possible.

## Code of conduct

Be respectful, be constructive, be specific. We don't have a formal CoC
document yet, but the bar is: would you say this in a code review at work?
