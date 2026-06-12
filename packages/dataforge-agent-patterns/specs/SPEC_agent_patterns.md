# SPEC: dataforge-agent-patterns

> Status: Draft
> Owner: pranesh
> Last updated: 2026-05-15

## 1. Purpose

Publish five reusable agentic primitives as a standalone Python package that
does not depend on the main `dataforge` package. The package should be small
enough for other agent projects to audit quickly.

## 2. Outcomes

- [ ] Five primitives are importable from `dataforge_agent_patterns`.
- [ ] Each primitive is pure Python, typed, documented, under 200 LOC, and has
  one doctest plus one pytest unit test.
- [ ] Runtime imports are stdlib + `pydantic`; `z3` is imported lazily only by
  `SMTVerifiedAction`.
- [ ] CI runs lint, type, tests, build, and import-boundary checks.
- [ ] PyPI publishing uses Trusted Publishing with job-level `id-token: write`.

## 3. Scope

**IN**:
- Progressive tool disclosure.
- Constitutional safety verdict wrapping.
- Reversible transaction decorator.
- Z3-backed structured action verification.
- Causal cascade detection over directed action effects.

**OUT**:
- Imports from `dataforge`.
- Provider SDK integrations.
- Runtime LLM calls.
- Local publishing to PyPI.

## 4. Constraints

- Python 3.11 / 3.12.
- No primitive file over 200 LOC.
- No PyPI API tokens in workflows.

## 5. Verification

- `python -m pytest`
- `python -m mypy src`
- `python -m ruff check src tests`
- `python -m build`
