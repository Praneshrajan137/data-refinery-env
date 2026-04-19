# DataForge Architecture

Week 0 note: this file captures the intended architecture and current
boundaries. The repository currently ships only the package skeleton, developer
tooling, CI workflow, and a smoke test.

## 1. System overview

The long-term product shape is:

`CLI -> agent loop -> detectors -> repairers -> safety filter -> SMT verifier -> transaction log -> data`

Week 0 ships only the repository scaffolding required to build toward that
design without overclaiming shipped behavior.

## 2. Layered structure

- **L1 - Detectors**: pure functions over tabular data, with no LLM calls.
- **L2 - Agent loop**: planning, tool use, and fix proposal orchestration.
- **L3 - Safety filter**: constitutional policy enforcement for proposed fixes.
- **L4 - SMT verifier**: schema- and constraint-aware validation before apply.
- **L5 - Transaction layer**: reversible repair logging with pre-state hashes.
- **L6 - Integrations**: adapters for dbt, Airbyte, warehouses, and MCP.

None of L1-L6 is implemented in Week 0 beyond directory structure.

## 3. Dependency guidance

The root `pyproject.toml` carries the planned dependency set for the first
substantive implementation wave. New dependencies should still be justified in
this document before they are added to the authoritative package config.

## 4. Week 0 boundaries

- The root `dataforge/` package contains only package markers and a minimal CLI
  app stub.
- CI validates `make setup`, `make lint`, `make type`, and `make test` against
  the empty scaffold.
- `tests/regression/test_env.py` is currently a placeholder smoke test. The
  larger regression suite is expected to land in Week 1.
