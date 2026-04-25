# DataForge Architecture

Week 2 note: reversible CSV repair now ships with `dataforge repair` and
`dataforge revert`, backed by append-only transaction journals, immutable
source snapshots, and thin safety / verifier gates. The full agent loop and
constraint-aware safety stack remain future work.

## 1. System overview

The long-term product shape is:

`CLI -> agent loop -> detectors -> repairers -> safety filter -> SMT verifier -> transaction log -> data`

Week 2 ships: L1 detectors, repairers for the first 3 detector families, and
the CLI `profile`, `repair`, and `revert` commands.

## 2. Layered structure

- **L1 - Detectors** ✅ (partial): pure functions over tabular data, no LLM calls.
  - `type_mismatch` — flags values whose type conflicts with column majority.
  - `decimal_shift` — flags power-of-10 outliers in numeric columns.
  - `fd_violation` — flags rows violating declared functional dependencies.
- **L2 - Agent loop / repair orchestration** ✅ (partial): issue-to-fix routing,
  deterministic repairers, and optional cache-backed fd-violation fallback.
- **L3 - Safety filter** ✅ (thin Week 2 gate): typed allow/deny gate that blocks
  conflicting writes before apply. Constitutional policy enforcement is deferred.
- **L4 - SMT verifier** ✅ (thin Week 2 gate): typed accept/reject gate that
  validates structural applicability before apply. Full constraint reasoning is deferred.
- **L5 - Transaction layer** ✅ (partial): append-only JSONL journals,
  immutable source snapshots, post-state hash guard, and byte-exact revert.
- **L6 - Integrations**: adapters for dbt, Airbyte, warehouses, and MCP. (not started)

## 3. Dependency guidance

The root `pyproject.toml` carries the planned dependency set for the first
substantive implementation wave. New dependencies should still be justified in
this document before they are added to the authoritative package config.

Current active dependencies (Week 2):
- `pandas` — DataFrame handling for detectors.
- `pydantic` — Issue, Schema, FunctionalDependency models with validation.
- `typer` + `rich` — CLI application and terminal output.
- `pyyaml` — Schema YAML parsing.
- `httpx` + `tenacity` — LLM provider HTTP client with retry logic.
- `numpy` — Numeric computations in DecimalShiftDetector.
- `pandas-stubs` — strict typing support for pandas under `mypy --strict`.

Playground-only dependencies (scoped to `playground/api/requirements.txt`,
NOT in core `pyproject.toml` runtime deps — see DECISIONS.md):
- `fastapi` — async web framework for the stateless playground REST API.
- `uvicorn` — ASGI server for the HF Docker Space (single-worker).
- `slowapi` — IP-based rate limiting on playground POST endpoints.
- `python-multipart` — multipart form parsing for CSV upload in the playground.


## 4. Week 2 boundaries

- **L1 detectors**: 3 implemented (`type_mismatch`, `decimal_shift`, `fd_violation`).
  All are pure, no LLM calls, no I/O beyond receiving a DataFrame.
- **Repairers**: 3 implemented (`type_mismatch`, `decimal_shift`, `fd_violation`).
  `type_mismatch` and `decimal_shift` are deterministic; `fd_violation` prefers
  deterministic majority rules and uses cache-backed LLM fallback only when allowed.
- **CLI**:
  - `dataforge profile <csv> [--schema <yaml>]`
  - `dataforge repair <csv> [--schema <yaml>] [--dry-run | --apply]`
  - `dataforge revert <txn_id>`
- **Transaction layer**: `dataforge repair --apply` writes the transaction journal
  and source snapshot before mutating the CSV. `dataforge revert` restores the
  original bytes only when the current file matches the recorded post-state hash.
- **Safety / verifier gates**: Week 2 ships thin typed gates so the apply path
  already follows the final architecture shape without pretending the full policy
  or SMT stack is done.
- **Provider stub**: `dataforge.agent.providers` implements `groq` and `gemini`
  HTTP clients with retry logic. Only `fd_violation` repair may call them, and
  only with explicit permission plus cache-backed reuse.
- **Tests**: 118 tests, including property coverage for byte-identical revert.
- **Regression suite**: `tests/regression/test_env.py` remains the stable
  baseline; never modified without an auditable spec/issue path.
