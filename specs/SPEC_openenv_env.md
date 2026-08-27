# SPEC: OpenEnv-Compatible DataForge RL Environment

> Status: Accepted
> Owner: pranesh
> Last updated: 2026-05-20

## 1. Purpose

Provide a typed OpenEnv-compatible environment inside `dataforge` for
data-quality detection, diagnosis, root-cause analysis, and verified repair
experiments. The environment contains no hidden LLM calls and keeps durable file
mutation out of the training loop.

## 2. Outcomes

- [x] `DataForgeEnv.reset()` returns a valid observation with visible rows and
  remaining step budget.
- [x] All eight action types produce structured `StepResult` objects:
  `INSPECT_ROWS`, `SQL_QUERY`, `STAT_TEST`, `PATTERN_MATCH`, `HYPOTHESIS`,
  `DIAGNOSE`, `FIX`, `ROOT_CAUSE`.
- [x] Terminal reward matches `REWARD_DESIGN.md`:
  `detection_rate * 0.40 + fix_rate * 0.60 - false_positives * fp_rate`.
- [x] `FIX` actions pass through SafetyFilter -> SMTVerifier before acceptance.
- [x] `ROOT_CAUSE(error_indices: list[int])` performs read-only analyzer-backed
  root-set analysis and returns structured result data.
- [x] Step-budget termination auto-finalizes the episode.
- [x] HTTP endpoints return OpenEnv-compatible JSON.
- [x] HTTP `/reset` creates an `episode_id`; `/step`, `/state`, and `/close`
  accept optional `episode_id` while preserving legacy no-id behavior.
- [x] `SQL_QUERY` is sandboxed to a single read-only query over the registered
  `data` relation and rejects DuckDB file, network, extension, table-function,
  and multi-statement escape attempts.
- [x] The legacy `data_quality_env` package is gone and stays gone; regression tests
      assert the name is not importable rather than that it still imports (2026-08-27).

## 3. Scope

**IN**:

- `dataforge/env/environment.py` for `DataForgeEnv`.
- `dataforge/agent/tool_actions.py` for discriminated action models and
  `parse_action()`.
- `dataforge/agent/scratchpad.py` for in-episode hypothesis tracking.
- `dataforge/env/observation.py` for observation models.
- `dataforge/env/reward.py` for dense and terminal rewards.
- `dataforge/env/server.py` for FastAPI protocol endpoints.
- `dataforge/causal/` for read-only root-cause analysis used by `ROOT_CAUSE`.
- `openenv.yaml` and `Dockerfile.env`.

**OUT**:

- Agent loop or runtime LLM integration.
- Training orchestration.
- Durable CSV mutation or transaction log creation inside the environment.
- Reintroduction of the deleted `data_quality_env/` hackathon package in any form.
- Persistent multi-user state beyond an in-memory local session registry.

## 4. Constraints

- Python `>=3.11,<3.13`.
- `reset()` should complete under 500 ms on the hospital fixture.
- `step()` should complete under 200 ms for normal local actions.
- SQL actions are read-only, single-statement, and limited to the registered
  in-memory `data` relation.
- `causal-learn` must not receive NaN values.
- `openenv-core[core]` is optional and must not be required by the base CLI.

## 5. Prior Decisions

- Three severity tiers: SAFE / REVIEW / UNSAFE.
- Z3 remains the SMT solver.
- Safety pipeline: SafetyFilter -> SMTVerifier -> transaction log.
- INSPECT_ROWS returns up to 20 rows, not 20 cells.
- Legacy row-level noise with epsilon `0.15` remains the current noise model.
- 2026-05-15 decision adds `ROOT_CAUSE` as the eighth typed action.

## 6. Task Breakdown

### 6.1 Typed Actions

- Acceptance: `parse_action()` discriminates all eight action types and rejects
  invalid payloads.
- Depends on: none.
- Estimated complexity: M.

### 6.2 Environment Core

- Acceptance: `reset()`, `step()`, and `state()` return stable typed results;
  errors become observations rather than uncaught exceptions.
- Depends on: 6.1.
- Estimated complexity: L.

### 6.3 Reward Engine

- Acceptance: reward constants match `REWARD_DESIGN.md`, including
  `R_ROOT_CAUSE = 0.10`.
- Depends on: 6.1.
- Estimated complexity: M.

### 6.4 Root-Cause Action

- Acceptance: valid issue indices return minimal root indices; invalid indices
  return structured rejection; the action never mutates source data.
- Depends on: causal analyzer.
- Estimated complexity: M.

### 6.5 HTTP Server

- Acceptance: `/reset`, `/step`, `/state`, `/close`, `/health`, `/metadata`, and
  `/schema` expose the current action and observation contracts; optional
  `episode_id` routing prevents concurrent local sessions from sharing state.
- Depends on: 6.2.
- Estimated complexity: M.

### 6.6 SQL sandbox

- Acceptance: `SQL_QUERY` rejects multi-statement queries, non-SELECT
  statements, references to tables other than `data`, and DuckDB I/O or
  extension-loading functions.
- Depends on: 6.2.
- Estimated complexity: S.

## 7. Verification

- Unit: `tests/unit/test_tool_actions.py`, `tests/unit/test_reward.py`,
  `tests/unit/test_observation.py`, `tests/unit/test_scratchpad.py`,
  `tests/unit/test_sql_query.py`, `tests/unit/test_causal_root_cause.py`.
- Integration: `tests/integration/test_openenv_spec.py`.
- Property: `tests/property/test_reward_bounds.py`.
- Regression: `tests/regression/test_env.py`.
- Mapped gates: `make test-mapped FILE=dataforge/agent/tool_actions.py` and
  `make test-mapped FILE=dataforge/env/environment.py`.

## 8. Acceptance Gate

- [x] Section 2 outcomes are met.
- [x] Regression tests pass unchanged.
- [x] `DECISIONS.md` records the action-space decisions.
- [x] `ARCHITECTURE.md` documents the causal dependencies.

## Appendix A - Toy Cases

### Case A.1: Reset returns valid observation

Input: `env.reset(seed=42)`.
Expected output: an observation with visible rows and nonzero step budget.
Reasoning: validates lifecycle startup.

### Case A.2: SQL rejects writes

Input: `SQL_QUERY` with `DROP TABLE data`.
Expected output: structured rejection without episode termination.
Reasoning: protects read-only environment behavior.

### Case A.3: FIX accepted after gates

Input: a valid `FIX` for a known decimal-shift issue.
Expected output: positive reward after SafetyFilter and SMTVerifier acceptance.
Reasoning: validates the repair gate sequence.

### Case A.4: ROOT_CAUSE finds chain root

Input: selected errors on a chain `discount_pct -> order_total -> tax`.
Expected output: only the upstream selected error is returned as root.
Reasoning: validates the Week 10 analyzer contract.

### Case A.5: Episode sessions are isolated

Input: two `/reset` calls followed by one `/step` using only the first `episode_id`.
Expected output: first `/state?episode_id=...` has `step_count == 1`; second remains `0`.
Reasoning: prevents module-global state contamination between OpenEnv clients.

### Case A.6: SQL file escape is rejected

Input: `SQL_QUERY` with `SELECT * FROM read_csv_auto('secret.csv')`.
Expected output: structured rejection and no DuckDB execution.
Reasoning: protects local files and network-capable DuckDB extensions.
