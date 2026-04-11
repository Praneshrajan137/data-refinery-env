---
title: Data Quality Env
emoji: "🔍"
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
app_port: 7860
tags:
  - openenv
  - data-quality
  - rl-environment
---

# Data Quality Validation and Cleaning Pipeline

Data engineers spend 40-60% of their time finding and fixing data quality issues before analytics, billing, or ML pipelines can run. No existing OpenEnv environment benchmarks whether RL agents can learn this work. **This environment fills that gap.**

`data_quality_env` is a research-grade OpenEnv environment where agents must inspect tabular datasets under partial observability, diagnose concrete issues against ground truth, apply fixes when deterministic corrections exist, and finalize with a graded score. It is designed to be genuinely hard: the hard task requires multi-hop cross-table reasoning, root-cause identification for cascading errors, hidden business rule discovery, temporal consistency verification, and strategic exploration of 250 rows within a 65-step budget.

## Key Results

We find that rule-based heuristic agents achieve moderate scores on single-table tasks but plateau at ~0.43 average across all tasks, while random agents score 0.0 uniformly. Task 3 (multi-table integrity auditing) proves especially challenging: it requires compositional verification abilities that pure pattern matching cannot reliably achieve, suggesting this environment provides genuine signal for measuring agent reasoning capabilities.

### Benchmark Results (n=10 seeds per cell)

| Agent | Task 1 (Format) | Task 2 (Duplicate) | Task 3 (Integrity) | Average |
|-------|----------------:|-------------------:|-------------------:|--------:|
| Random         |    0.000 +/- 0.000 |    0.000 +/- 0.000 |    0.000 +/- 0.000 |   0.000 |
| Heuristic      |    0.100 +/- 0.000 |    0.593 +/- 0.000 |    0.610 +/- 0.000 |   0.434 |
| Oracle         |    1.000 +/- 0.000 |    1.000 +/- 0.000 |    1.000 +/- 0.000 |   1.000 |

The oracle baseline (`run_baseline.py`) confirms grading correctness. The gap between heuristic and oracle represents the genuine learning opportunity. Run `python benchmark.py --seeds 50` to reproduce with tighter confidence intervals.

## Formal MDP Specification

This environment defines a finite-horizon, deterministic Markov Decision Process:

```
M = (S, A, T, R, H, gamma)
```

**State space S.** Each state encodes: the full dataset (rows x columns), ground truth issue set, set of diagnosed issues, set of fixed issues, set of inspected row indices, false positive count, step counter, and finalization flag. Effective dimensionality: O(N * C + |GT| + step_count) where N = number of rows, C = number of columns, |GT| = ground truth issue count.

**Action space A.** Four action types with per-type cardinality:

| Action | Cardinality | Parameters |
|--------|------------:|------------|
| `inspect` | O(2^N) | subset of row indices (max 10), optional related_table |
| `diagnose` | O(N * C * 9) | row_index x column_name x 9 issue types |
| `fix` | O(N * C * V) | row_index x column_name x value space |
| `finalize` | 1 | none |

**Transition function T: S x A -> S.** Deterministic. Each action updates diagnosed/fixed issue sets, inspected indices, and step counter. The finalize action transitions to a terminal state.

**Reward function R: S x A -> R.**

| Signal | Range | Trigger |
|--------|------:|---------|
| Correct diagnosis | +0.10 | Issue matches ground truth (row, column) |
| Type bonus | +0.05 | Issue type also matches |
| Correct fix | +0.15 | Fix value matches expected |
| Partial fix (numeric) | +0.075 | Fix within 1% relative error |
| Partial fix (string) | +0.075 | Fix has >= 85% SequenceMatcher similarity |
| Justification bonus | +0.05 | Fix includes justification text |
| Exploration bonus | +0.01 * k | k = undiscovered ground truth issues in inspected rows |
| Coverage bonus | +0.005 * n * (1-c) | n = new rows inspected, c = coverage ratio (pseudo-count) |
| False positive | -0.05 | Diagnosis does not match any ground truth entry |
| Wrong fix | -0.08 | Fix value does not match expected |
| Late-step penalty | -0.02 | Applied per step after 80% of budget consumed |
| Re-inspection penalty | -0.01 | All inspected rows were already seen |
| Final score | variable | `detection_rate * 0.40 + fix_rate * 0.60 - FP * 0.05` |

**Horizon H.** Finite, task-dependent: H in {30, 50, 65}.

**Discount factor.** gamma = 1.0 (undiscounted finite-horizon).

**Partial observability.** The agent observes at most 10 rows per inspect action. Column statistics (type, null count, unique count, sample values) provide aggregate information. The full dataset is never revealed in a single observation.

**Information-theoretic properties.** The exploration bonus creates a submodular reward structure over the set of inspected rows, encouraging diverse coverage. Re-inspection penalty prevents degenerate policies. The late-step penalty creates time pressure that rewards efficient exploration.

## Why This Matters for RL

This environment has properties that make it a strong RL benchmark:

- **Dense step-level rewards** -- every correct diagnosis (+0.10), fix (+0.15), and even productive inspection (+0.01 per undiscovered issue in batch) provides learning signal, not just a binary end-of-episode outcome
- **Natural curriculum** -- three difficulty tiers (easy -> medium -> hard) with increasing dataset size, issue diversity, and reasoning depth
- **Partial observability** -- the agent sees at most 10 rows per inspect action from datasets of 50-250 rows; it must learn to prioritize which regions to explore
- **Explore-exploit tradeoff** -- an information-theoretic exploration bonus rewards agents that learn to inspect rows likely to contain issues, creating a genuine resource allocation problem
- **Partial credit** -- numerically close fixes (within 1%) earn half reward, testing precision vs approximation
- **Adversarial clean rows** -- All three tasks contain rows that look suspicious but are valid (unusual TLDs, boundary dates, shared names across different people, near-boundary quantities/discounts), directly testing false-positive discipline
- **Procedural generation** -- `reset(task_id, seed=N)` generates unlimited unique episodes for any seed, enabling statistically rigorous evaluation

## What Makes the Hard Task Hard

Task 3 (Integrity Auditor) requires capabilities beyond pattern matching:

1. **Cross-table referential integrity**: Check whether `product_id` values exist in a separate products table
2. **Arithmetic consistency**: Verify `order_total = qty * unit_price * (1 - discount/100)` across 250 rows
3. **Cascading errors**: A discount of 75% (should be 7.5%) causes the total to also be wrong -- the agent must identify the *root cause*, not just the symptom. A quantity inflated 10x creates a similar cascade where the total matches the original quantity.
4. **Floating-point traps**: Totals off by $0.01 from truncation instead of rounding -- tests numeric precision
5. **Semantic duplicates**: Two orders with identical customer + product + date but different order IDs -- requires row-comparison reasoning
6. **Business rule enforcement**: Constraints from metadata (max discount 50%, min discount 0%, valid years 2024-2025, quantity 1-100, min unit price $0.01, max shipping window 730 days)
7. **Hidden business rules**: `min_unit_price` and `min_discount_pct` must be discovered from business_rules metadata, not from data patterns
8. **Temporal consistency**: Ship dates before order dates (including subtle 1-year-off errors) and ship dates years in the future
9. **Null value derivation**: Missing order_total that is computable from other fields -- requires formula knowledge
10. **Strategic exploration**: 250 rows with a 65-step budget means the agent cannot brute-force inspect everything -- it must use column statistics and inspection bonuses to prioritize
11. **Adversarial clean rows**: 10 rows with edge-case values ($0.01 products, 49.99% discounts, same-day shipping, quantity at max boundary, near-zero discounts, same customer different orders) that penalize over-eager detection

## Tasks

| Task | ID | Difficulty | Dataset | Issues | Fixable | Det-only | Max steps |
|---|---|---|---|---:|---:|---:|---:|
| Format Fixer | `task_1_format_fixer` | Easy | 50 customers | 8 | 5 | 3 | 30 |
| Duplicate Detective | `task_2_duplicate_detective` | Medium | 120 contacts | 15 | 8 | 7 | 50 |
| Integrity Auditor | `task_3_integrity_auditor` | Hard | 250 orders + 42 products | 32 | 29 | 3 | 65 |

### Task 1: Format Fixer

Single-table formatting cleanup: malformed emails (missing/double @), invalid calendar dates (Feb 30, Apr 31), incomplete phone numbers, and zip code issues. Five issues have deterministic fixes; three are detection-only. Includes 4 adversarial clean rows (unusual TLD, leap year boundary, leading-zero zip, phone with extension) that test false-positive discipline.

### Task 2: Duplicate Detective

Exact duplicates (4 rows requiring DELETE_ROW), near-duplicates with typos, domain misspellings, phone reformatting, and transposed first/last names. Also includes missing values and type mismatches. Requires comparing rows in memory to identify near-duplicate pairs. Includes 5 adversarial clean rows (shared first name across different people, boundary date, international phone format, rare email domain, shared city) that test false-positive discipline.

### Task 3: Integrity Auditor

Multi-table audit across orders (250 rows) and products (42 items). 32 issues spanning 9 issue types: referential integrity (4, including multi-hop product ID swap), cross-field arithmetic (8), outliers (3), business rule violations (7), category mismatches (3), cascading discount errors (2), floating-point precision (1), null derivation (1), semantic duplicate (1), type mismatch (1, string-as-integer), format error (1, date format). 10+ adversarial clean rows test false-positive discipline. Requires cross-table reasoning, root-cause identification, and hidden business rule discovery.

## Architecture

```
+------------------+          action          +-------------------+
|                  | -----------------------> |                   |
|    RL Agent      |                          |   Environment     |
|  (inference.py)  | <----------------------- | (data_quality_    |
|                  |       observation        |  environment.py)  |
+------------------+                          +-------------------+
                                                      |
                                              +-------+-------+
                                              |               |
                                        +-----+-----+  +-----+-----+
                                        |  Dataset   |  |  Ground   |
                                        |  (rows,    |  |  Truth    |
                                        |   schema,  |  |  (issues, |
                                        |   rules)   |  |   fixes)  |
                                        +-----------+  +-----------+

Episode lifecycle:
  1. reset(task_id, seed?) -> initial observation (schema + sample rows)
  2. step(action) -> observation (reward, visible data, hints)  [loop]
  3. finalize -> terminal observation (final graded score)

Reward computation pipeline:
  action -> match against ground truth -> per-step reward
         -> accumulate into cumulative_reward
         -> at terminal: compute detection_rate, fix_rate, FP penalty
         -> final_score = 0.40 * detection + 0.60 * fix - FP * 0.05
```

## Action Space

| Action | Required fields | Optional fields | Purpose |
|---|---|---|---|
| `inspect` | at least one of `row_indices` or `column_names` | `related_table` | reveal rows, column stats, or secondary-table context |
| `diagnose` | `row_index`, `column_name`, `issue_type` | `related_table` | report a suspected issue |
| `fix` | `row_index`, `column_name`, `fix_type`, `justification` | `new_value` | apply a deterministic correction |
| `finalize` | none | none | end the episode and receive the final score |

### Fix types

| Fix type | `new_value` | Meaning |
|---|---|---|
| `correct_value` | required | replace the current value with the expected value |
| `delete_row` | forbidden | remove a duplicate row |
| `impute` | optional | let the environment compute the imputed value |
| `standardize` | optional | let the environment normalize formatting |

### Issue types

`format_error`, `missing_value`, `duplicate`, `near_duplicate`, `type_mismatch`, `outlier`, `referential_integrity`, `cross_field`, `business_rule`

## Observation Space

| Field | Type | Meaning |
|---|---|---|
| `task_id` | `str` | active task identifier |
| `schema_info` | `dict[str, str]` | column-to-type mapping |
| `total_rows` | `int` | row count for the active dataset |
| `visible_rows` | `list[dict]` | rows returned by the latest inspect |
| `column_statistics` | `dict[str, object]` | inspect-time column summaries |
| `secondary_table_rows` | `list[dict]` | related-table rows (Task 3) |
| `action_result` | enum | feedback: `correct`, `incorrect`, `partial`, `already_found`, `error`, `complete` |
| `reward_delta` | `float` | step reward or penalty |
| `cumulative_reward` | `float` | running episode reward |
| `issues_found` | `int` | correctly identified issues |
| `issues_remaining_hint` | enum | coarse progress hint: `none`, `few`, `some`, `many` |
| `difficulty_level` | `str` | task difficulty: `easy`, `medium`, `hard` |
| `steps_taken` | `int` | steps consumed |
| `max_steps` | `int` | budget for this task |
| `done` | `bool` | whether the episode has ended |
| `grader_diagnostics` | `dict` or `null` | detailed scoring breakdown at episode end (per-issue hit/miss, formula decomposition) |

## Reward Design

The reward function is designed for both trajectory learning and submission-time evaluation.

### Step rewards

| Event | Reward |
|---|---:|
| Correct diagnosis | `+0.10` |
| Correct issue type bonus | `+0.05` |
| Correct fix | `+0.15` |
| Numerically close fix (within 1%) | `+0.075` |
| String-similar fix (85%+ similarity) | `+0.075` |
| Justification bonus | `+0.05` |
| Exploration bonus (per undiscovered issue in inspected batch) | `+0.01` |
| Coverage bonus (pseudo-count, decays with coverage ratio) | `+0.005 * (1 - coverage)` |
| False positive diagnosis | `-0.05` |
| Wrong fix | `-0.08` |
| Late-step penalty (after 80% of budget) | `-0.02` per step |
| Re-inspection penalty (all rows already seen) | `-0.01` |

### Final score

```text
score = detection_rate * 0.40 + fix_rate * 0.60 - false_positives * 0.05
```

- Dense local rewards encourage productive exploration
- The final score reflects total task completion
- False-positive penalty is **uncapped** (linear) -- no free diagnoses. If total diagnoses exceed 2x the ground truth issue count, the penalty rate doubles (spam deterrent)
- Re-inspecting already-seen rows incurs a small penalty (-0.01), encouraging strategic exploration
- The exploration bonus creates an information-theoretic signal for learning inspection strategies
- String partial credit uses `SequenceMatcher` similarity for near-correct string fixes

## Related Work

| System | Focus | RL-Compatible | Dense Rewards | Partial Obs. | Multi-Task | Procedural |
|--------|-------|:-------------:|:-------------:|:------------:|:----------:|:----------:|
| DataPerf (Mazumder+ 2023) | Dataset curation | No | N/A | N/A | No | No |
| CleanLab (Northcutt+ 2021) | Label errors | No | N/A | N/A | No | No |
| TextWorld (Cote+ 2019) | Text games | Yes | Sparse | Yes | Yes | Yes |
| MiniWoB++ (Liu+ 2018) | Web interaction | Yes | Sparse | Yes | Yes | No |
| WebArena (Zhou+ 2024) | Web agents | Yes | Sparse | Yes | Yes | No |
| SWE-bench (Jimenez+ 2024) | Code repair | No | Binary | No | Yes | No |
| **data_quality_env** | **Tabular DQ** | **Yes** | **Dense** | **Yes** | **Yes (3)** | **Yes** |

Key differentiators: (1) dense per-step rewards with partial credit, (2) procedural generation for unlimited unique episodes, (3) partial observability with information-theoretic exploration bonuses, (4) multi-table reasoning with cascading error chains.

## Procedural Generation

Static datasets provide reproducibility; procedural generation provides generalization:

```python
from server.data_quality_environment import DataQualityEnvironment

env = DataQualityEnvironment()
obs = env.reset("task_3_integrity_auditor")                      # static dataset (default)
obs = env.reset("task_3_integrity_auditor", seed=42)             # procedural: deterministic
obs = env.reset("task_3_integrity_auditor", seed=99)             # procedural: different data
obs = env.reset("task_3_integrity_auditor", seed=42, noisy=True) # stochastic observation mode
```

- `seed=None` (default): loads static JSON files (backward compatible)
- `seed=N`: generates a unique dataset deterministically from seed N
- Same seed always produces identical episodes (reproducibility)
- Different seeds produce different datasets (generalization testing)
- `noisy=True`: enables stochastic observation mode (POMDP) where inspected row values may be perturbed (string truncation, case flips, numeric jitter) to simulate real-world data pipeline noise

## Environment Variables

| Variable | Default | Used by |
|---|---|---|
| `HF_TOKEN` | required for inference | `inference.py` |
| `ENV_URL` | `http://localhost:7860` | client and inference |
| `API_BASE_URL` | `https://api.openai.com/v1` | inference |
| `MODEL_NAME` | `gpt-4.1-mini` | inference |
| `TEMPERATURE` | `0.1` | inference |
| `MAX_TOKENS` | `1024` | inference |

## Quick Start

### Install dependencies

```bash
pip install -e .
pip install -e ".[server]"
```

### Run the automated checks

```bash
python test_env.py          # 389 assertions, 54 test functions
python validate.py --skip-docker
```

### Run benchmarks

```bash
python benchmark.py --seeds 10    # Full benchmark (random + heuristic)
python random_baseline.py         # Random agent only
python heuristic_baseline.py      # Heuristic agent only
```

### Run the local server

```bash
uvicorn server.app:app --host 0.0.0.0 --port 7860
```

The server exposes `GET /health`, `GET /`, and `WS /ws`.

### Run the deterministic baseline

```bash
python run_baseline.py --url http://localhost:7860
```

### Run the LLM baseline

```bash
set HF_TOKEN=your-key
python inference.py
```

## Verified Results

| Check | Result |
|---|---|
| Syntax compilation | All modules pass `py_compile` |
| Test suite | `python test_env.py` -- 389 passed, 0 failed, 54 test functions |
| Validator | `python validate.py --skip-docker` -- all checks pass |
| Health check | `/health` returns healthy |
| Dataset integrity | 28/28 verification gates pass |
| Procedural generation | Deterministic across seeds, backward compatible |

## Grader Diagnostics

On episode termination (via `finalize` or auto-finalize at max steps), the observation includes a `grader_diagnostics` field with a detailed scoring breakdown:

```python
obs = env.step(DataQualityAction(action_type="finalize"))
diag = obs.grader_diagnostics
# diag = {
#   "final_score": 0.8500,
#   "formula": {
#     "detection_rate": 0.875, "detection_weight": 0.4,
#     "fix_rate": 0.923, "fix_weight": 0.6,
#     "false_positives": 1, "fp_penalty_rate": 0.05,
#     "fp_penalty_total": 0.05, "raw_score": 0.854
#   },
#   "counts": {"total_issues": 8, "fixable_issues": 5, ...},
#   "per_issue": [{"row": 3, "column": "email", "detected": True, "fixed": True}, ...],
#   "noisy_mode": False
# }
```

This enables RL researchers to debug reward attribution, identify which issue types agents struggle with, and run ablation studies on reward components.

## Stochastic Observation Mode

Enable `noisy=True` for POMDP training where observed values are stochastically perturbed:

```python
obs = env.reset("task_1_format_fixer", seed=42, noisy=True)
# Inspected row values may have:
# - String truncation (drop last 1-3 chars)
# - Case flips (swapcase)
# - Numeric jitter (+/- 2%)
# The underlying dataset is NOT modified — only the observation copy.
```

This forces agents to be robust to observation uncertainty, a realistic constraint since real-world data pipelines often introduce noise during ETL.

## Trajectory Analysis

`analyze_trajectory.py` replays recorded trajectories and produces detailed analytics:

```bash
python analyze_trajectory.py --demo task_1_format_fixer -v  # demo with verbose output
python analyze_trajectory.py trajectory.json                 # analyze recorded trajectory
python analyze_trajectory.py                                 # demo on all 3 tasks
```

Output includes per-step reward breakdown, action-type distribution, wasted-step analysis, and grader diagnostics summary. See `REWARD_DESIGN.md` for the formal reward design document.

## Project Structure

```text
data_quality_env/
|- openenv.yaml             # OpenEnv spec (v1, port 7860, 3 tasks)
|- README.md
|- pyproject.toml
|- compat.py                # openenv-core import resolution
|- models.py                # Pydantic v2 schemas (Action/Observation/State)
|- client.py                # WebSocket client (openenv-native + fallback)
|- inference.py             # Multi-turn LLM agent
|- run_baseline.py          # Deterministic ground-truth oracle
|- random_baseline.py       # Random-action lower bound
|- heuristic_baseline.py    # Rule-based heuristic agent
|- benchmark.py             # Multi-seed benchmark runner
|- generate_datasets.py     # Dataset generator (static + procedural)
|- test_env.py              # 360 assertions, 49 test functions
|- validate.py              # Pre-submission validator
|- analyze_trajectory.py    # Trajectory replay and analytics tool
|- REWARD_DESIGN.md         # Formal reward design document
|- datasets/                # 7 JSON files (datasets + ground truth)
|- benchmark_results/       # Benchmark outputs (JSON, Markdown, LaTeX)
`- server/
   |- app.py                # ASGI server (openenv/FastAPI/Starlette)
   |- data_quality_environment.py  # Core RL environment (~1400 lines)
   `- Dockerfile            # Multi-stage production build
```

## Deployment

### Docker

```bash
docker build -t data_quality_env -f server/Dockerfile .
docker run -p 7860:7860 data_quality_env
curl http://localhost:7860/health
```

### Hugging Face Spaces

```bash
openenv push
```

Port 7860, health check at `/health`, WebSocket at `/ws`.

## Design Rationale

The reward architecture draws on established RL principles:

- **Dense reward shaping** -- per-step rewards for diagnose/fix avoid sparse-reward pathologies. The final-score formula acts as a potential-based shaping function that preserves optimal policies while accelerating learning.
- **Information-theoretic exploration** -- the exploration bonus (with diminishing returns for re-inspected rows) creates a genuine curiosity signal. Agents that learn to prioritize unexplored regions outperform those that re-inspect known areas.
- **Partial observability as curriculum** -- agents see at most 10 rows per inspect from datasets of 50-250 rows. This forces strategic resource allocation and prevents brute-force solutions.
- **Adversarial clean rows** -- Task 3 includes rows that appear suspicious (e.g., $0.01 unit price, 49.99% discount, same-day shipping, quantity at max boundary) but are valid, directly testing false-positive discipline.
- **Anti-exploit mechanisms** -- uncapped FP penalty with spam multiplier, re-inspection penalty, and late-step penalty prevent degenerate policies.
- **Procedural generation** -- seeded random generation enables statistically rigorous evaluation across unlimited unique episodes while maintaining full reproducibility.

Data quality costs enterprises an estimated $3.1 trillion per year in the US alone (IBM). No existing OpenEnv environment benchmarks whether RL agents can learn this high-value work. This environment fills that gap with a progressively challenging curriculum covering 9 issue types across single-table and multi-table scenarios.

## License

MIT. See `LICENSE`.
