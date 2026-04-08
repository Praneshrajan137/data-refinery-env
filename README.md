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

Data engineers spend 40–60% of their time finding and fixing data quality issues before analytics, billing, or ML pipelines can run. No existing OpenEnv environment benchmarks whether RL agents can learn this work. **This environment fills that gap.**

`data_quality_env` is a real-world OpenEnv environment where agents must inspect tabular datasets under partial observability, diagnose concrete issues against ground truth, apply fixes when deterministic corrections exist, and finalize with a graded score. It is designed to be genuinely hard: the hard task requires multi-hop cross-table reasoning, root-cause identification for cascading errors, and strategic exploration of 250 rows within a 65-step budget.

## Why This Matters for RL

This environment has properties that make it a strong RL benchmark:

- **Dense step-level rewards** — every correct diagnosis (+0.10), fix (+0.15), and even productive inspection (+0.01 per undiscovered issue in batch) provides learning signal, not just a binary end-of-episode outcome
- **Natural curriculum** — three difficulty tiers (easy → medium → hard) with increasing dataset size, issue diversity, and reasoning depth
- **Partial observability** — the agent sees at most 10 rows per inspect action from datasets of 50–250 rows; it must learn to prioritize which regions to explore
- **Explore-exploit tradeoff** — an information-theoretic exploration bonus rewards agents that learn to inspect rows likely to contain issues, creating a genuine resource allocation problem
- **Partial credit** — numerically close fixes (within 1%) earn half reward, testing precision vs approximation
- **Adversarial clean rows** — Task 3 contains rows that look suspicious but are valid ($0.01 products, 49.99% discounts, same-day shipping), directly testing false-positive discipline

## What Makes the Hard Task Hard

Task 3 (Integrity Auditor) requires capabilities beyond pattern matching:

1. **Cross-table referential integrity**: Check whether `product_id` values exist in a separate products table
2. **Arithmetic consistency**: Verify `order_total = qty * unit_price * (1 - discount/100)` across 250 rows
3. **Cascading errors**: A discount of 75% (should be 7.5%) causes the total to also be wrong — the agent must identify the *root cause*, not just the symptom
4. **Floating-point traps**: A total of `206.789` where the correct value is `206.79` — tests numeric precision
5. **Semantic duplicates**: Two orders with identical customer + product + date but different order IDs — requires row-comparison reasoning
6. **Business rule enforcement**: Constraints from metadata (max discount 50%, valid years 2024–2025, quantity 1–100)
7. **Strategic exploration**: 250 rows with a 65-step budget means the agent cannot brute-force inspect everything — it must use column statistics and inspection bonuses to prioritize

## Tasks

| Task | ID | Difficulty | Dataset | Issues | Fixable | Max steps |
|---|---|---|---|---:|---:|---:|
| Format Fixer | `task_1_format_fixer` | Easy | 50 customers | 8 | 5 | 30 |
| Duplicate Detective | `task_2_duplicate_detective` | Medium | 120 contacts | 15 | 8 | 50 |
| Integrity Auditor | `task_3_integrity_auditor` | Hard | 250 orders + 42 products | 21 | 18 | 65 |

### Task 1: Format Fixer

Single-table formatting cleanup: malformed emails (missing/double @), invalid calendar dates (Feb 30, Apr 31), incomplete phone numbers, and zip code issues. Five issues have deterministic fixes; three are detection-only.

### Task 2: Duplicate Detective

Exact duplicates (4 rows requiring DELETE_ROW), near-duplicates with typos, domain misspellings, phone reformatting, and transposed first/last names. Also includes missing values and type mismatches. Requires comparing rows in memory to identify near-duplicate pairs.

### Task 3: Integrity Auditor

Multi-table audit across orders and products. The agent must reason about referential integrity, arithmetic consistency, cascading errors, floating-point precision, semantic duplicates, business rules, category mismatches, and statistical outliers — all within a tight step budget that forces strategic inspection.

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

## Reward Design

The reward function is designed for both trajectory learning and submission-time evaluation.

### Step rewards

| Event | Reward |
|---|---:|
| Correct diagnosis | `+0.10` |
| Correct issue type bonus | `+0.05` |
| Correct fix | `+0.15` |
| Numerically close fix (within 1%) | `+0.075` |
| Justification bonus | `+0.05` |
| Exploration bonus (per undiscovered issue in inspected batch) | `+0.01` |
| False positive diagnosis | `-0.05` |
| Wrong fix | `-0.08` |
| Late-step penalty (after 80% of budget) | `-0.02` per step |

### Final score

```text
score = detection_rate * 0.40 + fix_rate * 0.60 - min(0.40, false_positives * 0.05)
```

- Dense local rewards encourage productive exploration
- The final score reflects total task completion
- False positives matter, but their penalty is capped
- The exploration bonus creates an information-theoretic signal for learning inspection strategies

## Environment Variables

| Variable | Default | Used by |
|---|---|---|
| `HF_TOKEN` | required for inference | `inference.py` |
| `ENV_URL` | `http://localhost:7860` | client and inference |
| `API_BASE_URL` | `https://api.openai.com/v1` | inference |
| `MODEL_NAME` | `gpt-4o-mini` | inference |
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
python test_env.py
python validate.py --skip-docker
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
| Syntax compilation | All 7 core modules pass `py_compile` |
| Test suite | `python test_env.py` — 272 passed, 0 failed |
| Validator | `python validate.py --skip-docker` — 62 passed, 0 failed |
| Health check | `/health` returns healthy |
| Deterministic baseline | Perfect scores on all tasks (ground-truth-aware) |

## Project Structure

```text
data_quality_env/
|- openenv.yaml
|- README.md
|- pyproject.toml
|- compat.py          # openenv-core import resolution
|- models.py          # Pydantic v2 schemas (Action/Observation/State)
|- client.py          # WebSocket client (openenv-native + fallback)
|- inference.py       # Multi-turn LLM agent with [START]/[STEP]/[END] output
|- run_baseline.py    # Deterministic ground-truth baseline
|- test_env.py        # 272 automated tests
|- validate.py        # Pre-submission validator (62 checks)
|- datasets/          # 7 JSON files (datasets + ground truth)
`- server/
   |- app.py          # ASGI server (openenv/FastAPI/Starlette)
   |- data_quality_environment.py  # Core RL environment
   `- Dockerfile      # Multi-stage production build
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

## License

MIT. See `LICENSE`.
