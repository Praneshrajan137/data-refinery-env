# dataforge-evals

`dataforge-evals` is an agent-agnostic evaluation harness for data-quality repair agents.

It gives any agent the same task, accepts only proposed cell fixes, and lets the grader compute exact precision, recall, F1, steps, failures, and free-tier quota usage. The harness can load DataForge's canonical Hospital, Flights, and Beers benchmark tasks when `dataforge_07` is installed, while the import namespace remains `dataforge` for the 0.1 line.
The PyPI package is not published yet; use the source install instructions
below until release ownership is configured.

```bash
pip install -e ".[dev]"
dataforge-evals run --agent mock --dataset synthetic --trials 3
```

## Install

### From source (development)

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1

pip install -e ".[dev]"
```

### With canonical DataForge datasets

```bash
pip install -e "../data_quality_env"
dataforge-evals run --agent mock --dataset hospital --trials 3
```

## Run a provider

```bash
set GROQ_API_KEY=...
dataforge-evals run --agent groq-llama-70b --dataset hospital --trials 3 --output reports/groq-hospital.md
```

### Bounded Groq smoke test

Use a single synthetic trial to verify Groq wiring without turning the smoke
check into a benchmark:

```bash
dataforge-evals run --agent groq-llama-70b --dataset synthetic --trials 1 --seed 0 --timeout-s 20 --output reports/groq-synthetic-smoke.md --output-json reports/groq-synthetic-smoke.json
```

For this smoke path, `trials_completed=1` and `Failures=none` prove the
integration completed successfully. F1 is a quality signal for the model's
proposed repairs, not the API health check. The JSON report includes the
normalized proposed `fixes` for debugging; Markdown stays summary-only.

### Built-in adapters

| Agent ID | Provider | Required Setup |
| --- | --- | --- |
| `mock` | local deterministic oracle for tests | none |
| `groq-llama-70b` | Groq | `GROQ_API_KEY` |
| `gemini-flash` | Gemini | `GEMINI_API_KEY` |
| `cerebras-llama` | Cerebras | `CEREBRAS_API_KEY` |
| `openrouter` | OpenRouter | `OPENROUTER_API_KEY` |
| `local-ollama` | local Ollama OpenAI-compatible endpoint | Ollama server on `localhost:11434` |
| `hf-local` | Hugging Face Transformers | optional `HF_TOKEN`; install `.[hf]` |

### Evaluating the historical DataForge SFT checkpoint

Use `hf-local` for base-vs-SFT checks with the same exact-match grader used by
hosted providers:

```bash
pip install -e ".[hf]"
dataforge-evals run --agent hf-local --dataset synthetic --trials 1 \
  --model-id Praneshrajan15/DataForge-0.5B-SFT \
  --output reports/dataforge-sft-smoke.md \
  --output-json reports/dataforge-sft-smoke.json
```

If `--model-id` is omitted, the adapter uses `DATAFORGE_EVAL_MODEL`, then the
authenticated `HF_TOKEN` user's `DataForge-0.5B-SFT`, then
`Praneshrajan15/DataForge-0.5B-SFT`.

### Discover agents and datasets

```bash
dataforge-evals list-agents
dataforge-evals list-datasets
```

## Custom CSV-pair evaluation

Bring your own dirty and clean CSV files:

```bash
dataforge-evals run --agent mock --dataset my-data \
    --dirty-csv path/to/dirty.csv \
    --clean-csv path/to/clean.csv \
    --trials 3
```

The dirty and clean CSVs must have the same number of rows and columns. Column names are taken from the clean file.

## Agent protocol

Any agent can plug in by implementing:

```python
from dataforge_evals import AgentTask, Fix

class MyAgent:
    name = "my-agent"

    def run(self, task: AgentTask) -> list[Fix]:
        return [Fix(row=0, column="Score", new_value="4.5", reason="example")]
```

Agents never report their own score. They return candidate fixes only. The grader is the only source of truth.
Normal agents receive a label-hidden `AgentTask`; only the built-in `mock`
oracle used by tests is marked to receive full ground truth.

### What agents receive

- `task.name` â€” dataset identifier
- `task.dirty_df` â€” pandas DataFrame with data-quality issues (all values as strings)
- `task.canonical_columns` â€” ordered column names from the clean reference
- `task.metadata` â€” provenance and descriptive metadata

### What agents return

Either a `list[Fix]` or an `AgentRunResult` with usage accounting:

```python
from dataforge_evals import AgentRunResult, Fix, Usage

return AgentRunResult(
    fixes=[Fix(row=0, column="Score", new_value="4.5")],
    usage=Usage(calls=1, prompt_tokens=500, completion_tokens=100, quota_units=0.001),
    steps=1,
    model="my-model-v1",
)
```

## What is graded

A `Fix` is correct only when `(row, column, new_value)` exactly matches a ground-truth dirty-to-clean cell correction. Duplicate predictions for the same cell use last-write-wins normalization. A wrong value on the right cell counts as both a false positive and a false negative.

## Quota accounting

Each report uses provider-normalized free-tier quota units rather than dollars. Built-in adapters record raw calls, prompt tokens, completion tokens, and quota units.

Provider-specific normalization (as of 2026-05-01):

| Provider | Free-tier basis | 1 quota unit = |
| --- | --- | --- |
| Groq | 14,400 RPD | 1 request |
| Gemini | 1,500 RPD | 1 request |
| Cerebras | 1,000 RPD | 1 request |
| OpenRouter | Nominal 1,000 RPD | 1 request |
| Ollama | unlimited (local) | always 0 |

On HTTP 429, the adapter waits with exponential backoff and logs `waiting N seconds for quota reset` to stderr. It does not fall back to another provider because fallback would contaminate the comparison.

## Reproducibility

Each report records:

- `dataforge-evals` commit hash
- `dataforge` source commit hash when canonical datasets are loaded through DataForge
- exact seeds
- provider model identifiers
- UTC run date
- dependency versions (pandas, pydantic, httpx, etc.)
- an explicit nondeterminism note

Deterministic and mock agents reproduce exactly from the recorded seeds. Hosted LLM providers may still change outputs because providers can update model weights, routing, safety systems, or tokenization without notice.

### Reproducibility limitations

- Provider model identifiers (e.g., `llama-3.3-70b-versatile`) may point to different weights on different dates.
- Token counts and quota units depend on provider-side tokenization, which can change.
- Network latency, rate limiting, and provider availability affect runtime measurements.
- Temperature 0 does not guarantee determinism across all providers.

## Not a leaderboard by default

Only compare reports when dataset versions, seeds, provider model identifiers, run date, and prompt/adapter code are identical. Otherwise the report is an evaluation artifact, not a leaderboard row.

## When dataforge-evals is the wrong tool

Do not use `dataforge-evals` if:

- **Your agent operates on streaming data** â€” the harness is batch-oriented and expects a complete dirty DataFrame.
- **You need end-to-end pipeline evaluation** â€” this tool evaluates cell-level repair accuracy, not detection, diagnosis, or pipeline orchestration.
- **Your ground truth is fuzzy or approximate** â€” the grader uses exact string match. If multiple correct values exist for a cell, you need a custom grader.
- **You need sub-second latency benchmarking** â€” the harness measures wall-clock time but is not designed as a latency benchmarking tool.
- **Your data is > 100K rows** â€” the harness loads the full DataFrame into memory and passes it to agents. For large-scale evaluation, sample first.

## Development

```bash
make setup     # pip install -e ".[dev]"
make lint      # ruff check
make format    # ruff format --check
make type      # mypy --strict
make test      # pytest
make test-cov  # pytest with coverage
make smoke     # end-to-end smoke test with mock agent
```

## Environment Variables

Provider keys belong in a root `.env` file (gitignored) loaded with `python-dotenv`:

- `GROQ_API_KEY`
- `GEMINI_API_KEY`
- `CEREBRAS_API_KEY`
- `OPENROUTER_API_KEY`

## License

Apache-2.0.
