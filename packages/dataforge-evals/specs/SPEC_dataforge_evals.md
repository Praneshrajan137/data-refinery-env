# SPEC: dataforge-evals

> Status: Reviewed
> Owner: pranesh
> Last updated: 2026-05-01

## 1. Purpose (2 sentences)

Provide an agent-agnostic, reproducible evaluation harness for data-quality
repair agents. Score any agent's proposed cell fixes against canonical
ground-truth datasets using exact-match precision, recall, and F1 with
deterministic grading, provider-normalized quota accounting, and auditable
reproducibility metadata.

## 2. Outcomes (measurable, binary pass/fail)

- [x] `pip install -e ".[dev]"` works from a clean Python 3.11 or 3.12 venv.
- [x] `dataforge-evals run --agent mock --dataset synthetic --trials 3` produces
  a well-formed Markdown report without network access.
- [ ] With `dataforge` installed and a provider key configured,
  `dataforge-evals run --agent groq-llama-70b --dataset hospital --trials 3`
  runs against canonical Hospital tasks.
- [x] Tests cover grader correctness (perfect, empty, duplicate, wrong-value,
  wrong-cell, extra-FP, whitespace, large-batch).
- [x] Tests cover harness smoke behavior (mock-agent, broken-agent, timeout,
  multi-agent cross-product, report writing).
- [x] No benchmark numbers are hardcoded or fabricated in README.

## 3. Scope

**IN**:
- Public data models: `Task`, `Fix`, `GroundTruthCell`, `Usage`, `AgentRunResult`, `Grade`
- Agent protocol: `name: str` + `run(task: Task) -> list[Fix] | AgentRunResult`
- Task loading: built-in synthetic, canonical DataForge (soft dependency), CSV-pair
- Grading: exact cell-diff P/R/F1 with last-write-wins normalization
- Provider adapters: Groq, Gemini, Cerebras, OpenRouter, local Ollama, mock oracle
- Harness: N trials x M agents x K datasets, timeout, failure taxonomy, aggregation
- Reporting: Markdown + JSON with reproducibility block
- CLI: `run`, `list-agents`, `list-datasets` subcommands

**OUT** (explicitly excluded, to prevent scope creep):
- Training agents or fine-tuning models
- Agent decision-making logic beyond the adapter prompt/parse boundary
- DataForge core detector/repairer functionality
- Leaderboard hosting or CI comparison pipelines
- Multi-step agentic loops (agents receive task and return fixes in one call)

## 4. Constraints

- Performance: single trial on synthetic task completes in < 1 second (no network)
- Compatibility: Python 3.11+, works on Linux / macOS / Windows
- Backward compatibility: Agent protocol is stable; adding optional parameters is allowed,
  removing or renaming existing fields is not
- Dependencies: minimal core (pydantic, typer, rich, httpx, tenacity, pandas, python-dotenv);
  provider SDKs and dataforge are optional extras

## 5. Prior decisions (locked - require new spec to change)

- Grader is sole authority: agents never report their own score
- Last-write-wins normalization: duplicate fixes per cell use the final prediction
- Wrong value on correct cell counts as both FP and FN
- 429 retries stay on the same provider with exponential backoff; no fallback
- DataForge is a soft optional dependency

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Project infrastructure
- Acceptance: `.gitignore`, `Makefile`, `py.typed`, `conftest.py`, spec file exist and are correct
- Depends on: none
- Estimated complexity: S

### 6.2 Model and type hardening
- Acceptance: all public classes have Google-style docstrings, `__version__` is exported,
  `Grade` is re-exported from `__init__.py`
- Depends on: none
- Estimated complexity: S

### 6.3 Grader correctness hardening
- Acceptance: whitespace normalization, malformed fix warning, all 8+ test cases pass
- Depends on: 6.2
- Estimated complexity: M

### 6.4 Provider adapter production hardening
- Acceptance: httpx client reuse, provider-specific quota formulas, JSON fence extraction,
  structured logging via `logging` module
- Depends on: 6.2
- Estimated complexity: M

### 6.5 Harness and reporting hardening
- Acceptance: true timeout via ThreadPoolExecutor, dependency versions in reproducibility,
  Rich progress bar, `--dirty-csv`/`--clean-csv` CLI options, `list-agents`/`list-datasets`
  subcommands
- Depends on: 6.3, 6.4
- Estimated complexity: L

### 6.6 Documentation and examples
- Acceptance: README has wrong-tool section, quota explanation, reproducibility limitations,
  CSV-pair docs; examples load dotenv; pyproject.toml version updated
- Depends on: 6.5
- Estimated complexity: M

## 7. Verification

- Unit tests: `tests/test_grader.py`, `tests/test_harness.py`
- Integration tests: `tests/test_cli.py` (CLI smoke via `typer.testing.CliRunner`)
- Coverage target: >= 90% line, >= 80% branch
- Type checking: `mypy --strict` clean
- Linting: `ruff check` clean, `ruff format --check` clean

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [x] All Section 2 outcomes are met.
- [x] All Section 6 tasks have "passes".
- [ ] Coverage thresholds (Section 7) are met.
- [x] No test in the suite fails.
- [x] No benchmark numbers are hardcoded or fabricated in README.
- [x] Re-run from the same committed seeds gives identical deterministic/mock outputs.

## Appendix A - Toy cases (write the FIRST failing tests from these)

### Case A.1: Perfect match
Input: ground truth = `[(0, "Score", "45", "4.5")]`, fixes = `[Fix(0, "Score", "4.5")]`
Expected output: `Grade(tp=1, fp=0, fn=0, precision=1.0, recall=1.0, f1=1.0)`
Reasoning: baseline sanity - a correct fix on the exact cell with the exact value scores perfectly.

### Case A.2: Empty prediction
Input: ground truth = `[(0, "Score", "45", "4.5")]`, fixes = `[]`
Expected output: `Grade(tp=0, fp=0, fn=1, precision=0.0, recall=0.0, f1=0.0)`
Reasoning: no predictions means all ground truth is missed.

### Case A.3: Wrong value on correct cell
Input: ground truth = `[(0, "Score", "45", "4.5")]`, fixes = `[Fix(0, "Score", "5.0")]`
Expected output: `Grade(tp=0, fp=1, fn=1, precision=0.0, recall=0.0, f1=0.0)`
Reasoning: the fix targets the right cell but proposes the wrong value - this is both a false positive and a false negative.

### Case A.4: Duplicate fix (last-write-wins)
Input: fixes = `[Fix(0, "Score", "4.0"), Fix(0, "Score", "4.5")]`
Expected output: normalized list = `[Fix(0, "Score", "4.5")]`
Reasoning: the second fix for the same cell overwrites the first.

### Case A.5: Wrong cell fix
Input: ground truth = `[(0, "Score", "45", "4.5")]`, fixes = `[Fix(1, "Phone", "555")]`
Expected output: `Grade(tp=0, fp=1, fn=1)`
Reasoning: fix targets a cell with no ground-truth issue - pure false positive, ground truth is unfixed.

### Case A.6: Extra false positive
Input: ground truth = `[(0, "Score", "45", "4.5")]`, fixes = `[Fix(0, "Score", "4.5"), Fix(1, "Phone", "555")]`
Expected output: `Grade(tp=1, fp=1, fn=0, precision=0.5, recall=1.0)`
Reasoning: one correct fix plus one spurious fix - recall is perfect but precision drops.
