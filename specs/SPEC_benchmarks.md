# SPEC: Real-World Benchmarks

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-04-21

## 1. Purpose (2 sentences)

Ship a real benchmark harness for DataForge on the upstream Raha `hospital`,
`flights`, and `beers` datasets. The benchmark must produce reproducible local
results, clearly-separated citation-only SOTA reference rows, and an idempotent
README benchmark block sourced only from generated artifacts.

## 2. Outcomes (measurable, binary pass/fail)

- [ ] `dataforge bench --methods heuristic --datasets hospital` completes on cached data and writes `eval/results/agent_comparison.json`.
- [ ] `dataforge.datasets.real_world.load_real_world_dataset()` caches raw upstream files under `~/.dataforge/cache/real_world/`.
- [ ] Hospital and Beers dirty/clean files are aligned by column position and scored on data cells only.
- [ ] LLM methods are skipped with a clear warning when `DATAFORGE_LLM_PROVIDER` is unset or not `groq`.
- [ ] Free-tier quota estimation blocks runs over 500 estimated calls unless `--really-run-big-bench` is passed.
- [ ] `scripts/bench/generate_report.py` produces `BENCHMARK_REPORT.md` and updates only the README benchmark marker block.
- [ ] README benchmark rows come from generated JSON, and citation-only SOTA rows appear only in `BENCHMARK_REPORT.md`.

## 3. Scope

**IN**:
- Canonical metadata registry for `hospital`, `flights`, and `beers`
- Real-world dataset loader with cache + manual-download error guidance
- Benchmark helpers for chunking, normalization, scoring, aggregation, and quota estimation
- Benchmark methods: `random`, `heuristic`, `llm_zeroshot`, `llm_react`
- `dataforge bench` CLI command
- Thin benchmark scripts under `scripts/bench/`
- Generated `BENCHMARK_REPORT.md`
- Idempotent README benchmark marker replacement

**OUT** (explicitly excluded, to prevent scope creep):
- Reproducing HoloClean / Raha / Baran from source code
- New benchmark-only detectors or repair heuristics
- Benchmarking the Week 3 apply-path safety/verifier gates
- Paid-API benchmarking
- Monthly or scheduled benchmark automation

## 4. Constraints

- Performance: cached `dataforge bench --methods heuristic --datasets hospital` completes in < 30 seconds.
- Compatibility: Python 3.11+ on Windows / macOS / Linux.
- Backward compatibility: all existing Week 1-3 tests remain green.
- Integrity: no fabricated benchmark numbers; missing LLM runs must be marked skipped.
- Reporting: README benchmark content must be generated from JSON and updated idempotently.
- Quota discipline: any benchmark estimated above 500 free-tier LLM calls requires an explicit override flag.

## 5. Prior decisions (locked — require new spec to change)

- Local benchmark metric is repair P/R/F1 on exact cell corrections.
- Dirty/clean header mismatches are resolved by positional alignment, not name-based joins.
- Citation-only SOTA rows live in `BENCHMARK_REPORT.md`, not in the README summary block.
- The current shipped deterministic detector/repair stack is the heuristic baseline without benchmark-only changes.

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Dataset registry + models
- Acceptance: metadata exists for `hospital`, `flights`, and `beers` with canonical counts and citations.
- Depends on: none
- Estimated complexity: S

### 6.2 Real-world loader + cache
- Acceptance: cache hits avoid network; cache misses raise a manual-download error on failure.
- Depends on: 6.1
- Estimated complexity: M

### 6.3 Benchmark core
- Acceptance: chunking, normalization, scoring, aggregation, and quota estimation are covered by unit tests.
- Depends on: 6.2
- Estimated complexity: M

### 6.4 Benchmark methods
- Acceptance: random + heuristic produce local metrics; LLM methods skip cleanly without Groq configuration.
- Depends on: 6.3
- Estimated complexity: L

### 6.5 CLI + scripts
- Acceptance: `dataforge bench` and the scripts write JSON/Markdown outputs to the expected paths.
- Depends on: 6.4
- Estimated complexity: M

### 6.6 Report generation + README marker updates
- Acceptance: README marker replacement is idempotent and the report shows local and citation-only tables separately.
- Depends on: 6.5
- Estimated complexity: M

## 7. Verification

- Unit tests: `tests/unit/test_bench_core.py`, `tests/unit/test_bench_real_world.py`, `tests/unit/test_bench_runner.py`, `tests/unit/test_cli_bench.py`
- Integration tests: `tests/integration/test_bench_workflow.py`
- Benchmarks: existing `tests/benchmarks/` remain intact; Week 4 adds a cached heuristic timing assertion in integration tests
- Coverage target: >= 90% line, >= 80% branch
- Mutation score target: >= 85%

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [ ] All Section 2 outcomes are met.
- [ ] All Section 6 tasks have "passes".
- [ ] Coverage thresholds (Section 7) are met.
- [ ] No test in `tests/regression/` fails.
- [ ] `DECISIONS.md` has a Week 4 entry for dual-table reporting and positional header alignment.
- [ ] `ruff check`, `ruff format --check`, and `mypy --strict dataforge` pass.

## Appendix A — Toy cases (write the FIRST failing tests from these)

### Case A.1: Positional header alignment
Input:
```python
dirty_header = ["provider_number", "name"]
clean_header = ["ProviderNumber", "HospitalName"]
```
Expected output: the loader aligns by column position and produces canonical columns `["ProviderNumber", "HospitalName"]`.
Reasoning: the upstream Hospital and Beers datasets do not share dirty/clean header names.

### Case A.2: Header-only differences do not become benchmark cells
Input: dirty and clean files differ only in their header row.
Expected output: `ground_truth == []`.
Reasoning: benchmark error cells should reflect row-level data differences only.

### Case A.3: Wrong value on right cell counts as FP + FN
Input:
```python
ground_truth = {(2, "Score"): "4.5"}
prediction = {(2, "Score"): "5.0"}
```
Expected output: `tp=0, fp=1, fn=1`.
Reasoning: predicting the wrong corrected value should not get partial credit.

### Case A.4: LLM skip path
Input: `DATAFORGE_LLM_PROVIDER` unset and methods `["heuristic", "llm_zeroshot"]`.
Expected output: heuristic row is `ok`; llm row is `skipped` with a clear reason.
Reasoning: the free-tier benchmark should degrade gracefully without provider config.

### Case A.5: Big-bench gate
Input: methods `["llm_zeroshot", "llm_react"]`, datasets `["hospital", "flights", "beers"]`, `seeds=3`.
Expected output: the pre-run estimator refuses the run unless `--really-run-big-bench` is set.
Reasoning: the benchmark must protect free-tier quotas by default.
