# Use Git Bash on Windows so Unix commands (rm, find, &&) work in recipes.
# The 8.3 short-path avoids GNU Make's space-in-path limitation.
ifeq ($(OS),Windows_NT)
SHELL := C:/PROGRA~1/Git/bin/bash.exe
VENV_PYTHON := .venv/Scripts/python.exe
else
VENV_PYTHON := .venv/bin/python
endif

ifndef PYTHON
PYTHON := $(if $(wildcard $(VENV_PYTHON)),$(VENV_PYTHON),python)
endif

.PHONY: help setup setup-all lint format type test test-serial test-mapped test-map-check gate-population frontend-install frontend-build frontend-test frontend-gate backend-gate release-gate playground-release-check sft-preflight coverage bench bench-free mutation clean lock uv-lock

help:
	@echo "DataForge dev targets"
	@echo "  setup         Install dev deps plus playground test deps"
	@echo "  setup-all     Install ALL extras (pip install -e '.[all]')"
	@echo "  lint          Run ruff check + ruff format --check"
	@echo "  format        Auto-fix: ruff format + ruff check --fix"
	@echo "  type          Run mypy --strict on core + shipped Python paths"
	@echo "  test          Run the full test suite in parallel"
	@echo "  test-serial   Run the full test suite serially (for --pdb / -s debugging)"
	@echo "  test-mapped   Run tests for a changed source file (FILE=path) -- the inner loop"
	@echo "  test-map-check Verify every dataforge module has a mapping decision"
	@echo "  gate-population Verify no gate quietly stopped checking something"
	@echo "  frontend-gate Run Vite typecheck, unit tests, build budget, and Playwright"
	@echo "  backend-gate  Run the canonical backend release-quality gate"
	@echo "  release-gate  Build, audit, offline-install, and smoke-test the wheel"
	@echo "  playground-release-check  Verify deployed Playground checklist"
	@echo "  sft-preflight Validate SFT JSONL/config before launching Kaggle"
	@echo "  coverage      Run tests with coverage using pyproject.toml policy"
	@echo "  bench         Run pytest-benchmark suites"
	@echo "  bench-free    Run the real-world benchmark scripts and regenerate reports"
	@echo "  mutation      Run mutmut on dataforge/ (target: >=85%)"
	@echo "  clean         Remove caches"
	@echo "  lock          Generate pip-tools constraints (optional)"
	@echo "  uv-lock       Generate uv.lock if 'uv' is installed (optional)"

setup:
	$(PYTHON) -m pip install -e ".[dev]"
	$(PYTHON) -m pip install -r playground/api/requirements.txt

setup-all:
	$(PYTHON) -m pip install -e ".[all]"

lint:
	$(PYTHON) -m ruff check dataforge tests scripts/ci scripts/perf scripts/playground scripts/data scripts/model scripts/preflight scripts/remote scripts/publish_model.py scripts/measure_payload_split.py scripts/measure_trust_ledger.py playground/api/app.py
	$(PYTHON) -m ruff format --check dataforge tests scripts/ci scripts/perf scripts/playground scripts/data scripts/model scripts/preflight scripts/remote scripts/publish_model.py scripts/measure_payload_split.py playground/api/app.py
	$(PYTHON) scripts/ci/generate_domain_vocabulary.py --check
	$(PYTHON) scripts/ci/generate_attestation_vectors.py --check
	$(PYTHON) scripts/ci/test_map_coverage.py --check

format:
	$(PYTHON) -m ruff format dataforge tests scripts/ci scripts/perf scripts/playground scripts/data scripts/model scripts/preflight scripts/remote scripts/publish_model.py scripts/measure_payload_split.py playground/api/app.py
	$(PYTHON) -m ruff check --fix dataforge tests scripts/ci scripts/perf scripts/playground scripts/data scripts/model scripts/preflight scripts/remote scripts/publish_model.py scripts/measure_payload_split.py playground/api/app.py

# scripts/preflight/check_kaggle_auth.py is type-checked because dataforge/release/doctor.py
# invokes it for the Kaggle OAuth clean-config check, so it is product-adjacent rather than a
# scratch script. scripts/remote/ is deliberately NOT in this list: it lints and formats clean
# (hence its inclusion above) but `mypy --strict` reports 46 errors across 12 files there, which
# is separate work rather than something to bundle into an unrelated change. Stated so the gap is
# a recorded decision, not an oversight.
type:
	$(PYTHON) -m mypy --strict dataforge playground/api/app.py scripts/ci/readme_truth.py scripts/ci/benchmark_truth.py scripts/ci/docs_truth.py scripts/ci/full_vision_external_gate.py scripts/ci/installed_package_smoke.py scripts/ci/pypi_publish_report.py scripts/ci/openapi_contract.py scripts/ci/backend_gate.py scripts/ci/generate_domain_vocabulary.py scripts/ci/mutate_domain_vocabulary.py scripts/ci/mutate_autoapply_guards.py scripts/ci/generate_attestation_vectors.py scripts/ci/attestation_conformance.py scripts/ci/mutate_adversarial_corpus.py scripts/ci/gate_population.py scripts/ci/test_map_coverage.py scripts/perf/measure_loop_cost.py scripts/perf/measure_verifier_work.py scripts/measure_payload_split.py scripts/measure_trust_ledger.py scripts/playground/build_samples.py scripts/playground/stage_space.py scripts/playground/verify_space_backend.py scripts/playground/monitor_playground.py scripts/preflight/check_kaggle_auth.py scripts/data/collect_sft_trajectories.py scripts/data/validate_sft_readiness.py scripts/model/verify_sft_release.py scripts/model/publish_dataset_readme.py scripts/publish_model.py

# `-n logical` rather than `-n auto`: this suite is dominated by subprocess launches and file
# I/O, not CPU, so logical cores are the right unit. `--dist loadgroup` comes from
# pyproject.toml addopts.
#
# `-v` was removed on 2026-08-28. It printed ~2,400 lines to the console per run, which on a
# Windows terminal is a measurable share of the wall clock and buries the summary. `-x` is kept,
# and pytest-xdist is floored at >= 3.8 because early exit under parallelism was only handled
# correctly from 3.6.1 onward.
test:
	$(PYTHON) -m pytest tests/ -x -n logical

# Serial, for reproducing a failure that parallel execution surfaced. xdist disables --pdb and
# -s, and `-n 0` turns distribution off while leaving the plugin loaded -- which is required,
# because `-p no:xdist` would make the `--dist` flag in pyproject's addopts unrecognised and exit 4.
test-serial:
	$(PYTHON) -m pytest tests/ -x -n 0

# The inner loop while editing one file. An unmapped file falls back to the full suite, so this
# is always safe to reach for; scripts/ci/test_map_coverage.py keeps the gap from growing.
# Add --hypothesis-profile dev to cut property-test examples from 100 to 10 when iterating.
test-mapped:
	$(PYTHON) scripts/test_mapped.py $(FILE)

test-map-check:
	$(PYTHON) scripts/ci/test_map_coverage.py --check

gate-population:
	$(PYTHON) scripts/ci/gate_population.py --check

frontend-install:
	npm --prefix playground/web ci

frontend-build:
	npm --prefix playground/web run build

frontend-test:
	npm --prefix playground/web run test

frontend-gate: frontend-install frontend-build frontend-test

backend-gate:
	$(PYTHON) scripts/ci/backend_gate.py

release-gate:
	$(PYTHON) -m dataforge.release.gate

playground-release-check:
	$(PYTHON) -m dataforge release playground-check --json

sft-preflight:
	$(PYTHON) scripts/data/validate_sft_readiness.py

coverage:
	$(PYTHON) -m pytest tests/ --cov=dataforge --cov-report=term-missing --cov-report=html

# `-o python_files` is load-bearing, not decoration. These files are named bench_*.py, and
# pytest's default `python_files = test_*.py *_test.py` does not match them, so this target
# collected ZERO tests and exited 5 from the day it was written until 2026-08-29. Both latency
# budgets inside -- SMT p95 under 200ms, safety filter p95 under 1ms -- had therefore never
# executed. The SMT one fails at ~248ms mean / 607ms max on its own 1000-row fixture.
#
# The bench_*.py naming is kept deliberately: it is what keeps 100 benchmark rounds out of
# `make test`, whose whole purpose is a fast inner loop. The names stay, the collection is
# fixed here and in the gate step that runs the same budgets.
#
# `-n 0` is also required, and also load-bearing. pytest-benchmark auto-activates
# --benchmark-disable when xdist is on, and this repo's addopts carry `--dist loadgroup`, so
# without `-n 0` the run dies with "Can't have both --benchmark-only and --benchmark-disable".
# It is right on the merits too: timing under parallel workers measures contention, not cost.
bench:
	$(PYTHON) -m pytest tests/benchmarks/ -o python_files="bench_*.py" -n 0 --benchmark-only --benchmark-autosave

bench-free:
	$(PYTHON) scripts/bench/run_agent_comparison.py --methods random,heuristic --datasets hospital,flights --seeds 3 --output-json eval/results/agent_comparison.json
	$(PYTHON) scripts/bench/run_sota_comparison.py
	$(PYTHON) scripts/bench/generate_report.py

mutation:
	$(PYTHON) -m mutmut run --paths-to-mutate dataforge/
	$(PYTHON) -m mutmut results

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist *.egg-info playground/web/dist playground/web/test-results playground/web/playwright-report
	find . -type d -name __pycache__ -exec rm -rf {} +

lock:
	@if command -v pip-compile >/dev/null 2>&1; then \
	  mkdir -p requirements; \
	  pip-compile --resolver=backtracking --allow-unsafe --strip-extras -o requirements/constraints-base.txt requirements/in/base.in; \
	  pip-compile --resolver=backtracking --allow-unsafe --strip-extras -o requirements/constraints-dev.txt requirements/in/dev.in; \
	  pip-compile --resolver=backtracking --allow-unsafe --strip-extras -o requirements/constraints-train.txt requirements/in/train.in; \
	  pip-compile --resolver=backtracking --allow-unsafe --strip-extras -o requirements/constraints-playground.txt requirements/in/playground.in; \
	  echo "Constraints generated under requirements/*.txt"; \
	else \
	  echo "pip-compile not found; install with 'pip install pip-tools'"; \
	fi

uv-lock:
	@if command -v uv >/dev/null 2>&1; then \
	  uv lock; \
	  echo "uv.lock generated"; \
	else \
	  echo "uv not found; install from https://github.com/astral-sh/uv"; \
	fi
