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

.PHONY: help setup setup-all lint format type test test-mapped frontend-install frontend-build frontend-test frontend-gate backend-gate release-gate playground-release-check sft-preflight coverage bench bench-free mutation clean lock uv-lock

help:
	@echo "DataForge dev targets"
	@echo "  setup         Install dev deps plus playground test deps"
	@echo "  setup-all     Install ALL extras (pip install -e '.[all]')"
	@echo "  lint          Run ruff check + ruff format --check"
	@echo "  format        Auto-fix: ruff format + ruff check --fix"
	@echo "  type          Run mypy --strict on core + shipped Python paths"
	@echo "  test          Run the full test suite"
	@echo "  test-mapped   Run tests for a changed source file (FILE=path)"
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
	$(PYTHON) -m ruff check dataforge tests scripts/ci scripts/playground scripts/data scripts/model scripts/publish_model.py playground/api/app.py
	$(PYTHON) -m ruff format --check dataforge tests scripts/ci scripts/playground scripts/data scripts/model scripts/publish_model.py playground/api/app.py

format:
	$(PYTHON) -m ruff format dataforge tests scripts/ci scripts/playground scripts/data scripts/model scripts/publish_model.py playground/api/app.py
	$(PYTHON) -m ruff check --fix dataforge tests scripts/ci scripts/playground scripts/data scripts/model scripts/publish_model.py playground/api/app.py

type:
	$(PYTHON) -m mypy --strict dataforge playground/api/app.py scripts/ci/readme_truth.py scripts/ci/benchmark_truth.py scripts/ci/docs_truth.py scripts/ci/full_vision_external_gate.py scripts/ci/installed_package_smoke.py scripts/ci/pypi_publish_report.py scripts/ci/openapi_contract.py scripts/ci/backend_gate.py scripts/playground/build_samples.py scripts/playground/stage_space.py scripts/playground/verify_space_backend.py scripts/playground/monitor_playground.py scripts/data/collect_sft_trajectories.py scripts/data/validate_sft_readiness.py scripts/model/verify_sft_release.py scripts/model/publish_dataset_readme.py scripts/publish_model.py

test:
	$(PYTHON) -m pytest tests/ -x -v

test-mapped:
	$(PYTHON) scripts/test_mapped.py $(FILE)

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

bench:
	$(PYTHON) -m pytest tests/benchmarks/ --benchmark-only --benchmark-autosave

bench-free:
	$(PYTHON) scripts/bench/run_agent_comparison.py --methods random,heuristic --datasets hospital,flights,beers --seeds 3 --output-json eval/results/agent_comparison.json
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
