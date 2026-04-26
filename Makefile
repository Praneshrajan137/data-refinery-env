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

.PHONY: help setup setup-all lint format type test test-mapped coverage bench bench-free mutation clean

help:
	@echo "DataForge dev targets"
	@echo "  setup         Install dev deps plus playground test deps"
	@echo "  setup-all     Install ALL extras (pip install -e '.[all]')"
	@echo "  lint          Run ruff check + ruff format --check"
	@echo "  format        Auto-fix: ruff format + ruff check --fix"
	@echo "  type          Run mypy --strict on core + shipped Week 5 Python paths"
	@echo "  test          Run the full test suite"
	@echo "  test-mapped   Run tests for a changed source file (FILE=path)"
	@echo "  coverage      Run tests with coverage (fails at <90%)"
	@echo "  bench         Run pytest-benchmark suites"
	@echo "  bench-free    Run the real-world benchmark scripts and regenerate reports"
	@echo "  mutation      Run mutmut on dataforge/ (target: >=85%)"
	@echo "  clean         Remove caches"

setup:
	$(PYTHON) -m pip install -e ".[dev]"
	$(PYTHON) -m pip install -r playground/api/requirements.txt

setup-all:
	$(PYTHON) -m pip install -e ".[all]"

lint:
	$(PYTHON) -m ruff check dataforge data_quality_env tests scripts/ci scripts/playground playground/api/app.py
	$(PYTHON) -m ruff format --check dataforge data_quality_env tests scripts/ci scripts/playground playground/api/app.py

format:
	$(PYTHON) -m ruff format dataforge data_quality_env tests scripts/ci scripts/playground playground/api/app.py
	$(PYTHON) -m ruff check --fix dataforge data_quality_env tests scripts/ci scripts/playground playground/api/app.py

type:
	$(PYTHON) -m mypy --strict dataforge data_quality_env playground/api/app.py scripts/ci/readme_truth.py scripts/playground/build_samples.py scripts/playground/stage_space.py

test:
	$(PYTHON) -m pytest tests/ -x -v

test-mapped:
	$(PYTHON) scripts/test_mapped.py $(FILE)

coverage:
	$(PYTHON) -m pytest tests/ --cov=dataforge --cov-report=term-missing --cov-report=html --cov-fail-under=90

bench:
	$(PYTHON) -m pytest tests/benchmarks/ --benchmark-only --benchmark-autosave

bench-free:
	$(PYTHON) scripts/bench/run_agent_comparison.py --methods random,heuristic,llm_zeroshot,llm_react --datasets hospital,flights,beers --seeds 3 --really-run-big-bench
	$(PYTHON) scripts/bench/run_sota_comparison.py
	$(PYTHON) scripts/bench/generate_report.py

mutation:
	$(PYTHON) -m mutmut run --paths-to-mutate dataforge/
	$(PYTHON) -m mutmut results

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
