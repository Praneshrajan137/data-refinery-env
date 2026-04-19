.PHONY: help setup setup-all lint format type test test-mapped coverage bench bench-free mutation clean

help:
	@echo "DataForge dev targets"
	@echo "  setup         Install dev deps only (pip install -e '.[dev]')"
	@echo "  setup-all     Install ALL extras (pip install -e '.[all]')"
	@echo "  lint          Run ruff check + ruff format --check"
	@echo "  format        Auto-fix: ruff format + ruff check --fix"
	@echo "  type          Run mypy --strict on dataforge/"
	@echo "  test          Run the full test suite"
	@echo "  test-mapped   Run tests for a changed source file (FILE=path)"
	@echo "  coverage      Run tests with coverage (fails at <90%)"
	@echo "  bench         Run pytest-benchmark suites"
	@echo "  bench-free    Run SOTA comparison using free-tier LLM providers only"
	@echo "  mutation      Run mutmut on dataforge/ (target: >=85%)"
	@echo "  clean         Remove caches"

setup:
	pip install -e ".[dev]"

setup-all:
	pip install -e ".[all]"

lint:
	ruff check dataforge tests scripts
	ruff format --check dataforge tests scripts

format:
	ruff format dataforge tests scripts
	ruff check --fix dataforge tests scripts

type:
	mypy --strict dataforge

test:
	pytest tests/ -x -v

test-mapped:
	python scripts/test_mapped.py $(FILE)

coverage:
	pytest tests/ --cov=dataforge --cov-report=term-missing --cov-report=html --cov-fail-under=90

bench:
	pytest tests/benchmarks/ --benchmark-only --benchmark-autosave

bench-free:
	DATAFORGE_LLM_PROVIDER=groq python scripts/bench/run_agent_comparison.py
	DATAFORGE_LLM_PROVIDER=gemini python scripts/bench/run_agent_comparison.py
	python scripts/bench/generate_report.py > BENCHMARK_REPORT.md

mutation:
	mutmut run --paths-to-mutate dataforge/
	mutmut results

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
