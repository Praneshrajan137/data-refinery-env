.PHONY: setup lint format type test test-cov smoke clean lock uv-lock

PACKAGE := dataforge_evals
TESTS := tests
EXAMPLES := examples

setup:
	python -m pip install -e ".[dev]"

lint:
	python -m ruff check $(PACKAGE) $(TESTS) $(EXAMPLES)

format:
	python -m ruff format --check $(PACKAGE) $(TESTS) $(EXAMPLES)

format-fix:
	python -m ruff format $(PACKAGE) $(TESTS) $(EXAMPLES)

type:
	python -m mypy $(PACKAGE) --strict

test:
	python -m pytest $(TESTS) -v --strict-markers --strict-config --tb=short

test-cov:
	python -m pytest $(TESTS) -v --strict-markers --strict-config --tb=short --cov=$(PACKAGE) --cov-report=term-missing

smoke:
	dataforge-evals run --agent mock --dataset synthetic --trials 3 --output reports/smoke.md --output-json reports/smoke.json

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache .pytest_cache .hypothesis dist build *.egg-info

lock:
	@if command -v pip-compile >/dev/null 2>&1; then \
	  mkdir -p requirements; \
	  pip-compile --resolver=backtracking --allow-unsafe --strip-extras -o requirements/constraints-base.txt requirements/in/base.in; \
	  pip-compile --resolver=backtracking --allow-unsafe --strip-extras -o requirements/constraints-dev.txt requirements/in/dev.in; \
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
