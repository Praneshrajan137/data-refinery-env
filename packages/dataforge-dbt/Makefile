.PHONY: lock uv-lock

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
