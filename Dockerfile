# HuggingFace Spaces Dockerfile
# Standalone build (no openenv-base dependency)

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl && \
    rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv && \
    mv /root/.local/bin/uvx /usr/local/bin/uvx

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY server/ server/
COPY datasets/ datasets/
COPY models.py compat.py generate_datasets.py openenv.yaml __init__.py ./
COPY README.md ./

# Install dependencies
RUN uv sync --frozen --no-editable

# Set environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:$PYTHONPATH"

EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:7860/health || exit 1

CMD ["uvicorn", "server.app:app", "--host", "0.0.0.0", "--port", "7860"]
