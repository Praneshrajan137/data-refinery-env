# DataForge Playground — Multi-stage Docker build for HF Spaces.
#
# Target: <= 600 MB image. Runs as non-root UID 1000 (HF requirement).
# Single-worker uvicorn with --timeout-keep-alive 5 (slowloris mitigation).
#
# See specs/SPEC_playground.md §4 and §6.5.

# ============================================================
# Stage 1: builder — install all Python dependencies
# ============================================================
FROM python:3.12-slim AS builder

WORKDIR /build

# System deps for building wheels
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install playground API requirements
COPY playground/api/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir -r /build/requirements.txt

# Copy dataforge source and install it
COPY pyproject.toml /build/dataforge_src/pyproject.toml
COPY README.md /build/dataforge_src/README.md
COPY dataforge/ /build/dataforge_src/dataforge/
COPY constitutions/ /build/dataforge_src/constitutions/
RUN pip install --no-cache-dir /build/dataforge_src

# ============================================================
# Stage 2: runtime — minimal image with only installed packages
# ============================================================
FROM python:3.12-slim

# HF Spaces requires non-root user with UID 1000
RUN useradd -m -u 1000 user

# Copy installed Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy constitutions to the site-packages-relative path SafetyFilter expects.
COPY --from=builder /build/dataforge_src/constitutions /usr/local/lib/python3.12/site-packages/constitutions

# Copy application code
COPY playground/api/app.py /home/user/app/app.py
COPY playground/api/samples/ /home/user/app/samples/

# Switch to non-root user
USER user
WORKDIR /home/user/app

# Expose the port HF Spaces expects
EXPOSE 7860

# Environment
ENV PORT=7860
ENV DATAFORGE_PLAYGROUND_DEV=0

# Start uvicorn with single worker (slowapi in-memory limiter contract)
# and honor PORT for Hugging Face runtime assignment.
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860} --workers 1 --timeout-keep-alive 5"]
