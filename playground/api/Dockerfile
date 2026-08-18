# DataForge Playground — Multi-stage Docker build.
#
# Serves BOTH the API and the SPA from one origin (Azure Container Apps). The frontend is built
# here rather than copied from the host because `.dockerignore` excludes `dist/`, so a locally
# built bundle never reaches the build context -- and because an image that builds its own
# frontend cannot ship a bundle that does not match its source.
#
# Target: <= 600 MB image. Measured 596 MB WITH the SPA and its build stage included, so the
# original target still holds -- this comment briefly claimed 700 MB on the assumption that adding
# a frontend must cost more headroom, and the measurement said otherwise. The Node stage is
# discarded; only dist/ (about 600 KB) reaches the runtime layer.
# Runs as non-root UID 1000. Single-worker uvicorn with --timeout-keep-alive 5.
#
# Build from the REPOSITORY ROOT: docker build -f playground/api/Dockerfile .
#
# See specs/SPEC_playground.md §4 and §6.5.

# ============================================================
# Stage 1: web — build the SPA bundle
# ============================================================
FROM node:22-slim AS web

WORKDIR /web

# Lockfile first, so dependency installation caches independently of source edits.
COPY playground/web/package.json playground/web/package-lock.json /web/
RUN npm ci

COPY playground/web/ /web/

# Deliberately NOT `npm run build`.
#
# That script also runs the colour, motion, perceptual, quantitative, doc-path and vocabulary
# audits. Those reach OUTSIDE playground/web -- audit_vocabulary.mjs hashes
# dataforge/domain/vocabulary.py, audit_doc_paths.mjs reads docs/ -- so they would need the whole
# repository in this stage, and they are CI's job, not the image's. CI fails the build if they
# fail; the image only needs the bundle. The typecheck IS kept, because shipping a bundle that
# does not typecheck is a different kind of mistake.
#
# BACKEND_URL is left unset on purpose: the default is now empty, meaning same-origin.
RUN npm run config:sync \
    && npx tsc -b \
    && npx vite build

# ============================================================
# Stage 2: builder — install all Python dependencies
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
# Stage 3: runtime — minimal image with only installed packages
# ============================================================
FROM python:3.12-slim

# Non-root user with UID 1000.
RUN useradd -m -u 1000 user

# Copy installed Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy constitutions to the site-packages-relative path SafetyFilter expects.
COPY --from=builder /build/dataforge_src/constitutions /usr/local/lib/python3.12/site-packages/constitutions

# Copy application code
COPY playground/api/app.py /home/user/app/app.py
COPY playground/api/samples/ /home/user/app/samples/
# Intentionally empty, and intentionally present: it stops SlowAPI reading a repository .env at
# import time. It was never copied before, so every container start logged
# "Config file '/home/user/app/slowapi.env' not found" -- a warning in production for a file
# whose whole job is to exist.
COPY playground/api/slowapi.env /home/user/app/slowapi.env

# The built SPA. app.py serves this under /playground and reports
# frontend_hosting="api_only" if it is absent, so a missing bundle is visible rather than silent.
COPY --from=web /web/dist /home/user/app/web

# Switch to non-root user
USER user
WORKDIR /home/user/app

# Container Apps target port, and the port HF Spaces expects.
EXPOSE 7860

# Environment
ENV PORT=7860
ENV DATAFORGE_PLAYGROUND_DEV=0

# Start uvicorn with single worker (slowapi in-memory limiter contract)
# and honor PORT for the host's runtime assignment.
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860} --workers 1 --timeout-keep-alive 5"]
