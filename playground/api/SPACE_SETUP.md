# DataForge Playground - HF Space Setup

This document contains the authoritative steps to deploy the API-only
playground backend to the Hugging Face Docker Space at
`<hf-user>/dataforge-playground`. It uses a staged build directory because the
Docker build needs files from the monorepo root.

## Prerequisites

- A Hugging Face account with write access to
  `https://huggingface.co/spaces/<hf-user>/dataforge-playground`
- A cached Hugging Face token available to `huggingface_hub`

## Step 1: Ensure the Space exists

The production target is:

```text
https://huggingface.co/spaces/<hf-user>/dataforge-playground
```

The Python deploy script creates it if needed.

## Step 2: Deploy the staged Space contents

From the monorepo root:

```bash
python scripts/playground/stage_space.py --output-dir .hf-space-stage
python scripts/playground/deploy_space.py \
  --repo-id Praneshrajan15/dataforge-playground \
  --origins https://dataforge.praneshrajan15.workers.dev
```

`stage_space.py` is the authoritative layout builder. The deploy script uses
the same staging flow, then uploads a clean Hugging Face Space repo root
containing:

- `README.md`
- `Dockerfile`
- `pyproject.toml`
- `playground/api/`
- `dataforge/`
- `constitutions/`

The backend remains API-only. The browser UI is deployed separately to
Cloudflare Workers Static Assets.

## Step 3: Configure Space variables and secrets

The deploy script sets:

- `DATAFORGE_PLAYGROUND_ORIGINS`
  Required in production. Set this to the exact Cloudflare frontend origin.
  Example: `https://dataforge.praneshrajan15.workers.dev`
- `GROQ_API_KEY` or `GEMINI_API_KEY`
  Optional. If present in the local environment, the deploy script syncs it as
  a Space secret and advanced mode becomes available.
- `DATAFORGE_LLM_PROVIDER`
  Optional. Set this explicitly if you want to force a provider selection.

If you attach a custom frontend domain, add that exact origin to
`DATAFORGE_PLAYGROUND_ORIGINS` as a comma-separated value.

## Step 4: Verify

```bash
curl -s https://Praneshrajan15-dataforge-playground.hf.space/api/health
curl -s -X POST \
  -F "file=@playground/api/samples/hospital_10rows.csv" \
  https://Praneshrajan15-dataforge-playground.hf.space/api/profile
python scripts/playground/verify_frontend_deploy.py \
  --backend-url https://Praneshrajan15-dataforge-playground.hf.space
```

Expected health response:

```json
{"status":"ok","advanced_available":false,"max_upload_bytes":1048576}
```

## Run locally

From the monorepo root:

```bash
python -m pip install -e ".[dev]"
pip install -r playground/api/requirements.txt
docker build -f playground/api/Dockerfile -t dataforge-playground .
docker run -p 7860:7860 -e DATAFORGE_PLAYGROUND_DEV=1 dataforge-playground
```

## Troubleshooting

- If the build cannot find `dataforge/` or `constitutions/`, the staged tree was
  not used.
- If advanced mode is unavailable unexpectedly, verify the provider key is set
  in the Space secrets.
- If rate limiting behaves inconsistently, make sure the container still runs
  with `--workers 1`.
- If you want GitHub-driven deployments, use the manual workflow in
  `.github/workflows/sync-to-hf.yml` instead of pushing the monorepo root to
  the Space.
