# DataForge Playground - HF Space Setup

This document contains the authoritative steps to deploy the API-only
playground backend to the Hugging Face Docker Space at
`Praneshrajan15/data-quality-env`. It uses a staged build directory because the
Docker build needs files from the monorepo root.

## Prerequisites

- A Hugging Face account with write access to
  `https://huggingface.co/spaces/Praneshrajan15/data-quality-env`
- Git installed locally
- Optional: `hf` if you want to create the Space from the CLI

## Step 1: Ensure the Space exists

The current production target is the existing Space
`Praneshrajan15/data-quality-env`.

If you ever need to recreate it from scratch:

```bash
hf repos create Praneshrajan15/data-quality-env --type space --space-sdk docker --exist-ok
```

## Step 2: Stage the exact Space contents

From the monorepo root:

```bash
python scripts/playground/stage_space.py --output-dir .hf-space-stage
```

This produces a clean Hugging Face Space repo root containing:

- `README.md`
- `Dockerfile`
- `pyproject.toml`
- `playground/api/`
- `dataforge/`
- `constitutions/`

## Step 3: Clone the target Space repo

```bash
git clone https://huggingface.co/spaces/Praneshrajan15/data-quality-env .hf-space-repo
```

If your Git credential helper is not already configured, authenticate with your
Hugging Face username and token when prompted.

## Step 4: Replace the Space contents with the staged tree

PowerShell:

```powershell
Get-ChildItem .hf-space-repo -Force | Where-Object { $_.Name -ne '.git' } | Remove-Item -Recurse -Force
Copy-Item .hf-space-stage\* .hf-space-repo -Recurse -Force
```

Bash:

```bash
rsync -a --delete --exclude '.git/' .hf-space-stage/ .hf-space-repo/
```

## Step 5: Commit and push

```bash
cd .hf-space-repo
git add .
git commit -m "deploy: sync staged DataForge Space"
git push origin main
```

The backend remains API-only. The browser UI is deployed separately to
Cloudflare Pages.

## Step 6: Configure Space variables and secrets

In the Space settings:

- `DATAFORGE_PLAYGROUND_ORIGINS`
  Example: `https://dataforge.pages.dev`
- `GROQ_API_KEY` or `GEMINI_API_KEY`
  Optional. Enables advanced mode in the hosted playground.
- `DATAFORGE_LLM_PROVIDER`
  Optional. Set this explicitly if you want to force a provider selection.

## Step 7: Verify

```bash
curl -s https://Praneshrajan15-data-quality-env.hf.space/api/health
curl -s -X POST \
  -F "file=@playground/api/samples/hospital_10rows.csv" \
  https://Praneshrajan15-data-quality-env.hf.space/api/profile
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
docker build -f playground/api/Dockerfile -t data-quality-env-playground .
docker run -p 7860:7860 -e DATAFORGE_PLAYGROUND_DEV=1 data-quality-env-playground
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
