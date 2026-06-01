# DataForge Playground - Cloudflare Workers Static Assets Deployment

This document contains the authoritative steps to deploy the React/Vite
playground frontend to Cloudflare Workers Static Assets. The API backend stays
in the separate Hugging Face Docker Space and is wired through
`playground/web/config.js`, which is copied into
`playground/web/public/config.js` during `npm run build`.

## Prerequisites

- A Cloudflare account on the free tier
- Node.js 22 or newer
- The repository pushed to GitHub
- A live Hugging Face Space URL for the API backend

## Step 1: Configure the backend URL

Set the backend URL in the shell that builds the frontend:

If deploying from Cloudflare's dashboard build settings, set the user build
command to render `playground/web/config.js` before running the build:

```powershell
$env:BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space"
python scripts/playground/render_web_config.py --output-path playground/web/config.js
npm --prefix playground/web ci
npm --prefix playground/web run build
```

For local scripted deployments, either edit `playground/web/config.js` directly
or render the public config file:

```powershell
$env:BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space"
python scripts/playground/render_web_config.py --output-path playground/web/config.js
```

## Step 2: Build the static app

From the repository root:

```bash
npm --prefix playground/web ci
npm --prefix playground/web run build
```

The build first syncs `playground/web/config.js` into
`playground/web/public/config.js`, then writes `playground/web/dist` with Vite
base `/playground/`.
`wrangler.toml` binds those assets to a tiny Worker router that strips the
`/playground` prefix before serving assets. `public/_headers` is copied into
the build so `config.js` is served with `Cache-Control: no-store` and hashed
assets are long-cacheable.

## Step 3: Deploy

```bash
npx wrangler@4.94.0 deploy --config wrangler.toml
```

The default config deploys to the mandatory production route:
`https://dataforge.dev/playground`.
`wrangler.toml` includes a `[build]` command, so `npx wrangler deploy` creates
`playground/web/dist` before Wrangler checks the static assets directory.

## Step 4: Verify

```bash
python scripts/playground/verify_frontend_deploy.py
python scripts/playground/monitor_playground.py --json
node scripts/playground/audit_live_playground.mjs --json
dataforge release playground-check --json
```

The verifier checks that:

- `https://dataforge.dev/playground` serves the built React shell and hashed assets.
- `config.js` contains the Hugging Face backend URL and is uncached.
- The backend root returns API metadata instead of stale frontend HTML.
- `/api/health` exposes `status`, `advanced_available`, and `max_upload_bytes`.
- Backend CORS allows the exact deployed frontend origin.
- The release checklist also rejects broad wildcard CORS, runs a sample
  profile/repair smoke flow, and confirms the local release doctor passes.

## Quality Gates

Run the frontend gates locally before deployment:

```bash
npm --prefix playground/web run typecheck
npm --prefix playground/web run test:unit
npm --prefix playground/web run test:e2e
```

The browser suite covers sample and upload flows, repair dry-run evidence,
keyboard tabs, mobile viewport behavior, export/copy actions, and axe-powered
accessibility checks.

The scheduled production monitor lives in
`.github/workflows/playground-monitor.yml` and runs the same public endpoint
checks every 15 minutes. Set `PLAYGROUND_ALERT_WEBHOOK_URL` as a repository
secret only if an external alert target is desired.

## Notes

- No browser persistence is used.
- No API keys are embedded in the frontend; provider keys stay in Hugging Face
  Space secrets.
- In production, `DATAFORGE_PLAYGROUND_ORIGINS` must contain the exact
  `https://dataforge.dev` origin. The backend does not allow broad wildcards.
- `https://dataforge.dev/playground` is not optional launch polish; it is the
  release-blocking production domain for the full original vision.
