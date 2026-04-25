# DataForge Playground - Cloudflare Pages Deployment

This document contains the authoritative steps to deploy the static frontend to
Cloudflare Pages. The backend is a separate Hugging Face Space and is wired into
the frontend through `playground/web/config.js`.

## Prerequisites

- A Cloudflare account on the free tier
- The repository pushed to GitHub
- A live Hugging Face Space URL for the API backend

## Step 1: Connect the repository

1. Log in to Cloudflare Dashboard.
2. Go to **Workers & Pages** -> **Create application** -> **Pages**.
3. Connect the GitHub repository.

Use these build settings:

- **Project name**: `dataforge`
- **Production branch**: `main`
- **Build command**:

```bash
sed -i "s|BACKEND_URL: \"\"|BACKEND_URL: \"$BACKEND_URL\"|g" playground/web/config.js
```

- **Build output directory**: `playground/web`
- **Root directory**: `/`

Only `config.js` is rewritten at build time. `app.js` remains a committed,
cacheable application bundle.

## Step 2: Set the backend URL

In the Cloudflare Pages project settings, add:

| Variable | Value | Scope |
| -------- | ----- | ----- |
| `BACKEND_URL` | `https://Praneshrajan15-data-quality-env.hf.space` | Production |
| `BACKEND_URL` | `https://Praneshrajan15-data-quality-env.hf.space` | Preview |

## Step 3: Deploy

After the first successful deploy, the frontend will be served at:

- Production: `https://dataforge.pages.dev`
- Preview: `https://<hash>.dataforge.pages.dev`

## Step 4: Verify

```bash
curl -s https://dataforge.pages.dev | head -5
curl -s https://Praneshrajan15-data-quality-env.hf.space/api/health
```

Confirm that:

- The page loads with `config.js`, `style.css`, and `app.js` as relative assets.
- The frontend warms `/api/health` on load.
- The advanced toggle matches the backend's `advanced_available` field.

## Notes

- No custom domain is required for the free-tier launch.
- No browser storage is used.
- No API keys are embedded in the frontend; all provider keys stay in Hugging
  Face Space secrets.
