# Azure OpenAI teacher / measurement setup

DataForge uses a strong hosted model for two **non-training** roles:

1. **Teacher** — generate expert repair trajectories for SFT
   (`scripts/data/collect_sft_trajectories.py --teacher-provider azure`).
2. **Measurement** — the `llm_corrector` benchmark
   (`dataforge bench --methods llm_corrector`).

API keys never fine-tune a hosted model. They generate data and measure.

## Which model you can actually use

Microsoft Foundry sells models in two categories:

| Category | Examples | Billing | Works on a **free-trial / credit** subscription? |
| --- | --- | --- | --- |
| **Sold directly by Azure** (first-party Azure OpenAI) | GPT-5.5, GPT-5, gpt-4.1 | Subscription meter | **Yes** |
| **Partners & community** (Marketplace SaaS) | Anthropic Claude (Sonnet 5, Opus 4.8), Cohere, Mistral Large | Azure Marketplace | **No** |

Microsoft's own docs exclude "free trial" and "credit-based" and "sponsored
subscriptions that only use Azure credits" from Marketplace (partner) models.
So on a **$200 free-trial credit** subscription:

- **Claude / Opus / Sonnet 5 are NOT available.** DataForge fails fast with an
  actionable message if you point `DATAFORGE_AZURE_MODEL` at a Claude deployment.
- **GPT-5.5 (Azure OpenAI, first-party) IS available** on the credit and is the
  strongest teacher/measurement model you can use. Use it.

If you later upgrade to pay-as-you-go, Claude on Foundry becomes usable, but it
is served via the Anthropic Messages API (a different endpoint), not the
first-party Azure OpenAI path this provider uses.

## One-time setup

1. **Create an Azure OpenAI (Foundry) resource** in a region that offers the
   GPT-5 family (e.g. `eastus2`, `swedencentral`). In the Azure portal:
   *Create resource -> Azure OpenAI* (or *Microsoft Foundry*), same subscription
   as your credit.
2. **Deploy the strongest first-party chat model available in your region.**
   Prefer `gpt-5.5`; fall back to `gpt-5` or `gpt-4.1` if 5.5 is not offered in
   your region. Note the **deployment name** you choose (you address the model
   by deployment name, not model id).
3. **Capture three values** from the resource's *Keys and Endpoint* page:
   - Endpoint, e.g. `https://<resource>.openai.azure.com`
   - One API key (already in repo-root `.env` as `AZURE_API_KEY`)
   - A supported API version (use a recent preview for GPT-5, e.g.
     `2025-04-01-preview`).
4. **Set a budget alert.** Portal -> *Cost Management -> Budgets* -> create a
   budget at ~$150 (of the $200) with an 80% alert. DataForge also enforces a
   hard in-run USD cap (below), but the portal budget is your backstop.

## Environment variables

Add these to the repo-root `.env` (next to the existing `AZURE_API_KEY`).
`.env` is gitignored; do not commit real values.

```dotenv
# --- Azure OpenAI (first-party) teacher / measurement ---
AZURE_API_KEY=<already set>
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com
AZURE_OPENAI_API_VERSION=2025-04-01-preview
DATAFORGE_AZURE_MODEL=<your-deployment-name>   # e.g. gpt-5.5
DATAFORGE_LLM_PROVIDER=azure

# --- Hard cost guard (finite trial credit) ---
DATAFORGE_AZURE_MAX_USD=25                      # in-run hard stop
DATAFORGE_AZURE_MAX_TOKENS=256
# GPT-5 / reasoning deployments reject temperature != 1. Leave this UNSET so the
# provider omits temperature. Only set it for a non-reasoning deployment:
# DATAFORGE_AZURE_SEND_TEMPERATURE=1
```

Optional per-call cost-estimate knobs (defaults are conservative-high so the
guard trips early): `DATAFORGE_AZURE_USD_PER_1K_INPUT` (0.005),
`DATAFORGE_AZURE_USD_PER_1K_OUTPUT` (0.015).

## Bounded smoke test (one call)

Confirm the deployment is reachable and billable to credit with a single call
(no loops, no cost guard needed for one call):

```powershell
cd data_quality_env
.venv\Scripts\python.exe -c "import asyncio, os; from dotenv import load_dotenv; load_dotenv('../.env'); from dataforge.agent.providers import complete; print(asyncio.run(complete([{'role':'user','content':'Reply with the single word: ready'}])))"
```

Expected: the model prints a short reply (e.g. `ready`). Common failures:

- `AZURE_OPENAI_ENDPOINT is not set` -> add the endpoint to `.env`.
- HTTP 404 / `DeploymentNotFound` -> `DATAFORGE_AZURE_MODEL` must be the
  **deployment name**, not the model id; check spelling and region.
- HTTP 401 -> key/endpoint mismatch (key is from a different resource).
- HTTP 400 about `temperature` -> ensure `DATAFORGE_AZURE_SEND_TEMPERATURE` is
  unset (GPT-5 rejects non-default temperature).
- `... looks like an Anthropic Claude model ...` -> you pointed the deployment
  at Claude, which is unavailable on trial credit. Deploy a GPT-5 family model.

Once the smoke test returns text, Azure is ready for teacher-data generation and
the corrector benchmark.

## Generate F1=1.0 verified teacher trajectories

The teacher proposes ReAct repair trajectories (with reasoning). Only episodes
whose repairs **exactly match ground truth** are kept, so the SFT supervision is
clean labels plus rich reasoning for the hard residual. Set `--min-episode-f1 1.0`
for the strict, teacher-quality tier:

```powershell
cd data_quality_env
.venv\Scripts\python.exe scripts/data/collect_sft_trajectories.py `
  --preset full `
  --teacher-provider azure `
  --teacher-model $env:DATAFORGE_AZURE_MODEL `
  --min-episode-f1 1.0 `
  --datasets hospital,flights,beers `
  --output data/sft_traj/expert_v1_azure_verified.jsonl
```

Guards that keep this safe and honest:

- **F1=1.0 accept gate** — an episode is discarded unless the teacher's repairs
  match the clean values exactly (`min_episode_f1`).
- **Flights verifier** — a second-pass check approves ambiguous Flights repairs.
- **Hard USD cap** — `DATAFORGE_AZURE_MAX_USD` stops the run before it can
  overspend the trial credit.
- **Readiness validation** — `scripts/data/validate_sft_readiness.py` re-checks
  schema, leakage, dedup, and no-op ratio before the data is used for training.

The verified output then feeds the curriculum chain
(v5 repair -> v6 contract-minimal -> v8 schema-distill -> v9 action-envelope)
to produce the `prompt_completion` training file.

## gpt-5.6-sol (this account) — reproducible recipe & guard coverage

The active first-party deployment is **`gpt-5.6-sol`** (Azure OpenAI, format
`OpenAI`, GlobalStandard, ver `2026-07-09`) on the Foundry resource
`praneshrajank15-8087-resource` (`rg-praneshrajank15-0536`, eastus2). It returns
content directly with no reasoning-token overhead, so no `DATAFORGE_AZURE_REASONING_EFFORT`
tuning is needed (unlike gpt-5-mini).

Self-reproducing env recipe (the bench artifact's own `reproduction_command` omits
these — record them here so any run is reproducible):

```dotenv
DATAFORGE_LLM_PROVIDER=azure
DATAFORGE_AZURE_MODEL=gpt-5.6-sol
AZURE_OPENAI_ENDPOINT=https://praneshrajank15-8087-resource.openai.azure.com
AZURE_OPENAI_API_VERSION=2025-04-01-preview
DATAFORGE_AZURE_MAX_USD=15
DATAFORGE_AZURE_MAX_TOKENS=2048
# leave DATAFORGE_AZURE_SEND_TEMPERATURE UNSET (GPT-5 family rejects temp != 1)
```

### Cost-guard coverage (know what is and isn't protected)

| Entry point | USD guard | Bound |
| --- | --- | --- |
| `dataforge bench --methods llm_corrector` | Yes — `DATAFORGE_AZURE_MAX_USD` via `AzureBenchClient._enforce_cost_guard` | hard stop |
| `collect_sft_trajectories.py --teacher-provider azure` | Yes — same guard on the collection client | hard stop |
| `dataforge repair --agent --provider azure` | **No per-call USD guard** — the product `providers.complete` path is unguarded | bounded only by `--max-steps` and sample size |

Consequence: for the agent path, bound spend with a small `--max-steps` and a small
input sample. Adding a USD guard to the product provider is tracked as future work.

### Trial-credit runway

`az consumption`/budget APIs are preview and are **not queryable** on this
subscription — check remaining credit in the Azure Portal *Cost Management* instead,
and keep the portal budget alert (step 4 above) as the backstop. For reference, the
3-seed hospital corrector benchmark cost ~$1.33 of guarded spend.

### Teacher `--min-episode-f1`: use 0.75, not 1.0

`--min-episode-f1` gates the **whole** multi-chunk episode, not per chunk, so `1.0`
is effectively unachievable and collects **zero** trajectories. The proven
expert_v1 tier used `0.6`; use `0.75` for a high-quality-but-achievable gate:

```powershell
.venv\Scripts\python.exe scripts/data/collect_sft_trajectories.py `
  --preset full --teacher-provider azure --teacher-model gpt-5.6-sol `
  --min-episode-f1 0.75 --min-interval-s 0.5 --teacher-max-tokens 2000 `
  --max-runtime-min 40 --output data/sft_traj/expert_v1_azure_verified.jsonl
```


