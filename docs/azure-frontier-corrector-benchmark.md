# Azure frontier-corrector benchmark (gpt-5.6-sol)

Definitive, reproducible measurement of a frontier model (Azure OpenAI
`gpt-5.6-sol`) used as an **auto-apply corrector** in DataForge, across the RAHA
datasets, with a **distribution-free conformal certificate** of how much
auto-apply coverage the model actually earns.

This is the honest, published counterpart to the product decision: the corrector
stays **propose-not-apply**. The artifact proves that decision is
*distribution-free correct*, rather than asserting it.

## Scope: do not burn credit to reconfirm a known null

The corrector result is already established and committed:
`eval/results/corrector_gpt56sol_certified_coverage.json` shows certified
auto-apply coverage 0.0 (confidently-wrong on the underivable residual, ECE ~0.96).
The verified gate rejects all LLM corrections *by design*, so benchmarking any
model at that gate answers a predetermined question. Treat the runs below as
**confirmatory, not exploratory**, and prefer the smallest evidence that adds a
new fact:

- Default: rely on the existing committed artifact. No new spend.
- If you want a generality data point beyond hospital, run **one** additional
  dataset - `tax` (largest / hardest) - and stop. Running all four is redundant.
- Each live run spends real Azure credit and needs explicit go-ahead.

## Why this is safe to run repeatedly

Two reliability fixes make a long paid run survive a slow chunk instead of
aborting mid-flight (the failure mode that killed earlier runs):

- `AzureBenchClient._post` now **retries `httpx.TimeoutException`** with bounded
  backoff (capped at `max_retry_after_s`) and only raises after the retry budget
  is exhausted - mirroring the existing 429/503 path.
- The agent-path `_complete_azure` now honours **`DATAFORGE_AZURE_TIMEOUT_S`**
  (default 60s) instead of a hardcoded 60s, matching the bench client.

Set a generous timeout for reasoning deployments, e.g. `DATAFORGE_AZURE_TIMEOUT_S=180`.

## Cost control (read before running)

Every run below spends real Azure credit. Each is **bounded**:

- `DATAFORGE_AZURE_MAX_USD` is a hard cost cap - the client stops issuing calls
  once the estimated spend exceeds it. Keep the account default (`15`).
- `DATAFORGE_CORRECTOR_MAX_ISSUES` caps the number of scored corrections per
  dataset (a deterministic seeded subsample of the residual). Start at `30`.
- Run **foreground and bounded** in a single terminal call. Detached / background
  runs are unreliable here (they die at session boundaries); a foreground call
  that finishes inside its own timeout is the proven pattern.

## Environment

```
DATAFORGE_LLM_PROVIDER=azure
AZURE_API_KEY=<key1 for the Foundry resource>
AZURE_OPENAI_ENDPOINT=https://praneshrajank15-8087-resource.openai.azure.com
AZURE_OPENAI_API_VERSION=2025-04-01-preview
DATAFORGE_AZURE_MODEL=gpt-5.6-sol
DATAFORGE_AZURE_MAX_USD=15
DATAFORGE_AZURE_MAX_TOKENS=2048
DATAFORGE_AZURE_TIMEOUT_S=180
DATAFORGE_CORRECTOR_MAX_ISSUES=30
```

`gpt-5.6-sol` returns content directly at default settings (no
`reasoning_effort` tuning needed), unlike `gpt-5-mini`.

## Step 1 - one bounded run per dataset (spends credit)

Run each on its own; do **not** overwrite `eval/results/agent_comparison.json`
(use the explicit `--output-json` scratch paths below - the benchmark-truth gate
depends on `agent_comparison.json`).

```powershell
# hospital (flagship regression anchor)
dataforge bench --methods llm_corrector --datasets hospital --seed-list 0,1,2 `
  --output-json eval/results/corrector_gpt56sol_hospital.json

# flights
dataforge bench --methods llm_corrector --datasets flights --seed-list 0,1,2 `
  --output-json eval/results/corrector_gpt56sol_flights.json

# rayyan
dataforge bench --methods llm_corrector --datasets rayyan --seed-list 0,1,2 `
  --output-json eval/results/corrector_gpt56sol_rayyan.json

# tax (large; consider a smaller DATAFORGE_CORRECTOR_MAX_ISSUES)
dataforge bench --methods llm_corrector --datasets tax --seed-list 0,1,2 `
  --output-json eval/results/corrector_gpt56sol_tax.json
```

Each output JSON carries `calibration_samples_by_class` per seed. If a dataset is
skipped (missing credential / cost cap), its record records the skip reason
honestly and it is simply omitted from the artifact.

## Step 2 - build the definitive artifact (offline, no credit)

Pools **all seeds** per dataset (rigorous - not seed-0 only) and certifies each
dataset independently:

```powershell
python scripts/bench/certified_coverage_report.py `
  --run hospital=eval/results/corrector_gpt56sol_hospital.json `
  --run flights=eval/results/corrector_gpt56sol_flights.json `
  --run rayyan=eval/results/corrector_gpt56sol_rayyan.json `
  --run tax=eval/results/corrector_gpt56sol_tax.json `
  --out-json eval/results/frontier_corrector_benchmark.json `
  --out-md docs/frontier-corrector-benchmark-results.md
```

Use whichever `--run` lines correspond to datasets you actually ran.

Pooling caveat: the post-processor pools samples across seeds to raise
certification power. This assumes approximate exchangeability across seeds; it is
sound here only because the outcome is a null (certified coverage 0.0), where
added dependence is conservative (it can only inflate coverage, never deflate).
Never use pooled certification to claim non-zero coverage.

## Expected, honest outcome

Based on the measured hospital result (63 pooled proposals, ~4.8% accuracy, ECE
~0.96) the corrector is *confidently wrong* on the underivable residual. The
certificate is expected to report **certified auto-apply coverage 0.0 at every
tested alpha** on every dataset. That is the point: the verified gate correctly
refuses to auto-apply frontier guesses, and the artifact proves propose-not-apply
is distribution-free correct - calibration, not model capability, is the binding
constraint.
