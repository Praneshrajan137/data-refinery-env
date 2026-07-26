# gpt-5.6-sol as a verified proposer in the playground

Phase C surfaces a first-party Azure deployment (gpt-5.6-sol) as an agent
proposer in the playground, driven through the **same verified gate** as every
other proposer. This makes the project's actual moat legible with a frontier
model: the model **proposes**, the verifier **disposes**.

## What changed

- **Policy resolution** (`_resolve_agent_policy` in `playground/api/app.py`).
  Agent mode is no longer hardwired to the weak remote Space. Resolution order
  (least surprise - an existing remote deployment is never silently switched):
  1. Explicit `DATAFORGE_PLAYGROUND_AGENT_POLICY` (e.g. `hosted:azure`, `remote`).
  2. Else `remote` when `DATAFORGE_REMOTE_MODEL_URL` is set (preserves existing deployments).
  3. Else `hosted:azure` when `AZURE_API_KEY` is set (the frontier proposer).
  4. Else `remote` (fails fast into a 400 when nothing is configured).
  To showcase gpt-5.6-sol on a box that also has a remote Space, set the explicit
  override `DATAFORGE_PLAYGROUND_AGENT_POLICY=hosted:azure`.
- **Capability gating**. `_agent_available()` is now true when either the remote
  Space or an Azure deployment is configured; `_advanced_available()` also
  recognizes `AZURE_API_KEY`.
- **Health payload**. `/api/health` now returns `agent_policy` (e.g.
  `hosted:azure`) and `agent_provider` (e.g. `azure`) so the UI can show which
  proposer is live. The frontend agent toggle labels an Azure proposer as
  "Frontier model (Azure), verified (dry run)".

## Configuration

Set these on the playground backend (the same values the corrector/teacher runs
use):

```
AZURE_API_KEY=<key1 for the Foundry resource>
AZURE_OPENAI_ENDPOINT=https://praneshrajank15-8087-resource.openai.azure.com
AZURE_OPENAI_API_VERSION=2025-04-01-preview
DATAFORGE_AZURE_MODEL=gpt-5.6-sol
DATAFORGE_LLM_PROVIDER=azure
```

Agent mode runs `run_agent_repair(policy="hosted", provider="azure", mode="dry_run")`.
Every proposed fix is safety- and SMT-verified before display; nothing is applied.

## Honest framing (do not oversell)

On the measured hospital run, **0 of the frontier model's proposals passed the
gate** (all agent FIX attempts were rejected by SMT+safety; only the deterministic
floor fixes were verified). That is the demonstration, not a failure: a stronger
proposer does **not** bypass the verified+calibrated gate. The value surfaced to
the browser is the legible proven-vs-rejected guardrail with a frontier model at
the wheel - not fix volume. This mirrors the corrector result (confidently wrong
on the underivable residual) and the DECISIONS 2026-07-24 / 2026-07-25 entries.
