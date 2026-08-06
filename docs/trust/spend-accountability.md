# Spend accountability

DataForge's guarantee is that nothing is written without proof, and that the proof
re-verifies independently. Paid inference was the one part of the system with none
of that. This document records what was wrong, what changed, and what is still
only an estimate.

## What was wrong

Four concrete defects, all measured rather than suspected:

1. **The product path was unmeterable.** `dataforge.agent.providers.complete()`
   returned a bare `str`. No token usage, no cost, therefore no cap. The runbook
   said so plainly: `dataforge repair --agent` had "No per-call USD guard -- the
   product `providers.complete` path is unguarded."
2. **Spend was computed and then discarded.** The benchmark clients accumulated a
   `cumulative_usd` estimate in memory and dropped it at process exit. Across the
   whole repository the only recorded dollar figure was a prose note (`~$1.33`).
3. **The pre-flight guard was not a spend guard.** `validate_estimated_calls`
   refused runs above 500 *calls*. Prices existed. A call estimate existed. They
   were never multiplied, so a bounded run against a metered frontier deployment
   passed unexamined and only tripped the in-flight cap after real money was gone.
4. **The guard was triplicated and one provider had none.** Three near-identical
   cost blocks lived in the OpenAI-compatible, Azure, and Bedrock clients, with
   prices in constructor defaults. `GeminiBenchClient` took no price arguments at
   all -- entirely unmetered despite being billable.

## What changed

One module, `dataforge/spend.py`, and three independent layers:

| Layer | Mechanism | Fails how |
| --- | --- | --- |
| Pre-flight | `estimate_usd` x `estimate_llm_calls`, checked in `validate_estimated_calls` | refuses **before** the first call |
| In-flight | `SpendMeter.record` raises `CostCapExceededError` | hard stop mid-run |
| After the fact | `SpendReceipt` appended to `eval/results/spend_ledger.json` | auditable afterwards |

Supporting changes:

- `providers.complete_with_usage()` returns text **plus** `Usage`
  (including `reasoning_tokens`, which are billed as output but were invisible).
  `complete()` delegates to it, so every existing caller is unaffected.
- The product path is now capped by `DATAFORGE_MAX_USD`, with
  `DATAFORGE_<PROVIDER>_MAX_USD` overriding per provider.
- One price table replaces the constructor-default prices. Prices are deliberately
  **conservative (high)** so the guard trips early: an estimate that is too
  pessimistic costs a re-run, one that is too optimistic costs money.
- Unpriced providers (Groq, Cerebras -- free tiers) have **no** table entry. A
  missing entry disables the USD guard rather than inventing a number, preserving
  the previous no-op behavior exactly.
- Missing usage now **fails closed** on a metered run. Previously a provider that
  omitted its usage payload silently contributed zero cost, which would let a
  capped run overspend undetected.
- `.env.example` documents every billable knob, and `tests/unit/test_env_contract.py`
  derives its expectations from the code: every priced provider in `PRICES` must
  have a documented cap, so adding a metered provider without documenting its cap
  fails CI.

## Honest limits

- **`estimated_usd` is an estimate, not an invoice.** It is conservative token
  accounting at table prices. The authoritative figure is the provider's billing
  portal. The field is named `estimated_usd` for that reason.
- **Much of this phase's own recorded spend was NOT measured.** The ledger reads
  **$17.81 total = $10.48 measured (9 receipts) + $7.34 reconstructed (3 receipts)** --
  **59% measured**, plus 1 no-op receipt. The certification work alone was only 46% measured;
  every run after this layer landed is fully measured. This is the most important limitation
  on this page, and it has a specific cause: the accountability layer was built *during* the
  runs it was meant to measure, so the early runs are exactly the ones it failed to capture.
  Do not read a ledger total as an observation without checking the split.
- **Use `ledger_summary`, not `total_estimated_usd`, when reporting to a human.** A bare
  total conceals the measured/reconstructed ratio. `LedgerSummary.describe()` prints it:

  ```
  $17.81 total = $10.48 measured (9 receipts) + $7.34 reconstructed (3 receipts); 59% measured; 1 no-op receipts
  ```

- **A receipt with `calls == 0` and nonzero USD is a reconstruction by definition** --
  without token counts its USD cannot have come from an observation. That is how the split
  is derived, rather than from a flag someone must remember to set.
- **A receipt with `calls == 0` and zero USD is a no-op, not an estimate.** A run whose
  every request was rejected (an expired key, say) made no billable call and claims nothing,
  so counting it as a reconstruction would overstate how much of the ledger is unverified.
  `LedgerSummary.noop_receipts` tracks these separately. This distinction was found by a
  guard test failing on a real 401'd run, not by inspection.

- **Token prices are configuration, and a model switch silently invalidates them.** When the
  Azure resource changed mid-campaign the deployment changed with it, from `gpt-5.6-sol` to
  `gpt-5-mini`, whose published rates are ~20x cheaper ($0.00025/$0.002 per 1K vs
  $0.005/$0.015). Leaving the old rates configured would have overstated every subsequent
  receipt by roughly that factor. `scripts/bench/repoint_azure_env.py` updates model,
  endpoint and both prices together, and records that the rates are Microsoft's published
  list values because the Azure retail prices API returned no `gpt-5-mini` meters.
- **Reconstructions must state their method and their bound.** One of this phase's
  reconstructions was initially wrong by up to ~100x: it assumed a stalled run had been
  throttled through ~540 calls, when the arithmetic
  (`max_retries=5 x timeout 180s + backoff = 920s`) showed a single hung request. It was
  reissued as a **rigorous upper bound** ($1.21, from "the run never reached its
  checkpoint at index 25, so fewer than 25x9=225 calls") with the point estimate
  (~$0.03-0.16) recorded alongside. Prefer a defensible bound over a plausible-sounding
  point estimate.
- **Reasoning tokens are counted inside `completion_tokens`**, as providers report
  them; `reasoning_tokens` is recorded separately for visibility, never added twice.
- **A cancelled process can under-report.** Receipts are now written at every
  checkpoint rather than only at completion, but a process killed between checkpoints
  loses the interval. Checkpoint intervals must therefore be short enough that the lost
  interval is small -- 25 issues was too coarse and cost this phase a whole run's data.
- **Resumed runs must carry prior spend forward.** `cumulative_usd` is per-process,
  so a resumed run reports only its own segment unless the prior amount is added.
  The sweep script does this and stamps
  `includes_prior_segments_usd=...` on the receipt.

## Reading the ledger

```
python -c "from pathlib import Path; from dataforge.spend import ledger_summary; print(ledger_summary(Path('eval/results/spend_ledger.json')).describe())"
```

Each receipt records `run_id`, UTC timestamp, provider, model, calls, prompt /
completion / reasoning tokens, `estimated_usd`, the `cap_usd` in force, and free-form
`notes` used to record deviations and reconstructions. The ledger is append-only
across runs; a single run that checkpoints repeatedly updates its own entry so ten
checkpoints do not masquerade as ten runs.

## Why this belongs in `docs/trust/`

The same doctrine that governs data mutations governs money: state what was done,
record it in a form someone else can check, and name the parts that are estimates.
A system that refuses to write a cell without proof should not spend a dollar
without a receipt.
