# The corrector's false positives are all one kind, and nobody could see that before

Measured 2026-08-24. Implementation: `dataforge/bench/core.py`
(`RepairScoreBreakdown`, `decompose_repair_score`).
Tests: `tests/unit/test_repair_score_breakdown.py`.

No API spend. Deterministic repairers only.

## What `fp` was hiding

`score_repairs` puts two different failures in one bucket:

```python
# core.py, inside score_repairs
if clean_value is not None and repair.new_value == clean_value:
    tp += 1
else:
    fp += 1        # <- a clean cell overwritten, OR a real error fixed wrongly
```

And `fn = len(ground_truth_map) - len(matched)` merges two more: cells never touched, and cells
touched with a wrong value. So `fp` and `fn` each conflate a safe failure with a damaging one, and
on the injected corpora that was tolerable because nearly every flagged cell was a real error.

Decomposing both, with the terms reconciling to the originals exactly:

| corpus | writes | tp | fp | **overwrote a clean cell** | **wrong value on a real error** | abstained | damage rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| hospital (injected) | 594 | 451 | 143 | **143** | **0** | 58 | **0.2407** |
| rayyan (natural) | 7 | 0 | 7 | **7** | **0** | 948 | 1.0000 |
| flights (contested) | 9 | 0 | 9 | **9** | **0** | 4920 | 1.0000 |

## The finding

**On all three corpora, `wrong_value_on_a_real_error` is zero.** Every false positive the
deterministic repairers produce is a write to a cell the labels call clean. When they fire on a
cell that genuinely is erroneous, they get it right -- 451 of 451 on hospital.

That is the safer of the two failure modes, and it was invisible in a single `fp` count. `fp=143`
on hospital reads as "wrong 143 times"; what it actually says is "wrote to 143 cells that were
already correct, and corrupted zero known-broken cells".

The complementary number is the one to worry about: **24.07% of hospital writes land on a cell that
was already correct.** On rayyan and flights it is 100% of a much smaller write volume, which is
consistent with the correction F1 of 0.0000 those corpora already report.

Stated carefully: this is measured on three corpora with the deterministic repairers. It is not a
proof that no repairer can ever write a wrong value onto a broken cell, and the LLM corrector is
not covered here at all.

## Why the distinction is load-bearing for the next corpus

The two terms have different epistemic status, and on a revision-history corpus they diverge
sharply.

**`repaired_a_clean_cell` is bounded by survivorship on a natural corpus.** A revision-history
corpus labels an error only if somebody noticed and fixed it. An error nobody ever fixed is
labelled clean, so a *correct* repair of it is counted here as a failure. The term is an upper
bound on real damage, not a measurement of it.

**`wrong_value_on_a_real_error` carries no such caveat.** The cell is known to have been wrong, a
human recorded what it should be, and the corrector produced something else. That is a genuine
capability failure and it is the term a corrector should be judged on.

Conflating them makes a revision-history corpus unusable for diagnosis: a corrector would be
penalised identically for a real mistake and for fixing something the archive never got round to.
That is the reason this decomposition exists before the corpus does.

On **injected** corpora the survivorship caveat does not apply -- the generator knows exactly which
cells it corrupted, so the labels are complete and `repaired_a_clean_cell` is true damage. So the
hospital 0.2407 is a real damage rate; a future revision-history figure would be an upper bound.

## The double-count is now visible instead of implicit

`wrong_value_on_a_real_error` appears in **both** reconciliation sums:

```
fp = repaired_a_clean_cell + wrong_value_on_a_real_error
fn = abstained_on_a_real_error + wrong_value_on_a_real_error
```

That is not an error in the decomposition. It is a property of `score_repairs`: a wrong value on a
real error is counted once as a false positive and again as a false negative, so `fp + fn`
double-counts those cells. The behaviour is unchanged and defensible -- such a cell is both a false
alarm and a miss -- but it was implicit, and naming the shared term makes it checkable.

On these three corpora the term is zero, so nothing is currently double-counted. That will change
the first time a corrector writes a wrong value onto a real error, and the arithmetic will then say
so out loud.

## What was deliberately not changed

`RepairScore` keeps exactly its six fields. Committed artifacts read them, and
`dataforge.bench.corrector_promotion_verdict` gates promotion on `precision_at_auto_apply` and
`ece` derived from them, so changing the type would either break the gate or silently move it. The
decomposition is a **separate type** that reconciles to the original rather than replacing it, and
a test asserts `RepairScore.model_fields` is unchanged.

`damage_rate` returns `None` rather than 0.0 when nothing was written. A corrector that abstained
everywhere has no damage rate, and 0.0 would read as a safety result rather than as silence -- the
same discipline `ThreeWayScore` uses for undefined precision.

## Limits

1. **Three corpora, deterministic repairers only.** The LLM corrector is not measured here.
2. **`repaired_a_clean_cell` is an upper bound on damage wherever labels are incomplete**, which
   is every natural corpus. It is exact on injected ones.
3. **No abstention credit is introduced.** `score_repairs` still scores abstention as a false
   negative; this only makes the abstention visible as its own term.
4. **Changes no write gate.** This is measurement decomposition. No detector moves onto
   `CONSTRAINT_CHECKABLE_DETECTORS`, and no threshold moves.
