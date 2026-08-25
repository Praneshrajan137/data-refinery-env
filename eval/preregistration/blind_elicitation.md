# Pre-registration: does blind elicitation reduce the false-accept rate?

Registered 2026-08-24, **before any labelling call**. Amendments are appended with timestamps and
never rewrite the original text.

## Why this exists

`docs/trust/stratified-label-noise-result.md` established that human-labelled per-table
certification at `alpha = 0.05` is dead: the binding control class gives `beta_upper = 0.8712`
against a pre-registered 0.35 threshold, and the inflation factor `1/(1 - beta)` = 7.76 puts 0.05
out of reach at any sample size.

That document names one surviving route:

> A protocol that elicits the correct value **before** revealing the machine's proposal is a
> different process with a different `beta`.

That is arm **H2** in `human_label_noise.md`. It is currently an argument, not a measurement. This
probe measures the mechanism H2 rests on.

## What this can and cannot claim

**The labeller is an LLM (gpt-5.6-sol), not a human.** This is recorded here rather than discovered
later, because the temptation to let it stand in for a human `beta` is exactly the category error
this project has retracted claims for. Three reasons it cannot:

1. It is not a human, and H2's claim is about human labelling.
2. It cannot be blind in the required sense; the operator's context carries the corpora.
3. **Labeller-corrector correlation.** If the corrector is gpt-5.6-sol and the labeller is
   gpt-5.6-sol, their errors are correlated, and correlated errors push `beta` **down** -- the
   anti-conservative direction that killed the pooled bound.

So this probe **may not certify anything** and its `beta` may not be substituted for a human one.
A new `label_source` value carries that refusal in code rather than in prose.

What it *can* establish is a **within-labeller protocol contrast**: hold the labeller fixed and vary
only whether the machine's proposal is revealed before or after the labeller commits. That is the
same paired-contrast design as `contamination_audit.md`, and it tests H2's mechanism directly.

## Substrate

`rayyan`, chosen because its errors are natural and owner-cleaned -- the strongest corpus in the
registry -- and because it retains ground truth, so every verdict is objectively scorable.

Measured census: **948 real errors with retained truth**, distributed
`article_jcreated_at` 722, `author_list` 84, `article_jissue` 53, `article_pagination` 32,
`article_jvolumn` 22, `article_title` 14, `journal_issn` 12, `journal_title` 9.

**76% sit in one date column.** Sampling proportionally would make this a measurement about date
corrections. The sample is therefore **stratified with a per-column cap**, and the composition is
reported. Pairing controls for composition anyway, because both arms see identical items.

A control item requires: retained truth, a real corrector proposal, and that proposal being
**wrong**. Only the LLM corrector can supply these -- `wrong_value_on_a_real_error = 0` for the
deterministic repairers on this corpus, because they abstain rather than guess.

## The two arms

Identical items, identical context, independent API calls with no shared conversation.

* **RATIFY** -- the flagged value, column context, and the proposed replacement, asked
  "is this replacement correct?". This is the protocol whose `beta` was measured at 0.5000.
* **ELICIT** -- the flagged value and column context only, asked "what is the correct value?".
  The proposal is **never shown**. Scored afterwards by comparing the elicited value to the
  proposal.

## The capability control, which decides whether the result means anything

A one-sided rate cannot separate the effect from an artifact. If ELICIT's `beta` is lower simply
because the labeller rarely reproduces *any* specific string, that is low agreement, not reduced
acquiescence. So ELICIT is scored **three ways**:

| outcome | reading |
| --- | --- |
| elicited == withheld truth | the labeller had the capability and used it |
| elicited == wrong proposal | a false accept, counted in `beta_elicit` |
| elicited == neither | uninformative; the labeller could not produce the value |

`elicited_matches_truth` is the capability control. **If it is near zero, the arm is uninformative
regardless of what `beta_elicit` shows**, and that is a VOID condition, not a favourable result.

String comparison is case-folded and whitespace-normalised, fixed here in advance.

## Predictions, and what refutes them

* **P1 (primary)**: `beta_elicit < beta_ratify` on paired items. This is H2's premise.
* **P2 (capability)**: `elicited_matches_truth > 0.10`. Below this the arm is VOID.
* **P3 (correlation)**: because labeller and corrector share a model, `beta_elicit` will be
  **higher than a labeller-independent protocol would give**. Registered because it predicts a
  *limit* on the result, and because it makes the shared-model design a stated confound rather than
  a discovered one.

## Kill criterion

**If `beta_elicit >= beta_ratify` and P2 holds, H2's mechanism is refuted for this labeller.**
Blind elicitation would then not be a route to certification, and the honest conclusion is that
per-table certification from machine-proposal review is closed by protocol, not merely by sample
size. That result is published with the same prominence as a favourable one.

**If P2 fails, the arm is VOID** and no claim is made in either direction.

## Statistics

Paired sign-flip permutation test on the per-item difference in accept indicator, 20,000 resamples,
seed 0, add-one correction, `alpha = 0.01`. Same implementation as the contamination audit
(`dataforge.bench.contamination.paired_signflip_p_value`), which raises on all-zero deltas rather
than reporting a p-value of 1.0.

Wilson intervals on each arm's `beta`. **No Clopper-Pearson certification bound is computed**,
because that would imply this probe could certify.

## Budget

`--max-usd 4`, hard-refused above it. Campaign spend to date is $8.94 of a $46 ceiling.

## Fixed before running

Sample seed 0. Per-column cap 12. `reasoning_effort="none"` for both arms, so the contrast is not
confounded by effort. `max_tokens` 512. Prompts are built by one function that takes the arm as a
parameter, and a test asserts the two prompts differ **only** in the proposal-revealing sentence.

---

## AMENDMENT 1, recorded after the run, 2026-08-24

The probe returned **VOID** on the pre-registered capability floor: `elicited_matches_truth` =
0.0278 against a floor of 0.10. Per the terms fixed above, **no directional claim is made**, even
though `beta_elicit` (0.8194) came out below `beta_ratify` (0.9583).

Recording three things the run exposed, so the next design starts from them.

**1. A selection artifact in my own design, and it is structural rather than a slip.**
Measuring `beta` requires conditioning on the proposal being wrong -- that is what `beta` is
defined on. But the proposals were harvested using the *same model, prompt and effort* that the
ELICIT arm then uses as the labeller. So conditioning on "the proposal is wrong" is identical to
conditioning on "the ELICIT labeller already got this wrong". The ELICIT arm was therefore run only
on items it had already failed, and it cannot succeed on them except by changing its mind. The
measured 0.0278 capability is the arithmetic consequence, not a property of the labeller.

**2. The ELICIT arm measured self-consistency, not independent judgement.** It reproduced the
corrector's exact wrong value on **81.94%** of items. That is P3 confirmed far more strongly than
predicted, and it means the arm was a test-retest of the corrector wearing a labeller's name.

**3. The precondition H2 was missing.** Blind elicitation cannot reduce `beta` unless the
**labeller is independent of the corrector**. Revealing the proposal later does not help if the
labeller would have produced the same wrong value anyway. human_label_noise.md specifies *when*
the proposal is revealed and says nothing about *who* labels; on this evidence the independence
condition is load-bearing and must be stated alongside the ordering condition.

**Consequence for the design, not a patch to this run.** A valid test needs a labeller drawn from a
different error distribution than the corrector. With one model available that is unreachable, so
this probe is not re-run at a different sample size or threshold -- it is the wrong instrument, and
re-running it would only buy a tighter estimate of an uninformative quantity. The pre-registered
VOID stands.

## Amendment 2, 2026-08-25: the premise in "Why this exists" was overstated

**The body above is not edited.** This records an error in this document's motivation section, found
while finally computing the alpha=0.20 fallback that `DECISIONS.md` had been naming for weeks.

Lines 8-11 state that `beta_upper = 0.8712` and an inflation factor of 7.76 "puts 0.05 out of reach
at any sample size". **That is false.** An inflation factor multiplies the required sample size; it
cannot create an asymptote, because the measured Clopper-Pearson bound `1 - (delta/2)^(1/n)` tends to
zero as `n` grows, so `measured / (1 - beta)` can be driven below any positive alpha. Computed at the
measured beta with `delta = 0.05` and zero observed errors, alpha=0.05 needs **572** error-free
labels rather than infinitely many -- roughly 7.76x the 82 required at a negligible beta, which is
the inflation factor almost exactly.

**Nothing in this pre-registration's design, predictions or verdict depends on the error.** The
kill criterion it inherited was pre-registered as `beta_upper > 0.35`, defined as "alpha=0.05
unreachable inside ~200 judgements", and 572 exceeds 200. Human ratification remains closed on
budget. The reason this document existed -- that blind elicitation is a different protocol with a
different `beta` -- is untouched, as is its VOID verdict and the labeller-independence precondition
recorded in Amendment 1.

Corrected in place in `docs/trust/stratified-label-noise-result.md` and `DECISIONS.md`, which also
carry the full alpha table. Recorded here rather than edited above, because a pre-registration whose
motivation can be quietly rewritten after the fact is not a pre-registration.