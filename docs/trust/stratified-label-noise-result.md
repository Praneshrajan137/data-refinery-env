# Stratifying the label-noise bound fires the kill criterion that pooling was suppressing

Measured 2026-08-24. Implementation: `dataforge/conformal.py`
(`label_noise_adjusted_bound_stratified`, `StratifiedLabelNoiseBound`).
Tests: `tests/unit/test_stratified_label_noise.py`.
Pre-registration: `eval/preregistration/human_label_noise.md`.

No API spend. This is arithmetic on already-measured controls.

## The verdict

`eval/preregistration/human_label_noise.md` fixed this in advance:

> **If `beta_upper > 0.35`: human-labelled per-table certification at alpha=0.05 is DEAD.**

| bound | `beta_upper` | kill criterion |
| --- | --- | --- |
| pooled, as previously carried | **0.3125** | does **not** fire |
| stratified, worst class binds | **0.8712** | **FIRES** |

**Human-labelled per-table certification at `alpha = 0.05` is dead.** Not marginal, not
underpowered: the binding bound is 0.8712 against a 0.35 threshold.

`docs/trust/local-certification-result.md` has recorded for some time that pooling the two control
classes is "not defensible", and the project carried a `beta_scope_note` on the artifact instead of
fixing it. What that note was concealing is now measurable: **pooling was the only thing keeping a
pre-registered kill criterion from firing.**

## The two control classes were never interchangeable

| control class | false accepts | controls | rate | bound at per-class alpha |
| --- | --- | --- | --- | --- |
| `column_distribution` (value resampled from the column) | 2 | 30 | 0.0667 | 0.2445 |
| `corrector_generated` (real wrong corrector proposals) | 4 | 8 | **0.5000** | **0.8712** |
| pooled | 6 | 38 | 0.1579 | 0.3125 |

A 7.5x gap in raw rate. The classes measure different things: a value resampled from the column is
usually obviously wrong, while a wrong value the corrector actually proposed is plausible by
construction -- that is why the corrector proposed it. A human ratifier accepts the second far more
often.

**The worst class binds, and that is not conservatism.** `beta` enters as
`p <= p_tilde / (1 - beta)`, and the guarantee must hold for the labelling process as operated. A
labeller who accepts half the corrector's own wrong values has that false-accept rate on those
items regardless of how well they handle easier plants. Averaging assumes a mixture the deployment
does not promise, and the items the deployment actually presents are corrector-generated ones.

Adjusted bound: a measured 0.1153 becomes **0.8953** once the binding `beta` is applied. The
labelling process cannot certify at any useful alpha.

## The union correction makes splitting a class cost power

`delta` is split `delta/2` for the measured bound and `delta/2` divided again across the `K`
classes, so `1 + K` bounds still hold jointly at `1 - delta`.

This is deliberate and has a purpose beyond correctness: **adding a class widens every bound**, so
there is no incentive to subdivide controls until a favourable class appears. A test asserts that
splitting one class of 40 into two of 20 at the same rate produces a *wider* bound, not a narrower
one.

## A bug my own test caught, in the metric I added to expose the problem

My first `heterogeneity_ratio` divided the stratified bound by the pooled one. A test with two
**identical** classes reported a ratio of 1.41 -- heterogeneity on classes that agreed exactly.

The cause: pooling gains sample size while stratifying pays a union correction, so the stratified
bound is wider even under perfect homogeneity. The ratio was measuring "stratified is wider", which
is always true, not "the classes disagree".

Now separated:

- **`heterogeneity_ratio`** compares the classes to each other at the same per-class alpha, so the
  union correction cancels. On the real controls: **3.56**. On identical classes: exactly 1.0.
- **`stratified_vs_pooled_ratio`** is carried separately and named so it cannot be mistaken for a
  heterogeneity measure. On the real controls: 2.79. It is the size of the change to what was
  published, not evidence of disagreement.

A metric that reports disagreement between things that agree would have made the case for
stratification look stronger than it is, in a document arguing for stratification. Worth recording.

## What is not changed

`label_noise_adjusted_bound` keeps its signature and behaviour. It is not wrong -- it computes a
bound from **one** control group, which is correct when you genuinely have one. What is not
defensible is feeding heterogeneous classes into it as a single pooled group. Existing callers
continue to work, and `pooled_beta_upper` on the new result reproduces the old pooled figure
exactly, asserted by a test, so the comparison is against what was actually being reported rather
than a reconstruction.

`pooled_beta_upper` is carried for comparison and **may not enter a decision**. It exists so an
artifact can show the gap rather than leaving it to prose -- the failure mode
`local-certification-result.md` fell into with `beta_scope_note`.

A class with zero controls **raises** rather than being dropped. A dropped class cannot bind, and
its absence would read as evidence of low noise.

## What this means for the roadmap

The kill criterion firing is a real result, and it closes a direction rather than blocking one.

**Closed:** certifying auto-apply at `alpha = 0.05` from human ratification of corrector proposals,
at any realistic labelling budget. `min_samples_under_label_noise` already put the floor at 82 real
items plus 30 controls, i.e. 112 human judgements; with `beta_upper = 0.8712` the adjusted bound
cannot reach 0.05 at any sample size, because the inflation factor is `1/(1 - 0.8712) = 7.76`.

**Not closed:** everything that does not route through human ratification of machine proposals.
The measured `beta` is a property of *that* labelling protocol -- acquiescence-biased ratification,
where a labeller is shown an answer and asked whether it is right. A protocol that elicits the
correct value **before** revealing the machine's proposal is a different process with a different
`beta`, and `human_label_noise.md` pre-registered a blind-elicitation arm (H2) for exactly this.
That arm is now the only live route to certification, and this result raises its priority.

**Also not closed:** the write gate is unaffected. Every corrector class already ships at the
unreachable `1.01` threshold via `corrector_default_policy`, so nothing auto-applies today and
nothing needed to change. This result explains *why* that posture is correct rather than merely
cautious.

## Limits

1. **Eight controls in the binding class.** The bound is 0.8712 because 4/8 with a union-corrected
   Clopper-Pearson interval is wide. More `corrector_generated` controls would tighten it, and the
   raw rate of 0.5000 would have to fall a long way for the criterion to stop firing.
2. **One labeller, one session.** `beta` is a property of a protocol *and* a person.
3. **Plant-scoped, as before.** `corrector_generated` plants are the closest available proxy for
   the corrector's real error distribution, but they are still plants.
4. **Changes no write gate**, and no detector moves onto `CONSTRAINT_CHECKABLE_DETECTORS`.
