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

## The live certification path now stratifies, not just the arithmetic helper

A result that fires a kill criterion but leaves the killed behaviour shipping is worse than not
having measured it. `certify_session` was pooling every planted control into one `beta`, so the
product would have kept issuing the anti-conservative bound while the trust doc said otherwise.

Changed:

- **`CalibrationSessionArtifact.controls_by_origin()`** groups labelled controls as
  `{origin: (false_accepts, controls)}`. `PlantedControl.origin` already carried the distinction --
  the data model had the information and the certification path was discarding it.
- **`certify_threshold_under_label_noise` takes `controls_by_class`**, and the pooled
  `false_accepts`/`controls` parameters are **gone rather than deprecated**. A pooled bound is not
  a weaker version of the honest one, it is a different and anti-conservative quantity, so it is
  unreachable. Three test call sites had to be migrated, which is the intended blast radius.
- **`_min_samples_given_beta`** factors the sample-floor arithmetic so the pooled entry point and
  the stratified certifier share one implementation rather than two that can drift.
- `observed_false_accepts()` remains, documented as pooled and unsuitable for certification. It is
  the reporting and comparison figure only.

**Blast radius is exactly the pooled multi-origin case.** With a single origin the union
correction divides `delta/2` by 1, so the stratified bound is bit-identical to the pooled one. A
test asserts this to 1e-12, so the migration cannot have quietly moved a number it was not meant to
move.

Four new tests pin the behaviour that ships, including
`test_a_dirty_origin_cannot_be_hidden_by_a_clean_one`: 0 false accepts on 200 easy plants plus 4 on
8 hard ones must still certify **nothing**. That is the failure mode pooling permits -- pad the
control set with easy plants and the false-accept rate on the hard class disappears into the
denominator.

Mutant **M11-label-noise-beta-repooled** re-pools the origins in `certify_session`. It is killed, so
a revert to pooling cannot pass CI as a tidy-up. 11/11 mutants killed.

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

## The certificate carries its own provenance

Fixing the estimator was not enough, because the artifact it produced still recorded the bound as a
bare scalar beside *pooled* control totals. A reader of `SessionCertification` saw
`label_noise_controls = 38`, `label_noise_false_accepts = 6` and `beta_upper = 0.8712` — three true
numbers arranged so that the first two look like the cause of the third. They are not: 6/38 implies
0.3125, and 0.8712 came from 4/8 in one class. The CLI printed exactly that sentence.

`SessionCertification` now carries `label_noise_controls_by_class` (a per-class tally of controls,
false accepts and that class's own bound), `binding_control_class`, and `pooled_beta_upper` marked
non-decisional. The pooled scalars survive only as a **checkable summary**: a validator refuses any
certificate whose totals do not equal the sum of its tallies.

The property this buys is that the bound is recomputable from the certificate alone:

```python
recomputed = label_noise_adjusted_bound_stratified(
    0, 1,
    controls_by_class={n: (t.false_accepts, t.controls)
                       for n, t in cert.label_noise_controls_by_class.items()},
    delta=cert.delta,
)
assert recomputed.beta_upper == cert.beta_upper
```

That reads nothing from the session that produced the certificate — no samples, no controls, only
the certificate's own fields. It is asserted in
`tests/unit/test_label_noise_certification.py::TestCertificateIsSelfChecking`, and it is the same
standard `label_source` already enforced: a certificate that does not say cannot be checked.

Four further validators refuse a read-back that has lost the property — no tallies behind a stated
`beta_upper`, a binding class not among the recorded classes, a binding class that is not the worst
one, and a headline `beta_upper` disagreeing with its own stated source. None of these can be
reached by anything this module constructs. They exist for artifacts on disk: an older file that
pooled its controls, or a hand-edited one, is indistinguishable from a sound certificate by shape
alone, and would licence auto-apply against an error budget nothing measured. Mutant `M13` removes
the first of them and dies.

`beta_scope_note` is **kept**, against the original plan to retire it in favour of the structured
field. The structured tallies make the arithmetic auditable; they cannot say whether the plants were
representative of the corrector's real errors, which is the limitation most likely to be forgotten
and the one no number here carries. Retiring a correct warning to satisfy a checkbox would have
traded a real caveat for a tidier schema.

## What this means for the roadmap

The kill criterion firing is a real result, and it closes a direction rather than blocking one.

**Closed:** certifying auto-apply at `alpha = 0.05` from human ratification of corrector proposals,
at any realistic labelling budget. `min_samples_under_label_noise` already put the floor at 82 real
items plus 30 controls, i.e. 112 human judgements; with `beta_upper = 0.8712` that floor becomes
**572** error-free labels.

### Correction, 2026-08-25: the inflation factor does not create an asymptote

This document previously read *"the adjusted bound cannot reach 0.05 at any sample size, because the
inflation factor is 1/(1 - 0.8712) = 7.76"*. **That is wrong, and it was wrong when written.** An
inflation factor multiplies the *sample cost*; it never puts a target out of reach. The measured
bound `1 - (delta/2)^(1/n)` tends to zero as `n` grows, so `measured / (1 - beta)` can be driven
below any positive alpha. What 7.76 buys is roughly 7.76x the labels: the alpha=0.05 floor is **72**
at a negligible beta and **572** at the binding one, a ratio of 7.94 -- the factor up to integer
ceiling effects. (The 82 quoted below is the floor at the *easy-class* beta of 0.1157, not at zero;
the three figures are 72, 82 and 572 for beta of 0, 0.1157 and 0.8712.)

Computed with `_min_samples_given_beta` at `beta_upper = 0.8712`, `delta = 0.05`, zero observed
errors:

| alpha | error-free repair labels required |
| --- | --- |
| 0.01 | 2863 |
| 0.05 | **572** |
| 0.10 | 285 |
| 0.20 | **142** |
| 0.30 | 94 |
| 0.50 | 56 |

**The verdict is unchanged and the reason is now correct.** The kill criterion was pre-registered as
`beta_upper > 0.35`, defined in `human_label_noise.md` as *"alpha=0.05 unreachable inside ~200
judgements"*. 572 error-free labels plus controls exceeds 200, so the criterion fires exactly as
recorded. The route is closed by **budget**, not by arithmetic impossibility -- a weaker and more
honest statement than the one this document made, and one that survives someone checking it.

### The alpha=0.20 fallback, finally computed

`DECISIONS.md` has named "advisory triage at alpha=0.20" as the honest fallback since the kill
criterion was written, and never evaluated it at the measured beta. It needs **142** error-free
repair labels plus the controls -- against 49 under the small beta originally assumed, and inside the
~200 budget. So the fallback is **arithmetically live**, which is more than could previously be
claimed for it.

Two caveats keep that from being good news. The 142 labels must contain **zero** observed errors, and
they come from the same labeller whose measured false-accept rate is bounded at 0.8712; a labeller
who accepted 4 of 8 planted wrong proposals is not a plausible source of 142 consecutive flawless
verdicts. And the controls must include `corrector_generated` plants, which `plant_controls` cannot
produce. So alpha=0.20 is reachable on paper and still blocked in practice, for a reason that has
nothing to do with sample size.

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
