# Local certification: a perfect corrector that still cannot certify at alpha = 0.05

**Date:** 2026-08-20
**Model:** Azure OpenAI `gpt-5.6-sol` (`2026-07-09`), structured-enum corrector, k=3
**Table:** hospital (1000 rows), default detection, 549 flagged cells, 441 sampled
**Spend:** $5.42 measured (`calibrate-propose-*` receipts)

## RETRACTION of an earlier result in this same session

On a 115-proposal run I reported that `entity_consensus` **certified at alpha = 0.05,
threshold 0.7**. That was a small-sample artifact and is **withdrawn**. At n=72 it looked like
71/72; at n=137 the same class measures **123/137 = 0.8978** and certifies nothing. The
number moved because the sample was too small, which is the ordinary reason such numbers move.
It is recorded here rather than quietly dropped.

## What the corrector actually achieves

229 proposals from 441 sampled cells, judged against ground truth:

| issue_type | correct | total | precision |
| --- | --- | --- | --- |
| `type_mismatch` | 92 | 92 | **1.0000** |
| `entity_consensus` | 123 | 137 | 0.8978 |
| **overall** | **215** | **229** | **0.9389** |

Against the benchmark's 0.059-0.297 for the same model. The gap is queue composition, not
capability -- see [corrector-queue-contamination.md](corrector-queue-contamination.md).

## DEFECT FOUND 2026-08-21, then RESOLVED by regenerating the session

Two problems were found while trying to re-derive the grid walk below. The first stands. The
second was diagnosed wrongly at first, and the corrected diagnosis is more interesting than the
complaint.

**1. The raw `(confidence, repair_decision)` pairs were NOT in the repository.** The session lived
at `.dataforge/calibration/session.json`, untracked and absent. Of 1,883 tracked files, none held
these labels, so every number in this document was a **report without retained evidence**. That is
the exact failure the arm-sweep artifact was built to avoid -- `DECISIONS.md` says persisting the
raw `(score, label)` pairs is "precisely why" those results survived a lost model. This session got
the opposite treatment.

**FIXED.** The session was regenerated on the same table and is now committed at
[`eval/results/hospital_calibration_session.json`](../../eval/results/hospital_calibration_session.json),
with the derived measurements in
[`eval/results/label_noise_instrument.json`](../../eval/results/label_noise_instrument.json).
Everything below is re-derivable from those bytes.

**2. The grid walk appeared to contradict the confidence histogram. It did not -- and my first
correction of it was wrong.** The walk reported `n=40` at threshold 0.99 while the histogram gave
`{0.99: 65, 1.00: 27}`, summing to 92. I recorded that as an internal inconsistency and said "40
matches nothing". **That diagnosis was incorrect.** The real cause is a floating-point hair, and
the regenerated session reproduces it exactly:

```
RAW confidences for type_mismatch: {1.0: 17, 0.9899999999999999: 56, 0.9933...: 9, 0.9966...: 10}
n(conf >= 0.99) = 36   of 92        <-- 56 samples excluded
n(conf >= 0.98) = 92
```

The corrector's nominal "0.99" is stored as **0.9899999999999999**, which is *strictly less than*
the grid's literal `0.99`. So 56 of 92 samples fall out of the accepted set at a threshold they
look equal to, leaving n=36. The old document displayed rounded confidences while the code compared
unrounded ones, which made a real and subtle effect look like an arithmetic error.

**This is a third, independent defect worth naming on its own:** the grid's semantics are fragile
against how confidence is computed. A threshold that is numerically indistinguishable from the
modal confidence can silently discard most of the sample. Candidate pruning cures it robustly --
36 is below the 59-sample Clopper-Pearson floor, so `0.99` is removed and the walk starts at
`0.98` -- which is a better fix than hand-tuning grid literals to dodge float representation.

---

## The finding: a PERFECT record at the top grid point blocks everything below it

`type_mismatch` has **zero errors in 92 proposals** and still does not certify at
`alpha = 0.05`. Precision is not the binding constraint. The grid walk shows why:

```
threshold 0.99 -> n=40   errors=0   CP_upper=0.0722   > alpha 0.05   -> REJECT
threshold 0.98 -> n=92   errors=0   CP_upper=0.0320  <= alpha 0.05   (support 92 >= 30)
```

> The two lines above are the **original 2026-08-20 walk, whose session was lost.** The
> regenerated and committed session reproduces the same pathology with its own numbers, and these
> are the ones to cite:
>
> ```
> type_mismatch, n=92, 92/92 correct, alpha=0.05, delta=0.05
>   t=0.99  n= 36  errors=0  CP=0.0798  FAIL -> break
>   t=0.98  n= 92  errors=0  CP=0.0320  PASS   (never reached under the unpruned walk)
>   ... 0.97 down to 0.60 all n=92, CP=0.0320, PASS
> ```
>
> Outcome at alpha = 0.05: **unpruned certifies NOTHING; pruned certifies threshold 0.60.** The
> fix is load-bearing here. At alpha = 0.10 and 0.20 both procedures certify 0.60, so pruning
> changes nothing -- the pathology needs a floor high enough to strand the top grid point.
> Re-derivable from `eval/results/hospital_calibration_session.json`.

`certify_threshold` uses **fixed sequential testing**: candidates are tested purest-first and
the procedure **stops at the first non-rejection**, which is exactly what controls family-wise
error at `delta` without a Bonferroni penalty. Having stopped, the sequence cannot continue to a
lower threshold, however well supported that threshold is.

The mechanism does not depend on the unverified figures above. With zero observed errors the
Clopper-Pearson upper bound is `1 - delta**(1/n)`, so
`min_samples_for_certification(0.05, 0.05) = 59` accepted-and-correct samples is a hard floor. A
top grid point that slices only a few dozen samples off the tail therefore **cannot clear the bound
however perfect its record**, and the walk halts there.

### CORRECTION (2026-08-21): "starved" was the wrong word, and it hid the fix

An earlier version of this section called the 0.99 point **starved** and said it "fails only
because 40 accepted samples is below the 59-sample floor". That description is wrong in a way
that matters, because it points at sample size as the cause when the cause is **ordering**.

Read the two branches of `conformal.py:181-191`. A genuinely starved point -- `n < min_support`
-- hits `continue` and does **not** halt the sequence. Only a point that is *tested and fails*
hits `break`. The 0.99 point had **n = 40 >= min_support = 30**, so it was tested, and it failed
on the Clopper-Pearson bound (`0.0722 > 0.05`) despite a **flawless 40/40 record**.

So the accurate statement is stronger and stranger than the original: **a perfect record at a
high threshold destroys certification of every lower threshold.** The procedure penalises purity.
`min_samples_for_certification(0.05, 0.05) = 59` explains why 40 cannot clear the bound -- 0
errors in 40 bounds the error rate only at 0.0722 -- but the *halt* is a consequence of the
descending order, not of the floor.

This interacts badly with a nearly binary confidence distribution. The corrector's confidences
are `{0.99: 65, 1.00: 27}` for `type_mismatch` and `{0.96: 2, 0.98: 3, 0.99: 126, 1.00: 6}` for
`entity_consensus`. Almost all mass sits at one grid point, so the top thresholds slice a small
set off the tail while the bulk sits just below.

### The fix the original text missed

The original named two legitimate fixes -- more samples, or a better grid chosen in advance --
and concluded the grid "cannot be tuned after seeing the labels". The second half of that is
true and the inference from it is too strong.

Fixed sequential testing requires the order to be **pre-specified**. It does not require the
order to be **descending**. And the choice of which threshold to test may legitimately depend on
the **confidences**, because a confidence is a feature, not a label:

* conditional on the calibration confidences, the accepted-set labels are independent Bernoulli;
* the accepted set `{i : conf_i >= t}` is a function of the confidences alone, so it is
  measurable with respect to them;
* a binomial Clopper-Pearson bound applied to the mean of independent non-identical Bernoullis
  is **conservative**, because a Poisson-binomial is less dispersed than the binomial with the
  same mean (Hoeffding 1956).

This is the same latitude Mondrian conformal prediction already grants when it lets the taxonomy
depend on the covariates. What breaks validity is selecting a threshold using the **labels** --
for example, picking whichever threshold minimises observed errors. That distinction is now
enforced rather than argued: see `select_threshold_by_confidence_mass` in `dataforge/conformal.py`
and the label-permutation invariance test in
`tests/unit/test_threshold_selection_label_independence.py`.

**Raising certified coverage is the convenient direction, so it does not ship alone.** The same
change adds the human-label-noise denominator described in
[human-label-noise.md](human-label-noise.md); the net effect on the advertised guarantee is
stricter, not looser.

## Regenerated, committed run (2026-08-21) — cite these

Same table, `--per-class 92`, structured-enum corrector, `gpt-5.6-sol`. 376 cells sampled from 549
flagged, 176 proposals, labels from RAHA ground truth so `label_source = oracle`. Raw pairs in
[`eval/results/hospital_calibration_session.json`](../../eval/results/hospital_calibration_session.json).
Spend $4.3441 (plus $1.9512 for a superseded first draw; see the ledger).

| issue_type | correct | total | precision | alpha=0.05 unpruned | alpha=0.05 pruned |
| --- | --- | --- | --- | --- | --- |
| `type_mismatch` | 92 | 92 | **1.0000** | none | **0.60** |
| `entity_consensus` | 76 | 84 | 0.9048 | none | none |

`entity_consensus` certifies only at alpha = 0.20. Note it moved again — 0.8978 at n=137
previously, 0.9048 at n=84 here — which is the ordinary behaviour of a proportion at this sample
size and the reason the earlier alpha=0.05 claim for it was retracted.

### Model acquiescence, and why plant difficulty is not a detail

A cheap probe asked `gpt-5.6-sol` to ratify values known to be wrong. **This measures a MODEL. It
is not `beta`, it may not enter a certificate, and it is reported here only because it settles a
design question.**

| item class | ratified known-wrong | n | rate | upper bound |
| --- | --- | --- | --- | --- |
| `column_distribution` (value resampled from the column) | 2 | 30 | 0.0667 | 0.2207 |
| `corrector_generated` (real wrong proposals the corrector made) | 4 | 8 | **0.5000** | 0.8430 |

**A 7.5x gap.** Real corrector errors are ratified far more readily than plants built by
resampling the column. At n=8 the magnitude is not established -- the upper bound is 0.843 -- but
the direction is evidence for something that had only been argued: **the two control classes are
not interchangeable, and pooling them is not defensible.** A `beta` estimated on
`column_distribution` plants alone would understate the false-accept rate, leaving the
"noise-corrected" bound anti-conservative while carrying the authority of having been checked.
That is precisely the category error [human-label-noise.md](human-label-noise.md) is designed
around, and it now has a number attached rather than a warning.


## Independent validation

The project's standard is to certify on a calibration half and measure on a disjoint test half.
Both are reported, and they disagree with the full-sample result for a reason worth stating.

`certified_coverage_report(alpha=0.05, seed=20260806)`: every class gets the `1.01` abstain
sentinel, `auto_applied = 0` of 115 test cells. Splitting halves the calibration set, which
worsens the same starvation.

`repeated_split_certification(alpha=0.05, splits=200)`:

```
certified_rate      0.005      (1 split in 200)
over_alpha_rate     0.005      <= delta 0.05   -> VALIDITY HOLDS
mean_test_coverage  0.5478     (over splits that applied something)
mean_test_error     0.1429
```

The validity criterion passes: across 200 random splits the fraction whose measured test error
exceeded `alpha` is 0.005, well inside `delta = 0.05`. The procedure is behaving correctly. It
is simply almost never able to certify at this sample size, which is the honest conclusion.

## Scope of any certificate produced here

Bound to the table (`source_sha256` + shape fingerprint) and to the model. Verified: a
certificate earned on `azure/gpt-5.6-sol` is **refused** when `azure/gpt-5-mini` is running,
because corrector accuracy is model-specific and does not track capability.

## Honest limits

- The "user labels" are RAHA ground truth standing in for a human's judgement. A real user
  would be noisier and possibly biased in ways this cannot detect. **This limit is larger than
  the sentence suggests and is quantified in
  [human-label-noise.md](human-label-noise.md):** if a human ratifies a wrong repair with
  probability `beta`, the true error rate is `p_tilde / (1 - beta)`, so certifying a measured
  0.05 delivers 0.0625 at `beta = 0.2` and **0.10 at `beta = 0.5`**. Automation bias pushes the
  noise in exactly that direction, because the labeller is shown the machine's answer and asked
  to ratify it. Until `beta` is bounded by measurement, a certificate earned on human labels
  states an error budget it cannot support.
- hospital's corruptions are synthetic character substitutions (`al_axi-1` -> `al_ami-1`), a
  shape unusually well suited to pool-constrained repair.
- `type_mismatch` at 92/92 is a genuine zero-error observation, but zero errors in 92 still
  only bounds the true error rate at 0.032 with 95% confidence -- it is not proof of
  perfection.
- Certifying on the full labelled sample is valid conformal risk control (the guarantee is
  prospective, about future exchangeable cells). The held-out split is a stricter empirical
  check, and at this sample size it cannot both certify and validate. Both are reported so the
  difference is visible rather than hidden behind whichever number reads better.
