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

## The finding: a starved top grid point blocks everything below it

`type_mismatch` has **zero errors in 92 proposals** and still does not certify at
`alpha = 0.05`. Precision is not the binding constraint. The grid walk shows why:

```
threshold 0.99 -> n=40   errors=0   CP_upper=0.0722   > alpha 0.05   -> REJECT
threshold 0.98 -> n=92   errors=0   CP_upper=0.0320  <= alpha 0.05   (support 92 >= 30)
```

`certify_threshold` uses **fixed sequential testing**: candidates are tested purest-first and
the procedure **stops at the first non-rejection**, which is exactly what controls family-wise
error at `delta` without a Bonferroni penalty. The 0.99 point fails only because 40 accepted
samples is below the 59-sample floor (`min_samples_for_certification(0.05, 0.05) = 59`). Having
failed, the sequence cannot continue to 0.98, where 92 all-correct samples would have certified
comfortably.

So the requirement is not merely "59 all-correct accepted samples". It is **59 all-correct
accepted samples at every grid point above the one you want certified**. That is a materially
harder condition and it was not obvious from the floor arithmetic.

This interacts badly with a nearly binary confidence distribution. The corrector's confidences
are `{0.99: 65, 1.00: 27}` for `type_mismatch` and `{0.96: 2, 0.98: 3, 0.99: 126, 1.00: 6}` for
`entity_consensus`. Almost all mass sits at one grid point, so the top thresholds slice a small
starved set off the tail while the bulk sits just below.

**The grid must be pre-specified for the validity claim to hold** (`conformal.py` documents
data-dependent grids as a *validity* weakness, not merely a power one), so it cannot be tuned
after seeing the labels. The legitimate fixes are more samples, or a grid whose top points are
reachable given the confidence distribution -- chosen in advance, for stated reasons.

## What does certify

| alpha | outcome |
| --- | --- |
| 0.05 | **NONE** (starved 0.99 grid point, as above) |
| 0.10 | `type_mismatch` at threshold 0.6 |

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
  would be noisier and possibly biased in ways this cannot detect.
- hospital's corruptions are synthetic character substitutions (`al_axi-1` -> `al_ami-1`), a
  shape unusually well suited to pool-constrained repair.
- `type_mismatch` at 92/92 is a genuine zero-error observation, but zero errors in 92 still
  only bounds the true error rate at 0.032 with 95% confidence -- it is not proof of
  perfection.
- Certifying on the full labelled sample is valid conformal risk control (the guarantee is
  prospective, about future exchangeable cells). The held-out split is a stricter empirical
  check, and at this sample size it cannot both certify and validate. Both are reported so the
  difference is visible rather than hidden behind whichever number reads better.
