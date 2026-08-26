# Pre-registration: does a reviewer decide better with `tested_confidence`?

Written 2026-08-26, **before** any reviewer is shown anything. Nothing below is edited afterwards;
results and deviations are appended as amendments.

## The gap, and why it implicates my own work

This project has measured the machine repeatedly and the human almost never. Two prior programmes
touched humans, and neither asked this question:

- `eval/preregistration/human_label_noise.md` measured **how noisy a labeller is** (`beta`). Its kill
  criterion fired.
- `eval/preregistration/blind_elicitation.md` measured **whether eliciting before ratifying reduces
  that noise**. It returned VOID on a capability floor.

Both measure label *quality*. Neither measures whether **a field in the interface changes a
decision**. That distinction is the whole gap.

On 2026-08-25 a signal was found that separates true from false mined dependencies perfectly on
hospital, and correctly **declined as a gate** because its threshold is fitted to one corpus with
nothing to validate it against. The resolution was to report it to the human instead:

> the score informs a decision, it does not make one.

On 2026-08-26 I completed that delivery — added `tested_confidence` to the machine-readable review
summary and gave it its own column in the review table — and **did not measure whether it helps.**
That is the named-consumer rule one level deeper than the level at which this project already
applies it: I named the consumer, delivered the artifact to them, and never checked that it changes
what they do. A field shipped on the argument that it informs a human is a claim about a human, and
it is currently unmeasured.

## The instrument is already available

The shipped-premise measurement produced four mined dependencies that the published measurement had
excluded, on hospital, and **all four are false on ground truth**:

```
City    -> HospitalOwner    conf=0.905  tested=0.9017
ZipCode -> ProviderNumber   conf=0.949  tested=0.9477
ZipCode -> Address1         conf=0.946  tested=0.9446
ZipCode -> PhoneNumber      conf=0.944  tested=0.9426
```

Every one has `tested_confidence` below the 0.9554 boundary that separated true from false, and
`confidence` in the same 0.90-0.95 band as several **true** dependencies. So the two fields disagree
on exactly these items, ground truth is known, and the population is the real one a user reviews.
This is not a constructed vignette.

## Hypothesis and design

**H1.** A reviewer shown `tested_confidence` alongside `confidence` accepts fewer **false**
dependencies than a reviewer shown `confidence` alone, without accepting fewer true ones.

Two arms, identical candidate list, identical order, one field hidden:

| arm | fields shown |
| --- | --- |
| `confidence_only` | candidate, `confidence`, evidence prose **with the tested-confidence sentence removed** |
| `both` | the same plus the `Tested` column and the full evidence prose |

The evidence-prose edit is essential and easy to forget: `tested_confidence` is currently embedded in
the English `evidence` string as well as in the column, so an arm that hides only the column is not
blind. If the prose cannot be cleanly separated, **the measurement is void** rather than approximate.

**Population**: all 85 hospital FD candidates. **Outcome**: accept/reject accuracy against
`fd_holds_on_clean`, reported as the count of false dependencies accepted and true dependencies
rejected — two numbers, never a single score, because the two errors have different costs.

**Assignment**: between-subjects, randomised, seed recorded before recruitment. Not
within-subjects: a reviewer who sees a candidate twice has learned it.

## Predictions

| # | Prediction |
| --- | --- |
| **P1** | `both` accepts fewer false dependencies than `confidence_only`. |
| **P2** | `both` does **not** reject more true dependencies. If it does, the field is trading one error for the other and the net effect must be reported as such rather than as an improvement. |
| **P3** | **Stated against myself.** The effect may be **absent**, because the two fields differ by less than 0.01 on most candidates (`conf=0.949` vs `tested=0.9477`) and a human is unlikely to act on a third-decimal difference they were given no threshold for. If P3 holds, the column I added is decoration. |
| **P4** | Reviewers will ask for a threshold. The refusal to supply one is deliberate and must be recorded as part of the protocol, not treated as a design flaw discovered during the run. |

## Kill criterion, fixed now

- **K1.** If `both` does not reduce false acceptances, **`tested_confidence` is decoration and the
  column added on 2026-08-26 is removed.** Not softened, not kept "for transparency" — removed, and
  the removal recorded in `DECISIONS.md`. A field that does not change a decision is cost without
  benefit: it widens the review surface and implies a precision the number cannot deliver.
- **K2.** If the `evidence` prose cannot be made blind, the run is **VOID**.
- **K3.** No post-hoc arm, no post-hoc outcome, and no reporting of a pooled accuracy score in place
  of the two error counts.

## The power problem, stated before recruiting rather than discovered during

This is the part most likely to end the programme, and it should end it *here* if it is going to.

There are **16 false dependencies** among hospital's 85 candidates (4 in the excluded band, 12 in the
proxy set). Detecting a meaningful reduction in false acceptances against a base rate on 16 items
requires either many reviewers or a large per-reviewer effect.

**Before recruiting anyone**, compute the required sample size for a plausible effect using the same
machinery already committed for the label-noise bound (`dataforge/conformal.py`
`_min_samples_given_beta` and `min_samples_under_label_noise`). If the required reviewer count is out
of reach at the available budget, **publish that and stop.**

That outcome is not a failure. It is precisely what `human_label_noise` found — 572 labels needed
against a ~200 budget — and rediscovering the same wall a second time without saying so in advance
would be the dishonest version. The difference between the two programmes is that this one states
the wall as a pre-registered stopping condition rather than as a result.

## What is deliberately not being decided here

**`tested_confidence` is not promoted to a gate under any outcome of this measurement.** Even if H1
holds strongly, that shows a human uses the number well — it says nothing about whether a *threshold*
on it generalises beyond hospital, which is the reason the gate was declined and which no reviewer
study can address.

**No new field is added to the review surface.** The temptation on a null result is to try a
different presentation rather than accept the finding. Any such change is a separate
pre-registration.
