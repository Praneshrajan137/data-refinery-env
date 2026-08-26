# Pre-registration: what do the two withheld repairers actually do when they write?

Written 2026-08-26, **before** running the measurement. Nothing below is edited afterwards; results
and deviations are appended as amendments.

## The gap

`dataforge/repairers/__init__.py`:62-66 states why two deterministic repairers are unreachable:

> ``format_violation`` and ``categorical_normalization`` deterministic repairers remain withheld
> from auto-apply (**they regressed benchmark precision**); the corrector replaces them as the
> calibrated, gated correction path for those classes.

There is **no committed artifact and no pre-registration** behind that parenthesis. I searched
`eval/results/` and `eval/preregistration/` for either repairer and found nothing. So the
withholding rests on a docstring.

By this project's own standard that is a defect **whichever way the number falls**. Either we are
withholding earned coverage on folklore, or we are shipping an unmeasured claim that happens to be
true. `PRODUCT.md` §1.4 already names the general form: a detector whose failure population is
absent from your corpora "has not been shown to be safe; it has been shown to be unreachable by your
evidence."

Two independent mechanisms hold these repairers today, and it is worth separating them because only
one is the allowlist:

1. **They are absent from the deterministic repairer registry entirely.** `build_repairers`
   constructs exactly four entries; with `allow_llm=True` these two keys are bound to the **LLM
   corrector**, not to the deterministic repairers. So no proposal is ever produced.
2. **`CONSTRAINT_CHECKABLE_DETECTORS` would hold them anyway** even if registered.

The brief for this work described them as "withheld from auto-apply." The reality is stronger: they
are not reachable at all, so the phrase "regressed benchmark precision" refers to a state of the code
that no longer exists in a form anyone can re-run.

## Why I expect this measurement to FAIL, stated before running it

Both repairers infer their reference from **the column's own distribution** and consult no premise.
`vocabulary.py`:176-179 defines allowlist membership as "checkable against a reference [...] rather
than inferred from the shape of the column's own distribution", and names the latter as requiring a
calibrated threshold. That is `decimal_shift`'s profile, and `decimal_shift` was removed on
measurement.

`format_violation` accepts a `schema` parameter and never reads it. `categorical_normalization`
executes `del retry_context, schema` on line 29 — it discards the premise explicitly.

**So the pre-registered expectation is that at least one of the two fails a kill criterion.** I am
running it because the artifact is the deliverable either way: a failure converts folklore into
evidence and closes the question, and a pass is a measured result the project has never had.

## Method, fixed now

Extend `scripts/bench/measure_bypass_allowlist.py`. It is already generic by construction —
`classify_writes` states that "the classification depends only on ground truth and the proposed
value" — so the extension is a `_build` entry per repairer, a `no_premise` arm, and a `--detector`
flag. `classify_writes` itself is not modified.

For each of `format_violation` and `categorical_normalization`, on **all four** corpora (hospital,
flights, rayyan, tax):

* run the **real detector** to produce the queue, then the **real repairer** on each flag — no
  reimplementation of either;
* classify every proposal **unconditionally** against retained ground truth into
  `repaired_a_real_error` / `wrong_value_on_a_real_error` / `corrupted_a_clean_cell` /
  `no_op_on_a_clean_cell`;
* account per **distinct cell**, never per detector flag;
* report `write_precision` = repaired / proposals, `harmful_write_rate` = (wrong + corrupted) /
  proposals, and `net_cells_improved` = repaired − (wrong + corrupted).

**Premise arm: `no_premise` only, for both.** That is not a simplification, it is the shipped
configuration: neither repairer reads a schema, so an oracle arm would be identical by construction
and reporting two arms would imply a premise sensitivity that does not exist.

**`tax` is measured unsampled** (200,000 rows). `decimal_shift`'s 263,428 false rewrites surfaced on
`tax` and on no other corpus, and `docs/trust/sampling-bias-measured.md` records that a head slice of
`tax` is a biased view of a different population rather than a weaker view of this one. A
head-sliced `tax` result would be the single most likely way for this measurement to miss the thing
it exists to catch.

**Regression guard on the refactor.** The committed `fd_violation` and `missing_value` figures must
reproduce **exactly**: 393/393/0/0 on hospital-oracle, 537/451/0/86 on hospital-mined, and
`missing_value` at 427 writes / 427 repaired / 0 harmful. Any drift means the harness moved, not the
product, and the run is void until reconciled.

## Predictions

| # | Quantity | Prediction and the structural reason |
| --- | --- | --- |
| **P1** | `categorical_normalization`, hospital, `corrupted_a_clean_cell` | **> 0, and the majority of its writes.** The registry records that 509 of 509 corrupted hospital cells contain an injected `'x'`. An `'x'`-injected value has a different normalization key from its clean form, so it forms its own cluster and is skipped for want of a second variant. What remains for this detector to flag is genuine case and spacing variation that is **present in the clean data** — so writing the majority form overwrites a correct cell. |
| **P2** | `format_violation`, `corrupted_a_clean_cell` | **> 0 on at least one corpus**, via the leading-zero branch. `_canonicalize` step 3 pads any shorter all-digit value to the dominant width. Where a shorter number is legitimate — an unpadded identifier, a measurement — this fabricates leading zeros that were never in the data and are not in the truth. |
| **P3** | `format_violation`, date branch | **Contributes few writes.** `_reformat_date` requires every successful parse across eight formats to agree on one target, and the ambiguous `DD/MM` vs `MM/DD` case therefore abstains. This branch should be the safe one. |
| **P4** | Both repairers, `rayyan` | Fire on **more** cells than on hospital. `rayyan` is the only corpus whose errors are natural rather than injected, so its format and casing inconsistencies are real rather than synthetic. |
| **P5** | At least one of the two | `write_precision` **below** `fd_violation`'s worst committed arm (0.5618 on flights). Neither consults a premise, and 0.5618 was measured *with* one. |

**Uncertainty stated plainly.** P1 is a structural argument about hospital's injection scheme and I
am confident in it. P2 depends on whether any corpus has an all-digit column with legitimate
width variation, which I have **not** computed — and this project has twice refuted a distribution
inferred from a column's name, so I am not predicting from column names here. P4 is the weakest
prediction on this list; `rayyan` has 11 columns and I do not know their shapes. If P4 fails it means
nothing about the repairers.

## Kill criteria, fixed now

Applied per repairer, after the measurement and before any code change:

**A repairer STAYS OUT of the deterministic registry and out of the allowlist if either:**

* **(K1)** `write_precision` is **0.0000** on every corpus where it proposes at all — the
  `decimal_shift` criterion, applied unchanged; or
* **(K2)** `corrupted_a_clean_cell + wrong_value_on_a_real_error` **exceeds**
  `repaired_a_real_error`, summed across corpora — it damages more cells than it fixes.

* **(K3) No tunable constant.** If admitting either repairer requires introducing or retuning a
  threshold — a precision floor, a dominance cut, a minimum cluster size — **abandon the change
  rather than fit it.** A parameter chosen after seeing the data is not a finding.

* **(K4) Zero writes is recorded as `unmeasured`, never as safe.** A repairer that proposes on zero
  cells across all four corpora is reported as unreachable by this evidence, and the words "safe",
  "harmless" and "clean" may not be used of it. `decimal_shift` was benchmark-quiet on three corpora
  at precision 0.0000 and a fourth corpus found 263,428 false rewrites.

**Non-vacuity.** The measurement must produce at least one proposal on at least **two** corpora. If
it does not, the result is published as **VOID** with the reason, and no conclusion about either
repairer's safety may be drawn from it in either direction.

## What is deliberately NOT being decided here

**Neither repairer enters `CONSTRAINT_CHECKABLE_DETECTORS` in this session, even if no kill
criterion fires.** This is pre-committed so that a good-looking number cannot become a write
permission by momentum.

Passing a harm measurement on four public academic corpora makes a repairer **eligible**, not
authorised. The `decimal_shift` precedent is exactly this: it was quiet on three corpora and would
have rewritten 263,428 values on the fourth. Four corpora is one more than three. Granting write
authority is a separate, reviewable decision that needs a named owner, and it should not be taken in
the same commit as the measurement that makes it arguable.

**No threshold is invented.** K1 and K2 are the criteria already applied to `decimal_shift` plus a
sign test.

**The two mechanisms are not conflated.** If a repairer passes, the recommendation available is to
add it to the *deterministic registry* — making it propose for review — which is a different and much
weaker act than adding it to the *allowlist*, which lets it write unsupervised without a threshold.
Measuring the second does not authorise the first by default, and this document does not recommend
either.
