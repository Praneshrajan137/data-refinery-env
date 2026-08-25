# Premise quality: a better signal, and why it is not shipped as a gate

Measured 2026-08-25. Artifacts: `eval/results/premise_quality_{hospital,flights,rayyan,tax}.json`.
Reproduce: `python scripts/bench/measure_premise_quality.py --corpus <name> --artifact <path>`.
Pre-registered in `eval/preregistration/premise_quality.md`. No API cost; deterministic.

## Verdict first

**Both pre-registered corrections fail their kill criteria. Neither is shipped as a gate.** The
pre-registration required precision to improve on hospital (K1) and corruption to fall below 86
(K2), and forbade introducing a tunable constant to achieve either (K3). C1 lowers precision.
C2 admits an identical candidate set at the existing floor. K1 and K2 both fire.

What the measurement found instead is more useful than the change would have been, and it
resolves a question this project had answered by reasoning alone.

## The incumbent miner, measured for the first time

| corpus | rows | candidates emitted | hold on clean | FD-set precision |
| --- | --- | --- | --- | --- |
| hospital | 1000 | 119 | 103 | 0.8655 |
| flights | 2376 | **0** | 0 | n/a |
| rayyan | 1000 | **0** | 0 | n/a |
| tax | 200000 | **4** | **4** | **1.0000** |

`replication_mismatches` is 0 on all four: each candidate's shipped confidence was recomputed
from the frame and matched the value the miner emitted, so the counterfactuals below are
computed the way the miner computes its own score.

**The architectural defense is vindicated, and this is the first measurement of it.**
`docs/trust/constraint-circularity.md` records that on `tax`, accepting inferred constraints
produced **696-708 false-positive corrections with zero correct ones**. That was measured before
`_MAX_DETERMINANT_UNIQUE_FRACTION` and `_MIN_FD_SUPPORT_GROUPS` existed. Today the miner emits
**four** candidates on the same 200,000-row corpus and **all four are true**. The guards the doc
argued for on theory turn out to work on the corpus that motivated them.

**The mined-FD risk surface is narrower than assumed.** The miner finds nothing at all on
flights and rayyan. Every false dependency this project has ever measured comes from hospital.

## Both corrections, and why each failed

### C1, the majority baseline: refuted, and my reasoning behind it was wrong

Require `confidence > majority_share(dependent)` -- a dependency must beat the predictor that
ignores the determinant and emits the dependent's modal value.

| candidate set | incumbent precision | C1 precision |
| --- | --- | --- |
| all 119 | 0.8655 | **0.8493** |
| 85 non-vacuous | 0.8118 | 0.8493 |

On the pre-registered accounting C1 makes precision **worse**: it rejects 46 candidates, of
which **41 are true and 5 are false**.

The reason is a defect in the truth label, not in C1. 34 of the 119 candidates have `Address2`
or `Address3` as the dependent, and those columns are **literally constant** -- one distinct
value, RAHA's `'empty'` token, share 1.0000. A constant is determined by everything, so all 34
hold on clean and `fd_holds_on_clean` scores them as successes. C1 correctly identifies them as
uninformative and is punished for it. Excluding them, C1 does improve precision (0.8118 to
0.8493). **That re-scoring is post-hoc and is not used to claim C1 works** -- the pre-registered
comparison is the one that counts, and it fires K1.

**My stated prediction P1 was also simply wrong.** I predicted `State` and `Stateavg` were
near-constant and would be caught. Measured: `State` has 4 distinct values with a 0.9540 modal
share -- skewed but not constant -- and `Stateavg` has **74 distinct values with a 0.0400 modal
share**, which is not skewed at all. I inferred skew from column names instead of measuring it.
That is why C1 caught only 5 of the 16 false dependencies.

### C2, the tested denominator: inert at the existing floor, and much better than that

Measure `1 - violations / rows_in_multi_row_groups` rather than dividing by all rows, because a
singleton determinant group is consistent with any dependent value and cannot falsify anything.

At the retained 0.9 floor, C2 admits **all 119** candidates -- identical to the incumbent.
Prediction P2 was refuted for a measurable reason: `tested_row_fraction` has a **median of
0.9740 and a minimum of 0.8270**, so almost every row already lives in a multi-row group. The
existing `_MAX_DETERMINANT_UNIQUE_FRACTION = 0.9` guard ensures determinant repetition, which
leaves the denominator correction little to correct. I had assumed `ZipCode` groups were mostly
singletons; they are not.

**But the correction is not worthless -- it is a strictly better discriminator.** On hospital's
85 non-vacuous candidates:

| signal | FALSE range | TRUE range | separable by one threshold |
| --- | --- | --- | --- |
| `confidence` (shipped) | 0.9050 - **0.9620** | **0.9610** - 0.9770 | **no**, the classes overlap |
| `tested_confidence` (C2) | 0.9017 - **0.9554** | **0.9599** - 0.9762 | **yes** |

A threshold of 0.9599 on `tested_confidence` reaches **precision 1.0000 while retaining all 69
of 69** true dependencies. The shipped score cannot do this at any threshold: its best is 0.9640
for precision 1.0000 at the cost of 5 true dependencies.

## Why a perfectly separating threshold is still not shipped

This is the decision the whole exercise turns on.

That threshold is **fitted to one corpus**, and there is no second corpus to validate it on:
flights and rayyan emit no candidates, and tax emits four that are all true. So the separation
is a property of 85 hospital candidates, and nothing establishes that 0.9599 transfers.
`eval/preregistration/premise_quality.md` anticipated exactly this and forbade it in **K3** --
"a parameter added after seeing the data is the overfitting `constraint-circularity.md` forbids,
and the correct response is to abandon C1 and C2 rather than to fit them."

Shipping it would also repeat a mistake this project has already made and retracted: choosing a
constant because it separated the corpora at hand.

**So `tested_confidence` ships as a reported number, not as a gate.** It is computed per
candidate and surfaced to the human who accepts or rejects it, alongside the support statistics
that were previously computed and then discarded into an English sentence. That is the
architecture `constraint-circularity.md` prescribed -- *"the defense must be architectural, not a
smarter score"* -- with the score informing a decision rather than making one.

## What this does to constraint-circularity's claim

The document says:

> **No in-table signal separates** "these violations are errors to fix" from "these violations
> are correct variation to keep" [...] The defense must therefore be architectural, not a
> smarter score.

I argued in the pre-registration that this conflated two questions, and that Q2 -- does the
dependency hold at all -- might be separable where Q1 is not. **The measurement supports the
document's conclusion and corrects its stated reason.**

- The conclusion stands. No signal is shipped as a gate, for precisely the reason the document
  gives: any threshold that works is corpus-fitted.
- The reason needs narrowing. It is **not** true that no in-table signal separates on Q2. One
  does, perfectly, on the only corpus where false dependencies exist. What is true is that the
  separation is **unvalidatable with the corpora available**, which is a claim about evidence
  rather than about signal.

That distinction matters for anyone who later reads the document and concludes the search is
futile. It is not futile; it is unfinished, and what finishes it is a second corpus containing
false dependencies with retained ground truth.

## What is shipped

1. **Constant dependents are no longer emitted.** A dependency whose dependent has one distinct
   value is vacuous -- a constant is determined by everything -- and 34 of hospital's 119
   candidates were of this kind. This is `nunique <= 1`, the exact rule this project's own bench
   oracle already applies with the rationale "a single-valued column is determined by
   everything", and it introduces no constant. **It does not improve FD-set precision** -- it
   lowers the measured figure from 0.8655 to 0.8118 by removing 34 true-but-vacuous candidates
   -- and it is justified on reviewer burden instead: 34 candidates a human must adjudicate for
   no possible repair.
2. **`tested_confidence`, `majority_share_of_dependent` and the support statistics become
   structured fields** on the candidate, surfaced in review, replacing prose.

## Limits

1. **One corpus carries the entire false-dependency result.** hospital is the only corpus where
   the miner emits a false dependency at all. Every claim about separability is scoped to 85
   candidates on 1000 rows.
2. **The truth label rewards vacuity.** `fd_holds_on_clean` counts a constant dependent as a
   true dependency. That is why the headline 0.8655 is higher than the 0.8118 measured on
   non-vacuous candidates, and the two must not be compared across the change.
3. **Corruption is unchanged.** The 86 clean cells overwritten on hospital came from
   `ZipCode -> HospitalName` and `ZipCode -> HospitalOwner`, neither of which has a constant
   dependent. Nothing shipped here reduces that number, and K2 fires accordingly.
4. **Single-column determinants only.** The miner never emits a composite determinant, so real
   composite keys are invisible and this measurement says nothing about them.
5. **tax is measured at 200,000 rows**, two orders of magnitude larger than the others, so its
   4-of-4 result reflects a very different candidate-generation regime.

## What this authorises

**Authorises** the claim that the miner's FD-set precision is 0.8655 on hospital and 1.0000 on
tax, and that it emits nothing on flights and rayyan.

**Authorises** the claim that `_MAX_DETERMINANT_UNIQUE_FRACTION` and `_MIN_FD_SUPPORT_GROUPS`
resolved the tax failure that motivated them, measured on the same corpus.

**Authorises** reporting `tested_confidence` as better-separating than the shipped confidence on
hospital.

**Does not authorise** a threshold on it. One corpus is not validation, and K3 forbids the
parameter.

**Does not authorise** any claim that the miner is now safer. Corruption is unchanged; the only
improvement is that 34 meaningless candidates no longer reach a human.

**Does not authorise** reading "no in-table signal separates" as established. It is refuted on
hospital and unvalidated everywhere else.
