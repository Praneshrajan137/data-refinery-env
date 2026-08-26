# The two withheld repairers: one killed with evidence, one that passes and still must not write

**Status**: decided 2026-08-26. Pre-registered in `eval/preregistration/withheld_repairer_coverage.md`
before any run. Artifacts: `eval/results/withheld_repairers_{hospital,flights,rayyan,tax}.json`.

## Why this was measured

`dataforge/repairers/__init__.py` justified withholding two deterministic repairers with a
parenthesis — "they regressed benchmark precision" — and there was **no artifact and no
pre-registration** behind it. The withholding rested on a docstring. By this project's standard that
is a defect whichever way the number falls: either we withhold earned coverage on folklore, or we
ship an unmeasured claim that happens to be true.

Both repairers are also stronger than "withheld": they are absent from `build_repairers`'
deterministic registry entirely, so nothing in the product can reach them. The measurement had to
construct them directly.

## Results, per distinct cell, unconditional over every cell touched

| repairer | corpus | flagged | abstained | writes | repaired | wrong | **corrupted clean** | precision | net |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `format_violation` | hospital *(injected, tripwire)* | 128 | 29 | 99 | **0** | 0 | **99** | 0.0000 | −99 |
| `format_violation` | flights *(contested, diagnostic)* | 0 | 0 | 0 | 0 | 0 | 0 | — | 0 |
| `format_violation` | rayyan *(natural, diagnostic)* | 356 | 247 | 109 | **0** | 0 | **109** | 0.0000 | −109 |
| `format_violation` | tax *(synthetic, diagnostic)* | 20018 | 9870 | 10148 | **0** | 0 | **10148** | 0.0000 | −10148 |
| **`format_violation` total** | | 20502 | | **10356** | **0** | **0** | **10356** | **0.0000** | **−10356** |
| `categorical_normalization` | hospital *(injected, tripwire)* | 0 | 0 | 0 | 0 | 0 | 0 | — | 0 |
| `categorical_normalization` | flights *(contested, diagnostic)* | 0 | 0 | 0 | 0 | 0 | 0 | — | 0 |
| `categorical_normalization` | rayyan *(natural, diagnostic)* | 25 | 0 | 25 | **0** | 0 | **25** | 0.0000 | −25 |
| `categorical_normalization` | tax *(synthetic, diagnostic)* | 976 | 0 | 976 | 604 | 77 | 295 | 0.6189 | +232 |
| **`categorical_normalization` total** | | 1001 | | **1001** | **604** | **77** | **397** | 0.6034 | **+207** |

## Verdict 1 — `format_violation`: both kill criteria fire

**K1 fires**: `write_precision` is 0.0000 on every corpus where it proposes.
**K2 fires**: 10,356 harmful against 0 repaired.

**Not one of 10,356 writes repaired a real error.** This is a cleaner kill than `decimal_shift`'s,
which at least had corpora where it was quiet.

The mechanism is a single branch. `_canonicalize` step 3 pads any shorter all-digit value to the
dominant column width, and every corpus has an identifier or amount column with legitimate width
variation:

| corpus | column | was | would be written | truth |
| --- | --- | --- | --- | --- |
| hospital | `index` | `1` | `001` | `1` |
| rayyan | `id` | `98234` | `098234` | `98234` |
| tax | `salary` | `5000` | `05000` | `5000` |

A row index, a record id, and a salary. The repairer's abstention machinery works — it declined
9,870 of 20,018 tax flags — it simply does not abstain on the branch that is wrong. The date branch
contributed no writes at all, confirming prediction P3: requiring every parse across eight formats to
agree on one target is a real guard.

So **the docstring's claim was true, and understated the magnitude.** "Regressed benchmark precision"
describes a repairer that is sometimes wrong. This one has never been right.

## Verdict 2 — `categorical_normalization`: passes, and still must not write

Neither kill criterion fires. `write_precision` is 0.6189 on tax, and 604 repaired exceeds 397
harmful.

**Passing a removal criterion is not earning admission.** K1 and K2 are the criteria that removed
`decimal_shift` — a floor for *ejecting* a member that already had authority, not a bar for granting
it. Read as an admission test they would authorise any repairer that fixes one more cell than it
breaks, which is not the standard this product sells. **38% of its writes are harmful.**

More decisive than the number is the mechanism, which the per-cell examples make plain. The
repairer's reference is the **majority exact form among the column's own values**, and tax's errors
are apostrophe doublings inside names, so all forms collapse to one normalization key:

| what happened | count | example |
| --- | --- | --- |
| majority form is correct → repairs a real error | 604 | `Ra''ed` → `Raed` |
| majority form is the corrupted one → **corrupts a clean cell** | 295 | `Raed` → `Ra''ed` |
| majority is a third, also-wrong form → wrong value | 77 | `Jun''ichi` → `Junichi`, truth `Jun'ichi` |

**A majority vote over a column's own values measures how corrupted the corpus is, not what is
true.** Where corruption is a minority the vote finds the truth; where corruption is the majority
within a cluster the vote writes the corruption into the cells that were still correct. The repairer
cannot tell which regime it is in, because it has no premise — it executes `del retry_context,
schema` on its first line.

That makes the guarantee **self-defeating in the direction that matters**: the dirtier the data, the
more a user needs repair, and the more likely the majority is wrong. It is the same shape as
`missing_value`'s unanimity guard, which degrades to a majority of one precisely when the dependency
is false.

It also means 0.6189 is not a property of this repairer. It is a property of tax's corruption rate.
The same code scores 0.0000 on rayyan. There is no single number here, and quoting one would be
quoting the corpus it came from.

## What this authorises

- Recording `format_violation`'s exclusion as **earned**: the docstring parenthesis is replaced by a
  citation to this page and its artifacts. It is permanently withheld, on measurement.
- Replacing "no measurement exists for either" with "one is measured harmful, one is measured
  corpus-dependent".
- `categorical_normalization` being described as **eligible for review-only proposal**, if anyone
  wants to argue for that separately, with 38% harmful writes stated in the same sentence.

## What this does NOT authorise

- **Write authority for either repairer.** Neither enters `CONSTRAINT_CHECKABLE_DETECTORS`. This was
  pre-committed before the numbers existed, precisely so a passing result could not become a write
  permission by momentum.
- **Quoting 0.6034 or 0.6189 as `categorical_normalization`'s precision.** Both are corpus-determined
  and the corpus that produced them is synthetic and diagnostic-tier. Its companion figure is 0.0000.
- **Reading `format_violation`'s zero writes on flights as safety.** It is unreachable there. The
  words safe, harmless and clean may not be used of that cell.
- **Concluding that the *detectors* are wrong.** Only the repairers were measured. `format_violation`
  flagged 20,502 cells and this page says nothing about how many were real errors — detection and
  correction are different problems, and a detector can be useful in a review queue while its
  repairer must never write.

## What I predicted and got wrong

**P1 was refuted.** I predicted `categorical_normalization` would corrupt clean cells on hospital via
genuine casing variation present in the clean data. It flags **zero** cells on hospital. My
structural argument about hospital's `'x'`-injection scheme was correct as far as it went — the
injected form does key separately — but I then assumed the remaining casing variation would meet the
detector's cluster criteria, and it does not. That was a prediction about a distribution I had not
computed, which is the error this project has now refuted three times.

The direction was right on the wrong corpus: the corruption appeared on rayyan and tax instead.

P2 was confirmed exactly, including the mechanism. P3 and P4 held. P5 held for `format_violation`
and failed for `categorical_normalization`, whose 0.6189 exceeds `fd_violation`'s worst arm.

## A performance finding, measured on the way

`FormatViolationRepairer._dominant_profile` rescans the entire column and recomputes `value_shape`
for every value **once per flag**. On tax:

| quantity | value |
| --- | --- |
| detector time | 5.7 s |
| flags emitted | 20018 |
| repairer cost per flag | 632 ms |
| extrapolated time to propose | **211 minutes** |

The cost is O(flags × rows), so it is invisible on the 1,000-row corpora and intractable on the only
corpus that has ever caught a repairer this project removed. Had the tax arm been abandoned as too
slow, `format_violation` would have been recorded as 208 corrupted cells rather than 10,356 — a
50-fold understatement — and the tempting shortcut, a head slice of tax, is pre-registered as
forbidden because it is a biased view of a different population.

The arm was made reachable by memoising the profile **in the harness**, which cannot change a
proposal: `_dominant_profile(df, column)` is pure and the harness does not mutate either argument.
Equivalence was verified rather than argued — with the memo enabled, hospital still reports 99/99 and
rayyan 109/109, asserted in `tests/unit/test_withheld_repairer_harness.py`.

Not fixed in the product, deliberately. The repairer is unreachable and this page records it as
permanently withheld; optimising a component with no consumer buys correctness of a report, not of
the product.

## Regression guard

Before any new number was believed, the committed figures reproduced exactly through the extended
harness: `fd_violation` 393/393/0/0 on hospital-oracle and 537/451/0/86 on hospital-mined. Any drift
would have meant the harness moved rather than the product, and the run would have been void.
