# Correction: the distinct-value adapter invalidated three detectors' measurements

Dated 2026-08-23, same day as the result it corrects. Supersedes specific claims in
`real-error-detection-result.md`, which is amended in place rather than replaced.

## What was wrong

`RT-bench` and `ST-bench` ship `dist_val` -- a **distinct**-value list. The adapter in
`dataforge/bench/detection.py` builds a one-column frame with one row per distinct value.
That was recorded as limit L1 ("this is not cell-level precision") and the consequence was
not traced into the detectors.

**Three detectors require value multiplicities.** Measuring them on a deduplicated column
measures them on a distribution that does not exist.

The irony is exact and worth keeping: `real-error-detection-result.md` warns that
"conflating 'cannot apply' with 'failed' is the same category error as conflating
abstention with failure, one level up." The document committed that error while naming it.

## The mechanism, measured

Both demonstrations use the same distinct values and the same anomaly. The **only**
difference is whether repeated values are retained.

### `OutlierDetector` -- deduplication makes it fire where the real column abstains

Column: 500 copies of `100`, then `101`-`114` once each, then `9999`.

| | rows | distinct | median | MAD | outlier flags |
| --- | --- | --- | --- | --- | --- |
| frequencies preserved | 515 | 16 | 100.0 | **0.0** | **0** |
| deduplicated | 16 | 16 | 107.5 | **4.0** | **1** |

`outlier.py:74` reads:

```python
if mad == 0:
    return []  # degenerate spread; do not flag
```

On a real column with a dominant value the MAD is frequently zero, and the detector
**correctly abstains**. Deduplication destroys the dominance, so the MAD becomes non-zero
and the detector fires. Its 609 false positives on `ST-bench` -- half the ensemble's entire
error volume -- are substantially an artifact of the adapter, not a property of the detector
on real data.

### `CategoricalNormalizationDetector` -- deduplication silences it entirely

Column: 500 `NY`, 400 `CA`, 3 `ny`, 2 `ca`.

| | rows | distinct | flags |
| --- | --- | --- | --- |
| frequencies preserved | 905 | 4 | **5** |
| deduplicated | 4 | 4 | **0** |

Two independent guards close it, and either alone is sufficient:

- `categorical_normalization.py:68` -- `len(distinct_keys) / len(entries) > _MAX_DISTINCT_RATIO`
  (0.6). On deduplicated values the ratio is ~1.0, so it returns `[]` before doing any work.
- `categorical_normalization.py:88` -- `if top <= second: continue`. With every value
  appearing once, `top == second == 1`, so every cluster is skipped for having no
  strict-majority canonical form.

It finds all five case variants on the real column and nothing on the deduplicated one.

### `DecimalShiftDetector`

Same class: it uses `median` plus a log-space inter-quartile range, both computed over the
flattened distribution. Not separately demonstrated here; the mechanism is the one above.

## Deduplication is not a conservative bias. It is an unrelated one.

The two measured directions are **opposite**:

- `Outlier`: dedup **manufactures** false positives the real column would not produce.
- `CategoricalNormalization`: dedup **suppresses** true positives the real column would find.

So there is no correction factor and no "errs safe" argument. This is the same shape as the
finding in `sampling-bias-measured.md`, where a `head` slice overstated one class's detection
recall by 23x while understating another's from 0.45 to 0.00. A biased view is not a weaker
view of the same population; it is a view of a different one.

## Claims retracted

From `real-error-detection-result.md`:

1. **"`Outlier` and `DecimalShift` were not wronged by injected data ... The existing
   dispositions were right, and are now right on better evidence."**
   Retracted. The evidence is invalid, not better. Their dispositions -- no repairer for
   `outlier`, `decimal_shift` off the allowlist -- may still be correct on the RAHA
   evidence that originally justified them, which is a different and weaker claim.

2. **"`CategoricalNormalization` and `TimeFormatCruft` fired zero times across 166,387 real
   values. Not imprecisely -- not at all ... any claim of ensemble breadth should exclude
   them."**
   Retracted for `CategoricalNormalization`: it was structurally excluded by the adapter and
   correctly declined to fire. The recommendation to exclude it from claims of ensemble
   breadth was unjustified. `TimeFormatCruft` appears to be per-value and its silence is not
   yet explained; it is recorded as an open question rather than a finding.

3. **The ensemble precision figures.** Re-measured with the frequency-dependent detectors
   excluded. The result is more specific than first stated, and the first statement of it
   was itself imprecise -- recorded below rather than quietly improved.

   | corpus | published, no SDC | published, with SDC | **corrected `evaluable_ensemble`** | effect of exclusion |
   | --- | --- | --- | --- | --- |
   | RT-bench | 0.0255 | 0.0285 | **0.0285** | **none** |
   | ST-bench | 0.0113 | 0.0113 | **0.0215** | **1.90x** |

   **The error was almost entirely an ST-bench phenomenon.** On RT-bench the invalid
   detectors emitted 13 flags and every one of them was *also* flagged by a valid detector,
   so excluding them changed the ensemble by exactly zero. On ST-bench they emitted 620
   flags of which roughly 590 were unique to them -- `Outlier` alone accounted for 610 --
   so false positives fell from 1,227 to 637 and precision rose from 0.0113 to 0.0215.

   **Meta-correction.** The first draft of this document asserted the published figures were
   "too alarming by roughly 2x" and gave an RT bound of `<= 0.0263`. Both were wrong: the 2x
   applies to ST only, and the RT bound was computed by mixing the with-SDC and without-SDC
   baselines. A correction document that is itself imprecise is just another wrong claim, so
   the arithmetic is now taken from `eval/results/detection_*.json` rather than derived by
   hand.

   The general lesson holds and is the reason the bound was stated as a bound: **an ensemble
   precision cannot be corrected by subtracting per-detector false positives**, because the
   ensemble is a union over values and the overlap is unknown until measured. RT and ST sat
   at opposite extremes of that overlap -- total and near-zero -- from the same pair of
   detectors on the same run.

4. **The abstention-penalty arithmetic.** The claim that two-way scoring understates the
   semantic-domain detector's ST precision by 46% (0.5333 -> 0.2857) was computed by
   collapsing the debatable class to *clean*. Collapsing it to *error* gives 0.75, an
   overstatement of 41%. The favourable direction was selected.

   The correct claim is stronger: under two-way scoring this detector's measured precision
   ranges over **0.2857 to 0.75** -- a factor of 2.6 -- purely as a function of an arbitrary
   labelling choice. Two-way scoring on ambiguous cells is not merely biased, it is
   **unidentified**. The three-way value of 0.5333 is not "the truth"; it is the value that
   does not depend on the collapse.

## A false remediation was written into code

`registry.py` recorded `flights` as "Diagnostic until re-scored under
`specs/SPEC_abstention_scoring.md`". The three-way rule requires a `ground_truth_debatable`
label class and RAHA ships none, so **that re-scoring is not possible**. A promise that
cannot be kept was embedded where it reads as a plan. Corrected, with a test that no
`tier_reason` may promise a remediation requiring a label class its corpus lacks.

## What survives

Every claim resting only on **per-value** detectors -- a predicate on one value, with no
reference to the column's distribution. That is `TypeMismatch`, `MissingValue`,
`FormatViolation` and `SemanticDomain`, which is where the substantive finding lives:

| detector | reference checked against | ST precision |
| --- | --- | --- |
| SemanticDomain | external: 200k other columns | 0.5333 |
| MissingValue | external: fixed placeholder vocabulary | 0.4000 |
| TypeMismatch | internal: type inferred from this column | 0.0372 |
| FormatViolation | internal: pattern inferred from this column | 0.0215 |

The flat risk-coverage frontier also survives: it was measured on `TypeMismatch` and
`FormatViolation`, both per-value. So does the finding that nothing certifies at
`alpha = 0.05`.

See `reference-externality.md` for what that table implies, which is more consequential than
the number this document corrects.

## Root cause, and the structural fix

The applicability taxonomy had two classes (`column_intrinsic`, `row_context`) and needed
four. A corpus without frequencies cannot evaluate a detector that needs them, and the
scorer had no way to know that.

Fixed by classifying every detector as `per_value`, `proportion_gated`,
`frequency_dependent` or `row_context`, adding `frequencies_available` to corpus metadata,
and making the scorer **raise** rather than return a number when a `frequency_dependent`
detector meets a corpus that cannot support it. A number that cannot be valid must not be
obtainable -- the same fail-closed discipline the write allowlist uses.
