# Pre-registration: can an LLM filter improve the rayyan review queue without damaging it?

Registered 2026-08-24, **before any LLM call was made against rayyan**.
Spec: reuses `specs/SPEC_abstention_scoring.md` for the abstention discipline; no new normative
rule is introduced.
Implementation: `dataforge/bench/stratified.py`, `scripts/bench/probe_queue_filter.py`.
Baselines: `eval/results/cell_detection_rayyan.json`.

## Why this file exists before the code

The obvious version of this experiment reports one number -- pooled review-queue precision
before and after an LLM filter -- and that number is close to meaningless on this corpus.
Measured offline, before any spend:

| detector | flagged | true | precision | share of queue |
| --- | --- | --- | --- | --- |
| `missing_value` | 1155 | 75 | **0.0649** | 49.9% |
| `date_transposition` | 637 | **637** | **1.0000** | 27.5% |
| `format_violation` | 349 | 85 | 0.2436 | 15.1% |
| `outlier` | 76 | 0 | 0.0000 | 3.3% |
| `type_mismatch` | 64 | 2 | 0.0312 | 2.8% |
| `categorical_normalization` | 25 | 0 | 0.0000 | 1.1% |
| `decimal_shift` | 7 | 0 | 0.0000 | 0.3% |
| `entity_consensus` | 1 | 0 | 0.0000 | 0.0% |
| **ensemble** | **2314** | **799** | **0.3453** | recall 0.8428 |

The strata are **disjoint**: `run_all_detectors` keeps one issue per cell by precedence, and the
per-detector counts sum to exactly 2314. So every flagged cell has exactly one attribution and
no cell is double-counted.

Half the queue is a detector at 0.0649 with 1,080 false positives to remove. A quarter is a
detector at 1.0000 with nothing to gain and 637 correct detections to lose. A pooled lift
figure adds a possible large gain to a possible large loss and reports the sum, which is how a
filter that destroys a perfect detector could look like an improvement.

**Therefore the headline is per stratum, and the safety condition outranks the gain.**

## Hypotheses

**H1 (gain).** The LLM filter raises precision on the `missing_value` stratum, whose base rate
is 0.0649.

**H2 (safety, the one that decides shipping).** The LLM filter does not materially reduce
recall on the `date_transposition` stratum, which is at 1.0000.

Directional prediction, recorded so it can be wrong: **H1 will hold and H2 will fail.**
rayyan's missing values are largely legitimate absent optional bibliographic fields, which a
model reading the row should recognise, so false-positive removal should be substantial. But
`date_transposition` flags real errors whose wrongness is visible only against the column's
convention, not from a single row -- a transposed day and month is a perfectly plausible date
in isolation. A row-context filter should therefore reject many of them, and each rejection is
a true error dropped from the queue.

If that prediction holds, the conclusion is **not** "the LLM does not work". It is that a
uniform filter across a heterogeneous queue is the wrong architecture, and the filter must be
applied per detector.

## Thresholds

| id | quantity | threshold | rationale |
| --- | --- | --- | --- |
| Q1 | `missing_value` stratum precision after filter | `>= 0.20` to support H1 | A 3x lift from 0.0649. Below that the filter is not worth a call per cell. |
| Q2 | `date_transposition` recall retained | `>= 0.95` to support H2 | A perfect detector may lose up to 5% to a filter and still be worth filtering. Below 0.95 the filter is destroying evidence. |
| Q3 | projected whole-queue recall retained | `>= 0.90` | The queue exists to surface errors. A filter that raises precision by discarding a tenth of the errors has not improved the queue. |
| Q4 | zero-precision tail (`outlier`, `categorical_normalization`, `decimal_shift`, `entity_consensus`: 109 cells, 0 true) rejection rate | `>= 0.50` | **Non-vacuity.** These cells are all false positives by construction. A filter that cannot reject cells with no true errors in them is not discriminating at all, and any apparent gain elsewhere is noise. |

Q4 is the load-bearing sanity check. It is the one condition whose correct answer is known in
advance from ground truth, so failing it invalidates the rest.

## Fixed analysis parameters

| parameter | value | source |
| --- | --- | --- |
| model | `gpt-5.6-sol` (Azure) | fixed here |
| seed | 0 | existing probe convention |
| `reasoning_effort` | `"none"` | cost control; identical across all strata |
| sampling | stratified, enriched within stratum | fixed here |
| `missing_value` sample | all 75 true + 125 random false | fixed here |
| `date_transposition` sample | 100 random (all true; no false exist) | fixed here |
| `format_violation` sample | 60 true + 60 random false | fixed here |
| zero-precision tail sample | 60 random across the four detectors | fixed here |
| `type_mismatch` | 64 cells, 2 true; **excluded** | too small to estimate a rate; recorded as uncovered |
| projection | known population counts x sampled keep-rates | fixed here |
| interval | 95% Wilson on each keep-rate | existing convention |

**Why enriched-within-stratum rather than uniform.** A uniform sample of the queue would put
~50% of its calls on `missing_value` and recover only ~6% true cells there, leaving the
keep-rate for true cells estimated on a handful. Enriching within a stratum and projecting back
onto **known** population counts estimates both keep-rates precisely. The population
composition is not estimated -- ground truth is complete -- so the only sampled quantities are
the two conditional keep-rates per stratum.

## Stopping rule

**Fixed n, declared here:** 200 + 100 + 120 + 60 = **480 calls**, projected at roughly $2
against a $10 cap. Sampling stops at those counts regardless of the running numbers.

## Committed in advance

- Thresholds above. **Lowering Q2 or Q3 after seeing a large Q1 gain is forbidden.** A filter
  that trades recall for precision must be judged against the bar set before the trade was
  visible.
- Per-stratum reporting is mandatory. A pooled-only result may not be published from this
  design, because pooling is the specific failure the design exists to avoid.
- A result where H1 holds and H2 fails is a **publishable finding**, not a failed experiment.
  It would say the filter must be applied per detector rather than across the queue.
- Verdicts are bound to `(model, seed, dirty_sha256)`.
- This measures **detection triage only**. Nothing here proposes a value, nothing auto-applies,
  and no result may move a detector onto `CONSTRAINT_CHECKABLE_DETECTORS`.

## Known limits of the design, recorded now

1. **One corpus.** rayyan was chosen because it is the worst case for the heuristic ensemble and
   because the same detector scores 1.0000 on flights and 0.0649 here. Nothing measured here
   transfers to another table; that 15x swing is the reason.
2. **`type_mismatch` is uncovered.** 64 cells with 2 true errors cannot support a rate estimate,
   so it is excluded and its 2.8% of the queue is carried at the unfiltered rate in the
   projection. Recorded rather than silently pooled into another stratum.
3. **Two-way labels.** RAHA ships no `ground_truth_debatable` class, so a cell the model
   plausibly regards as arguable is scored as a hard error or a hard non-error. This is the
   identification problem `SPEC_abstention_scoring.md` removes on corpora that have the class,
   and rayyan does not.
4. **Row context only.** The filter sees one row. A detector whose evidence is the column's
   distribution is being judged by an instrument that cannot see that evidence, which is
   precisely why H2 is predicted to fail. This is a property of the experiment, not a defect,
   but it bounds what a negative result means.
5. **The projection assumes sampled keep-rates transfer to the unsampled remainder of each
   stratum.** Within-stratum heterogeneity would break that, and with 1,080 false
   `missing_value` cells represented by 125 draws the assumption is doing real work.

## AMENDMENT 1 (2026-08-24) - recorded after the run, as a correction to this design

**This amendment is written after seeing results and corrects a defect in the pre-registration
itself. It does not change any threshold or verdict.** The measured verdicts stand as registered:
Q1, Q2 and Q3 NOT SUPPORTED; Q4 SUPPORTED. Result:
`docs/trust/queue-filter-result.md`.

### Q4 was not the non-vacuity check it was described as

Q4 was registered as "the load-bearing sanity check ... the one condition whose correct answer is
known in advance from ground truth, so failing it invalidates the rest." It passed at 0.9833.

**A filter that answers "no" to every cell passes Q4 perfectly while being useless.** Q4 measures
rejection on a stratum containing no true errors, which is one side of the confusion matrix, so
it cannot distinguish discrimination from a constant answer. The observed filter answered "no" to
478 of 480 cells and Q4 raised no objection.

The condition it should have been is **discrimination**:

```
discrimination = P(keep | true error) - P(keep | false positive) > 0
```

Measured: **+0.0250** under the guarded prompt and **-0.7625** under the neutral one. That is the
statistic that exposes the degeneracy, and a one-sided rejection rate cannot.

**Binding on future work:** any queue-filter pre-registration in this project must use a
discrimination condition rather than a one-sided rejection rate. Q4 as written is recorded here as
a defective condition so it is not copied.

### A prompt-sensitivity control was added, and it should have been pre-registered

The system prompt carried a hint -- "An empty or absent value in an optional field is NOT an
error" -- which is a plausible cause of a near-constant "no". That was not identified as a
confound in advance. A paired control on identical cells was run afterwards
(`eval/results/queue_filter_prompt_sensitivity.json`) and **refuted** the confound: removing the
hint made the filter worse, and the `date_transposition` failure is stable across both prompts.

The control vindicated the result, but it was run after the fact and could have gone the other
way, in which case the measurement would have been wasted. **Any probe whose system prompt
contains a domain hint must pre-register a variant without it.**

