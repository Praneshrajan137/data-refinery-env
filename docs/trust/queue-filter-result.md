# An LLM row-context filter cannot triage the rayyan queue, and the failure is structural

Measured 2026-08-24. Artifacts: `eval/results/queue_filter_rayyan.json`,
`eval/results/queue_filter_prompt_sensitivity.json`.
Pre-registration: `eval/preregistration/queue_filter_rayyan.md`.
Baselines: `eval/results/cell_detection_rayyan.json`. Estimator: `dataforge.bench.stratified`.

Reproduce with `python scripts/bench/probe_queue_filter.py --max-usd 10` and
`--prompt-sensitivity`.

Total spend: **$0.95** across 770 calls, against a $10 cap.

## Result

Baseline queue: **2,314 flagged cells, 799 true, precision 0.3453**, recall 0.8428 of the
table's 948 errors.

| stratum | population | baseline precision | keep true | keep false | true errors lost |
| --- | --- | --- | --- | --- | --- |
| `missing_value` | 1155 | 0.0649 | **0.000** | 0.008 | 75.0 |
| `date_transposition` | 637 | **1.0000** | **0.010** | n/a | **630.6** |
| `format_violation` | 349 | 0.2436 | **0.000** | 0.000 | 85.0 |
| zero-precision tail | 109 | 0.0000 | n/a | 0.017 | 0.0 |
| `type_mismatch` | 64 | 0.0312 | uncovered | uncovered | 0.0 |

| condition | measured | threshold | verdict |
| --- | --- | --- | --- |
| Q1 `missing_value` precision after filter | **0.0000** | `>= 0.20` | NOT SUPPORTED |
| Q2 `date_transposition` recall retained | **0.0100** | `>= 0.95` | NOT SUPPORTED |
| Q3 whole-queue recall retained | **0.0105** | `>= 0.90` | NOT SUPPORTED |
| Q4 tail rejection rate | 0.9833 | `>= 0.50` | SUPPORTED, but the condition is defective -- see below |

**The filter destroys the queue.** Projected precision falls from 0.3453 to 0.1036 while
discarding **790.6 of 799 true errors**. It answered "no, this cell is fine" to 478 of 480 cells
with zero parse failures.

## The confound I found in my own instrument, and what happened when I tested it

The system prompt contained a hint I added to help the `missing_value` stratum: *"An empty or
absent value in an optional field is NOT an error."* A near-constant "no" is exactly what such a
hint could produce, so publishing this result from one prompt would have been publishing a
finding about my own sentence.

Identical cells, identical order, only the system prompt differing:

| prompt | keep true (n=80) | keep false (n=60) | discrimination |
| --- | --- | --- | --- |
| guarded (with the hint) | 0.0250 | **0.0000** | +0.0250 |
| neutral (hint removed) | 0.0375 | **0.8000** | **-0.7625** |

**The hint was not the cause, and removing it makes the filter worse.** Two prompt-independent
facts fall out:

1. **The model rejects 96 to 97.5% of genuine `date_transposition` errors under both prompts.**
   This is the safety failure, and it is stable. It is not a tuning problem.
2. **The false-positive behaviour is entirely controlled by the hint.** Rejection of empty
   optional fields goes from 100% to 20% on one sentence of instruction. So the guarded prompt's
   apparent competence on `missing_value` is *the prompt's* competence, not the model's -- the
   hint tells it the answer and it complies.

That second point is the sharper one. **When a filter's behaviour on false positives swings from
0.000 to 0.800 on one sentence, the filter is not reading the data.** Under the neutral prompt
discrimination is **-0.7625**: it keeps false positives 21x more often than true errors. Not
weakly useful, actively inverted.

## The pre-registration predicted the failure, and gave the right reason

> **H1 will hold and H2 will fail.** ... `date_transposition` flags real errors whose wrongness
> is visible only against the column's convention, not from a single row -- a transposed day and
> month is a perfectly plausible date in isolation. A row-context filter should therefore reject
> many of them, and each rejection is a true error dropped from the queue.

**Right on H2, and right on the mechanism.** `13/07/2011` and `07/13/2011` are both plausible
dates in isolation; only the column's convention makes one of them wrong. The filter is being
asked to see evidence it is not shown, and it fails almost totally: 0.01 recall retained.

**Wrong on H1.** I predicted useful false-positive removal on `missing_value`. Under the guarded
prompt removal is near-total (0.008 kept) but it takes all 75 true errors with it, so precision
goes to 0.0000 rather than up. Under the neutral prompt it keeps 80% of them. There is no prompt
under which H1 holds usefully, and the registered prediction that it would was too generous.

## Q4 is a defective condition, and I am recording that rather than taking the pass

Q4 was registered as the load-bearing non-vacuity check: the filter must reject at least 50% of
a stratum containing no true errors. It passed at 0.9833.

**A model that answers "no" to everything passes Q4 perfectly while being useless.** The
condition checks one side of the confusion matrix, so it cannot distinguish discrimination from
a constant answer. It provided no protection here, and the pass should be read as no information
rather than as reassurance.

The condition it should have been is **discrimination**: `keep_true - keep_false > 0`, i.e. the
filter keeps true cells more often than false ones. Measured: **+0.0250 guarded, -0.7625
neutral**. That is the number that exposes the degeneracy, and it is now computed by
`scripts/bench/probe_queue_filter.py --prompt-sensitivity` and reported on the artifact. A
future queue-filter pre-registration must use it in place of a one-sided rejection rate.

## An honest note on the headline precision figure

The projected 0.1036 is **distorted by the one uncovered stratum**. `type_mismatch` (64 cells,
2 true) was excluded in advance as too small to estimate a rate, and is carried through
unfiltered, so its 62 false positives survive into the projected queue while the filter removes
almost everything it did see.

Restricted to the covered strata, projected precision is **0.3785** against a 0.3453 baseline --
a nominal gain, on 6.4 surviving true errors out of 799. Both numbers describe the same
catastrophe from different angles, and neither should be quoted as a lift. The recall figure
(0.0105) is the one that characterises the result.

Carrying an unsampled stratum unfiltered rather than dropping it is deliberate: dropping it
would shrink the denominator and flatter the filter. `dataforge.bench.stratified` names every
such stratum on `uncovered_strata` for this reason.

## What this means

**An undifferentiated LLM filter across a heterogeneous review queue is the wrong
architecture.** Not because the model is weak, but because the queue's strata need opposite
treatments and a single filter cannot provide them: half the queue needs aggressive
false-positive removal and a quarter needs to be left completely alone.

The result also sharpens what `docs/trust/cell-level-detection-result.md` reported. That document
noted `DateTransposition` at precision 1.0000 on rayyan, and observed that per-detector
performance swings 15x across corpora. This adds that the perfect detector is also the one an
LLM filter is least equipped to check, because its evidence is distributional and the filter's
context is a single row. **The strongest heuristic detector and the weakest LLM check coincide**,
which is an argument for routing by detector rather than filtering uniformly.

## Is / is not

**Is:** a stratified measurement with known population counts, pre-registered thresholds
including a safety condition fixed before any gain was visible, and a paired prompt-sensitivity
control that tested the experimenter's own prompt as a candidate cause.

**Is not** evidence that LLMs cannot triage review queues in general. Five limitations are
recorded on the artifact:

1. **One corpus.** rayyan is the worst case for the heuristic ensemble by construction, and the
   same detector scores 1.0000 on flights against 0.0649 here.
2. **Two-way labels.** RAHA ships no `ground_truth_debatable` class, so a cell the model
   plausibly regards as arguable is scored as a hard error or a hard non-error.
3. **The filter sees one row.** This is the experiment's design and the cause of the H2 failure,
   but it bounds the conclusion: a filter given column context might behave differently, and
   that is untested.
4. **The projection assumes sampled keep-rates transfer within each stratum.** 1,080 false
   `missing_value` cells are represented by 125 draws.
5. **`type_mismatch` is uncovered** and carried unfiltered.

**Does not change any write gate.** This is detection triage: nothing proposed a value, nothing
auto-applied, and no result here may move a detector onto `CONSTRAINT_CHECKABLE_DETECTORS`.
