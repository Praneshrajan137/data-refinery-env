# On wild columns the LLM beats the heuristics on recall by 4x, and the precision verdict is not decisive

Measured 2026-08-24. Artifact: `eval/results/wild_column_detection.json`.
Gated by: `docs/trust/contamination-audit-result.md` (verdict `CLEAN`).
Scoring: `specs/SPEC_abstention_scoring.md`. Estimator: `dataforge.bench.stratified`.

Reproduce with `python scripts/bench/probe_wild_column_detection.py --max-usd 20`.
415 calls, 0 failed, **$0.75**.

**Read this first.** The heuristic figures here are **not** the published full-corpus numbers and
must not be compared to them. Published evaluable-ensemble precision is 0.0285 (RT) and 0.0215
(ST) over every value of every column. This measurement truncates long columns to 60 values and
projects false positives from a 120-column sample, so both arms sit higher. **The comparison is
valid because both arms saw byte-identical inputs; the absolute levels are not comparable to
anything else.**

## Result

On the **census** -- every one of the 175 columns carrying a label, scored exactly:

| corpus | arm | tp | fp | fn | debatable flagged | precision | recall |
| --- | --- | --- | --- | --- | --- | --- | --- |
| RT-bench | **LLM** | 28 | 14 | 13 | 14 | **0.6667** | **0.6829** |
| RT-bench | heuristic (evaluable) | 7 | 12 | 34 | 9 | 0.3684 | 0.1707 |
| ST-bench | **LLM** | 30 | 15 | 17 | 30 | **0.6667** | **0.6383** |
| ST-bench | heuristic (evaluable) | 14 | 23 | 33 | 26 | 0.3784 | 0.2979 |

Projected to the population, with false positives on unlabelled columns estimated from a
120-column sample:

| corpus | arm | projected precision | 95% interval | recall (exact) | projected fp total |
| --- | --- | --- | --- | --- | --- |
| RT-bench | LLM | **0.1323** | [0.0663, 0.6667] | **0.6829** | 183.7 |
| RT-bench | heuristic | 0.1235 | [0.0536, 0.3684] | 0.1707 | 49.7 |
| ST-bench | LLM | **0.1503** | [0.0994, 0.3080] | **0.6383** | 169.6 |
| ST-bench | heuristic | 0.0591 | [0.0314, 0.3784] | 0.2979 | 223.0 |

## What is solid, and what is not

**Solid: recall.** Recall is a **census**, not an estimate. Every ground-truth error lives in a
labelled column and all 175 were scored, so there is no sampling error in these numbers at all.
The LLM finds **4.0x** as many unambiguous errors as the evaluable heuristic ensemble on RT-bench
(0.6829 against 0.1707) and **2.1x** on ST-bench (0.6383 against 0.2979). That is the clearest
positive result for an LLM anywhere in this project.

**Solid: precision on labelled columns.** 0.6667 on both corpora against 0.3684 and 0.3784 --
roughly 1.8x, on exactly scored counts.

**Not decisive: population precision.** The intervals are wide because 120 columns is a small
sample of ~1,100, and per-column false-positive counts are highly variable. On RT-bench the two
arms are effectively tied (0.1323 against 0.1235, intervals overlapping heavily). On ST-bench the
LLM leads 2.5x (0.1503 against 0.0591) and the intervals still overlap. **The precision comparison
should be read as "the LLM is not worse, and is probably better on ST-bench", not as a measured
2.5x win.** A larger unlabelled sample is the fix, and it is cheap: this run cost $0.75.

Note the upper interval bounds equal the census precision. That is not a coincidence -- it is what
you get if the unlabelled columns contribute zero false positives, which the sample does not rule
out on RT-bench.

## The LLM discriminates rather than flagging uniformly

A model that flagged values at a constant rate would score well on labelled columns purely by
volume. Flag rates per column, which involve no projection:

| corpus | arm | labelled columns | unlabelled columns | ratio |
| --- | --- | --- | --- | --- |
| RT-bench | LLM | 0.81 | 0.15 | **5.4x** |
| RT-bench | heuristic | 0.41 | 0.03 | 12.2x |
| ST-bench | LLM | 0.71 | 0.14 | **5.0x** |
| ST-bench | heuristic | 0.59 | 0.18 | 3.2x |

The LLM flags 5x more often on columns that actually contain an error. This is the statistic that
task 5 taught me to compute first: on rayyan, a filter with 98% rejection looked disciplined until
discrimination showed it was answering "no" regardless of the data. Here discrimination is real.

## The three-way rule matters more for the LLM than for the heuristics

The LLM flagged 14 debatable values on RT-bench and **30** on ST-bench -- on ST that is twice its
own false-positive count. Under the two-way scoring every RAHA-derived benchmark uses, each would
have been a false positive:

| corpus | arm | three-way census precision | two-way (debatable collapsed to error) |
| --- | --- | --- | --- |
| RT-bench | LLM | 0.6667 | 0.5000 |
| RT-bench | heuristic | 0.3684 | 0.2500 |
| ST-bench | LLM | 0.6667 | 0.4000 |
| ST-bench | heuristic | 0.3784 | 0.2222 |

Two things follow. The abstention rule is **not** what produces the LLM's advantage: the ratio
between arms is ~1.8x either way, so the ordering is robust to the convention. But the *level*
moves a long way, and a benchmark without a debatable class would understate this model by up to
40%. That is the effect `SPEC_abstention_scoring.md` was written to remove, measured on a second
detector family.

## What this does and does not authorise

**Authorises** the claim that on unconstrained wild columns an LLM detector finds substantially
more real errors than this project's evaluable heuristic detectors at no worse precision, on a
corpus a two-probe audit found no memorisation of.

**Does not authorise** any comparison with the published 0.0285 / 0.0215 ensemble figures. Those
were measured on all values of all columns; this truncates and projects.

**Does not authorise** a correction claim. These corpora ship no clean values, so a repair number
sourced here would be fabricated.

**Does not authorise** a cell-level or review-queue claim. The unit is the distinct value, and
`docs/trust/scoring-unit-reconciliation.md` measures gaps up to total between the two units. Note
the contrast with `docs/trust/queue-filter-result.md`, where the same model destroyed the rayyan
cell-level queue: **an LLM shown a whole column succeeds where an LLM shown a single row fails**,
which is consistent and is the most useful architectural signal in both documents.

**Changes no write gate.** Detection only. Nothing here may move a detector onto
`CONSTRAINT_CHECKABLE_DETECTORS`.

## Limitations, all carried on the artifact

1. **L1** Distinct values, not cells. Not comparable to `cell_detection_*.json`.
2. **L2** `ground_truth` holds only unambiguous errors, so recall is an upper bound on recall over
   all real errors.
3. **L3** No clean values ship. Detection only.
4. **L4** Unlabelled values are scored as clean, but the corpus labels only errors annotators
   found. An unlabelled-but-erroneous value counts against **both** arms, so absolute precision is
   a lower bound while the comparison stays fair.
5. **L5** The false-positive projection assumes unlabelled columns are exchangeable draws.
   Zero-flag columns are retained so the per-column rate is not inflated.
6. **L6** Long columns are truncated to 60 shown values -- 48 of 189 columns on RT-bench and 61 of
   226 on ST-bench. All labelled values are always retained, so truncation cannot depress recall.
   It reduces the false-positive surface for both arms; the net direction on the *comparison* is
   not established, because the heuristics and the model would both produce more flags on more
   values.
7. **L7** The heuristic baseline excludes the six frequency-dependent and row-context detectors,
   which cannot be evaluated on a deduplicated corpus. It is not the full ensemble, and calling it
   one would repeat the error in `docs/trust/frequency-dependence-correction.md`.
