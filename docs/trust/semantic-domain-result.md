# Semantic-domain detection: the most precise detector in the suite, and still no write path

Measured 2026-08-23. Artifacts: `eval/results/detection_rt_bench.json`,
`eval/results/detection_st_bench.json` (regenerate with
`python scripts/bench/measure_detection.py --with-semantic-domain`).
Implementation: `dataforge/detectors/semantic_domain.py`.

## What was added

The *Pattern* family of Semantic-Domain Constraints learned offline by Auto-Test
(Chen et al., SIGMOD 2025, arXiv:2504.10762) over ~200,000 real table columns. An SDC pairs
a pre-condition that decides whether it applies to a column ("at least 95% of values match
this regex") with a post-condition naming the violations, and carries a confidence measured
on held-out real columns.

**60 of 505 SDCs are implemented.** The artifact also holds 361 `Embedding` (Sentence-BERT
distance), 60 `CTA` (Sherlock/Doduo) and 24 `Function` SDCs, which need model weights a
detector must not take on. `load_pattern_sdcs()` reports the declined count by family
rather than silently skipping them, because a detector that quietly implements 12% of an
artifact while being described by the artifact's name is misreporting its coverage.

Pinned to the same Auto-Test commit as the corpora, hash-verified, never vendored
(upstream publishes no licence).

## Result

| detector | RT precision | ST precision | ST tp | ST fp |
| --- | --- | --- | --- | --- |
| **SemanticDomain** | **0.3333** | **0.5333** | 8 | **7** |
| MissingValue | 0.3333 | 0.4000 | 4 | 6 |
| TypeMismatch | 0.0252 | 0.0372 | 9 | 233 |
| FormatViolation | 0.0133 | 0.0215 | 8 | 364 |
| Outlier | 0.0000 | 0.0000 | 0 | 609 |
| DecimalShift | 0.0000 | 0.0000 | 0 | 10 |

The comparison that matters is **SemanticDomain against FormatViolation on ST-bench**. Both
find 8 of the 47 unambiguous errors -- identical recall, 0.1702. One does it with 7 false
positives, the other with 364. **A 52x difference in review-queue cost for the same
detections.**

Single-detector F1 of **0.258** (ST) and **0.12** (RT) also exceeds the entire pre-existing
ensemble's 0.0218 and 0.0444, by 12x and 3x. That is not because the ensemble is large and
this is small; it is because most of the ensemble's flag volume is wrong.

## Hypothesis: why it may work when local inference does not

> **DOWNGRADED 2026-08-23.** This section originally asserted a mechanism. It is now recorded
> as an open hypothesis, because the ordering it rests on **inverts** under a change of
> scoring unit: on `rayyan` at cell level, the externally-referenced `MissingValue` scores
> 0.0649 against internally-referenced `FormatViolation` at 0.2388 -- the opposite of the
> prediction, by 3.7x. See `scoring-unit-reconciliation.md`. One supporting measurement and
> one non-replication is a hypothesis, not a mechanism.

Every other inferred constraint in this repository is derived from the column it is applied
to. `docs/trust/deterministic-is-not-sound.md` records where that leads: a `decimal_shift`
inference is deterministic and its only evidence is the shape of the column's own
distribution, which flagged 263,428 money cells with zero true errors.

An SDC's evidence comes from 200,000 *other* columns. It is the first inferred-constraint
family here whose error rate was estimated on data other than the table under test, and the
confidence it carries onto each issue is that estimate rather than a heuristic score.

## It still cannot certify, and therefore still has no repairer

Best-case one-sided upper bound on selective risk:

| corpus | threshold | accepted | errors | risk | `risk_upper` |
| --- | --- | --- | --- | --- | --- |
| RT | 0.95 | 4 | 2 | 0.5000 | 0.9024 |
| ST | 0.95 | 4 | 1 | 0.2500 | 0.7514 |
| ST | 0.90 | 15 | 7 | 0.4667 | **0.7000** |

`0.7000` against `alpha = 0.05` is fourteen times the budget. Nothing certifies at any
threshold on this evidence, and the correct disposition is unchanged.

So `semantic_domain_violation` ships with **no repairer at all** and is deliberately absent
from `CONSTRAINT_CHECKABLE_DETECTORS`. This is structural, not procedural: with no repairer
there is no write path on any surface, so the question of whether an SDC is "checkable
enough" never has to be adjudicated at write time.

The argument for allowlisting it is genuinely tempting and worth recording as refused. One
could say an SDC *is* an external reference and so satisfies the soundness axis as
`vocabulary.py:141-155` defines it. The refusal rests on a mechanism rather than on taste:
`verification_strength_for("deterministic", ...)` returns `proven` **regardless of schema**,
so allowlisting a detector with a deterministic repairer would grant proven-strength writes
on statistical evidence -- the exact conflation that produced the 263,428 false rewrites.

There is also a clean convergence. `RT-bench`/`ST-bench` ship no clean values, so the only
corpus that can validate this capability validates detection and nothing else. The evidence
available and the capability shipped are the same size, and that is not a coincidence to be
engineered around.

Assertions in `tests/unit/test_semantic_domain_detector.py`: not in the allowlist, no
registered repairer under either `allow_llm` setting, absent from `default_detectors()`,
never emitted by `run_all_detectors`, and **bytes unchanged on disk** after
`run_repair_pipeline(..., mode="apply")` -- through the real surface, because a mutant that
deleted a guard from a calling surface while leaving the guarded function intact has
survived this suite before.

## Two-way scoring is not merely biased here. It is unidentified.

> **CORRECTED 2026-08-23.** This section originally claimed the two-way rule "would have
> understated this detector's precision by 46%". That computed one of two possible collapses
> and reported the one that supported the thesis. The corrected claim below is stronger.

On ST-bench this detector flagged 28 values: 8 unambiguous errors, 7 clean values, and
**13 values labelled debatable**.

A two-way rule has no debatable class, so it must resolve those 13 one way or the other. Both
resolutions are available and they disagree by a factor of 2.6:

| resolution of the 13 debatable values | tp | fp | precision |
| --- | --- | --- | --- |
| three-way rule: excluded from both terms | 8 | 7 | **0.5333** |
| two-way, collapsed to *clean* | 8 | 20 | 0.2857 |
| two-way, collapsed to *error* | 21 | 7 | 0.7500 |

So a two-way rule does not systematically understate or overstate. **It does not identify the
quantity at all**: the measured precision ranges over `0.2857` to `0.7500` purely as a
function of an arbitrary labelling choice made by whoever built the corpus, and nothing in
the resulting number discloses which choice was made.

The three-way value of `0.5333` is not "the truth". It is the only one of the three that does
not depend on the collapse. That is a weaker claim about accuracy and a much stronger one
about validity, which is the right trade: a number whose value depends on an undisclosed
convention cannot be compared across corpora, and comparison is the entire purpose of a
benchmark.

Both collapses also change the detector's *rank*: below `MissingValue` (0.4000) under one,
above it under the other. So the ordering that `docs/trust/reference-externality.md` rests on
would itself be an artifact of the convention, if the convention were all we had.

This is the first concrete measurement of the effect `specs/SPEC_abstention_scoring.md` was
written to address. The `flights` F1 of 0.0000 was the motivating case but could always be
waved away as a peculiarity of one dataset. Here the ambiguity is 13 values on 1,197 columns,
it moves a detector two places in the ranking, and it falls hardest on the most careful
detector -- because a constraint learned from real corpora is precisely the thing that flags
values humans found genuinely arguable.

One limit on the generality of this: the counterfactual is computed over *this* label file.
A benchmark built two-way from the start would have produced different labels, not merely a
different collapse of these ones. The identification problem is real; the specific interval
`[0.2857, 0.7500]` is a property of this corpus.

## Train/test contamination: probed, not resolved

`RT-train` (~200,000 columns, from which the SDCs were learned) and `RT-bench` (1,200
columns, the evaluation set) are both sampled from the same relational-table corpus. The
first version of this document did not check disjointness. It should have.

**The decisive test cannot be run with pinned artifacts.** `RT-train` and `ST-train` are
distributed via Google Drive, not the repository, so they cannot be fetched and hash-verified
the way the benchmarks are. A column-level overlap check is therefore not reproducible here.

**A partial probe is available and was run.** Each pattern SDC embeds an `example` value
drawn from its training corpus. Checking those 59 distinct examples against the benchmark
values:

| corpus | distinct values (global union) | training-example hits | rate |
| --- | --- | --- | --- |
| RT-bench | 68,638 | **19** | 32% of examples |
| ST-bench | 64,694 | 3 | 5% of examples |

Hits include generic strings that would recur independently in any real corpus (`'q1'`,
`'6 pm'`, `'02/2021'`) and two highly specific timestamps (`'2021-01-25 00:00:00.000'`,
`'2020-12-07 00:00:00.000'`).

**Reading this honestly, in both directions:**

- The 32%-versus-5% asymmetry points the way contamination would: the SDCs learned from
  RT-train share more values with RT-bench than with ST-bench.
- But value overlap is **not** column overlap, and RT-train and RT-bench are drawn from the
  same distribution by design. Shared common values are expected even under perfect
  column-level disjointness, and 59 examples is a small probe.
- Two specific timestamps recurring is more suggestive than `'q1'` recurring, and 19 hits is
  more than nothing.

**Verdict: `contamination_unverified`.** Not shown, not excluded. The precision figures in
this document must carry that flag, and a reader comparing them to Auto-Test's published
curves should know that the paper's own train/test discipline has not been independently
confirmed here. Resolving it requires the training corpus, which is not pinnable.

This is not a defect in Auto-Test. It is a limit on what this project can verify about a
result it is reusing, and an unverifiable dependency is worth naming as one.

## Honest limits

- **Recall is low**: 0.0732 (RT) and 0.1702 (ST). 60 pattern constraints cover a small part
  of the semantic-domain space, and the 361 embedding SDCs that would extend it are not
  implemented.
- **Support is small.** 3 and 8 true positives. Precision of 0.5333 on 15 accepted values
  has a wide interval, which is exactly what `risk_upper = 0.7000` is reporting.
- **Not a reproduction of Auto-Test's published result.** Their method applies all four SDC
  families with their own scoring harness. This is DataForge's detector interface over 12%
  of their artifact, and the comparison to their PR curves in the paper is not
  protocol-controlled until the remaining families are implemented.
- **Detection only.** No clean values exist to score a repair against, and no correction
  number may be sourced from this measurement.
- **The precision figures are distinct-value, not cell-level, and the two are not
  convertible.** Measured on `rayyan`, the same detector family shows gaps up to total
  between the two units, in both directions. See `scoring-unit-reconciliation.md`. The
  0.5333 here is therefore **not** a claim about review-queue precision on a real table.
- **`contamination_unverified`**, per the section above.
