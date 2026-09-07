# Pre-registration: at which stage is DataForge's headline capability measured?

- **Registered** 2026-09-07, before the pipeline-stage arm was run.
- **Status at registration**: the proposal-stage number exists (0.7926) and is quoted in
  `PRODUCT.md`, `docs/STRATEGY.md`, `docs/trust/accuracy-frontier.md` and the SOTA comparison
  table. The pipeline-stage number **has never been measured.**
- **Amendments are appended, never edited.** Predictions below stand as written even when refuted.

## Why this exists

`eval/preregistration/premise_acquisition.md` AMENDMENT 1 established that hospital's widely
quoted *451 repairs / 116 corruptions* are **repairer proposals**, because
`measure_deductive_coverage.py` runs no verifier and no auto-apply gate, while the shipped
`run_repair_pipeline` writes **1** cell on the same table with the same premise. That amendment
deliberately left one question open, and this document is that question:

**Is the headline correction F1 of 0.7926 a proposal-stage number too, and if so, what is the
pipeline-stage number?**

Two facts make this urgent rather than academic:

1. `dataforge/bench/methods.py::run_heuristic_episode` -- the producer of 0.7926 -- calls
   `_repairs_from_proposed_fixes`, which is `run_all_detectors` then `propose_fixes`, scored
   directly. It never constructs a `RepairPipelineRequest`. **There is no verifier, no safety
   filter and no auto-apply gate between the proposal and the score.**
2. That same function builds its schema with `include_inferred_constraints=True` -- the **mined**
   premise. As of 2026-09-07 the shipped default refuses to write from a mined premise (C4). So
   the headline number is measured on a premise the product now declines to act on.

`docs/trust/baseline-protocol-comparability.md` already records that 0.7926 is not
protocol-comparable with BClean or Cocoon on dataset and protocol grounds. If it is also measured
at a **different stage of our own pipeline** than those systems measure theirs, that is a
strictly deeper defect, and it is one this project can fix without anyone's cooperation.

## Hypothesis

**H2.** The correction F1 of 0.7926 is not attainable through the shipped write path on the same
table with the same premise, and the gap is a stage difference rather than a scoring difference.

## Predictions

Fixed before the pipeline-stage arm ran. All arms use hospital, the same `dataset.ground_truth`,
and `dataforge.bench.core.score_repairs` -- the identical scorer the published number uses --
so no arm can differ from another by its scoring.

- **P1.** Pipeline stage, mined premise, **shipped default (C4)**: correction F1 = **0.0000**,
  because zero cells are write candidates.
- **P2.** Pipeline stage, mined premise, **legacy authority**: correction F1 <= **0.01**. One
  write against 509 ground-truth cells cannot exceed that.
- **P3.** Proposal stage, scored by this harness, **reproduces 0.7926 to within 0.0001.** This is
  the load-bearing prediction: it is what licenses any claim about a gap.
- **P4.** Pipeline-stage **precision** >= proposal-stage precision. The verifier and the
  abstain rule are expected to trade recall away for precision, not to be uniformly worse.
- **P5.** The ratio of proposal-stage to pipeline-stage true positives on hospital exceeds
  **100x**.

## Kill criteria

- **K1 -- H2 is refuted.** If pipeline-stage F1 >= **0.70**, there is no material stage gap, the
  headline number is defensible as shipped behaviour, and this document must record H2 as wrong
  rather than search for a framing that rescues it.
- **K2 -- the instrument is refuted, and this outranks every finding below it.** If the
  proposal-stage arm does **not** reproduce 0.7926 (P3), then this harness is not measuring what
  the published number measures, and **nothing in this document may be reported.** Fix the
  harness first. A claim that the project's headline is misstated, made with an instrument that
  cannot reproduce that headline, would be the same error being criticised.
- **K3.** If hospital yields 0 ground-truth cells, the dataset is misloaded; abort.
- **K4.** Zero new tunable constants or thresholds. This is a measurement, not a mechanism.
- **K5 -- anti-motivated-stopping.** The result is published if it holds, **including if it shows
  the project's most-quoted number overstates shipped capability by two orders of magnitude.**
  Discovering that is the purpose of running this, not a reason to stop running it.

## Scope, stated so it cannot expand quietly

- **hospital only.** It is the only corpus with a published 0.7926 to reproduce. flights and
  rayyan mine no dependencies and would test nothing here.
- **`tax` is out of scope**, for the reason in `docs/trust/sampling-bias-measured.md`: a head
  slice is not a sample.
- **This measures a stage, not a mechanism.** No claim is made or intended here that the verifier
  or the abstain rule is wrong. They may well be correct and the headline number simply
  mis-scoped. Which of the two numbers should anchor the product's capability claim is a
  **separate decision** that this evidence informs and does not settle.
- **`beers` remains excluded** by the dataset-scope rule.

## AMENDMENT 1 (2026-09-07): K2 fired, and it was right to

**Recorded before the pipeline arms were reported.** Nothing above is edited.

On its first run the harness scored the published path at **0.8352**, not 0.7926 — a delta of
0.0426, far outside K2's 0.0001 tolerance. **K2 therefore blocked the run and no pipeline-stage
result was reported.** That was the correct outcome: a claim that the project's headline number is
mis-scoped, made with an instrument that cannot reproduce that headline, would be the exact error
being criticised.

### What the investigation found

The instrument was right and the **referent was stale**. Three independent lines of evidence:

1. The shipped CLI itself — `dataforge bench --methods heuristic --datasets hospital` — prints
   **0.8352**. That is a separate process, entry point and code path from this harness, and it
   agrees to four decimal places.
2. `git bisect run` over the 187 commits since the artifact was generated attributes the change
   precisely: `c207617` (2026-08-22) moved false positives 178 → 143, and `4ad3760` (2026-08-25)
   moved them 143 → 120. `tp` (451) and `fn` (58) never moved.
3. `CLAUDE.md` had already recorded on 2026-08-27 that 0.7926 is "a HISTORICAL RECORD, not a live
   regression floor" and that "no current configuration can produce it" — a correction that
   reached one file and none of the eleven others stating it as current.

Note that `CLAUDE.md`'s *stated cause* — detectors leaving the auto-apply set — cannot be right,
because this number comes from a path with no auto-apply stage. Correct conclusion, wrong
mechanism. The bisect supplies the actual one.

### How K2 is re-set, and why this is not moving the goalposts

K2's purpose is to ensure this harness measures what the published path measures. Its **referent**
was a documented constant, and that constant had rotted. The invariant is restored by binding K2
to the reproducible value with an independent witness rather than to a number in a document:

> **K2 (as amended).** The proposal-stage arm must reproduce the F1 that
> `scripts/ci/anchor_truth.py` obtains from `run_heuristic_episode` and that the committed
> `eval/results/agent_comparison.json` records — **0.8352** as of 2026-09-07. If they disagree,
> nothing may be reported.

This is strictly stronger than the original, because the referent is now itself gated: the code is
re-run against the artifact by `anchor_truth`, and the prose against the artifact by `docs_truth`.
The original K2 could be satisfied by a number nobody had checked in 54 days.

**H2 is unaffected in substance.** It predicted the headline is not attainable through the write
path. The headline is now 0.8352 rather than 0.7926, which makes the predicted gap *larger*, not
smaller. P1, P2, P4 and P5 stand as written; P3's numeral is superseded by this amendment.
