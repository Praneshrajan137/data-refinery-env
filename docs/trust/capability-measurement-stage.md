# The headline capability number is measured at a stage users do not reach

**Pre-registration:** [`eval/preregistration/capability_measurement_stage.md`](../../eval/preregistration/capability_measurement_stage.md)
(H2, P1-P5, K1-K5, plus AMENDMENT 1 recording that K2 fired).
**Artifact:** `eval/results/capability_measurement_stage.json`.
**Harness:** `scripts/bench/measure_capability_stage.py`.

## The result

One table, one ground truth, one scorer — `dataforge.bench.core.score_repairs`, imported rather
than reimplemented. The arms differ **only** in the stage at which the correction set is taken, so
a difference between them cannot be a scoring difference.

| arm | writes | tp | fp | precision | recall | **F1** |
| --- | --- | --- | --- | --- | --- | --- |
| **proposal stage** — the published path | 571 | 451 | 120 | 0.7898 | 0.8861 | **0.8352** |
| **pipeline, legacy mined authority** | 1 | 1 | 0 | 1.0000 | 0.0020 | **0.0039** |
| **pipeline, C4 shipped default** | 0 | 0 | 0 | 0.0000 | 0.0000 | **0.0000** |

**H2 is confirmed.** The published 0.8352 is not attainable through the shipped write path. The
ratio is **214.2x on F1** and **451x on true positives**. K1 did not fire: the pipeline's best F1 is
0.0039, far below the 0.70 that would have meant no material gap.

All five predictions held: P1 (C4 F1 exactly 0), P2 (legacy <= 0.01), P3 (proposal stage
reproduced to 0.000000), P4 (pipeline precision 1.0000 >= proposal 0.7898), P5 (tp ratio 451 >
100).

## Why the numbers are what they are

`dataforge/bench/methods.py::run_heuristic_episode` — the producer of the published figure — calls
`_repairs_from_proposed_fixes`, which is `run_all_detectors`, then `propose_fixes`, then score.
**There is no verifier, no safety filter and no auto-apply gate between the proposal and the
score.** It also builds its schema with `include_inferred_constraints=True`, so the premise is
*mined*, which since 2026-09-07 is a premise the shipped default declines to write from.

Between proposal and write, the pipeline discards 570 of 571 candidates. The mechanism is the
repairer abstaining rather than the verifier rejecting: hospital detects 10,373 issues and records
10,368 `attempted_not_fixed` with *"No repair proposal was available for this issue."* With 85
applicable dependencies the lexicographically-first usually agrees with the cell's current value,
and the shipped rule returns `None` rather than falling through — deliberately, because falling
through was measured to cause +45 corruptions.

## What this does and does not mean

**It is not a claim that the verifier or the abstain rule is wrong.** Read P4 again: the pipeline's
one write is *correct*, precision 1.0000 against the proposal stage's 0.7898. The gates trade
recall away for precision, which is what they were built to do and what this project says it
values. A tool that writes one cell correctly is not obviously worse than one that writes 571 and
gets 120 wrong.

**It is a claim about where the number is measured.** 0.8352 is a real measurement of the
detector-and-repairer stack. It is published — in `PRODUCT.md`, `README.md`, `docs/STRATEGY.md`,
[accuracy-frontier.md](accuracy-frontier.md), and tabulated against BClean and Cocoon figures —
without stating that no user can obtain it by running `dataforge repair`. Those baselines report
what their systems output. This project reports what its repairer proposes before its own gates
reject 99.8% of it.

That is a **strictly deeper defect than the one
[baseline-protocol-comparability.md](baseline-protocol-comparability.md) documents.** That
document explains that our figure and BClean's differ in dataset, protocol and premise. This one
adds that they differ in *which stage of the pipeline is being measured* — and unlike the others,
this axis is entirely within this project's control.

## The scoping decision, settled 2026-09-08 by the measurement it asked for

Three candidate numbers could anchor a capability claim:

1. **0.8352** — what the stack proposes. Honest about the detector and repairer; silent about the
   gates. Currently published.
2. **0.0039** — what the pipeline writes under a mined premise with legacy authority. Honest about
   the write path; describes a configuration that is no longer the default.
3. **0.0000** — what the shipped default writes on a table with no declared schema. Honest about
   what a new user gets; says nothing about the declared-premise path, which is where the product
   actually claims to work.

This document originally declined to pick one, and named the missing fourth arm — the pipeline
under a **declared** premise — as the measurement the decision needed. That arm has now been run:
[declared-premise-capability.md](declared-premise-capability.md), pre-registered with the
decision rule fixed **before** the result was known.

**The declared premise writes zero cells on hospital.** Not vacuously: it raises 8,223 FD issues
and the repairer proposes 399 repairs, more than the oracle premise's 397. And the ceiling through
the write path — every dependency admitted *by ground truth* — reaches only **F1 0.1918** with 54
writes, against the 393 repairs that same premise produces at proposal stage.

So the pre-committed rule fires its second branch, and none of the three numbers above anchors a
capability claim:

> **There is no demonstrated end-to-end correction capability on hospital.** 0.8352 is retained
> as a **proposal-stage** measurement of the detector-and-repairer stack, and only that. The
> product's claim is detection, advisory triage and reversibility — which is what `DECISIONS.md`
> already recorded as "the honest product" when human-labelled certification died.

**This settles more than the anchor.** H2 said the headline is not attainable through the write
path. The declared-premise measurement adds *why not*, and it is not the reason anyone assumed:
the gap is **not** a premise-quality problem. A premise no user can obtain still loses 339 of 393
repairs, and a 13-dependency premise every member of which ground truth admits writes zero, the
same as the hand-authored one. Better schema authoring cannot close this, so the recommendation
*"Prefer --schema"* that shipped in `dataforge repair --help` had no support at any premise
quality. That help text is corrected.


## What was fixed rather than argued about

- `scripts/ci/anchor_truth.py` now re-runs `run_heuristic_episode` and fails when the committed
  artifact stops describing the code, on `tp`/`fp`/`fn` as well as F1. Before it, prose was checked
  against the artifact and the artifact against prose, with the code outside the loop; the anchor
  drifted 0.7926 -> 0.8178 -> 0.8352 across `c207617` and `4ad3760` and every gate stayed green for
  54 days.
- The anchor is registered in `docs/quantitative_claims.yaml` for the first time. It had **zero**
  registered claims while being the project's most-cited number, and `docs_truth` is an allowlist,
  so it could not see it.
- K2 of the pre-registration now reads the anchor from the gated artifact instead of a constant in
  the harness, because a documented constant is precisely what rotted.
