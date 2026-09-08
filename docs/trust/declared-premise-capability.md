# A declared premise repairs nothing *by default*, and the cause is a 100-cell batch cap

> **RETRACTION, 2026-09-08, one commit after this document was written.** This document concluded
> that *"there is no demonstrated end-to-end correction capability on hospital"* and that the write
> path's suppression of repairs was **not** attributable to premise quality. The second half is
> correct. **The first half is false and is retracted.**
>
> The same premise, through the same write path, with one shipped and documented flag
> (`--confirm-escalations`), writes **152 of 509 real errors correctly at precision 1.0000 with zero
> corruptions — F1 0.4599.** Every number below is reproducible and every interpretation of the zero
> is wrong: the pipeline was not refusing on evidence, it was refusing on **volume**.
> `dataforge/engine/repair.py:1920-1932` discards the whole batch when it exceeds 100 cells, and
> discards it **silently** — into neither `result.fixes` nor `receipt.suggested_fixes`.
>
> **The oracle premise never looked better than the declared premise. It looked smaller** — 54 cells
> is under the cap, 152 is not.
>
> Full result and the two hypotheses refuted on the way:
> [fd-repair-yield-mechanism.md](fd-repair-yield-mechanism.md). This document is left standing
> because PRODUCT.md §5 forbids rewriting frozen evidence to look better; read it as what was
> believed on 2026-09-08 **before** a flag was varied.
>
> **The durable lesson is about the instrument, not the cap.** The receipt carried
> `safety_verdict = escalate` and the exact string `NO_HIGH_VOLUME_AUTO_APPLY` the entire time. Five
> instruments reported a bare zero and none reported *why*, because they recorded `writes`, `tp` and
> `fp` and never the batch verdict. **A pipeline-stage measurement that does not record why the
> pipeline refused is an incomplete instrument.**


**Pre-registration:** [`eval/preregistration/declared_premise_capability.md`](../../eval/preregistration/declared_premise_capability.md)
(H3, P1-P6, K1-K6, plus AMENDMENT 1 recording that three of six predictions were refuted).
**Artifact:** `eval/results/declared_premise_capability.json`.
**Harness:** `scripts/bench/measure_declared_premise_capability.py`.
**Premise:** `eval/premises/hospital_declared.yaml`, frozen at sha256 `4b2780a7...` before any
ground truth was consulted.

## The result

One table, one ground truth, two shared scorers — `dataforge.bench.core.score_repairs` and the
four-outcome tally from `measure_premise_write_exposure.py`, both imported rather than
reimplemented. The pipeline arms differ **only** in their premise.

| arm | premise | writes | tp | corrupted | **F1** | write precision |
| --- | --- | --- | --- | --- | --- | --- |
| proposal stage — the published path | mined | 571 | 451 | 120 | **0.8352** | 0.7898 |
| pipeline, C4 shipped default | mined | 0 | 0 | 0 | **0.0000** | `null` |
| **pipeline, declared premise** | **declared** | **0** | **0** | **0** | **0.0000** | `null` |
| **pipeline, oracle premise** | **oracle** | **54** | **54** | **0** | **0.1918** | 1.0000 |

Two findings, and the second is the one that matters.

**1. The premise the product tells users to supply repairs nothing.** A schema authored from the
CMS Hospital Compare data dictionary — 15 functional dependencies, loaded by the shipped
`load_schema`, entering through the same door as `dataforge repair --schema` — writes **zero
cells**. It is not a vacuous premise: it raises **8,223** FD issues on its declared dependents and
the repairer proposes **399** candidate repairs. Every one is discarded before the write.

**2. That zero is not the premise's fault, and no user can declare their way out of it.** The
oracle premise — every dependency admitted *by ground truth*, which no user can obtain — writes
54 cells for F1 **0.1918** against the proposal stage's **0.8352**, a ratio of **4.4x**, and
against the **393** repairs the same oracle premise produces at proposal stage. So the write path
discards 339 of 393 repairs *even when the premise is perfect*.

`write_precision` is `null`, not `0.0`, for the zero-write arms. Reporting `0/0` as `0.0` would
describe a refusal as a wrong answer. The shared scorer's `precision` of `0.0` is reported
alongside it for comparability with the stage result, and the two are distinguishable in the
artifact without reading the harness.

## Why the numbers are what they are, and where the explanation stops

The obvious reading — *the user declared too little* — does not survive the counts.

| arm | issues detected | candidates proposed | held for review | authoritative columns | writes |
| --- | --- | --- | --- | --- | --- |
| declared | 7,705 | **399** | 4 | 15 | **0** |
| oracle | 8,133 | **397** | 4 | 14 | **54** |
| mined, C4 | 10,373 | 573 | 5 | **0** | 0 |

The declared premise proposes **more** candidate repairs than the oracle premise (399 against
397), holds authority over **more** columns (15 against 14), and its 13 declared dependents
already include **all 10** columns the oracle wrote to. It still writes nothing.

Nor is it the C4 authority gate. That gate is visible in the mined arm's **0** authoritative
columns — C4 withholding write authority from a mined premise, exactly as designed. The declared
arm has 15 authoritative columns and is blocked anyway, downstream of authority.

Two probes, added **post hoc** and labelled as such in the artifact and the pre-registration
amendment, bound the mechanism from both directions:

| probe | FDs | max determinants per dependent | all hold on clean | candidates | writes |
| --- | --- | --- | --- | --- | --- |
| oracle thinned to one determinant per dependent | 13 | 1 | yes | 390 | **0** |
| declared plus the oracle's other two `Condition` determinants | 17 | 3 | yes | 399 | **0** |

- The **reverse** probe is decisive for finding 2. Thirteen dependencies, **every one admitted by
  ground truth**, covering the same 13 columns, with only the *redundancy* removed — and it
  writes **zero**, exactly like the declared arm. At equal size a declared premise and a
  ground-truth-admitted premise behave identically. **The declared arm's zero is therefore not a
  penalty for being hand-authored, imperfect or small.**
- The **forward** probe shows per-column redundancy is not sufficient: the same three `Condition`
  determinants that produce 11 writes inside the 53-dependency oracle premise produce **0**
  inside a 17-dependency premise. Whatever gates the write is a property of the premise as a
  whole, not of the column being repaired.

**Redundancy in the dependency set is necessary and not sufficient, and this document does not
claim to know the mechanism.** Naming it needs its own pre-registration. What is established is
the negative, and the negative is what the anchor decision needs: the write path's near-total
suppression of repairs is **not** attributable to premise quality.

## What this corrects, in shipped code

Until 2026-09-08 two shipped sites credited the **oracle** ceiling to a **declared** premise, and
one of them was user-facing output:

- `dataforge/cli/repair.py`, the `--trust-mined-constraints` help text: *"a declared premise
  repaired 393 and corrupted none. **Prefer --schema.**"*
- `dataforge/engine/repair.py`, the C4 field docstring: *"the declared premise produced 393 with
  none."*

**393 / 0 is the oracle arm.** Every trust document that sources it says so —
[deductive-coverage-result.md](deductive-coverage-result.md),
[bypass-allowlist-evidence.md](bypass-allowlist-evidence.md),
[shipped-premise-result.md](shipped-premise-result.md) — and
`measure_deductive_coverage.py::discover_oracle_fds` admits a dependency only if it holds exactly
on the clean frame, its docstring stating *"No user has this. It is the ceiling."*

So C4's shipped default and the advice given to every user who read that flag were substantiated
by a number produced by a premise no user can author. Both sites now cite the measured declared
figure. `DECISIONS.md` states C4's reversal criterion with the same conflation — *"if the declared
arm's numbers move (hospital oracle must stay at 393 repairs / 0 corruptions)"* — and that entry
is historical and append-only, so it stands, as does AMENDMENT 1 of
`eval/preregistration/premise_acquisition.md` which writes *"the declared/oracle arm"*.

**This does not retract C4.** C4 prevents corruptions, the declared arm corrupted nothing, and the
oracle arm's 54 writes were correct at precision 1.0000. What is retracted is the *evidence
offered for it*, and the claim that a user's declaration recovers the capability. On this corpus
it recovers none of it.

## What this does and does not mean

**It is not a claim that the gates are wrong.** Every write in this measurement was correct: the
oracle arm's 54 at precision 1.0000, against the proposal stage's 571 writes of which 120
corrupted clean cells. A tool that writes 54 cells correctly is defensible. A tool that writes
571 and gets 120 wrong is the alternative on offer.

**It is a claim about what may be said.** The product's central promise is that declaring a schema
makes repairs provable. On its flagship corpus, declaring a schema authored from the public data
dictionary produces **no repairs at all**, and the pre-registered decision rule therefore records
that **there is no demonstrated end-to-end correction capability on hospital.** 0.8352 remains a
real measurement of the detector-and-repairer stack at proposal stage, and only that.

**`tax` is untested here and reported as such.** A head slice is not a sample; see
[sampling-bias-measured.md](sampling-bias-measured.md). `beers` remains excluded by the
dataset-scope rule. Nothing is claimed about any corpus that was not run.

## What the instrument had to prove before it was allowed to say any of this

The result is a **zero**, and at least four bugs produce a zero: an unbound premise, a misspelled
column, a scorer reporting `0/0` as `0.0`, and a hash check that silently passes. Each would have
produced this headline for the wrong reason. So:

- **K1a and K1b — two independently gated referents.** The harness had to reproduce the published
  anchor F1 from `eval/results/agent_comparison.json` (0.8352, delta 0.000000) **and** the
  committed mined-C4 arm's writes/tp/fp from `eval/results/capability_measurement_stage.json`
  (0/0/0) exactly. Both read from their artifacts, never from constants — the stage
  pre-registration's AMENDMENT 1 records a constant that had rotted for 54 days and blocked an
  entire result when it fired.
- **K2 — vacuity.** A premise that never bound and a premise that bound then refused produce the
  same numeral. The harness asserts every declared column exists, no dependent is constant, and
  FD detection raises at least one issue, *before* scoring. It reports `vacuous`, never `0.0`.
- **K3 — the premise is frozen by hash**, and the hash is read from the pre-registration rather
  than held as a constant in the harness. The hash is taken over the file's text with line endings
  normalised, because `core.autocrlf=true` would otherwise make the same committed file hash
  differently on Windows and in Linux CI, voiding every run for a reason unrelated to the premise.
- **K4 — anti-motivated-stopping**, fixed in advance: publish even if the declared arm writes
  zero. It did.

`tests/unit/test_declared_premise_harness.py` pins these refusals independently of the corpus. It
caught one real defect while being written: the K3 failure branch raised `ValueError` from
`Path.relative_to` instead of the `SystemExit` it was written to raise, so the kill criterion
could not have fired cleanly.
