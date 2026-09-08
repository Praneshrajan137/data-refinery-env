# DataForge does correct data end to end: F1 0.4599 at precision 1.0000, once a 100-cell cap is confirmed

**Pre-registration:** [`eval/preregistration/fd_repair_yield_mechanism.md`](../../eval/preregistration/fd_repair_yield_mechanism.md)
(H4, H5, H6, P1-P13, K1-K10, plus AMENDMENT 1 refuting H4, AMENDMENT 2 refuting H5, and AMENDMENT 3
confirming H6).
**Artifact:** `eval/results/fd_repair_yield_mechanism.json`.
**Harness:** `scripts/bench/measure_fd_repair_yield.py`.

## The result

Same corpus, same premise, same scorer as
[declared-premise-capability.md](declared-premise-capability.md) — one flag different.

| configuration | batch verdict | writes | tp | fp | precision | recall | **F1** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| declared premise, shipped default | **escalate** | **0** | 0 | 0 | — | 0.0000 | **0.0000** |
| declared premise, `--confirm-escalations` | allow | **152** | **152** | **0** | **1.0000** | 0.2986 | **0.4599** |
| oracle premise, either | allow | 54 | 54 | 0 | 1.0000 | 0.1061 | 0.1918 |
| proposal stage, for reference | n/a | 571 | 451 | 120 | 0.7898 | 0.8861 | 0.8352 |

**This retracts a claim made in this directory one commit earlier.**
[declared-premise-capability.md](declared-premise-capability.md) concluded *"there is no demonstrated
end-to-end correction capability on hospital."* That is false. There is: **152 of 509 real errors
corrected, zero wrong values, zero clean cells touched**, through `dataforge repair --schema` with a
premise authored from the corpus's public data dictionary.

The number must always be quoted **with its configuration**, because the same premise scores 0.0000
without the flag. Both are true of different configurations, and quoting either bare would be the
stage-and-instrument defect this project keeps correcting.

## Why the previous measurement read zero

`dataforge/engine/repair.py:1920-1932`:

```
safety_verdict = escalate
reason         = NO_HIGH_VOLUME_AUTO_APPLY: Batches rewriting more than 100 cells
                 require explicit review.
```

On a non-ALLOW batch verdict the pipeline sets `accepted_fixes = []`. **The discard is total and
silent** — the fixes reach neither `result.fixes` nor `receipt.suggested_fixes`. A user sees zero
repairs *and an empty review queue*.

**This explains every arm, including the one that looked like success.** The oracle premise wrote 54
because 54 is under the cap. The declared premise wrote 0 because it found more than 100.

> A premise that finds **more** correct repairs writes **fewer** cells, and past 100 it writes none.

The oracle premise never looked better. It looked *smaller*.

## Two hypotheses were refuted first, and both refutations were load-bearing

**H4 — "a repair verifies only when it is the last remaining violation in its determinant group."**
Real, and exactly modelled: a structural predictor reimplementing `direct.py:245-260` agreed with
the shipped `DirectVerifier` at **1.0000 on all five arms**. And it predicted **160** declared writes
against an actual **0**, so K3 fired and H4 was recorded refuted. P4 fell with it: adding
`Address1 -> City`, which sorts before every other determinant and therefore wins
`fd_violation.py:158`, left the count at 0.

**H5 — "the z3 leg returns UNKNOWN and fail-closed discards the repair."** Refuted outright. Of the
declared premise's 160 Direct-accepted proposals, the SMT leg returned **`accept` on all 160** — not
one `UNKNOWN` — and `differential_verify` accepted all 160. K7 fired: the loss was **downstream of
the verifier**.

Refuting these mattered. Without them the plausible readings were "FD repair is architecturally
capped" or "the solver is too weak", and both would have sent work in the wrong direction. What
survived is that **both independent verifiers proved 160 repairs and all 160 were correct**
(precision 1.0000 against ground truth, before the cap was ever reached).

## What is defective, and what is not

**The cap is defensible.** Refusing to rewrite more than 100 cells unattended is reasonable. It is
also *reversible* by a documented flag, so nothing is unreachable. Three things are not defensible:

1. **Exceeding the cap discards proven fixes silently instead of holding them for review.** Every
   other gate in this pipeline routes what it will not auto-apply into `suggested_fixes` with a
   `review_reason`. This one drops them. That is why five separate instruments reported a bare zero
   and none reported *why*.
2. **It inverts the incentive**, as above.
3. **Every capability number this project has published was measured through it**, and no
   pre-registration, harness or gate had ever recorded `receipt.safety_verdict`.

Point 3 is the durable lesson and it generalises past this cap: **a pipeline-stage measurement that
does not record why the pipeline refused is an incomplete instrument.** The receipt carried
`safety_verdict = escalate` and the exact `NO_HIGH_VOLUME_AUTO_APPLY` string the entire time. Nothing
read it, because the harnesses recorded `writes`, `tp` and `fp`.

**None of this is changed in this commit** (K10). Routing a volume-capped batch into
`suggested_fixes` alters what a shipped receipt contains and needs its own pre-registration.

## What this does and does not claim

- **It does not claim the default should change.** The zero-config default remains: propose, prove,
  then refuse to rewrite 152 cells unattended. `--confirm-escalations` is the user's decision.
- **It does not claim protocol comparability.** 0.4599 is measured by this project's harness on its
  own dirty/clean cut. See [baseline-protocol-comparability.md](baseline-protocol-comparability.md).
  hospital's errors are also **injected**, one substituted character in 509 of 509 cells.
- **It is a narrower flag than "allow more than 100 cells".** `--confirm-escalations` confirms *all*
  soft safety escalations, including aggregate-sensitive edits. A user enabling it grants more than
  the volume allowance measured here, and that is an honest caveat rather than a footnote.
- **`tax` remains untested** — a head slice is not a sample. `beers` remains excluded. Nothing is
  claimed about any corpus not run.

## Also corrected

`dataforge/repairers/fd_violation.py` stated that the differential verifier "checks a candidate
against the WHOLE schema". It does not: `direct.py:129-133` and `smt.py:245-248` both scope to
dependencies where the fixed column is the dependent or appears in the determinant. The check is
global over **rows**, not over the schema. That sentence was load-bearing in an argument about why
order-dependence had caused no corruption, so it is corrected rather than left standing.
