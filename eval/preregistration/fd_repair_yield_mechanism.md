# Pre-registration: why does FD repair write almost nothing, and does premise SIZE cause it?

- **Registered** 2026-09-08, **before** any arm of `scripts/bench/measure_fd_repair_yield.py` was run.
- **Status at registration:** [`docs/trust/declared-premise-capability.md`](../../docs/trust/declared-premise-capability.md)
  established that a declared premise writes **0** cells on hospital while the oracle premise writes
  **54**, that the declared premise proposes **more** candidates (399 against 397), and that its 13
  dependent columns already include all 10 the oracle wrote to. It reported redundancy in the
  dependency set as **necessary and not sufficient** and explicitly declined to name a mechanism.
  This document names one and tries to break it.
- **Amendments are appended, never edited.**

## Why this exists

The declared-premise result is a bare zero. A bare zero is not actionable: it does not say whether
the ceiling is architectural, whether it is a defect that has been silently costing repairs, or
whether it can be lifted at all. That question is the only identified route to DataForge having any
end-to-end correction capability, so it is worth a pre-registration rather than a paragraph.

## What the code already says, read before predicting

Three findings from reading the shipped path. They are the input to H4, not its output.

1. **The verifier's FD check is global over ROWS.** `dataforge/verifier/direct.py:245-260` rejects a
   candidate if **any other row** sharing the candidate's determinant value disagrees on the
   post-fix dependent value. `dataforge/verifier/smt.py` encodes the same condition.
2. **It is NOT global over the schema.** Both verifiers scope to `relevant_fds` — dependencies where
   the fixed column is the dependent **or appears in the determinant**
   (`direct.py:129-133`, `smt.py:245-248`). This **corrects the repairer's own docstring**, which
   states at `dataforge/repairers/fd_violation.py:99-103` that the differential verifier "checks a
   candidate against the WHOLE schema". It does not. That sentence is wrong and is corrected as part
   of this work.
3. **Which dependency acts is a property of premise MEMBERSHIP, not just of the data.**
   `fd_violation.py:158` sorts the applicable dependencies by determinant name and takes the first;
   `:177` abstains outright if that dependency's vote agrees with the cell's current value. So
   adding a dependency can change which determinant wins, and therefore which **partition** of the
   table supplies both the vote and the row check. `Address1 -> City` partitions hospital
   differently from `ProviderNumber -> City`.

A fourth finding, about the instrument rather than the mechanism: the ~395 candidates that vanish
die at the verifier inside `propose_repairs` (`dataforge/engine/repair.py:934-948`), and **a
verifier-rejected fix is never surfaced in `suggested_fixes`.** There is no per-stage drop counter
on `RepairReceipt`, which is why the previous measurement could not answer this from its artifact.

## Hypothesis

**H4.** A single-cell FD repair can be verified only when it is the **last remaining violation** in
its determinant group. Precisely: for a proposed value `v` at cell `(r, A)`, the fix is accepted iff
for every dependency in `relevant_fds(A)`, every other row sharing `r`'s determinant value already
holds `v`. Therefore FD-repair yield equals the number of determinant groups containing exactly one
cell whose value differs from the proposal, and premise **size** matters only through which
determinant wins the sort at `fd_violation.py:158`.

If H4 holds, a group with k >= 2 corrupted cells yields **zero** repairs, however good the premise.

## Predictions

Fixed before any arm ran. Four premise arms, all on hospital, all reusing the **already frozen**
premise `eval/premises/hospital_declared.yaml` (sha256
`4b2780a74be800808aa013ff2b9ed18b49dfad25e6b6142f2323c4dc119a9228`) — no new premise is authored
here, so no new premise needs freezing.

- **P1.** Every one of the oracle arm's **54** writes lies in a determinant group containing exactly
  one cell that differs from the proposed value.
- **P2.** For the declared premise the count of such singleton-violation groups is **0**, matching
  its 0 writes.
- **P3.** Among the declared arm's non-surviving candidates, the dominant final disposition is a
  verifier **REJECT** whose `unsat_core` names an `fd::` constraint — not `denied` (safety), not
  `unknown` (encoding), not the auto-apply gate.
- **P4.** Adding to the declared premise a **logically redundant** dependency whose determinant name
  sorts *before* the current winner **changes the write count**. Order-independence within a premise
  was fixed on 2026-08-29 by sorting; dependence on premise **membership** was not, and is the thing
  that made 53 dependencies behave differently from 17 containing the same relevant three.
- **P5 — the strong form, and the one that would establish the mechanism.** A **structural
  predictor** that never runs the pipeline — accept iff `remaining_disagreements(r, A, v) == 0`,
  computed from the frame, the proposal and `relevant_fds` — predicts the pipeline's actual write
  count within **10%** on all four arms. *If a predictor with no pipeline in it reproduces the
  pipeline's output, the mechanism is established rather than argued.*
- **P6.** **Joint** verification of every violation in a group at once accepts materially more
  repairs than sequential single-cell verification. Note `direct.py:104-111` applies a fix list
  *sequentially*, verifying each against a frame in which the others are **not yet applied**, so
  merely passing the group as a list still fails. P6 measures the headroom a joint check would
  unlock, by verifying against a frame with the whole group already substituted. **This measures
  headroom; it does not implement it.**

## Kill criteria

- **K1 — instrument falsification, and it outranks every finding below.** The harness must reproduce,
  **read from `eval/results/declared_premise_capability.json` and never from a constant**, the
  declared arm's `writes == 0` and the oracle arm's `writes == 54`. If either disagrees, **nothing
  from the run may be reported** and the harness is fixed first. This is the third time this
  criterion is imposed and the second time its referent is a gated artifact rather than a documented
  number; a constant is what rotted for 54 days.
- **K2 — the predictor must be falsified against the shipped verifier, not against my reasoning.**
  The structural predictor of P5 is a partial reimplementation of `direct.py:245-260`, and a
  reimplementation that has drifted would confirm H4 for the wrong reason. The predictor's per-cell
  verdict must agree with the **shipped `DirectVerifier`** on at least **99%** of proposals. Below
  that, the predictor is wrong, P5 is unreportable, and the disagreeing cells are reported instead.
- **K3 — H4 is refuted rather than rescued.** If P5's predictor misses the actual write count by
  more than **25%** on any arm, H4 is **wrong as stated** and is recorded refuted. No terms may be
  added to the formula to close the gap after seeing it. A formula fitted to the residual is not a
  mechanism.
- **K4 — zero shipped-behaviour change.** This is a measurement. No edit to the verifier, the
  repairer, the auto-apply gate, or the receipt schema — the last because `schema_version`, the
  attestation vectors and the conformance gate all key off `RepairReceipt`, and regenerating vectors
  to accommodate an observability field would be changing the evidence to fit the instrument. The
  per-stage breakdown for P3 is derived from `result.failures`, which already exists. Any *fix*
  suggested by this result needs its own pre-registration.
- **K5 — anti-motivated-stopping.** Published whatever it shows, including: that the ceiling is
  architectural and permanent; that the verifier has been silently rejecting correct repairs for the
  entire life of the project; or that P6's headroom is zero and there is no route forward at all.
  The uncomfortable outcome here is *not* the negative one — it is P6 being large, because that
  would mean the capability was reachable all along and nobody measured it.
- **K6 — scope.** hospital only: it is the only corpus with a committed declared-premise result to
  bind K1 against. `beers` remains excluded by the dataset-scope rule. `tax` stays **untested** —
  a head slice is not a sample (`docs/trust/sampling-bias-measured.md`). No claim is made about any
  corpus not run.

## Reporting rules fixed in advance

- **A predictor that agrees with the pipeline is not proof the pipeline is right.** P5 establishes
  *what* gates the write, not that gating is correct. The verifier refusing to write a value that
  would still leave the dependency violated is defensible behaviour; H4 is a claim about the
  consequence, not a defect finding.
- **P6 is headroom, not a result.** Any number it produces describes a configuration that does not
  exist. It must be labelled as such everywhere it appears, or it becomes the next
  proposal-stage-quoted-as-shipped defect.
- **The `fd_violation.py:99-103` docstring correction is reported as a finding**, because a wrong
  statement about what the verifier checks is exactly the class of error this project keeps finding,
  and it was load-bearing in an argument about why order-dependence had caused no corruption.

## What this cannot settle

Whether the mechanism is a **defect** or a **design**. H4 describes what happens. Deciding that
single-cell verification is the wrong granularity, and that repairs should be proposed and verified
group-atomically, is a product decision with a corruption surface of its own — a joint write that
verifies as a set can be wrong as a set. That decision requires its own pre-registration with its
own corruption measurement, and this document deliberately does not make it.

## AMENDMENT 1 (2026-09-08): H4 is REFUTED, and the refutation locates something larger

**Recorded after the five arms ran and BEFORE any SMT-leg verdict was inspected. Nothing above is
edited.** Artifact: `eval/results/fd_repair_yield_mechanism.json`.

### K1 and K2 passed, so the numbers are reportable

| criterion | outcome |
| --- | --- |
| **K1** committed write counts reproduced | **PASS.** declared 0, oracle 54, exactly as committed. |
| **K2** predictor agrees with shipped `DirectVerifier` | **PASS, at 1.0000 on all five arms.** |

K2 passing at *1.0* is what makes the rest of this amendment trustworthy: the structural predictor
is not an approximation of `direct.py:245-260`, it is exact on every proposal on every arm.

### P5 fails, so K3 fires and H4 is wrong as stated

| arm | FDs | pipeline actual writes | predictor predicted | relative error |
| --- | --- | --- | --- | --- |
| declared | 15 | **0** | **160** | refuted |
| oracle | 53 | **54** | **217** | 3.02 |
| oracle thinned to one determinant | 13 | 0 | 138 | refuted |
| declared + `Condition` redundancy | 17 | 0 | 175 | refuted |
| declared + earlier-sorting determinant | 16 | 0 | 168 | refuted |

**H4 is recorded REFUTED.** K3 forbids adding terms to close that residual, and no terms are added.
The "last remaining violation in the group" rule is a real and exactly-modelled property of
`direct.py:245-260` — but it is **not** what determines the pipeline's write count.

**P4 is also refuted**, and informatively: adding `Address1 -> City`, which sorts before every other
determinant in the declared premise and therefore wins `fd_violation.py:158`'s sort for `City`, left
the write count at **0**. Premise membership changes which dependency acts, and it does not change
the outcome, because the outcome is not being decided there.

### What the refutation locates

The predictor and the shipped `DirectVerifier` agree, at 1.0, that **~160 declared-premise
proposals satisfy every dependency that touches their column.** The pipeline writes **zero** of
them. Two facts narrow where they die:

- `docs/trust/declared-premise-capability.md` recorded that the declared arm's only
  `suggested_fixes` were **4 `decimal_shift` fixes held at `failed_conformal_threshold`**. No
  `fd_violation` fix ever reached the auto-apply gate.
- The agent-traced path shows the loss is inside `propose_repairs`
  (`dataforge/engine/repair.py:934-948`), where a verifier non-ACCEPT is recorded and **never
  surfaced in `suggested_fixes`**.

So the rejection is at the verifier, and it is not the Direct leg. `_verify_fix`
(`repair.py:719-752`) calls `differential_verify`, which requires **both** `SMTVerifier` and
`DirectVerifier` to accept and is **fail-closed** — an SMT `UNKNOWN` is discarded exactly like a
`REJECT`. `fd_violation.py:99-103` already records, of 21 cells examined in a different
investigation, that *"SMT returned UNKNOWN on all 21"*.

### H5, pre-registered here before any SMT verdict was read

**H5.** The differential verifier's **z3 leg**, not FD semantics and not premise quality, is what
suppresses FD repair. Proposals that the independently-written `DirectVerifier` proves are
discarded because the SMT leg returns `UNKNOWN`, and the fail-closed combination treats an
undecided solver identically to a disproof.

- **P7.** On the declared arm, `differential_verify` accepts approximately the pipeline's actual
  count (**0**), while `DirectVerifier` alone accepts **160**.
- **P8.** Among proposals `DirectVerifier` accepts and `differential_verify` does not, the dominant
  SMT verdict is **`UNKNOWN`**, not `REJECT`.
- **P9 — the value question, and the one that decides whether this matters.** Of the declared
  arm's `DirectVerifier`-accepted proposals, at least **70%** match retained ground truth. They are
  FD-majority values on a premise whose 15 dependencies all hold on the clean frame, so they ought
  to be mostly right. **If P9 fails, the SMT leg is protecting the corpus, the "lost capability" is
  lost corruption, and this is a defence of the design rather than a defect.**
- **P10.** The oracle arm's 54 actual writes are a **subset** of its 217 `DirectVerifier`-accepted
  proposals.

- **K7 — H5 is refuted too if the loss is downstream.** If `differential_verify` accepts materially
  more than the pipeline writes, then the verifier is not the last gate and the loss is in
  `partition_auto_apply`, the safety filter or the FD-authority hold. Report that instead; do not
  retrofit H5 onto it.
- **K8 — no mechanism change, restated because the temptation is now concrete.** A measured finding
  that the z3 leg discards provable repairs is **not** a licence to weaken fail-closed verification
  in this commit. Two independent checkers combined fail-closed is a deliberate safety property
  (`differential.py:71-86`), and relaxing it needs its own pre-registration with its own corruption
  measurement. This document measures; it does not repair the repairer.

## AMENDMENT 2 (2026-09-08): H5 refuted, K7 fired, and the cause is a 100-cell batch cap

**Recorded after the verifier-leg arms ran and BEFORE the confirm-escalations arm was run. Nothing
above is edited.**

### H5 is REFUTED, and not narrowly

| arm | pipeline writes | Direct leg accepts | SMT accepts | `differential_verify` accepts |
| --- | --- | --- | --- | --- |
| declared | **0** | **160** | **160** | **160** |
| oracle | 54 | 217 | 217 | 217 |
| oracle thinned | 0 | 138 | 138 | 138 |
| declared + `Condition` | 0 | 175 | 175 | 175 |
| declared + earlier determinant | 0 | 168 | 168 | 168 |

- **P7 REFUTED.** `differential_verify` accepts **160** on the declared arm, not ~0.
- **P8 REFUTED.** The SMT leg returns **`accept` on all 160**. Not one `UNKNOWN`. The z3 leg is
  exonerated, and the *"SMT returned UNKNOWN on all 21"* note in `fd_violation.py` does not
  generalise to this population.
- **P9 HELD, at the ceiling.** Of the 160 proposals both verifiers prove, **160 repair a real error,
  0 write a wrong value, 0 touch a clean cell — precision 1.0000.** The predicted floor was 0.70.
- **P10 HELD.** 54 <= 217.
- **K7 FIRED.** `differential_accepts` (160) far exceeds `pipeline_actual_writes` (0), so by K7's own
  terms **the loss is downstream of the verifier** and H5 must not be retrofitted onto it.

### The cause, read from the receipt rather than inferred

```
safety_verdict = escalate
reason         = NO_HIGH_VOLUME_AUTO_APPLY: Batches rewriting more than 100 cells
                 require explicit review.
```

`dataforge/engine/repair.py:1920-1932` runs `SafetyFilter().evaluate_batch(...)` and, on a non-ALLOW
verdict, sets `accepted_fixes = []`. That discard is **total and silent**: the fixes appear in
neither `result.fixes` nor `receipt.suggested_fixes`, which is why every earlier instrument saw a
bare zero with nothing held for review.

**This explains every arm, including the one that looked like success.** The oracle premise wrote 54
**because 54 is under the cap.** The declared premise wrote 0 **because it produced more than 100.**

> **A premise that finds MORE correct repairs writes FEWER cells, and past 100 it writes none.**

That inverts the incentive the whole product rests on. It also reframes the previous result: the
declared premise does not repair nothing because declaring is weak. It repairs nothing **because it
repairs too much**, and the batch gate is volume-based rather than evidence-based.

### H6, pre-registered here before the confirm-escalations arm was run

**H6.** The declared premise's capability is present, proven and correct, and is gated only by the
volume cap. Confirming the escalation releases it.

- **P11.** The declared premise with `confirm_escalations=True` writes **> 100** cells, against 0
  without it.
- **P12.** Those writes are **at least 95% correct** against retained ground truth, because the same
  population measured 160/160 at the verifier.
- **P13.** The oracle arm's `safety_verdict` is **`allow`**, confirming the cap and not some other
  property distinguishes it from the declared arm.
- **K9.** If the confirmed arm still writes 0, H6 is refuted and a further gate exists; report that
  rather than extending H6.
- **K10 — restated because the temptation is now maximal.** This measurement does **not** change the
  cap, the safety filter, or any default. A 100-cell cap on unattended rewriting may well be correct
  product behaviour; what is defective is that exceeding it **silently discards** proven fixes
  instead of holding them for review, and that every capability number this project has published
  was measured through it without anyone noticing. Changing the behaviour needs its own
  pre-registration.

## AMENDMENT 3 (2026-09-08): H6 CONFIRMED, and it overturns the previous session''s conclusion

**Recorded after the confirm-escalations arms ran. Nothing above is edited.**

| configuration | batch verdict | writes | tp | fp | precision | recall | **F1** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| declared, default | **escalate** | **0** | 0 | 0 | — | 0.0000 | **0.0000** |
| declared, `--confirm-escalations` | allow | **152** | **152** | **0** | **1.0000** | 0.2986 | **0.4599** |
| oracle, default | allow | 54 | 54 | 0 | 1.0000 | 0.1061 | 0.1918 |
| oracle, `--confirm-escalations` | allow | 54 | 54 | 0 | 1.0000 | 0.1061 | 0.1918 |

- **P11 HELD.** 152 > 100.
- **P12 HELD at the ceiling.** Precision **1.0000** — 152 correct, **zero** wrong values, **zero**
  clean cells corrupted. The predicted floor was 0.95.
- **P13 HELD.** The oracle arm''s verdict is `allow` with or without confirmation, because 54 is
  under the cap. It never needed the flag, which is why it looked like the better premise.
- **H6 is CONFIRMED.** The capability was present, proven by two independent verifiers, and correct.
  It was gated by a volume cap and discarded silently.

### The retraction this forces

`docs/trust/declared-premise-capability.md` and the surfaces it propagated to stated:

> **There is no demonstrated end-to-end correction capability on hospital.**

**That is now false and is retracted.** Measured through the shipped write path, under a premise
authored from the corpus''s public data dictionary, using one shipped and documented flag:
**F1 0.4599 at precision 1.0000 with zero corruptions.** This is the first end-to-end correction
result this project has ever measured.

The pre-committed anchor rule from `declared_premise_capability.md` — declared F1 >= 0.05 takes the
anchor — is **satisfied**, and it must be stated with its configuration, because the same premise
scores 0.0000 without the flag. Both numbers are true of different configurations, and quoting
either without naming the flag would be the stage-and-instrument defect again.

The stage gap narrows from **214.2x** to about **1.8x** (0.8352 proposal stage against 0.4599
end-to-end). The write path trades recall away for precision exactly as designed — 1.0000 against
the proposal stage''s 0.7898 — and that trade is now **demonstrated** rather than asserted.

### What was wrong in my own previous conclusion, and why

The declared-premise measurement was correct in every number and wrong in its interpretation. It
concluded the ceiling was architectural because the loss survived every premise it varied. It never
varied a **flag**. The receipt had carried `safety_verdict = escalate` and the exact string
*"NO_HIGH_VOLUME_AUTO_APPLY: Batches rewriting more than 100 cells require explicit review"* the
whole time; no instrument read it, because the harnesses recorded `writes`, `tp` and `fp` and not
the batch verdict. **A pipeline-stage measurement that does not record why the pipeline refused is
an incomplete instrument**, and that is the durable lesson here.

### The defect that remains, stated precisely

The cap itself is defensible: refusing to rewrite more than 100 cells unattended is reasonable
product behaviour. Three things about it are not:

1. **Exceeding it discards proven fixes silently.** `repair.py:1920-1932` sets
   `accepted_fixes = []`, and those fixes reach neither `result.fixes` nor
   `receipt.suggested_fixes`. A user sees zero repairs and an empty review queue. Holding them for
   review is what the rest of the pipeline does with everything it will not auto-apply.
2. **It inverts the incentive.** A premise that finds more correct repairs writes fewer cells, and
   past 100 it writes none. The oracle premise looked better than the declared premise purely
   because it found less.
3. **Every capability number this project has published was measured through it**, and no
   pre-registration, harness or gate had ever recorded the batch verdict.

**Per K10, none of that is changed here.** Fixing (1) — routing a volume-capped batch into
`suggested_fixes` instead of dropping it — is the obvious next change and needs its own
pre-registration, because it alters what a shipped receipt contains.
