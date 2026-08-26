# Pre-registration: what does the premise the product actually ships corrupt?

Written 2026-08-26, **before** running the measurement. Nothing below is edited afterwards; results
and deviations are appended as amendments.

## The gap

`docs/trust/deductive-coverage-result.md` publishes the mined-premise arm as **537 writes, 451
repaired, 86 clean cells corrupted**, and frames that arm as the journey a real user takes:

> A mined dependency reaches `FDViolationRepairer` only after all three of:
> 1. `dataforge profile <csv> --constraints-out <artifact.json>`
> 2. `dataforge constraints review <artifact.json> --accept <cnd-id>`
> 3. `dataforge repair <csv> --constraints <artifact.json>`

That arm does not run those three steps. `scripts/bench/measure_deductive_coverage.py` builds its
premise from `infer_verification_schema(dirty)`, which applies `_VERIFY_FD_MIN_CONFIDENCE = 0.95`
(`dataforge/schema_inference.py:31`, used at `:802`).

The shipped path applies **no confidence floor of its own**.
`ConstraintReviewArtifact.to_schema()` admits any accepted functional dependency
(`dataforge/schema_inference.py:183`), so the effective floor is the miner's own emission floor of
**0.90** (`:664`).

So the published number scores a **strictly more conservative premise than the product ships.**

### What is already known about the gap, and why this measurement is worth running

Measured before writing this document, on hospital, because it determines whether the question is
real. Only the *premise*, never the outcome:

| FD set | Count | True on clean | FD-set precision |
| --- | --- | --- | --- |
| Measured proxy (>= 0.95) | 81 | 69 | 0.8519 |
| Shipped accept path (>= 0.90) | 85 | 69 | 0.8118 |
| **The band the measurement excluded, [0.90, 0.95)** | **4** | **0** | **0.0000** |

All four excluded dependencies are **false on ground truth**, and all four are in the family already
blamed for 23 of 25 sampled corruptions:

```
City    -> HospitalOwner    conf=0.905  tested=0.9017  TRUE_ON_CLEAN=False
ZipCode -> ProviderNumber   conf=0.949  tested=0.9477  TRUE_ON_CLEAN=False
ZipCode -> Address1         conf=0.946  tested=0.9446  TRUE_ON_CLEAN=False
ZipCode -> PhoneNumber      conf=0.944  tested=0.9426  TRUE_ON_CLEAN=False
```

**The outcome has deliberately not been computed.** Knowing that four false dependencies are
admitted does not tell us how many cells they touch: a false dependency whose determinant groups are
mostly singletons corrupts nothing, because there is no disagreement for the repairer to resolve.
That is exactly what this measurement is for, and it is why P4 below is stated.

## Method, fixed now

Add a `shipped_accept_all` arm to `scripts/bench/measure_deductive_coverage.py`. Unlike the existing
arms, it must construct its premise **through the code the user's commands run**:

1. `infer_schema(dirty)` to get candidates;
2. `build_constraint_review_artifact(...)`;
3. set every functional-dependency candidate's decision to `accepted`;
4. `merge_schema_with_reviewed_constraints(...)` to obtain the effective schema.

No reimplementation of the premise. The existing arms derive a schema directly; this one exercises
the artifact, the acceptance flag and the merge, because the claim being tested is about the shipped
path and a proxy is what created the defect.

Three arms per corpus, all four corpora (hospital, flights, rayyan, tax):

| arm | premise |
| --- | --- |
| `oracle` | dependencies that hold exactly on clean — unchanged, the upper bound |
| `proxy_0.95` | `infer_verification_schema`, reproducing the published figure exactly |
| `shipped_accept_all` | the accept path, floor 0.90, via artifact + merge |

Classification is unchanged and unconditional over every distinct cell touched:
`repaired_a_real_error` / `wrong_value_on_a_real_error` / `corrupted_a_clean_cell` /
`no_op_on_a_clean_cell`, plus `write_precision`, `harmful_write_rate`, `net_cells_improved`.

Additionally recorded per arm, because the reconciliation of three different published FD-set sizes
(115, 85, 81) is part of the deliverable: `fd_count`, `fd_count_holding_on_clean`,
`fd_set_precision`, and the effective floor.

## Predictions

| # | Prediction |
| --- | --- |
| **P1** | `shipped_accept_all` corrupts **more than 86** clean cells on hospital. The four added dependencies are all false and three share the `ZipCode` determinant already responsible for most sampled corruptions. |
| **P2** | flights and rayyan report **0 writes in every arm**. They mine no dependencies at all, so the accept path has nothing to accept. |
| **P3** | The added corruptions concentrate in the columns of the four added dependencies — `HospitalOwner`, `ProviderNumber`, `Address1`, `PhoneNumber`. If corruptions appear in unrelated columns, my attribution is wrong and the added dependencies are interacting with the repairer in a way I have not understood. |
| **P4** | **Stated because I may be wrong, and it is the outcome I would find most instructive.** The added corruptions may be **few or zero**, if those four dependencies' determinant groups are mostly singletons or unanimous. A `ZipCode` group with one row cannot exhibit disagreement, so a false dependency over it never fires. Last session I predicted `ZipCode` groups were mostly singletons and was refuted — median tested-row fraction 0.9740 — so I expect P4 to fail. But if it holds, the published 86 is **right by luck rather than by design**, which is still a measurement-validity defect and must be published as one. |
| **P5** | `tax` shows no difference between arms, because its four candidates are all >= 0.95 and all true. |

**Uncertainty stated plainly.** P1 and P4 are complementary and I cannot have both. I believe P1.
What I am confident about is only the premise arithmetic above; the cell counts are unknown to me.

## Kill criteria, fixed now

- **K1 (harness integrity).** If `proxy_0.95` does not reproduce **537 / 451 / 0 / 86** on hospital
  exactly, the harness moved rather than the product. The run is **void** and nothing is published
  until reconciled.
- **K2 (no fishing).** No re-run for a better number. No arm added, and no corpus added, after
  seeing a result.
- **K3 (retraction clause, aimed at me).** If `shipped_accept_all` corrupts **fewer** cells than 86,
  I must publish that and **retract my framing** of this as an understatement, in
  `DECISIONS.md` and in the plan that proposed it. I may not reframe the finding to preserve it.
  The measurement-validity defect stands either way — the docs still describe a different premise
  from the one that ships — but the severity claim would be mine and wrong.
- **K4 (non-vacuity).** The `shipped_accept_all` arm must differ from `proxy_0.95` on at least one
  corpus. If the two are identical everywhere, the arm is not measuring what it claims to and the
  result is **VOID**, not "no difference found".

## What is deliberately NOT being decided here

**The accept-path floor is not raised.** Setting `to_schema()` to 0.95, or to anything else, would
be choosing a constant *after* seeing that it excludes four false dependencies. That is a fit, not a
finding, and it is forbidden by this document. If the measurement argues for a floor, that is a
separate reviewable decision with a named owner, taken after the number is public.

**`tested_confidence` is not promoted to a gate.** All four added dependencies sit below the 0.9554
boundary that separated true from false on hospital last session, so gating on it would appear to
fix this. That threshold is still fitted to one corpus with nothing to validate it against, and the
prior refusal stands. This measurement makes the cost of that refusal visible; it does not overturn
its reasoning.

**No claim is made about composite determinants.** Neither arm produces them — a fact two committed
artifacts currently deny, which is corrected separately.

---

## Amendment 1 - outcome, 2026-08-26

Appended, not edited. Result: `docs/trust/shipped-premise-result.md`. Artifacts:
`eval/results/deductive_coverage_{hospital,flights,rayyan}.json`.

**P1 CONFIRMED. The shipped premise corrupts 116 clean cells on hospital, against the published 86.**

| arm | FDs | fd_set_precision | writes | repaired | corrupted | write precision |
| --- | --- | --- | --- | --- | --- | --- |
| oracle | 53 | 1.0000 | 393 | 393 | 0 | 1.0000 |
| mined (0.95 proxy, published) | 81 | 0.8519 | 537 | 451 | 86 | 0.8399 |
| shipped_accept_all (0.90) | 85 | 0.8118 | 567 | 451 | 116 | 0.7954 |

The sharpest number is the one that did NOT move: `repaired_a_real_error` is **451 in both arms**. The
four added dependencies repaired nothing and corrupted thirty more clean cells. The added premise is
pure harm with no offsetting benefit.

K1 held: `mined` reproduced 537/451/0/86 exactly, and flights' `oracle` reproduced
1807/1193/270/344 exactly. K4 held: the arms differ on hospital.

### Prediction outcomes

| # | Outcome |
| --- | --- |
| **P1** | CONFIRMED. 116 > 86. |
| **P2** | **REFUTED as written, and the error is mine.** I wrote "flights and rayyan report 0 writes in every arm" while reasoning about the mined arms. flights' `oracle` arm writes 1807 -- it discovers on the clean frame and does not mine. The claim I meant holds; the claim I wrote does not. |
| **P3** | CONFIRMED exactly. The +30 splits `ProviderNumber` +23 and `HospitalOwner` +7, and 0 elsewhere. |
| **P4** | REFUTED, as I expected. But **partially instructive**: `ZipCode -> Address1` and `ZipCode -> PhoneNumber` are equally false and corrupted **nothing**, because a false dependency is inert where its determinant group holds no visible disagreement. So the P4 mechanism is real but not dominant -- and it means **FD-set precision does not predict corruption count**. Half the added false dependencies were harmless. |
| **P5** | CONFIRMED by set equality, which is stronger than by outcome: `mined_fds(tax)` and `shipped_accept_all_fds(tax)` return the identical 4-tuple. |

### Deviation: tax's outcome arms were not computed

The method specified all four corpora. tax's arms were not run to completion.

Measured reason: FD detection on tax emits **169,208** flags and `_acting_group` costs ~8 ms per
flag, so one arm is ~23 minutes and three arms across three decision rules is hours. Against that,
the two arms under comparison are **provably identical** on tax by tuple equality, so recomputing
them is the same computation twice rather than additional evidence.

What it costs, stated rather than buried: tax's `oracle` arm has never been measured in this harness,
so the ceiling on the mechanism is unknown for the only large corpus. The identity argument does not
close that, and it is not claimed to.

### What was NOT done, as pre-committed

The accept-path floor was not raised. `tested_confidence` was not promoted to a gate. Both remain
separate reviewable decisions with named owners, and the reasoning for the second is unchanged: the
threshold is still fitted to one corpus. This result makes the cost of that refusal visible without
supplying the missing validation.