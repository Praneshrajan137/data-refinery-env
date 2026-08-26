# The premise the product ships corrupts 116 clean cells, not 86

**Status**: decided 2026-08-26. Pre-registered in `eval/preregistration/shipped_premise_coverage.md`
before any outcome was computed. Artifacts:
`eval/results/deductive_coverage_{hospital,flights,rayyan}.json`.

## The defect this measures

`docs/trust/deductive-coverage-result.md` publishes **86 clean cells corrupted** and frames that arm
as the journey a real user takes — `profile --constraints-out`, `constraints review --accept`,
`repair --constraints`. It does not run those three steps. The harness built its premise from
`infer_verification_schema`, which applies `_VERIFY_FD_MIN_CONFIDENCE = 0.95`.

The shipped accept path applies **no confidence floor of its own**:
`ConstraintReviewArtifact.to_schema()` admits any accepted functional dependency. Its effective floor
is therefore the miner's own emission floor of **0.90**.

So every published number about "the mined premise" described a **strictly more conservative product
than the one that ships.**

## The result

Hospital, 1000 rows, 509 real errors, shipped majority rule, unconditional over every distinct cell
touched:

| arm | FDs | FD-set precision | cells flagged | writes | repaired | **clean cells corrupted** | write precision | net |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oracle` — holds exactly on clean | 53 | 1.0000 | 7905 | 393 | 393 | **0** | 1.0000 | +393 |
| `mined` — the 0.95 proxy, **the published figure** | 81 | 0.8519 | 10064 | 537 | 451 | **86** | 0.8399 | +365 |
| **`shipped_accept_all` — floor 0.90, via the real artifact and merge** | **85** | **0.8118** | **10192** | **567** | **451** | **116** | **0.7954** | **+335** |

**The four dependencies the published measurement excluded repaired zero additional real errors and
corrupted thirty more clean cells.** `repaired_a_real_error` is 451 in both arms. The added premise is
pure harm with no offsetting benefit.

All four are false on ground truth, and three share the `ZipCode` determinant already responsible for
most sampled corruptions:

```
City    -> HospitalOwner    conf=0.905  tested=0.9017  TRUE_ON_CLEAN=False
ZipCode -> ProviderNumber   conf=0.949  tested=0.9477  TRUE_ON_CLEAN=False
ZipCode -> Address1         conf=0.946  tested=0.9446  TRUE_ON_CLEAN=False
ZipCode -> PhoneNumber      conf=0.944  tested=0.9426  TRUE_ON_CLEAN=False
```

### Attribution, and the nuance that a false premise is not automatically harmful

The extra 30 corruptions fall entirely in the columns of two of the four added dependencies:

| column | proxy | shipped | delta | from |
| --- | --- | --- | --- | --- |
| `ProviderNumber` | 0 | 23 | **+23** | `ZipCode -> ProviderNumber` |
| `HospitalOwner` | 23 | 30 | **+7** | `City -> HospitalOwner` |
| `HospitalName` | 23 | 23 | 0 | pre-existing |
| `State` | 20 | 20 | 0 | pre-existing |
| `Stateavg` | 20 | 20 | 0 | pre-existing |

`ZipCode -> Address1` and `ZipCode -> PhoneNumber` are equally false and corrupted **nothing**. A
false dependency only causes harm where its determinant group contains visible disagreement for the
repairer to resolve; where the group is unanimous or singleton, a false premise is inert. That is the
same mechanism recorded for `missing_value`'s unanimity guard, seen from the other side, and it means
**FD-set precision does not predict corruption count.** Half the false dependencies here were
harmless.

## Other corpora

| corpus | oracle | mined (proxy) | shipped_accept_all |
| --- | --- | --- | --- |
| flights *(contested, diagnostic)* | 4 FDs, 1807 writes, 1193 repaired, 344 corrupted | **0 FDs, 0 writes** | **0 FDs, 0 writes** |
| rayyan *(natural, diagnostic)* | 0 FDs, 0 writes | 0 FDs, 0 writes | 0 FDs, 0 writes |
| tax *(synthetic, diagnostic)* | not measured — see deviation | 4 FDs | 4 FDs, **provably the same set** |

flights and rayyan mine nothing, so the accept path has nothing to accept and the two arms coincide
trivially. **Hospital carries this entire finding**, exactly as it carried the false-dependency
finding before it. A result resting on one corpus is a hypothesis about the others.

## Regression guards, checked before any new number was believed

- `mined` on hospital reproduced **537 / 451 / 0 / 86** exactly — the published figure.
- `oracle` on flights reproduced **1807 / 1193 / 270 / 344** exactly.

K1 would have voided the run on any drift.

## Pre-registered predictions

| # | Outcome |
| --- | --- |
| **P1** — more than 86 on hospital | **CONFIRMED.** 116. |
| **P2** — flights and rayyan report 0 writes in every arm | **REFUTED as stated, and the error is mine.** flights' `oracle` arm writes 1807. I wrote "every arm" while reasoning about the mined arms; the oracle arm does not mine, it discovers on the clean frame. The claim I meant — that both *mined* arms are empty on both corpora — holds. |
| **P3** — corruptions concentrate in the added dependencies' columns | **CONFIRMED**, exactly: +23 `ProviderNumber`, +7 `HospitalOwner`, 0 elsewhere. |
| **P4** — the added corruptions may be few or zero | **REFUTED**, as I expected it to be, and it was the more interesting outcome had it held. Partially instructive anyway: two of the four added dependencies did contribute zero, so the P4 mechanism is real but not dominant. |
| **P5** — tax shows no difference between arms | **CONFIRMED by set equality**, which is stronger than by outcome: `mined_fds(tax) == shipped_accept_all_fds(tax)`, 4 FDs, identical tuples. |

## Deviation from the pre-registered method

The pre-registration specified all four corpora. **tax's outcome arms were not computed.**

Measured reason: FD detection on tax emits **169,208** flags, and `_acting_group` costs ~8 ms per
flag, so one arm is ~23 minutes and the three arms with three decision rules each are hours. Against
that, the two arms being compared are **provably identical** on tax — `mined_fds` and
`shipped_accept_all_fds` return the same 4-tuple, verified by equality — so recomputing them would be
the same computation twice, not additional evidence. Set equality is a proof where the outcome
comparison would be a sample.

What this costs, stated plainly: tax's `oracle` arm has never been measured in this harness, so the
ceiling on the mechanism is unknown for the only large corpus. That is a real gap and it is not closed
by the identity argument.

## What this authorises

- Reading **116**, not 86, as the corruption count for the premise a zero-config user actually gets on
  hospital.
- Amending `deductive-coverage-result.md` to state that its figure scores a 0.95 proxy and is a
  **bound** on the shipped path rather than a measurement of it.
- The general rule: **an arm that models a user journey must be built from the code that journey
  runs.** The proxy here was more conservative than the product, which is the direction that produces
  understatement rather than overclaim — but it is still a measurement of something the product does
  not do.

## What this does NOT authorise

- **Raising the accept-path floor to 0.95.** That constant would be chosen *after* seeing that it
  excludes four false dependencies, which is a fit rather than a finding. Forbidden by the
  pre-registration, and it remains a separate reviewable decision with a named owner.
- **Promoting `tested_confidence` to a gate.** All four added dependencies sit below the 0.9554
  boundary it separates on, so gating would appear to fix this. That threshold is still fitted to one
  corpus with nothing to validate it against. This result makes the *cost* of the earlier refusal
  visible; it does not supply the missing validation.
- **Generalising 116 beyond hospital.** flights and rayyan mine nothing and tax mines four true
  dependencies. One corpus produced every number in this document.
- **Reading FD-set precision as a corruption predictor.** Half the false dependencies added here
  corrupted nothing.

## Reconciling three published FD-set sizes

"The mined premise" has meant three different things across this project's documents. Recorded so it
stops being ambiguous:

| count | floor | what it is | where |
| --- | --- | --- | --- |
| 115 | 0.95 | `infer_verification_schema` **before** the vacuity filter removed constant dependents | `deductive-coverage-result.md`, 2026-08-25 |
| 85 | 0.90 | every candidate the miner emits — **what the accept path admits** | `premise-quality-result.md`, and this document |
| 81 | 0.95 | `infer_verification_schema` after the vacuity filter — the proxy | `mined` arm here |

The 0.8118 shipped FD-set precision and the 0.8519 proxy figure are the same quantity measured over
the 85-set and the 81-set respectively.
