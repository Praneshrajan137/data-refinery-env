# The calibration-bypass allowlist, measured against its own standard

Measured 2026-08-25. Artifacts: `eval/results/bypass_allowlist_{hospital,rayyan,flights}.json`.
Reproduce: `python scripts/bench/measure_bypass_allowlist.py --corpus <name> --artifact <path>`.
Pre-registered in `eval/preregistration/bypass_allowlist_evidence.md`. No API cost; deterministic.

## The standard, and who met it

`dataforge/domain/vocabulary.py`:181-184 sets the rule:

> **This is an allowlist, and that is deliberate.** [...] A new detector is calibration-bound until
> it earns an entry here **with a committed measurement**.

A `deterministic` fix from a member bypasses the calibration threshold entirely -- no threshold, no
confidence, no labels. Before today only one of the three members had such a measurement.
`decimal_shift` was *removed* on measurement, so the standard was enforced on exit and never on
entry.

## Results

Per distinct cell, unconditional on error status. `oracle` and `mined` are alternative premise
configurations, not additive.

| detector | corpus | arm | flagged | writes | repaired | wrong | **corrupted** | precision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `fd_violation` | hospital | oracle | 7905 | 393 | 393 | 0 | **0** | 1.0000 |
| `fd_violation` | hospital | mined (opt-in) | 10064 | 537 | 451 | 0 | **86** | 0.8399 |
| `fd_violation` | flights | oracle | 8473 | 1807 | 1193 | 270 | **344** | 0.6602 |
| `fd_violation` | rayyan | either | 0 | 0 | 0 | 0 | 0 | n/a |
| `missing_value` | flights | oracle | 2370 | **427** | **427** | **0** | **0** | **1.0000** |
| `missing_value` | flights | mined | 2370 | 0 | 0 | 0 | 0 | n/a |
| `missing_value` | rayyan | either | 1155 | 0 | 0 | 0 | 0 | n/a |
| `missing_value` | hospital | either | 0 | 0 | 0 | 0 | 0 | n/a |
| `type_mismatch` | hospital | no premise | 92 | **0** | 0 | 0 | 0 | n/a |
| `type_mismatch` | rayyan | no premise | 64 | **0** | 0 | 0 | 0 | n/a |
| `type_mismatch` | flights | no premise | 0 | **0** | 0 | 0 | 0 | n/a |

The committed `fd_violation` figures reproduce **exactly** through the new harness, which is the
regression check on the refactor.

## `missing_value` earns its entry, decisively

**427 writes, 427 repaired, 0 wrong, 0 corrupted: write precision 1.0000.** That is the strongest
measured result of any repairer in this project, and it is a genuine measurement rather than a
conditional one -- it counts every cell the repairer touched, not only the cells that were broken.

The mechanism explains it. `_lookup` returns a value only when the determinant group contains exactly
**one** distinct non-missing dependent value, and `_derive_from_fds` returns one only when every
matching dependency agrees. That is unanimity, strictly stricter than `fd_violation`'s majority, and
it buys precision at a heavy cost in coverage: 427 repairs against 4920 real errors on flights, and
**zero** writes on rayyan despite 1155 flags.

Two limits worth stating. Its precision rests on a premise that is *exactly true* -- the oracle arm.
Its `mined` arm writes nothing on flights only because the miner finds no dependency there, so the
mined-premise exposure that cost `fd_violation` 86 cells is **untested for this repairer**, not
absent.

## `type_mismatch` wrote nothing, on any corpus

**156 cells flagged across three corpora, 4,376 rows and 6,377 real errors. Zero proposals.**

The abstention reason is uniform and correct: every one of the 156 flags fails the sentinel test.
Hospital's flagged values are RAHA `x`-substitution corruptions (`25x47x7xx0`, `3342938xxx`,
`1xx32`); rayyan's are plain numerals (`34`, `25`, `22`). None is in
`_MISSING_SENTINELS = {"n/a", "na", "null", "none", "nan", "not available", "unknown", "-", ""}`, so
the repairer returns `None` at `type_mismatch.py`:52 before its distributional test is even reached.

**My two pre-registered predictions were refuted.** P1 said `wrong_value_on_a_real_error > 0` for this
detector, contradicting `repair-failure-decomposition.md`'s pooled zero. P2 predicted low precision.
Both assumed it would write. It does not, so the pooled artifact's zero is **reconciled rather than
contradicted**, and my structural argument was correct about what *would* happen while wrong about
whether the population exists in these corpora.

### Why zero writes is not a safety result

It is tempting to read 0 harmful writes as reassurance. It is not, and the reason is the
`decimal_shift` precedent.

`decimal_shift` was benchmark-quiet too: 39 flags on hospital, 92 on flights, 112 on rayyan, precision
0.0000 on all three. What removed it was a *fourth* dataset -- error-free TPC-H, where it would have
rewritten **263,428** monetary values, plus 9.86% of a Snowflake usage column. The harm lived outside
the benchmark set.

`type_mismatch` has the same shape. Its firing population is *a missing sentinel in a
mostly-numeric column*. All three corpora encode missingness as empty strings or as `x`-substitutions,
so that population is **unrepresented here** -- while `N/A` and `unknown` in a numeric column is
among the commonest shapes of real-world dirty data. So these three corpora do not measure this
repairer; they fail to reach it.

That is what "unmeasured" means, and it is the pre-registered outcome: neither kill criterion can fire
on a detector that never proposes, and *silence is not evidence*.

## Criteria evaluated

| detector | K1 precision 0.0000 wherever it proposes | K2 harmful exceeds repaired | outcome |
| --- | --- | --- | --- |
| `fd_violation` | no (1.0000 / 0.8399 / 0.6602) | no (700 harmful, 2037 repaired) | **retained, measured** |
| `missing_value` | no (1.0000) | no (0 harmful, 427 repaired) | **retained, measured** |
| `type_mismatch` | vacuous -- proposes nowhere | vacuous | **unmeasured** |

## Decision taken

`type_mismatch` was **removed** from `CONSTRAINT_CHECKABLE_DETECTORS` on 2026-08-25. `fd_violation`
and `missing_value` are retained, now with the measurements their entries always required.

Removal does **not** disable the repairer. Its fixes now face the calibration threshold like every
other fallible source, which is the fail-closed direction. Measured cost across all three corpora is
exactly zero, because it wrote nothing on any of them.

Three consequences, all accepted rather than avoided:

1. **Decision-table row 6 now holds.** `'N/A' -> ''` with no premise was the last unpremised write in
   the product, and it is gone. Two write rows remain -- `fd_violation` and `missing_value` under
   declared dependencies -- so the table is not vacuous.
2. **The schema-free auto-apply path is genuinely empty.** `deterministic-is-not-sound.md` had claimed
   this for weeks while being wrong; it is true as of this change, and that document now records both
   facts. **Nothing in the product writes without a declared premise.**
3. **The release gate got stronger, not weaker.** Its repair-audit-revert lifecycle had been running
   on `hospital_10rows.csv`, whose only auto-appliable fix came from `type_mismatch` -- so the gate
   was smoke-testing the entire write chain through the least-evidenced detector in the product. It
   now runs on `premised_fd_10rows.csv`, where `state -> city` is a declared dependency with a 9-to-1
   majority and the write comes from `fd_violation`.

Mutant `M17` restores the bypass and dies against both the decision table and the playground smoke
test.

**What would bring it back.** A committed measurement of its writes on a corpus that actually contains
its firing population -- missing sentinels in mostly-numeric columns, with retained ground truth. That
is a corpus this project does not currently have, and building one is the honest route to
re-admission. Restoring the entry without it would repeat exactly the reasoning that let
`decimal_shift` sit in the trusted set.

## Limits

1. **Three corpora, and they do not span the failure populations.** `type_mismatch`'s trigger
   population is absent; `fd_violation`'s mined-premise exposure appears only on hospital;
   `missing_value` writes only on flights. Every conclusion here is scoped to what these corpora
   contain, and the `decimal_shift` history says that is a real limitation rather than a formality.
2. **All columns declared `str` in the FD arms**, so type narrowing cannot be confused with what the
   dependency did. Real deployments have types, so real corruption counts may be lower.
3. **`missing_value`'s mined arm is untested, not clean.** It wrote nothing because no dependency was
   mined on the only corpus where it writes at all.
4. **Ground truth is the corpora's labels.** On flights those labels are contested; see
   `docs/trust/scoring-unit-reconciliation.md`.
5. **The oracle arm is not available to any user.** It requires the clean frame.

## What this authorises

**Authorises** the claim that `missing_value` has a committed unconditional write measurement of
precision 1.0000 on 427 writes with zero harmful writes, and that `fd_violation` has one ranging
0.6602 to 1.0000.

**Authorises** describing `type_mismatch`'s bypass as **unexercised on every corpus this project
holds** -- 156 flags, zero proposals -- and therefore unjustified by measurement under the allowlist's
own rule.

**Does not authorise** reading `type_mismatch`'s zero as evidence of safety. Its firing population is
absent from these corpora, which is the condition under which `decimal_shift` looked harmless too.

**Does not authorise** a claim that `missing_value` is safe under a mined premise. That arm never
fired.

**Does not authorise** any coverage claim for the allowlist as a whole. On flights the two FD-driven
repairers together repair 1620 of 4920 real errors; on rayyan they repair none.
