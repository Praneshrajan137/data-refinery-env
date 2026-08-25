# Deterministic is not sound

**Date:** 2026-08-22
**Status:** the corruption path is closed on all write surfaces and proven closed by
mutation testing. The 13 fixtures are migrated. **This document's original status line was
wrong; see the retraction immediately below.**

## RETRACTION (2026-08-22, same day)

This document originally opened with:

> **Status:** the corruption path is closed and proven closed. 13 test fixtures still
> encode the old behaviour and are enumerated at the bottom; that migration is NOT done.

**"Closed and proven closed" was false when written.** The fix landed in
`partition_auto_apply`. The agent write surface never calls `partition_auto_apply`, so the
corruption path stayed open there for the rest of that session. Measured directly:

```
=== AGENT, NO SCHEMA ===
  [trace] issues=1 floor_from_propose_repairs=1
     detector=decimal_shift col=amount prov=deterministic new='102'
  RESULT floor_fix_count=1 fixes_count=1 applied=True
  row4 now: 4,102          <-- WROTE. run_repair_pipeline refused this exact fix.
```

Three things about this are worth carrying forward:

1. **The inversion.** Without a schema the agent wrote; *with* a schema it did not. The
   less-premised, default invocation was the dangerous one. The first probe of this
   question used a schema, came back clean, and nearly closed the investigation.
2. **A green test depended on the hole.**
   `test_surface_uniformity.py::test_agent_receipt_is_a_verifiable_certificate` asserted
   `applied is True` and passed *because* the agent applied a fix the product does not
   stand behind. Closing the hole turned it red. Nothing else in the suite objected.
3. **Why the existing structural guard missed it.** `enforce_proven_only` already lived at
   the mutation primitives precisely so a surface could not bypass a gate by forgetting to
   call the partitioner — its docstring cites this exact class of past incident. It was
   incomplete in the one dimension newly added: it enforces STRENGTH, and
   `verification_strength_for("deterministic", …)` returns `proven` regardless of schema,
   so the detector allowlist was never consulted there.

The generalisable lesson is not "check the agent path too". It is that **a claim of the
form "gate G is closed" is a claim about every write surface, and is only as good as the
enumeration of surfaces backing it.** The enumeration existed
(`test_surface_uniformity.py::_WRITE_PRIMITIVE_REGISTRY`) and was not consulted when the
allowlist was added.

### What is true now

- The allowlist is enforced at both mutation primitives —
  `enforce_constraint_checkable_only` (CSV) and
  `enforce_plan_constraint_checkable_only` (warehouse SQL) — and proactively at each
  partition point so surfaces *hold and agree* rather than raising.
- Nine mutants covering every guard added are killed, including one that only dies when
  two redundant guards are reverted together.
- The decision is now an executable specification: `specs/SPEC_autoapply_decision.md` with
  `tests/integration/test_autoapply_decision_table.py`, asserting **bytes on disk** across
  detector × premise × surface.
- Suite: 1857 passed, 0 failed.

### A second correction, to this document's own reasoning

The claim below that the bundled `hospital_10rows.csv` floor consisted solely of the
`decimal_shift` `45.0 → 4.5` repair is **also wrong, and was never measured before being
asserted.** Measured floors after the allowlist change:

| fixture | floor | detector |
| --- | --- | --- |
| `premised_fd_10rows.csv` | 1 | `fd_violation` (added 2026-08-22) |
| `hospital_10rows.csv` | 1 | `type_mismatch` on `phone_number` |
| `dirty.csv` | 2 | `type_mismatch` on `age`, twice |

So the agent parity gate was never actually vacuous. The non-vacuity guard added to
`AgentGateReport` is **prophylactic, not a fix for a live defect**, and is documented as
such in its own docstring.

## The bug, in one line

`dataforge repair --apply` on a 25-row table containing **no errors** rewrote a legitimate
`1131.20` as `113120` -- a 100x monetary inflation -- recorded it `proven`, and was held back by
nothing.

## Verified end to end

```
before sha256: f8c47b777f6111526b29bd2346bafbce83f2d4ad8c105846ca39660731e25aea
after  sha256: 8c5b625f7d295b5c46d8beea9abdf06f2a6e6c46364b7d7d88757419363721f7
row: 1016,113120,papa          <- was 1016,1131.20,papa
journal: {"detector_id": "decimal_shift", "new_value": "113120", "old_value": "1131.20"}
```

Default settings, no schema, one `--confirm-escalations`. Revert restored the exact original
bytes, so reversibility (INV3) held -- but the corruption happened silently first, and a bad
write discovered late is the asymmetric cost this project exists to avoid.

## Why every gate failed

| gate | why it did not fire |
| --- | --- |
| `enabled_classes == []` | bypassed: `partition_auto_apply` read `if deterministic or policy...` |
| `strength_for_fix` | `TRUSTED_PROVENANCE = {"deterministic"}`, so strength was `proven` |
| the verifier | no schema declared, so there was no constraint to violate |
| `allow_unproven_autoapply=False` | irrelevant; the fix was never `plausibility_only` |

Reproduced by direct call under the strictest possible settings -- `covered_columns=frozenset()`,
`allow_unproven_autoapply=False`, default policy -- and the fix landed in the **auto** bucket with
`calibration_held=0, plausibility_held=0`.

## Blast radius, measured on error-free data

Every flag below is a false positive by construction: TPC-H is produced by a reference generator
and contains no errors.

| column | rows | would be rewritten | rate |
| --- | --- | --- | --- |
| `lineitem.l_extendedprice` | 6,001,215 | **212,358** | 3.54% |
| `orders.o_totalprice` | 1,500,000 | 41,685 | 2.78% |
| `customer.c_acctbal` | 150,000 | 9,385 | 6.26% |
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.total_elapsed_time` (real) | 42,245 | 4,167 | **9.86%** |

**263,428 monetary values** across three TPC-H tables. The last row is real Snowflake operational
telemetry -- the table billing and query monitoring run on.

And it has never found a real error: `decimal_shift` scored precision **0.0000** on hospital
(39 flags), flights (92) and rayyan (112).

## Root cause: three layers

**1. The rule.** It compared each value to the median with no awareness of the column's own
spread. Measured log-space IQR of the affected columns is **0.44-0.62 dex**, so a 10x offset sits
only 1.6-2.3 IQR units out -- entirely ordinary. Fixed by a dispersion gate
(`|log10(v/median)| > 3 x log-IQR`), which removes **98.1%** of the false positives
(267,595 -> 5,141; 212,358 -> **0** on `l_extendedprice`) and loses no true positives.

**2. The architecture.** `TRUSTED_PROVENANCE = {"deterministic"}` conflated *determinism of the
procedure* with *soundness of the inference*. A rule that cannot invent a value can still
deterministically choose the wrong one. And the hierarchy was **inverted against the evidence**:
the LLM corrector measures 92/92 on `type_mismatch` and is gated as `plausibility_only`, while
`decimal_shift` measures 0.0000 and was trusted as `proven`. Trust was conferred by
implementation category rather than earned by measurement -- the opposite of what `PRODUCT.md`
says, and the deterministic path was the one place it was not applied.

Fixed by `CONSTRAINT_CHECKABLE_DETECTORS`, an **allowlist**: a deterministic repair bypasses the
calibration gate only if its rule is checkable against a reference (a declared type, a declared
functional dependency) rather than inferred from a distribution. A detector nobody classified is
calibration-bound, not exempt.

**3. The test design -- the layer that let it ship.**
`tests/property/test_no_corruption_invariant.py` calls itself *"The Corruption Oracle: a universal
no-regression property test"* and its own docstring says it generates clean numeric columns
*"clustered (low variance) **so no correct cell is a decimal-shift outlier** -- this keeps INV1
sound (no false positives)"*.

**The oracle constructed its data so the bug could not occur, then concluded the invariant held.**
The precondition that makes the detector sound became a property of the fixture instead of a check
in the code. "Universal" was false. Real warehouse columns violate that precondition universally.

The fix reuses the oracle's own words: the dispersion gate *is* that precondition, enforced at
runtime. Injected 10x errors on clustered columns still clear it, which is why the oracle keeps
passing.

## A mistake made while fixing it, kept on the record

The first version of the gate keyed off the local `deterministic` flag. But
`_LLM_PROVENANCE = {llm_cache, llm_live}`, so `provenance not in _LLM_PROVENANCE` is **also true
for `external` and `entity_consensus`**. That silently blocked the schema-proven external write
path -- a legitimate premised write -- and broke 15 tests. The variable name asserted something
the value did not mean. Corrected to test `fix.provenance == "deterministic"` explicitly.

## What is now enforced, and mutation-verified

`tests/property/test_clean_data_is_not_flagged.py`, 32 tests. Both P0 fixes were mutation-tested
with the mutation's *application* verified before the result was trusted:

| mutant | result |
| --- | --- |
| `_MIN_LOG_IQR_DISTANCE = 0.0` (gate disabled) | **10 failures** |
| `decimal_shift` re-added to the allowlist | **2 failures**, including the no-write invariant |

The new module also guards its own fixture (`test_the_generated_column_really_is_wide`), because a
fixture that silently narrowed would repeat exactly the oracle's mistake.

## The consequence nobody had stated: no premise means no write

While fixing this it emerged that **`decimal_shift` was the only deterministic repairer that
proposed anything without a declared schema.** `type_mismatch`, `fd_violation` and
`missing_value` all return `None` absent a declared type or dependency. So with `decimal_shift`
correctly removed from the bypass, **the schema-free auto-apply path is now empty**.

### Correction, 2026-08-25: that was false when written, and is true now

The sentence above was wrong on its own terms, and this document contradicted itself 115 lines
earlier by recording schema-free deterministic floors of 1 on `hospital_10rows.csv` and 2 on
`dirty.csv` -- both from `type_mismatch`.

The error is in the middle clause. `type_mismatch` does **not** return `None` absent a declared type:
it discards the schema entirely on its first line (`del schema`, `dataforge/repairers/type_mismatch.py`:44)
and fires on a sentinel value in a mostly-numeric column with no premise at all. Decision-table row 6
wrote `'N/A' -> ''` with `schema_text=None`, asserted against bytes on disk, for as long as this
paragraph claimed the path was empty. `specs/SPEC_autoapply_decision.md` recorded the same floor of 1
and even explained it occurred *"only because it has no declared schema"*.

**It became true on 2026-08-25.** `type_mismatch` was removed from
`CONSTRAINT_CHECKABLE_DETECTORS` after measurement found 156 flags and **zero** proposals across
hospital, rayyan and flights, so no committed evidence of a real write existed -- see
`docs/trust/bypass-allowlist-evidence.md` and `eval/preregistration/bypass_allowlist_evidence.md`.
Row 6 now holds. Nothing in the product writes without a declared premise.

Worth stating plainly because it is the more useful lesson: this claim was **not** caught by any test,
gate or review for weeks. It was caught by measuring the detector it depended on. A sentence asserting
that a path is empty is unfalsifiable unless something counts what travels down it, and nothing did.

That is the correct fail-closed behaviour -- no declared premise, no proof, no write -- and it is
what `PRODUCT.md` already implied. But it must be stated rather than discovered, because it makes
several existing tests **vacuous rather than merely failing**: INV1 ("a correct cell is never
changed") and INV2 ("a changed cell holds ground truth") are both satisfied by changing nothing.
`TestSchemaFreeApplyWritesNothing` and `TestDeclaredPremiseStillAutoApplies` exist to make that
invariant explicit and falsifiable instead of silently trivial.

## Known limitation, not fixed here

A held fix is **invisible in the `repair` view**. `fixes=0, failures=0` while `issues_count=1`:
the finding survives and `dataforge profile` still shows it, but the proposed value is dropped
from the repair result. This is **pre-existing** for the LLM path (every LLM fix is held under
`enabled_classes == []`) and my change routes `decimal_shift` into the same gap. Recorded rather
than half-fixed.

## Outstanding: 13 fixtures encode the old behaviour

The release gate passes (**exit 0**) and 1,800 tests pass. These 13 fail because their fixtures
depend on `decimal_shift` auto-applying, and several fail *vacuously* -- which is itself the
finding, since it shows those tests only ever exercised this one path:

- `tests/unit/test_agent_gate.py` (2) -- `floor_fix_count=0, agent_fix_count=1`. The
  deterministic floor genuinely moved; the bundled `hospital_10rows.csv` fixture's only floor fix
  was `rating` 45.0.
- `tests/unit/test_certificate.py` (2) -- `test_tampered_receipt_hash_is_detected` now passes
  verification because there is no applied fix to tamper with. **Vacuous, not merely red.**
- `tests/integration/test_surface_uniformity.py` (2), `tests/unit/test_engine_repair.py` (2),
  `tests/unit/test_cli_repair.py` (1), `tests/unit/test_cli_watch.py` (1),
  `tests/unit/test_table_store_patch_plan.py` (1), plus 2 others.

Each needs the same migration already applied to `tests/unit/test_cli_repair.py` and
`tests/property/test_triage_cannot_mutate.py`: supply a **declared functional dependency** so the
fixture exercises a write the product stands behind. `_write_premised_repairable` is the pattern.
