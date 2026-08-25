# SPEC: The auto-apply decision

Status: **normative**. Measured 2026-08-22 on `data_quality_env` @ working tree.
Executable counterpart: `tests/integration/test_autoapply_decision_table.py`.
Every row below is asserted there against **bytes on disk**, on **both write surfaces**.

## Why this document exists

The auto-apply decision has three axes — **provenance**, **premise**, and **detector
class** — and until 2026-08-22 they were specified in three different files and composed
nowhere:

| Axis | Where it was specified |
| --- | --- |
| provenance | `dataforge/domain/vocabulary.py` (`verification_strength_for`) |
| premise | `dataforge/engine/repair.py` (`authoritative_columns`, `strength_for_fix`) |
| detector class | `dataforge/domain/vocabulary.py` (`CONSTRAINT_CHECKABLE_DETECTORS`) |

No artifact stated what happens for a given *combination*. The consequences were not
hypothetical:

1. **The agent bypass.** `partition_auto_apply` gained the detector-allowlist check; the
   agent controller never calls `partition_auto_apply`. Measured: `run_agent_repair`
   rewrote `4,1020` to `4,102` with no schema while `run_repair_pipeline` refused the
   identical fix on the identical table. The strength gate did not catch it because
   `verification_strength_for("deterministic", …)` is `proven` regardless of schema.
2. **Thirteen test fixtures encoded the wrong assumption.** Their authors reasonably
   believed declaring a schema would rescue a `decimal_shift` write. It does not.
3. **Six tests kept passing while proving nothing**, because "no write happened" is
   silently compatible with most assertions written to check that a write happened
   correctly.

A truth table cannot prevent a missing row from being *wrong*, but it makes the row
*visible*, and an executable one makes it *falsifiable*.

## Vocabulary

**Premise** — an authority supplied from *outside* the data: a declared schema, a
declared functional dependency, a declared not-null constraint. A pattern inferred from
the column's own contents is *not* a premise, however reliable it looks.

**Constraint-checkable detector** — one whose proposed value can be checked against a
reference rather than against the shape of the column's own distribution. This is an
**allowlist** (`CONSTRAINT_CHECKABLE_DETECTORS`), never a denylist: a denylist fails
open, so a detector nobody classified would inherit write access by default.

**Disposition** — the observable outcome, of which there are exactly three:

| Disposition | Meaning | Observable |
| --- | --- | --- |
| `WRITE` | The product stands behind the value and applies it | bytes change |
| `HELD` | A value was proposed and deliberately not applied | bytes unchanged, fix surfaced for review |
| `ABSTAIN` | No value could be proposed at all | bytes unchanged, no candidate |

`HELD` and `ABSTAIN` are distinguished because they carry different obligations: a `HELD`
fix must appear in the review queue, an `ABSTAIN` has nothing to show. They are
deliberately *not* distinguished by the byte assertion, because the safety property is
the same and asserting the byte outcome is what makes the row falsifiable.

## The decision table

Measured, not intended. `legacy` = `run_repair_pipeline`; `agent` =
`run_agent_repair(policy="deterministic")`. **The two surfaces agree on every row** —
that agreement is itself asserted, and is the property whose absence was the bypass.

| # | Detector | Provenance | Premise | Disposition | Why |
| --- | --- | --- | --- | --- | --- |
| 1 | `fd_violation` | deterministic | declared FD | **WRITE** | Value determined by an operator-declared dependency |
| 2 | `fd_violation` | deterministic | none | ABSTAIN | Repairer returns `None` without a declared FD — it will not infer one |
| 3 | `missing_value` | deterministic | declared FD | **WRITE** | Fill derived from the declared dependency |
| 4 | `missing_value` | deterministic | `not_null` only | ABSTAIN | Knowing a value is required does not say what it is |
| 5 | `missing_value` | deterministic | none | ABSTAIN | `schema is None` → immediate abstain |
| 6 | `type_mismatch` | deterministic | none | **HELD** | Not constraint-checkable as of 2026-08-25. Removed from the bypass allowlist for lack of evidence: 156 flags and **zero** proposals across three corpora |
| 7 | `type_mismatch` | deterministic | declared `integer` | **HELD** | The proposed `''` would violate the declared type, so the verifier refuses |
| 8 | `decimal_shift` | deterministic | none | **HELD** | Not constraint-checkable |
| 9 | `decimal_shift` | deterministic | declared `float` | **HELD** | Not constraint-checkable. **A schema does not rescue it** |
| 10 | any | `llm_cache` / `llm_live` | any | HELD | `NO_UNCONFIRMED_LLM_WRITE`; separate gate |

### Rows 6 and 7 after 2026-08-25: both held, for different reasons

Row 6 used to **write**, and the pairing with row 7 was the interesting case: `type_mismatch` wrote
*without* a schema and was held *with* one. That inversion was checked rather than assumed and found
sound in the opposite direction:

- With no declared type, `'N/A' → ''` violates nothing that was declared.
- With `age: integer` declared, `''` is not an integer, so the declared premise *catches that the
  repair produces an invalid value* and the fix is refused.

Supplying a premise can only narrow what gets written. That is the correct direction, and it is the
exact opposite of rows 8/9's relationship to the bypass, where the *absence* of a premise made the
agent looser. Both are recorded here so the next reader does not have to rediscover which inversions
are safe.

**Row 6 now holds too**, because `type_mismatch` was removed from `CONSTRAINT_CHECKABLE_DETECTORS`.
The reason is absence of evidence rather than evidence of harm: measured across hospital, rayyan and
flights -- 4,376 rows and 6,377 real errors -- it flagged 156 cells and proposed on **zero**, so no
committed measurement of a single real write exists. See `docs/trust/bypass-allowlist-evidence.md`
and `eval/preregistration/bypass_allowlist_evidence.md`. The observation above still stands as the
reason row 7 held even while row 6 wrote; it is kept because the inversion argument is what makes the
direction of premise strength legible.

**Consequence, and it is the point.** `dataforge/fixtures/hospital_10rows.csv` used to reach a
deterministic floor of 1 (`'not available' → ''` on `phone_number`) **only because it had no declared
schema**; declaring one reduced that floor to zero. That floor is now zero either way, and the
schema-free auto-apply path is **genuinely empty** -- which is what
`docs/trust/deterministic-is-not-sound.md` already claimed and, until today, was wrong about. Nothing
in this product now writes without a declared premise. The cost is the only zero-configuration write.

## Invariants (asserted, not aspirational)

**I1 — No uncheckable write, on any surface.** No fix with `deterministic` provenance and
a detector outside `CONSTRAINT_CHECKABLE_DETECTORS` changes bytes, via any write path,
with or without a premise. There is no opt-in flag; see I4.

**I2 — Surface parity.** `legacy` and `agent` reach the same disposition on every row.
Divergence is a release-gate failure (`dataforge/release/agent_gate.py`).

**I3 — Defence in depth, and it must be unreachable.** The allowlist is enforced twice:
proactively at each partition point (`partition_auto_apply`, `_is_held`) so surfaces
*hold* and agree, and structurally at the mutation primitives
(`enforce_constraint_checkable_only`, `enforce_plan_constraint_checkable_only`) so a
surface that forgets *raises*. The primitive is the backstop; reaching it from a shipped
code path is a bug in that path, not a safe outcome. It is reachable only by calling
`apply_transaction` directly, which the tests do deliberately.

**I4 — No opt-in.** `allow_unproven_autoapply` exists because an unproven write is a
defensible product choice: the value stays reversible and the certificate records it
honestly as not-proven. An uncheckable-detector write is not defensible at any
confidence, because there is no evidence to record. A flag would be a flag for corrupting
data on request. The value is still surfaced for human review.

**I5 — The gates agree.** For every registered repairer, the partition-point decision and
the primitive decision agree. Asserted by iterating `build_repairers(...)` rather than a
hardcoded list, so registering a new repairer without classifying it fails the suite.

## Scope and known limits

- **Ten rows is not the product.** `entity_consensus` and `external` provenance are
  covered by `test_surface_uniformity.py`, not here.
- **A `HELD` fix is currently invisible in the `repair` view** — it reports
  `fixes=0, failures=0` while `issues_count=1`. Recorded, not fixed; pre-existing for the
  LLM path. The fix is surfaced in the review queue, not the repair summary.
- **`outlier` has no repairer at all**, so it cannot write on any surface and does not
  appear in this table. Its measured precision of 0.0000 on three datasets is a
  review-queue noise problem, not a corruption risk.
- **The table asserts bytes, not receipts.** `receipt.applied` was itself satisfiable
  while no write occurred, which is why the observable is the file.
