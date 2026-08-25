# Pre-registration: a premise must discriminate before it can confer proof

Written 2026-08-25, **before** any code change. Nothing below is edited afterwards; results and
deviations are appended as amendments.

## The finding this responds to

Two measurements, taken independently, show the same defect at two levels of the write gate.

`eval/results/trust_ledger_adversarial.json`: under a tight premise **0 of 14** constraint-violating
attacks were written; under a premise declaring every column `str`, **10 of 14** were written. The
gate is identical in both runs and `DECISIONS.md`:540 records that both premises cover every column,
so **every write was labelled `proven` under both**.

`eval/results/deductive_coverage_hospital.json`: under a premise whose dependencies all hold, the
label-free FD repairer corrupted **0** clean cells; under the product's **default** mined premise it
corrupted **86**, a harmful write rate of 0.1601. Every sampled corruption traces to a mined
dependency that is false. Both sets of writes carry `deterministic` provenance, which
`partition_auto_apply` lets bypass calibration entirely.

The common cause is a naming defect. `verification_strength_for` returns `proven` when
`is_trusted_provenance(provenance) or authoritative_schema_present`, and `authoritative_schema_present`
means only that the column **appears** in the schema. A column declared `str` with no other
constraint appears in the schema and constrains nothing. So `proven` currently names the presence of
a premise rather than its power to reject anything.

## The change, fixed now

**A column is `discriminating` under a schema iff at least one of:**

1. its declared type is not `str` -- a non-string type genuinely narrows the value space;
2. it carries a regex constraint;
3. it carries accepted values;
4. it carries a domain bound;
5. it is in `not_null_columns`, `unique_columns` or `primary_key_columns`;
6. it is the dependent of a **declared** functional dependency.

A column declared `str` with nothing else is **non-discriminating**, and the schema's mention of it
must not confer `proven`. `authoritative_schema_present` becomes "this column is discriminating"
rather than "this column is listed".

Two further changes, each justified by its own measurement:

* `FDViolationRepairer._deterministic_choice` implements the **strict majority** its docstring
  already claims, replacing the plurality.
* An FD-derived repair may bypass calibration only when the acting dependency was **declared**. A
  dependency that was mined is a hypothesis, and a hypothesis is not a premise.

## Predictions

| # | Quantity | Prediction |
| --- | --- | --- |
| **P1** | `trust_ledger_adversarial`, tight, discriminable | `cells_applied` **1**, `corruptions` **0** -- UNCHANGED. The one legitimate repair targets `score`, which is `float` with a 0-100 bound under the tight premise and therefore discriminating. |
| **P2** | `trust_ledger_adversarial`, permissive, discriminable | `corruptions` **10 -> 0**, everything held. Every column is bare `str`, so nothing is discriminating. |
| **P3** | `test_autoapply_decision_table` | All 9 rows unchanged, including all 3 write rows. Every write there is `deterministic` provenance, which this change does not touch. |
| **P4** | `deductive_coverage_hospital`, majority rule | Byte-identical to the plurality rule: 393 writes, 393 repaired, 0 corrupted. Measured `plurality_only_not_majority` is 0 on hospital. |
| **P5** | `deductive_coverage_flights`, majority rule | Harmful writes **1433 -> 614** (wrong 702->270, corrupted 731->344); net cells improved **+404 -> +579**. |
| **P6** | `deductive_coverage_hospital`, mined premise, after the bypass change | The 86 corruptions are no longer auto-applied. Coverage from *declared* dependencies is unaffected. |

## Kill criteria, fixed now

* **If P2 fails** -- permissive corruptions do not reach 0 -- the discriminating-constraint model is
  the wrong account of the 10-of-14 result. Revert the change; do not weaken the definition until it
  passes.
* **If P1 fails** -- the tight premise loses its one legitimate write -- the definition is too strict
  and the verification-layer feature is destroyed rather than corrected. Revert. Do **not** relax the
  test to accept the loss.
* **If P3 fails** -- any decision-table row flips -- the taxonomy change has leaked into the
  deterministic path it was scoped to leave alone. Revert.
* **If P4 fails** -- hospital changes at all under the majority rule -- then the measured
  `plurality_only_not_majority = 0` is wrong and the whole measurement is suspect, not just this
  change.

## What is deliberately not being done, and why

**Unanimity is rejected on evidence, not on taste.** The plan that produced this document proposed
requiring every other row in the determinant group to agree, on the grounds that it is the only rule
under which premise plus data entail a unique value. Measured first: on hospital-oracle it halves
coverage (0.7721 -> 0.3517) **and introduces 3 corruptions where the shipped rule had none**; on
flights it proposes nothing. Plurality and majority count the target cell's own value and unanimity
excludes it, so when a group is split the cell's own vote is what prevents a confident overwrite.
Counting the cell's own value is a safety property. The change is not made.

**No new threshold is invented.** The `discriminating` definition is a disjunction over constraints
the schema already carries. Introducing a tunable (say, minimum selectivity) would make the gate's
verdict depend on a number nobody measured, which is the defect being fixed.

## Scope

**Does not** touch `is_trusted_provenance` or `TRUSTED_PROVENANCE`. Deterministic provenance keeps
conferring `derived`; that path's weakness is premise provenance, addressed separately by the mined
dependency change.

**Does not** claim the resulting gate is sound. It claims the gate stops labelling a premise powerful
when it is not, and the two measurements above give the before-and-after numbers that show whether
that claim holds.
