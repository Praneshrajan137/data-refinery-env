# Authority is mutable, and it is per-column

Two facts about `proven` that a reader of the word would not guess, and one live defect that
followed from them.

## Fact 1: the boundary between proven and unproven is a user-writable file

A fix is `proven` when it is deterministic, or when it was verified against an *authoritative*
schema. "Authoritative" means the declared schema **plus every accepted candidate in the
constraints review artifact** (`merge_schema_with_reviewed_constraints`,
`dataforge/engine/repair.py`).

That artifact is a file on disk, rewritten in place by
`write_constraint_review_artifact_atomic` (`dataforge/schema_inference.py:322`). So the set of
facts the prover treats as axioms is **user-mutable**. Accepting a candidate does not merely
record an opinion; it changes what the verification layer will subsequently call proven.

This is intended — `dataforge constraints review` is a deliberate human-in-the-loop step, and a
tool that could not be told domain truth would be useless. It is documented here because the
product vocabulary ("proven", "certificate") suggests a fixed axiom set, and the axiom set is
neither fixed nor tool-controlled. It is the same shape of problem as
[constraint-circularity.md](constraint-circularity.md): there, inferred constraints justify a
repair; here, accepted constraints justify a *strength label*.

Practical consequence: anyone who can write the constraints artifact can promote future
untrusted fixes to `proven`. Protect that file like a config, not like a cache.

## Fact 2: authority is scoped per column — since 2026-08-09

It was not. `authoritative_schema_present` was a table-level boolean, and that was a live
defect, verified end to end with a real write to disk.

**Reproduction (pre-fix).** No declared schema. Accept exactly ONE inferred `column_type`
candidate, on column `id`. The effective schema becomes `{"id": "int"}` with no other
constraints, so the boolean flips to `True`. Then submit an external fix setting column `city`
to `ZZZ_GARBAGE`:

| Constraints accepted | Result |
| --- | --- |
| none | held — correct |
| one `column_type` on `id` | **applied to disk, labelled `proven`** |

Three things made it serious:

- **No exotic configuration.** No LLM, no agent, no unusual flags — `verify_and_apply`, which
  ships as both a CLI command and an MCP tool, plus one accepted constraint.
- **No calibration backstop.** `external` is not in `_LLM_PROVENANCE`, so a fix labelled proven
  there auto-applies immediately rather than waiting on a per-class threshold.
- **The certificate said `proven`.** That is a truthfulness violation, not merely an unproven
  write, and truthfulness is the product claim.

Root cause: the flag was a *table-level summary of per-column evidence*. Narrow evidence granted
blanket authority. A schema that declares one column's type says nothing whatsoever about any
other column, but the boolean could not express that.

**Fix.** `authoritative_columns(schema)` returns the columns a schema actually constrains — its
declared types plus every column named in any constraint, with a functional dependency covering
both its determinant and its dependent. Strength is then decided per fix, for the fix's own
column, by `strength_for_fix`. The `PatchPlan` carries `authoritative_columns` so the warehouse
primitive can make the same per-column decision from the plan alone.

Pinned by `tests/unit/test_column_scoped_authority.py`, both directions: a fix on an uncovered
column is held, and a fix on the covered column still applies. Mutation-verified — restoring
table-level authority makes the test fail by writing garbage to `city`.

## What is still not guaranteed

- Covering a column is not the same as constraining the *value*. A schema that declares
  `city: str` covers `city`, so a `str` value there is proven against a constraint that almost
  nothing can violate. Column scoping removes blanket authority; it does not make a weak
  constraint strong. The strength of a proof is still the strength of its premise.
- Accepting a constraint is still an authority-granting act with no second signature. There is
  no separation of duties between the person who accepts a constraint and the person who runs
  the repair.
- `authoritative_columns` is a static read of the schema object. It does not evaluate whether
  the SMT encoding of a given constraint actually discharges an obligation for a given fix; a
  constraint the verifier cannot encode still counts as coverage.
