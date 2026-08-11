# Write-surface uniformity: where the proven-only invariant actually held

**Status**: closed 2026-08-09. Recorded because the *shape* of the failure recurs, and because
for four weeks this page's claim was true of the pipeline and false of the product.

## The claim, and where it was false

`DECISIONS.md` 2026-07-11 declared:

> only proven fixes auto-apply unless `allow_unproven_autoapply` is set

and justified it as making the verifier-floor gaps stay latent **"under any policy"**.

The invariant was implemented as a call to `_partition_auto_apply` inside
`dataforge/engine/repair.py`. That function had exactly two callers. DataForge had four write
surfaces.

| Surface | Mutation primitive | Gated 2026-07-11 → 2026-08-09 |
| --- | --- | --- |
| `run_repair_pipeline` | `apply_transaction` | Yes |
| `verify_and_apply` | `apply_transaction` | Yes |
| `run_agent_repair` | `apply_transaction` (called directly) | **No** |
| `run_table_store_repair` | `DuckDBStore.apply_patch_plan` (raw SQL `UPDATE`) | **No** |

On the agent surface the failure compounded. `agent/executor.py` called
`SMTVerifier.verify(df, [fix], schema)` with three positional arguments, omitting
`verification_schema`. With `schema=None` the verifier short-circuits to a vacuous ACCEPT — it
confirms the row is in bounds and the column exists, then accepts. So a schema-less `llm_live`
value was written to a user's file with **nothing having examined the value at all**, and was
reported as "verified by the SMT verifier".

On MCP this was the *default* path: `dataforge_agent_repair` defaulted `confirm_escalations=True`
(every other surface defaulted `False`) and hardcoded `schema=None`, so a proven agent fix was not
reachable at all. The only barrier was the server-level `--enable-apply` kill switch, which is an
operator toggle, not a verification gate.

## Why the test suite was green

Two guards each assumed the other covered these surfaces:

- `tests/property/test_no_corruption_invariant.py` asserted it proved the guarantee "for **ANY**
  configuration". It varied the *corrector policy* and only ever called `run_repair_pipeline`.
  "Any configuration" meant any policy, not any surface.
- `tests/integration/test_surface_uniformity.py` explicitly delegated "nothing unverified is
  auto-applied" to that file, and its static allowlist governs only *which modules call
  `apply_transaction`* — it says nothing about whether a caller cleared the value gate, and it
  cannot see `DuckDBStore`'s raw SQL at all, that being a different mechanism.

The static guard was honest about its own limits. The runtime guard was not honest about its scope.
The gap between them was the bug.

## What changed

Enforcement moved *into* the two mutation primitives:

- `enforce_proven_only` at the top of `apply_transaction`
- `enforce_plan_proven_only` at the top of `DuckDBStore.apply_patch_plan`

Both default to the safe behaviour, so a caller passing neither keyword gets proven-only rather than
a silent unproven write. This is the difference between enforcement by **convention** (each surface
remembers to gate) and by **construction** (a surface must pass through a primitive, so it inherits
the gate; one that tries to bypass it raises).

Two supporting properties make that real rather than nominal:

- Strength is **computed** from `provenance` inside the primitives, never read from
  `ProposedFix.verification_strength`. That field is stamped late — often `None` at write time — so
  trusting it would make the gate both unreliable and spoofable by any caller that set it.
- `PatchPlan` carries `authoritative_schema_present`, so the SQL primitive decides from the plan
  alone. Plans persisted before the field existed deserialize to the safe `False`, so an old
  journaled plan cannot be replayed as though it had been proven.

## Mutation evidence

A guard that passes when removed is decorative. All three were killed:

| Mutant | Test that failed |
| --- | --- |
| `enforce_proven_only` → no-op | `test_write_primitive_refuses_any_unproven_value` |
| controller strength filter → always `"proven"` | `test_agent_surface_holds`, **via the primitive raising** |
| `enforce_plan_proven_only` → no-op | 2 tests in `test_table_store_proven_gate.py` |

The second is the informative one: with the controller's filter neutered, the write was stopped by
the primitive rather than landing. The layers are independently effective, which is what
defense-in-depth is supposed to mean and usually does not.

## What is still not guaranteed

- The proven-only gate decides *whether* an unproven value may be written. It does not make an
  unproven value correct, and it does not narrow the inferred-guard gaps enumerated in
  [inferred-guard-gaps.md](inferred-guard-gaps.md). With `allow_unproven_autoapply` set, those gaps
  become live — recorded truthfully in the certificate, but live.
- The advisory inferred guard is bounded by what can be inferred from *dirty* data. A column that
  still contains its dirty value infers as `str`, so it carries no numeric type or domain
  constraint, and a garbage value in that column clears the guard. Pinned by
  `test_dirty_column_infers_as_str_so_the_guard_cannot_constrain_it`. For that case the proven-only
  gate, not the guard, is the defence.
- The playground is safe because `mode` is a hardcoded `"dry_run"` literal at three call sites
  writing into a temporary directory. It now inherits the structural gate as well, but its safety
  still rests on a literal rather than a type. Not changed; recorded.
- The static write-caller allowlist remains a string scan. It did not catch this and will not catch
  the next new mechanism. Being on that allowlist is not coverage.

## Measured exposure: zero, and the severity claim is corrected

The first version of this page ranked the defect above everything in the trust register on the
strength of reachability alone. That was an asserted severity. Measured, it is smaller.

`eval/results/agent_gpt56sol_hospital.json` is a committed run of the verified agent driven by a 2026
frontier model (gpt-5.6-sol) over the first 150 rows of RAHA hospital `dirty.csv`, with
`authoritative_schema_present: false` -- precisely the configuration where the bug was reachable:

| Quantity | Value |
| --- | --- |
| `agent_fix_count` | **0** |
| `agent_proven` / `agent_held` | 0 / 0 |
| agent FIX attempts | 2, both rejected before staging |
| `applied_fix_provenance` | `{deterministic: 6}` |

Exposure on the agent surface equals its staged-fix count 1:1, because with no schema every agent fix
is `plausibility_only` by definition. That count was zero. Two further conditions bound it: an
unproven agent write also needed `--confirm-escalations` (`NO_UNCONFIRMED_LLM_WRITE` covers
`llm_live`), and on MCP it needed `--enable-apply`.

**Limits**: n=1, 150 rows, one dataset, one model, dry-run. This bounds realized exposure at zero for
*that configuration*. It does not prove exposure was zero for every user, and no telemetry exists that
could establish it.

So: a real defect of a serious class, with **no measured realized harm**. Worth fixing because it
prevents the class and because three shipped claims were false -- not because data was being
corrupted.

## What Round 1 of this fix got wrong

Recorded because the pattern matters more than the instances.

**It claimed the wrong primitive count.** The Round 1 fix stated, in `engine/repair.py` and in
`PRODUCT.md`, that the invariant is enforced "inside the two mutation primitives". There are four leaf
write primitives, and three paths to user data bypass the pair called exhaustive. This is the same
scope error the fix existed to correct, committed inside the correction.

**It fixed the less serious invariant first.** `apply_fixes_to_csv` was in the public
`engine.__all__`, shipped under `py.typed`, took `CellFix` (no provenance, so strength is
*undecidable* rather than merely unchecked), and wrote a user's CSV with no journal, no snapshot and no
source lock. A write through it was **irreversible**. Reversibility is a stronger claim in `PRODUCT.md`
than provable-only and it had no surface coverage at all. Round 1 fixed the invariant that had a
decision record and a docs page; it missed the one with a stronger promise and no test. It is now
private (`_apply_fixes_to_csv`) and pinned by `test_raw_byte_writer_is_not_public`.

The generalisable lesson: **the invariant most discussed is not the invariant most broken.** Prioritise
by which promise is strongest and least covered, not by which has the most documentation.

## The registry is keyed by primitive, not by caller

`_WRITE_CALLER_ALLOWLIST` allowlisted the *callers* of one primitive. It failed twice over: it could
see only `apply_transaction`, and it rotted -- it carried `cli/repair.py` on the strength of
`_apply_transaction`, a wrapper with no callers anywhere in the repo.

`_WRITE_PRIMITIVE_REGISTRY` in `tests/integration/test_surface_uniformity.py` replaces it. It scans
every package in the repo for write primitives and requires each site to be classified
`user_data` / `metadata` / `scratch` / `read_only`, with every `user_data` entry naming its gate.
Callers are unbounded; primitives are ~30 and change rarely.

Three properties make it more than a longer list:

- **Bidirectional.** An unregistered write fails, *and* a registry entry whose write no longer exists
  fails. The predecessor could rot silently; this cannot.
- **Repo-wide.** It covers `dataforge`, `dataforge-mcp`, and all three `packages/`, so a side package
  cannot write outside the registry.
- **It already caught a real miss.** Widening the pattern set surfaced
  `schema_inference.py:333`, which uses `os.fdopen` and was invisible to an
  `.open("wb")`-only scan. That is the alias hole in action, not a hypothetical.

Still a regex scan: a write reached through an alias, `getattr`, or an entry point will not appear.
Being registered proves somebody classified it, not that it is safe.

## The generalisable failure mode: guard delegation without scope

This is the same failure `claim-scope-discipline.md` records: a claim correct about its sample and
false about the world. The three prior instances were prose numbers, remediated by binding prose to
artifacts in CI. This one was a *safety* claim, and no artifact bound it — the test that would have
caught it asserted a scope it did not have.

The generalisable rule: when a guard delegates part of its guarantee to another guard, the
delegation must name what the other guard actually covers. "Enforced elsewhere" is not a scope.
