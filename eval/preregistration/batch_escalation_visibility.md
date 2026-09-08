# Pre-registration: a volume-capped batch must be held for review, not discarded

- **Registered** 2026-09-08, **before** the change was written.
- **This pre-registers a SHIPPED BEHAVIOUR CHANGE**, not a measurement. Most documents in this
  directory measure; this one alters what a receipt contains, so it needs predictions and a
  falsifiable safety criterion before rather than after.
- **Amendments are appended, never edited.**

## Why this exists

[`fd_repair_yield_mechanism.md`](fd_repair_yield_mechanism.md) AMENDMENT 3 established that on
hospital, under a declared premise, **152 verified and ground-truth-correct repairs** are discarded
because the batch exceeds a 100-cell cap, and that the discard is **total and silent**:

```
dataforge/engine/repair.py:1932-1934
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        accepted_fixes = []
        reason = batch_safety.reason
```

The fixes reach neither `result.fixes` nor `receipt.suggested_fixes`. A user sees zero repairs
**and an empty review queue**, with the cause available only in `receipt.reason`, which no harness
and no gate had ever read.

**The cap itself is not the defect.** `HIGH_VOLUME_CELL_BUDGET = 100`
(`dataforge/safety/constitution.py:156`) refusing to rewrite more than 100 distinct cells
unattended is defensible, is a `soft_require_confirm` tier rule, and is reversible with
`--confirm-escalations`. The defect is that **exceeding it drops proven work on the floor** where
every other gate in this pipeline routes what it will not auto-apply into `suggested_fixes` with a
structured `review_reason`. Eight such routes already exist in `verify_and_apply` alone.

## Hypothesis

**H7.** Capped fixes can be surfaced for review without changing what is written. The batch gate's
purpose is to prevent an unattended *write*; suppressing the *report* is incidental to that and
costs a user the ability to see, review, or act on 152 proven repairs.

## What changes

Two sites, both in `dataforge/engine/repair.py`, both following the established
`_suggestion_candidates(...)` shape:

1. `run_repair_pipeline` (~line 1932) — capture `accepted_fixes` before emptying it.
2. `verify_and_apply` (~line 2286) — capture `auto` before emptying it. This is the path the
   hosted playground uses.

The `review_reason` is **not** a new vocabulary member. `evaluate_batch`
(`dataforge/safety/filter.py:162-190`) returns exactly two non-ALLOW verdicts, and both already
have exact reason codes in `ReviewReason`, in `REVIEW_REASON_HUMAN`, in the generated TypeScript
projection, and in `specs/repair_attestation.schema.json`'s enum:

- `SafetyVerdict.DENY` → **`safety_denied`** (`NO_CONFLICTING_CELL_WRITES`, `hard_never`)
- `SafetyVerdict.ESCALATE` → **`safety_escalation`** (`NO_HIGH_VOLUME_AUTO_APPLY`, soft)

Inventing `high_volume_escalation` would require editing the vocabulary, regenerating the
TypeScript, and extending the attestation schema enum — the exact multi-file hand-copy path whose
drift `dataforge/engine/repair.py:217-219` already records ("the humanizer once carried 12 of 13
reasons, so a held fix rendered as a raw machine token to a user"). The specific cause travels in
`verifier_reason`, which carries `batch_safety.reason` verbatim. That is the existing division of
labour between a machine-parseable category and a human-readable detail, and it is respected.

## Predictions

- **P1.** On hospital with the frozen declared premise and no `--confirm-escalations`,
  `receipt.suggested_fixes` grows from **4** to **more than 100**, every added entry carrying
  `review_reason == "safety_escalation"`.
- **P2.** `verifier_reason` on those entries contains **`NO_HIGH_VOLUME_AUTO_APPLY`**, so a reader
  of the receipt alone can tell why, without access to `receipt.reason`.
- **P3.** `len(result.fixes)` stays **0** on that arm. The write set does not move.
- **P4.** The oracle arm is **byte-identical** in every reported field: 54 writes, 4 suggestions. It
  never escalates, because 54 is under the cap, so the change must not touch it at all.
- **P5.** No existing test changes status. The risk survey found no test asserting
  `suggested_fixes == []` in a high-volume scenario; the three that assert it do so where the batch
  is empty for unrelated reasons.

## Kill criteria

- **K1 — write-set identity. This is the criterion the change lives or dies by.** Re-running
  `scripts/bench/measure_fd_repair_yield.py` must reproduce **every** arm's
  `pipeline_actual_writes` unchanged: declared **0**, oracle **54**, and **0** for the three
  remaining arms. `scripts/ci/anchor_truth.py` must still match on `tp`/`fp`/`fn`/F1 for both
  corpora. **If any write count moves by a single cell, the change is reverted**, because a
  visibility fix that alters what is written is not a visibility fix.
- **K2 — no new vocabulary, no schema edit.** If the change turns out to require a new
  `ReviewReason`, a regenerated `vocabulary.generated.ts`, or an edit to
  `specs/repair_attestation.schema.json` or the attestation vectors, **stop and report** rather
  than regenerate. Attestation vectors are frozen evidence and must not be regenerated to
  accommodate an instrument.
- **K3 — the cap is not touched.** `HIGH_VOLUME_CELL_BUDGET` stays 100, the constitution is
  unedited, and no default flips. This change makes a refusal *visible*; it does not make it
  *weaker*. A commit that quietly raised the budget while claiming to improve reporting would be
  the worst possible outcome here.
- **K4 — no auto-apply path may consume the new entries.** The risk survey traced all nine
  consumers of `suggested_fixes` (CLI panel, playground API and four frontend readers, MCP
  response, attestation `held[]`, bench instrumentation) and found every one read-only. If any
  consumer is found to write from `suggested_fixes`, this change is abandoned.
- **K5 — anti-motivated-stopping.** Reported even if it shows the receipt now carries an
  inconveniently large held list, or if the added entries reveal further gates nobody had noticed.

## Scope, stated so it cannot expand quietly

- **Two sites only**, both in `dataforge/engine/repair.py`. The identical discard also exists at
  `dataforge/agent/controller.py:446-452` and `dataforge/stores/repair.py:115-117`. Those are **not
  changed here** and are recorded as a known remaining instance, because the agent controller and
  the store have their own contracts and neither is on the `dataforge repair` or playground path.
  Naming them is how the next session finds them.
- No change to the cap, the constitution, the safety filter, any default flag, the receipt's
  `schema_version`, or the attestation surface.
- hospital only for verification. `beers` remains excluded by the dataset-scope rule; `tax` stays
  untested because a head slice is not a sample.

## AMENDMENT 1 (2026-09-08): every prediction held, and K1 is satisfied

**Recorded after the change was made and verified. Nothing above is edited.**

### The measured effect, on the frozen declared premise

| | before | after |
| --- | --- | --- |
| `result.fixes` | **0** | **0** (unchanged, K1) |
| `receipt.suggested_fixes` | 4 | **156** |
| of which `safety_escalation` | 0 | **152** |
| naming `NO_HIGH_VOLUME_AUTO_APPLY` in `verifier_reason` | 0 | **152** |

- **P1 HELD.** 4 → 156, with 152 carrying `safety_escalation`.
- **P2 HELD.** All 152 name `NO_HIGH_VOLUME_AUTO_APPLY`, so the receipt alone explains the refusal.
- **P3 HELD.** `result.fixes` is still 0. The write set did not move.
- **P4 HELD.** The oracle arm is unchanged: 54 writes, 4 suggestions, 0 cap-related entries. It never
  escalates, and the change did not touch it.
- **P5 HELD.** No existing test changed status: **2833 passed / 9 skipped**, against 2832 before,
  the +1 being a newly added test.

**152 is exactly the number `--confirm-escalations` writes** (`fd_repair_yield_mechanism.md`
AMENDMENT 3), which is the consistency check that matters: the fixes now surfaced for review are
precisely the ones the flag releases.

### Kill criteria

- **K1 — write-set identity: SATISFIED.** Declared 0, oracle 54, unchanged. `anchor_truth`
  re-measured both corpora and matched on `tp`/`fp`/`fn`/F1.
- **K2 — no new vocabulary, no schema edit: SATISFIED.** `safety_escalation` and `safety_denied`
  already existed in `ReviewReason`, in `REVIEW_REASON_HUMAN`, in the generated TypeScript, and in
  `specs/repair_attestation.schema.json`'s enum. The attestation schema check reports it still
  current; no vector was regenerated.
- **K3 — the cap is untouched: SATISFIED.** `HIGH_VOLUME_CELL_BUDGET` is still 100, the
  constitution is unedited, and no default flipped.
- **K4 — no consumer writes from `suggested_fixes`: SATISFIED** by the survey of all nine consumers
  before the change.

### What this did NOT fix, named so it is findable

The identical silent discard remains at `dataforge/agent/controller.py:446-452` and
`dataforge/stores/repair.py:115-117`. Neither is on the `dataforge repair` or playground path, and
both have their own contracts, so they were left alone per the scope section rather than changed
opportunistically.
