# Reasoning-preserving SFT from the gpt-5.6-sol teacher: a NO-GO (post-mortem)

An earlier draft proposed a "grounded-rationale" SFT track: keep the frontier
teacher's natural-language reason on each repair, but only when the repair is
functional-dependency grounded, so we distil reasoning without distilling
guesses. It was built, tested, and then **retracted** on adversarial re-review.
This note records why, so the mistake is not repeated. See DECISIONS 2026-07-25.

## The decisive measurement

The grounding gate (`fd_grounding_determinant`) was a hand-rolled single-column
window-unanimity check with **none** of the project's anti-spurious FD guards.
Re-measured against the single source of truth
(`dataforge.verifier.inferred.fd_consensus_violation` over
`infer_verification_schema`, which enforces near-key rejection at 0.9,
`_MIN_FD_SUPPORT_GROUPS=2`, confidence >=0.9, full-table scope):

| Teacher set | repairs | naive window "grounded" | ROBUSTLY grounded (guarded FD) |
|---|---|---|---|
| smoke | 43 | 36 (84%) | **3 (7%)** |
| full | 216 | 170 (79%) | **29 (13%)** |

Reproduce: `python scripts/data/measure_teacher_grounding.py`.

The naive check over-counted grounding by 6-11x. Entity-clustered context windows
(all rows the same provider/city/county) make many columns coincidentally
unanimous - exactly the low-cardinality FDs (`f_name->gender`) that DECISIONS
2026-07-19 ruled in-table-indistinguishable from genuine ones (`zip->city`) and
refused to mine. Moreover, of the cells that DO have a robust unanimous group,
most disagree with the teacher's value (15/18 smoke, 77/106 full): the teacher
followed local coincidence, not a robust functional dependency.

## Three independent reasons it fails

1. **Spurious grounding.** Only 7-13% of teacher repairs are robustly grounded;
   the gate that was supposed to prevent distilling confident-wrongness instead
   admitted it. A second, unguarded FD notion also violates the `inferred.py`
   single-source-of-truth invariant.
2. **Redundancy.** The robust 7-13% overlaps the cells the deterministic FD
   repairer already handles, so there is ~0 marginal repair signal to supervise.
3. **Production mechanism.** The strict v3 constrained decoder rejects a
   `rationale` key, so a rationale-trained model cannot even emit it in the
   product - the training-time channel could only help via internal computation,
   a far weaker mechanism than claimed.

This is the fourth NO-GO with the same root cause (DECISIONS 1282-1293): the RAHA
residual is semantic, and in-table local signal for it is indistinguishable from
coincidence.

## What was kept

- **Phase A reliability** (unrelated to this NO-GO): Azure timeout-retry and
  `DATAFORGE_AZURE_TIMEOUT_S` on the agent path.
- **`scripts/data/promote_expert_v1_to_v4.py`**, reframed: it produces
  *verified-abstention hard-negative* v4 records (correct "finish / no-repair"
  examples on cells a small model should not guess), using the real guarded
  classifier - not repair supervision.
- **`scripts/data/measure_teacher_grounding.py`**: the reproducible evidence.

## The real frontier (not a bigger LLM)

Certified auto-apply coverage grows only where a value is provably derivable -
which the deterministic floor already covers. The remaining honest levers are
declared conventions / authoritative reference data (e.g. a governed zip->city
table) and calibration research. A stronger teacher does not provide either;
gpt-5.6-sol's durable role is the legible-guardrail demo (the Phase C playground
proposer), where a frontier model proposes and the verifier disposes.
