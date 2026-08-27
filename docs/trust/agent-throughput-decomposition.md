# Agent throughput: two gates, and the published number measured neither

**Status**: measured 2026-08-27. Artifact: `eval/results/agent_throughput_decomposition.json`.
Reproduce with
`python scripts/bench/measure_agent_throughput_decomposition.py --artifact eval/results/agent_throughput_decomposition.json`.

## Why this was measured

`docs/STRATEGY.md` recorded that a frontier agent proposer through the gate yields **0 fixes
pass -- every FIX rejected by SMT+safety**, and named autonomous agents the primary wedge in the
same document. Those two statements together say the product's declared growth market gets
nothing through its gate, and that the verification layer is what stops it.

That reading was about to drive a redesign. Before spending it, the claim needed checking --
which is the discipline this repo already applies to its own numbers, applied here to a sentence
rather than a metric.

## What the code says

Two things, both verifiable by reading:

1. **Safety runs before verification and returns.** `dataforge/agent/executor.py` evaluates the
   safety constitution and returns an `ActionOutcome` with `accepted=False` *before* the SMT
   verifier is reached.
2. **The rule that fires inspects only the fix's origin.** `NO_UNCONFIRMED_LLM_WRITE`'s predicate
   in `dataforge/safety/constitution.py` tests `provenance` against a set of literals. It never
   looks at the value, the declared premise, or any constraint. Its tier is
   `soft_require_confirm`, gated on `confirm_escalations`, which defaults to `False`.

And the reproduction command committed alongside the published figure --
`eval/results/agent_gpt56sol_hospital.json` -- does not pass `--confirm-escalations`.

So the published refusal was a **policy default keyed on a label**, and the verifier was almost
certainly never consulted. The artifact does not record a per-fix reason, so that last step is a
strong inference rather than a reading; the measurement below is what settles it.

## What was measured

A deterministic external proposal through the shipped `verify_and_apply`. Not an LLM arm: a model
confounds *the gate refused* with *the model proposed badly*, and the question here is only about
the gate. Not a local reimplementation of the write loop either -- in this repo a shorter local
loop is evidence against a measurement rather than for it, so the script imports the shipped
entry point.

The fixture is `dataforge/fixtures/premised_fd_10rows.csv`, whose single repairable cell
(`bostonn` -> `boston` on row 10) is determined by a declared dependency `state -> city`.

| arm | premise | confirmed | proposed | observed |
| --- | --- | --- | --- | --- |
| `no_premise_unconfirmed` | none | no | `boston` | `safety_escalation` |
| `no_premise_confirmed` | none | yes | `boston` | `floor_cannot_verify` |
| `premise_unconfirmed` | declared | no | `boston` | `safety_escalation` |
| `premise_confirmed` | declared | yes | `boston` | **applied** |
| `premise_confirmed_violating_value` | declared | yes | `worcester` | `verifier_rejected` |

Every arm matched an expectation stated in the script before the run.

## The finding

**Agent throughput was never architecturally zero.** Of the five arms, **1** arm wrote.

And **2** arms were refused on the fix's origin label before any verification ran -- including one
whose value was fully provable against a declared premise.

Two independent conditions must both hold, and the published measurement satisfied neither:

- `confirm_escalations` clears `NO_UNCONFIRMED_LLM_WRITE`, a policy rule about *where a fix came
  from*.
- A discriminating declared premise makes the fix `proven` rather than `plausibility_only`.

Neither is sufficient alone, and the 2x2 shows why the distinction matters. `premise_unconfirmed`
refuses a **fully provable** fix on its origin label, because the escalation is checked first.
`no_premise_confirmed` refuses a **confirmed** fix for want of a premise. The two gates are
orthogonal and the ordering hides the second behind the first.

The fifth arm is the reason the fourth means anything. Under identical permissive settings, a
value contradicting the declared dependency is refused at `verifier_rejected`. The gate still
closes; it was never the thing holding agent work back.

## What this does NOT authorise

- **Any claim that an agent proposes good values.** This measures the gate with a hand-written
  correct value. It says nothing about model quality, and the semantic root cause recorded in
  `docs/STRATEGY.md` is untouched: the RAHA residual is not in-table-derivable, and four
  independent attempts to raise correction accuracy still return NO-GO.
- **Flipping `confirm_escalations` to default-on.** That flag currently also gates
  `NO_HIGH_VOLUME_AUTO_APPLY`, so defaulting it on would silently disable a blast-radius guard
  along with the untrusted-write guard. Those two rules protect unrelated things and share one
  boolean; decoupling them is a prerequisite for any default change, not a follow-up to it.
- **A conclusion about the *hosted* agent path.** This exercised `verify_and_apply`, the
  external-proposer surface. `dataforge/agent/executor.py` reaches the same constitution but by a
  different route, and its per-fix refusal reasons are still unrecorded in any artifact.

## What this changes

The correction is to a claim, not to code. `docs/STRATEGY.md`'s agent row now points here instead
of attributing the refusal to SMT.

It also removes the motivation for a redesign that was being scoped: a verdict lattice admitting
"contained and reversible" writes as an alternative to "proven". That design targeted the premise
gate, which is downstream of the rule that actually refused everything -- so it would have
changed nothing observable for an agent while weakening the invariant the fourth arm shows is
already satisfiable. The cheaper and more honest intervention is to separate the two flags and
make the blast-radius budget real.

## What would close the remaining gap

1. Per-fix refusal reasons recorded in the agent artifact, so the hosted path can be decomposed
   the same way rather than inferred from the external path.
2. A measurement of whether a *real* agent, given a declared premise and confirmation, proposes
   values that survive `verifier_rejected` at a useful rate. That is the throughput question that
   actually matters commercially, and it is unmeasured. This document establishes only that the
   ceiling is not zero.
