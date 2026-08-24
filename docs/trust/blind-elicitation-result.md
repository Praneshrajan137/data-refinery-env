# Blind elicitation: VOID, and the reason it voids is the finding

Measured 2026-08-24. Probe: `scripts/bench/probe_label_protocol.py`.
Artifact: `eval/results/blind_elicitation.json`. Tests: `tests/unit/test_blind_elicitation_probe.py`.
Pre-registration: `eval/preregistration/blind_elicitation.md` (Amendment 1 records the outcome).

Labeller gpt-5.6-sol, corpus rayyan, 72 controls, 237 calls, **$0.0891**.

## Verdict

**VOID on the pre-registered capability floor.** No directional claim is made.

| quantity | value | 95% Wilson |
| --- | --- | --- |
| `beta_ratify` (shown the proposal, asked if correct) | 0.9583 | 0.8845 - 0.9857 |
| `beta_elicit` (asked blind, scored against the proposal) | 0.8194 | 0.7152 - 0.8913 |
| `elicited_matches_truth` -- **the capability control** | **0.0278** | floor was 0.10 |
| `elicited_matches_neither` | 0.1528 | |

`beta_elicit` came out below `beta_ratify`. **That is not reported as evidence for blind
elicitation**, because the pre-registration fixed in advance that a capability rate under 0.10
voids the arm regardless of which way the betas fall. Without that clause this run would have
looked like a clean win for the protocol H2 depends on.

## The finding: blind elicitation needs an independent labeller, and that was never stated

The ELICIT arm reproduced the corrector's **exact wrong value on 81.94% of items.**

That is the whole result. Asked blind, with the proposal never shown, the labeller walked into the
corrector's own error four times in five. Revealing the proposal after the labeller commits cannot
protect against a labeller who would have produced the same wrong value anyway.

`eval/preregistration/human_label_noise.md` specifies H2 in terms of **when** the proposal is
revealed. It says nothing about **who** labels. On this evidence the independence of labeller from
corrector is load-bearing and belongs in the protocol definition next to the ordering condition.

This also closes off a tempting shortcut: **an LLM cannot stand in for the human labeller in this
protocol**, and not merely because it is not a human. When the corrector is a model and the labeller
is the same model, their errors are correlated, correlated errors push `beta` **down**, and the
resulting bound is anti-conservative in exactly the direction that killed the pooled bound.
`label_source="llm_probe"` now makes `certify_session` refuse outright rather than leaving that to
prose.

## A selection artifact in my own design, stated plainly

Measuring `beta` requires conditioning on the proposal being wrong -- that is what `beta` is defined
on, and it is not optional. But the proposals were harvested with the **same model, prompt and
effort** the ELICIT arm then uses as its labeller. So "the proposal is wrong" and "the ELICIT
labeller already got this wrong" are the same condition.

The ELICIT arm was therefore run only on items it had already failed, and could succeed only by
changing its mind. **The 0.0278 capability figure is the arithmetic consequence of that, not a
measurement of the labeller's ability.** The corrector answered 21 of 93 sampled cells correctly
(22.6%) using that identical prompt; the 72 that entered the probe are precisely the residue where
it did not.

The instrument is wrong, not merely underpowered. So this probe is **not** re-run at a larger sample
or a different threshold -- that would only buy a tighter estimate of an uninformative quantity.

## What is measured and does stand

* **An LLM ratifier accepts 95.83% of wrong proposals it is shown** (0.8845 - 0.9857). For scale,
  the human `corrector_generated` rate was 0.5000 at n=8. This is a measurement of a model asked
  "is this right?" about its own output, and it is close to unconditional assent.
* **Self-consistency is 81.94%** on cells the model gets wrong: re-asking the same question returns
  the same wrong answer. Relevant well beyond this probe -- it bounds what any
  resample-and-compare, self-consistency or self-critique scheme can detect on this corpus.
* **The corrector answered 22.6% of rayyan real errors correctly** (21 of 93) at
  `reasoning_effort="none"` with eight column-context values. A cheap, honest baseline for the
  corrector axis on natural errors.

## What this does not change

No write gate moves. Every corrector class still ships at the unreachable 1.01 threshold. No
detector joins `CONSTRAINT_CHECKABLE_DETECTORS`. Human-labelled per-table certification at
`alpha = 0.05` remains dead per `stratified-label-noise-result.md`; this probe was an attempt to
reopen it and **did not**.

The H2 route is not refuted. It is **unmeasured**, and now carries a stated precondition it did not
have this morning: the labeller must be independent of the corrector. A human labeller satisfies
that by construction, which is why H2 remains the live route -- and why it cannot be simulated
cheaply with the corrector's own model.

## Limits

1. **One model.** Labeller-corrector independence is unreachable with a single deployment, so the
   valid version of this experiment could not be run, only designed.
2. **One corpus.** rayyan is the strongest available (natural, owner-cleaned), but 72 controls from
   8 columns is one corpus and one sampling seed.
3. **`reasoning_effort="none"`** for both arms, fixed so the contrast is not confounded by effort.
   A higher effort would plausibly raise capability and is untested here.
4. **The three-way ELICIT scoring is exact string matching** after case-folding and whitespace
   normalisation, fixed in advance. A semantically-correct-but-differently-formatted answer scores
   as `neither`, which understates capability.
