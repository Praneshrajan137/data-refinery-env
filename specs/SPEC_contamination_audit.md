# SPEC: Contamination audit

Status: **normative**. Adopted 2026-08-24.
Executable counterpart: `tests/unit/test_contamination_audit.py`.
Implementation: `dataforge/bench/contamination.py`, `scripts/bench/probe_contamination.py`.
Pre-registration: `eval/preregistration/contamination_audit.md`.

## The refusal this spec exists to state

> A language model's score on a public labelled benchmark is admissible as a measurement of
> capability only if a converging multi-method audit has failed to detect memorisation of
> that benchmark. Where two or more independent probes detect it, the measurement is
> **cancelled, not caveated**. Where the provable method is unavailable, its absence is
> recorded as a limit and **never** counted as a clean result.

## Why a spec rather than a probe script

The temptation this forecloses is specific and strong: an audit whose result determines
whether expensive work proceeds has an incentive gradient pointing at inconclusiveness. An
inconclusive audit lets the work go ahead. So the conditions under which the audit *stops*
work must be written down before the audit runs, and the stopping rule must not be reachable
by choosing a metric afterwards.

This project has already made the analogous mistake in the other direction. The retraction
notice in `eval/preregistration/api_phase_certification.md:262-276` records four false
statements in a single amendment, and the reason no guard caught them is stated at `:416-418`:
the guards check artifact *fields* and *numbers*, not claim **scope**. A claim can be
arithmetically correct about its sample and false about the world. A contamination verdict is
exactly a scope claim.

## Provenance of the methods

None of the three is invented here. Each is taken from a published, peer-reviewed method, and
the adaptation to this corpus is stated explicitly so the deviation is visible.

| id | method | source | adaptation made here |
| --- | --- | --- | --- |
| C1 | Exchangeability of benchmark orderings | Oren et al., arXiv:2310.17623 | **Unavailable on this deployment.** `logprobs` is rejected, and chat-completions logprobs cover generated tokens only, so the log-likelihood of a provided ordering is not obtainable. |
| C2 | Guided vs general instruction | Golchin & Surdeanu, arXiv:2308.08493 (ICLR 2024 Spotlight) | Primary metric is exact-value recall over a held-out `dist_val` suffix, not ROUGE-L over prose. Restricted to columns with `>= 20` distinct values. |
| C3 | Testset Slot Guessing | Deng et al., arXiv:2311.09783 (NAACL 2024) | The masked slot is the **column-level error/debatable label**, and recovery is measured as a paired guided-vs-general delta rather than against a base rate. See below. |

### C3's masked slot must be capability-free, and pairing is how that is achieved

This is the load-bearing design constraint, and getting it wrong yields a number that looks
like a contamination measurement and is not one.

Deng et al. mask a *wrong* answer in a multiple-choice item precisely because a distractor
carries no capability signal: no amount of subject knowledge lets you infer which specific
wrong option an annotator wrote. Masking the *correct* answer would measure competence.

Applied to an error-detection benchmark, masking "which value is erroneous" measures error
detection -- the capability under test. The confound is total and unrecoverable. **A
conforming implementation must never mask the identity of the erroneous value.**

The first design here masked the error/debatable partition and compared recovery against a
marginal base rate. That was wrong twice, and both faults were found by measurement before any
run (`eval/preregistration/contamination_audit.md`, Amendment 1):

1. **Degenerate.** Only 3 of 175 labelled columns contain both an unambiguous error and a
   debatable value, so within-column partition is ~98% recoverable by guessing that a column's
   labels are homogeneous. It measured a structural property of the corpus, not memory.
2. **Not capability-free after all.** A value like `"total"` in a numeric column is visibly
   arguable; `"12/31/9999"` is visibly wrong. A capable model infers part of the split with no
   memorisation, and a base-rate comparison would credit that inference to contamination.

**Both are fixed by pairing.** The slot is the *column-level* label, and recovery is measured
as a guided-minus-general delta over identical columns in identical order. Capability is held
constant by construction, because the only difference between the arms is whether the corpus
is named. A conforming implementation must obtain its contamination signal from a **paired
contrast**, never from an absolute score against chance.

## Conditions

| # | Condition | Threshold |
| --- | --- | --- |
| C1 | One-sided p-value, log-likelihood of the canonical ordering against 20 shuffles, per corpus | flags at `p < 0.01`. **Unavailable on this deployment; excluded from the count, never scored as passing.** |
| C2 | Paired one-sided sign-flip permutation test on exact-value recall, guided minus general | flags at `p < 0.01` **and** mean delta `>= 0.05` |
| C3 | Paired one-sided sign-flip permutation test on column-label recovery, guided minus general | flags at `p < 0.01` **and** mean delta `>= 0.05` |
| C4 | C2 and C3 executed against a synthetic never-published corpus of matched shape | **must not flag** |

C2 and C3 each require both clauses. A significant but negligible delta is what a large paired
sample produces from prompt-format asymmetry, and admitting it would let formatting masquerade
as memory.

The paired test is fixed as a **sign-flip permutation test, 20,000 resamples, seed 0** --
exact under sign exchangeability, assumption-free about the delta distribution, and
deterministic so a Monte Carlo p-value cannot be re-rolled.

Column-level marginal base rates are **descriptive output only and may not enter a decision**:
RT-bench 0.5735, ST-bench 0.5769. They are recorded because a reader should be able to see that
the task is not degenerate, not because they gate anything.

## Verdicts

| flagged among C1-C3 | verdict | consequence |
| --- | --- | --- |
| 0 | `CLEAN` | Proceed. `contamination_suspected: false`. The audit's power limits travel with it. |
| 1 | `SUSPECTED` | Proceed. Every downstream artifact carries `contamination_suspected: true` and names the firing probe. |
| >= 2 | `CONTAMINATED` | **The wild-column LLM detection measurement is cancelled**, published as a refusal. |
| C4 flagged | `VOID` | No verdict may be read. The remedy is a better probe, not a reinterpretation. |

`VOID` dominates: it is evaluated before the C1-C3 count and overrides any verdict that count
would produce.

## Properties a conforming implementation must hold

| # | Property |
| --- | --- |
| P1 | An unavailable C1 yields `exchangeability_available: false` and is excluded from the flag count. It is never scored as "did not flag". |
| P2 | A `VOID` C4 outcome overrides the C1-C3 count and raises rather than returning a verdict. |
| P3 | The contamination signal comes from a **paired** guided-vs-general contrast. An absolute score against chance may be reported but may not gate. |
| P4 | Verdicts are keyed on `(model, seed, reference_sha256)`. A verdict computed for one corpus may not be reported for another. |
| P5 | ROUGE-L is carried as descriptive output and cannot enter any decision. |
| P6 | Both arms are paired by construction: identical items in identical order, differing only in whether the corpus is named. Any other difference between arms voids the contrast. |

## The four limits that must always be reported with the verdict

Under `PRODUCT.md:120-144` an aggregate that hides a weakness may not be published, so each of
these is carried in the artifact as a field rather than only in prose.

| # | Limit | Field |
| --- | --- | --- |
| L1 | C1 is unavailable on this deployment, leaving only behavioural probes. | `exchangeability_available: bool` |
| L2 | Power is modest: 172 column items for C3, 60 columns per corpus for C2. A weak signal will not be detected, so `CLEAN` means "not detected", not "absent". | `power_note`, `c2_columns`, `c3_items` |
| L3 | BLEURT and the GPT-4 classifier arm of Golchin & Surdeanu are not implemented, so this is a weaker instrument than the published method. C2 and C3 also share a form, so they are not methodologically independent. | `methods_implemented: [...]` |
| L4 | A verdict is bound to one deployment and does not transfer across models or refreshes. | `model`, `deployment`, `api_version` |
| L5 | C2 covers only columns with `>= 20` distinct values. A corpus memorised solely in its short columns would be missed. | `c2_min_distinct_values` |

## Non-vacuity requirements

1. **C4 must run.** A probe suite with no negative control cannot distinguish a contaminated
   corpus from a leading prompt. If C4 is skipped the audit raises.
2. **C3 must cover the census** of label-homogeneous labelled columns, not a sample. 172
   columns is the whole population; sampling it would discard power the audit cannot spare.
3. **A verdict over zero executed probes must raise**, not return `CLEAN`. An aggregate
   computed over nothing reporting a clean result is the `all_parity` failure -- three zeros
   satisfying `0 == 0` -- which this project has already shipped once.
4. **The synthetic control corpus must be generated, not sampled from a real corpus.** A
   control drawn from any published source is not a control.

## What this spec does not authorise

It does not authorise any claim about detection quality. A `CLEAN` verdict says a measurement
is worth making; it cannot make the measurement credible, and it is not a result about the
model's ability.

It does not resolve, or bear on, whether Auto-Test's own SDC training corpus overlapped its
benchmarks. That is a separate question recorded as `contamination_unverified` in
`docs/trust/semantic-domain-result.md`, and neither verdict here implicates or clears it.

It does not affect any heuristic detection number already published on these corpora. A regex
cannot memorise a benchmark.

It does not change any write gate. Contamination is an evidence question with no write path,
and nothing measured under this spec may be used to add a detector to
`CONSTRAINT_CHECKABLE_DETECTORS`.
