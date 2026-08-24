# Pre-registration: contamination audit of gpt-5.6-sol against RT/ST-bench

Registered 2026-08-24, **before any contamination probe was implemented or run, and before
any LLM measurement on RT-bench or ST-bench**.
Spec: `specs/SPEC_contamination_audit.md`.
Implementation: `scripts/bench/probe_contamination.py`.

## Why this file exists before the code

RT-bench and ST-bench are a public GitHub corpus with published labels. A language model
trained on internet text may have seen them. If it has, its detection precision on them is
not a measurement of detection ability, and any number produced would be a memorisation
score wearing a benchmark's name.

This project already carries the analogous lesson twice. `dataforge/conformal.py:215-223`
makes the certification grid a module constant because a label-derived grid invalidates the
guarantee rather than weakening it. And `eval/preregistration/api_phase_certification.md`
exists because choosing an arm on the data you then certify on is post-hoc selection wearing
a conformal hat. A contamination threshold chosen after seeing the contamination result is
the same failure a third time.

The audit runs **first** because it can cancel the expensive work. Sequencing it after the
measurement would mean discovering the measurement was meaningless after paying for it, and
would create a standing incentive to find the audit inconclusive.

## Scope, stated narrowly because two different contaminations are easy to conflate

**In scope:** whether *the language model* has memorised RT-bench or ST-bench.

**Not in scope:** whether *Auto-Test's own* SDC training corpus overlapped its benchmarks.
That is a separate question, separately probed, and recorded as unresolved
(`contamination_unverified`) in `docs/trust/semantic-domain-result.md`. A clean verdict here
would say nothing about it, and a dirty verdict here would not implicate it.

**Unaffected either way:** every heuristic detection number already published on these
corpora. A regex cannot memorise a benchmark. `docs/trust/real-error-detection-result.md`,
`frequency-dependence-correction.md` and `scoring-unit-reconciliation.md` do not depend on
this verdict.

## Hypothesis

gpt-5.6-sol has not memorised RT-bench or ST-bench at a level detectable by three
independent probes.

Directional prediction, recorded so it can be wrong: **C3 will be the one that fires, if any
does.** RT-bench and ST-bench are small CSV files in a research repository, far less mirrored
than MMLU or TruthfulQA, so verbatim-completion signal (C2) should be weak. But the
error/debatable partition is exactly the kind of short, high-salience annotation that shows
up in papers, issues and derivative repos, and C3 is the probe designed to detect it.

Secondary prediction: RT-bench will show more signal than ST-bench, because the partial probe
already run found 19 of 59 embedded SDC training examples in RT-bench against 3 of 59 in
ST-bench.

## Methods, and why three

The standard for a contamination claim is converging evidence across independent methods, not
a single test ([arXiv:2603.16197](https://arxiv.org/abs/2603.16197), whose three-experiment
audit design this follows). Each method here fails differently, so agreement is informative
and disagreement is recorded rather than resolved by preference.

| id | method | citation | needs logprobs |
| --- | --- | --- | --- |
| C1 | Exchangeability: canonical row order scores higher than shuffled | Oren et al., [arXiv:2310.17623](https://arxiv.org/abs/2310.17623) | **yes** |
| C2 | Guided vs general instruction, completion overlap | Golchin & Surdeanu, [arXiv:2308.08493](https://arxiv.org/abs/2308.08493), ICLR 2024 Spotlight | no |
| C3 | TS-Guessing on a capability-free target | Deng et al., [arXiv:2311.09783](https://arxiv.org/abs/2311.09783), NAACL 2024 | no |

C1 is the only one that yields a *provable* guarantee. It may be unavailable: reasoning
deployments commonly reject `logprobs`. **Unavailability is recorded as a limit and never as
a pass.** An audit that returns "clean" because its strongest instrument was missing is
reporting an absence of measurement as an absence of contamination.

### The C3 target is the design decision that matters

The obvious TS-Guessing adaptation -- mask the erroneous value and ask the model to name it --
is **invalid here**, and would have produced a confidently wrong answer. A model naming the
error correctly may have memorised the label or may simply be good at spotting errors, which
is the very capability under test. The confound is total.

Deng et al. avoid it by masking a *wrong* answer in a multiple-choice item, which carries no
capability signal. The capability-free analogue in this corpus is the
**`ground_truth` versus `ground_truth_debatable` partition**: whether human annotators judged
a flagged value unambiguously erroneous or genuinely arguable is a convention of *this
annotation effort*, not a property recoverable from the value. Recovery above the marginal
base rate is memorisation, not skill.

## Thresholds

| id | quantity | threshold | rationale |
| --- | --- | --- | --- |
| C1 | one-sided p-value, log-likelihood of canonical ordering vs 20 shuffles, per corpus | `p < 0.01` flags | Oren et al.'s test at a stricter alpha than their 0.05, because a false contamination finding here cancels real work. |
| C2 | exact-value recall of the held-out suffix, guided minus general, paired one-sided | `p < 0.01` **and** mean delta `>= 0.05` flags | Both required. A significant but negligible delta is what a large paired sample produces from prompt-format artifacts, not from memory. |
| C3 | exact-match recovery rate of the error/debatable partition | binomial `p < 0.01` above `max(base, 1 - base)` flags | Compared against *majority-class guessing*, not 0.5. RT's marginal split is 41/76, ST's is 47/124, so chance is 0.539 and 0.621 respectively -- using 0.5 would manufacture a finding. |
| C4 | C2 and C3 run on a synthetic never-published corpus of the same shape | must **not** flag | **Non-vacuity.** A probe that flags content the model provably has not seen is measuring its own prompt design. Without C4 a "contaminated" verdict is uninterpretable. |

Primary metric for C2 is **exact-value recall**, fixed here. ROUGE-L is computed and reported
for comparability with Golchin & Surdeanu but is **descriptive only and not a decision
input** -- naming two metrics and choosing between them afterwards is threshold-shopping.

## Fixed analysis parameters

| parameter | value | source |
| --- | --- | --- |
| model | `gpt-5.6-sol` (Azure deployment) | fixed here |
| temperature | 0.0 where the deployment accepts it | existing default (`groq_client.py:213`) |
| seed | 0 | existing probe convention |
| C1 shuffles per corpus | 20 | fixed here |
| C1 slice | first 50 content rows, canonical order | fixed here |
| C2 columns per corpus | 60, sampled at random without replacement | fixed here |
| C2 prefix / held-out split | first 60% of `dist_val`, remainder held out | fixed here |
| C3 items | **all** 200 labelled values (RT 41+35, ST 47+77) | census, not a sample |
| C4 synthetic columns | 60, matched on value count and type mix | fixed here |
| alpha for every test | 0.01 | fixed here |

## Stopping rule

**Fixed n, declared here.** The counts above are a census for C3 and fixed samples for C1,
C2 and C4. Probing stops at those counts regardless of what the running p-values look like.
No corpus, column or arm may be added after seeing a result.

## Kill criterion

**Two or more of C1-C3 flagging cancels the wild-column LLM detection measurement**
(task 6 of the plan: the RT/ST-bench LLM adapter). That work is not deferred, it is cancelled,
and the cancellation is published in `docs/trust/contamination-audit-result.md` and
`DECISIONS.md` with the same prominence a passing verdict would receive.

Exactly one of C1-C3 flagging proceeds, but every downstream artifact carries
`contamination_suspected: true` and names which probe fired.

Zero flagging proceeds with `contamination_suspected: false`, and the audit's own limits
carried alongside -- a clean verdict from three probes is evidence of absence only to the
extent those probes are powered, which at these sample sizes is modest.

**If C4 flags, the audit is void.** No verdict may be read off a probe that fires on content
the model has not seen. The remedy is a better probe, not a reinterpretation.

## Committed in advance

- Thresholds above. **Loosening one to obtain a clean verdict, or tightening one to obtain a
  dirty verdict, is forbidden.** A probe that fires cancels work; that is the point of
  registering it first.
- A flagged verdict is a publishable result and cancels work rather than being iterated away.
  So is a *clean* verdict, which must not be quietly upgraded into a claim that the corpus is
  contamination-free.
- Verdicts are bound to `(model, seed, reference_sha256)` per corpus. None may be varied post
  hoc to find a preferred combination.
- The audit authorises nothing about detection *quality*. It gates whether a measurement is
  worth making, and cannot make one credible on its own.
- It changes no write gate. Contamination is an evidence question and has no write path.

## Known limits of the design, recorded now

1. **C1 may be unavailable.** If the deployment rejects `logprobs`, the only provable method
   is lost and the audit rests on two behavioural probes. Recorded as
   `exchangeability_available: false` in the artifact, and the trust document must say so in
   its first paragraph rather than in a footnote.
2. **Low power throughout.** 200 items for C3 and 60 columns per corpus for C2. A weak
   memorisation signal will not be detected. "No contamination detected" is not "no
   contamination", and the artifact says so.
3. **C2's primary metric is stricter than the paper's.** Exact-value recall on discrete
   values is less forgiving than ROUGE-L on prose, which cuts power further. Chosen anyway
   because near-miss overlap on a list of values is not evidence of having seen the list.
4. **BLEURT is not implemented.** Golchin & Surdeanu's best-performing configuration pairs
   ROUGE-L with BLEURT and a GPT-4 few-shot classifier. This runs neither, so it is a weaker
   instrument than the published method and may not be described as reproducing it.
5. **C4 controls the probe, not the model.** A synthetic corpus proves the prompt design does
   not manufacture signal. It cannot prove the model would have flagged a genuinely
   contaminated corpus it happened to memorise weakly.
6. **A clean verdict does not transfer across models or dates.** It is bound to this
   deployment. A model refresh invalidates it, and the artifact records the deployment
   identifier so that is checkable rather than assumed.

## AMENDMENT 1 (2026-08-24)

**Recorded before any contamination probe ran. These are deviations from the design above,
not interpretations of a result.** No probe has been executed and no LLM call has been made
against RT-bench or ST-bench. The original text is left unchanged so the change stays visible.

### A. C1 is unavailable. Measured, not assumed.

`gpt-5.6-sol` at api-version `2025-04-01-preview` returns HTTP 400 for `logprobs`:

```
"message": "Unsupported parameter: 'logprobs' is not supported with this model.",
"code": "unsupported_parameter"
```

Artifact: `eval/results/azure_capability_probe.json`, re-confirmed 2026-08-24 at a cost of
$0.0016.

There is a second, structural reason no workaround exists. Oren et al. need the
log-likelihood of a **provided** ordering, which requires teacher-forced scoring of
caller-supplied text. Chat Completions returns logprobs for tokens the model **generated**, so
even on a deployment that accepted the parameter the required quantity is not obtainable.
Asking the model to reproduce the canonical order is a different experiment; it is C2.

The audit therefore rests on two behavioural probes. `exchangeability_available: false` is
carried on the artifact and stated in the first paragraph of the trust document. **This is a
loss of power and is not scored as a passing condition.**

### B. C3 is redesigned. The original design was degenerate.

Measured on the corpora before running:

| corpus | labelled columns | contain BOTH an error and a debatable value |
| --- | --- | --- |
| RT-bench | 69 | **1** |
| ST-bench | 106 | **2** |

So 172 of 175 labelled columns are label-**homogeneous**. Asking "which values in this column
are unambiguous errors and which are arguable" is therefore answerable at roughly 98% by
guessing that a column's labels are all alike. The task measures a structural property of the
corpus, not memory.

A second defect, independent of the first: the base-rate comparison was **not
capability-controlled**. I claimed the error/debatable split is capability-free. On inspection
that is only partly true -- a value like `"total"` in a numeric column is visibly arguable
while `"12/31/9999"` is visibly wrong, so a capable model can infer part of the split with no
memorisation at all. Comparing recovery against a base rate would have attributed that
inference to contamination.

**Both defects are fixed by the same change: C3 becomes a paired guided-vs-general test at
the column level.**

- **Item:** one of the 172 label-homogeneous labelled columns. Binary target: were this
  column's flagged values recorded as unambiguous errors, or as debatable?
- **Arms:** identical columns and values in identical order; the guided arm names the corpus
  and the general arm does not. Capability is held constant *by construction*, because the
  only difference between arms is corpus identification.
- **Statistic:** paired delta in exact-match recovery, guided minus general. Same rule as C2:
  flags at `p < 0.01` **and** mean delta `>= 0.05`.

Measured column-level base rates, recorded now and reported as **descriptive only, never a
decision input**: RT-bench 0.5735 (39 all-error against 29 all-debatable, n=68), ST-bench
0.5769 (44 against 60, n=104). Total n=172.

This supersedes the C3 row of the thresholds table and the `C3 items` row of the fixed
parameters table. The pre-registered kill criterion, the alpha, and the C4 requirement are
**unchanged**.

Note the honest consequence: C2 and C3 now share a form. They remain independent in what they
interrogate -- C2 whether the model reproduces corpus *content*, C3 whether it reproduces
corpus *annotations* -- and the 2026 multi-method audit distinguishes verbatim recall from
distributed memorisation on exactly that axis. But they are no longer methodologically
independent, and two probes of one form is weaker evidence than two of different forms. That
cost is accepted because the alternative was one degenerate probe.

### C. The paired test is named

Unspecified above as "paired one-sided test", which leaves a choice that must not be made
after seeing data. Fixed now: **one-sided sign-flip permutation test on the paired deltas,
20,000 resamples, seed 0.** Exact under the exchangeability of signs, assumption-free about
the delta distribution, and dependency-free. Mean delta is the effect size for the second
clause.

### D. C2 is restricted to columns with at least 20 distinct values

The median column holds 11-12 distinct values, so a 60/40 split leaves a held-out set of 4-5
values and exact-value recall becomes almost binary. Restricting to columns with `>= 20`
distinct values gives a held-out set of at least 8. Eligible: 475 of 1200 in RT-bench, 437 of
1197 in ST-bench, so the 60-column sample per corpus is unaffected.

This narrows what C2 covers, and the narrowing is recorded: it tests memorisation of the
**larger** columns only. A corpus memorised only in its short columns would be missed.

### E. `reasoning_effort` is pinned to `"none"`

Accepted by this deployment per the capability artifact. Both probes are classification tasks
with no chain of dependent inference, and reasoning tokens are billed as output at
$0.015/1k -- unpinned they dominate the budget and could push a ~$7 run past the $15 cap.
Pinned for cost control, identically across both arms, so it cannot bias the paired
comparison.

### What is NOT changed

- The kill criterion: two or more of C1-C3 flagging cancels the wild-column measurement.
- `alpha = 0.01` and the C2/C3 dual-clause rule with `delta >= 0.05`.
- C4 as a mandatory negative control, and `VOID` dominating any verdict.
- The 60-columns-per-corpus C2 sample, the 60/40 split, and seed 0.
- Verdicts bound to `(model, seed, reference_sha256)`.
- The $15 cap.
