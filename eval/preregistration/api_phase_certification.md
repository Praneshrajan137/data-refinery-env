# Pre-registration: certified auto-apply for the schema-constrained corrector

**Status: committed before any Stage-3 (flagship) spend.** This document exists so
the certification result cannot be produced by choosing an arm and then certifying
on the same data. DataForge has already rejected one attempt at raising accuracy
by threshold tuning as "dishonest overfitting to two datasets"
([STRATEGY.md](../../docs/STRATEGY.md)); a favourable-looking result obtained by
post-hoc arm selection would be the same mistake wearing a conformal hat.

Everything below is fixed **in advance**. Deviations must be recorded as
deviations, not silently absorbed.

## Question

Does constraining the LLM corrector to a hard decode-time candidate `enum`
(Structured Outputs) produce a confidence signal good enough that
`conformal.certify_threshold` certifies a **non-zero** auto-apply threshold at
`alpha = 0.05`?

Every prior attempt certified **0.0 coverage** (ECE 0.80-0.96, ~5-16% precision).
The pre-registered expectation is therefore *not* success.

## Why this lever and not another

Two candidate levers were considered. One was closed by measurement before any
money was spent on it:

- **Token logprobs as a continuous confidence** -- **CLOSED**. The deployment
  rejects the parameter: `"Unsupported parameter: 'logprobs' is not supported
  with this model."` Recorded in
  [azure_capability_probe.json](../results/azure_capability_probe.json).
- **Structured Outputs `enum`** -- **OPEN**. Accepted, and the returned value was
  verified to come from the enum (`enum_honoured: true`).

The same probe also established that `temperature` is rejected
(`"Only the default (1) value is supported"`). Consequence: the corrector's
`temperature=0.4` never took effect on Azure, so sample diversity **cannot** be
tuned by temperature and `k` must be chosen empirically. That is why `k` is an
arm below rather than a constant.

## Fixed analysis parameters

| Parameter | Value | Source |
| --- | --- | --- |
| `alpha` (target error rate) | `0.05` | existing default |
| `delta` (confidence) | `0.05` | existing default |
| `min_support` (accepted-set floor) | `30` | `conformal_corrector_policy` default |
| `calib_fraction` | `0.5` | `split_by_class` default |
| Split seed | `20260804` | fixed here |
| Policy keying | `issue_type` (`calibration_samples_by_type`) | the key `_partition_auto_apply` actually uses at inference |
| Calibration method | `isotonic` | existing default |
| Dataset | `hospital` | the only categorical-heavy RAHA dataset; where the pool lever applies |

`min_samples_for_certification(0.05, 0.05) = 59`. So certification requires an
accepted set of **>= 59 all-correct** samples, or roughly **>= 100 with at most one
error**. This is stated in advance so the outcome is interpretable either way.

## Disjoint split between arm selection and certification

Detected issues on `hospital` are ordered deterministically by the detector
ensemble, then shuffled with `random.Random(20260804)`:

- **SWEEP slice** -- the first 20% of the shuffled list. Used **only** to choose
  the arm. Never used to certify.
- **FLAGSHIP set** -- the remaining 80%. Used **only** for the certification
  attempt, which is itself split into calibration/test halves by
  `conformal.split_by_class`.

The certification number is therefore computed on data that played no part in
choosing the arm, and on a test half that played no part in fitting the
calibration map or picking the threshold.

## Arms (Stage 2, sweep slice only)

Draws are **paired**: nine samples are drawn once per issue under the structured
schema, and `k = 3, 5, 9` are evaluated on nested prefixes of the same draws.
Prefixes of iid draws are themselves iid, so this is valid, and it removes
between-arm sampling noise while cutting the call count roughly in half.

| Arm | Mode | k |
| --- | --- | --- |
| `A_freetext_k3` | current pool-constrained free text (baseline) | 3 |
| `B_structured_k3` | structured enum | 3 (prefix) |
| `B_structured_k5` | structured enum | 5 (prefix) |
| `B_structured_k9` | structured enum | 9 (all) |

Cost: `12` calls per issue (3 free-text + 9 structured).

## Arm-selection rule (fixed in advance)

1. Compute, per arm, the **projected certified threshold** by running the real
   `conformal.certify_threshold` on that arm's sweep-slice samples with the fixed
   parameters above.
2. Choose the arm with the **highest projected certified coverage** (fraction of
   proposals at or above the certified threshold).
3. Ties, or all arms projecting zero coverage, break to the **lowest `k`** (cost).
4. If every arm projects zero coverage, the flagship still runs on the selected
   arm. A null is a valid, pre-registered outcome; it is not grounds for changing
   `alpha`, `min_support`, or the split.

## Primary endpoint

> A certified threshold `<= 1.0` exists for at least one `issue_type` on the
> flagship calibration half, **and** the disjoint test half measures non-zero
> certified coverage at `alpha = 0.05`.

## Secondary, reported regardless

- ECE before and after calibration, on the disjoint test half only.
- Proposal precision and abstention rate per arm.
- Confidence-grid cardinality per arm (the mechanical reason a 3-point grid
  cannot be certified).
- For a null: the `uncertified_reasons_by_class` cause, distinguishing
  `insufficient_support` ("not enough evidence yet") from
  `precision_below_target` ("the corrector is genuinely too imprecise"). This
  distinction is the deliverable when the primary endpoint fails.

## Guards against a false positive

- **No pooling across seeds.** One seed, one run. Pooled certification is sound
  only for a null result
  ([azure-frontier-corrector-benchmark.md](../../docs/azure-frontier-corrector-benchmark.md)).
- **No re-running the flagship to get a better number.** If the flagship is
  re-run for an operational reason (timeout, cap trip), every attempt is recorded
  in the spend ledger and the artifact notes say why.
- **Certification does not change the default.** Even on success, certified
  auto-apply requires an authoritative schema **and** an explicitly loaded
  calibration artifact (`--corrector-calibration`). The default stays
  propose-not-apply. Ratified with the maintainer before implementation.
- **The release gate must agree.** `dataforge/release/corrector_gate.py` fails the
  release if any threshold `<= 1.0` lacks a passing committed measurement, so a
  hand-edited artifact cannot unlock auto-apply.

## Budget

| Stage | Cap | Purpose |
| --- | --- | --- |
| 1 capability probe | $1 | spent: $0.0016 |
| 2 arm sweep | $8 | this pre-registration's experiment |
| 3 flagship | $20 | certification attempt |
| 4 negative control (rayyan) | $6 | abstention must hold |
| Campaign ceiling | $50 | enforced by pre-flight estimate, in-flight cap, and ledger |

---

## AMENDMENT 1 (2026-08-04) -- sweep enlarged so the selection rule is evaluable

**Recorded before the flagship ran, and before the enlarged sweep completed.**

A first sweep of 100 issues produced only **17-22 proposals per arm** (proposal
rate ~18%), which is below the fixed `min_support = 30`. Consequently
`certify_threshold` returned `None` for *every* arm with the reason
`insufficient_support`, the primary metric was **undefined rather than zero**, and
the tie-break selected the baseline arm (`A_freetext_k3`) by position alone --
despite structured winning every secondary metric.

**Decision (maintainer): enlarge the sweep until each arm clears `min_support`.**
Nothing else changes: `alpha`, `delta`, `min_support`, `calib_fraction`, the split
seed, the arms, and the selection rule are all untouched.

Why this is not "sampling until significance": the quantity being fixed is a
**pre-specified support floor** that the rule requires as an input, not a p-value
or an effect size. The rule cannot be evaluated at all below `min_support`, so
collecting enough data to evaluate it is a precondition of the pre-registration,
not a violation of it. No arm's outcome influenced the decision to enlarge -- all
four were equally undefined.

Sweep cap raised **$8 -> $11** accordingly. Campaign ceiling unchanged at $50.

### Interim observation (reported for transparency, not used for selection)

At 80/250 issues the secondary metrics separate cleanly and monotonically:

| Arm | proposals | confidence-grid size | ECE |
| --- | --- | --- | --- |
| `A_freetext_k3` | 16 | 2 | 0.729 |
| `B_structured_k3` | 14 | 6 | 0.595 |
| `B_structured_k5` | 14 | 7 | 0.579 |
| `B_structured_k9` | 15 | 10 | 0.544 |

The grid widening (2 -> 10) is the mechanical effect the structured mode was built
to produce, and ECE improves on the historical 0.80-0.96. Selection still awaits
the primary metric once `min_support` is reached.

---

## AMENDMENT 2 (2026-08-04) -- selection rule overridden, and the flagship's purpose restated

**Recorded before the flagship ran. This is a deviation, not an interpretation.**

### What the enlarged sweep found

At 173 issues every arm cleared `min_support` in total proposals (35-39), so the
primary metric became **evaluable** and returned a definitive answer: not
`insufficient_support` but `precision_below_target` for all four arms. Amendment 1's
enlargement therefore did its job.

Analysing the persisted `(confidence, correct)` pairs shows *why*, and the two arms
are not equivalent:

| Arm | overall precision | top-confidence slice | discriminates? |
| --- | --- | --- | --- |
| `A_freetext_k3` | 0.282 | n=36 -> 0.306 | **no** -- the top bucket is the whole set |
| `B_structured_k9` | 0.297 | n=3 -> 1.000; n=6 -> 1.000 | **yes** |

Free-text self-consistency confidence carries **no information**: its most-confident
bucket is no more precise than the population. That, not "the model is confidently
wrong", is the mechanical reason every prior attempt certified 0.0 coverage. The
structured enum converts the same underlying corrector into a score that separates a
perfectly-clean slice from a 30%-precise population.

### The deviation

The pre-registered tie-break ("ties and all-zero break to lowest k") selected
`A_freetext_k3`. **Overridden: the flagship runs `B_structured_k9`.**

Justification: a tie-break exists to choose between arms whose evidence is *equal*.
These arms are not equal -- one has measured zero discrimination -- but the primary
metric cannot express that, because it collapses every uncertified arm to
"coverage 0.0". Running the flagship on an arm with provably no signal would spend
the budget to produce an uninformative null. The secondary metrics
(pre-registered above as "reported regardless": ECE, precision, grid cardinality)
favour `B_structured_k9` unanimously and monotonically.

This is recorded as a **deviation from the rule as written**, decided by the
maintainer, with the rule's text left unchanged so the override stays visible.

### The primary endpoint is now expected to FAIL, and that is stated in advance

Certification at `alpha = delta = 0.05` needs an accepted set of **>= 59 all-correct**
samples (or ~100 with at most one error). The clean slice is roughly **16% of
proposals**, so certification would require on the order of **1,700-2,800 issues at
k=9 -- about $49-80**, against $37.8 remaining. It is **arithmetically out of reach
within budget.**

The flagship's purpose is therefore restated, before it runs:

- **Not** "attempt certification and see". Certification will not be claimed.
- **Instead**: (a) discharge the pre-registered primary endpoint honestly as a NULL
  with its measured cause, and (b) obtain the largest clean-slice measurement this
  budget buys, to convert "the top 6 were perfect" into a quantified **propose-only**
  precision tier. Expected clean slice at $20: n ~= 24, still below 59.

Any coverage number this run produces is a **measurement, not a certificate**. The
release gate (`dataforge/release/corrector_gate.py`) still refuses to enable
auto-apply without a passing committed measurement, and every threshold stays at the
`1.01` disabled sentinel.

### What is NOT changed

`alpha`, `delta`, `min_support`, `calib_fraction`, the split seed, the
sweep/flagship partition, the primary endpoint's definition, the no-pooling rule,
and the opt-in-only stance on certified auto-apply.

Budget: flagship cap $20 (unchanged). Campaign ceiling $50 (unchanged). Spend to
date $12.16.

---

## AMENDMENT 3 (2026-08-04) -- outcome: primary endpoint discharged as a NULL

> **RETRACTION NOTICE (2026-08-05).** The first version of this amendment contained
> four false or unsupported statements, corrected below and listed here so the error
> is visible rather than quietly overwritten:
>
> 1. It claimed "the binding constraint is no longer *calibration quality*, it is
>    **accepted-set sample size**." **Retracted.** The precision gradient refutes it;
>    the constraint is still the achievable precision level.
> 2. It cited **ECE 0.68 -> 0.46** as evidence of the structured mode's value. **
>    Withdrawn as evidence** -- ECE is confounded here (see below). Replaced by
>    ROC-AUC.
> 3. It said the flagship run was **throttled**. **Retracted** -- the arithmetic points
>    to a retry-timeout stall.
> 4. It said "**two of five** ledger receipts are reconstructions." **Wrong: three of
>    seven**, and under half of recorded spend is a measurement (see Final accounting).

### The primary endpoint

> A certified threshold `<= 1.0` exists for at least one `issue_type` [...] and the
> disjoint test half measures non-zero certified coverage at `alpha = 0.05`.

**Result: NULL.** Not certified. This was the pre-registered expectation.

The finding rests on the **arm sweep** (`eval/results/corrector_arm_sweep.json`,
173 issues, 35-39 proposals per arm), not on the flagship run, which produced no data.

Every arm returned `precision_below_target` -- a *measured* refusal, not
`insufficient_support`. Certification needs >= **59** all-correct accepted samples
(`min_samples_for_certification(0.05, 0.05)`).

### Result 1: free-text confidence has no usable discriminating signal

Measured by **ROC-AUC of confidence against correctness** -- the threshold-free
metric, chosen because it cannot be gamed by picking a favourable cut:

| Arm | n | precision | **ROC-AUC** | 95% CI (bootstrap) | top-20% precision |
| --- | --- | --- | --- | --- | --- |
| `A_freetext_k3` | 39 | 0.282 | **0.554** | [0.500, 0.617] | 0.375 |
| `B_structured_k3` | 35 | 0.314 | **0.862** | [0.741, 0.956] | 0.857 |
| `B_structured_k5` | 35 | 0.314 | **0.879** | [0.772, 0.963] | 0.857 |
| `B_structured_k9` | 37 | 0.297 | **0.948** | [0.885, 0.990] | 0.857 |

Free-text's CI lower bound sits exactly at 0.5, the "no information" convention. The
structured CIs do not overlap it. Both arms share the same 11 positives and are paired
on the same issues, so the contrast is not a sampling artifact.

**Why ECE was the wrong instrument.** `expected_calibration_error` is a weighted mean
of `|mean_confidence - accuracy|` over equal-width bins. When accuracy is low, *any*
score shifted uniformly downward scores better on ECE with **zero** improvement in
ordering. ECE conflates calibration with discrimination, and `certify_threshold` cares
only about the precision of the accepted set -- i.e. discrimination. The ECE figures
are retained below as a secondary observation, not as evidence.

### Result 2: the structured enum creates real ordering, but not a certifiable tier

The same corrector and the same model go from chance-level ordering to AUC 0.948. But
the precision **level** in the top tier does not clear the auto-apply bar:

| slice (`B_structured_k9`) | precision | errors | CP95 upper error | certifiable? |
| --- | --- | --- | --- | --- |
| top-6 | 1.000 | 0 | 0.393 | no (needs <= 0.05) |
| top-10 | 0.800 | 2 | 0.507 | no |
| top-17 | 0.647 | 6 | 0.580 | no |

**The earlier "n=6 -> 1.000" headline was a selected extremum** -- the largest
all-correct prefix -- and its own Clopper-Pearson bound admits a true error rate up to
39%. Precision decays quickly as the slice grows, so **more data would most likely
produce a firmer NO, not a YES**. Certification is therefore still bound by the
achievable **precision level** (~0.80 in the top tier, against a 0.95 bar), which is
the same conclusion prior work reached for the pool-constrained corrector (0.85,
propose-only). This work **refines the diagnosis; it does not overturn the finding.**

**Strong inference, not verified**: whether a genuinely >= 95%-precise tier exists is
unresolvable at n=37. Settling it needs roughly an order of magnitude more data
(~$50-80), and the observed gradient predicts the answer is no.

### Result 3 (unplanned, and the most useful): the score is a good ranker

AUC 0.948 is a *triage* result. DataForge already has a shipped consumer for exactly
that -- the review queue. **Caveat that bounds the claim:** this AUC is measured only
over cells where the corrector chose to propose (about 18% of attempts), so it
describes ranking *proposals*, not ranking an unfiltered detector queue. It therefore
does **not**, on its own, license replacing `ReviewRanker`.

### Result 4 (2026-08-05, follow-up): on hospital the two triagers are not distinguishable

The redundancy question was previously **unanswerable** from the numbers on record: the
corrector's 0.948 and the ranker's 0.946 came from *different populations* (proposals
only vs every flagged cell) and *different labels* ("was the proposed value correct" vs
"is this flagged cell really an error"). Comparing them was invalid, so the comparison
was rerun properly on **hospital** -- same cells, same label, paired
(`scripts/bench/compare_triage_scorers.py`, artifact
`eval/results/triage_scorer_comparison.json`, $2.10). The single-dataset scope is
load-bearing; see the retraction below.

| scorer | n | positives | ROC-AUC | 95% CI | calls/cell |
| --- | --- | --- | --- | --- | --- |
| `ReviewRanker` | 150 | 40 | 0.958 | [0.912, 0.996] | **1** |
| structured corrector | 150 | 40 | 0.979 | [0.946, 0.998] | **3** |

**Paired AUC delta (corrector - ranker): 95% CI [-0.029, +0.074]** -- straddles zero.
The corrector is nominally ahead by 0.021, but the difference is **not detectable** at
this sample size. Note this is a failure to detect a difference, **not** a proof of
equivalence.

Two methodological points that materially changed this result, both worth carrying:

1. **The natural base rate makes the naive experiment useless.** The detector queue is
   only ~4.5% precise under **inferred FD constraints** (371 genuine errors among 8,299
   flagged cells in the flagship split; the shipped **default** detector path is 56%
   precise on the same dataset -- see `eval/results/detector_queue_composition.json`), so
   the first
   150-cell run captured **3 positives** and produced deceptively narrow CIs. The sample
   was therefore **enriched to 40 positives**, which is legitimate precisely because
   ROC-AUC is invariant to class balance. `precision@k` is *not* invariant and is
   suppressed in the enriched artifact; the natural-rate run is retained separately in
   `triage_scorer_comparison_natural.json`.
2. **Tie handling is load-bearing here.** The corrector abstains on 67% of cells, all
   scored 0.0, so two-thirds of its scores are one tie block. `roc_auc` uses average-rank
   Mann-Whitney (verified: an all-tied input returns 0.5, not 1.0), so the abstention
   block earns no spurious credit. Abstention is scored rather than dropped because
   dropping it would restore the survivor bias this experiment exists to remove -- and
   because in product use "the corrector declined" is itself a triage signal.

**Consequences, correctly scoped.** On hospital, for ranking alone the ranker is the
better buy: same measurable power at one third the calls; for ranking *plus* a candidate
value, the corrector alone (3 calls) dominates ranker-then-corrector (4 calls) at no
measurable loss of ordering.

> **RETRACTION (2026-08-05, same day).** The sentence that stood here -- "the features
> **are substantially redundant as rankers**, and the choice is a product decision" --
> was **generalised from a single dataset** and is withdrawn as a general claim. The
> comparison above ran on **hospital only**. The disconfirming evidence was already
> committed, in this same session, in `eval/results/review_gate_probe.json`:
>
> | dataset | queue | LLM ranker ROC-AUC | free-baseline ROC-AUC |
> | --- | --- | --- | --- |
> | hospital | 10,373 | 0.9459 | 0.488 |
> | rayyan | 2,336 | 0.9545 | 0.5404 |
> | **flights** | 1,941 | **0.514** | **0.0201** |
>
> On flights the LLM ranker is **at chance**. Redundancy therefore holds where both
> scorers work and is meaningless where neither does. The corrected claim: **the LLM
> ranker's value is dataset-dependent, and the dependence is not predictable at
> runtime** (see the regime problem below).
>
> Two further consequences of the same error:
>
> 1. **There was never an honest free control.** The `baseline_roc_auc` figures above are
>    the detector's own sort order, which on hospital is a *near-constant feature*:
>    10,261 of 10,373 cells share `confidence = 0.95` (normalised entropy 0.0263), so
>    0.488 measures the absence of a *feature*, not the absence of free *signal*. On
>    flights the baseline is 0.0201 -- strongly anti-correlated, inverting to ~0.98, i.e.
>    **better than the LLM** -- so free signal is abundant there with a dataset-dependent
>    sign. Two paid options were compared against each other with no fair cheap control.
> 2. **No guard test caught this**, because the guards added earlier that day check
>    artifact *fields* and *numbers*, not claim **scope**. A claim can be arithmetically
>    correct about its sample and false about the world.

**The regime problem (the binding constraint, previously buried).** The probe's own
conclusion records why this cannot be papered over: baseline informativeness "depends on
whether confidence CORRELATES with correctness, which requires ground truth the product
does not have at runtime." Dispersion was tested as a proxy and **refuted** -- rayyan has
a well-spread confidence distribution (entropy 0.641) and a chance-level baseline (0.540)
that the LLM beats decisively (0.955). So at runtime, on a user's own table, **there is
currently no known way to predict whether paid triage will help.** Every transfer
assumption in this phase -- conformal exchangeability, free-ranker weights, LLM value --
rests on this one unsolved problem.

A third finding falls out for free, **scoped to this regime**: under inferred constraints
~95% of hospital's flagged cells are not real errors.

> **RE-SCOPED (2026-08-05).** This was originally stated as "~95% of flagged cells are
> not real errors" -- a property of the detector queue. It is not. Measured in both
> regimes (`eval/results/detector_queue_composition.json`), the shipped **default**
> detector path yields precision **0.561** (hospital), **0.947** (flights), **0.342**
> (rayyan), i.e. only **1.06-2.92 cells reviewed per real error**. The 4.4% figure is
> hospital **under inferred FD constraints only** -- and those constraints change nothing
> at all on flights or rayyan. Since the flooding that justifies triage is a configuration
> choice rather than a dataset property, the "strongest argument for having a triager"
> reading is withdrawn: the cheaper argument is to stop emitting the spurious flags.


### Operational failure, recorded rather than hidden

The flagship run was authorised and launched and **produced no data**. It never reached
its first checkpoint (interval 25 issues).

**Corrected cause.** Not throttling. `AzureBenchClient` runs `max_retries=5` against
`DATAFORGE_AZURE_TIMEOUT_S=180`, and its timeout branch sleeps `min(2*(attempt+1), 120)`
between attempts. A single hung request therefore consumes
`5 x 180s + (2+4+6+8)s = 920s ~= 15.3 minutes` before raising `TimeoutError`, which the
flagship loop catches per-issue and continues past. That matches the observed
~15-minute silent window exactly. **Strong inference** (the arithmetic matches; retries
were not directly instrumented at the time).

**Consequences for the spend record.** If the stall was one hung request, real billable
calls in that window were roughly five, not ~540. The original receipt's ~$2.90 is
therefore an overstatement of up to ~100x and has been reissued with the corrected
cause and a defensible bound. Overstating spend is safer than understating it, but it
still corrupts the artifact whose only purpose is truthfulness.

**Corrected root cause of the data loss** (the earlier "checkpoint every 5 issues" was
a fix for the symptom):

1. unbounded per-issue retry wall-time (920s for one request);
2. no per-issue progress logging, so a 15-minute stall is indistinguishable from death;
3. a 180s timeout applied to ~30-token enum-constrained answers, where it is absurd --
   it was set from runbook guidance for long reasoning outputs.

No number from the flagship is claimed anywhere. The release gate
(`dataforge/release/corrector_gate.py`) independently confirms no corrector class is
promoted: `enabled_classes == []`, every threshold at the `1.01` disabled sentinel.

### Final accounting -- measured and estimated, separately

The earlier single total implied a precision the ledger does not have.

| | receipts | USD |
| --- | --- | --- |
| **Measured** (token counts recorded) | 9 | **$10.48** |
| **Reconstructed / estimated** (`calls == 0`) | 3 | **$7.34** |
| **Total** | 12 | **$17.81** |

Of that, the certification work itself was **$13.51** (46% measured) and the 2026-08-05
matched triage follow-up added **$4.15**, fully measured -- which is why the campaign-wide
measured share rose to **58%**.

**Still, 42% of recorded spend is a reconstruction rather than a measurement.** That is an
uncomfortable result for a phase whose thesis was that spend must be accountable, and it is
stated here rather than buried: the accountability layer was built *during* the runs it was
meant to measure, so the early runs are exactly the ones it failed to capture. Every run
after it landed is fully measured. The three reconstructions are the evidence for why
receipts are now written at every checkpoint instead of only at completion, and why
`dataforge.spend.ledger_summary` reports measured and estimated separately so no future
report can present a partly-reconstructed total as fact.

(The earlier figure of `$15.20` included a `~$2.90` estimate for the failed flagship run
that assumed throttling at ~540 calls. That receipt has been superseded by a
`$1.21` **rigorous upper bound**, derived from the fact that the run never reached its
first checkpoint at index 25 and therefore issued fewer than `25 x 9 = 225` calls. The
point estimate under the single-hung-request explanation is ~$0.03-0.16; the bound is
recorded instead so the ledger stays conservative without being fabricated.)






