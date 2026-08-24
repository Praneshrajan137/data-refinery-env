# Contamination audit: CLEAN, and the audit's own first design would have been wrong

Measured 2026-08-24. Artifact: `eval/results/contamination_audit.json` (primary run).
Replication: `eval/results/contamination_audit_run1.json`.
Spec: `specs/SPEC_contamination_audit.md`.
Pre-registration: `eval/preregistration/contamination_audit.md` (see Amendment 1).

Reproduce with `python scripts/bench/probe_contamination.py --max-usd 15`.

**Read this first: the only provable method was unavailable.** `gpt-5.6-sol` rejects
`logprobs` (HTTP 400 `unsupported_parameter`), so Oren et al.'s exchangeability test could not
run, and it is excluded from the flag count rather than scored as passing. This verdict rests
on two behavioural probes of modest power. **`CLEAN` here means "not detected", never
"absent".**

## Result

| condition | measured | threshold | verdict |
| --- | --- | --- | --- |
| C1 exchangeability | **unavailable** | `p < 0.01` flags | excluded, not passed |
| C2 value completion, guided minus general | **-0.0030** (p = 0.7681) | `p < 0.01` and delta `>= 0.05` | did not flag |
| C3 column-label recovery, guided minus general | **-0.0581** (p = 0.9881) | `p < 0.01` and delta `>= 0.05` | did not flag |
| C4 negative control | **-0.0090** (p = 0.6847) | must not flag | passed, audit is valid |

**Verdict: `CLEAN`.** Zero of the two available probes flagged. The wild-column LLM detection
measurement is **not cancelled**, and downstream artifacts carry
`contamination_suspected: false`.

453 calls, 1 failed, $3.6116 against a $15 cap.

## It replicated

The audit was run twice against the same fixed item sets. Only model nondeterminism differs;
the seed, columns and prompts are identical.

| probe | run 1 delta | run 2 delta | run 1 p | run 2 p |
| --- | --- | --- | --- | --- |
| C2 | -0.0102 | -0.0030 | 0.9717 | 0.7681 |
| C3 | **-0.0523** | **-0.0581** | 0.9684 | 0.9881 |
| C4 control | +0.0017 | -0.0090 | 0.3635 | 0.6847 |

Both runs returned `CLEAN`, both controls passed, and C3 -- the probe with the largest effect --
reproduced to within 0.006. Run 1 is retained rather than overwritten, because a replication
reported only as a summary is not checkable.

The second run exists because the first omitted a per-corpus breakdown, which left a
pre-registered secondary prediction unevaluable. That is a defect in the instrument rather
than a neutral omission, so it was fixed and re-run at a cost of $3.61. Total for this stage:
$7.24 of a $15 cap.

## Both probes ran negative, which is worth more than a bare pass

Naming the corpus made the model slightly **worse** in both probes, not better:

| probe | guided (corpus named) | general (corpus not named) | delta |
| --- | --- | --- | --- |
| C2 value completion | 0.2865 | 0.2895 | **-0.0030** |
| C3 column-label recovery | 0.6105 | 0.6686 | **-0.0581** |

A memorising model should improve when told which corpus it is looking at -- that is the whole
premise of guided instruction. Both deltas are negative in both runs, and C3 sits near the 99th
percentile of the null in the *wrong* direction. This is a stronger reading than "no
significant difference": there is no detectable memorisation signal, and if anything the corpus
name is a mild distraction.

C2's ~0.29 recall in both arms is baseline plausible-value generation. Given 13 values from a
column, a competent model guesses several more that fit the pattern. That is pattern
completion, not recall.

## The finding that matters most: my first design would have decided this on an unpinned choice

The original pre-registration compared C3 recovery against an absolute base rate. Amendment 1
replaced that with a paired guided-vs-general contrast, after measurement showed only 3 of 175
labelled columns contain both an unambiguous error and a debatable value. The results show the
redesign was not cosmetic.

**There is a real capability signal.** The general arm never names the corpus, and still
recovers the column label at 0.6628 -- above every candidate base rate. The model can partly
tell "clearly wrong" from "arguable" with no corpus knowledge at all, exactly the confound
Amendment 1 identified.

**Whether that signal crossed the significance bar depended on which base rate was used:**

| arm | recovery | vs pooled column rate 0.5174 | vs per-corpus rate 0.5769 |
| --- | --- | --- | --- |
| guided | 105/172 = 0.6105 | p = 0.0088 -> **FLAGS** | p = 0.2083 -> no flag |
| general | 114/172 = 0.6628 | p = 0.00008 -> **FLAGS** | p = 0.0131 -> no flag |

So the original comparison structure produces a contamination flag under one defensible base
rate and not under another, with a genuine capability signal sitting on the decision boundary.
The pre-registration fixed `alpha` but did not pin the base-rate estimator precisely enough,
which left exactly the degree of freedom pre-registration exists to remove.

Note the direction that settles it: **the arm that never names the corpus flags harder than the
arm that does** (p = 0.00008 against p = 0.0088). Under an absolute comparison, "contamination"
would have been attributed most strongly to the condition containing no corpus identification
at all. That is incoherent as a contamination claim, and it is visible only because the paired
arm exists.

The paired design removes the choice rather than making it better: the comparison point is the
model's own other arm on identical items, so no base rate is estimated and there is nothing
left to pick.

## Where the pre-registration was right, and where it was wrong

The registered directional prediction:

> **C3 will be the one that fires, if any does.** RT-bench and ST-bench are small CSV files in
> a research repository, far less mirrored than MMLU or TruthfulQA, so verbatim-completion
> signal (C2) should be weak.

**Right on C2 being weak. Wrong on C3.** Neither fired, and C3 went furthest in the *opposite*
direction (-0.0581, the largest negative delta measured, reproduced in both runs). The reasoning
behind the prediction -- that short high-salience annotations are more likely to be memorised
than bulk content -- was not supported.

The secondary prediction was:

> RT-bench will show more signal than ST-bench, because the partial probe already run found 19
> of 59 embedded SDC training examples in RT-bench against 3 of 59 in ST-bench.

**Refuted.** The per-corpus deltas are indistinguishable:

| probe | RT-bench delta | ST-bench delta |
| --- | --- | --- |
| C2 (n = 60 each) | -0.0032 | -0.0029 |
| C3 (n = 68 / 104) | -0.0588 | -0.0577 |

Within 0.001 of each other on both probes, and negative in both corpora. The 19-of-59 versus
3-of-59 SDC overlap does **not** predict LLM memorisation asymmetry.

That is worth stating as its own result, because it converts an assertion into evidence. The
spec claims the two contaminations are independent questions -- whether Auto-Test's training
corpus overlapped its benchmarks, and whether a language model memorised those benchmarks. This
measurement supports that separation instead of merely asserting it.

**RT-bench is easier, and equally easier in both arms.** Absolute recovery is markedly higher on
RT-bench (C2 0.34 against 0.23; C3 0.68/0.74 against 0.57/0.63), so its columns are more
predictable. But the guided-general gap is the same on both. Higher absolute performance with an
unchanged paired delta is the signature of capability, not memory, and it is a clean
illustration of why the paired design was necessary.

## Two operational defects, recorded

**Calibration under-projected by 1.94x.** Six calls projected $1.87 for the full run; run 1
cost $3.6245. Cause: C2 cost scales with column size, and the two columns sampled for
calibration were smaller than average. Nothing was harmed -- both runs finished far inside the
cap -- but a calibration this small is a lower bound, not an estimate. A future probe should
calibrate across the size distribution rather than the head of it.

**The C4 breakdown in the committed artifact is noise.** The per-corpus split groups on a
`corpus:index` key prefix, and the synthetic control uses bare `S<n>` keys, so C4 emitted 60
single-item "corpora". The grouping now requires namespaced keys, but **the committed artifact
still contains the spurious field**: it was not hand-edited out, because editing an evidence
artifact to look tidier is the wrong instinct even when the edit is harmless. Read
`probes.C4.by_corpus_descriptive_only` as an artifact of a since-fixed bug, and
`probes.C4.mean_delta` as the real control result.

One call failed in run 2 (`failed_calls: 1`). It was counted, treated as no signal, and
surfaced on the artifact rather than retried into silence.

## Is / is not

**Is:** a converging two-probe paired audit with a passing negative control, thresholds fixed
before the run, and a verdict bound to `(model, seed, reference_sha256)`.

**Is not** a certificate that these corpora are uncontaminated, and the artifact says so in
five recorded limitations:

1. **C1, the only provable method, is unavailable.** `logprobs` is rejected on this deployment,
   and chat-completions logprobs cover *generated* tokens only, so the log-likelihood of a
   provided ordering is unobtainable even where the parameter is accepted. Excluded from the
   count, never scored as a pass.
2. **Power is modest.** 172 column items and 120 completion items. A weak memorisation signal
   would not be detected.
3. **C2 and C3 share a paired form**, so they are not methodologically independent. Two probes
   of one form is weaker evidence than two of different forms, and that cost was accepted
   because the alternative was one degenerate probe.
4. **C2 covers only columns with at least 20 distinct values.** A corpus memorised solely in
   its short columns would be missed.
5. **The verdict is bound to this deployment and api-version.** A model refresh invalidates it.

## What this does and does not authorise

**Authorises:** proceeding with the wild-column LLM detection measurement on RT/ST-bench, with
`contamination_suspected: false` recorded on its artifacts.

**Does not authorise** any claim about detection quality. A clean audit says a measurement is
worth making; it cannot make the measurement credible.

**Does not bear on** whether Auto-Test's own SDC training corpus overlapped its benchmarks.
That remains `contamination_unverified` in `docs/trust/semantic-domain-result.md`, it is a
different question, and neither verdict implicates or clears the other.

**Does not affect** any heuristic detection number already published on these corpora. A regex
cannot memorise a benchmark, so `real-error-detection-result.md`,
`frequency-dependence-correction.md` and `scoring-unit-reconciliation.md` are untouched either
way.

**Changes no write gate.** Contamination is an evidence question with no write path.
