# Pre-registration: can a bias-corrected measure gate the premise without a fitted constant?

Written 2026-09-01, **before** installing a dependency, implementing a measure, or
computing any quantity on any corpus. Nothing below is edited afterwards; results and
deviations are appended as amendments.

This is the direct continuation of `premise_quality.md` (2026-08-25), which shipped C1 and
C2. Read that first. It established the frame this document works inside:

- **Q1** — given a dependency that holds on the true data, are these violations errors or
  legitimate variation? **Undecidable in-table, conceded in full.** Untouched here.
- **Q2** — does the dependency hold on the true data *at all*? Partly decidable, and C1/C2
  made it more decidable without introducing a parameter.

## The wall this responds to

C2 introduced `tested_confidence` — confidence measured only over rows in multi-row
determinant groups, since a singleton group is consistent with any dependent value and
tests nothing. It was then measured on hospital's 85 non-vacuous candidates
(`docs/trust/premise-quality-result.md`) and it **separates perfectly**: false dependencies
at most 0.9554, true at least 0.9599, retaining all 69 true dependencies.

That result has not been shipped as a gate, and the reason is this project's own K3:

> **K3** the change requires introducing a tunable constant to satisfy K1 or K2. A
> parameter added after seeing the data is the overfitting `constraint-circularity.md`
> forbids, and the correct response is to abandon C1 and C2 rather than to fit them.

A threshold at 0.9599 is exactly that constant, chosen after seeing which side of it the
false dependencies fell on, from one corpus. `docs/trust/shipped-premise-result.md` records
the refusal holding even when it was expensive — gating there would have appeared to fix the
116-corruption result. The refusal was correct. **The conclusion drawn from it was not.**

The conclusion recorded in `DECISIONS.md` (2026-09-01) was that shipping requires a second
corpus with naturally-occurring false dependencies and retained ground truth. That is one
route. This document tests a different and better one: **the constant is not needed at all.**

## What the literature says, and why it changes the question

`tested_confidence` is a hand-rolled approximation of a measure that already exists, and
the gap between them is precisely the fitted constant.

1. **The defect is formal, named, and proved.** Parciak, Vandevoort, Neven, Peeters and
   Vansummeren, *Measuring Approximate Functional Dependencies: a Comparative Study*, ICDE
   2024 (arXiv:2312.06296), name the axis **LHS-uniqueness** and prove the inflation
   mechanism. For the standard measure `g3`:

   > "For any non-empty R we can always obtain a subrelation R′ of size |dom_X(R)| by
   > arbitrarily fixing one y-value for each x-value. As such, **g3 is bounded from below by
   > |dom_X(R)|/|R| > 0**."

   Every distinct determinant value contributes one free, unfalsifiable tuple. As groups
   become singletons the floor rises to 1 and the statistic becomes uninformative regardless
   of the truth of the dependency. This is `ZipCode -> HospitalName` surviving, stated as a
   theorem rather than as an observation about one column.

2. **The same defect, information-theoretically.** Mandros, Boley and Vreeken,
   *Discovering Reliable Approximate Functional Dependencies*, KDD 2017:

   > "This is especially obvious in the extreme case when the empirical count c(X=x) is
   > equal to 1 [...] **Ĥ(Y|X=x) is trivially equal to 0 independent of the true
   > distribution p.**"

3. **The correction has a threshold of zero.** Piatetsky-Shapiro and Matheus (1993)
   normalize `pdep` by its expectation under random permutation rather than by a fitted cut:

   ```
   mu  = 1 - [ (1 - pdep(X->Y,R)) / (1 - pdep(Y,R)) ] * [ (|R| - 1) / (|R| - |dom_X(R)|) ]
   mu+ = max(mu, 0)
   ```

   The right-hand factor is a **closed-form singleton-determinant-group penalty**. As groups
   become singletons, `|dom_X(R)| -> |R|`, the factor diverges, and `mu+ -> 0`. The decision
   point is 0, and 0 comes from the permutation null, not from a corpus.

   Parciak et al. evaluate every published AFD measure against error rate, LHS-uniqueness and
   RHS-skew and recommend `mu+` for practical use — `RFI'+` has equal properties but is slow.
   They also report that `g1` and `g1'`, the measures real discovery tools optimise (`g1'` is
   Pyro's objective), have **near-zero separating power on all three axes**.

**Why this satisfies K3 where 0.9599 did not.** `mu+`'s correction term is computed from the
observed marginals of the specific table and column pair. It is data-dependent, not fitted.
Nothing is chosen by looking at which side of a line the false dependencies fall on. This is
the same standard C1 and C2 met — "no constant appears" — and it is why this is C3 rather
than a retuning of C2.

## C3, stated

> **Require `mu+ > 0`,** computed per candidate dependency, in addition to the retained
> guards (the 0.9 emission floor, `_MAX_DETERMINANT_UNIQUE_FRACTION`,
> `_MIN_FD_SUPPORT_GROUPS`, C1's majority baseline, and C2's `tested_confidence` as a
> reported field).

`g3'` (Giannella and Robertson) is computed and reported alongside as the combinatorial
sibling — it subtracts the same `|dom_X(R)|` floor and rescales — but is **not** gated on,
because Parciak et al. report it sensitive to RHS-skew where `mu+` is not. Reporting both
makes the choice auditable rather than asserted.

C3 is strictly conservative: it can only reject candidates the current miner emits, never
admit new ones.

## The independent corpus, and its declared weakness

`rwd` (Parciak et al., MIT, Zenodo record 8098909, GitHub
`UHasselt-DSI-Data-Systems-Lab/paper-afd-comparative-study`): **143 human-annotated
ground-truth FDs across 10 real tables**, with derived `rwd_e` (errors injected) and
`syn_u` (an LHS-uniqueness sweep — the exact axis under test).

This satisfies the reversal criterion recorded in `DECISIONS.md` verbatim: a corpus with
naturally-occurring dependencies and retained ground truth, not one whose false
dependencies we injected ourselves. That earlier refusal — "validating it against a corpus
whose false dependencies we injected validates the injector" — stands, and `rwd` is not
that.

**Two weaknesses declared before measuring, not after.**

- **The negatives are constructed, not annotated.** `ground_truth.csv` is positives-only.
  Negatives require a closed-world assumption: every candidate the miner emits that is not
  in the annotated set, and not implied by it, is treated as false. Annotators who listed 2
  FDs for `adult.csv` did not necessarily enumerate every implied one, so **the negative
  label set carries unquantified noise, and it biases toward finding false positives.** Any
  result must state this. The implication closure is code, not prose, so it is reproducible.
- **`rwd`'s `hospital.csv` and `tax.csv` are almost certainly not the RAHA tables of the
  same names** — roughly 30.6 MB and 73.0 MB against RAHA hospital's ~1,000 rows. They are
  treated as independent tables and the non-correspondence is **verified, not assumed**. If
  they turn out to overlap, the independence claim is void and this document is amended.

## Predictions

| # | Quantity | Prediction |
| --- | --- | --- |
| **P1** | `mu+` on hospital's 85 non-vacuous candidates | `mu+ > 0` excludes every false dependency that `tested_confidence >= 0.9599` excludes, **retaining all 69 true ones**, with no constant. |
| **P2** | the four dependencies added by the shipped floor | `ZipCode -> HospitalName` and `ZipCode -> HospitalOwner` score `mu+ = 0` or near it, because their determinant groups are mostly singletons. This is the mechanism, so if it fails the whole argument fails. |
| **P3** | hospital write exposure under C3 | `corrupted_a_clean_cell` falls below **116**, and `repaired_a_real_error` stays at **451**. Strongly predicted, since the four added dependencies were already measured repairing **nothing** in either arm — there was no trade to weigh. |
| **P4** | `rwd` | `mu+ > 0` separates annotated-true from constructed-false dependencies on **at least 6 of the 10** tables, and does not invert on any. |
| **P5** | parameters | **zero new constants**, verifiable by inspecting the diff. The threshold is 0 and 0 is not fitted. |
| **P6** | coverage | falls somewhere. `missing_value`'s 427 flights writes and `fd_violation`'s 451 hospital-mined writes are the figures most at risk and are reported before and after on every corpus. |

**Uncertainty stated plainly, and this is the part I expect to be wrong about.** `mu+ > 0`
is a **weak** threshold. A false dependency needs only to beat its permutation expectation
by any margin to pass, and there is no guarantee the four damaging hospital dependencies
fail it. P1 and P2 are structural arguments from the closed form; I have computed neither.
P4 is the least certain of all — `rwd` has never been run through this miner and its tables
are far larger than anything measured here.

## Kill criteria, fixed now

**Revert, do not retune, if any of:**

- **K1** `mu+ > 0` discards any dependency that is true on hospital's clean frame. A gate
  that loses true dependencies to gain precision is the net-harm case, not a win.
- **K2** `mu+ > 0` fails to exclude the false dependencies that caused the measured
  corruption, i.e. P2 is refuted. Then `mu+` may be a better *ranking* signal than
  `tested_confidence` and is **still not a gate**, and this document says so rather than
  looking for a cut point.
- **K3** separation fails on `rwd`, or inverts on any table. Then `mu+` is not
  corpus-general on this miner's candidate space, the second-corpus route is back, and the
  gate is not shipped.
- **K4 — inherited verbatim, and the one most likely to bind.** Satisfying K1 or K2 requires
  introducing a tunable constant. **A threshold `mu+ > c` for any `c != 0` is exactly the
  fitted constant this document exists to avoid, and reaching for it is the failure, not the
  fix.** The correct response is to abandon C3 and publish that `mu+` does not gate.

**Net-harm rule, inherited.** Precision bought by writing nothing is not an improvement. If
`net_cells_improved` decreases on any corpus, the change is reported as a trade for the user
to accept explicitly, not shipped as a win.

**Measurement-validity rule, added here.** Every figure is produced by importing the shipped
write path, never by reimplementing it. `PRODUCT.md` records that a reimplementation of
`_write_exposure` reported 959 writes where the truth was 74 and nearly published a finding
that writes are 95% no-ops. A shorter local loop is evidence against a measurement.

## Scope and reporting

All four existing corpora — hospital, flights, rayyan, tax — plus `rwd`. Per-corpus, never
pooled: pooling is what let a dirty control class hide behind easy plants in the label-noise
work. Because C3 has no parameter, no corpus is used to *choose* anything; there is nothing
to choose. That is the strongest available answer to the overfitting objection, and it is
why the criterion was written before any number was computed.

Reported together, never precision alone: `proposals`, `repaired_a_real_error`,
`corrupted_a_clean_cell`, `wrong_value_on_a_real_error`, `net_cells_improved`, and coverage.

## AMENDMENT 1 (2026-09-01)

**Recorded after implementing the measure and BEFORE running it on any corpus.** No
candidate has been scored on hospital, flights, rayyan, tax or `rwd`. This is a deviation
in what the measure can do, not an interpretation of a result. The original text above is
left unchanged so the change stays visible.

### `mu+` has a blind spot the document above did not anticipate

`mu+` **does not penalise an exact dependency for being unfalsifiable.** For an exact
dependency `pdep(X->Y) == 1`, so the numerator `(1 - pdep(X->Y))` is zero and the
singleton-correction factor `(N-1)/(N-|dom_X|)` � however large � is multiplied by zero.
`mu+` returns 1.0 whether the dependency rests on two testable rows or two thousand. `g3'`
shares the property, for the same reason: both are error-based measures normalised so that
zero measured error scores 1.

This was found by writing a unit test that asserted the opposite and watching it fail. It is
recorded here rather than quietly accommodated because it narrows C3.

### What it changes

- **P1 and P2 are narrowed.** C3 can only discriminate among **approximate** dependencies.
  The prediction still stands for hospital, because its measured false dependencies carry
  `confidence` between 0.9050 and 0.9620 and are therefore approximate � the correction does
  bite on them. But P2's mechanism is now stated precisely: `ZipCode -> HospitalName` is
  expected to fail C3 **because it has violations concentrated in a few testable rows**, not
  merely because its determinant is sparse. If it turns out to be exact on the rows present,
  C3 will not reject it and P2 is refuted.
- **A new kill criterion, K5.** If the dependencies responsible for the measured corruption
  are *exact* on the dirty data, `mu+` cannot reject them, C3 does not address the observed
  damage, and the honest report is that the measure does not gate this failure. The response
  is not to add a support threshold on top � that is K4 again.
- **The pre-existing near-key guard is load-bearing and stays.**
  `_MAX_DETERMINANT_UNIQUE_FRACTION = 0.9` is what rejects an exact dependency on a
  near-unique determinant. It is a constant that predates C3, C3 does not remove it, and P5
  ("zero new constants") must not be read as implying the old ones became unnecessary.

### What it does not change

P5 still holds: C3 introduces no constant. The threshold remains 0 and 0 remains derived
from the permutation null. K4 is unaffected and is still the criterion most likely to bind.

## AMENDMENT 2 (2026-09-07)

**Recorded after the corpus was checksum-verified and the measurement re-run.** This amendment
does not reinterpret a result; it records that the first result was computed against the wrong
input, and what changed when that was fixed. The text above is left unchanged, including the
statements this amendment falsifies, so the correction stays visible.

### The corpus was not the corpus this document cites

The local `included_candidates.csv` held **1,170** candidates in a 68,995-byte file. The
published artifact on Zenodo record 8098909 holds **1,262** in 79,075 bytes. The candidate
universe *defines the negative label set*, so every separation count in
`docs/trust/premise-quality-measure-result.md` was computed against an unverified premise — in
a document about unverified premises.

It was found by adding checksum verification (`scripts/bench/fetch_rwd_corpus.py`), not by
re-reading anything, and `docs_truth` then refused the stale prose. Both halves matter: a
checksum found it, and a claim ledger stopped it being quietly overwritten.

**Also corrected: the licence.** This document states "MIT" at line 104. The Zenodo record
states **CC-BY-4.0**. The wrong value had propagated to the measurement script, the emitted
artifact and the result document. Line 104 stands, per the no-edit rule; the three live
surfaces are fixed.

### A bias in the candidate universe that was not known when C3 was written

`excluded_candidates.csv` was not read before. The authors excluded a candidate when no tuple
had both attributes present **or when its `g3_prime` value was too small**. The universe is
therefore already `g3'`-filtered, which truncates the low end of the `g3_prime` distribution
and flatters every error-based measure normalised the same way — `g3_prime` directly, and
`mu+` by construction. This **weakens** the case for a gate and was working in C3's favour
undetected.

### What changed, and one thing that changed against us

All ten tables are now downloaded and verified, where this document could previously speak to
three. Counts moved; **no separation verdict changed**:

| figure | unverified | verified |
| --- | --- | --- |
| hospital candidates | 74 | 75 |
| hospital `mu+` false-above-zero | 45/45 | 46/46 |
| dblp10k candidates | 567 | 620 |
| dblp10k `mu+` false-above-zero | 356/490 | 407/543 |
| dblp10k `g3'` false-above-zero | 466/490 | **543/543** |
| adult candidates | 116 | 111 |

The `g3'` row is the one to read. On the unverified universe `g3'` had *some* discrimination on
dblp10k; on the published one it has **none**. That is a larger movement than `mu+`'s and it
runs against the measure this document reports alongside `mu+`, which is the direction a
correction should be checked in rather than the direction that flatters the conclusion.

### P4 is now measurable, and is refuted

P4 predicted `mu+ > 0` separating on "at least 6 of the 10" tables. Measured across all ten:
**`mu_plus` separates perfectly on 4**, the other three measures on 3. P4 joins P1 and P2 as
refuted. The verdict of the document — C3 refused, `mu+` retained as a reported field — is
unchanged and now rests on the full corpus rather than on a three-table subset.

Worse for any future gate than the count: on `claims`, `tax` and
`t_biocase_identification_r91800` the ordering **inverts**, with an annotated-false candidate
scoring the maximum 1.0 at or above some annotated-true candidate. No threshold can separate a
class whose maximum carries the wrong label. That is the evidence
`eval/preregistration/premise_acquisition.md` was written to test properly.
