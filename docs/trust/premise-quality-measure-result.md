# Premise quality: mu+ does not gate, and the reason is a small-sample artifact

Executes `eval/preregistration/premise_quality_measure.md` and its AMENDMENT 1 and 2.
Artifact: `eval/results/premise_quality_rwd.json`. Reproduce with
`python scripts/bench/fetch_rwd_corpus.py` followed by
`python scripts/bench/measure_premise_quality_rwd.py`.

> **CORRECTED 2026-09-07, and the correction is worth more than the numbers it changed.**
> The first run of this measurement used a local `included_candidates.csv` that **did not match
> the Zenodo record this document cites**: 1,170 candidates against the published 1,262, a
> 68,995-byte file against 79,075. The candidate universe defines the negative label set, so
> every count below was computed against a premise that was itself unverified -- in a document
> whose entire subject is unverified premises. It was caught by adding checksum verification
> (`scripts/bench/fetch_rwd_corpus.py`) rather than by re-reading anything, and `docs_truth`
> then refused the stale prose, which is the gate working as designed.
> The corpus is also **CC-BY-4.0, not MIT** as three places in this repository stated.
> **All 10 tables are now downloaded and checksum-verified**, where this document previously
> reported 3. The separation verdicts below did not change. The counts did.

**Verdict: C3 is refused. Two kill criteria fired and the gate is not shipped.** The measure
stays a reported field, which is where it already was. What follows is why, and the mechanism
turns out to be more informative than the gate would have been.

## What was measured

`rwd` (Parciak et al., ICDE 2024, arXiv:2312.06296; CC-BY-4.0; Zenodo 8098909) supplies 143
hand-annotated true dependencies over 10 real tables **and** the 1,262-candidate universe
those annotations were made against. All ten tables are now scored:

| table | rows | candidates scored | annotated true | annotated false |
| --- | --- | --- | --- | --- |
| `hospital.csv` | 114,919 | 75 | 29 | 46 |
| `dblp10k.csv` | 10,000 | 620 | 77 | 543 |
| `adult.csv` | 32,561 | 111 | 2 | 109 |
| `claims.csv` | 97,231 | 58 | 4 | 54 |
| `tax.csv` | 1,000,000 | 95 | 3 | 92 |
| `t_biocase_identification_r91800_c38.csv` | 91,799 | 116 | 14 | 102 |
| `t_biocase_gathering_r90992_c35.csv` | 90,991 | 79 | 1 | 78 |
| `t_biocase_gathering_agent_r72738_c18.csv` | 72,737 | 62 | 7 | 55 |
| `t_biocase_gathering_namedareas_r137711_c11.csv` | 137,710 | 44 | 5 | 39 |
| `t_biocase_identification_highertaxon_r562959_c3.csv` | 562,958 | 2 | 1 | 1 |

Every candidate resolved on every table: `candidates_unresolved` is 0 for all ten, so no
count above is a column-name mismatch reading as a measurement.

Measures are imported from `dataforge.premise_quality`, the module the miner uses. Nothing
was reimplemented.

**An improvement on the pre-registration's declared weakness.** It warned that negatives
would need a closed-world assumption over *our* miner's output. They did not: the authors
published `included_candidates.csv`, so the negative set is **their** closed world, defined
before we arrived. The label noise that remains is theirs and is stated in their paper, not
ours.

**One bias in that universe, now that `excluded_candidates.csv` has been read.** The authors
excluded a candidate when no tuple had both attributes present **or when its `g3_prime` value
was too small**. The candidate universe is therefore already `g3'`-filtered, which truncates
the low end of the `g3_prime` distribution and flatters any `g3'`-derived measure -- including
`mu+`, which shares the same error-based normalisation. This was not known when C3 was
pre-registered and it weakens, not strengthens, the case for a gate.

## The result that refutes P1 and P2

P2 predicted that `ZIPCode -> HospitalName` and `ZIPCode -> HospitalOwner` — the two
dependencies that caused 23 of 25 sampled corruptions on RAHA hospital — would score
`mu+ = 0` or near it. Measured:

| candidate | annotated | `mu+` | `confidence` |
| --- | --- | --- | --- |
| `ZIPCode -> HospitalName` | **false** | **0.9064** | 0.9105 |
| `ZIPCode -> HospitalOwner` | **false** | **0.9140** | 0.9347 |
| `ZIPCode -> City` | true | 1.0000 | 1.0000 |

Not near zero. A gate at `mu+ > 0` admits both, and **46 of 46 annotated-false candidates on
hospital score above zero.** P1 and P2 are refuted as stated.

### Why, and this is the part worth keeping

The singleton-inflation mechanism is **a small-sample artifact, not a property of the
dependency.** On RAHA hospital — 1,000 rows — ZIP codes are nearly unique, so most
determinant groups are singletons, `|dom_X|` approaches `N`, and the correction factor
`(N-1)/(N-|dom_X|)` is large. On rwd hospital — 114,919 rows of the same schema — ZIP codes
repeat heavily, the groups are well populated, the correction is negligible, and `mu+`
collapses toward `confidence`.

So `mu+` corrects a defect that **the small corpus had**. Given enough rows, a false
dependency stops looking unfalsifiable and starts looking like what it is: an approximate
dependency at 0.91 confidence. And distinguishing "approximate because the data is dirty"
from "approximate because the dependency is false" is **Q1**, which
`eval/preregistration/premise_quality.md` conceded as undecidable in-table and which no
measure in this family addresses.

That reframes the original hospital result. `tested_confidence` separated true from false
there not because it captures dependency validity, but because at 1,000 rows the false
dependencies happened to be the ones with sparse determinant groups. It was a real signal
about that corpus. It was not the general signal it looked like.

## K1 fires: a true dependency scores zero

On dblp10k, exactly one annotated-true dependency scores `mu+ = 0.0`:

| candidate | annotated | `mu+` | `confidence` | determinant distinct | rows |
| --- | --- | --- | --- | --- | --- |
| `p1booktitle -> p1publisher` | **true** | **0.0000** | 0.9985 | 1,920 | 10,000 |

A gate at `mu+ > 0` would discard a dependency the annotators judged real, at confidence
0.9985. **K1 fired.** One is enough: the kill criterion was written as "any dependency that
is true", not "an acceptable fraction".

*Uncertain, and flagged as such:* Parciak et al. report `mu+` insensitive to RHS-skew, and
`p1publisher` is heavily skewed. Whether this case is an extreme of that axis, a defect in
this implementation, or an annotation the authors would revise, is **not established here.**
It would need the authors' own `mu+` values on the same candidate to separate those
explanations, and `included_candidates.csv` publishes `g3` only.

## What `mu+` is actually worth: the best ranking signal of the four

This is the outcome K2 anticipated in advance — "`mu+` may be a better *ranking* signal than
`tested_confidence` and is **still not a gate**."

Annotated-false candidates scoring above zero, lower is better:

| measure | hospital | dblp10k | adult |
| --- | --- | --- | --- |
| `mu_plus` | 46/46 | **407/543** | **104/109** |
| `g3_prime` | 46/46 | 543/543 | 109/109 |
| `confidence` | 46/46 | 543/543 | 109/109 |
| `tested_confidence` | 46/46 | 543/543 | 109/109 |

Highest-scoring false candidate, lower is better:

| measure | hospital | dblp10k | adult |
| --- | --- | --- | --- |
| `mu_plus` | **0.9694** | 1.0000 | **0.8981** |
| `g3_prime` | 0.9941 | 1.0000 | 0.9696 |
| `confidence` | 0.9943 | 1.0000 | 0.9898 |
| `tested_confidence` | 0.9943 | 1.0000 | 0.9807 |

`mu+` is the only measure with any discrimination on dblp10k, and it leaves the widest margin
on the two tables where separation exists. It earns its place as a reported field beside
`tested_confidence`. It does not earn a gate.

**The correction cost `g3'` its only claim to discrimination.** On the unverified universe it
scored 466/490 on dblp10k; on the published one it scores **543/543** — no discrimination at
all. That is a larger change than the `mu+` one and it runs against the measure this document
reports alongside `mu+`, which is the direction a correction should be checked in.

### All ten tables, which is what the refusal now rests on

Perfect separation by *some* threshold, per measure, across the full corpus:

| measure | tables with perfect separation |
| --- | --- |
| `mu_plus` | **4 of 10** — adult, hospital, `t_biocase_gathering_r90992`, `t_biocase_identification_highertaxon` |
| `g3_prime` | 3 of 10 |
| `confidence` | 3 of 10 |
| `tested_confidence` | 3 of 10 |

The original P4 predicted separation "on **at least 6 of the 10** tables". Measured: **4**.
**P4 is refuted**, and it is refuted on the full corpus rather than on the three-table subset
that was all this document could previously speak to.

Worse for the gate than the count: on `claims`, `tax` and `t_biocase_identification_r91800`
the ordering **inverts** — an annotated-false candidate scores the maximum 1.0 while some
annotated-true candidate scores at or below it. No threshold, fitted or otherwise, can
separate a class whose maximum belongs to the wrong label.

## K4 is the criterion that matters, and it holds

Perfect separation **does** exist on hospital and adult — `mu+` min-true 0.9967 against
max-false 0.9694 on hospital, 1.0 against 0.8981 on adult. A threshold near 0.98 would
separate both perfectly, and it is worth naming plainly that this would look like a clean
result in any table published here.

It is refused, for the reason fixed in advance:

- The constant would be chosen **after** seeing which side of it the false dependencies fell
  on. That is the definition of the fitted parameter K3 and K4 forbid.
- It would not survive dblp10k, where no threshold separates at all: min-true 0.0 sits below
  max-false 1.0.
- The same temptation was refused once already, for `tested_confidence >= 0.9599`. Refusing
  it there and accepting it here for a different statistic would be the same error wearing a
  new measure.

## What did not change, and one thing that got better

- **The reviewer surface improves.** `mu+` and `g3'` are reported per candidate, in the
  machine-readable summary and as a column in the review table. On hospital they would have
  ranked the two damaging dependencies at 0.906 and 0.914 against 1.0 for the true ones —
  informative to a human, insufficient for a machine.
- **`_MAX_DETERMINANT_UNIQUE_FRACTION` remains load-bearing**, per AMENDMENT 1.
- **No constant was introduced.** P5 holds, and it holds by refusing rather than by passing.

## Independent corroboration worth recording separately

Before any measure ran, the annotations answered a question this project had asked on its
own evidence. `docs/trust/shipped-premise-result.md` argued that `ZipCode -> HospitalName` is
false — "a zip code does not determine a hospital name" — from 25 sampled corruptions on a
1,000-row table.

The `rwd` annotators, independently, on a 114,919-row instance of the same schema, annotated
`ZIPCode -> City`, `-> State` and `-> CountyName` as true and **omitted `-> HospitalName` and
`-> HospitalOwner`**. Same verdict, different people, 115x the rows, published in a
peer-reviewed venue before this project looked.

**Also verified, and it contradicts what this document's pre-registration assumed.** It
predicted `rwd`'s `hospital.csv` was "almost certainly not the RAHA table", from file size.
Wrong: **15 of 17 RAHA hospital columns are present** (only `Address1` and `Score` are
absent, and `rwd` has no column RAHA lacks). They are different instances of one schema, not
different tables. Recorded here because the prediction was checkable and was checked.

## Limitations

- **Three of ten tables.** `claims`, `tax` and five `t_biocase_*` tables were not downloaded;
  together they hold 35 of the 143 annotations. The refutation does not need them — K1 and K2
  fired on what was measured — but no claim here is a claim about all of `rwd`.
- **adult carries 2 annotated-true candidates.** Its separation numbers rest on two points
  and should not be read as a result about adult.
- **Annotation noise is real and is theirs.** The authors annotated design FDs by hand; a
  dependency they omitted is not thereby proven false.
- **No write-exposure arm was run.** P3 predicted corruption falling from 116 with repairs
  held at 451, and it is **not tested**, because P3 only matters if the gate ships and the
  gate does not. Reporting P3 as untested is the accurate outcome, not a gap.
- **`mu+` was measured on rwd, not on RAHA hospital's own 85 candidates.** The RAHA arm would
  test the small-sample regime where the correction does bite, and the mechanism above
  predicts `mu+` would separate there — which is exactly why it is not evidence for a gate.
  A measure that works only where the corpus is small is not a gate; it is a description of
  that corpus.
