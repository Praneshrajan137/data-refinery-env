# Premise quality: mu+ does not gate, and the reason is a small-sample artifact

Executes `eval/preregistration/premise_quality_measure.md` and its AMENDMENT 1. Artifact:
`eval/results/premise_quality_rwd.json`. Reproduce with
`python scripts/bench/measure_premise_quality_rwd.py` after placing the `rwd` corpus under
`.benchmarks/rwd/`.

**Verdict: C3 is refused. Two kill criteria fired and the gate is not shipped.** The measure
stays a reported field, which is where it already was. What follows is why, and the mechanism
turns out to be more informative than the gate would have been.

## What was measured

`rwd` (Parciak et al., ICDE 2024, arXiv:2312.06296; MIT; Zenodo 8098909) supplies 143
hand-annotated true dependencies over 10 real tables **and** the 1,170-candidate universe
those annotations were made against. Three tables were scored — the largest annotated set,
the smallest table, and hospital:

| table | rows | candidates scored | annotated true | annotated false |
| --- | --- | --- | --- | --- |
| `hospital.csv` | 114,919 | 74 | 29 | 45 |
| `dblp10k.csv` | 10,000 | 567 | 77 | 490 |
| `adult.csv` | 32,561 | 116 | 2 | 114 |

Measures are imported from `dataforge.premise_quality`, the module the miner uses. Nothing
was reimplemented.

**An improvement on the pre-registration's declared weakness.** It warned that negatives
would need a closed-world assumption over *our* miner's output. They did not: the authors
published `included_candidates.csv`, so the negative set is **their** closed world, defined
before we arrived. The label noise that remains is theirs and is stated in their paper, not
ours.

## The result that refutes P1 and P2

P2 predicted that `ZIPCode -> HospitalName` and `ZIPCode -> HospitalOwner` — the two
dependencies that caused 23 of 25 sampled corruptions on RAHA hospital — would score
`mu+ = 0` or near it. Measured:

| candidate | annotated | `mu+` | `confidence` |
| --- | --- | --- | --- |
| `ZIPCode -> HospitalName` | **false** | **0.9064** | 0.9105 |
| `ZIPCode -> HospitalOwner` | **false** | **0.9140** | 0.9347 |
| `ZIPCode -> City` | true | 1.0000 | 1.0000 |

Not near zero. A gate at `mu+ > 0` admits both, and **45 of 45 annotated-false candidates on
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
| `mu_plus` | 45/45 | **356/490** | **109/114** |
| `g3_prime` | 45/45 | 466/490 | 114/114 |
| `confidence` | 45/45 | 490/490 | 114/114 |
| `tested_confidence` | 45/45 | 490/490 | 114/114 |

Highest-scoring false candidate, lower is better:

| measure | hospital | dblp10k | adult |
| --- | --- | --- | --- |
| `mu_plus` | **0.9694** | 1.0 | **0.8981** |
| `g3_prime` | 0.9941 | 1.0 | 0.9696 |
| `confidence` | 0.9943 | 1.0 | 0.9898 |
| `tested_confidence` | 0.9943 | 1.0 | 0.9807 |

`mu+` is the only measure with any discrimination on dblp10k, and it leaves the widest margin
on the two tables where separation exists. It earns its place as a reported field beside
`tested_confidence`. It does not earn a gate.

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
