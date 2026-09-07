# Premise acquisition: no in-table measure transfers, so authority must follow provenance

Executes the H1 half of [eval/preregistration/premise_acquisition.md](../../eval/preregistration/premise_acquisition.md).
Artifact: `eval/results/premise_acquisition_h1.json`. Reproduce with

```
python scripts/bench/fetch_rwd_corpus.py
python scripts/bench/measure_premise_quality_rwd.py
python scripts/bench/measure_premise_acquisition_h1.py
```

**Verdict: H1 stands. K1 did not fire. No measure separates true from false dependencies on
every held-out table, and every measure destroys true dependencies trying.**

This is the fourth consecutive refusal of an in-table premise gate, and the first one that is
not a refusal about a single corpus. The three before it — `confidence`, `tested_confidence`,
`mu+` — were each refused because a threshold fitted on hospital could not be shown to transfer.
This one measures the transfer directly, across ten annotated tables, and finds it absent.

## What was done, and why it is not the same experiment again

`rwd` (Parciak et al., ICDE 2024, arXiv:2312.06296; CC-BY-4.0; Zenodo 8098909) supplies 143
hand-annotated true dependencies over ten real tables, plus the 1,262-candidate universe the
annotations were made against — so the negatives are the authors' published closed world, not
ours. All ten tables are checksum-verified, and **10 of 10 are foldable** (both label classes
non-empty), so K5's minimum of four is satisfied and this is a validation rather than a gesture.

For each measure and each held-out table: fit a threshold on the other nine by maximising
Youden's J, then apply it to the tenth. A gate admits a candidate when `score > threshold`.

**The fit is deliberately generous.** Nothing about the threshold rule is tuned to produce a
refusal — J is symmetric in the two error types and needs no cost ratio, and ties break toward
the more conservative gate. H1 is the claim that even a *well-fitted* threshold does not
transfer, so the experiment is only worth anything if the fitting is done in good faith. Had any
measure survived every fold, K1 would have fired, H1 would be falsified, and the correct output
would have been to ship that measure and abandon the constructive half of the pre-registration.

## The result

| measure | clean folds | folds losing no true FD | true dependencies discarded |
| --- | --- | --- | --- |
| `mu_plus` | **4 / 10** | 6 / 10 | **16** |
| `g3_prime` | 2 / 10 | 5 / 10 | 17 |
| `tested_confidence` | 2 / 10 | 5 / 10 | 18 |
| `confidence` | 1 / 10 | 3 / 10 | 23 |

A "clean" fold is one where the transferred gate loses no annotated-true dependency **and**
admits no annotated-false one. The best measure manages that on four tables out of ten.

The second column is the one that should end the search. `mu_plus`, the best of the four,
**discards 16 of the 143 hand-annotated true dependencies** when its threshold is carried to a
table it was not fitted on. `confidence` — the measure the shipped miner actually uses for its
0.9 emission floor — discards 23.

## Where it fails, and why the shape matters

`mu_plus` per fold. The threshold is stable: 0.991374 on nine of ten folds.

| held-out table | threshold | true lost | false admitted | of |
| --- | --- | --- | --- | --- |
| `adult` | 0.991374 | 0 | 0 | 2T / 109F |
| `hospital` | 0.991374 | 0 | 0 | 29T / 46F |
| `t_biocase_gathering_r90992` | 0.991374 | 0 | 0 | 1T / 78F |
| `t_biocase_identification_highertaxon` | 0.991374 | 0 | 0 | 1T / 1F |
| `tax` | 0.991374 | 0 | 1 | 3T / 92F |
| `t_biocase_identification_r91800` | 0.991374 | 0 | 5 | 14T / 102F |
| `claims` | 0.991374 | 1 | 12 | 4T / 54F |
| `t_biocase_gathering_namedareas` | 0.991374 | **3** | 3 | **5T** / 39F |
| `t_biocase_gathering_agent` | 0.991374 | **4** | 4 | **7T** / 55F |
| `dblp10k` | 0.914047 | **8** | 46 | 77T / 543F |

**A single threshold, clean on hospital, discards 4 of 7 true dependencies on
`t_biocase_gathering_agent` and 3 of 5 on `namedareas`** — 57% and 60% of everything the
annotators identified on those tables.

That is the finding, and it is stronger than "the threshold does not transfer". The threshold
barely moves; **the tables move under it.** `mu+`'s correction term is a function of
`|dom_X| / N`, so what the measure reads is the shape of the determinant-group distribution,
which is a property of the instance. A dependency does not become true or false because its
table is wide, or long, or sparsely keyed. The measure is reading the corpus and the label is
about the dependency, and no amount of threshold-fitting reconciles those two things.

This is the mechanism `premise-quality-measure-result.md` identified from a single contrast
between 1,000-row and 114,919-row hospital, now demonstrated across ten tables with the
label sets held out.

## What this licenses, and what it does not

**It licenses abandoning the search.** Four measures, four refusals, and now a direct
measurement of the property all four were assumed to have. Proposing a fifth statistic over the
miner's output is not a research direction; it is the same experiment with a new name. The
pre-registration's K4 — "reaching for a constant is the failure, not the fix" — has now been
paid four times.

**It does not license the claim that no such function exists.** Seven features were tested, not
the space of all functions of a table. A learned model over many features, or a measure keyed on
something other than group-size distribution, is untested here. What is established is narrower
and sufficient: **the measures this repository actually computes do not transfer, and the best
of them destroys true dependencies at rates up to 60% per table when carried across.**

**It does not by itself validate C4.** `rwd` supplies annotations, not dirty/clean pairs, so it
can test H1 and cannot test P3-P5. The write-exposure arm runs on the four existing corpora and
is reported separately. Conflating the two would be the scoping error this project has made
before.

## The consequence for the product

`ConstraintReviewArtifact.to_schema()` applies no confidence floor, and this result says a floor
would not have helped: the statistic a floor would be applied to does not carry the signal. The
116 clean-cell corruptions on hospital were never going to be fixed by a better number.

So the remaining lever is not *how confident* the miner is, but *where the constraint came
from*. That is C4, and this document is the evidence that the alternative was exhausted rather
than merely unattractive.

### C4 as shipped, and what implementing it revealed

Write authority now derives from the **declared** schema. A mined constraint accepted in review
still drives detection and verification; it confers no right to write. The opt-out is
`--trust-mined-constraints` (`mined_constraints_grant_write_authority`), and the receipt names
the hold `mined_constraint_not_declared` rather than the previous `floor_cannot_verify` -- which
would have been a lie, because a schema exists and the constraint simply was not declared.

**Two things implementation exposed that the pre-registration did not anticipate.**

First, narrowing authority is **not sufficient on its own**. `verification_strength_for` treats
a `deterministic` fix as proven by construction, independent of which columns the schema covers,
so an FD repair is unaffected by the authority set. Both a narrowed authority set (which gates
untrusted provenance) and a declared-FD requirement (which gates the deterministic FD path) are
needed. This was found by writing a test that asserted the write did not happen and watching it
happen anyway -- not by reading the code, which had looked sufficient.

Second, and more usefully: **the entire pre-existing test suite passed unchanged when C4 was
switched on.** That was not evidence of safety; it was evidence that nothing in 2,753 tests
exercised the mined-premise write path as a subject. Twelve tests then failed once the
mechanism was complete, and their names are the old specification -- including
`test_accepted_inferred_fd_auto_applies_by_default`. A change no test can see is
indistinguishable from no change, which is why
`tests/integration/test_premise_provenance_authority.py` asserts both directions.

**The cost is real and is recorded in PRODUCT.md 1.5 rather than buried here.** The default now
writes nothing on a table with no declared schema, the playground's guardrail demo shows uniform
refusal instead of a proven-vs-blocked split, and `independent_verification` reports `not_run`
where no value is a write candidate. "Zero writes is not a safety result" applies to this change
first.

## Limitations

- **The negative labels are the authors' and still noisy.** They annotated design FDs by hand;
  an omitted dependency is not proven false. That noise inflates "false admitted" and is not
  symmetric with "true discarded", which is the column this document leans on.
- **The candidate universe is already `g3'`-filtered.** The authors excluded candidates whose
  `g3_prime` was too small, which truncates the low end and *flatters* every error-based
  measure here. The refusal is therefore measured under conditions favourable to the measures.
- **Youden's J is one fitting rule.** A cost-weighted rule would trade the two error columns
  differently. It would not change that `mu_plus` needs threshold 0.914 to be clean on dblp10k
  and 0.991 elsewhere, nor that no single value is clean on all ten.
- **`t_biocase_identification_highertaxon` carries 1 true and 1 false candidate.** Its clean
  fold is arithmetic, not evidence, and should not be read as a result about that table.
- **No claim here is about detection**, or about any dependency this repository's own miner
  would emit — the candidates are the authors'.
