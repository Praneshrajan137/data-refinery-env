# Pre-registration: is premise validity decidable from the table, or must it be acquired?

Written 2026-09-07, **before** loading a new table, implementing a rule, or computing any
quantity. Nothing above the amendment line is edited afterwards; results and deviations are
appended as amendments.

This continues `premise_quality.md` (C1, C2) and `premise_quality_measure.md` (C3). Read both
first. C3 was **refused**: K1 fired when `p1booktitle -> p1publisher`, annotated true at
confidence 0.9985, scored `mu+ = 0.0` on dblp10k, and K2 was refuted when
`ZIPCode -> HospitalName` and `ZIPCode -> HospitalOwner` — the two dependencies responsible
for 23 of 25 sampled corruptions — scored 0.9064 and 0.9140 rather than near zero.

The published mechanism is the reason this document exists:

> `mu+` corrects a defect that **the small corpus had**. Given enough rows, a false dependency
> stops looking unfalsifiable and starts looking like what it is: an approximate dependency at
> 0.91 confidence.

That reframes three prior results at once. `tested_confidence` separated perfectly on hospital
not because it captures dependency validity, but because at 1,000 rows the false dependencies
happened to have sparse determinant groups. **Every in-table statistic tried so far has been
measuring the corpus, not the dependency.**

## The question this document actually asks

Four measures have now been refused as gates, each for its own reason, each after the fact.
Continuing to propose a fifth is the K4 failure wearing a new statistic. So the question is
raised one level:

> **Is premise validity decidable from the table at all — by any parameter-free function of
> the observable marginals — or is it a property of the constraint's provenance?**

This is falsifiable in both directions, and the answer changes what gets built. If it is
decidable, the correct work is to ship the measure that decides it. If it is not, then no
amount of statistical work on the miner's output can help, and the authority to write must
stop being a function of the miner's confidence.

## H1 — the negative half, and how it can fail

> **H1.** No parameter-free function of `(confidence, tested_confidence, mu_plus, g3_prime,
> |dom_X|, N, support_groups)` separates annotated-true from annotated-false dependencies
> across **held-out tables**.

Tested by **leave-one-table-out** on `rwd`. For each measure and each held-out table T:

1. Choose the threshold that maximises separation on all tables **except** T. This threshold
   is fitted deliberately and openly — the point is to give the in-table hypothesis its very
   best shot, not to defend a refusal.
2. Apply that threshold to T, which was not seen when choosing it.
3. Record whether it separates on T, and whether it discards any annotated-true dependency.

**This is the fold structure the previous three attempts could not run**, because they had one
corpus and a fitted constant on one corpus is indistinguishable from a memorised one. Ten
annotated tables permit fitting and testing on disjoint data, which is validation rather than
fitting. **The refusal recorded in `DECISIONS.md` five separate times — that shipping needs "a
second corpus with naturally-occurring false dependencies and retained ground truth" — is
therefore satisfied by `rwd`, and this document is the first to use it that way.**

H1 fails, cleanly and usefully, if any measure separates on **every** held-out fold while
discarding no annotated-true dependency. In that case C4 below is the wrong answer, the
measure is the right one, and this document says so.

## C4 — the constructive half

> **Write authority derives from a constraint's PROVENANCE, not from the miner's confidence
> in it.**

| Provenance | Authority |
| --- | --- |
| `declared` — written by a human in a schema file | write |
| `external` — annotated in an external ground-truth corpus | write |
| `derived` — read from a DDL, dbt schema, foreign key, or Iceberg table constraint | write |
| `mined` — discovered by this repository's miner, at any confidence, **including after human acceptance in the review UI** | review only; no unconditional write authority |

The last row is the whole change, and it is the row that will be argued with, so its
justification is stated before any measurement.

`ConstraintReviewArtifact.to_schema()` (`dataforge/schema_inference.py:177-207`) applies **no
confidence floor at all**; the gate on that path is explicit human acceptance. The implicit
premise is that a human accepting a mined candidate is equivalent to a human declaring a
constraint. **That premise has never been measured.**
`eval/preregistration/reviewer_decision_quality.md` was written to measure exactly it and has
never been run, because it requires recruited human reviewers. So the strongest claim available
today is that acceptance-equals-declaration is an *untested assumption* sitting directly
upstream of every measured corruption: all 116 clean-cell corruptions on hospital trace to
four false mined dependencies that passed review.

C4 is parameter-free **by construction, not by argument**. It introduces a categorical
partition over provenance, not a threshold over a statistic. There is no constant to fit, so
K4 — the criterion that killed C3 and C2 — cannot bind. That is the point of preferring it to a
fifth measure.

## What C4 costs, stated before measuring it

C4 makes the default zero-config path **write less**, and on hospital it is expected to write
nothing at all unless the user declares or the table supplies a premise. PRODUCT.md is explicit
that this is not automatically a win:

> **Zero writes is not a safety result.**

So the honest framing of C4 is not "safer". It is a **relocation of capability**: the product
stops writing on its own guesses and starts writing on the user's stated premise. The measured
arms make the trade concrete — the mined premise produces 451 repairs with 116 corruptions
(precision 0.7954), while the oracle premise produces 393 repairs with **0** corruptions. The
capability was never in the miner; it was in the premise.

If that trade is unacceptable it must be rejected explicitly by a user, not silently retained
by a default. That is the disclosure this pre-registration exists to force.

## Predictions

| # | Quantity | Prediction |
| --- | --- | --- |
| **P1** | leave-one-table-out separation, all four measures | **Fails on at least one fold for every measure.** `confidence` and `tested_confidence` fail on nearly all folds (both already score 490/490 and 114/114 annotated-false above zero on dblp10k and adult). `mu_plus` is the most likely to survive several folds and still expected to fail on dblp10k, where min-true 0.0 sits below max-false 1.0. |
| **P2** | the shape of the failure | Failures are **table-dependent, not measure-dependent** — the same measure separates on small tables and fails on large ones, because the correction term that does the separating is a function of `|dom_X|/N`. If failures are instead spread evenly across tables, the small-sample explanation is wrong and P2 is refuted. |
| **P3** | hospital write exposure under C4 | `corrupted_a_clean_cell` falls from **116 to 0** on the default path, because mined dependencies stop carrying write authority. `repaired_a_real_error` on the default path falls from **451 to 0** for the same reason. The declared/oracle arm is **unchanged at 393 repairs / 0 corruptions**. This is the trade, not a win, and it is reported as one. |
| **P4** | `tax` | Currently 603 repairs / 40 corruptions from 4 mined FDs. Under C4 the default path writes **0**. Reported, not celebrated. |
| **P5** | flights, rayyan | **No change: 0 writes before, 0 after.** The miner finds no dependency, so there is nothing for C4 to withdraw. Any change here means C4 touched something it was not supposed to. |
| **P6** | parameters | **Zero new constants**, verifiable by inspecting the diff. C4 is a partition over an enum. |
| **P7** | `_MAX_DETERMINANT_UNIQUE_FRACTION` | Becomes **less** load-bearing on the write path, since mined FDs no longer write, but is **retained** — it still governs what the reviewer is shown, and removing a guard because another change made it redundant is how guards get lost. |

**Where I expect to be wrong.** P1 is the prediction I hold most loosely. `mu+` was the best
ranking signal of the four and it is entirely possible that leave-one-table-out separation
holds on 9 of 10 folds, which is a much more interesting result than a flat refutation and
would properly reopen the measure route. I have computed nothing. P3's "116 to 0" is a
structural consequence of removing write authority rather than a measurement, and if it comes
back non-zero then something else is granting authority and the finding is that, not C4.

## Kill criteria, fixed now

**Revert, do not retune, if any of:**

- **K1 — H1 is falsified.** Any measure separates on **every** held-out fold while discarding
  no annotated-true dependency. Then premise validity **is** decidable in-table, C4 is
  unnecessary, and the correct output is to ship that measure and delete this document's
  constructive half. This is the criterion that makes H1 a hypothesis rather than a posture.
- **K2 — C4 removes authority the evidence does not implicate.** If the declared/`external`
  arm's numbers move at all — hospital oracle must stay at exactly **393 repairs / 0
  corruptions** — then C4 has changed more than provenance and is withdrawn.
- **K3 — no fitted constant.** If satisfying K1 or K2 requires introducing a threshold on any
  statistic, that is the fitted constant C2 and C3 were refused for. Abandon C4 and publish
  the refusal. **Inherited verbatim and still the criterion most likely to bind.**
- **K4 — the trade is hidden.** If C4 ships without the user-visible statement that the default
  path now writes nothing on a mined premise, it is shipped dishonestly regardless of its
  numbers. "Zero writes is not a safety result" applies to this change first.
- **K5 — the fold structure is fake.** If fewer than **4** `rwd` tables carry enough annotated
  positives *and* constructed negatives to fit and test on disjoint data, leave-one-table-out
  is not a real validation and H1 is reported as **untested** rather than confirmed. A refusal
  supported by an inadequate fold is worth less than an honest "not measured".

**Net-harm rule, inherited.** Precision bought by writing nothing is not an improvement. If
`net_cells_improved` decreases on any corpus, the change is reported as a trade for the user to
accept explicitly, not shipped as a win. C4 is expected to trip this rule on hospital and
`tax`, and reporting it is the deliverable.

**Measurement-validity rule, inherited.** Every figure is produced by importing the shipped
write path, never by reimplementing it. A reimplementation of `_write_exposure` once reported
959 writes where the truth was 74. A shorter local loop is evidence against a measurement.

**K4 fence, inherited from `measure_deductive_coverage`.** hospital FD counts **53/81/85**,
repairs **393/451/451**, majority corruptions **0/86/116**, `replication_mismatches` **0**.
Any movement in the oracle column is K2 firing.

## Scope and reporting

- **Two arms declared now, so the conclusion's scope cannot be widened later.** The
  **3-table arm** (`hospital.csv`, `dblp10k.csv`, `adult.csv` — already downloaded) and the
  **10-table arm** (adding `claims`, `tax`, and five `t_biocase_*`, holding 35 of the 143
  annotations). If the remaining seven do not arrive, K5 governs and the reported conclusion
  is explicitly about three tables. **No result may be stated as a result about `rwd`.**
- Measures are imported from `dataforge.premise_quality`, the module the miner uses. Nothing is
  reimplemented.
- Per-corpus, never pooled. Pooling is what let a dirty control class hide behind easy plants
  in the label-noise work.
- Reported together, never precision alone: `proposals`, `repaired_a_real_error`,
  `corrupted_a_clean_cell`, `wrong_value_on_a_real_error`, `net_cells_improved`, coverage, and
  for H1 the per-fold threshold, the per-fold verdict, and every annotated-true dependency any
  fold would have discarded.

## Two weaknesses declared before measuring

- **The negative labels are the authors', which is a strength, and still noisy.**
  `included_candidates.csv` defines the closed world, so negatives are not our own miner's
  leftovers under a closed-world assumption — a real improvement over what
  `premise_quality_measure.md` predicted it would have to do. But the authors annotated design
  FDs by hand, and a dependency they omitted is not thereby proven false. That noise is theirs,
  is stated in their paper, and biases toward finding false positives.
- **C4 cannot be validated on `rwd` at all.** `rwd` supplies annotations, not dirty/clean
  pairs, so it can test H1 but **not** P3-P5. The write-exposure arm runs on the four existing
  corpora only. Conflating the two would be the scoping error this project has made before.
