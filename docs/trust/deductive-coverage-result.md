# The label-free repair path, measured

> **AMENDMENT, 2026-08-26. The `mined` arm below is a PROXY for the shipped path, not the shipped
> path, and its numbers are a bound rather than a measurement of what a user gets.**
>
> This document frames its `mined` arm as the three-step journey `profile --constraints-out`,
> `constraints review --accept`, `repair --constraints`. It does not run those three steps: the
> harness builds its premise from `infer_verification_schema`, which applies
> `_VERIFY_FD_MIN_CONFIDENCE = 0.95`. The shipped accept path applies **no floor of its own**, so its
> effective floor is the miner's emission floor of **0.90** — a strictly larger and lower-quality FD
> set of 85 rather than 81.
>
> Measured through the real artifact and merge in `docs/trust/shipped-premise-result.md`: the premise
> a zero-config user actually gets corrupts **116** clean cells on hospital, not 86, and the four
> added dependencies repaired **zero** additional real errors. Every figure below stands as
> reproduced — the `mined` arm still yields 537/451/0/86 exactly, and that reproduction is the
> regression guard for the new measurement — but the *interpretation* changes: 86 is what a 0.95
> premise costs, and no user is given one.
>
> Nothing below is edited. The general rule this produced: **an arm that models a user journey must
> be built from the code that journey runs.**

Measured 2026-08-25. Artifacts: `eval/results/deductive_coverage_hospital.json`,
`eval/results/deductive_coverage_rayyan.json`, `eval/results/deductive_coverage_flights.json`.
Reproduce: `python scripts/bench/measure_deductive_coverage.py --corpus <name> --artifact <path>`.
No API cost; deterministic; ground truth retained.

## Why this measurement had to exist

When human-labelled certification died, `DECISIONS.md` recorded that "the honest product is
soundness-plus-reversibility (which needs no labels) plus advisory triage". That fallback was
asserted for months and never given a denominator. The only coverage figures in the repository were
incidental and unusable: `specs/SPEC_autoapply_decision.md` reports a deterministic floor of **1**
cell on `hospital_10rows.csv` that drops to **zero** once a schema is declared, and
`eval/results/trust_ledger_adversarial.json` writes **1** cell out of 14 attack proposals on a
corpus containing exactly one real error. One is a fixture built to be non-zero; the other has a
denominator of one.

This matters more than an ordinary gap, because an FD-derived repair carries `deterministic`
provenance and `partition_auto_apply` lets `deterministic` fixes on allowlisted detectors **bypass
calibration entirely** -- no threshold, no confidence, no labels, nothing downstream. It is the one
path in the product that writes to a user's data on its own authority.

## Headline: that authority is not warranted as currently granted

| corpus | premise | rule | writes | repaired | wrong | **corrupted a clean cell** | write precision | net cells improved |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| hospital | oracle | plurality (former) | 393 | 393 | 0 | **0** | 1.0000 | +393 |
| hospital | oracle | **majority (ships)** | 393 | 393 | 0 | **0** | 1.0000 | +393 |
| hospital | oracle | unanimity (rejected) | 182 | 179 | 0 | **3** | 0.9835 | +176 |
| hospital | mined (opt-in) | plurality (former) | 537 | 451 | 0 | **86** | 0.8399 | +365 |
| hospital | mined (opt-in) | **majority (ships)** | 537 | 451 | 0 | **86** | 0.8399 | +365 |
| hospital | mined (opt-in) | unanimity (rejected) | 207 | 195 | 0 | **12** | 0.9420 | +183 |
| flights | oracle | plurality (former) | 3270 | 1837 | 702 | **731** | 0.5618 | +404 |
| flights | oracle | **majority (ships)** | 1807 | 1193 | 270 | **344** | 0.6602 | **+579** |
| flights | mined | any | 0 | 0 | 0 | 0 | n/a | 0 |
| rayyan | either | any | 0 | 0 | 0 | 0 | n/a | 0 |

`write precision` is repaired real errors over all writes. `corrupted a clean cell` is a proposal to
change a cell that was **already correct**. Every figure is per distinct cell, not per detector flag:
with a mined premise the same cell is flagged once per FD naming its column, which inflated the flag
count to 48,599 against 10,064 distinct cells and would have inflated every rate computed from it.

## Correction, 2026-08-25: the mined arm is opt-in, not the default

The first version of this document, and the artifacts it cites, described the mined premise as "what
`fd_detection_source='accepted'` keeps, and `'accepted'` is the default". The second clause is true
and the implication drawn from it was not. **`--fd-detection accepted` filters dependencies already
present in the effective schema; it mines nothing.**

A mined dependency reaches `FDViolationRepairer` only after all three of:

1. `dataforge profile <csv> --constraints-out <artifact.json>`, which writes every candidate with
   `decision="pending"` (`dataforge/schema_inference.py`:119);
2. `dataforge constraints review <artifact.json> --accept <cnd-id>`, over the queue-cost warning
   already printed at `dataforge/cli/constraints.py`:318-345;
3. `dataforge repair <csv> --constraints <artifact.json>`.

Without an artifact, `merge_schema_with_reviewed_constraints` returns the declared schema untouched
(`schema_inference.py`:356-357); with one whose candidates are still pending, nothing is merged
(`:363-366`). The only auto-mined schema on the repair path, `infer_verification_schema` at
`dataforge/engine/repair.py`:1680, is routed exclusively to the SMT guard (`:661-670`) and can reach
neither detection nor repair.

**The 86 corruptions stand; their reachability was misdescribed.** They are the measured cost of a
deliberate, warned opt-in rather than of a default. That lowers the severity and changes the remedy:
the warning at acceptance time should carry this number, which is a cheaper and better-targeted fix
than a new gate. It does not make the number less true, and it does not change that these writes
carry `deterministic` provenance and bypass calibration entirely once accepted.

## The measurement everyone was reporting instead

Conditional on a cell already being a real error, hospital reports **precision 1.0000** on 451
proposals. That number is true and nearly meaningless. It can only report how good the repairs are
on cells that needed one; it is silent on the failure that actually costs a user data. The same
premise, measured unconditionally, corrupts **86** clean cells.

That asymmetry is why the table above is unconditional. A repair path evaluated only on cells known
to be broken cannot be shown to be safe.

## Premise quality decides corruption, and the label does not

Hospital, identical gate, identical rule, identical `deterministic` label:

- **oracle premise** (53 single-column FDs, each holding exactly on ground truth): **0** corruptions.
- **mined premise** (115 FDs from the product's own `infer_verification_schema` at its shipped 0.95
  floor): **86** corruptions, a harmful write rate of **0.1601**.

This is the same shape as `eval/results/trust_ledger_adversarial.json`, where 0 of 14 attacks were
written under a tight premise and 10 of 14 under a permissive one with every write labelled `proven`
in both runs. That result was about the `proven` label. This one shows `deterministic` inherits the
identical defect: **it names the mechanism that produced the value, not the entailment that
justifies it.**

Attribution is complete rather than statistical. Every sampled corruption traces to a mined FD that
does not hold on clean:

| acting dependency | corruptions sampled | holds on clean |
| --- | --- | --- |
| `ZipCode -> HospitalName` | 23 | no |
| `ZipCode -> HospitalOwner` | 2 | no |

A zip code does not determine a hospital name; several hospitals share one. The full spurious set is
recognisable as the classic failure of confidence-thresholded FD mining on a skewed column:
`Score -> State`, `Sample -> State`, `EmergencyService -> State`, `Condition -> State`,
`MeasureCode -> Stateavg`. `State` is dominated by one value, so any column "determines" it in 95% of
rows. 12 of 115 mined dependencies are false, an FD-set precision of **0.8957**, and that is enough
to corrupt 86 cells.

`fd_detection_source` already exists with `declared` / `accepted` / `none`
(`dataforge/engine/repair.py`:920) and already carries a docstring recording that accepted FDs
degrade the *detection* queue from 56% real errors to 4.4%. Two things were still missing: the
default is `accepted`, and nothing propagates premise provenance into the **repair** path's strength.
The 86 corruptions are the cost of that second gap.

## The decision rule matters too, and the docstring was right

`FDViolationRepairer._deterministic_choice` documents itself as returning "a strict majority value"
and implements `ranked[0][1] > ranked[1][1]`, which is a **plurality**. The gap was assumed cosmetic.
It is not:

- **hospital**: plurality and majority are bit-identical. `plurality_only_not_majority` is **0** in
  both arms. Zero divergence, so aligning the code to its docstring costs nothing here.
- **flights**: they diverge on **1732** cells, and majority is strictly better on every axis --
  precision 0.6602 against 0.5618, wrong values 270 against 702, corruptions 344 against 731, and
  net cells improved **+579 against +404** despite lower coverage.

So the honest reading is not "a harmless naming slip". Implementing what the docstring already
claimed **halves harmful writes on flights** (614 against 1433) and improves the net outcome. It was
found by measuring a documentation defect rather than tidying it.

**Shipped 2026-08-25.** `_deterministic_choice` now requires `top_count * 2 > group_size`. The
artifacts above carry both rules: `majority` is what runs, `plurality` is retained as a
counterfactual so the cost of the change stays measurable instead of becoming folklore. The
measurement script's `replication_mismatches` field is 0 on all three corpora, which is a check that
the reimplemented rule matches the real repairer -- it read 1463 on flights in the interval between
changing the repairer and renaming the script's arms, which is exactly what it exists to catch.

## Unanimity is worse, and the reason is instructive

The obvious tightening -- propose only when every *other* row in the determinant group agrees, the
only rule under which premise plus data genuinely entail a unique value -- was measured before being
implemented, and it fails:

- hospital oracle: coverage falls from 0.7721 to 0.3517 **and introduces 3 corruptions where the
  shipped rule had none**;
- hospital mined: coverage falls from 0.8861 to 0.3831, corruptions fall from 86 to 12;
- flights: proposes nothing at all.

Introducing corruption while halving coverage is a strictly worse trade, and the mechanism explains
why. Plurality and majority count the target cell's **own** value; unanimity deliberately excludes
it. When a group is split, the cell's own vote is what stops a confident overwrite. **Counting the
cell's own value is a safety property, not sloppiness.** A tightening that looked principled would
have destroyed it.

This is the clearest available argument for measuring before changing: the planned fix was
harmful, and only an unconditional metric revealed it.

## Coverage, stated honestly

On hospital the label-free path repairs **0.8861** of all real errors in the table under the default
mined premise and **0.7721** under a perfect one -- far higher than anything previously suggested by
the 1-cell and 1-of-14 figures. But the same path contributes **nothing** on rayyan (no dependency
survives mining or discovery) and reaches **0.3734** on flights at write precision 0.5618.

So there is no single coverage number. Coverage is a joint property of corpus, premise source and
decision rule, and it ranges from **0.0** to **0.8861** with write precision from **0.5618** to
**1.0000**. Any product claim that quotes one figure is quoting the corpus it was measured on.

## Limits

1. **Single-column determinants only, in EVERY arm.** ~~The mined arm supplies multi-column
   determinants; the discovery arm does not. Oracle coverage is therefore a floor on the ceiling.~~
   **RETRACTED 2026-08-26.** That was false. `_fd_candidates` emits `columns=(determinant,)`
   unconditionally, so the mined arm supplies no composite determinant either, and neither does
   `shipped_accept_all`. The stated reason why oracle coverage is "a floor on the ceiling" therefore
   does not hold — the arms are not confounded by determinant arity at all, because none of them has
   any. The same false sentence stood in `scripts/bench/measure_deductive_coverage.py` and is
   corrected there. `docs/trust/premise-quality-result.md` limit 4 had it right the whole time; two
   artifacts contradicted it.

   What remains true, and is the real limit: `FDViolationDetector` and `FDViolationRepairer` both
   fully support composite keys, and the only producer in the zero-config path can never emit one. So
   dependencies that are *only* composite — `(store, sku) -> price`, `(year, zip) -> tax_rate` — are
   invisible to every measurement here and to every zero-config user. The capability is built and
   unreachable.
2. **Exact-hold is a harsh definition of a true FD.** `fd_holds_on_clean` admits no exceptions, so a
   dependency violated by one cell in 1000 counts as false. The 0.8957 FD-set precision should not
   be read as "10% of mined dependencies are nonsense" -- the attribution table shows which ones
   actually caused harm, and it is a much shorter list.
3. **All columns declared `str`.** A tighter type premise would also narrow writes, and attributing
   that narrowing to the dependency would overstate what the FD did. Real deployments have types, so
   real corruption counts may be lower.
4. **Three corpora, two of which contribute no dependencies.** hospital and flights carry the whole
   result. rayyan's zero is itself a finding, not a gap.
5. **`net cells improved` is positive everywhere the path runs.** A user is better off on net in
   every measured configuration. That is not a defence of the 86 and 731 corruptions: those cells
   were correct, the writes are unreviewed by construction, and a net gain is not consent.

## What this authorises

**Authorises** the claim that the label-free FD repair path has measured, unconditional write
precision between 0.5618 and 1.0000 on retained ground truth, and that its corruption count is
governed by premise provenance rather than by anything the write gate currently inspects.

**Authorises** the strict-majority rule in `_deterministic_choice`, now shipped: zero measured cost
on hospital, strictly better on flights.

**Authorises** refusing calibration bypass for repairs derived from *mined* dependencies, on the
grounds that all attributed corruption came from mined dependencies that are false.

**Does not authorise** any claim that the deductive path is "sound". It is sound under a premise
that is exactly true, and it corrupted 86 and 731 clean cells under premises that were not.

**Does not authorise** a single headline coverage number. There isn't one.

**Does not authorise** reading hospital's `precision 1.0000` as a property of the path. It is a
property of that corpus, that premise and that conditioning.
