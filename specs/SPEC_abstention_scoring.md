# SPEC: Abstention-neutral detection scoring

Status: **normative**. Adopted 2026-08-23.
Executable counterpart: `tests/integration/test_abstention_scoring_table.py`.
Implementation: `dataforge/bench/abstention.py`.

## Why this document exists

DataForge's product behaviour is to refuse to invent a value it cannot prove. Under the
two-way scoring rule used by every RAHA-derived benchmark in this repository, that
behaviour is **arithmetically indistinguishable from failure**.

The concrete case is measured and committed. On `flights`, the heuristic method scores
correction F1 **0.0000** (`BENCHMARK_REPORT.md:40`, tp=0 fp=92 fn=4920). The same flight's
arrival time appears in the upstream sources as 10:30, 10:31, 10:28 and 10:39; the RAHA
ground truth picks one, so it encodes an arbitrary convention. A system that declines to
invent one truth is scored identically to a system that guesses wrong.

`docs/trust/accuracy-frontier.md:41-51` already argues that the 0.0000 is honest
abstention rather than a defect. That argument was, until this spec, **unfalsifiable** —
no instrument in the repository could distinguish the two hypotheses.

This spec adopts a scoring rule that can.

## Provenance of the rule

This is **not** an invention of this project. It is the published scoring rule of
`RT-bench` and `ST-bench`, the two manually-labelled 1200-column benchmarks shipped with:

> Qixu Chen, Yeye He, Raymond Chi-Wing Wong, Weiwei Cui, Song Ge, Haidong Zhang,
> Dongmei Zhang, Surajit Chaudhuri. *Auto-Test: Learning Semantic-Domain Constraints for
> Unsupervised Error Detection in Tables.* SIGMOD 2025. arXiv:2504.10762.

Rule as stated verbatim in `benchmarks/benchmark_readme.md` of `github.com/qixuchen/AutoTest`:

- `ground_truth` — errors labelled **unambiguous** and **obvious**. *"An algorithm that
  misses errors in `ground_truth` will be counted as a recall loss."*
- `ground_truth_debateable` — errors that are **debatable** in nature. *"Given the
  debatable nature of such values, predictions made or missed by an algorithm for values
  in `ground_truth_debateable` will not affect its precision or recall."*
- `dist_val` — the full distinct-value list. *"An algorithm that predicts any values in
  this column, other than the values listed in `ground_truth` and
  `ground_truth_debateable`, will count as precision loss."*

Adopting somebody else's published rule rather than defining our own is deliberate. It is
the only way this project can make a **protocol-controlled** comparison, which
`PRODUCT.md:129-135` correctly records it has never been able to do.

## The rule

Let, for a single column:

- `V` — the set of distinct values in the column (`dist_val`)
- `G ⊆ V` — unambiguous errors (`ground_truth`)
- `D ⊆ V` — debatable errors (`ground_truth_debateable`), with `G ∩ D = ∅`
- `P ⊆ V` — the values a detector flagged

Then:

```
TP = |P ∩ G|
FP = |P \ (G ∪ D)|
FN = |G \ P|

precision = TP / (TP + FP)        undefined if TP + FP == 0
recall    = TP / (TP + FN)        undefined if TP + FN == 0
```

`D` appears in **no** term. It is subtracted from the false-positive set and never added
to the false-negative set.

## The four properties that make this correct

| # | Property | Consequence |
| --- | --- | --- |
| P1 | Flagging a value in `D` costs nothing | A system that flags contested cells is not punished |
| P2 | Not flagging a value in `D` costs nothing | A system that abstains on contested cells is not punished |
| P3 | `D` is excluded from both numerator and denominator | `D` is **not** silently reclassified as clean, which would make P1 false |
| P4 | `G ∩ D = ∅` is required, not assumed | A value cannot be simultaneously unambiguous and debatable; violation is a data defect and must raise |

P1 and P2 together are the point: the neutral zone makes "I abstained because it was
genuinely ambiguous" a **measurable** claim rather than a defence.

## The four limits that must always be reported with the numbers

These are not caveats to be buried. Under `PRODUCT.md:120-144` an aggregate that hides a
weakness may not be published, so each of these is carried in the artifact as a field, not
only in prose.

| # | Limit | Field |
| --- | --- | --- |
| L1 | `dist_val` is **distinct** values. A value occurring 900 times counts once. This is **not** cell-level precision and must never be reported as though it were. | `distinct_values_only: true` |
| L2 | `G` contains only **unambiguous** errors. Recall here is recall-on-easy-errors, and is therefore an **upper** bound on recall over all real errors. | `ground_truth_scope: "unambiguous_only"` |
| L3 | No clean value ships with either benchmark. This axis measures **detection only**. It cannot score a repair. | `axis: "detection"` |
| L4 | Each benchmark row is a single column. Detectors requiring row or cross-column context cannot fire and are `not_applicable`, which is **not** recall 0. | `not_applicable_detectors: [...]` |

L4 is the same category error as the one this whole spec exists to fix. Conflating "cannot
apply" with "failed" is conflating abstention with failure one level up.

## Undefined-value discipline

`precision` and `recall` are genuinely undefined at a zero denominator, and the two
conventional fillers are both wrong here:

- Reporting `precision = 1.0` when nothing was flagged would let a system that flags
  nothing report perfect precision. This is the dominant failure mode recorded in
  `docs/trust/` and in this project's testing discipline: an assertion that passes by
  proving nothing.
- Reporting `recall = 1.0` when `G` is empty would let a column with no labelled errors
  contribute a perfect score to an aggregate.

Therefore:

| Condition | `precision` | `recall` | Rationale |
| --- | --- | --- | --- |
| `TP + FP == 0` (nothing flagged) | `None` | as computed | No prediction was made; there is no precision to report |
| `TP + FN == 0` (no unambiguous errors) | as computed | `None` | The column carries no recall obligation |
| both | `None` | `None` | Column contributes to no aggregate |

`None` propagates: a column with `None` precision is **excluded from the denominator** of
any aggregate precision, rather than contributing a 1.0 or a 0.0. `ThreeWayScore.f1` is
`None` whenever either input is `None`.

## Aggregation

Aggregate over columns by **pooling counts**, not by averaging per-column rates:

```
precision = sum(TP) / (sum(TP) + sum(FP))
recall    = sum(TP) / (sum(TP) + sum(FN))
```

Macro-averaging per-column rates would give a 3-distinct-value column the same weight as
a 900-distinct-value column. Both are reported; pooled is normative because it is the one
Auto-Test publishes.

## Abstention is reported, never inferred

Two further quantities are reported because they are the ones the rule is designed to make
visible, and neither is derivable from `precision`/`recall`:

- `coverage` — `|P| / |V|`, the fraction of distinct values on which a decision was made.
- `abstention_rate` — `1 - coverage`.

A single F1 forces a selective system to report its worst operating point. These two
fields plus the risk-coverage frontier below report the whole frontier instead.

## Risk-coverage frontier

For a detector emitting a confidence per flagged value, sweep a threshold `t` and report:

- `coverage(t)` — fraction of distinct values flagged at or above `t`
- `selective_risk(t)` — `1 - precision` on the accepted set at `t`
- `risk_upper(t)` — one-sided upper confidence bound on `selective_risk(t)`

`risk_upper` **must** reuse `dataforge.conformal._clopper_pearson_upper`. There must not be
a second definition of risk in this repository. The certification path
(`conformal.certify_threshold`, fixed sequential testing per Bates et al. / Learn-then-Test)
already defines selective risk this way, and a benchmark that measured risk differently
from the gate that acts on it would be measuring a different system.

The frontier is reported at a **pre-specified** threshold grid, for the same reason
`CERTIFICATION_GRID` is a module constant (`calibration_session.py:506-523`): a grid
derived from the labels is a validity weakness, not merely a power one.

## Non-vacuity requirements

Enforced by tests, because every one of these has a shipped precedent in this repository:

1. An all-abstain system must score `recall == 0.0` on a column with `G` non-empty. It must
   **not** score `None`, and must not be excluded from the recall aggregate. Abstaining is
   free only inside `D`.
2. A scorer given empty `G` and empty `D` must not report `precision == 1.0` for zero
   predictions. See the table above.
3. An aggregate computed over zero admissible columns must **raise**, not return zeros.
   `all_parity` reducing to `0 == 0 and 0 == 0` is a recorded failure in this project.
4. `G ∩ D ≠ ∅` must raise. It indicates a corrupted or misparsed benchmark row, and
   silently preferring one label would make P3 false without any signal.

## What this spec does not authorise

- It does not authorise a repair claim. L3 is structural: there is no clean value to score
  against. Any correction number sourced from RT-bench or ST-bench is fabricated.
- It does not authorise a SOTA claim. It authorises exactly one comparison — against
  Auto-Test's published curves on the same bytes under this rule — with L1 through L4
  stated alongside.
- It does not change any write gate. Detection scoring has no write path. The auto-apply
  decision remains governed by `SPEC_autoapply_decision.md`, and nothing measured under
  this spec may be used to add a detector to `CONSTRAINT_CHECKABLE_DETECTORS`.
