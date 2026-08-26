# Design-partner instrumentation: how one real table could yield decision-grade evidence

**Status**: specification, 2026-08-26. Nothing here is built. It answers a question that had been
asked and not answered: *what instrumentation would let a single design-partner table yield
decision-grade evidence?*

## The problem, stated exactly

Every published number in this project comes from four public academic corpora with **retained
ground truth** — a clean column beside the dirty one. A real customer table has no clean column, so
none of the harnesses can run on it: `classify_writes`, `_write_exposure` and `fd_holds_on_clean` all
take `dataset.clean_df`.

A buyer's first question is "on data like mine?" and the honest answer today is that we cannot
answer it. `docs/trust/column-benchmark-scope.md` and `docs/trust/sampling-bias-measured.md` both
record how this project reasons about generalising from one corpus, and neither route reaches a
customer table.

## The instrument already exists, and was built for something else

`dataforge/calibration_session.py::plant_controls` **manufactures local ground truth on a table that
has none.** Its own docstring states the mechanism:

> Pick a cell **no detector flagged**, so its current value ``V`` is the best available truth.
> Corrupt it ourselves into ``V'`` and show *that* as the flagged value. Because we performed the
> corruption, ``V`` is known by construction.

It was built to measure the **labeller** — the human β term in per-table certification — and that
programme's pre-registered kill criterion fired (`beta_upper` 0.8712 against a 0.35 criterion, 572
labels needed against a ~200 budget). So it is a carefully-reasoned component whose consumer died.

**The proposal is to repoint it from the labeller to the write path.** Plant known corruptions in a
customer's own table, run the real detector and repairer, and measure whether the product restores
`V`. Nothing about the mechanism is specific to human labelling.

## What that measures, and what leaves the customer's machine

| Quantity | Ground truth needed | Available on a customer table |
| --- | --- | --- |
| `repaired_a_planted_error` | the planted `V` — known by construction | **yes** |
| `wrong_value_on_a_planted_error` | same | **yes** |
| `corrupted_a_clean_cell` (an unplanted, unflagged cell the repairer rewrites) | none — any write to a cell we did not corrupt is measurable as a write | **yes** |
| abstention rate, flag rate, proposal rate | none | **yes** |
| `tested_confidence` distribution, mined FD count, `fd_covered_columns` | none | **yes** |
| `fd_set_precision` | a clean frame | **no** |
| recall on the table's *real* errors | the real errors | **no** |
| `cells_reviewed_per_true_error` | the real errors | **no** |

**What leaves the machine is counts.** No cell value, no column value, no row. Where a column
identity is needed to interpret a result, reuse the pattern already committed for the wild-column
corpora, which solved exactly this problem: `dataforge/datasets/wild_corrections.py` joins on
`corpus:column_index:sha256(value)[:16]` precisely because vendoring the bytes is forbidden by
licence. A design-partner report is that pattern applied to a customer instead of a licence.

## What this would establish, and what it would NOT

**Would establish**, for the first time, a claim about customer data:

> On your table, for corruptions we planted and can therefore score, the product restored the
> original value in N of M cases, wrote a wrong value in K, and rewrote J cells we had not
> corrupted.

That is decision-grade for the question a buyer actually asks: *will this damage my data?* The
`corrupted_a_clean_cell` column is the one that matters, and it needs no ground truth at all —
any write to a cell we did not corrupt is observable.

**Would NOT establish**, and the spec must say so in the report itself:

1. **Precision on the table's real errors.** The planted corruptions are drawn from the column's own
   observed values, so they are as plausible as a real mistake but are **not** distributionally
   identical to one. `plant_controls` already draws this distinction for the labeller case, naming
   the weaker class `column_distribution` and the stricter `corrector_generated`. The same split
   applies here and the two must never be pooled.
2. **Recall.** We cannot know which of the customer's cells are actually wrong, so we cannot say what
   fraction of their errors we would catch. Any report must state recall as **unmeasurable**, not as
   absent.
3. **That an unflagged cell was correct.** "No detector flagged it" means no detector *noticed*
   anything, and detector recall is well below 1. The docstring is already explicit about this.

### The bias direction, which is what makes it usable

For the labeller measurement, the unflagged-cell assumption errs **conservative**, and the docstring
argues it carefully. The direction must be re-derived for the write path rather than assumed, and it
comes out the same way:

Suppose a selected cell's `V` is in fact wrong, and the repairer proposes the genuinely true value
`T != V`. This instrument scores that as a failure, because it scores against `V`. So the effect is
to **under-count** correct repairs and **over-count** wrong ones — write precision is understated.
The bias runs toward making the product look worse than it is, which is the only direction an
asymmetric-cost product may accept in its own favour-free evidence.

The magnitude is second-order and must not be modelled away: it is bounded by the probability that
an unflagged cell is wrong **and** the product proposes its true value. Reporting a corrected figure
would trade a conservative bound for a modelled one.

## What a design partner would run

A single command, on their machine, against their table, with `--dry-run` semantics throughout — the
instrument must never need write permission to produce its evidence:

```
dataforge measure-on-my-table <csv> --plants 200 --report report.json
```

and hand back `report.json`. Its contents are counts, hashes and configuration; a schema check must
**refuse to emit** any field that could carry a value. That refusal is the load-bearing engineering,
and it should be enforced by a test that plants a recognisable sentinel string in the table and
asserts it appears nowhere in the report bytes.

## Why this is not built in this session

Three reasons, in order of weight:

1. **It has no committed measurement of its own.** Repointing `plant_controls` at the write path is a
   new measurement instrument, and this project's rule is that an instrument must be validated before
   its output is believed. The validation is available and cheap: run it on the **four corpora that
   do have ground truth**, and compare the planted-error write precision against the true-error write
   precision already published. If the two diverge wildly, the instrument does not predict what a
   buyer cares about, and that must be known before any customer sees a report.
2. **The privacy guarantee is the product here**, and a value-leak in a customer report is not a bug
   that can be fixed after shipping.
3. It is capability rather than correction, and the session's corrective work has priority.

## What would make this real, in order

1. Validate the instrument on hospital, flights, rayyan and tax: does planted-error write precision
   track true-error write precision? Publish the correlation, or the lack of it.
2. Only if it tracks: build the command, with the emit-refusal test.
3. Recruit one design partner. One is enough for the first claim, and the claim must be scoped to
   their table — `sampling-bias-measured.md` is the precedent for refusing to generalise from one.

Until step 1 is done, the honest statement is unchanged: **no real customer table has ever been
tested, and this project cannot yet say what it would do to one.** The difference this document makes
is that the route is now specific, the instrument is identified, and the blocker is a validation
someone can run rather than an open question.
