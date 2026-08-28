# The FD repair path costs about 100 milliseconds per flag on a 200,000-row table

**Status**: measured 2026-08-26, re-measured 2026-08-28 after an optimisation. Artifact:
`eval/results/harness_cost.json`. Reproduce with
`python scripts/bench/measure_harness_cost.py --artifact eval/results/harness_cost.json`.

## What was being asked, and what was found instead

The question was administrative: why has tax's `oracle` arm never completed? The answer is not
administrative, and it is not about the measurement harness.

`FDViolationRepairer.propose` — the shipped repairer, on the shipped path — costs about **100**
**milliseconds per flag** on a 200,000-row table. Not the benchmark wrapper. The product.

Two runs on 2026-08-28, on one machine, with no other load:

| rows | `_acting_group` (harness) | `propose` (product) | `propose`, random rows |
| --- | --- | --- | --- |
| 5,000 | 0.4 ms | 2.3–3.4 ms | 2.2–2.4 ms |
| 20,000 | 0.8–1.0 ms | 8.6–8.9 ms | 9.3–9.4 ms |
| 50,000 | 1.9–2.0 ms | 22.7–28.7 ms | 22.1–23.3 ms |
| 100,000 | 3.4–4.2 ms | 44.5–48.9 ms | 44.4–48.4 ms |
| **200,000** | **6.8–7.3 ms** | **89.0–99.7 ms** | **89.2–96.4 ms** |

Both costs are **linear in table size**. Each is called **once per flag**, and flag count also grows
with the table, so the whole pass is **quadratic in table size**. That shape is unchanged by the
optimisation below — only the constant moved.

**`propose` is 93% of it.** The harness helper I had assumed was the problem is the other 7%.

## What the 2026-08-28 optimisation did, and did not, change

Two constant-factor defects were removed from `FDViolationRepairer._matching_group`, the hot loop:

- `column_names(df)` was evaluated *inside* a generator expression, so it was rebuilt once per
  required column, each time allocating a fresh list of every column name;
- the O(rows) scan called `cell_value(df, row, column)` per cell — a method call plus a dict lookup
  per value. Each required column is now materialised once into a list, and the scan indexes those
  lists.

And in `dataforge/table.py`, `column not in self._columns` was a **linear scan of a list** on every
cell read and write, so at 100 columns it cost 100 string comparisons to fetch one value. It is now
a `frozenset` built from the same list in the same place.

Measured effect at 200,000 rows: `propose` fell from 595 ms to about 95 ms, and per-flag cost from
about 600 ms to about 100 ms. Roughly **6x**, stated to one significant figure because that is what
a wall clock supports.

**What did not change is more important.** No verdict moved. `measure_deductive_coverage.py`
re-derived with **zero** non-timing fields differing from the committed artifact: 116 clean cells
corrupted, 451 real errors repaired, all write precisions, and FD counts 53/81/85 all reproduce
exactly. The differential verifier equivalence property (200 Hypothesis examples, SMT against
direct), the no-corruption invariant, revert byte-identity, and all 18 auto-apply mutants are
unchanged.

**The quadratic shape is still there.** The asymptotic fix is to group once per functional
dependency and share the grouping across flags — the *detector* already computes exactly that
grouping and discards it. That was declined here, deliberately: it requires memoising over a
**mutable** table on the write path, and a stale group would propose a repair from data that has
since changed. That is a design change needing an invalidation contract, not an optimisation.

## Consequence for tax, and for any large table

tax's oracle arm flags **164,718** cells. At about a tenth of a second each that is
roughly **5** hours for the write-exposure phase alone, excluding detection and the replay phase.
Two runs projected 4.8 and 4.9 hours. So the arm was never twenty minutes from finishing, as
`eval/preregistration/shipped_premise_coverage.md` Amendment 1 estimated — and it is no longer the
**4** days for the same phase that this document reported on 2026-08-26.

The user-facing statement is the one that matters more: **a user with a 200,000-row table who**
**accepts mined dependencies and runs `repair --constraints` is starting a job measured in hours.**
That is better than the days it was, and it is still a scalability limit at a table size well inside
normal. No document in this project said so before 2026-08-26. Every FD number ever published here
comes from tables of 1,000 to 2,376 rows, where per-flag cost is single-digit milliseconds and the
problem is invisible.

## How the attribution was nearly published wrong

The first diagnosis was inferred from the operating system's CPU accounting: the replay phase took
over 600 CPU-seconds for 800 cells, so per-cell cost was over 0.75 s, and I attributed that to
`_acting_group` because it visibly scans the whole frame. The reasoning was structurally sound and
the conclusion was wrong: `_acting_group` was 23 ms, not 750 ms.

Had it been published, the recorded cause would have been a **harness** defect — mine to memoise,
invisible to users — instead of a **product** defect a customer will hit, and the remedy would have
been aimed at 1% of the cost.

What corrected it was measuring the two calls separately instead of reasoning about which looked
expensive. That is the third time in this project that direct measurement has overturned a mechanism
already argued for, and the lesson is the same each time: **your reasoning is your least reliable
instrument.**

## How the ledger nearly kept a false number

When the optimisation landed, `docs_truth.py --check` **passed**. It compares this document to a
committed JSON file, and that file had not been regenerated, so two of its three claims were
silently false: `propose_share` still read 99% against a measured 93%, and the projection still read
4 days against a measured 4.9 hours. The gate that exists to catch prose drifting from evidence
could not see a code change that invalidated the evidence itself.

That is the known architectural limit of the ledger — it binds a document to an artifact, not to
executing code — and this is the first recorded case of it producing a concrete false negative. It
was found by hand.

The second lesson is about rounding. Both original coarse renderings **collapsed to zero**:
`propose_seconds_per_flag_rounded` because a flag now costs under a tenth of a second, and
`tax_oracle_days_rounded` because the projection fell below half a day. A claim reading "0 seconds"
or "0 days" is not merely stale, it is false in the direction that flatters the product. So a coarse
rendering must be coarse **at the right scale**: those two keys were retired and replaced with
milliseconds and hours. Note the retired days claim's own note read "days rather than hours because
hours do not survive a re-run" — hours now do, at 4.8 and 4.9.

## Two controls, because either could have faked this

**Sampling.** Per-flag cost depends on determinant group size, and the first N rows of a table are
not a random sample of its groups. So `propose` was timed twice at every size: on rows 0–39, and on
40 rows drawn with a fixed seed. At 200,000 rows the two agree within a few percent, and the earlier
run's largest single discrepancy (58 ms against 179 ms at 20,000 rows, before the optimisation) was
noise that a second baseline run did not reproduce. The headline is not an artifact of where I
sampled.

**Reproducibility.** Before the optimisation, repeated runs moved the 200,000-row `propose` figure
between roughly 1,950 and 2,210 ms; after it, between 89 and 100 ms. So the decimals here are not
reproducible, and **only coarse renderings of this measurement are bound to the claim ledger** —
"about 100 milliseconds", "93%", "about 5 hours". The exact millisecond figures in the table above
are deliberately *unbound*: they describe two runs and would be false after the next.

That is a finding about the evidence infrastructure, not just about this measurement.
`docs/quantitative_claims.yaml` was built for deterministic counts, and it will happily pin a decimal
that noise moves — producing a document that fails its own gate for no reason, which is the fastest
way to teach people to ignore a gate. **Precision must not exceed reproducibility.**

## What this authorises

- Retiring the twenty-minute estimate in Amendment 1, and with it any suggestion that tax's `oracle`
  arm is merely pending.
- Stating that the FD repair path has a scalability limit, in the product, at a table size well
  inside normal — now hours rather than days, and still quadratic.
- Reading every FD result in this project as measured **at 1,000 to 2,376 rows** — a caveat that was
  always true and never written down.
- Binding timing claims only at a precision, and a unit, that survive a re-run.

## What this does NOT authorise

- **Any claim about what the repair path costs in a real deployment.** This measures `propose` in
  isolation, on one corpus, with 4 oracle dependencies. A mined premise has more dependencies and
  `propose` loops them internally, so the real figure is probably worse — but "probably worse" is not
  a measurement.
- **Any claim that the quadratic shape has been addressed.** It has not. The constant fell by about
  6x; the exponent is unchanged, and the remedy for it was explicitly declined above.
- **Any claim about tax's ceiling.** Still unmeasured. It is refused rather than pending; see
  `DECISIONS.md`.
- **Reading this as a benchmark of pandas or of any platform.** It is a property of this repairer's
  implementation, on one machine, under no contention.
