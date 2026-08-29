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

## 2026-08-29: this document measured the wrong half of the cost

Everything above is true **as scoped**, and the scope is narrower than a reader will assume. The
harness behind it, `scripts/bench/measure_harness_cost.py`, times `_acting_group` and
`FDViolationRepairer.propose`. **It never calls a verifier.** So "`propose` is 93% of it" means 93%
of those two functions, not 93% of repairing a cell, and the roughly 5-hour tax figure is the
write-exposure phase only -- which this document says, and which is easy to read past.

Measured end to end on `hospital` (1,000 rows, 20 columns, 53 oracle FDs), one machine:

| Stage | Before | After |
| --- | --- | --- |
| `propose` per flag | 1.43 ms | **0.046-0.053 ms** |
| `differential_verify` per fix | 1,524 ms, SMT verdict UNKNOWN | **136-143 ms**, real verdicts |
| Projected end to end, 7,905 cells | about 200 minutes | **about 18-19 minutes** |

So **verification was about 98% of the cost of repairing a cell, and `propose` was about 0.1%.**
The quadratic this document correctly identified was real and was not the dominant term.

### What changed

* **The SMT encoding is scoped to each constraint's footprint.** It asserted a ground equality for
  every cell of every relevant column -- 10,000 assertions and roughly 40,000 z3 AST nodes per
  fix here -- when an FD verdict depends only on rows sharing the candidate's determinant tuple,
  uniqueness only on rows already holding the candidate value, and bounds, accepted values,
  NOT NULL and regex only on the candidate row. Counted rather than timed, so it does not move
  with the machine: at 2,000 rows the encoding fell from about 4,000 ground assertions to **6**,
  and `tests/unit/test_verifier_locality.py` fails if it becomes a function of row count again.
* **The FD `ForAll` is expanded over that finite footprint.** A quantifier over an unbounded `Int`
  in an otherwise ground problem left the decidable fragment for nothing, and it is why the
  verdict was UNKNOWN on 60 of 60 real proposals at the shipped 200 ms budget. Expansion is the
  same transformation z3's own documentation uses to argue decidability for bounded quantifiers.
* **The determinant grouping is built once per repair pass**, stamped with each determinant
  column's write revision, which is the invalidation contract the section above correctly
  required before this was allowed. `propose` cost is now flat from 1,000 to 16,000 rows at fixed
  group size -- measured 1 index build, 200 reuses, 0 rescans.

### The scoping introduced a verifier divergence, which is the more important finding

Scoping what the encoding *asserts* also changed what the SMT verifier *concluded*, on an input
class the equivalence property could not reach.

`DirectVerifier` returns UNKNOWN when any value in a relevant column cannot be coerced to its
declared type, justified in `direct.py` on the grounds that "the primary verifier likewise cannot
encode it". True while every cell was encoded; false once only the footprint was. With an
uncoercible value in a row *outside* the footprint, **SMT returned ACCEPT while Direct returned
UNKNOWN.**

`differential_verify` caught it and failed closed, so no unsound value could ever have been
written -- the invariant did its job. It is still a defect: an N-version check exists to *detect*
disagreement, and here it absorbed a disagreement silently across a whole class of tables. The
property test missed it because its frames are 2-4 rows of well-typed values, so footprint and
table are effectively the same set and no generated value is uncoercible.

Coercibility is therefore now checked over the **whole** relevant column while only the footprint
is asserted into the solver. It is a Python type check, not z3 AST construction, and it cost
nothing measurable. `tests/unit/test_verifier_scope_parity.py` pins the parity on 40-row frames
where footprint and table genuinely differ.

Note what is deliberately **not** decided here: whether holding a provable repair because an
unrelated row holds garbage is the right semantics. Arguably the scoped ACCEPT was the better
answer and `DirectVerifier` is the over-conservative one. That is a coverage change and belongs in
its own pre-registered decision, not smuggled in as a side effect of a speedup.

### What has NOT changed, and what is still not authorised

* **Every verdict.** `measure_deductive_coverage.py` reproduces 393/451/451 repaired, 0/86/116
  corrupted and FD counts 53/81/85 with `replication_mismatches` 0 in all three arms -- the
  pre-registered K4 oracle, unmoved.
* **The tax projection is still not a measurement**, and the corrected shape is *worse* than the
  figure this document publishes. At 136-143 ms per fix and 164,718 flags, verification alone is
  on the order of **6-7 hours**, against the roughly 5 hours projected here for write exposure.
  The number that flatters the product is the one that omits the verifier.
* **The timings are ranges from three repeats, and earlier readings on the same code did not
  reproduce.** A single run once read 42 ms/fix, and a set of three read 166-249 ms while the
  machine was loaded; the 136-143 ms figure has a 5% spread and is the one quoted. Only the
  counted assertion figures are stable across machines. The speedup is stated as **about 11x end
  to end** and should not be quoted more precisely.
* **`DirectVerifier` is NOT the larger remaining term, and an earlier version of this section said
  it was.** That was inference from its O(columns x rows) coercion sweep, not measurement.
  Profiled: **SMT 127-130 ms/fix against Direct 6.3-6.7 ms/fix** -- Direct is about 5% of SMT.
  The retraction matters more than the number, because the same mistake produced the "propose is
  93%" framing this section corrects: reasoning about which code looks expensive instead of
  measuring it.
* **The dominant cost was a defect introduced by the parity fix above.** The coercibility sweep
  called the z3 value factory per cell, so it built one AST node for every value it checked --
  counted at **10,232 `StringVal` calls per fix**, about 40% of verification. For a `str` column
  the check is also incapable of failing, because the cell is already a string and `str` is total,
  and all 20 of hospital's declared columns are `str`. Replacing it with a pure-Python coercion
  predicate, and skipping `str` entirely, cut it to **232 calls per fix**. That figure is a count,
  not a timing, so it is reproducible across machines; the wall clock over the same change spanned
  79.8 to 352.2 ms/fix and settled nothing.
* **Scoping `DirectVerifier` the same way is declined, on a stronger basis than cost.** It would
  need its own footprint computation. Written from the same relevance argument, the two verifiers
  would then share a *specification* while differing only in code -- the residual correlation
  channel Knight and Leveson identified when they found that independently written programs failed
  together far more often than independence predicts, and concluded that "all design specification
  must be redundant and independent for the versions to have any chance of avoiding common design
  faults" (IEEE TSE, 1986). Implementation diversity does not close a fault in the shared argument.
  Note the honest limit: no study located here quantifies shared specification against shared code
  as the dominant source, so this is a documented channel, not a measured ranking.

## 2026-08-29: what representation these numbers were measured on

Every timing on this page was produced by a harness in `scripts/`, and every harness in `scripts/`
builds a `pandas.DataFrame`. The CLI does not. `read_csv` returns `dataforge.table.Table`, so every
run a user performs uses `Table`.

That is not a cosmetic difference. `DeterminantGroupIndex` reuses a cached grouping only when the
table can report a per-column write counter through `column_revision`. `Table` has that method;
`DataFrame` does not. So the harnesses take the uncached branch -- one scan per fix -- while the
product takes the cached one. **The measured path and the shipped path are different code.**

Three consequences, stated plainly:

* The per-flag timings here (`fd_scalability_propose_ms`, `fd_scalability_propose_share`,
  `fd_scalability_hours`) describe the *uncached* branch. They are therefore conservative for the
  product -- the shipped path does strictly less grouping work -- but "conservative" is a claim about
  direction, not magnitude, and this page does not quantify the magnitude.
* `fd_scalability_tax_flags` is unaffected. It is a deterministic count, not a timing.
* One earlier attempt to quantify the gap on this page compared 65.5 ms on `Table` against 16.0 ms on
  pandas and reported the cache as a slowdown. That comparison was invalid -- it varied the
  representation and the branch together -- and was retracted above.

**What was done about it.** Two things, neither of which is "rewrite every harness":

1. `tests/property/test_representation_parity.py` asserts that both representations reach the same
   verdict, over generated cases and over a deterministic case per constraint family fixed at a
   contested verdict. This makes the divergence non-load-bearing *for correctness*: if the two ever
   decide differently, that test fails, and the pandas-based evidence on this page stops being
   evidence at that moment instead of quietly becoming wrong.
2. `scripts/perf/measure_verifier_work.py` measures on `Table`, and measures counted work rather than
   wall clock. It is the only instrument here that runs the shipped representation.

**What was not done, and why it is still open.** The harnesses still measure pandas. Parity of
verdicts is not parity of cost, and the honest position is that the timings on this page are
approximately right for the product rather than measured on it. Converting the harnesses is a real
change -- they use pandas for scoring, not only for holding rows -- and it is recorded as an open
item rather than claimed as done. Anyone tightening these numbers should convert the harness first
and re-measure, not adjust the figures.
