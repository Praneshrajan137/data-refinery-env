# The FD repair path costs about 2 seconds per flag on a 200,000-row table

**Status**: measured 2026-08-26. Artifact: `eval/results/harness_cost.json`. Reproduce with
`python scripts/bench/measure_harness_cost.py --artifact eval/results/harness_cost.json`.

## What was being asked, and what was found instead

The question was administrative: why has tax's `oracle` arm never completed? The answer is not
administrative, and it is not about the measurement harness.

`FDViolationRepairer.propose` — the shipped repairer, on the shipped path — costs about **2 seconds per
flag** on a 200,000-row table. Not the benchmark wrapper. The product.

One run, on one machine, with no other load:

| rows | `_acting_group` (harness) | `propose` (product) | `propose`, random rows |
| --- | --- | --- | --- |
| 5,000 | 0.4 ms | 15 ms | 14 ms |
| 20,000 | 1.1 ms | 58 ms | 57 ms |
| 50,000 | 2.3 ms | 421 ms | 508 ms |
| 100,000 | 12 ms | 1074 ms | 1010 ms |
| **200,000** | **23 ms** | **2126 ms** | **2279 ms** |

Both costs are **linear in table size**. Each is called **once per flag**, and flag count also grows
with the table, so the whole pass is **quadratic in table size**.

**`propose` is 99% of it.** The harness helper I had assumed was the problem is the other 1%.

## Consequence for tax, and for any large table

tax's oracle arm flags **164,718** cells. At about two seconds each that is roughly **4** days for the
write-exposure phase alone, excluding detection and the replay phase. So the arm was never twenty
minutes from finishing, as `eval/preregistration/shipped_premise_coverage.md` Amendment 1 estimated.

The user-facing statement is the one that matters more: **a user with a 200,000-row table who accepts
mined dependencies and runs `repair --constraints` is starting a job measured in days.** No document in
this project said so. Every FD number ever published here comes from tables of 1,000 to 2,376 rows,
where per-flag cost is about 50 ms and the problem is invisible.

## How the attribution was nearly published wrong

The first diagnosis was inferred from the operating system's CPU accounting: the replay phase took over
600 CPU-seconds for 800 cells, so per-cell cost was over 0.75 s, and I attributed that to
`_acting_group` because it visibly scans the whole frame. The reasoning was structurally sound and the
conclusion was wrong: `_acting_group` is 23 ms, not 750 ms.

Had it been published, the recorded cause would have been a **harness** defect — mine to memoise,
invisible to users — instead of a **product** defect a customer will hit, and the remedy would have been
aimed at 1% of the cost.

What corrected it was measuring the two calls separately instead of reasoning about which looked
expensive. That is the third time in this project that direct measurement has overturned a mechanism
already argued for, and the lesson is the same each time: **your reasoning is your least reliable
instrument.**

## Two controls, because either could have faked this

**Sampling.** Per-flag cost depends on determinant group size, and the first N rows of a table are not a
random sample of its groups. So `propose` was timed twice at every size: on rows 0–39, and on 40 rows
drawn with a fixed seed. At 200,000 rows: 2126 ms on head rows against 2279 ms on random rows. The
headline is not an artifact of where I sampled, and the published figure is the *cheaper* of the two.

**Reproducibility.** Repeated runs moved the 200,000-row `propose` figure between roughly 1,950 and
2,210 ms, and the smaller sizes moved proportionally more. So the decimals here are not reproducible,
and **only coarse renderings of this measurement are bound to the claim ledger** — "about 2 seconds",
"99%", "about 4 days". The exact millisecond figures in the table above are deliberately *unbound*:
they describe one run and would be false after the next.

That is a finding about the evidence infrastructure, not just about this measurement.
`docs/quantitative_claims.yaml` was built for deterministic counts, and it will happily pin a decimal
that noise moves — producing a document that fails its own gate for no reason, which is the fastest way
to teach people to ignore a gate. **Precision must not exceed reproducibility.**

## What this authorises

- Retiring the twenty-minute estimate in Amendment 1, and with it any suggestion that tax's `oracle` arm
  is merely pending.
- Stating that the FD repair path has a scalability limit, in the product, at a table size well inside
  normal.
- Reading every FD result in this project as measured **at 1,000 to 2,376 rows** — a caveat that was
  always true and never written down.
- Binding timing claims only at a precision that survives a re-run.

## What this does NOT authorise

- **Any claim about what the repair path costs in a real deployment.** This measures `propose` in
  isolation, on one corpus, with 4 oracle dependencies. A mined premise has more dependencies and
  `propose` loops them internally, so the real figure is probably worse — but "probably worse" is not a
  measurement.
- **Any claim that this is easy to fix.** The obvious remedy is the memo pattern already proven for
  `FormatViolationRepairer._dominant_profile`, whose equivalence had to be verified cell-for-cell before
  it could be trusted. That is real work and its outcome is not known.
- **Any claim about tax's ceiling.** Still unmeasured, and now known to be days of compute away rather
  than minutes. It is refused rather than pending; see `DECISIONS.md`.
- **Reading this as a benchmark of pandas or of any platform.** It is a property of this repairer's
  implementation, on one machine, under no contention.
