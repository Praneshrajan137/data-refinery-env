# Pre-registration: the performance programme

**Status: committed before any optimisation lands.** This document exists so a speedup cannot
be declared by choosing which number to report after seeing the result. Every criterion below
is fixed **in advance**. Deviations must be recorded as deviations, not silently absorbed.

The instruments this depends on were built first, deliberately:

- [`scripts/perf/measure_loop_cost.py`](../../scripts/perf/measure_loop_cost.py) — timings with
  repeats and ranges, one step at a time.
- [`scripts/ci/gate_population.py`](../../scripts/ci/gate_population.py) — the derived set of
  checks each gate polices, frozen in
  [`eval/results/gate_population.json`](../results/gate_population.json).

## Question

Can the verification loop be made materially faster **without any gate policing less**?

The second clause is the whole difficulty. Reordering, parallelising and deduplicating gate
steps all reduce wall clock, and every one of them can also reduce coverage with no visible
symptom — a gate that checks less still exits 0. Faster and weaker are indistinguishable from
the outside, which is why the population manifest exists and why it is the first kill criterion
rather than the last.

## Measured baseline (2026-08-28, HEAD 0214b3c)

Established before any change, on this machine, Python 3.12.10, 12 logical CPUs:

| Quantity | Measured | Note |
| --- | --- | --- |
| Full suite, serial | 118.66 s (2401 passed, 4 skipped) | `pytest tests/ -q` |
| Slowest 25 tests, summed | ~72 s | So this is a long-tail suite, not a few slow tests |
| Collection only | 5.04 s | Fixed cost of every one of the 25 pytest invocations per gate |
| `dataforge --version` | 891–940 ms | Paid by all 39 subprocess launches in the suite |
| Bare interpreter | 93–108 ms | So ~820 ms of the above is DataForge import |
| `dataforge.cli` cumulative import | 700 ms | `cli.constraints` alone 189 ms (textual chain) |
| Gate population | 2405 node ids, 31 `_run` steps, 18 mutants, 79 claims | Frozen |

These are ranges where repeated, and single figures where measured once. The decimals are not
reproducible and are not bound to the claim ledger.

## Pre-committed kill criteria

Each fires automatically. None may be renegotiated after the fact.

| # | Criterion | Consequence |
| --- | --- | --- |
| K1 | `gate_population.py --check` reports any **removed** pytest node id, gate step, mutant, mutant test path, claim id, or scanned document | Revert the responsible change. A faster gate that polices less is not a faster gate. Additions are permitted and must be explained. |
| K2 | Parallel test execution fails to beat serial by **≥1.5x** on `measure_loop_cost.py` | `-n logical` does **not** become the default. The suite makes 39 subprocess launches and `test_concurrent_apply.py` runs two processes per test across 8 tests, so oversubscription is a real possibility. A sub-1.5x result is published, not hidden. |
| K3 | `tests/property/test_verifier_equivalence.py` reports any verdict divergence between the SMT and direct verifiers | The solver-reuse and undo-log changes revert **entirely**. |
| K4 | `measure_deductive_coverage.py` yields anything other than corrupted 0/86/116, repaired 393/451/451, and FD counts 53/81/85 | Revert the responsible step. These reproduced byte-identically under a prior audit; they are the non-circular oracle. |
| K5 | `measure_fd_mining_width.py` yields anything other than 2466 candidates and a 25.42x review ratio | The FD mining prune reverts. Both are bound ledger claims. |
| K6 | Any of the 18 auto-apply mutants survives | Revert. The guards are the product. |
| K7 | The tracked-file integrity guard fires during any test run | Stop. A test is writing the repository. |
| K8 | A container image tag is reused after a dependency file changed | The tag hash is wrong; no CI timing may be trusted until it is fixed. |

## What is deliberately excluded, and why

Each of these was considered and refused on evidence, not taste.

- **`pytest-testmon`.** Upstream documents that it does not track *"static files (txt, xml,
  other project assets)"*. This suite is corpus-driven — CSV and YAML fixtures — so a tool that
  is blind to data-file changes would silently stop re-running the tests that matter. The
  curated `test_map.json` is better here precisely because a human bound the dependency.
- **Caching Playwright browsers.** Playwright upstream: *"Caching browser binaries is not
  recommended, since the amount of time it takes to restore the cache is comparable to the time
  it takes to download the binaries."* Their recommendation is the container.
- **Caching `.hypothesis/`.** Hypothesis ships a built-in `ci` profile setting `database=None`
  and `derandomize=True`, and the docs *"recommend against relying on the database for the
  correctness of your tests"* because the database key changes when the test source changes.
  Consequence for this work: any `settings.load_profile` call must be **guarded**, or it will
  override the profile the maintainers auto-load in CI and quietly weaken it.
- **Batching the verifier.** The verifier already accepts a list of fixes, and the engine
  already passes one at a time, so batching looks like free speed. It is refused: verifying each
  fix individually is a safety property, and a batch could let one fix mask another's
  unsoundness. The per-flag table copy is removed by an **undo log** instead, which preserves
  the per-fix semantics exactly.
- **Unevidenced levers**: `-X frozen_modules` (no relevant evidence), `nektos/act` (a workflow
  emulator, not a speedup), `sccache` (a no-op without compiled extensions), and any
  vendor-published runner figure. Also noted: uv's "10–100x" is **not** a published measurement
  — its `BENCHMARKS.md` contains chart images and a reproduction script, no numeric results, for
  one project.

## Pre-committed expectations

Stated now so that being wrong is visible later.

1. **K2 is the criterion most likely to fire.** A subprocess-heavy suite parallelises poorly,
   and I expect well under linear scaling. If it fires, the honest outcome is that the largest
   local win comes from import cost, not from workers.
2. **Lazy CLI registration is the highest-confidence win.** 700 ms of import, paid 39 times in
   the suite and 22 more times in the release gate, for a command that prints a version string.
3. **The product's Θ(R²) repair behaviour is real but not what costs the day.** It is a
   user-facing cost at 200k rows; the developer loop is dominated by process startup and
   serialization. Both are worth fixing; only one shortens a session.

## Reporting rules

Fixed in advance, following [`docs/trust/fd-repair-scalability.md`](../../docs/trust/fd-repair-scalability.md):

- Exact timings are reported **unbound**, with the observed run-to-run range.
- **No timing is added to `docs/quantitative_claims.yaml`.** The ledger removed its only timing
  claim on 2026-08-27 with the rationale recorded in place; re-adding one would contradict a
  documented decision.
- Speedups are stated to **one significant figure**.
- The words `beats`, `outperforms`, `improves on` and `SOTA` are avoided in the four public
  documents `readme_truth.py` scans, because its `PUBLIC_CLAIM_PATTERNS` fire on them.
- What regressed, and what was attempted and abandoned, is published alongside what improved.

## Amendment 1 (2026-08-29): the loop was the wrong target, and expectation 3 was wrong

**Recorded before any optimisation in this phase lands.** The original document measured the
*developer* loop and explicitly deprioritised the product's repair cost. Expectation 3 above
reads: *"The product's Θ(R²) repair behaviour is real but not what costs the day."* That was
correct about the day and **wrong about where the product's cost is**, for a reason no instrument
in this repository could see: **nothing here has ever timed the verifier.**

### Why it was invisible

[`scripts/bench/measure_harness_cost.py`](../../scripts/bench/measure_harness_cost.py) times
`_acting_group` and `FDViolationRepairer.propose`. It does not call a verifier.
[`docs/trust/fd-repair-scalability.md`](../../docs/trust/fd-repair-scalability.md) therefore
states "`propose` is 93% of it" — true of the two functions measured, and not a statement about
end-to-end cost. The tax projection of about 5 hours is scoped to the write-exposure phase and
excludes verification, which the document says. No document states an end-to-end figure.

Meanwhile [`tests/benchmarks/bench_smt.py`](../../tests/benchmarks/bench_smt.py) asserts an SMT
p95 under **200 ms**, and [`tests/benchmarks/bench_safety_filter.py`](../../tests/benchmarks/bench_safety_filter.py)
asserts a safety-filter p95 under **1 ms**. `pytest tests/benchmarks/ --collect-only` reports
**"no tests collected"**: the files are named `bench_*.py`, `pyproject.toml` sets no
`python_files`, and there is no `tests/benchmarks/conftest.py`, so `make bench` collects nothing.
Both budgets have never executed. This is the orphaned-gate defect this repository has already
fixed three times, in the one place that would have caught a 1.2-second verifier.

### Measured baseline for this phase (2026-08-29, HEAD f41d8ab + the FD attribution fix)

`hospital`, 1,000 rows, 20 columns, 53 oracle FDs, one machine, no other load. Per-fix figures
are means over the repeat counts stated; end-to-end figures are projections from them and are
labelled as such.

| Quantity | Measured | Note |
| --- | --- | --- |
| `FDViolationDetector.detect` | 0.42 s | 28,679 flags collapsing to 7,905 distinct cells |
| `FDViolationRepairer.propose` | 1.43 ms/flag (40 calls) | 11.3 s for all 7,905 cells |
| `copy_table` | 0.10 ms (20 calls) | Two per `differential_verify` |
| `DirectVerifier.verify` | 29.9 ms/fix (10 calls) | Returns a real verdict |
| `SMTVerifier.verify` | 1,192 ms/fix (10 calls) | Verdict **UNKNOWN** |
| `differential_verify` | 1,524 ms/fix (10 calls) | The shipped default |
| SMT verdict distribution | **UNKNOWN on 60 of 60** real proposals | Not incidental; universal at this scale |
| **Projected end-to-end** | **about 200 minutes**, of which about 98% is SMT | 7,905 cells, one attempt each |

Encoding cost isolated from solve cost, same fix, varying only `timeout_ms`:

| `timeout_ms` | Cost/fix | Verdict |
| --- | --- | --- |
| 1 | 618 ms | UNKNOWN |
| 50 | 670 ms | UNKNOWN |
| 200 (shipped) | 1,192 ms | UNKNOWN |
| 2,000 | 1,509 ms | REJECT |

At a 1 ms budget a verify still costs 618 ms, so **that 618 ms is Python-side z3 AST
construction, not solving.** For one fix on this corpus: 10 relevant columns × 1,000 rows =
10,000 ground assertions, roughly 40,000 AST nodes, from `smt.py:224-236`. No timeout setting can
remove it. The shipped configuration pays 1.19 s to learn nothing; a 2 s budget pays 1.51 s to
learn what `DirectVerifier` returns in 29.9 ms.

### Diagnosis, stated as a claim to be tested and not as a conclusion

A constraint check on a concrete table has **no unknowns**, so its truth is settled by evaluation
rather than by search. The encoding asserts the whole table while each constraint's verdict
depends on far less: an FD `X -> Y` verdict depends only on rows sharing the candidate's `X`
value; UNIQUE is set membership in one column; domain bounds, accepted values, NOT NULL and regex
depend on the candidate row alone. The hypothesis of this phase is that **scoping the encoding to
that footprint is both much cheaper and strictly more informative**, because a scoped solve
returns ACCEPT or REJECT where the whole-table solve returns UNKNOWN.

### Additional kill criteria, fixed in advance

K1–K8 continue to apply unchanged. K3 and K4 are the primary fences for this phase.

| # | Criterion | Consequence |
| --- | --- | --- |
| K9 | Scoped SMT encoding does not reduce per-fix cost on `hospital` by **≥10x** | The locality refactor is abandoned and the result published, not defended. A 2x win does not justify touching the prove gate. |
| K10 | Any verdict in `tests/property/test_verifier_equivalence.py` changes, **including UNKNOWN becoming ACCEPT or REJECT on an input where the two verifiers previously disagreed** | Revert entirely. This is K3 restated for the encoding change, and it is deliberately stricter: more informative is only better if it is also identical where it was already decided. |
| K11 | The group index does not make `propose` cost **independent of table size at fixed group size** | It is not merged. An index that only moves the constant is the change already made on 2026-08-28. |
| K12 | Encoded assertion count remains a function of `row_count` after the locality change | The change did not do what it claims. Assertion count is the mechanism; wall clock is only its symptom. |

### Pre-committed expectations for this phase

1. **The whole-table encoding is the dominant cost and scoping it is the largest available win.**
   Stated at 98% of measured end-to-end cost, so if this is wrong the error will be large and
   obvious.
2. **`copy_table` hoisting is not worth doing.** Measured at 0.10 ms against 1,524 ms, i.e.
   0.013%. This contradicts the original document's own remedy — it names an undo log as the way
   to remove "the per-flag table copy". That remedy is hereby recorded as **aimed at a
   non-problem**; the per-fix table copy is real and cheap. The undo log is not pursued.
3. **Scoped SMT will start returning real verdicts, and this will surface latent disagreements
   with `DirectVerifier` that UNKNOWN has been masking.** If K10 fires it most likely fires here,
   and that would be a *correctness* finding worth more than the speedup.
4. **The detector ensemble is a distant third.** Measured at 0.42 s against 200 minutes. It is
   included because it is cheap to fix, not because it matters at this scale.

### Amendment 1 outcome, recorded 2026-08-29 after the run

| # | Criterion | Result |
| --- | --- | --- |
| K1 | Population removals | **Pass.** Only additions: the `latency budgets` gate step and four new test files. |
| K3 / K10 | Verifier equivalence | **Pass.** 200 Hypothesis examples, verdicts unchanged, and the property already asserted strict equality so ACCEPT agreement was covered. |
| K4 | Deductive coverage oracle | **Pass.** 393/451 repaired, 0/86 corrupted, FD counts 53/81/85, `replication_mismatches` 0 in all three arms. |
| K9 | Scoped SMT ≥10x per fix | **Not resolvable on this machine, and reported as such.** `bench_smt.py`'s own fixture went from failing at about 248 ms to passing at about 0.91 ms, which is far past 10x; but the `hospital` measurement varied 166-249 ms across three repeats, with one earlier single run reading 42 ms. Wall clock here is not reproducible enough to discharge the criterion, so it is left open rather than declared passed. |
| K11 | `propose` cost independent of table size | **Pass.** 0.010 / 0.008 / 0.011 ms per flag at 1,000 / 4,000 / 16,000 rows at fixed group size; 1 index build, 200 reuses, 0 rescans. |
| K12 | Assertion count no longer a function of `row_count` | **Pass, and this is the load-bearing evidence.** At 2,000 rows the encoding is 6 ground assertions against roughly 4,000 before. Counted, not timed, so it does not move with the machine. |

Expectations, scored:

1. **Right.** The whole-table encoding was the dominant cost, measured at about 98% of end-to-end.
2. **Right.** `copy_table` is 0.10 ms against 1,524 ms. Not pursued, and the original document's
   undo-log remedy is confirmed as aimed at a non-problem.
3. **Wrong, in the good direction, and then right after all.** Scoped SMT did start returning real
   verdicts -- UNKNOWN on 60 of 60 became REJECT. My first reading was that it surfaced **no**
   latent disagreement with `DirectVerifier`. That reading was wrong: a targeted test found
   **SMT ACCEPT against Direct UNKNOWN** whenever a relevant column holds an uncoercible value in a
   row outside the footprint. The differential failed closed so nothing unsound could be written,
   but two independent verifiers disagreeing on an input class is what the N-version design exists
   to detect, not absorb. The equivalence property could not see it: 2-4 row frames of well-typed
   values make footprint and table the same set. Parity was restored by checking coercibility over
   the whole relevant column while asserting only the footprint, and pinned on 40-row frames in
   `tests/unit/test_verifier_scope_parity.py`. **This was the most valuable finding of the phase,
   and it was a correctness finding produced by a performance change.**
4. **Right.** The detector ensemble is 0.42 s against minutes. Not pursued.

Two things I got wrong during the work, recorded because the rules above require it:

* **I twice attributed a slowdown to my own change without testing it.** A 29 s to 112 s whole-suite
  move looked like the group index. An A/B with the change stashed measured **112.69 s without it
  against 111.75 s with it** -- the change was performance-neutral on the suite and the earlier 29 s
  readings were the anomaly. This is the same defect the trust docs already name: reasoning about
  which thing looks expensive instead of measuring it.
* **I published a 36x end-to-end figure from single runs, then had to retract it to 6-9x** once I
  ran repeats, and it settled at **11x (136-143 ms/fix, 5% spread)** on a quiet machine. Three
  successive readings of the same code gave 42, 166-249, and 136-143 ms/fix. Within-configuration
  variance once exceeded the difference I was trying to measure between two configurations, so that
  comparison was abandoned as unresolvable rather than decided on the noisier reading. Only the
  counted assertion figures (4,000 to 6) are stable enough to quote without a range.

Not done, and not started: the shared detector column profiles and the gate wall-clock items, both
declined on measured grounds -- the ensemble is 0.42 s against minutes, and the gate items trade
real risk in the "faster and weaker are indistinguishable" layer for no product gain. Scoping
`DirectVerifier` is declined for a different and stronger reason: it would need its own,
independently written footprint computation, or the two verifiers would share a failure point and
the N-version property would be weakened rather than accelerated. That is a design change, not an
optimisation, and it belongs in its own pre-registration.

### Reporting rules for this phase

The rules above apply unchanged. Two additions:

- **The end-to-end figure must always carry its scope.** "About 200 minutes" is a projection from
  per-fix means on one corpus, one attempt per cell, not a measured wall clock of a full run.
  Stating it without that scope would repeat exactly the error this amendment corrects.
- **The corrected tax projection is published even though it is worse.** At 136-143 ms/fix and
  164,718 flags the verification phase alone is on the order of **6-7 hours**, against the
  roughly 5 hours the current document projects for write-exposure. Before this phase, at about
  1.5 s/fix, the same arithmetic gave about 68 hours. The number that flatters the product is the
  one that omits the verifier.

---

## Amendment 2, 2026-08-29: measurement fidelity

This amendment records a defect in the *instrument*, not in the product. It is registered here rather
than in a commit message because it bears on every timing this document has published.

### The finding

`TableLike` admits `pandas.DataFrame` and `dataforge.table.Table`. The split in use is total:

| path | representation | grouping branch |
| --- | --- | --- |
| CLI, via `read_csv` | `Table` | cached, `column_revision` available |
| every harness in `scripts/` | `pandas.DataFrame` | uncached, one scan per fix |

`DeterminantGroupIndex` keys its cache on a per-column write counter that only `Table` exposes.
So every published timing measured a branch the product never takes. The direction of the error
favours the product (the shipped path does less work), which is the direction that makes it easy to
leave alone, and is exactly why it is registered.

### Kill criteria added

- **K13.** If `Table` and `pandas.DataFrame` ever reach different verdicts on identical data, all
  timing claims sourced from pandas harnesses are withdrawn from `docs/trust/` in the same change,
  not annotated. Fenced by `tests/property/test_representation_parity.py`.
- **K14.** No new performance claim may be added to `docs/quantitative_claims.yaml` from a harness
  running `pandas.DataFrame` unless the claim text names the representation. Existing claims are
  grandfathered and annotated in `docs/trust/fd-repair-scalability.md`, not silently kept.
- **K15.** The counted-work budgets in `scripts/perf/measure_verifier_work.py` may not be raised in
  the same change that causes them to fail. Raising a budget to make a gate pass is the failure mode
  the gate exists to prevent, and this repository has already shipped three gates that could not
  fail.

### Expectation scored: peer deduplication

Pre-registered before implementing: deduplicating FD peers by dependent value should reduce ground
assertions materially on hospital, because determinant groups there run to dozens of rows over few
distinct dependent values. Kill criterion: if it does not reduce them, it is reverted rather than kept
on the argument that it should help.

Measured, on `Table`, hospital/oracle, per verified fix:

| stage | z3 value constructions | ground assertions | tracked assertions |
| --- | --- | --- | --- |
| whole-table encoding | 10,232 | -- | -- |
| after locality scoping | 734 | 232 | 12 |
| after peer deduplication | **176** | **73** | 12 |

Direction correct, magnitude larger than expected. `tracked` is unchanged at 12, which is right: it is
one assertion per relevant FD, and dedup shrinks each assertion rather than removing any. **No wall
clock is published for this change.** The counted figures are bit-identical across runs; the timings
over the same code spanned 79.8 to 352.2 ms/fix earlier in this programme and settled nothing.

Correctness fence: the K4 oracle held exactly -- FD counts 53/81/85, repairs 393/451/451, majority
corruptions 0/86/116, `replication_mismatches` 0 on all three arms. Dedup is semantics-preserving by
construction (duplicate conjuncts in a conjunction), and the oracle confirms it empirically.

### Still open

K9 remains open: this machine cannot produce stable wall-clock timings, and no Windows equivalent of
Cachegrind or `iai` was located. The harnesses still measure pandas. Both are recorded as limits, not
resolved.
