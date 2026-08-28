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
