# The verification loop: 119 seconds to 28, and what the measurement refused

**Status**: measured 2026-08-28. Artifacts: `eval/results/loop_cost.json` (timings),
`eval/results/gate_population.json` (the set of checks each gate polices). Reproduce with
`python scripts/perf/measure_loop_cost.py --artifact eval/results/loop_cost.json` and
`python scripts/ci/gate_population.py --check`.

Every figure below is a wall clock on one machine — Python 3.12.10, 12 logical CPUs, the repository
on a OneDrive-synced path — and is therefore **reported, not bound to the claim ledger**. Ranges are
given where a step was repeated. Speedups are stated to one significant figure because that is what
a wall clock supports. `docs/trust/fd-repair-scalability.md` explains why this project stopped
binding timings at all.

## The problem was serialization, not computation

Nothing in this repository ever did two things at once. A grep for
`multiprocessing|concurrent.futures|ThreadPoolExecutor|-n auto|xdist` across every gate script, the
Makefile and the CI workflow returned exactly one hit: the line in `pyproject.toml` declaring
`pytest-xdist` as a dev dependency. It was declared and invoked by nothing.

Meanwhile one `backend_gate.py` run was 85 steps, 73 subprocess launches, 25 pytest invocations and
6 wheel builds, strictly sequential — inside which `test_autoapply_decision_table.py` ran seven
times, `test_label_noise_certification.py` seven times, `readme_truth.py` twice, and the
`dataforge_07` wheel was built twice.

## What was measured, before and after

| Step | Before | After | Note |
| --- | --- | --- | --- |
| Full suite | 118.7 s (2,401 tests) | 27.8–28.2 s (2,431 collected) | `-n logical`, 12 workers |
| Full suite, serial | 118.7 s | 85.6–86.5 s with `-n 0` | see the variance section |
| `dataforge --version` | 891–940 ms | 140–170 ms | bare interpreter is 70–110 ms |
| `dataforge.cli` import | 700 ms cumulative | not imported | lazy subcommand registration |
| pytest collection | 5.0 s | 5.4–6.2 s | unchanged in kind; a fixed cost of all 25 invocations |
| FD `propose`, 200k rows | 592–595 ms | 89–100 ms | see the FD scalability document |
| Backend gate, end to end | not measured before | 1,436 s | two concurrent groups |
| Auto-apply mutants | 47.4 s | 47.4 s | left sequential, deliberately |

Roughly **4x** on the suite and roughly **6x** on both the CLI launch and the FD repair path.

The suite figure is not the whole story: the serial number fell **while 26 tests were added**, and
the drop is close to the 39 subprocess launches in the suite times the ~750 ms of import each one
stopped paying.

**The serial figure moved on re-measurement, I attributed it to the wrong cause, and the correction**
**is the useful part.** A run immediately after the import change gave 91.3 s; two runs later in the
same session gave 110.9 s and 111.6 s. I read that as ~20% variance, attributed it to the repository
sitting on a OneDrive-synced path, and published that reading.

It was wrong. Those were **two different commands**. The 91.3 s run passed `-n 0`; the 110.9/111.6 s
runs came from `measure_loop_cost.py`, which omits `-n` entirely. Compared within a fixed command,
110.9 against 111.6 is a 0.6% spread — not variance at all.

The repository was relocated to `C:\dev\dataforge`, off OneDrive, and each command re-run there:

| Command | OneDrive path | `C:\dev\dataforge` |
| --- | --- | --- |
| `pytest tests/ -n 0` | 91.3 s (1 run) | 85.6 / 86.0 / 86.5 s |
| `pytest tests/` (no `-n`) | 110.9 / 111.6 s | 99.75 / 100.01 s |
| `pytest tests/ -n logical` | 32.4 / 34.5 s | 27.8 / 28.2 s |

So the honest result is a **consistent 6–15% improvement** from leaving OneDrive, with spreads of
0.3–1.1% on both disks. There was no variance to collapse; my inference invented one out of a
command difference. The single unexplained outlier — a parallel coverage run that took 294 s once
against 48–60 s either side — remains unexplained and was one event, not a pattern.

The relocation was still worth doing, for a reason the timings understate: OneDrive was syncing
107,650 files and 3.07 GB of build output, virtualenv and caches on every change.

Two further qualifications, both against my own numbers. The relocated environment runs **five**
**fewer tests** — `tests/integration/test_openenv_core_adapter.py` and four in
`tests/unit/test_model_space_contract.py` now skip, because the rebuilt virtualenv omits `openenv`
and `gradio`. Those are optional extras **CI does not install either**, so local now matches CI
rather than exceeding it, and the previous superset is what an earlier session recorded as the cause
of a local pip-audit failure CI did not have. But `test_openenv_core_adapter` alone was 5.76 s in the
original baseline, so a good part of the serial gain is those tests not running rather than the disk.
And `-n 0` measures about 14% faster than omitting `-n` on both disks, which is a real difference
between two commands I had been treating as equivalent.

No decimal in this document is bindable, which is the same conclusion
`docs/trust/fd-repair-scalability.md` reached about millisecond figures. This section is why: the
figures were stable all along, and the instrument that misled me was my own reading of them.

No decimal in this document is bindable, which is the same conclusion
`docs/trust/fd-repair-scalability.md` reached about millisecond figures.

## The measurement refuted three things I had planned

Recorded because being wrong in public is the only reason a pre-registration is worth writing. All
three were pre-committed in `eval/preregistration/performance.md` before any change landed.

**Parallel test execution would under-deliver.** I expected a suite making 39 subprocess launches,
with `test_concurrent_apply.py` running two processes per test, to oversubscribe and scale badly.
The kill criterion was 1.5x. It came in at about 4x. Those launches are I/O-bound waiting on child
processes, which is the case that parallelises best — the opposite of my reasoning.

**Isolating the mutation harness into git worktrees was worth doing.** It is not. The harness
measures 47.4 s for a green baseline plus 18 mutants, so worktree lifecycle complexity would have
bought about 30 seconds. It stays sequential, and the reason is now correctness rather than speed.

**Parallelising the coverage step was free speed.** It is not free. Parallel coverage ran 48–60 s
against 126 s serial, but the reported total moved across three runs: 83.73%, 83.75%, 83.76%. That
gate enforces thresholds — 82% policy, 88% critical path — and this project's CI has already failed
once at 84.98% against a required 85%. A threshold gate whose input wobbles run to run is the same
defect that made this repository pin its ruff and mypy versions. The step stayed serial and the
speed was taken from the suite's own parallel run instead.

## The prerequisite was a bug, not a configuration change

`pytest -n auto` was unsafe, and the reason was live: `tests/unit/test_docs_truth.py` falsified three
**committed** files — `DECISIONS.md`, `eval/results/free_vs_llm_ranker.json` and
`docs/trust/apply-rewrites-line-endings.md` — and restored them in a `finally`. Correct serially. In
parallel, two workers can each read the original bytes while the other holds the file falsified, and
the second restore then writes the falsified bytes back permanently.

That is the same failure shape that once left the write-safety allowlist **inverted** in the working
tree after a killed mutation run — `not in` had become `in` — caught only because someone read
`git status` before staging. A `finally` cannot survive a race or a hard kill.

So the fix was not a flag. `docs_truth.py` took a `--root` argument, its tests build a sandbox
**derived from the ledger itself** so they cannot check a thinner population than CI does, and a
session-scoped fixture now compares `git status` before and after every test session and names any
test that leaves the tree changed. That guard is deliberately about the class, not either instance.

## What the work found that was not about speed

Parallelising and wiring things up surfaced five defects that had nothing to do with performance.
Each is a case of a check that existed and did not run.

- **Three gate harnesses had never executed.** `attestation_conformance.py`,
  `mutate_domain_vocabulary.py` and `mutate_adversarial_corpus.py` appeared only in the Makefile's
  mypy argument list — type-checked, never invoked. This is the same orphaned-gate defect fixed for
  `mutate_autoapply_guards.py` on 2026-08-26 and never generalised to its siblings.
- **A mutant that could never apply.** `mutate_adversarial_corpus.py`'s fourth mutant anchored on
  `covered: set[str] = set(schema.columns)`, which no longer exists — `authoritative_columns` was
  narrowed to require a discriminating type. It was a permanent no-op reported as "the corpus does
  not actually test the gate". Now 4 of 4.
- **Two harnesses left the tree phantom-modified.** They restored via `write_text(newline="")`
  instead of the original bytes, emitting LF where the file was CRLF — a "modified" file with an
  empty content diff. Worse, it made them state-dependent: the first run on a fresh checkout wrote
  LF, so every later run saw LF, and a multi-line anchor could fail on run one and pass on run two.
- **`.gitignore` matched `.coverage` but not `.coverage.*`.** Harmless while the suite was serial;
  under `-n logical` every worker writes one, so a parallel coverage run left a dozen untracked
  files. Found by the new tree-integrity guard on its first real outing.
- **The claim ledger kept two false numbers through a code change.** Documented in
  `docs/trust/fd-repair-scalability.md`: `docs_truth.py --check` passed while `propose_share` read
  99% against a measured 93%, because claims bind a document to a committed artifact rather than to
  executing code. First recorded false negative from a known architectural limit.

## Why a population manifest exists

Reordering, parallelising and deduplicating gate steps all reduce wall clock, and every one of them
can also reduce coverage with no symptom — a gate that checks less still exits 0. Faster and weaker
are indistinguishable from outside.

`scripts/ci/gate_population.py` therefore freezes the derived set of checks: every pytest node id,
every gate step name, every mutant and its test paths, every claim id, every scanned document.
Everything is derived — mutant ids by importing `MUTANTS`, claim ids from the ledger loader, node ids
from pytest's own collector, step names from an AST parse — because a hand-maintained copy can agree
with the manifest while both disagree with the gate.

It earned itself twice. When steps moved into concurrent groups, the `_run`-only parse reported **21
steps removed**; nothing had stopped being checked, but the manifest cannot distinguish "moved" from
"deleted", so it refused. Teaching it to read the second constructor was the fix — re-emitting over
the alarm would have hidden a real deletion next time. And it is what proved the concurrency
refactor lost nothing: zero removals, four additions, all four being the newly-wired harnesses.

## What was not done

- **The `dataforge_07` wheel is still built twice** per backend gate run, once by the gate and once
  inside the release gate. Passing the artifact through was planned and not implemented.
- **The release gate's fourteen sequential CLI smokes are still sequential.** Seven of them are
  read-only and mutually independent.
- **CI is unchanged.** The container image, the removal of `needs: quality`, and the deduplication of
  the three full suite runs per push are all still planned. The CI critical path is still additive.
- **The quadratic shape of FD repair remains**, and the remedy was explicitly declined; see the FD
  scalability document for why memoising over a mutable table on the write path was refused.
- **`-n logical` needs `psutil` on Python before 3.13** and falls back to physical cores *silently*
  without it. It is now a declared dev dependency, but a silent fallback is a poor design and the
  declaration is the only thing standing between the command and a wrong unit.
