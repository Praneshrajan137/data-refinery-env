# Wide tables: the cliff is the review queue, not the CPU

**Status**: measured 2026-08-26. Artifact: `eval/results/fd_mining_width.json`. Reproduce with
`python scripts/bench/measure_fd_mining_width.py --artifact eval/results/fd_mining_width.json`.

## Why this was measured

`dataforge/schema_inference.py::_fd_candidates` is a bare nested loop over the column list, so it is
Theta(C^2 * R), with **no column cap and no timeout**. The widest table this project had ever run it
against was **hospital at 20 columns**; flights has 7, rayyan 11, tax 15. The two 1200-column
benchmarks are not a counterexample — each row there is a single-column record, so `fd_violation` is
structurally `not_applicable` and the miner cannot run on that corpus at all.

So wide-table behaviour was entirely unmeasured, on the one component every zero-config user reaches.

## What was measured

500 rows, synthetic tables whose columns survive the miner's determinant guards:

| columns | ordered pairs | seconds | candidates a human must adjudicate |
| --- | --- | --- | --- |
| 10 | 90 | 0.0152 | 28 |
| 20 *(hospital's width)* | 380 | 0.0327 | 97 |
| 40 | 1560 | 0.1186 | 398 |
| 60 | 3540 | 0.2749 | 885 |
| 80 | 6320 | 0.4378 | 1588 |
| **100** | 9900 | 0.6437 | **2466** |

The candidate counts are **bound** in `docs/quantitative_claims.yaml`; the `seconds` column is
deliberately **not**, and neither is the time ratio below.

The seconds are a record of one run on one machine, not a reproducible quantity. Re-deriving this
artifact on the same machine on 2026-08-27 returned **0.8263 s** at 100 columns against the 0.6437 s
recorded here -- **28% higher** -- moving the time ratio from 19.69x to 24.23x, while every candidate
count reproduced *exactly*. A wall-clock figure published to four decimal places reads as a stable
fact and is not one; binding it would make CI fail on a busy machine and teach everyone to ignore a
red gate. Counts are bindable, timings are not, and the distinction is the reason this table has two
kinds of column.

100 columns is roughly **20x** the time and **25.42x** the review rows of a 20-column table. The time
ratio tracks the pair-count ratio, which confirms the quadratic loop dominates; it is stated to one
significant figure because that is all a wall clock supports.

## The finding, which refutes the framing that motivated the measurement

**I expected a performance cliff and there is not one.** 0.64 seconds for 100 columns is
unremarkable, and even scaling to tax's 200,000 rows puts a 100-column mining pass in the region of
minutes rather than hours. The Theta(C^2 * R) complexity is real and the wall-clock consequence is
not the problem.

**The cliff is the human.** A 100-column table produces **2,466 constraint candidates**, every one of
which is a row someone must accept or reject in `constraints review`, and nothing caps that number.
Extrapolating on the pair count, a 500-column table would produce on the order of 60,000 candidates.

That reframes the risk. It is not a denial of service against the process; it is a denial of service
against the reviewer — and it lands on the one gate that stands between a mined dependency and an
unsupervised write. `docs/trust/shipped-premise-result.md` measures what accepting mined dependencies
costs when a human reviews 85 of them on a 20-column table. Nothing is known about what a human does
when handed 2,466, and the plausible behaviours — accept all, reject all, spot-check — are all worse
than reviewing.

It also compounds a cost already measured elsewhere: accepting hospital's mined dependencies takes
its queue from 549 cells at 0.5610 precision to 10,373 at 0.0440, i.e. 1.78 to 22.80 cells reviewed
per real error. Wide tables multiply the number of *decisions* that lead into that.

## What this authorises

- Stating the wide-table limit as a **review-burden** limit with a number behind it, replacing the
  previous position, which was silence.
- Retiring the derivation I had reasoned to before measuring — that a 500-column table lands in the
  hours-to-days range. That was arithmetic on the loop shape with a throughput guess, and the guess
  was wrong by orders of magnitude. The pair-count arithmetic was right; the conclusion drawn from it
  was not.

## What this does NOT authorise

- **Any claim about wide-table repair quality.** This measures how many candidates appear and how
  long they take to appear. It says nothing about how many are true. FD-set precision on a wide table
  is unmeasured, and there is no wide corpus with ground truth to measure it on.
- **Reading 0.6437 seconds as "wide tables are fine".** They are fine for the CPU at 500 rows. The
  cost that matters was not the one being timed.
- **A column cap.** Capping mining at some width would silently change the premise a user gets, and a
  premise mined from a subset of columns is not a weaker version of the full premise — it is a
  different one. If a limit ships it must be a **reported refusal**: `profile` declines, says why, and
  emits nothing, so the user knows they have no premise rather than an abridged one.

## What would close the remaining gap

1. A wide corpus with retained ground truth, to measure whether FD-set precision degrades with width.
   None exists, and this is the same shape of blocker as the false-dependency validation problem: the
   evidence does not exist rather than the question being closed.
2. A measurement of what a human does with a 2,466-row review queue, which belongs with the
   reviewer-decision work pre-registered in `eval/preregistration/reviewer_decision_quality.md` and
   inherits its power problem.
