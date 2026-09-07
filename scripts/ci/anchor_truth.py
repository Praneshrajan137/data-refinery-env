"""CI check: the committed benchmark anchors still match what the code produces.

## The hole this closes

Three gates read `eval/results/agent_comparison.json`, and every one of them compared it to
something *other than the code*:

- `scripts/ci/benchmark_truth.py` checks the generated report agrees with the artifact.
- `scripts/ci/docs_truth.py` checks prose agrees with the artifact.
- `tests/unit/test_corpus_tiering.py` checks the artifact agrees with a constant in the test.

So prose was verified against the artifact and the artifact against prose, and **the code sat
outside the loop entirely.** Nothing re-ran the benchmark. `test_corpus_tiering`'s own docstring
identifies the mirror-image failure -- "does not help if the artifact itself is overwritten by a
bad run" -- while committing this one.

It drifted, in two steps, and every gate stayed green for 54 days:

| commit | date | hospital fp | F1 |
| --- | --- | --- | --- |
| `236df758` (the artifact) | 2026-07-15 | 178 | 0.7926 |
| `c207617` refuse uncheckable-detector writes | 2026-08-22 | 143 | 0.8178 |
| `4ad3760` strict-majority `_deterministic_choice` | 2026-08-25 | 120 | 0.8352 |

`tp` (451) and `fn` (58) never moved, so both steps were genuine precision gains rather than a
relabelled corpus -- but nothing measured that, and `4ad3760` recorded "hospital byte-identical"
in its own pre-registration on the strength of a *write-path* measurement while the proposal
stage moved by 23 false positives.

This gate re-runs `run_heuristic_episode` -- the very function the bench runner calls, not a
private helper or a reimplementation -- and compares it to the committed record. It is the
missing code-to-artifact edge.

## Why it is unconditional despite the cost

hospital takes about a minute (flights about a second). That is in line with the "latency
budgets" step, and a gate on the project's most-cited number that only runs when someone
remembers is not a gate. The alternative -- a `@pytest.mark.slow` test excluded from the default
run -- is precisely the dead-gate pattern this repository has been burned by before.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dataforge.bench.methods import run_heuristic_episode
from dataforge.datasets.real_world import load_real_world_dataset

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = PROJECT_ROOT / "eval" / "results" / "agent_comparison.json"
_METHOD = "heuristic"
_F1_PLACES = 4


def _committed(artifact: Path) -> dict[str, dict[str, object]]:
    """Return one committed heuristic record per dataset.

    Seeds are collapsed because the heuristic method is deterministic: the artifact carries
    three identical records per dataset, and re-measuring each would triple the runtime to
    prove the same thing.
    """
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    by_dataset: dict[str, dict[str, object]] = {}
    for record in payload.get("records", []):
        if record.get("method") != _METHOD:
            continue
        dataset = record.get("dataset")
        if isinstance(dataset, str) and dataset not in by_dataset:
            by_dataset[dataset] = record
    return by_dataset


def _measured(value: float | int | None, field: str, dataset: str) -> float:
    """Read one numeric field off a fresh measurement, refusing to guess.

    `SeedBenchmarkResult` types these as optional. A `None` here means the episode produced no
    metric, and comparing `None` to `None` would pass vacuously -- the exact failure this file
    exists to prevent, so it is an error instead.
    """
    if value is None:
        raise SystemExit(
            f"FAIL: re-measuring {dataset} produced no '{field}'. The episode did not score, "
            "so there is nothing to compare the artifact against."
        )
    return float(value)


def _number(record: dict[str, object], field: str, dataset: str) -> float:
    """Read one numeric field from a committed record, refusing to guess.

    A missing or non-numeric field means the artifact is malformed. Defaulting it would let a
    truncated record compare equal to a real measurement, which is the vacuous pass this whole
    file exists to prevent.
    """
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SystemExit(
            f"FAIL: committed record for {dataset} has no usable numeric '{field}' "
            f"(found {value!r}). The artifact is malformed; regenerate it."
        )
    return float(value)


def main() -> int:
    """Re-measure each committed heuristic anchor and compare it to the artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Accepted for symmetry; the default.")
    parser.add_argument("--artifact", type=Path, default=ARTIFACT)
    args = parser.parse_args()

    if not args.artifact.exists():
        print(f"FAIL: {args.artifact} is missing; there is no anchor to check.", file=sys.stderr)
        return 1

    committed = _committed(args.artifact)
    if not committed:
        print(
            f"FAIL: {args.artifact} holds no '{_METHOD}' records. A gate that checks nothing "
            "passes vacuously, which is the failure mode this file exists to prevent.",
            file=sys.stderr,
        )
        return 1

    failures: list[str] = []
    for dataset in sorted(committed):
        record = committed[dataset]
        try:
            loaded = load_real_world_dataset(dataset)
        except Exception as error:  # noqa: BLE001
            failures.append(f"{dataset}: could not load the corpus to re-measure it ({error})")
            continue

        measured = run_heuristic_episode(loaded, seed=int(_number(record, "seed", dataset)))
        actual = {
            "tp": _measured(measured.tp, "tp", dataset),
            "fp": _measured(measured.fp, "fp", dataset),
            "fn": _measured(measured.fn, "fn", dataset),
            "f1": round(_measured(measured.f1, "f1", dataset), _F1_PLACES),
        }
        expected = {
            "tp": _number(record, "tp", dataset),
            "fp": _number(record, "fp", dataset),
            "fn": _number(record, "fn", dataset),
            "f1": round(_number(record, "f1", dataset), _F1_PLACES),
        }
        if actual != expected:
            differing = ", ".join(
                f"{field}: artifact {expected[field]} -> code {actual[field]}"
                for field in ("tp", "fp", "fn", "f1")
                if expected[field] != actual[field]
            )
            failures.append(f"{dataset}: {differing}")
        else:
            print(
                f"  {dataset}: F1 {actual['f1']} (tp {actual['tp']:.0f}, fp {actual['fp']:.0f}) matches"
            )

    if failures:
        print("Anchor truth check FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            "\nThe committed artifact no longer describes what the code does. Do NOT simply "
            "regenerate it: identify WHY the number moved first. An upward drift from a "
            "relabelled corpus or a changed scorer looks identical to a genuine improvement, "
            "and 'tp and fn unchanged' is the cheap way to tell them apart. Then regenerate "
            "with the FULL matrix and propagate the number to every document that states it.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Anchor truth check passed. Re-measured {len(committed)} committed {_METHOD} "
        f"anchor(s) against the code that produces them."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
