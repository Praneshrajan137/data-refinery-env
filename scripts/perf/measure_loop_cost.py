"""Measure the wall clock of the verification loop, with variance, before optimising it.

A speedup that was never measured is a claim, not a result. This script produces the artifact
that every later performance change is judged against, and it is deliberately built to make
one specific mistake impossible: reporting a single run as if it were the truth.

Three design decisions, each from a defect this repo already recorded:

* **Repeats and ranges, never a point estimate.** ``docs/quantitative_claims.yaml`` removed
  ``wide_table_seconds_at_100_columns`` on 2026-08-27 because a wall clock bound to four
  decimal places re-derived 28% higher on the same machine. So every step here reports min,
  median, max and the spread, and the write-up quotes ranges. Precision must not exceed
  reproducibility.
* **Steps run one at a time.** Timing several steps concurrently would measure contention
  rather than cost. A prior session corrupted a runtime measurement exactly that way.
* **The interpreter is explicit.** ``sys.executable`` is used for every child, never a bare
  ``python``. A bare ``python`` on this machine resolves to a 3.14 install with no dependencies,
  which once turned the mutation gate into a rubber stamp -- every mutant "died" of
  ``ModuleNotFoundError`` and was scored as killed.

Usage::

    python scripts/perf/measure_loop_cost.py --artifact eval/results/loop_cost.json
    python scripts/perf/measure_loop_cost.py --artifact <path> --only suite,imports
    python scripts/perf/measure_loop_cost.py --artifact <path> --include-gates
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Step:
    """One measured command.

    Attributes:
        key: Stable identifier used by ``--only`` and by the artifact.
        label: Human-readable description for the write-up.
        argv: Command to run, with ``sys.executable`` already substituted where needed.
        repeats: How many times to run it. Cheap steps get more; whole gates get one,
            and a single run is reported as such rather than dressed up as a median.
        group: Coarse bucket, so the artifact can be read by concern.
        expect_zero: Whether a non-zero exit invalidates the measurement. The mutation
            harness and the gates must succeed; a timing taken from a failed run measures
            how fast the thing broke.
    """

    key: str
    label: str
    argv: tuple[str, ...]
    repeats: int = 3
    group: str = "loop"
    expect_zero: bool = True


@dataclass
class Result:
    """Timings for one step."""

    step: Step
    seconds: list[float] = field(default_factory=list)
    exit_codes: list[int] = field(default_factory=list)

    def payload(self) -> dict[str, object]:
        """Render the measurement, keeping the spread visible."""
        ordered = sorted(self.seconds)
        body: dict[str, object] = {
            "label": self.step.label,
            "group": self.step.group,
            "command": " ".join(self.step.argv),
            "runs": len(ordered),
            "exit_codes": self.exit_codes,
            "seconds_min": round(ordered[0], 3),
            "seconds_max": round(ordered[-1], 3),
            "seconds_median": round(statistics.median(ordered), 3),
        }
        if len(ordered) > 1:
            spread = ordered[-1] - ordered[0]
            body["seconds_spread"] = round(spread, 3)
            body["spread_pct_of_min"] = (
                round(100.0 * spread / ordered[0], 1) if ordered[0] else None
            )
        else:
            body["seconds_spread"] = None
            body["spread_pct_of_min"] = None
            body["single_run_warning"] = (
                "One run only. Treat as an order of magnitude, not a reproducible figure."
            )
        return body


def _loop_steps() -> list[Step]:
    """Steps cheap enough to repeat: the inner loop a developer actually pays for."""
    py = sys.executable
    return [
        Step(
            key="interpreter_floor",
            label="Bare interpreter start (the floor every subprocess pays)",
            argv=(py, "-c", "pass"),
            repeats=5,
            group="imports",
        ),
        Step(
            key="cli_version",
            label="dataforge --version (full CLI import cost)",
            argv=(py, "-m", "dataforge", "--version"),
            repeats=5,
            group="imports",
        ),
        Step(
            key="pytest_collect_only",
            label="pytest collection only (fixed cost of every pytest invocation)",
            argv=(py, "-m", "pytest", "tests/", "--collect-only", "-q", "--no-header"),
            repeats=3,
            group="suite",
        ),
        Step(
            key="suite_serial",
            label="Full suite, serial (the committed default)",
            argv=(py, "-m", "pytest", "tests/", "-q", "--no-header"),
            repeats=2,
            group="suite",
        ),
        Step(
            key="suite_parallel_logical",
            label="Full suite, -n logical --dist loadgroup",
            argv=(
                py,
                "-m",
                "pytest",
                "tests/",
                "-q",
                "--no-header",
                "-n",
                "logical",
                "--dist",
                "loadgroup",
            ),
            repeats=2,
            group="suite",
        ),
        Step(
            key="lint",
            label="make lint equivalent (ruff check + format + 2 generators)",
            argv=(py, "-m", "ruff", "check", "dataforge", "tests", "scripts"),
            repeats=3,
            group="static",
        ),
        Step(
            key="mypy_core",
            label="mypy --strict dataforge (warm cache)",
            argv=(py, "-m", "mypy", "--strict", "dataforge"),
            repeats=2,
            group="static",
        ),
        Step(
            key="docs_truth",
            label="docs_truth --check",
            argv=(py, "scripts/ci/docs_truth.py", "--check"),
            repeats=3,
            group="truth",
        ),
        Step(
            key="readme_truth",
            label="readme_truth",
            argv=(py, "scripts/ci/readme_truth.py"),
            repeats=3,
            group="truth",
        ),
    ]


def _gate_steps() -> list[Step]:
    """The expensive gates. One run each: repeating a ten-minute gate is not worth the day."""
    py = sys.executable
    return [
        Step(
            key="mutation_gate",
            label="Auto-apply guard mutants (baseline + 18 mutants)",
            argv=(py, "scripts/ci/mutate_autoapply_guards.py"),
            repeats=1,
            group="gate",
        ),
        Step(
            key="release_gate",
            label="Release gate (build, wheelhouse, clean venv, 14 CLI smokes)",
            argv=(py, "-m", "dataforge.release.gate"),
            repeats=1,
            group="gate",
        ),
        Step(
            key="backend_gate",
            label="Canonical backend gate (whole tree)",
            argv=(py, "scripts/ci/backend_gate.py"),
            repeats=1,
            group="gate",
        ),
    ]


def _time_once(step: Step) -> tuple[float, int]:
    """Run a step once, returning ``(seconds, exit code)``."""
    started = time.perf_counter()
    completed = subprocess.run(
        list(step.argv),
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    return time.perf_counter() - started, completed.returncode


def measure(steps: list[Step]) -> list[Result]:
    """Run each step sequentially, reporting progress as it goes."""
    results: list[Result] = []
    for index, step in enumerate(steps, start=1):
        print(f"[{index}/{len(steps)}] {step.key}: {step.label} (x{step.repeats})", flush=True)
        result = Result(step=step)
        for run in range(step.repeats):
            seconds, code = _time_once(step)
            result.seconds.append(seconds)
            result.exit_codes.append(code)
            print(f"    run {run + 1}: {seconds:.2f}s exit {code}", flush=True)
            if step.expect_zero and code != 0:
                print(
                    f"    WARNING: {step.key} exited {code}. A timing from a failed run "
                    "measures how fast it broke, not how fast it works.",
                    file=sys.stderr,
                    flush=True,
                )
        results.append(result)
    return results


def build_payload(results: list[Result]) -> dict[str, object]:
    """Assemble the artifact, environment included so the numbers are interpretable."""
    return {
        "schema": 1,
        "measured_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "executable": sys.executable,
        },
        "binding_note": (
            "No value in this artifact is bound to docs/quantitative_claims.yaml. Timings are "
            "reported, not bound: the ledger removed its only timing claim on 2026-08-27 after a "
            "four-decimal wall clock re-derived 28% higher on the same machine. Quote ranges."
        ),
        "steps": {result.step.key: result.payload() for result in results},
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path, help="Where to write the JSON.")
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated step keys to run. Default: every loop step.",
    )
    parser.add_argument(
        "--include-gates",
        action="store_true",
        help="Also measure the mutation, release and backend gates. Slow.",
    )
    args = parser.parse_args(argv)

    steps = _loop_steps()
    if args.include_gates:
        steps += _gate_steps()
    if args.only:
        wanted = {key.strip() for key in args.only.split(",") if key.strip()}
        known = {step.key for step in _loop_steps() + _gate_steps()}
        unknown = sorted(wanted - known)
        if unknown:
            print(f"Unknown step key(s): {unknown}. Known: {sorted(known)}", file=sys.stderr)
            return 2
        steps = [step for step in steps if step.key in wanted]

    results = measure(steps)
    payload = build_payload(results)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"\nWrote {args.artifact}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
