"""The latency budgets must remain collectable, and the gate must keep running them.

Two budgets existed in ``tests/benchmarks/`` from 2026-04-20 and neither had ever executed. The
files are named ``bench_*.py``; pytest's default ``python_files = test_*.py *_test.py`` does not
match that, and no conftest overrode it. So ``make bench`` collected zero tests and exited 5,
and ``pytest tests/benchmarks/ --collect-only`` reported "no tests collected". The SMT budget it
was silently not enforcing -- p95 under 200 ms -- fails at roughly 248 ms mean and 607 ms max on
its own 1000-row fixture.

That is the orphaned-gate defect this repository has already fixed three times, in the one place
that would have caught a 1.2-second verifier. These tests exist so it cannot recur:

* a rename, a move, or a ``python_files`` change that empties the directory fails here rather
  than silently reporting success;
* removing the gate step fails here as well as in ``gate_population.py``;
* a budget file that contains no threshold assertion is caught, because a benchmark that asserts
  nothing is the same defect wearing a different hat.

The collection flags are asserted to match the gate's, rather than being written twice and
trusted to agree.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
BENCHMARK_DIR: Final[Path] = PROJECT_ROOT / "tests" / "benchmarks"

#: The flags the gate uses. `-o python_files` because of the bench_*.py naming, and `-n 0`
#: because pytest-benchmark auto-activates --benchmark-disable under xdist while this repo's
#: addopts carry `--dist loadgroup` -- without it pytest exits 4 on a usage error, which is
#: indistinguishable from a pass unless someone reads the exit code.
COLLECTION_FLAGS: Final[tuple[str, ...]] = ("-o", "python_files=bench_*.py", "-n", "0")

GATE_STEP_NAME: Final[str] = "latency budgets"


def test_benchmark_directory_collects_at_least_one_test() -> None:
    """The budgets must be reachable under the gate's own flags."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(BENCHMARK_DIR),
            *COLLECTION_FLAGS,
            "--collect-only",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=300,
    )
    assert result.returncode == 0, (
        "collection under the gate's flags must succeed. A usage error (exit 4) here is how a "
        f"latency gate goes dark without anyone noticing.\nstdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    node_ids = [line for line in result.stdout.splitlines() if "::" in line]
    assert node_ids, (
        "tests/benchmarks/ collected NOTHING. This is the exact state the directory was in "
        "until 2026-08-29, when both latency budgets had never run. If the files were renamed "
        "to test_*.py, update COLLECTION_FLAGS here and in scripts/ci/backend_gate.py together."
    )


def test_the_gate_still_runs_the_budgets() -> None:
    """The backend gate must keep a step that executes the benchmark directory."""
    sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "ci"))
    try:
        from gate_population import _step_names  # noqa: PLC0415
    finally:
        sys.path.pop(0)

    names = _step_names(PROJECT_ROOT / "scripts" / "ci" / "backend_gate.py")
    assert GATE_STEP_NAME in names, (
        f"the {GATE_STEP_NAME!r} step is gone from backend_gate.py. Budgets that only run when "
        "someone remembers to run them are not budgets."
    )


def test_every_budget_file_asserts_a_threshold() -> None:
    """A benchmark that asserts nothing measures nothing."""
    budget_files = sorted(BENCHMARK_DIR.glob("bench_*.py"))
    assert budget_files, "no bench_*.py files found; this test would be vacuous"

    for path in budget_files:
        source = path.read_text(encoding="utf-8")
        assert "assert" in source and "stats" in source, (
            f"{path.name} contains no threshold assertion over benchmark statistics. "
            "pytest-benchmark records timings by default and fails nothing, so a file without "
            "an explicit assert is a report, not a gate."
        )
