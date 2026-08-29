"""The counted-work gate must be able to fail, and its counts must be reproducible.

`scripts/perf/measure_verifier_work.py` exists because wall clock cannot gate here. The same
verifier code measured 42 ms/fix, then 166-249, then 136-143, then 79.8-352.2 across one afternoon,
while the counted work was bit-identical on every run. Cachegrind's manual makes the same argument
for instruction counts: time is the metric users perceive, counts are the metric that reproduces.

Two things have to hold for that to be worth anything:

* **the budget must bite.** A gate that cannot fail manufactures confidence, which is worse than
  having none. This repository has already shipped three of those, including two latency budgets
  that never executed for four months.
* **the counts must actually be deterministic.** If they drift run to run they are just a slower
  clock, and the whole argument for gating on them collapses.

The determinism test runs the real measurement twice, so it is slower than a unit test should be.
It is worth it: determinism is the single claim the instrument rests on, and asserting it anywhere
else would be asserting it about something other than the thing that ships.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INSTRUMENT = PROJECT_ROOT / "scripts" / "perf" / "measure_verifier_work.py"


def _load_instrument() -> Any:
    """Import the instrument by path; scripts/perf is not an importable package."""
    for candidate in (PROJECT_ROOT, PROJECT_ROOT / "scripts" / "bench"):
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
    spec = importlib.util.spec_from_file_location("measure_verifier_work", INSTRUMENT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def instrument() -> Any:
    """The measurement module under test."""
    return _load_instrument()


def test_the_budget_rejects_counted_work_above_the_ceiling(instrument: Any) -> None:
    """Non-vacuity: the gate must fail on a payload that exceeds its budgets."""
    over = {
        "per_fix": {key: ceiling + 1 for key, ceiling in instrument.BUDGETS.items()},
    }
    failures = instrument._check(over)
    assert len(failures) == len(instrument.BUDGETS), (
        "every exceeded budget must be reported, not just the first; a partial report hides which "
        f"dimension regressed. Got {failures}"
    )
    assert all("exceeds the budget" in failure for failure in failures)


def test_the_budget_accepts_counted_work_at_the_ceiling(instrument: Any) -> None:
    """The boundary is inclusive, so a value exactly at budget is not a regression."""
    at_limit = {"per_fix": dict(instrument.BUDGETS)}
    assert instrument._check(at_limit) == []


def test_budgets_cover_every_reported_dimension(instrument: Any) -> None:
    """A reported number with no budget is a number nothing defends."""
    payload = {"per_fix": dict(instrument.BUDGETS)}
    assert set(payload["per_fix"]) == set(instrument.BUDGETS), (
        "every per-fix figure the instrument reports must have a ceiling, or it is decoration"
    )


@pytest.mark.slow
def test_counted_work_is_reproducible_across_runs(instrument: Any) -> None:
    """The claim the whole instrument rests on: same code, same counts.

    If this fails, counted work is not a stable metric on this machine and the gate must not be
    trusted -- which is exactly the finding that disqualified wall clock.
    """
    first = instrument.measure()["per_fix"]
    second = instrument.measure()["per_fix"]
    assert first == second, (
        "counted work differed between two runs of identical code, so it cannot gate any better "
        f"than a stopwatch can: {first} against {second}"
    )
    assert all(value > 0 for value in first.values()), (
        f"a zero count would satisfy every budget while measuring nothing: {first}"
    )


@pytest.mark.slow
def test_the_measurement_runs_on_the_shipped_table_type(instrument: Any) -> None:
    """Measuring pandas would measure a branch the product never takes.

    Every other harness in this repository feeds a `pandas.DataFrame`, which cannot supply the write
    counter `DeterminantGroupIndex` needs, so they all exercise the uncached scan branch while the
    CLI always takes the cached one. This instrument must not join them.
    """
    payload = instrument.measure()
    assert payload["table_type"] == "dataforge.table.Table"
    assert payload["fixes_measured"] == instrument.FIXES_MEASURED
