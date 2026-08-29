"""Measure verification cost as COUNTED WORK, not as wall clock, and gate on it.

## Why this exists

This repository has no automated performance gate on the Python side, and the reason is recorded
rather than accidental: `eval/preregistration/performance.md` forbids adding a timing to the claim
ledger because "a four-decimal wall clock re-derived 28% higher on the same machine", and
`tests/unit/test_cli_import_cost.py` refuses a millisecond threshold because "wall clock on a shared
machine is not reproducible enough to gate".

Both refusals are correct about time and neither is a reason to leave performance ungated. During
the 2026-08-29 verifier work the same wall clock produced, for *identical code*, per-fix figures of
42 ms, then 166-249 ms, then 136-143 ms, then 79.8-352.2 ms. A gate on any of those numbers would
fire on machine load. Meanwhile the change that mattered was measurable exactly: the coercibility
sweep went from **10,232 z3 value constructions per fix to 232**, and that figure is identical on
every machine.

Valgrind's Cachegrind manual states the tradeoff in both directions, and is worth quoting because it
concedes the point against itself first:

    "execution time is a better metric than instruction counts because it's what users perceive.
    However, execution time often has high variability ... In contrast, instruction counts are
    highly reproducible ... This means the effects of small changes in a program can be measured
    with high precision."
    -- https://valgrind.org/docs/manual/cg-manual.html

The Rust `iai` crate makes the CI case explicitly ("Iai can work reliably in noisy CI environments
or even cloud CI providers like GitHub Actions"). Neither tool is usable here: both document that
they cannot run on Windows, and CodSpeed's equivalent mode is Valgrind-backed. So this measures the
counts that matter for *this* encoder directly, in Python, which needs no external tool and works on
every platform CI runs.

## What is counted, and why these

* **z3 value constructions** (`StringVal`, `IntVal`, `RealVal`) -- the Python-side AST construction
  that dominated verification. This is the number that caught the coercibility defect.
* **`solver.add` calls** -- ground assertions, i.e. the footprint actually encoded.
* **`solver.assert_and_track` calls** -- tracked constraints, i.e. what can appear in an unsat core.

These are proxies for cost, not cost. `iai` is candid about the same limitation -- "Iai's
measurements merely correlate with wall-clock time (which is usually what you actually care
about)" -- and so is this script. A change that halves assertion counts while tripling solve time
would pass this gate and should be caught by the latency budgets in `tests/benchmarks/`. The two
instruments are complementary: counts detect *structural* regressions reproducibly, timings detect
*actual* slowness unreproducibly.

## What is measured, and on which representation

`hospital` with the oracle premise, through `dataforge.table.Table` -- **the representation the CLI
actually ships**, obtained via `read_csv`. This is deliberate. Every other harness in this repository
feeds a `pandas.DataFrame`, which cannot supply the write counter `DeterminantGroupIndex` needs, so
they all silently exercise the uncached scan branch while the product always takes the cached one.
Measuring the shipped representation is the point.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "scripts" / "bench") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "bench"))

#: Ceilings on counted work per verified fix. Loose enough not to fire on a harmless refactor, tight
#: enough to actually catch something -- a budget with 11x headroom, which is what these had before
#: peer deduplication landed, defends nothing and is indistinguishable from having no gate.
#:
#: Provenance, all measured on hospital/oracle through Table on 2026-08-29:
#:
#:   =========================  ========  =======  ======  ======
#:   stage                      z3 vals   ground   track   note
#:   =========================  ========  =======  ======  ======
#:   whole-table encoding         10,232      n/a     n/a   pre-locality
#:   after locality scoping          734      232      12
#:   after peer deduplication        176       73      12   current
#:   ceiling below                    400      160      30   ~2.3x current
#:   =========================  ========  =======  ======  ======
#:
#: The ceilings sit at roughly 2.3x the measured figures. That tolerates ordinary churn while still
#: failing loudly on the one regression that matters: an encoding that stops being local and goes
#: back to touching every row, which lands two orders of magnitude above these lines. `tracked` is
#: one assertion per relevant FD, so its ceiling has more slack -- the FD set is a property of the
#: corpus, not of the encoding, and a legitimately richer premise would raise it.
BUDGETS: Final[dict[str, int]] = {
    "z3_value_constructions_per_fix": 400,
    "ground_assertions_per_fix": 160,
    "tracked_assertions_per_fix": 30,
}

FIXES_MEASURED: Final[int] = 15


def _load_case() -> tuple[Any, Any, list[Any]]:
    """Return ``(table, schema, proposals)`` for hospital/oracle on the shipped Table type."""
    # Imported from scripts/bench rather than reimplemented, so the premise this measures is the
    # SAME premise the coverage harness measures. `scripts/bench` is not on mypy's path (it is not
    # in the Makefile's strict list), hence the ignore; restating oracle FD discovery here to avoid
    # it would be the "derive, never restate" defect this repository keeps fixing.
    from measure_deductive_coverage import (  # type: ignore[import-not-found]
        _schema_for,
        discover_oracle_fds,
    )

    from dataforge.datasets.real_world import load_real_world_dataset
    from dataforge.detectors.fd_violation import FDViolationDetector
    from dataforge.repairers.fd_violation import FDViolationRepairer
    from dataforge.table import Table

    dataset = load_real_world_dataset("hospital")
    dirty_frame = dataset.dirty_df
    columns = [str(column) for column in dirty_frame.columns]
    fds = discover_oracle_fds(dataset.clean_df, columns=tuple(columns))
    schema = _schema_for(dirty_frame, fds)

    # Through Table, not the pandas frame the loader hands back: this is the shipped path.
    table = Table(
        columns,
        (
            {column: str(dirty_frame.iat[row, index]) for index, column in enumerate(columns)}
            for row in range(len(dirty_frame))
        ),
    )

    repairer = FDViolationRepairer(cache_dir=None, allow_llm=False)
    issues = FDViolationDetector().detect(table, schema)
    seen: set[tuple[int, str]] = set()
    proposals: list[Any] = []
    for issue in issues:
        key = (issue.row, issue.column)
        if key in seen:
            continue
        seen.add(key)
        proposal = repairer.propose(issue, table, schema)
        if proposal is not None:
            proposals.append(proposal)
        if len(proposals) >= FIXES_MEASURED:
            break
    if len(proposals) < FIXES_MEASURED:
        raise RuntimeError(
            f"only {len(proposals)} proposals available; the measurement needs {FIXES_MEASURED} "
            "and a short run would silently change the per-fix denominators"
        )
    return table, schema, proposals


def measure() -> dict[str, Any]:
    """Run the verifier over a fixed set of fixes and return counted work."""
    import dataforge.verifier.smt as smt_module

    table, schema, proposals = _load_case()
    counts: Counter[str] = Counter()

    real_solver = getattr(smt_module, "Solver")  # noqa: B009

    class _CountingSolver:
        """Delegates to the real solver while recording what passes through it."""

        def __init__(self) -> None:
            self._inner = real_solver()

        def set(self, **kwargs: Any) -> None:
            self._inner.set(**kwargs)

        def add(self, *args: Any) -> None:
            counts["ground_assertions"] += len(args)
            self._inner.add(*args)

        def assert_and_track(self, formula: Any, label: Any) -> None:
            counts["tracked_assertions"] += 1
            self._inner.assert_and_track(formula, label)

        def check(self) -> Any:
            return self._inner.check()

        def unsat_core(self) -> Any:
            return self._inner.unsat_core()

        def reason_unknown(self) -> str:
            return str(self._inner.reason_unknown())

    def _counted(name: str, original: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            counts[name] += 1
            return original(*args, **kwargs)

        return wrapper

    originals = {name: getattr(smt_module, name) for name in ("StringVal", "IntVal", "RealVal")}
    try:
        # setattr rather than attribute assignment: `Solver` is re-exported from z3 into this
        # module's namespace, so mypy --strict rejects assigning to it directly.
        setattr(smt_module, "Solver", _CountingSolver)  # noqa: B010
        for name, original in originals.items():
            setattr(smt_module, name, _counted(name, original))
        verifier = smt_module.SMTVerifier()
        for proposal in proposals:
            verifier.verify(table, [proposal], schema)
    finally:
        setattr(smt_module, "Solver", real_solver)  # noqa: B010
        for name, original in originals.items():
            setattr(smt_module, name, original)

    value_constructions = counts["StringVal"] + counts["IntVal"] + counts["RealVal"]
    per_fix = {
        "z3_value_constructions_per_fix": round(value_constructions / FIXES_MEASURED),
        "ground_assertions_per_fix": round(counts["ground_assertions"] / FIXES_MEASURED),
        "tracked_assertions_per_fix": round(counts["tracked_assertions"] / FIXES_MEASURED),
    }
    return {
        "schema": 1,
        "measured_utc": datetime.now(UTC).isoformat(),
        "corpus": "hospital",
        "premise": "oracle",
        "table_type": "dataforge.table.Table",
        "fixes_measured": FIXES_MEASURED,
        "note": (
            "Counted work, not wall clock. These figures are identical on every machine, which is "
            "why they can gate. They are a PROXY for cost: a change that reduced assertions while "
            "slowing the solver would pass here and must be caught by tests/benchmarks/."
        ),
        "totals": {
            "StringVal": counts["StringVal"],
            "IntVal": counts["IntVal"],
            "RealVal": counts["RealVal"],
            "ground_assertions": counts["ground_assertions"],
            "tracked_assertions": counts["tracked_assertions"],
        },
        "per_fix": per_fix,
        "budgets": dict(BUDGETS),
    }


def _check(payload: dict[str, Any]) -> list[str]:
    """Return budget violations, if any."""
    per_fix: dict[str, int] = payload["per_fix"]
    failures: list[str] = []
    for key, ceiling in BUDGETS.items():
        observed = per_fix[key]
        if observed > ceiling:
            failures.append(
                f"{key}: {observed:,} exceeds the budget of {ceiling:,}. Counted work grew, which "
                "means the encoding got structurally bigger. If that is intended, raise the budget "
                "in this file and say why in the commit message."
            )
    return failures


def main() -> int:
    """Measure counted verification work, optionally gating on the budgets."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, default=None, help="Where to write the JSON.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if counted work exceeds the committed budgets.",
    )
    args = parser.parse_args()

    payload = measure()

    if args.artifact is not None:
        args.artifact.parent.mkdir(parents=True, exist_ok=True)
        args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(f"verifier counted work, {payload['corpus']}/{payload['premise']}, per fix:")
    for key, value in payload["per_fix"].items():
        print(f"  {key:36} {value:8,}   budget {BUDGETS[key]:,}")

    if not args.check:
        return 0

    failures = _check(payload)
    if failures:
        print("\nCounted-work budget FAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nCounted-work budgets held.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
