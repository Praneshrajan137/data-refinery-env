"""Freeze the *population* every gate polices, so a speedup cannot quietly shrink it.

This file exists because of a specific hazard in performance work. Reordering, parallelising
and deduplicating gate steps are all changes that make a gate finish sooner, and every one of
them can also make it check less -- with no visible symptom, because a gate that checks less
still exits 0. Faster and weaker look identical from the outside.

So the invariant this enforces is not "the gate passes". It is "the gate runs the same checks
over the same tests as it did before". Concretely, the manifest records:

* every pytest node id collected under ``tests/`` -- the set a deduplication step could shrink;
* every ``backend_gate`` step name -- the set a reordering could drop;
* every mutant id and the test paths it runs -- the set a parallelisation could skip;
* the trust-invariant test paths, the ``docs_truth`` claim ids, and the ``readme_truth``
  scanned documents -- three populations that already have a history of drifting.

**Everything is derived, never restated.** The mutant ids come from importing ``MUTANTS``; the
claim ids from ``docs_truth._load_claims()``; the node ids from pytest's own collector; the step
names from an AST parse of ``backend_gate.py``. A hand-maintained copy of any of these lists
would be a second source of truth that can agree with the manifest while both disagree with the
gate -- which is how a check ends up policing its own restatement instead of the real thing.

The step names need the AST route specifically because ``backend_gate.main`` evaluates its steps
*inside* a list literal (``checks: list[bool] = [_run(...), ...]``), so the names cannot be
enumerated without also executing every gate step. Parsing the source reads the declaration
without paying for the run.

Usage::

    python scripts/ci/gate_population.py --emit    # refresh the committed manifest
    python scripts/ci/gate_population.py --check   # fail if the population moved
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Final

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = PROJECT_ROOT / "eval" / "results" / "gate_population.json"

#: Parsed rather than imported, for the reason given in the module docstring.
BACKEND_GATE = PROJECT_ROOT / "scripts" / "ci" / "backend_gate.py"

#: Helpers whose first positional argument is a gate step's human-readable name. ``_run`` runs a
#: step sequentially; ``GateCommand`` declares one for a concurrent group. Both must be read, and
#: this is why: when steps were moved into concurrent groups on 2026-08-28 the ``_run``-only parse
#: reported 21 steps REMOVED. Nothing had stopped being checked -- only the constructor changed --
#: but the manifest is supposed to be unable to tell the difference between "moved" and "deleted",
#: so it correctly refused. Teaching it the second form is the fix; re-emitting over the alarm
#: would have been the mistake, and would have hidden a real deletion the next time.
STEP_CALLEES: Final[tuple[str, ...]] = ("_run", "GateCommand")


def _step_names(source: Path) -> list[str]:
    """Return every gate step label declared in ``source``.

    Reads the declaration instead of the execution. A step whose name is built dynamically would
    be invisible here, so a non-literal first argument raises rather than being silently omitted.
    """
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    names: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Name) or func.id not in STEP_CALLEES:
            continue
        if not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            names.append(first.value)
        else:
            raise ValueError(
                f"{source.name}:{node.lineno}: {func.id}() called with a non-literal step "
                "name. The population manifest derives step names statically, so a computed "
                "name would be omitted from the manifest while still running in the gate."
            )
    return sorted(names)


def _pytest_node_ids() -> list[str]:
    """Collect every test node id pytest sees under ``tests/``.

    Uses pytest's own collector, so the manifest cannot disagree with the runner about what
    exists. ``-p no:cacheprovider`` keeps collection from racing a concurrent run's cache.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/",
            "--collect-only",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "pytest collection failed, so the population is unknown. Refusing to emit a "
            f"manifest from a broken collection:\n{result.stdout[-4000:]}\n{result.stderr[-4000:]}"
        )
    ids = [
        line.strip().replace("\\", "/")
        for line in result.stdout.splitlines()
        if "::" in line and not line.startswith(" ")
    ]
    if not ids:
        raise RuntimeError(
            "pytest reported no node ids. An empty population would make this gate vacuous."
        )
    return sorted(ids)


def _mutant_population() -> dict[str, list[str]]:
    """Return ``{mutant id: sorted test paths}`` by importing the harness's own table."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.ci import mutate_autoapply_guards

    return {
        mutant.name: sorted(path.replace("\\", "/") for path in mutant.tests)
        for mutant in mutate_autoapply_guards.MUTANTS
    }


def _claim_ids() -> list[str]:
    """Return every ``docs_truth`` claim id, from the ledger loader itself."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.ci import docs_truth

    return sorted(str(claim["id"]) for claim in docs_truth._load_claims())


def _readme_truth_docs() -> list[str]:
    """Return the documents ``readme_truth`` polices for public claim kinds."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.ci import readme_truth

    return sorted(
        str(Path(doc).relative_to(PROJECT_ROOT)).replace("\\", "/")
        if Path(doc).is_absolute()
        else str(doc).replace("\\", "/")
        for doc in readme_truth.PUBLIC_CLAIM_TRUTH_DOCS
    )


def _trust_invariant_tests() -> list[str]:
    """Return the trust-invariant test paths the backend gate runs standalone."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.ci import backend_gate

    return sorted(path.replace("\\", "/") for path in backend_gate.TRUST_INVARIANT_TESTS)


def build() -> dict[str, Any]:
    """Derive the full population manifest."""
    node_ids = _pytest_node_ids()
    mutants = _mutant_population()
    payload: dict[str, Any] = {
        "schema": 1,
        "note": (
            "Derived, never hand-written. Refresh with "
            "'python scripts/ci/gate_population.py --emit' and explain any change in the "
            "commit message: a shrinking population is a weaker gate, not a faster one."
        ),
        "pytest": {
            "node_id_count": len(node_ids),
            "node_ids_sha256": hashlib.sha256("\n".join(node_ids).encode("utf-8")).hexdigest(),
            "node_ids": node_ids,
        },
        "backend_gate_steps": _step_names(BACKEND_GATE),
        "trust_invariant_tests": _trust_invariant_tests(),
        "mutants": mutants,
        "mutant_count": len(mutants),
        "mutant_test_paths": sorted({path for paths in mutants.values() for path in paths}),
        "docs_truth_claim_ids": _claim_ids(),
        "readme_truth_docs": _readme_truth_docs(),
    }
    return payload


def _diff(committed: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Report every population that moved, naming direction and members."""
    errors: list[str] = []

    old_ids = set(committed.get("pytest", {}).get("node_ids", []))
    new_ids = set(current["pytest"]["node_ids"])
    removed = sorted(old_ids - new_ids)
    added = sorted(new_ids - old_ids)
    if removed:
        errors.append(
            f"{len(removed)} pytest node id(s) NO LONGER COLLECTED. A gate that runs fewer "
            f"tests is weaker, not faster. First 10: {removed[:10]}"
        )
    if added:
        errors.append(f"{len(added)} pytest node id(s) added. First 10: {added[:10]}")

    for key in (
        "backend_gate_steps",
        "trust_invariant_tests",
        "mutant_test_paths",
        "docs_truth_claim_ids",
        "readme_truth_docs",
    ):
        old = set(committed.get(key, []))
        new = set(current[key])
        gone = sorted(old - new)
        fresh = sorted(new - old)
        if gone:
            errors.append(f"{key}: REMOVED {gone}")
        if fresh:
            errors.append(f"{key}: added {fresh}")

    old_mutants = set(committed.get("mutants", {}))
    new_mutants = set(current["mutants"])
    if old_mutants - new_mutants:
        errors.append(f"mutants: REMOVED {sorted(old_mutants - new_mutants)}")
    if new_mutants - old_mutants:
        errors.append(f"mutants: added {sorted(new_mutants - old_mutants)}")
    for name in sorted(old_mutants & new_mutants):
        if committed["mutants"][name] != current["mutants"][name]:
            errors.append(
                f"mutant {name}: test paths changed from {committed['mutants'][name]} "
                f"to {current['mutants'][name]}"
            )
    return errors


def check() -> int:
    """Compare the live population against the committed manifest."""
    if not MANIFEST.exists():
        print(
            f"{MANIFEST.relative_to(PROJECT_ROOT)} does not exist. Run --emit first.",
            file=sys.stderr,
        )
        return 1
    committed = json.loads(MANIFEST.read_text(encoding="utf-8"))
    current = build()
    errors = _diff(committed, current)
    if errors:
        print("Gate population check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "\nIf the change is intended, run 'python scripts/ci/gate_population.py --emit' "
            "and say in the commit message why the population moved.",
            file=sys.stderr,
        )
        return 1
    print(
        f"Gate population unchanged: {current['pytest']['node_id_count']} pytest node ids, "
        f"{len(current['backend_gate_steps'])} backend gate steps, "
        f"{current['mutant_count']} mutants over {len(current['mutant_test_paths'])} test paths, "
        f"{len(current['docs_truth_claim_ids'])} docs_truth claims."
    )
    return 0


def emit() -> int:
    """Write the manifest from the live population."""
    payload = build()
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(
        f"Wrote {MANIFEST.relative_to(PROJECT_ROOT)}: "
        f"{payload['pytest']['node_id_count']} pytest node ids, "
        f"{len(payload['backend_gate_steps'])} backend gate steps, "
        f"{payload['mutant_count']} mutants, "
        f"{len(payload['docs_truth_claim_ids'])} docs_truth claims."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--emit", action="store_true", help="Refresh the committed manifest.")
    mode.add_argument("--check", action="store_true", help="Fail if the population moved.")
    args = parser.parse_args(argv)
    return emit() if args.emit else check()


if __name__ == "__main__":
    raise SystemExit(main())
