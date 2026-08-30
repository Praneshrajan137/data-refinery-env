"""Keep the mapped fast path fast, without letting a careless mapping make it wrong.

``scripts/test_mapped.py`` runs only the tests a source file is mapped to. Two facts about it
shape this gate:

* An **unmapped** file falls back to the full suite (``test_mapped.py:94-95``). So the map cannot
  go blind -- it fails safe. The cost of a gap is speed, not correctness: editing an unmapped
  module means paying the whole suite instead of a few files.
* A **wrong** mapping is a different matter. It silently runs the wrong tests and reports green.
  So a mapping is a safety-relevant assertion, and bulk-adding 76 of them to close the gap
  quickly would create 76 chances to be wrong. That trade is not worth making.

Hence a ratchet rather than a mandate. Every module under ``dataforge/`` must be either mapped
or **explicitly declared** as falling back to the full suite, in ``_unmapped_full_suite``. A new
module therefore forces a decision, and neither outcome can be reached by accident. The gap is
recorded rather than hidden, and it cannot grow silently.

The gate also refuses two shapes that would make the declaration meaningless: a module that is
both mapped and declared unmapped (a contradiction the reader cannot resolve), and a declared
path that no longer exists (a stale entry that keeps a closed gap looking open).

Usage::

    python scripts/ci/test_map_coverage.py --check
    python scripts/ci/test_map_coverage.py --list-unmapped
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Final

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_MAP = PROJECT_ROOT / "test_map.json"
PACKAGE = PROJECT_ROOT / "dataforge"
TEST_MAPPED = PROJECT_ROOT / "scripts" / "test_mapped.py"


def _known_category_keys() -> frozenset[str]:
    """Return the category keys ``scripts/test_mapped.py`` actually collects.

    Derived by parsing that module's ``_ALWAYS_COLLECT`` and ``_BENCH_COLLECT`` tuples rather than
    restating them here. A restated list would drift from its consumer, and the drift would be
    invisible: a key this gate accepted but ``test_mapped.py`` did not collect would read as a
    valid mapping while contributing no tests to the fast path.

    AST rather than import, so that reading the constants cannot execute the module and does not
    depend on where ``sys.path`` points when this script is run directly.
    """
    import ast

    module = ast.parse(TEST_MAPPED.read_text(encoding="utf-8"))
    keys: set[str] = set()
    for node in module.body:
        if not isinstance(node, ast.AnnAssign | ast.Assign):
            continue
        targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
        names = {target.id for target in targets if isinstance(target, ast.Name)}
        if not names & {"_ALWAYS_COLLECT", "_BENCH_COLLECT"}:
            continue
        if isinstance(node.value, ast.Tuple):
            keys.update(
                element.value
                for element in node.value.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            )
    if not keys:
        raise ValueError(
            f"Could not read the collected category keys from {TEST_MAPPED.name}. This gate "
            f"derives them rather than restating them, so it fails closed instead of validating "
            f"against an empty set."
        )
    return frozenset(keys)


#: Category keys a mapped entry may use, derived from the consumer at import time.
KNOWN_CATEGORY_KEYS: Final[frozenset[str]] = _known_category_keys()

#: Key holding the explicit full-suite fallback declarations. Underscore-prefixed because
#: ``test_mapped.py`` already skips such keys as metadata.
UNMAPPED_KEY: Final[str] = "_unmapped_full_suite"


def _module_paths() -> set[str]:
    """Return every importable module under ``dataforge/``, excluding package initialisers.

    ``__init__.py`` files are excluded because they are re-export surfaces rather than logic;
    ``dataforge/__init__.py`` is a lazy ``__getattr__`` table with nothing to test directly.
    """
    return {
        str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        for path in PACKAGE.rglob("*.py")
        if path.name != "__init__.py"
    }


def _load() -> dict[str, Any]:
    """Read the test map."""
    payload = json.loads(TEST_MAP.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("test_map.json must contain a JSON object at the top level.")
    return payload


def entry_shape_errors(payload: dict[str, Any]) -> list[str]:
    """Return schema defects in the mapped entries.

    Added 2026-08-30 because the schema was policed in exactly one place -- an inline heredoc in
    ``.github/workflows/ci.yml`` -- while this script, which runs in ``make lint`` and in the
    backend gate, checked only that a *decision* existed. Two validators with different schemas
    over one file is drift by construction, and it produced a four-commit red ``main``: four
    entries had been added as bare lists (``"module": [...]`` instead of
    ``"module": {"direct_tests": [...]}``), which every local gate accepted and CI rejected.

    The shape is not cosmetic. ``scripts/test_mapped.py`` rejects a non-object entry outright and
    collects paths only from its known category keys, so a bare list did not merely offend a
    validator -- it broke the fast path for those four modules, which is the thing this map exists
    to provide.

    Unknown category keys are reported too: ``test_mapped.py`` silently ignores them, so a typo
    like ``direct_test`` would drop tests from the fast path while looking mapped.
    """
    problems: list[str] = []
    for source_file, entry in sorted(payload.items()):
        if source_file.startswith("_"):
            continue
        if not isinstance(entry, dict):
            problems.append(
                f"{source_file}: expected an object of category -> list[str], got "
                f"{type(entry).__name__}. scripts/test_mapped.py rejects this shape, so the "
                f"mapped fast path for this file does not work."
            )
            continue
        for key, value in sorted(entry.items()):
            if key not in KNOWN_CATEGORY_KEYS:
                problems.append(
                    f"{source_file}.{key}: unknown category. scripts/test_mapped.py collects "
                    f"only {sorted(KNOWN_CATEGORY_KEYS)}, so these tests would be silently "
                    f"dropped from the fast path."
                )
                continue
            if not isinstance(value, list) or not all(isinstance(path, str) for path in value):
                problems.append(f"{source_file}.{key}: expected list[str].")
                continue
            for test_path in value:
                if not (PROJECT_ROOT / test_path).exists():
                    problems.append(f"{source_file}.{key}: missing path {test_path}")
    return problems


def errors() -> list[str]:
    """Return every coverage defect, or an empty list."""
    payload = _load()
    mapped = {key for key in payload if not key.startswith("_")}
    declared_raw = payload.get(UNMAPPED_KEY, [])
    if not isinstance(declared_raw, list) or not all(isinstance(x, str) for x in declared_raw):
        return [f"{UNMAPPED_KEY} must be a list of repo-relative path strings."]
    declared = {path.replace("\\", "/") for path in declared_raw}
    modules = _module_paths()

    problems: list[str] = entry_shape_errors(payload)

    undecided = sorted(modules - mapped - declared)
    if undecided:
        problems.append(
            f"{len(undecided)} module(s) under dataforge/ are neither mapped nor declared in "
            f"'{UNMAPPED_KEY}'. Editing one costs the whole suite instead of a few files. Add a "
            f"mapping, or declare the fallback deliberately: {undecided}"
        )

    contradictory = sorted(mapped & declared)
    if contradictory:
        problems.append(
            f"{len(contradictory)} module(s) are BOTH mapped and declared unmapped. A reader "
            f"cannot tell which is intended: {contradictory}"
        )

    stale = sorted(path for path in declared if not (PROJECT_ROOT / path).exists())
    if stale:
        problems.append(
            f"{len(stale)} declared path(s) do not exist. A stale entry keeps a closed gap "
            f"looking open: {stale}"
        )

    return problems


def check() -> int:
    """Verify coverage; return a process exit code."""
    problems = errors()
    if problems:
        print("test_map coverage check FAILED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    payload = _load()
    mapped = {key for key in payload if not key.startswith("_")}
    modules = _module_paths()
    declared = {str(path).replace("\\", "/") for path in payload.get(UNMAPPED_KEY, [])}
    covered = len(modules & mapped)
    print(
        f"test_map coverage: every one of {len(modules)} dataforge module(s) has a decision "
        f"({covered} mapped to specific tests, {len(modules & declared)} declared as "
        f"full-suite fallback). {len(mapped - modules)} further entries map non-package files."
    )
    return 0


def list_unmapped() -> int:
    """Print the modules with no specific mapping, as a JSON array ready to paste."""
    payload = _load()
    mapped = {key for key in payload if not key.startswith("_")}
    print(json.dumps(sorted(_module_paths() - mapped), indent=4))
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Fail if a module has no decision.")
    mode.add_argument(
        "--list-unmapped",
        action="store_true",
        help="Print modules lacking a specific mapping.",
    )
    args = parser.parse_args(argv)
    return list_unmapped() if args.list_unmapped else check()


if __name__ == "__main__":
    raise SystemExit(main())
