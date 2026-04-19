"""Run only the tests mapped to a given source file in ``test_map.json``.

Usage:
    python scripts/test_mapped.py <source_file>
    python scripts/test_mapped.py <source_file> --bench
    python scripts/test_mapped.py <source_file> --validate
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_ALWAYS_COLLECT: tuple[str, ...] = (
    "direct_tests",
    "integration_tests",
    "property_tests",
    "regression_tests",
    "adversarial_tests",
)
_BENCH_COLLECT: tuple[str, ...] = ("benchmark",)
_ALL_KNOWN_KEYS: frozenset[str] = frozenset(_ALWAYS_COLLECT + _BENCH_COLLECT)


def _validate_paths(tests: list[str]) -> list[str]:
    """Warn about mapped test files that do not exist on disk."""
    missing: list[str] = []
    for test_path in tests:
        if not Path(test_path).exists():
            missing.append(test_path)
            print(f"  WARN: mapped test path does not exist: {test_path}", file=sys.stderr)
    return missing


def _load_mapping(map_path: Path) -> dict[str, object]:
    """Load the test mapping file from disk."""
    return json.loads(map_path.read_text(encoding="utf-8"))


def main(changed_file: str, *, include_bench: bool = False, validate_only: bool = False) -> int:
    """Run the tests mapped to a changed source file.

    Args:
        changed_file: Repository-relative path to the changed source file.
        include_bench: Whether benchmark mappings should be included.
        validate_only: Whether to validate mappings without running tests.

    Returns:
        Process exit code.
    """
    map_path = Path("test_map.json")
    if not map_path.exists():
        print("ERROR: test_map.json not found. Create it per Section 4.3.", file=sys.stderr)
        return 2

    raw_mapping = _load_mapping(map_path)
    if not isinstance(raw_mapping, dict):
        print("ERROR: test_map.json must contain a JSON object at the top level.", file=sys.stderr)
        return 2

    mapping: dict[str, object] = raw_mapping

    for src_file, entry in mapping.items():
        if src_file.startswith("_"):
            continue
        if not isinstance(entry, dict):
            print(
                f"  ERROR: test_map.json['{src_file}'] must be an object, "
                f"got {type(entry).__name__}.",
                file=sys.stderr,
            )
            return 2
        for key, value in entry.items():
            if key not in _ALL_KNOWN_KEYS:
                print(
                    f"  WARN: unknown key '{key}' in test_map.json['{src_file}']. "
                    f"Known keys: {sorted(_ALL_KNOWN_KEYS)}",
                    file=sys.stderr,
                )
            if not isinstance(value, list):
                print(
                    f"  ERROR: test_map.json['{src_file}']['{key}'] must be list[str], "
                    f"got {type(value).__name__}. Fix the schema.",
                    file=sys.stderr,
                )
                return 2

    entry = mapping.get(changed_file)
    if entry is None:
        print(f"WARN: no mapping for {changed_file} - running full suite.")
        return subprocess.call(["pytest", "tests/", "-x"])

    if not isinstance(entry, dict):
        print(f"ERROR: mapping for {changed_file} must be an object.", file=sys.stderr)
        return 2

    collect_keys = list(_ALWAYS_COLLECT)
    if include_bench:
        collect_keys.extend(_BENCH_COLLECT)

    tests: list[str] = []
    for key in collect_keys:
        value = entry.get(key, [])
        if isinstance(value, list):
            tests.extend(value)

    if not tests:
        print(f"WARN: mapping for {changed_file} has no test files - running full suite.")
        return subprocess.call(["pytest", "tests/", "-x"])

    missing = _validate_paths(tests)
    if validate_only:
        if missing:
            print(f"FAIL: {len(missing)} mapped test path(s) do not exist.", file=sys.stderr)
            return 1
        print(f"OK: all {len(tests)} mapped test path(s) exist.")
        return 0

    existing_tests = [test_path for test_path in tests if Path(test_path).exists()]
    if not existing_tests:
        print(
            f"ERROR: all {len(tests)} mapped test path(s) for {changed_file} are missing.",
            file=sys.stderr,
        )
        return 2

    print(f"Running {len(existing_tests)} mapped test file(s) for {changed_file}:")
    for test_path in existing_tests:
        print(f"  - {test_path}")
    return subprocess.call(["pytest", *existing_tests, "-x", "-v"])


if __name__ == "__main__":
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]
    flags = {arg for arg in sys.argv[1:] if arg.startswith("-")}

    if "--help" in flags or "-h" in flags:
        print(__doc__)
        sys.exit(0)

    if len(args) != 1:
        print(
            "Usage: python scripts/test_mapped.py <source_file> [--bench] [--validate]",
            file=sys.stderr,
        )
        sys.exit(2)

    sys.exit(
        main(
            args[0],
            include_bench="--bench" in flags,
            validate_only="--validate" in flags,
        )
    )
