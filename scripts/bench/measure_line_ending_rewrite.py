"""Measure what a one-cell repair does to the bytes it was not asked to change.

Why this script exists
----------------------
While replacing the concurrency claim in ``tests/integration/test_surface_uniformity.py`` -- which
asserted in prose that "the lock is what makes concurrent writes to one source safe" -- the first
race test failed. It failed on line endings, not on the race.

Applying a **one-cell** repair to a CRLF-delimited CSV rewrites **every line ending in the file**,
because the apply path re-serialises the table rather than patching bytes in place, and the
serialiser emits ``\\n`` regardless of the input dialect. Nothing records the input dialect, so
nothing can restore it.

This is not a data-safety defect: revert restores byte identity from the pre-apply snapshot and the
journal audits ``verified``. Both are asserted in
``tests/integration/test_concurrent_apply.py``. What it costs is the reviewability of an applied
diff, which is the product -- see ``docs/trust/apply-rewrites-line-endings.md``.

What is measured
----------------
The real CLI, in a subprocess, on the fixture whose repair is PROVABLE (``state -> city`` is a
declared functional dependency, so ``fd_violation`` writes exactly one cell). For both input
dialects: terminator counts and byte length before and after, the number of cells that changed, and
whether revert restores the original bytes exactly.

Both dialects are measured because the LF case is the control. Reporting only the CRLF case would
leave open whether apply rewrites lines unconditionally or only when it changes the dialect, and
those are different defects with different fixes.

Usage:
    python scripts/bench/measure_line_ending_rewrite.py --artifact eval/results/line_ending_rewrite.json
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

FIXTURES = PROJECT_ROOT / "dataforge" / "fixtures"
SOURCE_NAME = "premised_fd_10rows.csv"
SCHEMA_NAME = "premised_fd_10rows.schema.yaml"


def _cells(raw: bytes) -> list[list[str]]:
    """Parse to cells so a content comparison is independent of the terminator."""
    text = raw.decode("utf-8")
    return [row for row in csv.reader(text.splitlines()) if row]


def _changed_cell_count(before: bytes, after: bytes) -> int:
    """Count cells whose value differs, which is the quantity a repair is supposed to move."""
    rows_before, rows_after = _cells(before), _cells(after)
    if len(rows_before) != len(rows_after):
        return -1  # shape changed; the comparison is not meaningful and must not read as 0
    return sum(
        1
        for row_before, row_after in zip(rows_before, rows_after, strict=True)
        for cell_before, cell_after in zip(row_before, row_after, strict=True)
        if cell_before != cell_after
    )


def _run(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, cwd=str(cwd), timeout=120)


def measure_dialect(terminator: bytes, *, workspace: Path) -> dict[str, Any]:
    """Apply one repair to a source written with ``terminator`` and report what moved."""
    shutil.copyfile(FIXTURES / SOURCE_NAME, workspace / SOURCE_NAME)
    shutil.copyfile(FIXTURES / SCHEMA_NAME, workspace / SCHEMA_NAME)
    source = workspace / SOURCE_NAME
    # Normalise to LF first, then to the requested terminator, so the checkout's own line endings
    # (CRLF on Windows, LF on Linux) cannot decide the result. A measurement that depends on the
    # developer's git config is not a measurement.
    source.write_bytes(source.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", terminator))

    before = source.read_bytes()
    applied = _run(
        [
            sys.executable,
            "-m",
            "dataforge",
            "repair",
            str(source),
            "--schema",
            str(workspace / SCHEMA_NAME),
            "--apply",
            "--json",
        ],
        workspace,
    )
    payload = json.loads(applied.stdout)
    receipt = payload["receipt"]
    if receipt.get("applied") is not True:
        raise RuntimeError(
            "the fixture did not apply, so this measurement is vacuous: "
            f"reason={receipt.get('reason')!r}"
        )
    after = source.read_bytes()

    reverted = _run(
        [
            sys.executable,
            "-m",
            "dataforge",
            "revert",
            str(receipt["txn_id"]),
            "--search-root",
            str(workspace),
            "--json",
        ],
        workspace,
    )
    restored = source.read_bytes()

    return {
        "input_terminator": terminator.decode("unicode_escape").encode("unicode_escape").decode(),
        "crlf_before": before.count(b"\r\n"),
        "crlf_after": after.count(b"\r\n"),
        "lf_before": before.count(b"\n") - before.count(b"\r\n"),
        "lf_after": after.count(b"\n") - after.count(b"\r\n"),
        "bytes_before": len(before),
        "bytes_after": len(after),
        "bytes_delta": len(before) - len(after),
        "rows": len(_cells(before)),
        "cells_changed": _changed_cell_count(before, after),
        "lines_reterminated": before.count(b"\r\n") - after.count(b"\r\n"),
        "fixes_applied": len(receipt.get("applied_fixes") or []),
        "revert_exit_code": reverted.returncode,
        "revert_restores_byte_identity": restored == before,
        "sha256_before": hashlib.sha256(before).hexdigest(),
        "sha256_after": hashlib.sha256(after).hexdigest(),
    }


def measure() -> dict[str, Any]:
    """Measure both dialects. LF is the control arm."""
    results: dict[str, Any] = {}
    for label, terminator in (("crlf", b"\r\n"), ("lf", b"\n")):
        with tempfile.TemporaryDirectory() as raw:
            results[label] = measure_dialect(terminator, workspace=Path(raw))

    crlf, lf = results["crlf"], results["lf"]
    results["summary"] = {
        # The headline: lines rewritten per cell repaired. 11 on an 11-row table, and it is the ROW
        # COUNT rather than a constant, so the collateral scales with the table while the repair
        # does not.
        "crlf_lines_reterminated_per_cell_repaired": (
            crlf["lines_reterminated"] // crlf["cells_changed"] if crlf["cells_changed"] > 0 else -1
        ),
        # The control. If this were also non-zero the defect would be "apply always rewrites
        # lines", which is a different and larger claim than the one being published.
        "lf_lines_reterminated": lf["lines_reterminated"],
        "both_dialects_revert_to_byte_identity": bool(
            crlf["revert_restores_byte_identity"] and lf["revert_restores_byte_identity"]
        ),
        "cells_changed_is_one_in_both_dialects": bool(
            crlf["cells_changed"] == 1 and lf["cells_changed"] == 1
        ),
    }
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True, help="where to write the JSON")
    args = parser.parse_args()

    results = measure()
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    crlf = results["crlf"]
    print(f"wrote {args.artifact}")
    print(
        f"CRLF: {crlf['lines_reterminated']} of {crlf['rows']} lines re-terminated to change "
        f"{crlf['cells_changed']} cell(s); {crlf['bytes_delta']} bytes smaller; "
        f"revert byte-identical: {crlf['revert_restores_byte_identity']}"
    )
    print(f"LF control: {results['lf']['lines_reterminated']} lines re-terminated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
