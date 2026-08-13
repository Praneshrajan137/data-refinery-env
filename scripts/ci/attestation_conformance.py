"""Cross-implementation conformance gate for the repair attestation.

Runs BOTH implementations over the committed vectors and fails if either disagrees with
the recorded verdicts. This is the gate that makes ``dataforge.repair.attestation/v1`` a
specification: a format with one implementation is a program, and its behaviour is
whatever that program happens to do.

Why run both rather than trust the two test suites separately: nothing otherwise stops one
suite from being pointed at a different vector file, skipped, or deleted, and the failure
would look like a passing build. Here the two are named together in one gate, and a
missing runner is a failure rather than a silence.

Usage:
    python scripts/ci/attestation_conformance.py --check
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WEB = PROJECT_ROOT / "playground" / "web"
VECTORS = PROJECT_ROOT / "tests" / "fixtures" / "attestation" / "vectors.json"

MINIMUM_VECTORS = 15


def _run(command: list[str], cwd: Path) -> tuple[int, str]:
    completed = subprocess.run(  # noqa: S603
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout + completed.stderr


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", required=True)
    parser.parse_args()

    if not VECTORS.exists():
        print(f"MISSING: {VECTORS.relative_to(PROJECT_ROOT)}")
        return 1

    payload = json.loads(VECTORS.read_text(encoding="utf-8"))
    vectors = payload.get("vectors", {})
    if len(vectors) < MINIMUM_VECTORS:
        print(
            f"VACUOUS: only {len(vectors)} vectors, expected at least {MINIMUM_VECTORS}. "
            "A conformance suite that shrinks silently stops being evidence."
        )
        return 1

    rejecting = [name for name, case in vectors.items() if not case["expect_ok"]]
    if len(rejecting) < len(vectors) // 3:
        print("VACUOUS: too few rejection vectors; a happy-path-only suite proves little.")
        return 1

    print(f"vectors: {len(vectors)} total, {len(rejecting)} rejection cases")

    failures: list[str] = []

    print("\n==> Python implementation")
    code, output = _run(
        [sys.executable, "-m", "pytest", "tests/unit/test_attestation_vectors.py", "-q"],
        PROJECT_ROOT,
    )
    print(output.strip()[-600:] if code else "PASS")
    if code != 0:
        failures.append("python")

    print("\n==> TypeScript implementation")
    npx = shutil.which("npx") or shutil.which("npx.cmd")
    if npx is None:
        print(
            "FAIL: npx not found, so the second implementation could not be verified. "
            "A single-implementation format is not a specification, so this is a failure "
            "rather than a skip."
        )
        failures.append("typescript-unavailable")
    else:
        code, output = _run([npx, "vitest", "run", "src/attestation", "--silent"], WEB)
        print(output.strip()[-600:] if code else "PASS")
        if code != 0:
            failures.append("typescript")

    if failures:
        print(f"\nCONFORMANCE FAILED: {failures}")
        return 1

    print("\nConformance verified: both implementations agree on every vector.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
