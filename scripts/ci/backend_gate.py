"""Canonical backend release-quality gate for DataForge."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON = sys.executable

PYTHON_PATHS = [
    "dataforge",
    "tests",
    "scripts/ci",
    "scripts/playground",
    "scripts/data",
    "scripts/model",
    "scripts/publish_model.py",
    "playground/api/app.py",
]
MYPY_PATHS = [
    "dataforge",
    "playground/api/app.py",
    "scripts/ci/readme_truth.py",
    "scripts/ci/benchmark_truth.py",
    "scripts/ci/full_vision_external_gate.py",
    "scripts/ci/openapi_contract.py",
    "scripts/ci/backend_gate.py",
    "scripts/playground/build_samples.py",
    "scripts/playground/stage_space.py",
    "scripts/playground/verify_space_backend.py",
    "scripts/playground/monitor_playground.py",
    "scripts/data/collect_sft_trajectories.py",
    "scripts/data/validate_sft_readiness.py",
    "scripts/model/verify_sft_release.py",
    "scripts/model/publish_dataset_readme.py",
    "scripts/publish_model.py",
]
EXCLUDED_SECRET_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "benchmark_results",
    "build",
    "datasets",
    "dist",
    "htmlcov",
    "node_modules",
}
SECRET_PATTERNS = [
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bghp_[A-Za-z0-9_]{36,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9]{32,}\b"),
]


def _clean_package_artifacts() -> None:
    """Remove generated package metadata before release builds."""

    def _make_writable_and_retry(
        function: Callable[[str], Any],
        path: str,
        _exc_info: object,
    ) -> None:
        target = Path(path)
        target.chmod(target.stat().st_mode | stat.S_IWRITE)
        function(path)

    for path in [
        PROJECT_ROOT / "build",
        PROJECT_ROOT / "dist",
        PROJECT_ROOT / "dataforge.egg-info",
        PROJECT_ROOT / "dataforge15.egg-info",
        PROJECT_ROOT / "dataforge-mcp" / "build",
        PROJECT_ROOT / "dataforge-mcp" / "dist",
        PROJECT_ROOT / "dataforge-mcp" / "dataforge_mcp.egg-info",
        PROJECT_ROOT / "dataforge-mcp" / "dataforge15_mcp.egg-info",
    ]:
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path, onerror=_make_writable_and_retry)
            else:
                path.chmod(path.stat().st_mode | stat.S_IWRITE)
                path.unlink()


def _run(
    label: str,
    command: list[str],
    *,
    optional: bool = False,
    timeout_seconds: int | None = None,
) -> bool:
    """Run a gate command and return whether it passed."""
    print(f"\n==> {label}")
    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        if optional:
            print(f"SKIP {label}: {exc}")
            return True
        print(f"FAIL {label}: {exc}")
        return False
    except subprocess.TimeoutExpired:
        if optional:
            print(f"SKIP {label}: timed out after {timeout_seconds}s")
            return True
        print(f"FAIL {label}: timed out after {timeout_seconds}s")
        return False
    if result.returncode == 0:
        print(f"PASS {label}")
        return True
    if optional:
        print(f"SKIP {label}: command exited {result.returncode}")
        return True
    print(f"FAIL {label}: command exited {result.returncode}")
    return False


def _secret_scan() -> bool:
    """Scan first-party files for high-confidence secret material."""
    print("\n==> secret scan")
    findings: list[str] = []
    for path in PROJECT_ROOT.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(PROJECT_ROOT)
        if any(
            part in EXCLUDED_SECRET_DIRS or part.startswith(".hf-space") for part in relative.parts
        ):
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".parquet", ".bin"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                findings.append(str(relative))
                break
    if findings:
        for finding in findings:
            print(f"SECRET? {finding}")
        return False
    print("PASS secret scan")
    return True


def main() -> int:
    """Run the backend gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-full-tests", action="store_true", help="Skip full pytest suite.")
    parser.add_argument(
        "--require-optional",
        action="store_true",
        help="Fail when optional supply-chain tools are unavailable.",
    )
    parser.add_argument(
        "--dependency-audit-timeout",
        type=int,
        default=120,
        help="Seconds before optional dependency audit is skipped or required audit fails.",
    )
    args = parser.parse_args()

    checks: list[bool] = [
        _run("ruff check", [PYTHON, "-m", "ruff", "check", *PYTHON_PATHS]),
        _run("ruff format --check", [PYTHON, "-m", "ruff", "format", "--check", *PYTHON_PATHS]),
        _run("strict mypy", [PYTHON, "-m", "mypy", "--strict", *MYPY_PATHS]),
    ]
    if not args.skip_full_tests:
        checks.append(_run("root pytest", [PYTHON, "-m", "pytest", "tests/", "-x", "-v"]))
    checks.extend(
        [
            _run("MCP pytest", [PYTHON, "-m", "pytest", "dataforge-mcp/tests", "-v"]),
            _run("README truth", [PYTHON, "scripts/ci/readme_truth.py"]),
            _run("benchmark truth", [PYTHON, "scripts/ci/benchmark_truth.py", "--check"]),
            _run("OpenAPI drift", [PYTHON, "scripts/ci/openapi_contract.py", "--check"]),
            _secret_scan(),
        ]
    )

    pip_audit_optional = not (
        args.require_optional or os.environ.get("DATAFORGE_REQUIRE_PIP_AUDIT")
    )
    checks.append(
        _run(
            "pip-audit",
            [PYTHON, "-m", "pip_audit", "--progress-spinner", "off"],
            optional=pip_audit_optional,
            timeout_seconds=args.dependency_audit_timeout,
        )
    )

    _clean_package_artifacts()
    (PROJECT_ROOT / "dist").mkdir(exist_ok=True)

    sbom_optional = not (args.require_optional or os.environ.get("DATAFORGE_REQUIRE_SBOM"))
    checks.append(
        _run(
            "CycloneDX SBOM",
            [PYTHON, "-m", "cyclonedx_py", "environment", "-o", "dist/cyclonedx-env.json"],
            optional=sbom_optional,
        )
    )

    build_optional = not (args.require_optional or os.environ.get("DATAFORGE_REQUIRE_BUILD"))
    checks.append(
        _run(
            "dataforge package build",
            [PYTHON, "-m", "build", "--sdist", "--wheel"],
            optional=build_optional,
        )
    )
    checks.append(
        _run(
            "dataforge-mcp package build",
            [PYTHON, "-m", "build", "--sdist", "--wheel", "dataforge-mcp"],
            optional=build_optional,
        )
    )
    checks.append(
        _run(
            "dataforge release gate",
            [PYTHON, "-m", "dataforge.release.gate"],
            timeout_seconds=360,
        )
    )

    if all(checks):
        print("\nBackend gate passed.")
        return 0
    print("\nBackend gate failed.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
