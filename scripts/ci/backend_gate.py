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
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_gate_python() -> str:
    override = os.environ.get("DATAFORGE_GATE_PYTHON")
    if override:
        return override
    venv_python = (
        PROJECT_ROOT / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


PYTHON = _resolve_gate_python()
NPM = "npm.cmd" if os.name == "nt" else "npm"

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
    "scripts/ci/installed_package_smoke.py",
    "scripts/ci/openapi_contract.py",
    "scripts/ci/pypi_publish_report.py",
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
PACKAGE_ROOTS = [
    PROJECT_ROOT / "dataforge-mcp",
    PROJECT_ROOT / "packages" / "dataforge-evals",
    PROJECT_ROOT / "packages" / "dataforge-dbt",
    PROJECT_ROOT / "packages" / "dataforge-agent-patterns",
]
SIDE_PACKAGE_PATHS = [
    "dataforge-mcp",
    "packages/dataforge-evals",
    "packages/dataforge-dbt",
    "packages/dataforge-agent-patterns",
]
SIDE_PACKAGE_MYPY_PATHS = [
    "dataforge-mcp/dataforge_mcp",
    "packages/dataforge-evals/dataforge_evals",
    "packages/dataforge-dbt/dataforge_dbt",
    "packages/dataforge-agent-patterns/src/dataforge_agent_patterns",
]
CRITICAL_COVERAGE_INCLUDE = ",".join(
    [
        "dataforge/engine/repair.py",
        "dataforge/transactions/*.py",
        "dataforge/stores/*.py",
        "dataforge/verifier/*.py",
        "dataforge/http/problem.py",
    ]
)
CRITICAL_COVERAGE_FAIL_UNDER = "88"

# Trust invariants that must ALWAYS run, even when --skip-full-tests drops the
# full coverage suite. These enforce the product's core guarantee and must never
# be silently skippable: the corruption oracle (no correct cell is ever changed;
# plausibility-only fixes never auto-apply without the explicit opt-in), the
# N-version differential-verifier equivalence suite, byte-for-byte revert, the
# fail-closed differential combiner, and the self-verifying trust certificate.
TRUST_INVARIANT_TESTS = [
    "tests/property/test_no_corruption_invariant.py",
    "tests/property/test_verifier_equivalence.py",
    "tests/property/test_revert_is_bytes_identical.py",
    "tests/unit/test_differential_verifier.py",
    "tests/unit/test_certificate.py",
]


@dataclass(frozen=True)
class PipAuditException:
    """One explicitly triaged pip-audit exception."""

    vuln_id: str
    package: str
    scope: str
    expires_on: date
    reason: str
    upstream_reference: str


PIP_AUDIT_EXCEPTIONS = [
    PipAuditException(
        vuln_id="CVE-2025-3000",
        package="torch",
        scope="optional train/HF-local evaluation extras only",
        expires_on=date(2026, 10, 14),
        reason=(
            "Re-triaged 2026-07-16: pip-audit still reports no fixed version; DataForge "
            "core, playground, and MCP runtime surfaces do not require Torch."
        ),
        upstream_reference="https://github.com/advisories/GHSA-rrmf-rvhw-rf47",
    ),
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


def _today_utc() -> date:
    """Return today's UTC date for deterministic release checks."""
    return datetime.now(UTC).date()


def pip_audit_exception_errors(
    exceptions: list[PipAuditException] = PIP_AUDIT_EXCEPTIONS,
    *,
    today: date | None = None,
) -> list[str]:
    """Return validation errors for pip-audit exceptions."""
    observed_today = today or _today_utc()
    errors: list[str] = []
    for exception in exceptions:
        if not exception.vuln_id:
            errors.append("pip-audit exception is missing vuln_id.")
        if not exception.package:
            errors.append(f"{exception.vuln_id} is missing package.")
        if not exception.scope:
            errors.append(f"{exception.vuln_id} is missing scope.")
        if not exception.reason:
            errors.append(f"{exception.vuln_id} is missing reason.")
        if not exception.upstream_reference.startswith("https://"):
            errors.append(f"{exception.vuln_id} must have an HTTPS upstream_reference.")
        if exception.expires_on < observed_today:
            errors.append(
                f"{exception.vuln_id} for {exception.package} expired on "
                f"{exception.expires_on.isoformat()}."
            )
    return errors


def pip_audit_ignore_args(
    exceptions: list[PipAuditException] = PIP_AUDIT_EXCEPTIONS,
) -> list[str]:
    """Return pip-audit CLI ignore arguments for validated exceptions."""
    args: list[str] = []
    for exception in exceptions:
        args.extend(["--ignore-vuln", exception.vuln_id])
    return args


def coverage_policy_errors(
    *,
    makefile_path: Path = PROJECT_ROOT / "Makefile",
    pyproject_path: Path = PROJECT_ROOT / "pyproject.toml",
) -> list[str]:
    """Reject duplicated or drifting coverage thresholds."""
    errors: list[str] = []
    makefile_text = makefile_path.read_text(encoding="utf-8")
    pyproject_text = pyproject_path.read_text(encoding="utf-8")
    if "--cov-fail-under" in makefile_text:
        errors.append("Makefile must not duplicate coverage fail-under policy.")
    if "fails at <90%" in makefile_text:
        errors.append("Makefile help still advertises the stale 90% coverage threshold.")
    if not re.search(r"(?m)^fail_under\s*=\s*82(?:\.0+)?\s*$", pyproject_text):
        errors.append("pyproject.toml must remain the single 82.0 coverage threshold source.")
    return errors


def _coverage_policy_check() -> bool:
    """Print and return the coverage policy drift result."""
    print("\n==> coverage policy")
    errors = coverage_policy_errors()
    if errors:
        for error in errors:
            print(f"FAIL coverage policy: {error}")
        return False
    print("PASS coverage policy")
    return True


def _pip_audit_exception_check() -> bool:
    """Print and return the pip-audit exception validation result."""
    print("\n==> pip-audit exception policy")
    errors = pip_audit_exception_errors()
    if errors:
        for error in errors:
            print(f"FAIL pip-audit exception policy: {error}")
        return False
    print("PASS pip-audit exception policy")
    return True


def _corrector_promotion_gate() -> bool:
    """Enforce that no LLM corrector class auto-applies without committed evidence."""
    print("\n==> corrector auto-apply promotion gate")
    from dataforge.release.corrector_gate import check_corrector_release_gate

    result = check_corrector_release_gate()
    if result.passed:
        print(f"PASS corrector promotion gate: {result.reason}")
        return True
    print(f"FAIL corrector promotion gate: {result.reason}")
    return False


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
        PROJECT_ROOT / "dataforge_07.egg-info",
        PROJECT_ROOT / "dataforge.egg-info",
        PROJECT_ROOT / "dataforge15.egg-info",
        *[
            path
            for package_root in PACKAGE_ROOTS
            for path in (
                package_root / "build",
                package_root / "dist",
                package_root / "dataforge_07_mcp.egg-info",
                package_root / "dataforge_mcp.egg-info",
                package_root / "dataforge15_mcp.egg-info",
                package_root / "dataforge_07_evals.egg-info",
                package_root / "dataforge_evals.egg-info",
                package_root / "dataforge15_evals.egg-info",
                package_root / "dataforge_07_dbt.egg-info",
                package_root / "dataforge_dbt.egg-info",
                package_root / "dataforge15_dbt.egg-info",
                package_root / "src" / "dataforge_07_agent_patterns.egg-info",
                package_root / "src" / "dataforge_agent_patterns.egg-info",
                package_root / "src" / "dataforge15_agent_patterns.egg-info",
            )
        ],
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
    cwd: Path = PROJECT_ROOT,
    optional: bool = False,
    timeout_seconds: int | None = None,
) -> bool:
    """Run a gate command and return whether it passed."""
    print(f"\n==> {label}")
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
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
    parser.add_argument(
        "--npm-audit-timeout",
        type=int,
        default=120,
        help="Seconds before optional npm audit is skipped or required audit fails.",
    )
    args = parser.parse_args()

    checks: list[bool] = [
        _coverage_policy_check(),
        _pip_audit_exception_check(),
        _corrector_promotion_gate(),
        _run("ruff check", [PYTHON, "-m", "ruff", "check", *PYTHON_PATHS]),
        _run("ruff format --check", [PYTHON, "-m", "ruff", "format", "--check", *PYTHON_PATHS]),
        _run("strict mypy", [PYTHON, "-m", "mypy", "--strict", *MYPY_PATHS]),
        _run("side package ruff check", [PYTHON, "-m", "ruff", "check", *SIDE_PACKAGE_PATHS]),
        _run(
            "side package ruff format --check",
            [PYTHON, "-m", "ruff", "format", "--check", *SIDE_PACKAGE_PATHS],
        ),
        _run(
            "side package strict mypy",
            [PYTHON, "-m", "mypy", "--strict", *SIDE_PACKAGE_MYPY_PATHS],
        ),
    ]
    # Trust invariants run UNCONDITIONALLY -- they must never be skippable, even
    # under --skip-full-tests, because they enforce the product's core guarantee
    # (no corruption; fail-closed N-version verification; reversible; honest
    # certificate). Fast (bounded property examples), so always affordable.
    checks.append(
        _run(
            "trust invariants",
            [PYTHON, "-m", "pytest", *TRUST_INVARIANT_TESTS, "-p", "no:cacheprovider", "-q"],
        )
    )
    if not args.skip_full_tests:
        checks.append(
            _run(
                "root pytest with coverage",
                [
                    PYTHON,
                    "-m",
                    "pytest",
                    "tests/",
                    "--cov=dataforge",
                    "--cov-report=term-missing",
                    "-x",
                    "-v",
                ],
            )
        )
        checks.append(
            _run(
                "critical-path coverage",
                [
                    PYTHON,
                    "-m",
                    "coverage",
                    "report",
                    f"--include={CRITICAL_COVERAGE_INCLUDE}",
                    f"--fail-under={CRITICAL_COVERAGE_FAIL_UNDER}",
                ],
            )
        )
    checks.extend(
        [
            _run(
                "MCP pytest with tools coverage",
                [
                    PYTHON,
                    "-m",
                    "pytest",
                    "tests",
                    "--cov=dataforge_mcp.tools",
                    "--cov-report=term-missing",
                    "--cov-fail-under=85",
                    "-v",
                ],
                cwd=PROJECT_ROOT / "dataforge-mcp",
            ),
            _run(
                "dataforge_07_evals pytest",
                [PYTHON, "-m", "pytest", "tests", "-v"],
                cwd=PROJECT_ROOT / "packages" / "dataforge-evals",
            ),
            _run(
                "dataforge_07_agent_patterns pytest",
                [PYTHON, "-m", "pytest", "tests", "-v"],
                cwd=PROJECT_ROOT / "packages" / "dataforge-agent-patterns",
            ),
            _run(
                "dataforge_07_dbt pytest",
                [PYTHON, "-m", "pytest", "integration_tests", "-v"],
                cwd=PROJECT_ROOT / "packages" / "dataforge-dbt",
            ),
            _run("README truth", [PYTHON, "scripts/ci/readme_truth.py"]),
            _run("benchmark truth", [PYTHON, "scripts/ci/benchmark_truth.py", "--check"]),
            _run("OpenAPI drift", [PYTHON, "scripts/ci/openapi_contract.py", "--check"]),
            _run(
                "release doctor",
                [PYTHON, "-m", "dataforge", "release", "doctor", "--core", "--json"],
            ),
            _run(
                "docs strict build",
                [PYTHON, "-m", "mkdocs", "build", "-f", "docs/mkdocs.yml", "--strict"],
            ),
            _run("playground build", [NPM, "--prefix", "playground/web", "run", "build"]),
            _run("playground test", [NPM, "--prefix", "playground/web", "run", "test"]),
            _secret_scan(),
        ]
    )

    pip_audit_optional = not (
        args.require_optional or os.environ.get("DATAFORGE_REQUIRE_PIP_AUDIT")
    )
    checks.append(
        _run(
            "pip-audit",
            [
                PYTHON,
                "-m",
                "pip_audit",
                "--local",
                "--progress-spinner",
                "off",
                *pip_audit_ignore_args(),
            ],
            optional=pip_audit_optional,
            timeout_seconds=args.dependency_audit_timeout,
        )
    )
    npm_audit_optional = not (
        args.require_optional or os.environ.get("DATAFORGE_REQUIRE_NPM_AUDIT")
    )
    checks.append(
        _run(
            "playground npm audit",
            [NPM, "--prefix", "playground/web", "audit", "--audit-level=moderate"],
            optional=npm_audit_optional,
            timeout_seconds=args.npm_audit_timeout,
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
            "dataforge_07 package build",
            [PYTHON, "-m", "build", "--sdist", "--wheel"],
            optional=build_optional,
        )
    )
    checks.append(
        _run(
            "dataforge_07_mcp package build",
            [PYTHON, "-m", "build", "--sdist", "--wheel", "dataforge-mcp"],
            optional=build_optional,
        )
    )
    checks.append(
        _run(
            "dataforge_07_evals package build",
            [PYTHON, "-m", "build", "--sdist", "--wheel", "packages/dataforge-evals"],
            optional=build_optional,
        )
    )
    checks.append(
        _run(
            "dataforge_07_agent_patterns package build",
            [
                PYTHON,
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "packages/dataforge-agent-patterns",
            ],
            optional=build_optional,
        )
    )
    checks.append(
        _run(
            "dataforge_07_dbt package build",
            [PYTHON, "-m", "build", "--sdist", "--wheel", "packages/dataforge-dbt"],
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
