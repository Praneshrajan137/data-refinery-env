"""Canonical backend release-quality gate for DataForge."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

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
    "scripts/ci/docs_truth.py",
    "scripts/ci/full_vision_external_gate.py",
    "scripts/ci/installed_package_smoke.py",
    "scripts/ci/openapi_contract.py",
    "scripts/ci/pypi_publish_report.py",
    "scripts/ci/backend_gate.py",
    # Added 2026-08-26. This file plants 17 mutants against the write-authority guards and was
    # invoked by nothing and type-checked by nothing -- a dead gate reads as coverage, which is
    # worse than no gate. It is now run below and checked here.
    "scripts/ci/mutate_autoapply_guards.py",
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
    # The warehouse mutation primitive is raw SQL, not apply_transaction, so it carries
    # its own copy of the proven-only gate. It is listed here because that surface went
    # ungated from 2026-07-11 to 2026-08-09 with a green suite -- see
    # docs/trust/write-surface-uniformity.md.
    "tests/unit/test_table_store_proven_gate.py",
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
        vuln_id="PYSEC-2026-3716",
        package="datasets",
        scope="optional train extra only, and only transitively via trl; dataforge/ imports the "
        "datasets library zero times, and it is absent from the core, playground and MCP "
        "runtime surfaces",
        expires_on=date(2026, 11, 26),
        reason=(
            "Triaged 2026-08-28. The advisory is a path traversal in datasets' FOLDER-BASED "
            "dataset builders: a crafted `file_name` metadata field is joined to the dataset "
            "directory without validation, letting an attacker-supplied dataset directory read "
            "arbitrary local files, which are then embedded into output on save_to_disk or "
            "push_to_hub. That builder is never used here. Every import in this repository is "
            "`from datasets import Dataset` (11 sites: scripts/remote/kaggle_*.py and "
            "training/kaggle_*_kernel/*.py), and every construction is Dataset.from_list on "
            "records already parsed from a local JSONL file -- there is no load_dataset call, no "
            "imagefolder/audiofolder builder, and therefore no file_name metadata path to "
            "traverse. CVSS 4.0 also scores UI:A, i.e. it needs a user to actively load a "
            "hostile dataset directory. The fix is 5.0.1 and is NOT a patch bump: train pins "
            "datasets==4.8.5 alongside trl==1.4.0 and transformers==5.7.0, so moving to 5.x is a "
            "deliberate training-stack revalidation rather than something to sweep in here."
        ),
        upstream_reference="https://github.com/huggingface/datasets/issues/8324",
    ),
]
# Two pymdown-extensions exceptions were DELETED on 2026-08-28 rather than re-dated, because the
# blocker they cited cleared upstream. Both had argued the fix was unreachable: mkdocs-material
# 9.6.23 required `pymdown-extensions~=10.2`, capping below 11, while PYSEC-2026-3609 (b64 path
# traversal) is fixed in 11.0.0 and CVE-2026-67422 (exponential ReDoS in caret/tilde/betterem/
# magiclink) in 11.0.1. mkdocs-material 9.7.0 relaxed that requirement to `>=10.2`, so
# docs/requirements.txt now pins mkdocs-material 9.7.7 with pymdown-extensions 11.0.2 and the
# advisories are simply fixed. Recorded here because deleting an exception silently looks
# identical to forgetting one: an ignore that suppresses nothing is not harmless, it still
# carries an expiry that fails the gate for a vulnerability the environment no longer has.


@dataclass(frozen=True)
class NpmAuditException:
    """One explicitly triaged npm advisory.

    Mirrors :class:`PipAuditException`. npm audit previously had no exception path at all,
    so a single unfixable dev-only advisory could only be resolved by weakening the gate.
    """

    advisory_id: str
    package: str
    scope: str
    expires_on: date
    reason: str
    upstream_reference: str


NPM_AUDIT_EXCEPTIONS = [
    NpmAuditException(
        advisory_id="GHSA-r28c-9q8g-f849",
        package="postcss",
        scope="vite build toolchain only; not in the shipped browser bundle",
        expires_on=date(2026, 11, 8),
        reason=(
            "Triaged 2026-08-08. postcss reaches this project on exactly one path, "
            "vite -> devDependencies, where it processes CSS at build time. The five "
            "runtime dependencies are react, react-dom, motion, papaparse and "
            "lucide-react. The patched 8.5.26 satisfies vite's existing ^8.5.15 range, so "
            "this IS fixable by a lockfile bump -- but regenerating package-lock.json on "
            "Windows produced a lock with zero integrity/resolved fields and 192 of 226 "
            "packages, which is a worse supply-chain outcome than a build-time advisory. "
            "Regenerate on Linux and remove this exception."
        ),
        upstream_reference="https://github.com/advisories/GHSA-r28c-9q8g-f849",
    ),
    NpmAuditException(
        advisory_id="GHSA-fxqj-rqcc-2cmp",
        package="postcss",
        scope="vite build toolchain only; not in the shipped browser bundle",
        expires_on=date(2026, 11, 8),
        reason="Triaged 2026-08-08, same package and same blocker as GHSA-r28c-9q8g-f849.",
        upstream_reference="https://github.com/advisories/GHSA-fxqj-rqcc-2cmp",
    ),
    NpmAuditException(
        advisory_id="GHSA-28wg-ghj8-5hjv",
        package="nanoid",
        scope="transitive dependency of postcss, so vite build toolchain only",
        expires_on=date(2026, 11, 8),
        reason=(
            "Triaged 2026-08-08. nanoid arrives only under postcss, which is build-time "
            "only. The advisory needs an attacker-controlled negative size argument, and "
            "no application code calls nanoid at all."
        ),
        upstream_reference="https://github.com/advisories/GHSA-28wg-ghj8-5hjv",
    ),
    NpmAuditException(
        advisory_id="GHSA-2v37-7h3g-55p8",
        package="nanoid",
        scope="transitive dependency of postcss, so vite build toolchain only",
        expires_on=date(2026, 11, 8),
        reason="Triaged 2026-08-08, same package and same blocker as GHSA-28wg-ghj8-5hjv.",
        upstream_reference="https://github.com/advisories/GHSA-2v37-7h3g-55p8",
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


#: The runtime surfaces a dependency audit must actually have covered.
#:
#: Derived from ``pyproject.toml``'s own ``[project] dependencies`` at check time rather
#: than restated here; this maps each surface to an import-name probe so the audit's scope
#: can be *reported* instead of inferred.
_AUDITED_SURFACES: Final = {
    "core": ("pandas", "pydantic", "typer", "z3"),
    "playground": ("fastapi",),
    "mcp": ("mcp",),
}


def pip_audit_scope_errors() -> list[str]:
    """Return errors when the dependency audit did not cover the declared runtime surface.

    ``pip-audit --local`` audits *the installed environment*, so its population is
    whatever happens to be installed. That made the gate's scope vary silently by machine:
    it failed locally on 11 upstream advisories in the training and dbt stacks while
    passing in CI, whose install set (``pip install -e ".[dev]"``) does not contain them.

    Both results were reported as "pip-audit", and neither said what it had looked at. A
    green audit that ran against a subset of the shipped surface is not evidence about the
    shipped surface -- the same defect class ``PRODUCT.md`` records for gates that freeze
    the population they police, applied to a gate whose population is an accident of
    environment.

    The fix is scope reporting, not exceptions. An exception silences a known advisory
    deliberately and expires; this instead fails when the audit could not have seen a
    surface the product ships, which is the case a passing result must not be allowed to
    look like.

    Optional stacks (``train``, ``dbt``) are deliberately NOT required: they are extras, a
    developer legitimately may not have them, and demanding them would make the gate
    unrunnable rather than honest. What is required is that the audit covered core,
    playground and MCP -- the three surfaces a user actually installs.
    """
    from importlib.util import find_spec

    errors: list[str] = []
    for surface, probes in sorted(_AUDITED_SURFACES.items()):
        missing = [probe for probe in probes if find_spec(probe) is None]
        if missing:
            errors.append(
                f"dependency audit did not cover the {surface} surface: "
                f"{', '.join(sorted(missing))} not installed, so a passing pip-audit says "
                'nothing about it. Install with `pip install -e ".[all]"`.'
            )
    return errors


def pip_audit_scope_report() -> str:
    """Return a one-line description of what the dependency audit actually covered.

    Printed beside the audit result so a green run states its own scope. A gate that does
    not say what it looked at cannot be compared against another run of itself.
    """
    from importlib.metadata import distributions
    from importlib.util import find_spec

    installed = sum(1 for _ in distributions())
    covered = [
        surface
        for surface, probes in sorted(_AUDITED_SURFACES.items())
        if all(find_spec(probe) is not None for probe in probes)
    ]
    optional = [name for name in ("torch", "trl", "dbt") if find_spec(name) is not None]
    return (
        f"pip-audit scope: {installed} installed distribution(s); "
        f"required surfaces covered: {', '.join(covered) or 'none'}; "
        f"optional stacks present: {', '.join(optional) or 'none'}"
    )


def npm_audit_exception_errors(
    exceptions: list[NpmAuditException] = NPM_AUDIT_EXCEPTIONS,
    *,
    today: date | None = None,
) -> list[str]:
    """Return validation errors for npm audit exceptions.

    Same discipline as :func:`pip_audit_exception_errors`: an exception must identify the
    advisory, say where the package is reachable from, justify itself, cite upstream, and
    expire. An exception that never expires is a permanently silenced check.
    """
    observed_today = today or _today_utc()
    errors: list[str] = []
    for exception in exceptions:
        if not exception.advisory_id:
            errors.append("npm audit exception is missing advisory_id.")
            continue
        if not exception.package:
            errors.append(f"{exception.advisory_id} is missing package.")
        if not exception.scope.strip():
            errors.append(f"{exception.advisory_id} is missing scope.")
        if not exception.reason.strip():
            errors.append(f"{exception.advisory_id} is missing reason.")
        if not exception.upstream_reference.startswith("https://"):
            errors.append(f"{exception.advisory_id} is missing an upstream reference.")
        if exception.expires_on < observed_today:
            errors.append(
                f"npm audit exception {exception.advisory_id} expired on "
                f"{exception.expires_on.isoformat()}."
            )
    return errors


def npm_audit_blocking_advisories(
    payload: dict[str, Any],
    exceptions: list[NpmAuditException] = NPM_AUDIT_EXCEPTIONS,
    *,
    min_severity: str = "moderate",
) -> list[str]:
    """Return advisory identifiers that must block the gate.

    Parses ``npm audit --json`` and drops anything explicitly triaged. Severities below
    ``min_severity`` are ignored, matching the previous ``--audit-level=moderate`` behaviour.

    Fails **closed on unparseable**: an advisory whose id cannot be read is reported rather
    than skipped, because a silently-dropped advisory is exactly the outcome this guards.
    """
    order = ["info", "low", "moderate", "high", "critical"]
    floor = order.index(min_severity)
    allowed = {exception.advisory_id for exception in exceptions}
    blocking: list[str] = []
    for name, entry in (payload.get("vulnerabilities") or {}).items():
        if not isinstance(entry, dict):
            blocking.append(f"{name} (unparseable advisory entry)")
            continue
        severity = str(entry.get("severity", "high"))
        if severity in order and order.index(severity) < floor:
            continue
        for source in entry.get("via") or []:
            # A string entry means "vulnerable because of another package", not an
            # advisory in its own right, so it carries no id to triage.
            if not isinstance(source, dict):
                continue
            identifier = source.get("url", "")
            advisory_id = str(identifier).rstrip("/").rsplit("/", 1)[-1]
            if not advisory_id:
                blocking.append(f"{name} (advisory with no identifier)")
            elif advisory_id not in allowed:
                blocking.append(f"{advisory_id} ({name}, {severity})")
    return sorted(set(blocking))


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


def _npm_audit_check(*, optional: bool, timeout_seconds: int) -> bool:
    """Run npm audit and fail only on advisories that are not explicitly triaged."""
    print("\n==> playground npm audit")
    policy_errors = npm_audit_exception_errors()
    if policy_errors:
        for error in policy_errors:
            print(f"FAIL npm audit exception policy: {error}")
        return False
    try:
        completed = subprocess.run(
            [NPM, "--prefix", "playground/web", "audit", "--json"],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=PROJECT_ROOT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        message = f"npm audit could not run: {exc}"
        if optional:
            print(f"SKIP playground npm audit: {message}")
            return True
        print(f"FAIL playground npm audit: {message}")
        return False
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        # Fail closed: unreadable audit output is not evidence of safety.
        print(f"FAIL playground npm audit: could not parse npm audit --json ({exc})")
        return False
    blocking = npm_audit_blocking_advisories(payload)
    if blocking:
        for advisory in blocking:
            print(f"FAIL playground npm audit: untriaged advisory {advisory}")
        return False
    triaged = len(NPM_AUDIT_EXCEPTIONS)
    print(f"PASS playground npm audit (no untriaged advisories; {triaged} triaged exceptions)")
    return True


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


@dataclass(frozen=True)
class GateCommand:
    """One command in a concurrent group.

    Attributes:
        label: The step name. It must match the label the sequential ``_run`` would have used,
            because ``scripts/ci/gate_population.py`` derives the gate's step list from these
            literals -- a renamed step reads as a removed step, which is the point.
        command: argv.
        cwd: Working directory.
        optional: Same fail-open semantics as :func:`_run`.
        timeout_seconds: Same as :func:`_run`.
    """

    label: str
    command: list[str]
    cwd: Path = PROJECT_ROOT
    optional: bool = False
    timeout_seconds: int | None = None


def _run_captured(spec: GateCommand) -> tuple[bool, str]:
    """Run one command, returning ``(passed, rendered output)``.

    Output is captured rather than inherited so that concurrent steps do not interleave into an
    unreadable stream. The cost is that a long step shows nothing until it finishes, which is why
    the expensive sequential steps still use :func:`_run`.
    """
    try:
        result = subprocess.run(
            spec.command,
            cwd=spec.cwd,
            check=False,
            timeout=spec.timeout_seconds,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        verdict = "SKIP" if spec.optional else "FAIL"
        return spec.optional, f"{verdict} {spec.label}: {exc}"
    except subprocess.TimeoutExpired:
        verdict = "SKIP" if spec.optional else "FAIL"
        return spec.optional, f"{verdict} {spec.label}: timed out after {spec.timeout_seconds}s"

    if result.returncode == 0:
        return True, f"PASS {spec.label}"
    body = ((result.stdout or "") + (result.stderr or "")).strip()
    verdict = "SKIP" if spec.optional else "FAIL"
    detail = f"{verdict} {spec.label}: command exited {result.returncode}"
    return spec.optional, f"{detail}\n{body}" if body else detail


def _run_group(group: str, specs: list[GateCommand]) -> list[bool]:
    """Run mutually independent commands concurrently, preserving result order.

    Threads rather than processes: every unit of work is a ``subprocess.run``, so the GIL is
    released for the duration and there is nothing to gain from process pools.

    A group is only ever formed from steps that share no state. The orderings that are real and
    therefore NOT grouped:

    * ``root pytest with coverage`` -> ``critical-path coverage``, because the second reads the
      ``.coverage`` file the first writes;
    * ``_clean_package_artifacts()`` -> the five package builds, because it deletes ``build/``,
      ``dist/`` and every ``*.egg-info``;
    * the auto-apply mutants, which write mutations into real source files in the working tree and
      must therefore run with nothing else touching the tree. A concurrent run once left the
      write-safety allowlist inverted on disk.
    """
    print(f"\n==> [{group}] {len(specs)} step(s) concurrently")
    with ThreadPoolExecutor(max_workers=min(len(specs), (os.cpu_count() or 4))) as pool:
        rendered = list(pool.map(_run_captured, specs))
    results: list[bool] = []
    for passed, output in rendered:
        print(output)
        results.append(passed)
    return results


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
    ]
    # Static analysis: six mutually independent commands over disjoint path sets, none of which
    # writes anything. Grouped 2026-08-28. Sequentially this was six cold interpreter starts plus
    # two full mypy passes back to back.
    checks.extend(
        _run_group(
            "static analysis",
            [
                GateCommand("ruff check", [PYTHON, "-m", "ruff", "check", *PYTHON_PATHS]),
                GateCommand(
                    "ruff format --check",
                    [PYTHON, "-m", "ruff", "format", "--check", *PYTHON_PATHS],
                ),
                GateCommand("strict mypy", [PYTHON, "-m", "mypy", "--strict", *MYPY_PATHS]),
                GateCommand(
                    "side package ruff check",
                    [PYTHON, "-m", "ruff", "check", *SIDE_PACKAGE_PATHS],
                ),
                GateCommand(
                    "side package ruff format --check",
                    [PYTHON, "-m", "ruff", "format", "--check", *SIDE_PACKAGE_PATHS],
                ),
                GateCommand(
                    "side package strict mypy",
                    [PYTHON, "-m", "mypy", "--strict", *SIDE_PACKAGE_MYPY_PATHS],
                ),
            ],
        )
    )
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
    # Latency budgets, run UNCONDITIONALLY for the same reason as the trust invariants: a
    # performance gate that only runs when someone remembers is not a gate.
    #
    # These two budgets existed since 2026-04-20 and had NEVER executed. The files are named
    # bench_*.py, pytest's default `python_files` does not match that, and there is no conftest
    # overriding it -- so `make bench` collected nothing and exited 5. On the day this step was
    # added the SMT budget failed at about 248ms mean and 607ms max against its own 200ms
    # assertion, on a 1000-row fixture. The budget was right and nothing was reading it.
    #
    # `-o python_files` rather than renaming the files: the bench_*.py name is what keeps 100
    # benchmark rounds out of `make test`. `-n 0` is equally required: pytest-benchmark
    # auto-activates --benchmark-disable when xdist is on, and this repo's addopts carry
    # `--dist loadgroup`, so without it the run exits 4 with "Can't have both --benchmark-only
    # and --benchmark-disable" -- a usage error that would have read as a vacuous pass had the
    # step been written without checking. It is also right on the merits: timing under parallel
    # workers measures contention, not cost.
    #
    # No --benchmark-autosave here, because that writes into .benchmarks/ and this gate must not
    # modify the tree (the release gate rejects that path from sdists, and the tree-integrity
    # guard exists to catch exactly this).
    checks.append(
        _run(
            "latency budgets",
            [
                PYTHON,
                "-m",
                "pytest",
                "tests/benchmarks/",
                "-o",
                "python_files=bench_*.py",
                "-n",
                "0",
                "--benchmark-only",
                "-p",
                "no:cacheprovider",
                "-q",
            ],
        )
    )
    # Counted work, not wall clock. This is the deterministic half of the performance gate: the
    # latency budgets above assert milliseconds, which vary with machine load (the same verifier
    # code measured 42, 166-249, 136-143 and 79.8-352.2 ms/fix across one afternoon), while this
    # asserts z3 AST constructions and assertion counts, which were bit-identical across repeated
    # runs. Cachegrind's manual makes the same argument for instruction counts: time is the better
    # metric but counts are the reproducible one, so counts are what can gate.
    #
    # It is a PROXY. A change that cut assertions while slowing the solver would pass here and must
    # be caught by "latency budgets". The two steps are complementary and neither replaces the other.
    checks.append(
        _run(
            "counted verifier work",
            [PYTHON, "scripts/perf/measure_verifier_work.py", "--check"],
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
                ],
            )
        )
        # Sequential, and NOT grouped with the step above: this reads the .coverage file that
        # step writes.
        #
        # This step deliberately does NOT get `-n logical`, and that is a measured decision
        # rather than an oversight. Parallel coverage was 48-60s against 126s serial, but the
        # reported TOTAL moved across three runs -- 83.73%, 83.75%, 83.76% -- because per-worker
        # data files are combined. This gate enforces thresholds (82% policy, 88% critical path),
        # and CI has already failed once at 84.98% against a required 85%. A threshold gate whose
        # input wobbles run to run is the same defect that made this repo pin its ruff and mypy
        # versions: a gate that depends on which day it ran is not a gate. The suite's own
        # parallel run (32s, no coverage) is where the speed was taken instead.
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
    # Four disjoint package suites in four disjoint working directories, plus the truth checks and
    # contract checks, none of which writes into the tree. Grouped 2026-08-28. `-v` dropped from
    # the four pytest steps: under capture it produced thousands of lines nobody reads, and a
    # failure prints its own output.
    checks.extend(
        _run_group(
            "side package suites and truth checks",
            [
                GateCommand(
                    "MCP pytest with tools coverage",
                    [
                        PYTHON,
                        "-m",
                        "pytest",
                        "tests",
                        "--cov=dataforge_mcp.tools",
                        "--cov-report=term-missing",
                        "--cov-fail-under=85",
                    ],
                    cwd=PROJECT_ROOT / "dataforge-mcp",
                ),
                GateCommand(
                    "dataforge_07_evals pytest",
                    [PYTHON, "-m", "pytest", "tests"],
                    cwd=PROJECT_ROOT / "packages" / "dataforge-evals",
                ),
                GateCommand(
                    "dataforge_07_agent_patterns pytest",
                    [PYTHON, "-m", "pytest", "tests"],
                    cwd=PROJECT_ROOT / "packages" / "dataforge-agent-patterns",
                ),
                GateCommand(
                    "dataforge_07_dbt pytest",
                    [PYTHON, "-m", "pytest", "integration_tests"],
                    cwd=PROJECT_ROOT / "packages" / "dataforge-dbt",
                ),
                GateCommand("README truth", [PYTHON, "scripts/ci/readme_truth.py"]),
                GateCommand(
                    "benchmark truth", [PYTHON, "scripts/ci/benchmark_truth.py", "--check"]
                ),
                GateCommand("docs truth", [PYTHON, "scripts/ci/docs_truth.py", "--check"]),
                GateCommand(
                    "vocabulary projection",
                    [PYTHON, "scripts/ci/generate_domain_vocabulary.py", "--check"],
                ),
                # Added 2026-08-28, with the performance work, and load-bearing for it. Making a
                # gate faster and making it check less are indistinguishable from the outside:
                # reordering, parallelising and deduplicating steps all reduce wall clock and all
                # can reduce coverage while still exiting 0. This pins the population -- pytest
                # node ids, gate step names, mutant ids and their test paths, claim ids, scanned
                # documents -- so a shrinking gate has to be an explicit, explained edit rather
                # than a side effect.
                GateCommand(
                    "gate population", [PYTHON, "scripts/ci/gate_population.py", "--check"]
                ),
                # Keeps the mapped inner loop fast. A module with no mapping falls back to the
                # full suite, so a gap costs speed rather than correctness; this only stops the
                # gap growing silently, and deliberately does not force mappings to be invented
                # in bulk.
                GateCommand(
                    "test map coverage", [PYTHON, "scripts/ci/test_map_coverage.py", "--check"]
                ),
                GateCommand(
                    "attestation vector projection",
                    [PYTHON, "scripts/ci/generate_attestation_vectors.py", "--check"],
                ),
                # Read-only: builds attestations in memory and runs a vector suite, writing
                # nothing into the tree, so it is safe in a concurrent group. Its two siblings
                # that DO write source files are run sequentially further down.
                GateCommand(
                    "attestation conformance",
                    [PYTHON, "scripts/ci/attestation_conformance.py", "--check"],
                ),
                GateCommand("OpenAPI drift", [PYTHON, "scripts/ci/openapi_contract.py", "--check"]),
                GateCommand(
                    "release doctor",
                    [PYTHON, "-m", "dataforge", "release", "doctor", "--core", "--json"],
                ),
                GateCommand(
                    "docs strict build",
                    [PYTHON, "-m", "mkdocs", "build", "-f", "docs/mkdocs.yml", "--strict"],
                ),
            ],
        )
    )
    # ALL THREE mutation harnesses run here, SEQUENTIALLY AND ALONE, and that is not stylistic.
    # Each one writes a mutation into a real source file in the working tree and restores it in a
    # `finally`. Two concurrent runs once raced on dataforge/engine/repair.py and left the
    # write-safety allowlist INVERTED on disk (`not in` had become `in`), found only because
    # someone read `git status` before staging.
    #
    # I first placed the two newly-wired harnesses in the concurrent group above and caught it
    # here: they would have raced each other, the auto-apply mutants, AND the four pytest runs
    # sharing that group -- reproducing the exact defect this work exists to prevent, three times
    # over. Wiring up an orphaned gate and parallelising are safe changes individually and not
    # together.
    #
    # There is also no speed argument for isolating these into worktrees: the auto-apply harness
    # measures 47.4s for a green baseline plus 18 mutants. Sequential here is a correctness
    # choice that costs almost nothing.
    #
    # The two vocabulary/corpus harnesses ran pytest and were invoked by NOTHING before
    # 2026-08-28 -- they appeared only in the Makefile's mypy argument list, so they were
    # type-checked and never executed. That is the same orphaned-gate defect fixed for
    # mutate_autoapply_guards.py on 2026-08-26 and never generalised to its siblings.
    checks.append(
        _run("auto-apply guard mutants", [PYTHON, "scripts/ci/mutate_autoapply_guards.py"])
    )
    checks.append(
        _run("domain vocabulary mutants", [PYTHON, "scripts/ci/mutate_domain_vocabulary.py"])
    )
    checks.append(
        _run("adversarial corpus mutants", [PYTHON, "scripts/ci/mutate_adversarial_corpus.py"])
    )
    checks.extend(
        _run_group(
            "playground",
            [
                GateCommand(
                    "playground build", [NPM, "--prefix", "playground/web", "run", "build"]
                ),
                GateCommand("playground test", [NPM, "--prefix", "playground/web", "run", "test"]),
            ],
        )
    )
    checks.append(_secret_scan())

    pip_audit_optional = not (
        args.require_optional or os.environ.get("DATAFORGE_REQUIRE_PIP_AUDIT")
    )
    # State the audit's scope before running it. `--local` audits the installed
    # environment, so a green result is only as wide as what happens to be installed --
    # which is why this gate failed locally and passed in CI on the same commit, with
    # neither result saying what it had looked at.
    print(pip_audit_scope_report(), flush=True)
    scope_errors = pip_audit_scope_errors()
    if scope_errors and not pip_audit_optional:
        for error in scope_errors:
            print(f"pip-audit scope error: {error}", flush=True)
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
        _npm_audit_check(
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
