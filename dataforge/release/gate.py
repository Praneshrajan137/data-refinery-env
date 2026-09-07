"""Canonical local release gate for DataForge.

The release gate is intentionally heavier than the release doctor: it proves a
fresh user can install the built wheel from a local wheelhouse and run the core
CLI lifecycle without relying on the source checkout.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "release_gate_report_v1"
PACKAGE_NAME = "dataforge_07"
CLI_NAMES = ("dataforge", "dataforge15")
REQUIRED_WHEEL_MEMBERS = frozenset(
    {
        "dataforge/py.typed",
        "dataforge/cli/__init__.py",
        "dataforge/cli/constraints.py",
        "dataforge/cli/repair.py",
        "dataforge/cli/revert.py",
        "dataforge/cli/audit.py",
        "dataforge/fixtures/hospital_10rows.csv",
        "dataforge/fixtures/hospital_schema.yaml",
        "dataforge/safety/constitutions/default.yaml",
        "dataforge/stores/cloud.py",
        "dataforge/stores/duckdb.py",
        "dataforge/stores/patch_plan.py",
        "dataforge/stores/registry.py",
        "dataforge/stores/repair.py",
        "dataforge/transactions/log.py",
        "dataforge/transactions/revert.py",
    }
)
# Prefix rejections kept ONLY where they are not already implied by the class rules below.
# `_audit_wheel_contents` requires every member to start with "dataforge/" or be dist-info
# metadata, so a rejection listing a specific directory adds nothing -- and once that
# directory is deleted it polices an empty population, which keeps the gate green while
# proving strictly less. "data_quality_env/" was removed from this tuple on 2026-08-27
# when that package was deleted, for exactly that reason.
REJECTED_WHEEL_PREFIXES = (
    "tests/",
    ".github/",
    ".hf-space",
    "build/",
    "dist/",
    "node_modules/",
)
REQUIRED_SDIST_MEMBERS = frozenset(
    {
        "PKG-INFO",
        "README.md",
        "LICENSE",
        "MANIFEST.in",
        "pyproject.toml",
        "dataforge/__init__.py",
        "dataforge/py.typed",
        "dataforge/cli/profile.py",
        "dataforge/cli/constraints.py",
        "dataforge/cli/repair.py",
        "dataforge/stores/cloud.py",
        "dataforge/stores/duckdb.py",
        "dataforge/stores/patch_plan.py",
        "dataforge/stores/registry.py",
        "dataforge/stores/repair.py",
        "dataforge/fixtures/hospital_10rows.csv",
        "dataforge/fixtures/hospital_schema.yaml",
    }
)
ALLOWED_SDIST_TOP_LEVEL = {
    "PKG-INFO",
    "README.md",
    "LICENSE",
    "MANIFEST.in",
    "pyproject.toml",
    "setup.cfg",
    "dataforge",
    f"{PACKAGE_NAME}.egg-info",
}
ALLOWED_SDIST_EGG_INFO = {
    "PKG-INFO",
    "SOURCES.txt",
    "dependency_links.txt",
    "entry_points.txt",
    "requires.txt",
    "top_level.txt",
}
REJECTED_SDIST_PREFIXES = (
    "tests/",
    ".github/",
    ".hf-space",
    "build/",
    "dist/",
    "node_modules/",
    "logs/",
    "eval/",
    "archive/",
    "playground/",
    "playground-model/",
    "dataforge-mcp/",
    "benchmark_results/",
    "scripts/",
    "docs/",
)
# REJECTED_ROOT_LEGACY_FILES used to enumerate fourteen root-level scripts by name
# (models.py, compat.py, benchmark.py, verify_nuclear.py, ...). It was deleted on
# 2026-08-27 along with the files themselves, and deliberately NOT replaced: it was
# already subsumed by ALLOWED_SDIST_TOP_LEVEL, which admits no root-level ".py" at all,
# so it caught nothing the class rule missed while implying the opposite. Enumerating
# instances also fails in the direction that matters -- a shim named something nobody
# thought of was never on the list. The surviving guards are the top-level allowlist here
# and tests/regression/test_env.py::test_repo_root_carries_no_loose_python_modules.
REJECTED_WHEEL_PARTS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".hypothesis",
}
REJECTED_SDIST_PARTS = REJECTED_WHEEL_PARTS | {
    ".benchmarks",
    ".claude",
    ".idea",
}


@dataclass(frozen=True)
class ReleaseGateStep:
    """One release-gate check or command result."""

    name: str
    ok: bool
    detail: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReleaseGateReport:
    """Machine-readable proof emitted by ``dataforge release gate``."""

    ok: bool
    steps: list[ReleaseGateStep]
    schema_version: str = SCHEMA_VERSION
    artifact_sha256: dict[str, str] = field(default_factory=dict)
    offline_install: bool = True
    secrets_printed: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report."""
        return asdict(self)


class ReleaseGateError(RuntimeError):
    """Raised when the release gate cannot be completed."""


def _project_root() -> Path:
    """Return the repository root for the DataForge package."""
    return Path(__file__).resolve().parents[2]


def _python_in_venv(venv_path: Path) -> Path:
    """Return the Python executable path inside a venv."""
    if os.name == "nt":
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


def _script_in_venv(venv_path: Path, name: str) -> Path:
    """Return an installed console-script path inside a venv."""
    suffix = ".exe" if os.name == "nt" else ""
    directory = "Scripts" if os.name == "nt" else "bin"
    return venv_path / directory / f"{name}{suffix}"


def _command_text(command: list[str | os.PathLike[str]]) -> str:
    """Return a readable command string without shell quoting semantics."""
    return " ".join(str(part) for part in command)


def _tail(text: str, *, limit: int = 4000) -> str:
    """Return the tail of command output for compact reports."""
    if len(text) <= limit:
        return text
    return text[-limit:]


def _output_text(value: str | bytes | None) -> str:
    """Normalize subprocess timeout output to text."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _gate_env() -> dict[str, str]:
    """Return a command environment that avoids source-checkout import leakage."""
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def _run_command(
    name: str,
    command: list[str | os.PathLike[str]],
    *,
    cwd: Path,
    timeout_seconds: int = 120,
) -> tuple[ReleaseGateStep, subprocess.CompletedProcess[str]]:
    """Run a command and convert the result into a release-gate step."""
    try:
        result = subprocess.run(
            [str(part) for part in command],
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
            env=_gate_env(),
        )
    except FileNotFoundError as exc:
        step = ReleaseGateStep(
            name=name,
            ok=False,
            detail=f"Command not found: {exc}",
            metadata={"command": _command_text(command)},
        )
        return step, subprocess.CompletedProcess([str(part) for part in command], 127)
    except subprocess.TimeoutExpired as exc:
        step = ReleaseGateStep(
            name=name,
            ok=False,
            detail=f"Timed out after {timeout_seconds}s.",
            metadata={
                "command": _command_text(command),
                "stdout_tail": _tail(_output_text(exc.stdout)),
                "stderr_tail": _tail(_output_text(exc.stderr)),
            },
        )
        return step, subprocess.CompletedProcess([str(part) for part in command], 124)

    ok = result.returncode == 0
    step = ReleaseGateStep(
        name=name,
        ok=ok,
        detail="passed" if ok else f"exited {result.returncode}",
        metadata={
            "command": _command_text(command),
            "returncode": result.returncode,
            "stdout_tail": _tail(result.stdout),
            "stderr_tail": _tail(result.stderr),
        },
    )
    return step, result


def _file_sha256(path: Path) -> str:
    """Return a SHA-256 digest for a file."""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _find_single_artifact(directory: Path, pattern: str) -> Path:
    """Return the unique newest artifact matching ``pattern``."""
    matches = sorted(directory.glob(pattern), key=lambda path: path.name)
    if not matches:
        raise ReleaseGateError(f"No artifact matched {pattern!r} in {directory}.")
    if len(matches) > 1:
        raise ReleaseGateError(f"Expected one artifact matching {pattern!r}, found {len(matches)}.")
    return matches[0]


def _audit_wheel_contents(wheel_path: Path) -> ReleaseGateStep:
    """Verify the wheel contains only the public package boundary."""
    errors: list[str] = []
    with zipfile.ZipFile(wheel_path) as archive:
        members = sorted(info.filename for info in archive.infolist() if not info.is_dir())

    for member in members:
        parts = set(Path(member).parts)
        if any(member.startswith(prefix) for prefix in REJECTED_WHEEL_PREFIXES):
            errors.append(f"Rejected path prefix: {member}")
        if parts & REJECTED_WHEEL_PARTS:
            errors.append(f"Rejected generated/cache path: {member}")
        if member.endswith((".pyc", ".pyo")):
            errors.append(f"Rejected bytecode file: {member}")
        if not (
            member.startswith("dataforge/")
            or (member.startswith(f"{PACKAGE_NAME}-") and ".dist-info/" in member)
        ):
            errors.append(f"Unexpected top-level wheel member: {member}")

    missing = sorted(REQUIRED_WHEEL_MEMBERS - set(members))
    errors.extend(f"Missing required wheel member: {member}" for member in missing)
    metadata = {
        "wheel": str(wheel_path),
        "member_count": len(members),
        "required_count": len(REQUIRED_WHEEL_MEMBERS),
        "missing": missing,
    }
    return ReleaseGateStep(
        name="wheel_contents_audit",
        ok=not errors,
        detail="wheel contains only the allowed DataForge package surface"
        if not errors
        else "wheel contents audit failed",
        metadata={**metadata, "errors": errors},
    )


def _strip_sdist_root(member: str) -> str:
    """Return an sdist member path without the archive root directory."""
    parts = member.split("/", 1)
    if len(parts) == 2 and parts[0].startswith(f"{PACKAGE_NAME}-"):
        return parts[1]
    return member


def _audit_sdist_contents(sdist_path: Path) -> ReleaseGateStep:
    """Verify the source distribution contains only release-owned core assets."""
    errors: list[str] = []
    with tarfile.open(sdist_path, "r:gz") as archive:
        members = sorted(
            _strip_sdist_root(info.name) for info in archive.getmembers() if info.isfile()
        )

    for member in members:
        parts = set(Path(member).parts)
        top_level = member.split("/", 1)[0]
        if any(member.startswith(prefix) for prefix in REJECTED_SDIST_PREFIXES):
            errors.append(f"Rejected path prefix: {member}")
        if parts & REJECTED_SDIST_PARTS:
            errors.append(f"Rejected generated/cache path: {member}")
        if member.endswith((".pyc", ".pyo", ".ipynb")):
            errors.append(f"Rejected generated or notebook file: {member}")
        if top_level not in ALLOWED_SDIST_TOP_LEVEL:
            errors.append(f"Unexpected top-level sdist member: {member}")
        if member.startswith(f"{PACKAGE_NAME}.egg-info/"):
            egg_info_name = member.split("/", 1)[1]
            if egg_info_name not in ALLOWED_SDIST_EGG_INFO:
                errors.append(f"Unexpected egg-info member: {member}")

    missing = sorted(REQUIRED_SDIST_MEMBERS - set(members))
    errors.extend(f"Missing required sdist member: {member}" for member in missing)
    metadata = {
        "sdist": str(sdist_path),
        "member_count": len(members),
        "required_count": len(REQUIRED_SDIST_MEMBERS),
        "missing": missing,
    }
    return ReleaseGateStep(
        name="sdist_contents_audit",
        ok=not errors,
        detail="sdist contains only the allowed DataForge core source surface"
        if not errors
        else "sdist contents audit failed",
        metadata={**metadata, "errors": errors},
    )


def _json_step(
    name: str,
    result: subprocess.CompletedProcess[str],
    *,
    expected: dict[str, Any],
) -> ReleaseGateStep:
    """Validate a command emitted JSON containing expected key/value pairs."""
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return ReleaseGateStep(
            name=name,
            ok=False,
            detail=f"Could not parse JSON output: {exc}",
            metadata={"stdout_tail": _tail(result.stdout), "stderr_tail": _tail(result.stderr)},
        )
    errors = [
        f"Expected {key}={value!r}, found {payload.get(key)!r}"
        for key, value in expected.items()
        if payload.get(key) != value
    ]
    return ReleaseGateStep(
        name=name,
        ok=not errors,
        detail="JSON contract matched" if not errors else "JSON contract mismatch",
        metadata={"expected": expected, "errors": errors, "keys": sorted(payload)},
    )


def _warehouse_dry_run_contract_step(result: subprocess.CompletedProcess[str]) -> ReleaseGateStep:
    """Validate the installed warehouse dry-run contract without credentials."""
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return ReleaseGateStep(
            name="smoke_warehouse_cloud_dry_run_contract",
            ok=False,
            detail=f"Could not parse JSON output: {exc}",
            metadata={"stdout_tail": _tail(result.stdout), "stderr_tail": _tail(result.stderr)},
        )

    plan = payload.get("patch_plan")
    errors: list[str] = []
    if payload.get("schema_version") != "table_store_repair_result_v1":
        errors.append("warehouse dry-run result schema_version mismatch")
    if payload.get("backend") != "snowflake":
        errors.append(f"expected backend='snowflake', found {payload.get('backend')!r}")
    if payload.get("mode") != "dry_run":
        errors.append(f"expected mode='dry_run', found {payload.get('mode')!r}")
    if not isinstance(plan, dict):
        errors.append("patch_plan is missing or is not an object")
    else:
        if plan.get("schema_version") != "patch_plan_v1":
            errors.append("patch_plan schema_version mismatch")
        if plan.get("apply_supported") is not False:
            errors.append("cloud warehouse dry-run must not claim apply support")
        if plan.get("reversible") is not False:
            errors.append("cloud warehouse dry-run must not claim reversibility")
        if plan.get("operations") != []:
            errors.append("cloud warehouse dry-run must not emit write operations")
    return ReleaseGateStep(
        name="smoke_warehouse_cloud_dry_run_contract",
        ok=not errors,
        detail="warehouse dry-run contract matched"
        if not errors
        else "warehouse dry-run contract mismatch",
        metadata={
            "errors": errors,
            "top_level_keys": sorted(payload) if isinstance(payload, dict) else [],
            "patch_plan_keys": sorted(plan) if isinstance(plan, dict) else [],
        },
    )


def _copy_packaged_fixtures(venv_python: Path, smoke_dir: Path, *, cwd: Path) -> ReleaseGateStep:
    """Copy demo fixtures from the installed wheel into a mutable smoke directory."""
    script = (
        "from importlib.resources import files\n"
        "from pathlib import Path\n"
        "import shutil, sys\n"
        "target = Path(sys.argv[1])\n"
        "target.mkdir(parents=True, exist_ok=True)\n"
        "fixtures = files('dataforge') / 'fixtures'\n"
        "shutil.copyfile(fixtures / 'hospital_10rows.csv', target / 'hospital_10rows.csv')\n"
        "shutil.copyfile(fixtures / 'hospital_schema.yaml', target / 'hospital_schema.yaml')\n"
        # The repair lifecycle below runs on this pair rather than the hospital one. Until
        # 2026-08-25 it ran on hospital_10rows, whose only auto-appliable fix came from
        # `type_mismatch` -- the one allowlist member that had never been measured, and which
        # has since been removed from the calibration bypass for that reason. The gate was
        # therefore smoke-testing the write path through the least-evidenced detector in the
        # product. `state -> city` on this fixture is a declared dependency with a 9-to-1
        # majority, so the write comes from `fd_violation`, whose write precision is measured
        # in `docs/trust/bypass-allowlist-evidence.md`.
        "shutil.copyfile(fixtures / 'premised_fd_10rows.csv', target / 'premised_fd_10rows.csv')\n"
        "shutil.copyfile("
        "fixtures / 'premised_fd_10rows.schema.yaml', target / 'premised_fd_10rows.schema.yaml'"
        ")\n"
    )
    step, _result = _run_command(
        "copy_packaged_fixtures",
        [venv_python, "-c", script, smoke_dir],
        cwd=cwd,
        timeout_seconds=30,
    )
    return step


def _append_step(steps: list[ReleaseGateStep], step: ReleaseGateStep) -> bool:
    """Append a step and return whether the gate can continue."""
    steps.append(step)
    return step.ok


def _preflight_commands() -> list[tuple[str, list[str | os.PathLike[str]], int]]:
    """Return the truth checks that must pass before anything is built or published.

    Module-level and returned as data so the population is testable. It was previously a
    local list inside :func:`run_release_gate`, which meant no test could assert what the
    release gate actually verifies -- and what it verified was narrower than it appeared.

    ``docs_truth`` was added 2026-08-29. Until then only ``readme_truth`` ran here.
    The two police different things: ``readme_truth`` checks the write-authority allowlist
    in documents, while ``docs_truth`` binds every quantitative claim to a committed
    artifact. So a release could be cut with 80 published numbers unverified, and the
    numbers *are* the product's claim -- ``PRODUCT.md`` leads with "verifiable", not with
    accuracy.

    Ordering is deliberate: these run before ``build_sdist_and_wheel``, and
    :func:`_append_step` returns ``False`` on the first failure, so a false claim aborts
    the gate rather than being reported alongside a built artifact.
    """
    return [
        (
            "release_doctor_core",
            [sys.executable, "-m", "dataforge.release.doctor", "--core", "--json"],
            60,
        ),
        (
            "readme_truth",
            [sys.executable, "scripts/ci/readme_truth.py"],
            60,
        ),
        (
            "docs_truth",
            [sys.executable, "scripts/ci/docs_truth.py", "--check"],
            120,
        ),
    ]


def run_release_gate(*, keep_artifacts: bool = False) -> ReleaseGateReport:
    """Run the complete local release gate."""
    project_root = _project_root()
    steps: list[ReleaseGateStep] = []
    artifact_hashes: dict[str, str] = {}

    with tempfile.TemporaryDirectory(prefix="dataforge-release-gate-") as temp_name:
        temp_root = Path(temp_name)
        dist_dir = temp_root / "dist"
        wheelhouse_dir = temp_root / "wheelhouse"
        smoke_dir = temp_root / "smoke"
        venv_path = temp_root / ".venv"
        dist_dir.mkdir()
        wheelhouse_dir.mkdir()
        smoke_dir.mkdir()

        preflight_commands = _preflight_commands()
        for name, command, timeout_seconds in preflight_commands:
            step, _result = _run_command(
                name,
                command,
                cwd=project_root,
                timeout_seconds=timeout_seconds,
            )
            if not _append_step(steps, step):
                return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, _result = _run_command(
            "build_sdist_and_wheel",
            [sys.executable, "-m", "build", "--sdist", "--wheel", "--outdir", dist_dir],
            cwd=project_root,
            timeout_seconds=180,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        try:
            wheel_path = _find_single_artifact(dist_dir, f"{PACKAGE_NAME}-*.whl")
            sdist_path = _find_single_artifact(dist_dir, f"{PACKAGE_NAME}-*.tar.gz")
        except ReleaseGateError as exc:
            steps.append(
                ReleaseGateStep(
                    name="find_built_artifacts",
                    ok=False,
                    detail=str(exc),
                    metadata={"dist_dir": str(dist_dir)},
                )
            )
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        artifact_hashes[wheel_path.name] = _file_sha256(wheel_path)
        artifact_hashes[sdist_path.name] = _file_sha256(sdist_path)
        if not _append_step(steps, _audit_wheel_contents(wheel_path)):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)
        if not _append_step(steps, _audit_sdist_contents(sdist_path)):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        shutil.copy2(wheel_path, wheelhouse_dir / wheel_path.name)
        step, _result = _run_command(
            "build_dependency_wheelhouse",
            [sys.executable, "-m", "pip", "download", "--dest", wheelhouse_dir, wheel_path],
            cwd=project_root,
            timeout_seconds=180,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, _result = _run_command(
            "create_clean_venv",
            [sys.executable, "-m", "venv", venv_path],
            cwd=temp_root,
            timeout_seconds=90,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        venv_python = _python_in_venv(venv_path)
        dataforge = _script_in_venv(venv_path, "dataforge")
        dataforge15 = _script_in_venv(venv_path, "dataforge15")
        step, _result = _run_command(
            "offline_wheel_install",
            [
                venv_python,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--find-links",
                wheelhouse_dir,
                PACKAGE_NAME,
            ],
            cwd=temp_root,
            timeout_seconds=180,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        import_script = (
            "import importlib.util\n"
            "from pathlib import Path\n"
            f"project_root = Path({str(project_root)!r}).resolve()\n"
            "dataforge_spec = importlib.util.find_spec('dataforge')\n"
            "assert dataforge_spec is not None\n"
            "origin = Path(str(dataforge_spec.origin)).resolve()\n"
            "assert project_root not in (origin, *origin.parents), origin\n"
            "assert importlib.util.find_spec('dataforge') is not None\n"
            "assert importlib.util.find_spec('data_quality_env') is None\n"
        )
        step, _result = _run_command(
            "installed_import_boundaries",
            [venv_python, "-c", import_script],
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        for cli_name, cli_path in (("dataforge", dataforge), ("dataforge15", dataforge15)):
            step, _result = _run_command(
                f"{cli_name}_cli_alias",
                [cli_path, "--version"],
                cwd=temp_root,
                timeout_seconds=30,
            )
            if not _append_step(steps, step):
                return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, _result = _run_command(
            "installed_release_doctor_core",
            [dataforge, "release", "doctor", "--core", "--json"],
            cwd=temp_root,
            timeout_seconds=60,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        if not _append_step(steps, _copy_packaged_fixtures(venv_python, smoke_dir, cwd=temp_root)):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, _result = _run_command(
            "packaged_fixture_alias_profile",
            [
                dataforge,
                "profile",
                "fixtures/hospital_10rows.csv",
                "--schema",
                "fixtures/hospital_schema.yaml",
                "--json",
            ],
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        # Deliberately the premised-FD pair, not the hospital pair: the lifecycle must
        # exercise a detector with a committed write measurement. See _copy_packaged_fixtures.
        source_path = smoke_dir / "premised_fd_10rows.csv"
        schema_path = smoke_dir / "premised_fd_10rows.schema.yaml"
        constraints_path = smoke_dir / "constraints.json"
        original_sha256 = _file_sha256(source_path)
        lifecycle_commands: list[tuple[str, list[str | os.PathLike[str]]]] = [
            (
                "smoke_profile",
                [dataforge, "profile", source_path, "--schema", schema_path, "--json"],
            ),
            (
                "smoke_profile_constraints_artifact",
                [
                    dataforge,
                    "profile",
                    source_path,
                    "--constraints-out",
                    constraints_path,
                    "--json",
                ],
            ),
            (
                "smoke_constraints_review_no_tui",
                [dataforge, "constraints", "review", constraints_path, "--no-tui", "--json"],
            ),
            (
                "smoke_repair_dry_run",
                [
                    dataforge,
                    "repair",
                    source_path,
                    "--schema",
                    schema_path,
                    "--dry-run",
                    "--json",
                ],
            ),
            (
                "smoke_watch_once",
                [dataforge, "watch", source_path, "--schema", schema_path, "--once", "--json"],
            ),
        ]
        for name, command in lifecycle_commands:
            step, _result = _run_command(name, command, cwd=temp_root, timeout_seconds=30)
            if not _append_step(steps, step):
                return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, warehouse_dry_run_result = _run_command(
            "smoke_warehouse_cloud_dry_run",
            [
                dataforge,
                "repair",
                "warehouse://snowflake?relation=PUBLIC.HOSPITALS&row_id=id",
                "--dry-run",
                "--json",
            ],
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)
        if not _append_step(steps, _warehouse_dry_run_contract_step(warehouse_dry_run_result)):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, apply_result = _run_command(
            "smoke_repair_apply",
            [dataforge, "repair", source_path, "--schema", schema_path, "--apply", "--json"],
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)
        try:
            apply_payload = json.loads(apply_result.stdout)
            txn_id = str(apply_payload["receipt"]["txn_id"])
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            steps.append(
                ReleaseGateStep(
                    name="smoke_apply_receipt_contract",
                    ok=False,
                    detail=f"Could not read txn_id from apply receipt: {exc}",
                    metadata={"stdout_tail": _tail(apply_result.stdout)},
                )
            )
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        audit_command: list[str | os.PathLike[str]] = [
            dataforge,
            "audit",
            txn_id,
            "--search-root",
            smoke_dir,
            "--json",
        ]
        step, audit_result = _run_command(
            "smoke_audit_applied",
            audit_command,
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)
        if not _append_step(
            steps,
            _json_step(
                "smoke_audit_applied_contract", audit_result, expected={"verdict": "verified"}
            ),
        ):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, revert_result = _run_command(
            "smoke_revert",
            [dataforge, "revert", txn_id, "--search-root", smoke_dir, "--json"],
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)
        if not _append_step(
            steps,
            _json_step("smoke_revert_contract", revert_result, expected={"txn_id": txn_id}),
        ):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        final_sha256 = _file_sha256(source_path)
        byte_identity_errors: list[str] = []
        if final_sha256 != original_sha256:
            byte_identity_errors.append(
                f"Expected reverted SHA-256 {original_sha256}, found {final_sha256}."
            )
        if not _append_step(
            steps,
            ReleaseGateStep(
                name="smoke_revert_byte_identity",
                ok=not byte_identity_errors,
                detail="reverted file bytes match original bytes"
                if not byte_identity_errors
                else "reverted bytes differ",
                metadata={
                    "original_sha256": original_sha256,
                    "final_sha256": final_sha256,
                    "errors": byte_identity_errors,
                },
            ),
        ):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        step, final_audit_result = _run_command(
            "smoke_audit_reverted",
            audit_command,
            cwd=temp_root,
            timeout_seconds=30,
        )
        if not _append_step(steps, step):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)
        if not _append_step(
            steps,
            _json_step(
                "smoke_audit_reverted_contract",
                final_audit_result,
                expected={"verdict": "verified"},
            ),
        ):
            return ReleaseGateReport(ok=False, steps=steps, artifact_sha256=artifact_hashes)

        if keep_artifacts:
            kept_dir = project_root / "dist" / "release-gate-latest"
            kept_dir.parent.mkdir(exist_ok=True)
            if kept_dir.exists():
                shutil.rmtree(kept_dir)
            shutil.copytree(temp_root, kept_dir)
            steps.append(
                ReleaseGateStep(
                    name="keep_artifacts",
                    ok=True,
                    detail="release gate artifacts copied",
                    metadata={"path": str(kept_dir)},
                )
            )

    return ReleaseGateReport(
        ok=all(step.ok for step in steps), steps=steps, artifact_sha256=artifact_hashes
    )


def main(argv: list[str] | None = None) -> int:
    """Script entrypoint used by CI and local release work."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Copy the temporary gate workspace to dist/release-gate-latest.",
    )
    args = parser.parse_args(argv)
    report = run_release_gate(keep_artifacts=args.keep_artifacts)
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for step in report.steps:
            status = "ok" if step.ok else "fail"
            print(f"{status:4} {step.name}: {step.detail}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
