"""Tests for the canonical release gate contracts."""

from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

from dataforge.release.gate import (
    REQUIRED_SDIST_MEMBERS,
    REQUIRED_WHEEL_MEMBERS,
    SCHEMA_VERSION,
    ReleaseGateReport,
    _audit_sdist_contents,
    _audit_wheel_contents,
)


def _write_wheel(path: Path, members: set[str]) -> None:
    """Write a minimal wheel-like zip for content-audit tests."""
    with zipfile.ZipFile(path, "w") as archive:
        for member in sorted(members):
            archive.writestr(member, "")


def _write_sdist(path: Path, members: set[str]) -> None:
    """Write a minimal sdist-like tarball for content-audit tests."""
    with tarfile.open(path, "w:gz") as archive:
        for member in sorted(members):
            payload = b""
            info = tarfile.TarInfo(f"dataforge_07-0.1.0/{member}")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def test_release_gate_report_schema_is_versioned() -> None:
    report = ReleaseGateReport(ok=True, steps=[])

    payload = report.to_dict()

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["offline_install"] is True
    assert payload["secrets_printed"] is False


def test_wheel_contents_audit_accepts_allowed_surface(tmp_path: Path) -> None:
    wheel_path = tmp_path / "dataforge_07-0.1.0-py3-none-any.whl"
    members = set(REQUIRED_WHEEL_MEMBERS) | {
        "dataforge/__init__.py",
        "dataforge_07-0.1.0.dist-info/METADATA",
        "dataforge_07-0.1.0.dist-info/WHEEL",
        "dataforge_07-0.1.0.dist-info/RECORD",
    }
    _write_wheel(wheel_path, members)

    step = _audit_wheel_contents(wheel_path)

    assert step.ok is True


def test_wheel_contents_audit_rejects_non_package_files(tmp_path: Path) -> None:
    wheel_path = tmp_path / "dataforge_07-0.1.0-py3-none-any.whl"
    members = set(REQUIRED_WHEEL_MEMBERS) | {
        "dataforge/__init__.py",
        "dataforge_07-0.1.0.dist-info/METADATA",
        "tests/test_leaked.py",
        "data_quality_env/legacy.py",
        "root_script.py",
    }
    _write_wheel(wheel_path, members)

    step = _audit_wheel_contents(wheel_path)

    assert step.ok is False
    assert step.metadata["errors"]


def test_sdist_contents_audit_accepts_allowed_surface(tmp_path: Path) -> None:
    sdist_path = tmp_path / "dataforge_07-0.1.0.tar.gz"
    members = set(REQUIRED_SDIST_MEMBERS) | {
        "dataforge_07.egg-info/PKG-INFO",
        "dataforge_07.egg-info/SOURCES.txt",
        "dataforge_07.egg-info/requires.txt",
        "dataforge_07.egg-info/entry_points.txt",
        "dataforge_07.egg-info/top_level.txt",
        "dataforge_07.egg-info/dependency_links.txt",
    }
    _write_sdist(sdist_path, members)

    step = _audit_sdist_contents(sdist_path)

    assert step.ok is True


def test_sdist_contents_audit_rejects_legacy_and_generated_files(tmp_path: Path) -> None:
    sdist_path = tmp_path / "dataforge_07-0.1.0.tar.gz"
    members = {
        "PKG-INFO",
        "README.md",
        "LICENSE",
        "MANIFEST.in",
        "pyproject.toml",
        "dataforge/__init__.py",
        "dataforge/py.typed",
        "dataforge/cli/constraints.py",
        "dataforge/cli/profile.py",
        "dataforge/cli/repair.py",
        "dataforge/fixtures/hospital_10rows.csv",
        "dataforge/fixtures/hospital_schema.yaml",
        "data_quality_env/legacy.py",
        "tests/test_leaked.py",
        "benchmark.py",
        "training/kaggle/sft_warmup.ipynb",
        "dataforge/__pycache__/leaked.pyc",
    }
    _write_sdist(sdist_path, members)

    step = _audit_sdist_contents(sdist_path)

    assert step.ok is False
    assert step.metadata["errors"]
