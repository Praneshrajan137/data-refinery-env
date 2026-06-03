"""Tests for PyPI publication evidence generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts.ci import pypi_publish_report


def _project_payload(package: str) -> dict[str, Any]:
    return {
        "info": {"version": "0.1.0"},
        "releases": {
            "0.1.0": [
                {
                    "filename": f"{package}-0.1.0-py3-none-any.whl",
                    "digests": {"sha256": "a" * 64},
                },
                {
                    "filename": f"{package}-0.1.0.tar.gz",
                    "digests": {"sha256": "b" * 64},
                },
            ]
        },
    }


def _simple_payload(package: str) -> dict[str, Any]:
    return {
        "files": [
            {
                "filename": f"{package}-0.1.0-py3-none-any.whl",
                "provenance": (
                    f"https://pypi.org/integrity/{package}/0.1.0/"
                    f"{package}-0.1.0-py3-none-any.whl/provenance"
                ),
            }
        ]
    }


def test_generate_publish_report_uses_public_metadata_and_smoke_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The report is generated only from public metadata plus stored smoke logs."""
    evidence_root = tmp_path / "evidence"
    smoke_dir = evidence_root / "pypi"
    smoke_dir.mkdir(parents=True)
    for package in pypi_publish_report.PACKAGES:
        (smoke_dir / f"{package}-testpypi-smoke.json").write_text("{}", encoding="utf-8")
        (smoke_dir / f"{package}-pypi-smoke.json").write_text("{}", encoding="utf-8")

    def fake_fetch_json(url: str) -> dict[str, Any]:
        for package in pypi_publish_report.PACKAGES:
            if f"/{package}/json" in url:
                return _project_payload(package)
            if f"/{package.replace('_', '-')}/" in url:
                return _simple_payload(package)
        raise AssertionError(url)

    monkeypatch.setattr(pypi_publish_report, "_fetch_json", fake_fetch_json)
    report = pypi_publish_report.generate_report(
        version="0.1.0",
        workflow_run_url="https://github.com/Aegis15/dataforge/actions/runs/1",
        evidence_root=evidence_root,
        smoke_log_dir="pypi",
    )

    assert report.schema_version == "dataforge_pypi_publish_report_v1"
    assert {item.name for item in report.packages} == set(pypi_publish_report.PACKAGES)
    assert all(item.trusted_publishing for item in report.packages)
    assert all(item.attestations for item in report.packages)
    assert all(item.wheel_sha256 == "a" * 64 for item in report.packages)
    assert all(item.sdist_sha256 == "b" * 64 for item in report.packages)


def test_generate_publish_report_refuses_missing_smoke_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing smoke logs keep publication evidence from being generated."""

    def fake_fetch_json(url: str) -> dict[str, Any]:
        package = pypi_publish_report.PACKAGES[0]
        if "/json" in url:
            return _project_payload(package)
        return _simple_payload(package)

    monkeypatch.setattr(pypi_publish_report, "_fetch_json", fake_fetch_json)

    with pytest.raises(RuntimeError, match="Smoke log does not exist"):
        pypi_publish_report.generate_report(
            version="0.1.0",
            workflow_run_url="https://github.com/Aegis15/dataforge/actions/runs/1",
            evidence_root=tmp_path / "evidence",
            smoke_log_dir="pypi",
        )
