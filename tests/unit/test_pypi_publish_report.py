"""Tests for PyPI publication evidence generation."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.ci import pypi_publish_report


def _hash(index: str, package: str, kind: str) -> str:
    return hashlib.sha256(f"{index}:{package}:{kind}".encode()).hexdigest()


def _download_url(index: str, filename: str) -> str:
    return f"https://files.example/{index}/{filename}"


def _provenance_url(index: str, package: str, filename: str) -> str:
    return f"https://{index}.example/integrity/{package}/0.1.0/{filename}/provenance"


def _project_payload(package: str, index: str) -> dict[str, Any]:
    wheel = f"{package}-0.1.0-py3-none-any.whl"
    sdist = f"{package}-0.1.0.tar.gz"
    return {
        "info": {"version": "0.1.0"},
        "releases": {
            "0.1.0": [
                {
                    "filename": wheel,
                    "packagetype": "bdist_wheel",
                    "url": _download_url(index, wheel),
                    "upload_time_iso_8601": "2026-06-03T00:00:00.000000Z",
                    "digests": {"sha256": _hash(index, package, "wheel")},
                },
                {
                    "filename": sdist,
                    "packagetype": "sdist",
                    "url": _download_url(index, sdist),
                    "upload_time_iso_8601": "2026-06-03T00:01:00.000000Z",
                    "digests": {"sha256": _hash(index, package, "sdist")},
                },
            ]
        },
    }


def _simple_payload(package: str, index: str) -> dict[str, Any]:
    wheel = f"{package}-0.1.0-py3-none-any.whl"
    sdist = f"{package}-0.1.0.tar.gz"
    return {
        "files": [
            {"filename": wheel, "provenance": _provenance_url(index, package, wheel)},
            {"filename": sdist, "provenance": _provenance_url(index, package, sdist)},
        ]
    }


def _statement(filename: str, sha256: str) -> str:
    payload = {
        "predicateType": pypi_publish_report.PUBLISH_ATTESTATION_PREDICATE,
        "subject": [{"name": filename, "digest": {"sha256": sha256}}],
    }
    return base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


def _provenance_payload(package: str, index: str, filename: str) -> dict[str, Any]:
    kind = "wheel" if filename.endswith(".whl") else "sdist"
    workflow = (
        pypi_publish_report.PYPI_WORKFLOWS[package]
        if index == "pypi"
        else pypi_publish_report.TESTPYPI_WORKFLOWS[package]
    )
    sha256 = _hash(index, package, kind)
    return {
        "trusted_publisher": {
            "repository": pypi_publish_report.EXPECTED_PUBLISHER_REPOSITORY,
            "workflow": workflow,
            "ref": "refs/tags/v0.1.0",
            "identity": (
                "https://github.com/Aegis15/dataforge/.github/workflows/"
                f"{workflow}@refs/tags/v0.1.0"
            ),
            "oidc_issuer": pypi_publish_report.EXPECTED_OIDC_ISSUER,
        },
        "attestation_bundles": [
            {"attestations": [{"envelope": {"statement": _statement(filename, sha256)}}]}
        ],
    }


def _install_fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch_json(url: str) -> dict[str, Any]:
        for package in pypi_publish_report.PACKAGES:
            for index in ("pypi", "testpypi"):
                if url == _provenance_url(index, package, f"{package}-0.1.0-py3-none-any.whl"):
                    return _provenance_payload(package, index, f"{package}-0.1.0-py3-none-any.whl")
                if url == _provenance_url(index, package, f"{package}-0.1.0.tar.gz"):
                    return _provenance_payload(package, index, f"{package}-0.1.0.tar.gz")
                if f"/pypi/{package}/json" in url:
                    found_index = "testpypi" if "test.pypi.org" in url else "pypi"
                    return _project_payload(package, found_index)
                if f"/simple/{package.replace('_', '-')}/" in url:
                    found_index = "testpypi" if "test.pypi.org" in url else "pypi"
                    return _simple_payload(package, found_index)
        raise AssertionError(url)

    def fake_fetch_bytes(url: str) -> bytes:
        for package in pypi_publish_report.PACKAGES:
            for index in ("pypi", "testpypi"):
                for kind, filename in (
                    ("wheel", f"{package}-0.1.0-py3-none-any.whl"),
                    ("sdist", f"{package}-0.1.0.tar.gz"),
                ):
                    if url == _download_url(index, filename):
                        return f"{index}:{package}:{kind}".encode()
        raise AssertionError(url)

    monkeypatch.setattr(pypi_publish_report, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(pypi_publish_report, "_fetch_bytes", fake_fetch_bytes)


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

    _install_fakes(monkeypatch)
    report = pypi_publish_report.generate_report(
        version="0.1.0",
        workflow_run_url="https://github.com/Aegis15/dataforge/actions/runs/1",
        evidence_root=evidence_root,
        smoke_log_dir="pypi",
    )

    assert report.schema_version == "dataforge_pypi_publish_report_v2"
    assert {item.name for item in report.packages} == set(pypi_publish_report.PACKAGES)
    assert all(item.trusted_publishing for item in report.packages)
    assert all(item.attestations for item in report.packages)
    for item in report.packages:
        assert item.pypi.wheel.sha256 == _hash("pypi", item.name, "wheel")
        assert item.pypi.sdist.sha256 == _hash("pypi", item.name, "sdist")
        assert (
            item.testpypi.wheel.trusted_publisher.workflow
            == (pypi_publish_report.TESTPYPI_WORKFLOWS[item.name])
        )
        assert (
            item.pypi.sdist.trusted_publisher.workflow
            == (pypi_publish_report.PYPI_WORKFLOWS[item.name])
        )


def test_generate_publish_report_refuses_missing_smoke_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing smoke logs keep publication evidence from being generated."""
    _install_fakes(monkeypatch)

    with pytest.raises(RuntimeError, match="Smoke log does not exist"):
        pypi_publish_report.generate_report(
            version="0.1.0",
            workflow_run_url="https://github.com/Aegis15/dataforge/actions/runs/1",
            evidence_root=tmp_path / "evidence",
            smoke_log_dir="pypi",
        )


def test_generate_publish_report_refuses_wrong_publisher_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Provenance must identify the configured workflow for the package and index."""
    evidence_root = tmp_path / "evidence"
    smoke_dir = evidence_root / "pypi"
    smoke_dir.mkdir(parents=True)
    for package in pypi_publish_report.PACKAGES:
        (smoke_dir / f"{package}-testpypi-smoke.json").write_text("{}", encoding="utf-8")
        (smoke_dir / f"{package}-pypi-smoke.json").write_text("{}", encoding="utf-8")

    _install_fakes(monkeypatch)
    original_fetch_json = pypi_publish_report._fetch_json

    def fake_fetch_json(url: str) -> dict[str, Any]:
        payload = original_fetch_json(url)
        if "/integrity/dataforge_07/0.1.0/" in url:
            payload["trusted_publisher"]["workflow"] = "wrong.yml"
        return payload

    monkeypatch.setattr(pypi_publish_report, "_fetch_json", fake_fetch_json)
    with pytest.raises(RuntimeError, match="Trusted Publisher workflow"):
        pypi_publish_report.generate_report(
            version="0.1.0",
            workflow_run_url="https://github.com/Aegis15/dataforge/actions/runs/1",
            evidence_root=evidence_root,
            smoke_log_dir="pypi",
        )
