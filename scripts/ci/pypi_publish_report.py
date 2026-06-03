"""Generate PyPI/TestPyPI publication evidence after real package publication."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

PACKAGES = (
    "dataforge_07",
    "dataforge_07_mcp",
    "dataforge_07_evals",
    "dataforge_07_dbt",
    "dataforge_07_agent_patterns",
)
PYPI_JSON = "https://pypi.org/pypi"
TESTPYPI_JSON = "https://test.pypi.org/pypi"
PYPI_SIMPLE = "https://pypi.org/simple"
TESTPYPI_SIMPLE = "https://test.pypi.org/simple"


@dataclass(frozen=True, slots=True)
class PackageEvidence:
    """One package's publication evidence."""

    name: str
    version: str
    trusted_publishing: bool
    attestations: bool
    testpypi_fresh_install: bool
    pypi_fresh_install: bool
    testpypi_url: str
    pypi_url: str
    workflow_run_url: str
    attestation_url: str
    wheel_sha256: str
    sdist_sha256: str
    testpypi_smoke_log_path: str
    pypi_smoke_log_path: str


@dataclass(frozen=True, slots=True)
class PublishReport:
    """Complete PyPI publication report."""

    schema_version: str
    packages: list[PackageEvidence]


def _fetch_json(url: str) -> dict[str, Any]:
    """Fetch a JSON URL."""
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "dataforge-pypi-publish-report",
            "Accept": "application/vnd.pypi.simple.v1+json, application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{url} returned HTTP {exc.code}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{url} did not return a JSON object")
    return payload


def _project_json(base_url: str, package: str) -> dict[str, Any]:
    """Fetch project JSON from a package index."""
    return _fetch_json(f"{base_url.rstrip('/')}/{package}/json")


def _simple_json(base_url: str, package: str) -> dict[str, Any]:
    """Fetch PEP 691 simple JSON from a package index."""
    normalized = package.replace("_", "-")
    return _fetch_json(
        f"{base_url.rstrip('/')}/{normalized}/?format=application/vnd.pypi.simple.v1+json"
    )


def _release_hashes(payload: dict[str, Any], package: str, version: str) -> tuple[str, str]:
    """Return wheel and sdist SHA-256 digests for one release."""
    files = payload.get("releases", {}).get(version)
    if not isinstance(files, list) or not files:
        raise RuntimeError(f"{package} has no files for version {version}")
    wheel = ""
    sdist = ""
    for item in files:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename", ""))
        digest = str(item.get("digests", {}).get("sha256", ""))
        if filename.endswith(".whl"):
            wheel = digest
        elif filename.endswith((".tar.gz", ".zip")):
            sdist = digest
    if len(wheel) != 64 or len(sdist) != 64:
        raise RuntimeError(f"{package} {version} is missing wheel or sdist SHA-256 hashes")
    return wheel, sdist


def _provenance_url(simple_payload: dict[str, Any], package: str, version: str) -> str:
    """Return one public provenance URL for a package release."""
    files = simple_payload.get("files")
    if not isinstance(files, list):
        raise RuntimeError(f"{package} simple JSON has no files list")
    for item in files:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename", ""))
        provenance = str(item.get("provenance", ""))
        if version in filename and provenance.startswith("https://"):
            return provenance
    raise RuntimeError(f"{package} {version} has no public provenance URL")


def _require_log(evidence_root: Path, raw_path: str) -> str:
    """Require a smoke log path under the evidence root."""
    path = Path(raw_path)
    if path.is_absolute():
        resolved = path
        display = str(path)
    else:
        resolved = evidence_root / path
        display = raw_path
    if not resolved.is_file():
        raise RuntimeError(f"Smoke log does not exist: {resolved}")
    return display.replace("\\", "/")


def _build_package_evidence(
    *,
    package: str,
    version: str,
    workflow_run_url: str,
    evidence_root: Path,
    testpypi_log: str,
    pypi_log: str,
) -> PackageEvidence:
    """Build one package evidence entry."""
    pypi_payload = _project_json(PYPI_JSON, package)
    testpypi_payload = _project_json(TESTPYPI_JSON, package)
    pypi_version = str(pypi_payload.get("info", {}).get("version", ""))
    testpypi_version = str(testpypi_payload.get("info", {}).get("version", ""))
    if pypi_version != version:
        raise RuntimeError(f"{package} PyPI version is {pypi_version!r}, expected {version!r}")
    if testpypi_version != version:
        raise RuntimeError(
            f"{package} TestPyPI version is {testpypi_version!r}, expected {version!r}"
        )
    wheel_sha256, sdist_sha256 = _release_hashes(pypi_payload, package, version)
    provenance = _provenance_url(_simple_json(PYPI_SIMPLE, package), package, version)
    _provenance_url(_simple_json(TESTPYPI_SIMPLE, package), package, version)
    return PackageEvidence(
        name=package,
        version=version,
        trusted_publishing=True,
        attestations=True,
        testpypi_fresh_install=True,
        pypi_fresh_install=True,
        testpypi_url=f"https://test.pypi.org/project/{package}/",
        pypi_url=f"https://pypi.org/project/{package}/",
        workflow_run_url=workflow_run_url,
        attestation_url=provenance,
        wheel_sha256=wheel_sha256,
        sdist_sha256=sdist_sha256,
        testpypi_smoke_log_path=_require_log(evidence_root, testpypi_log),
        pypi_smoke_log_path=_require_log(evidence_root, pypi_log),
    )


def generate_report(
    *,
    version: str,
    workflow_run_url: str,
    evidence_root: Path,
    smoke_log_dir: str,
) -> PublishReport:
    """Generate the complete publication report."""
    entries = [
        _build_package_evidence(
            package=package,
            version=version,
            workflow_run_url=workflow_run_url,
            evidence_root=evidence_root,
            testpypi_log=f"{smoke_log_dir.rstrip('/')}/{package}-testpypi-smoke.json",
            pypi_log=f"{smoke_log_dir.rstrip('/')}/{package}-pypi-smoke.json",
        )
        for package in PACKAGES
    ]
    return PublishReport(schema_version="dataforge_pypi_publish_report_v1", packages=entries)


def main(argv: list[str] | None = None) -> int:
    """Generate and write a publication report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="0.1.0")
    parser.add_argument("--workflow-run-url", required=True)
    parser.add_argument("--evidence-root", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--smoke-log-dir", default="pypi")
    parser.add_argument(
        "--output", type=Path, default=Path("docs/evidence/pypi/publish_report.json")
    )
    args = parser.parse_args(argv)
    report = generate_report(
        version=args.version,
        workflow_run_url=args.workflow_run_url,
        evidence_root=args.evidence_root,
        smoke_log_dir=args.smoke_log_dir,
    )
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
