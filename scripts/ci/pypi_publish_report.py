"""Generate PyPI/TestPyPI publication evidence after real package publication."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

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
PUBLISH_ATTESTATION_PREDICATE = "https://docs.pypi.org/attestations/publish/v1"
# Historical, not a link -- see the long note at the same constant in
# dataforge/release/full_vision.py. This is the OIDC publisher identity inside the
# attestations for the released 0.1.0 artifacts, so it must keep naming the repository as it
# stood at publication even though the repo has since been renamed and every [project.urls]
# entry now points at the new owner.
EXPECTED_PUBLISHER_REPOSITORY = "Aegis15/dataforge"
EXPECTED_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
PYPI_WORKFLOWS = {
    "dataforge_07": "publish-dataforge.yml",
    "dataforge_07_mcp": "publish-dataforge-mcp.yml",
    "dataforge_07_evals": "publish-dataforge-evals.yml",
    "dataforge_07_dbt": "publish-dataforge-dbt.yml",
    "dataforge_07_agent_patterns": "publish-dataforge-agent-patterns.yml",
}
TESTPYPI_WORKFLOWS = {
    "dataforge_07": "publish-testpypi.yml",
    "dataforge_07_mcp": "publish-dataforge-mcp-testpypi.yml",
    "dataforge_07_evals": "publish-dataforge-evals-testpypi.yml",
    "dataforge_07_dbt": "publish-dataforge-dbt-testpypi.yml",
    "dataforge_07_agent_patterns": "publish-dataforge-agent-patterns-testpypi.yml",
}
_GITHUB_WORKFLOW_IDENTITY = re.compile(
    r"^https://github\.com/(?P<repository>[^/]+/[^/]+)/\.github/workflows/"
    r"(?P<workflow>[^@]+)@(?P<ref>.+)$"
)


@dataclass(frozen=True, slots=True)
class PublisherEvidence:
    """Trusted Publisher identity extracted from PyPI provenance."""

    repository: str
    workflow: str
    ref: str
    identity: str
    oidc_issuer: str


@dataclass(frozen=True, slots=True)
class DistributionEvidence:
    """One wheel or sdist file's registry and provenance evidence."""

    filename: str
    package_type: str
    download_url: str
    sha256: str
    upload_time_iso_8601: str
    provenance_url: str
    integrity_predicate_type: str
    integrity_subject_sha256: str
    trusted_publisher: PublisherEvidence


@dataclass(frozen=True, slots=True)
class IndexEvidence:
    """One package's evidence on a single package index."""

    index: str
    project_url: str
    wheel: DistributionEvidence
    sdist: DistributionEvidence


@dataclass(frozen=True, slots=True)
class PackageEvidence:
    """One package's publication evidence."""

    name: str
    version: str
    trusted_publishing: bool
    attestations: bool
    testpypi_fresh_install: bool
    pypi_fresh_install: bool
    workflow_run_url: str
    testpypi_smoke_log_path: str
    pypi_smoke_log_path: str
    testpypi: IndexEvidence
    pypi: IndexEvidence


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
            "Accept": (
                "application/vnd.pypi.integrity.v1+json, "
                "application/vnd.pypi.simple.v1+json, application/json"
            ),
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


def _fetch_bytes(url: str) -> bytes:
    """Fetch bytes from a public package file URL."""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "dataforge-pypi-publish-report"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{url} returned HTTP {exc.code}") from exc
    if not isinstance(payload, bytes):
        raise RuntimeError(f"{url} did not return bytes")
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


def _is_sha256(value: str) -> bool:
    """Return whether a value is a SHA-256 hex digest."""
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value.lower())


def _require_https(value: str, *, field: str) -> str:
    """Require a field to contain an HTTPS URL."""
    if not value.startswith("https://"):
        raise RuntimeError(f"{field} must be an HTTPS URL")
    return value


def _release_file(payload: dict[str, Any], package: str, version: str, kind: str) -> dict[str, Any]:
    """Return one wheel or sdist release file from project JSON."""
    files = payload.get("releases", {}).get(version)
    if not isinstance(files, list) or not files:
        raise RuntimeError(f"{package} has no files for version {version}")

    matches: list[dict[str, Any]] = []
    for item in files:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename", ""))
        package_type = str(item.get("packagetype", ""))
        if kind == "wheel" and (filename.endswith(".whl") or package_type == "bdist_wheel"):
            matches.append(item)
        if kind == "sdist" and (filename.endswith((".tar.gz", ".zip")) or package_type == "sdist"):
            matches.append(item)
    if len(matches) != 1:
        raise RuntimeError(
            f"{package} {version} must have exactly one {kind}, found {len(matches)}"
        )
    return matches[0]


def _provenance_url(simple_payload: dict[str, Any], package: str, filename: str) -> str:
    """Return the public provenance URL for a package release file."""
    files = simple_payload.get("files")
    if not isinstance(files, list):
        raise RuntimeError(f"{package} simple JSON has no files list")
    for item in files:
        if not isinstance(item, dict):
            continue
        if str(item.get("filename", "")) != filename:
            continue
        provenance = str(item.get("provenance", ""))
        if provenance.startswith("https://"):
            return provenance
        raise RuntimeError(f"{package} file {filename} has no public provenance URL")
    raise RuntimeError(f"{package} simple JSON does not list {filename}")


def _decode_base64_json(value: str) -> dict[str, Any]:
    """Decode a base64 or base64url JSON payload."""
    padded = value + ("=" * ((4 - len(value) % 4) % 4))
    decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
    payload = json.loads(decoded.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Attestation statement did not decode to a JSON object")
    return payload


def _der_string(value: bytes) -> str:
    """Decode the small DER string values used by Fulcio custom extensions."""
    if not value:
        return ""
    tag = value[0]
    if tag not in (0x0C, 0x13, 0x16):
        return value.decode("utf-8", errors="replace").strip("\x00")
    if len(value) < 2:
        return ""
    length_byte = value[1]
    if length_byte & 0x80:
        length_length = length_byte & 0x7F
        start = 2 + length_length
        length = int.from_bytes(value[2:start], "big")
    else:
        start = 2
        length = length_byte
    return value[start : start + length].decode("utf-8", errors="replace")


def _certificate_publisher(certificate: str) -> PublisherEvidence:
    """Extract the GitHub Trusted Publisher identity from a Fulcio certificate."""
    try:
        from cryptography import x509
        from cryptography.x509.oid import ExtensionOID, ObjectIdentifier
    except ImportError as exc:  # pragma: no cover - release environment guard
        raise RuntimeError(
            "cryptography is required to verify Trusted Publisher certificate identity"
        ) from exc

    raw_certificate = certificate.strip()
    if raw_certificate.startswith("-----BEGIN CERTIFICATE-----"):
        cert = x509.load_pem_x509_certificate(raw_certificate.encode("utf-8"))
    else:
        cert = x509.load_der_x509_certificate(base64.b64decode(raw_certificate))

    try:
        san = cast(
            x509.SubjectAlternativeName,
            cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value,
        )
        identities = san.get_values_for_type(x509.UniformResourceIdentifier)
    except x509.ExtensionNotFound:
        identities = []
    identity = next(
        (value for value in identities if value.startswith("https://github.com/")),
        "",
    )

    def extension_text(oid: str) -> str:
        try:
            value = cert.extensions.get_extension_for_oid(ObjectIdentifier(oid)).value
        except x509.ExtensionNotFound:
            return ""
        raw_value = getattr(value, "value", b"")
        return _der_string(raw_value if isinstance(raw_value, bytes) else b"")

    issuer = extension_text("1.3.6.1.4.1.57264.1.1")
    repository = extension_text("1.3.6.1.4.1.57264.1.5")
    ref = extension_text("1.3.6.1.4.1.57264.1.6")
    workflow = ""
    match = _GITHUB_WORKFLOW_IDENTITY.match(identity)
    if match:
        repository = repository or match.group("repository")
        workflow = match.group("workflow")
        ref = ref or match.group("ref")
    return PublisherEvidence(
        repository=repository,
        workflow=workflow,
        ref=ref,
        identity=identity,
        oidc_issuer=issuer,
    )


def _direct_publisher(payload: dict[str, Any]) -> PublisherEvidence | None:
    """Read test-friendly direct publisher metadata if present."""
    raw = payload.get("trusted_publisher")
    if not isinstance(raw, dict):
        return None
    return PublisherEvidence(
        repository=str(raw.get("repository", "")),
        workflow=str(raw.get("workflow", "")),
        ref=str(raw.get("ref", "")),
        identity=str(raw.get("identity", "")),
        oidc_issuer=str(raw.get("oidc_issuer", "")),
    )


def _publisher_from_attestation(
    provenance_payload: dict[str, Any], attestation: dict[str, Any]
) -> PublisherEvidence:
    """Extract publisher evidence from direct metadata or certificate material."""
    direct = _direct_publisher(attestation) or _direct_publisher(provenance_payload)
    if direct is not None:
        return direct
    verification_material = attestation.get("verification_material", {})
    if not isinstance(verification_material, dict):
        raise RuntimeError("Attestation is missing verification_material")
    certificate = str(verification_material.get("certificate", ""))
    if not certificate:
        raise RuntimeError("Attestation is missing a signing certificate")
    return _certificate_publisher(certificate)


def _validate_publisher(
    publisher: PublisherEvidence, *, package: str, expected_workflow: str
) -> PublisherEvidence:
    """Require the Trusted Publisher identity expected for this package."""
    if publisher.repository != EXPECTED_PUBLISHER_REPOSITORY:
        raise RuntimeError(
            f"{package}: Trusted Publisher repository is {publisher.repository!r}, "
            f"expected {EXPECTED_PUBLISHER_REPOSITORY!r}"
        )
    if publisher.workflow != expected_workflow:
        raise RuntimeError(
            f"{package}: Trusted Publisher workflow is {publisher.workflow!r}, "
            f"expected {expected_workflow!r}"
        )
    if publisher.oidc_issuer != EXPECTED_OIDC_ISSUER:
        raise RuntimeError(
            f"{package}: Trusted Publisher OIDC issuer is {publisher.oidc_issuer!r}, "
            f"expected {EXPECTED_OIDC_ISSUER!r}"
        )
    if publisher.identity and expected_workflow not in publisher.identity:
        raise RuntimeError(
            f"{package}: Trusted Publisher identity {publisher.identity!r} does not "
            f"reference {expected_workflow!r}"
        )
    return publisher


def _verify_provenance(
    *,
    package: str,
    filename: str,
    sha256: str,
    provenance_url: str,
    expected_workflow: str,
) -> tuple[str, str, PublisherEvidence]:
    """Verify a file provenance object binds the expected filename, hash, and publisher."""
    payload = _fetch_json(provenance_url)
    bundles = payload.get("attestation_bundles")
    if not isinstance(bundles, list):
        raise RuntimeError(f"{package} provenance for {filename} has no attestation bundles")
    for bundle in bundles:
        if not isinstance(bundle, dict):
            continue
        attestations = bundle.get("attestations", [])
        if not isinstance(attestations, list):
            continue
        for attestation in attestations:
            if not isinstance(attestation, dict):
                continue
            envelope = attestation.get("envelope", {})
            if not isinstance(envelope, dict):
                continue
            statement_text = str(envelope.get("statement", ""))
            if not statement_text:
                continue
            statement = _decode_base64_json(statement_text)
            predicate_type = str(statement.get("predicateType", ""))
            if predicate_type != PUBLISH_ATTESTATION_PREDICATE:
                continue
            subjects = statement.get("subject", [])
            if not isinstance(subjects, list):
                continue
            for subject in subjects:
                if not isinstance(subject, dict):
                    continue
                digest = subject.get("digest", {})
                if (
                    subject.get("name") == filename
                    and isinstance(digest, dict)
                    and str(digest.get("sha256", "")).lower() == sha256.lower()
                ):
                    publisher = _publisher_from_attestation(payload, attestation)
                    publisher = _validate_publisher(
                        publisher,
                        package=package,
                        expected_workflow=expected_workflow,
                    )
                    return predicate_type, sha256.lower(), publisher
    raise RuntimeError(f"{package} provenance for {filename} did not bind the expected hash")


def _build_distribution_evidence(
    *,
    package: str,
    release_file: dict[str, Any],
    simple_payload: dict[str, Any],
    expected_workflow: str,
) -> DistributionEvidence:
    """Build file-level registry and provenance evidence."""
    filename = str(release_file.get("filename", ""))
    package_type = str(release_file.get("packagetype", ""))
    sha256 = str(release_file.get("digests", {}).get("sha256", "")).lower()
    if not filename:
        raise RuntimeError(f"{package} release file is missing filename")
    if not _is_sha256(sha256):
        raise RuntimeError(f"{package} {filename} is missing a SHA-256 digest")
    download_url = _require_https(str(release_file.get("url", "")), field=f"{package} download_url")
    upload_time = str(release_file.get("upload_time_iso_8601", ""))
    if not upload_time:
        raise RuntimeError(f"{package} {filename} is missing upload_time_iso_8601")
    downloaded_sha256 = hashlib.sha256(_fetch_bytes(download_url)).hexdigest()
    if downloaded_sha256 != sha256:
        raise RuntimeError(
            f"{package} {filename} download hash mismatch: "
            f"got {downloaded_sha256}, expected {sha256}"
        )
    provenance_url = _provenance_url(simple_payload, package, filename)
    predicate_type, subject_sha256, publisher = _verify_provenance(
        package=package,
        filename=filename,
        sha256=sha256,
        provenance_url=provenance_url,
        expected_workflow=expected_workflow,
    )
    return DistributionEvidence(
        filename=filename,
        package_type=package_type,
        download_url=download_url,
        sha256=sha256,
        upload_time_iso_8601=upload_time,
        provenance_url=provenance_url,
        integrity_predicate_type=predicate_type,
        integrity_subject_sha256=subject_sha256,
        trusted_publisher=publisher,
    )


def _build_index_evidence(
    *,
    index: str,
    project_base_url: str,
    simple_base_url: str,
    package: str,
    version: str,
    expected_workflow: str,
) -> IndexEvidence:
    """Build all evidence for a package on one package index."""
    project_payload = _project_json(project_base_url, package)
    found_version = str(project_payload.get("info", {}).get("version", ""))
    if found_version != version:
        raise RuntimeError(f"{package} {index} version is {found_version!r}, expected {version!r}")
    simple_payload = _simple_json(simple_base_url, package)
    wheel = _build_distribution_evidence(
        package=package,
        release_file=_release_file(project_payload, package, version, "wheel"),
        simple_payload=simple_payload,
        expected_workflow=expected_workflow,
    )
    sdist = _build_distribution_evidence(
        package=package,
        release_file=_release_file(project_payload, package, version, "sdist"),
        simple_payload=simple_payload,
        expected_workflow=expected_workflow,
    )
    index_host = "https://test.pypi.org" if index == "testpypi" else "https://pypi.org"
    normalized_package = package.replace("_", "-")
    return IndexEvidence(
        index=index,
        project_url=f"{index_host}/project/{normalized_package}/",
        wheel=wheel,
        sdist=sdist,
    )


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
    testpypi = _build_index_evidence(
        index="testpypi",
        project_base_url=TESTPYPI_JSON,
        simple_base_url=TESTPYPI_SIMPLE,
        package=package,
        version=version,
        expected_workflow=TESTPYPI_WORKFLOWS[package],
    )
    pypi = _build_index_evidence(
        index="pypi",
        project_base_url=PYPI_JSON,
        simple_base_url=PYPI_SIMPLE,
        package=package,
        version=version,
        expected_workflow=PYPI_WORKFLOWS[package],
    )
    return PackageEvidence(
        name=package,
        version=version,
        trusted_publishing=True,
        attestations=True,
        testpypi_fresh_install=True,
        pypi_fresh_install=True,
        workflow_run_url=_require_https(workflow_run_url, field=f"{package} workflow_run_url"),
        testpypi_smoke_log_path=_require_log(evidence_root, testpypi_log),
        pypi_smoke_log_path=_require_log(evidence_root, pypi_log),
        testpypi=testpypi,
        pypi=pypi,
    )


def _workflow_run_urls(
    *, workflow_run_url: str | None, workflow_run_urls: dict[str, str] | None
) -> dict[str, str]:
    """Resolve package-specific workflow run URLs."""
    if workflow_run_urls:
        missing = sorted(set(PACKAGES) - set(workflow_run_urls))
        if missing:
            raise RuntimeError(f"Missing workflow run URLs for packages: {missing}")
        return {package: workflow_run_urls[package] for package in PACKAGES}
    if workflow_run_url:
        return dict.fromkeys(PACKAGES, workflow_run_url)
    raise RuntimeError("A workflow run URL is required")


def generate_report(
    *,
    version: str,
    workflow_run_url: str | None = None,
    workflow_run_urls: dict[str, str] | None = None,
    evidence_root: Path,
    smoke_log_dir: str,
) -> PublishReport:
    """Generate the complete publication report."""
    urls = _workflow_run_urls(
        workflow_run_url=workflow_run_url,
        workflow_run_urls=workflow_run_urls,
    )
    entries = [
        _build_package_evidence(
            package=package,
            version=version,
            workflow_run_url=urls[package],
            evidence_root=evidence_root,
            testpypi_log=f"{smoke_log_dir.rstrip('/')}/{package}-testpypi-smoke.json",
            pypi_log=f"{smoke_log_dir.rstrip('/')}/{package}-pypi-smoke.json",
        )
        for package in PACKAGES
    ]
    return PublishReport(schema_version="dataforge_pypi_publish_report_v2", packages=entries)


def _parse_workflow_run_urls(values: list[str]) -> tuple[str | None, dict[str, str] | None]:
    """Parse --workflow-run-url values."""
    if len(values) == 1 and "=" not in values[0]:
        return values[0], None
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise RuntimeError(
                "Use either one shared --workflow-run-url URL or repeat "
                "--workflow-run-url PACKAGE=URL for every package"
            )
        package, url = value.split("=", 1)
        if package not in PACKAGES:
            raise RuntimeError(f"Unknown package in workflow URL mapping: {package}")
        parsed[package] = url
    return None, parsed


def main(argv: list[str] | None = None) -> int:
    """Generate and write a publication report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="0.1.0")
    parser.add_argument(
        "--workflow-run-url",
        action="append",
        required=True,
        help=(
            "Either one shared workflow URL or repeated PACKAGE=URL values for "
            "package-specific publish runs."
        ),
    )
    parser.add_argument("--evidence-root", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--smoke-log-dir", default="pypi")
    parser.add_argument(
        "--output", type=Path, default=Path("docs/evidence/pypi/publish_report.json")
    )
    args = parser.parse_args(argv)
    workflow_run_url, workflow_run_urls = _parse_workflow_run_urls(args.workflow_run_url)
    report = generate_report(
        version=args.version,
        workflow_run_url=workflow_run_url,
        workflow_run_urls=workflow_run_urls,
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
