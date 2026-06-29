"""CI check: verify README claims match shipped code.

Asserts that every public `dataforge <subcommand>` shown in the root README
resolves to a registered Typer command. It also guards the full-vision claim
boundary: the removed domain must not appear in public release docs.

Usage:
    python scripts/ci/readme_truth.py
"""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
README = PROJECT_ROOT / "README.md"
CONTRIBUTORS = PROJECT_ROOT / "CONTRIBUTORS.md"
CLAIM_LEDGER = PROJECT_ROOT / "docs" / "claims.yaml"
EVIDENCE_LEDGER = PROJECT_ROOT / "docs" / "evidence" / "ledger.json"
PUBLISH_REPORT = PROJECT_ROOT / "docs" / "evidence" / "pypi" / "publish_report.json"
CLAIM_LEDGER_STATUSES = frozenset({"shipped", "beta", "experimental", "roadmap"})
RELEASE_TRUTH_DOCS = [
    README,
    PROJECT_ROOT / "META_CONTEXT.md",
    PROJECT_ROOT / "docs" / "docs" / "index.md",
    PROJECT_ROOT / "docs" / "docs" / "quickstart.md",
    PROJECT_ROOT / "docs" / "docs" / "architecture.md",
    PROJECT_ROOT / "docs" / "docs" / "release.md",
    PROJECT_ROOT / "dataforge-mcp" / "README.md",
    PROJECT_ROOT / "packages" / "dataforge-evals" / "README.md",
    PROJECT_ROOT / "packages" / "dataforge-dbt" / "README.md",
    PROJECT_ROOT / "packages" / "dataforge-agent-patterns" / "README.md",
]
DESIGN_PARTNER_TRUTH_DOCS = [
    README,
    CONTRIBUTORS,
    PROJECT_ROOT / "META_CONTEXT.md",
    PROJECT_ROOT / "docs" / "docs" / "index.md",
    PROJECT_ROOT / "docs" / "docs" / "architecture.md",
]
PUBLIC_CLAIM_TRUTH_DOCS = [
    README,
    PROJECT_ROOT / "docs" / "docs" / "index.md",
    PROJECT_ROOT / "docs" / "docs" / "quickstart.md",
    PROJECT_ROOT / "docs" / "docs" / "release.md",
]
CUSTOM_DOMAIN_TRUTH_DOCS = sorted(
    set(RELEASE_TRUTH_DOCS + DESIGN_PARTNER_TRUTH_DOCS + PUBLIC_CLAIM_TRUTH_DOCS)
)
PUBLISHED_DISTS = (
    "dataforge_07",
    "dataforge_07_dbt",
    "dataforge_07_evals",
    "dataforge_07_mcp",
    "dataforge_07_agent_patterns",
)
PUBLISHED_DIST_PYPROJECTS = {
    "dataforge_07": PROJECT_ROOT / "pyproject.toml",
    "dataforge_07_mcp": PROJECT_ROOT / "dataforge-mcp" / "pyproject.toml",
    "dataforge_07_evals": PROJECT_ROOT / "packages" / "dataforge-evals" / "pyproject.toml",
    "dataforge_07_dbt": PROJECT_ROOT / "packages" / "dataforge-dbt" / "pyproject.toml",
    "dataforge_07_agent_patterns": PROJECT_ROOT
    / "packages"
    / "dataforge-agent-patterns"
    / "pyproject.toml",
}
EXPECTED_PUBLISH_WORKFLOWS = {
    "dataforge_07": {"pypi": "publish-dataforge.yml", "testpypi": "publish-testpypi.yml"},
    "dataforge_07_mcp": {
        "pypi": "publish-dataforge-mcp.yml",
        "testpypi": "publish-dataforge-mcp-testpypi.yml",
    },
    "dataforge_07_evals": {
        "pypi": "publish-dataforge-evals.yml",
        "testpypi": "publish-dataforge-evals-testpypi.yml",
    },
    "dataforge_07_dbt": {
        "pypi": "publish-dataforge-dbt.yml",
        "testpypi": "publish-dataforge-dbt-testpypi.yml",
    },
    "dataforge_07_agent_patterns": {
        "pypi": "publish-dataforge-agent-patterns.yml",
        "testpypi": "publish-dataforge-agent-patterns-testpypi.yml",
    },
}
PUBLISH_ATTESTATION_PREDICATE = "https://docs.pypi.org/attestations/publish/v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
STALE_PUBLICATION_PATTERNS = (
    re.compile(r"\bnot published(?:\s+to\s+pypi)?\s+yet\b", re.IGNORECASE),
    re.compile(r"\bafter\s+pypi\s+publication\b", re.IGNORECASE),
    re.compile(r"\bafter\s+publication\b", re.IGNORECASE),
    re.compile(r"\bpending\s+trusted\s+publishers?\b", re.IGNORECASE),
    re.compile(r"\btestpypi-only\b", re.IGNORECASE),
    re.compile(r"\breal\s+pypi\s+remains\s+blocked\b", re.IGNORECASE),
)
DESIGN_PARTNER_NOT_MET_MARKER = "Design Partner Gate: NOT MET"
DESIGN_PARTNER_CLAIM_PATTERNS = (
    re.compile(r"\bdesign[- ]partners?\b", re.IGNORECASE),
    re.compile(r"\bpilot users?\b", re.IGNORECASE),
    re.compile(r"\bcustomer validated\b", re.IGNORECASE),
    re.compile(r"\bcustomer validation\b", re.IGNORECASE),
    re.compile(r"\benterprise[- ]ready\b", re.IGNORECASE),
)
DESIGN_PARTNER_QUALIFIERS = (
    "not met",
    "not yet",
    "does not",
    "no ",
    "without",
    "future",
    "criteria",
    "permission-to-list",
    "empty",
    "unclaimed",
    "not claimed",
    "seeking",
    "before",
    "until",
)
BENCHMARK_BLOCK_START = "<!-- BENCH:START -->"
BENCHMARK_BLOCK_END = "<!-- BENCH:END -->"
PUBLIC_CLAIM_PATTERNS = (
    re.compile(r"\b(?:f1|f1-score|precision|recall)\b[^\n]*\d", re.IGNORECASE),
    re.compile(r"\b(?:sota|state[- ]of[- ]the[- ]art)\b", re.IGNORECASE),
    re.compile(r"\b(?:beats?|outperforms?|improves? on|quality milestone)\b", re.IGNORECASE),
    re.compile(r"\bproduction[- ](?:quality|grade) trained model\b", re.IGNORECASE),
    re.compile(r"\bproduction model[- ]quality claims?\b", re.IGNORECASE),
    re.compile(r"\b(?:live|hosted) (?:domain|playground|demo)\b", re.IGNORECASE),
    re.compile(r"\bdeployed at https?://", re.IGNORECASE),
    re.compile(r"\b0\.5B\b[^\n]*(?:->|to|-|→)[^\n]*\b7B\b", re.IGNORECASE),
    re.compile(r"\bSFT\b[^\n]*\bGRPO\b[^\n]*\bGiGPO\b", re.IGNORECASE),
)
PUBLIC_CLAIM_QUALIFIERS = (
    "not ",
    "not yet",
    "does not",
    "do not",
    "future",
    "planned",
    "until",
    "unless",
    "after",
    "before",
    "only after",
    "once",
    "generated from",
    "generated evidence",
    "citation-only",
    "not rerun",
    "verify",
    "verified",
    "verification",
    "evidence exists",
    "smoke",
    "testpypi",
    "source checkout",
    "not shipped yet",
)
REMOVED_CUSTOM_DOMAIN = "dataforge" + ".dev"
CUSTOM_DOMAIN_PATTERN = re.compile(
    rf"(?:https?://(?:www\.)?{re.escape(REMOVED_CUSTOM_DOMAIN)}(?:/[^\s)]*)?"
    rf"|\b{re.escape(REMOVED_CUSTOM_DOMAIN)}\b)"
)
UNSHIPPED_INTEGRATION_PATTERNS = (
    re.compile(r"\bdataforge-airbyte\b", re.IGNORECASE),
    re.compile(r"\bdataforge-databricks\b", re.IGNORECASE),
    re.compile(r"\bAirbyte\b", re.IGNORECASE),
    re.compile(r"\bDatabricks\b", re.IGNORECASE),
)
UNSHIPPED_INTEGRATION_QUALIFIERS = (
    "future",
    "planned",
    "not shipped",
    "not yet",
    "roadmap",
    "external adapter packages",
    "should only",
    "until",
    "once",
)


def load_claim_ledger(path: Path = CLAIM_LEDGER) -> list[dict[str, str]]:
    """Load the public claim ledger entries."""
    raw_payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        return []
    raw_entries = raw_payload.get("claims", [])
    if not isinstance(raw_entries, list):
        return []
    entries: list[dict[str, str]] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            continue
        entry = {
            "claim": str(raw_entry.get("claim", "")).strip(),
            "status": str(raw_entry.get("status", "")).strip(),
            "evidence": str(raw_entry.get("evidence", "")).strip(),
        }
        entries.append(entry)
    return entries


def check_claim_ledger(path: Path = CLAIM_LEDGER) -> list[str]:
    """Verify every public claim has closed-vocabulary status and evidence."""
    errors: list[str] = []
    try:
        display_path = path.relative_to(PROJECT_ROOT)
    except ValueError:
        display_path = path
    if not path.exists():
        return [f"{display_path} is missing."]

    try:
        raw_payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        return [f"{display_path} is not valid YAML: {exc}"]

    if not isinstance(raw_payload, dict):
        return [f"{display_path} must be a YAML mapping."]
    raw_entries = raw_payload.get("claims")
    if not isinstance(raw_entries, list):
        return [f"{display_path} must contain a claims list."]

    seen_claims: set[str] = set()
    for index, raw_entry in enumerate(raw_entries, start=1):
        if not isinstance(raw_entry, dict):
            errors.append(f"claim ledger entry {index} must be a mapping.")
            continue
        claim = str(raw_entry.get("claim", "")).strip()
        status = str(raw_entry.get("status", "")).strip()
        evidence = str(raw_entry.get("evidence", "")).strip()
        if not claim:
            errors.append(f"claim ledger entry {index} is missing claim.")
        elif claim in seen_claims:
            errors.append(f"claim ledger entry {index} duplicates claim '{claim}'.")
        seen_claims.add(claim)
        if status not in CLAIM_LEDGER_STATUSES:
            errors.append(f"claim ledger entry {index} has unknown status '{status}'.")
        if not evidence:
            errors.append(f"claim ledger entry {index} is missing evidence.")
    return errors


def check_evidence_ledger(path: Path = EVIDENCE_LEDGER) -> list[str]:
    """Verify the canonical evidence ledger can back public claim prose."""
    try:
        from scripts.evidence.evidence_ledger import validate_ledger
    except ImportError as exc:
        return [f"could not import evidence ledger validator: {exc}"]
    return validate_ledger(path)


def _project_version(path: Path) -> str:
    """Return the local project version from a pyproject.toml file."""
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    project = payload.get("project")
    if not isinstance(project, dict):
        return ""
    return str(project.get("version", "")).strip()


def _display_path(path: Path) -> Path:
    """Return a compact display path when possible."""
    try:
        return path.relative_to(PROJECT_ROOT)
    except ValueError:
        return path


def _check_smoke_log(
    package: dict[str, object],
    *,
    package_name: str,
    field: str,
    display_path: Path,
) -> list[str]:
    """Verify a referenced smoke log exists and is valid JSON."""
    raw_log_path = package.get(field)
    if not isinstance(raw_log_path, str) or not raw_log_path:
        return [f"{display_path} package {package_name} is missing {field}."]
    log_path = PROJECT_ROOT / "docs" / "evidence" / raw_log_path
    if not log_path.exists():
        return [f"{display_path} package {package_name} references missing {raw_log_path}."]
    try:
        json.loads(log_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{raw_log_path} is not valid JSON: {exc}"]
    return []


def _check_trusted_publisher(
    *,
    publisher: object,
    package_name: str,
    index_name: str,
    artifact_name: str,
    display_path: Path,
) -> list[str]:
    """Verify one artifact's Trusted Publisher identity."""
    errors: list[str] = []
    expected_workflow = EXPECTED_PUBLISH_WORKFLOWS[package_name][index_name]
    if not isinstance(publisher, dict):
        return [
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "is missing trusted_publisher."
        ]
    expected_identity = (
        f"https://github.com/Aegis15/dataforge/.github/workflows/"
        f"{expected_workflow}@refs/heads/main"
    )
    expected_values = {
        "repository": "Aegis15/dataforge",
        "ref": "refs/heads/main",
        "workflow": expected_workflow,
        "identity": expected_identity,
        "oidc_issuer": "https://token.actions.githubusercontent.com",
    }
    for field, expected in expected_values.items():
        if publisher.get(field) != expected:
            errors.append(
                f"{display_path} package {package_name} {index_name} {artifact_name} "
                f"has trusted_publisher.{field} != {expected!r}."
            )
    return errors


def _check_publish_artifact(
    *,
    artifact: object,
    package_name: str,
    index_name: str,
    artifact_name: str,
    display_path: Path,
) -> list[str]:
    """Verify wheel/sdist artifact evidence."""
    if not isinstance(artifact, dict):
        return [f"{display_path} package {package_name} {index_name} is missing {artifact_name}."]
    errors: list[str] = []
    filename = artifact.get("filename")
    if not isinstance(filename, str) or not filename:
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "is missing filename."
        )
    sha256 = artifact.get("sha256")
    if not isinstance(sha256, str) or not SHA256_PATTERN.fullmatch(sha256):
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "has invalid sha256."
        )
    if artifact.get("integrity_subject_sha256") != sha256:
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "has mismatched integrity_subject_sha256."
        )
    if artifact.get("integrity_predicate_type") != PUBLISH_ATTESTATION_PREDICATE:
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "is missing the PyPI publish attestation predicate."
        )
    provenance_url = artifact.get("provenance_url")
    expected_host = (
        "https://pypi.org/integrity/"
        if index_name == "pypi"
        else "https://test.pypi.org/integrity/"
    )
    if not isinstance(provenance_url, str) or not provenance_url.startswith(expected_host):
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "has invalid provenance_url."
        )
    download_url = artifact.get("download_url")
    if not isinstance(download_url, str) or not download_url.startswith("https://"):
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "has invalid download_url."
        )
    if not isinstance(artifact.get("upload_time_iso_8601"), str):
        errors.append(
            f"{display_path} package {package_name} {index_name} {artifact_name} "
            "is missing upload_time_iso_8601."
        )
    errors.extend(
        _check_trusted_publisher(
            publisher=artifact.get("trusted_publisher"),
            package_name=package_name,
            index_name=index_name,
            artifact_name=artifact_name,
            display_path=display_path,
        )
    )
    return errors


def _check_publish_index(
    *,
    index_payload: object,
    package_name: str,
    index_name: str,
    display_path: Path,
) -> list[str]:
    """Verify one package's PyPI or TestPyPI evidence block."""
    if not isinstance(index_payload, dict):
        return [f"{display_path} package {package_name} is missing {index_name} metadata."]
    errors: list[str] = []
    if index_payload.get("index") != index_name:
        errors.append(f"{display_path} package {package_name} has wrong {index_name}.index.")
    project_url = index_payload.get("project_url")
    expected_project_host = (
        "https://pypi.org/project/" if index_name == "pypi" else "https://test.pypi.org/project/"
    )
    if not isinstance(project_url, str) or not project_url.startswith(expected_project_host):
        errors.append(
            f"{display_path} package {package_name} {index_name} has invalid project_url."
        )
    for artifact_name in ("wheel", "sdist"):
        errors.extend(
            _check_publish_artifact(
                artifact=index_payload.get(artifact_name),
                package_name=package_name,
                index_name=index_name,
                artifact_name=artifact_name,
                display_path=display_path,
            )
        )
    return errors


def check_pypi_publish_report(path: Path = PUBLISH_REPORT) -> list[str]:
    """Verify the generated PyPI/TestPyPI evidence covers every public package."""
    errors: list[str] = []
    display_path = _display_path(path)
    if not path.exists():
        return [f"{display_path} is missing."]
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{display_path} is not valid JSON: {exc}"]
    if not isinstance(raw_payload, dict):
        return [f"{display_path} must be a JSON object."]
    if raw_payload.get("schema_version") != "dataforge_pypi_publish_report_v2":
        errors.append(f"{display_path} has an unexpected schema_version.")
    raw_packages = raw_payload.get("packages")
    if not isinstance(raw_packages, list):
        return [f"{display_path} must contain a packages list."]
    packages = {
        str(package.get("name", "")).strip(): package
        for package in raw_packages
        if isinstance(package, dict)
    }
    missing = set(PUBLISHED_DISTS) - set(packages)
    extra = set(packages) - set(PUBLISHED_DISTS)
    if missing:
        errors.append(f"{display_path} is missing package evidence for {sorted(missing)}.")
    if extra:
        errors.append(f"{display_path} has unexpected package evidence for {sorted(extra)}.")
    for name in PUBLISHED_DISTS:
        package = packages.get(name)
        if not isinstance(package, dict):
            continue
        expected_version = _project_version(PUBLISHED_DIST_PYPROJECTS[name])
        if package.get("version") != expected_version:
            errors.append(
                f"{display_path} package {name} version does not match local "
                f"pyproject version {expected_version}."
            )
        for field in ("pypi", "testpypi"):
            errors.extend(
                _check_publish_index(
                    index_payload=package.get(field),
                    package_name=name,
                    index_name=field,
                    display_path=display_path,
                )
            )
        for field in (
            "attestations",
            "pypi_fresh_install",
            "testpypi_fresh_install",
            "trusted_publishing",
        ):
            if package.get(field) is not True:
                errors.append(f"{display_path} package {name} has {field} != true.")
        if not str(package.get("workflow_run_url", "")).startswith(
            "https://github.com/Aegis15/dataforge/actions/runs/"
        ):
            errors.append(f"{display_path} package {name} is missing workflow_run_url.")
        errors.extend(
            _check_smoke_log(
                package,
                package_name=name,
                field="pypi_smoke_log_path",
                display_path=display_path,
            )
        )
        errors.extend(
            _check_smoke_log(
                package,
                package_name=name,
                field="testpypi_smoke_log_path",
                display_path=display_path,
            )
        )
    return errors


def extract_subcommands_from_readme(text: str) -> set[str]:
    """Find all DataForge CLI subcommand references in the README."""
    pattern = re.compile(r"\bdataforge(?:15)?\s+([a-z][a-z0-9_-]*)")
    return {m.group(1) for m in pattern.finditer(text)}


def extract_release_subcommands_from_readme(text: str) -> set[str]:
    """Find all nested ``dataforge release <command>`` references."""
    pattern = re.compile(r"\bdataforge(?:15)?\s+release\s+([a-z][a-z0-9_-]*)")
    return {m.group(1) for m in pattern.finditer(text)}


def get_registered_typer_commands() -> set[str]:
    """Import the Typer app and list registered command names."""
    try:
        from dataforge.cli import app as typer_app
    except ImportError as exc:
        print(f"WARNING: could not import dataforge.cli: {exc}", file=sys.stderr)
        return set()

    registered: set[str] = set()
    if hasattr(typer_app, "registered_commands"):
        for cmd in typer_app.registered_commands:
            if hasattr(cmd, "name") and cmd.name:
                registered.add(cmd.name)
    if hasattr(typer_app, "registered_groups"):
        for group in typer_app.registered_groups:
            if hasattr(group, "name") and group.name:
                registered.add(group.name)

    # Also check the callback (single-command mode)
    if hasattr(typer_app, "info") and hasattr(typer_app.info, "name") and typer_app.info.name:
        registered.add(typer_app.info.name)

    return registered


def get_registered_release_commands() -> set[str]:
    """Import the release Typer app and list registered release commands."""
    try:
        from dataforge.cli.release import release_app
    except ImportError as exc:
        print(f"WARNING: could not import dataforge.cli.release: {exc}", file=sys.stderr)
        return set()

    registered: set[str] = set()
    if hasattr(release_app, "registered_commands"):
        for cmd in release_app.registered_commands:
            if hasattr(cmd, "name") and cmd.name:
                registered.add(cmd.name)
    return registered


def extract_playground_urls(text: str) -> list[str]:
    """Find live playground URLs in the README."""
    pattern = re.compile(r"https?://[^\s)`]+(?:workers\.dev|pages\.dev|hf\.space)[^\s)`]*")
    return [match.rstrip(".,;:") for match in pattern.findall(text)]


def check_playground_urls(urls: list[str]) -> list[str]:
    """Check that playground URLs return 200 (if any are present)."""
    if not urls:
        return []

    errors: list[str] = []
    try:
        import httpx
    except ImportError:
        print("WARNING: httpx not available, skipping URL checks.", file=sys.stderr)
        return []

    for url in urls:
        try:
            response = httpx.get(url, timeout=30.0, follow_redirects=True)
            if response.status_code != 200:
                errors.append(f"URL {url} returned {response.status_code}")
        except Exception as exc:
            errors.append(f"URL {url} failed: {exc}")

    return errors


def check_stale_publication_claims(paths: list[Path]) -> list[str]:
    """Reject stale docs that describe already-published packages as unpublished."""
    errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            display_path = path.relative_to(PROJECT_ROOT)
        except ValueError:
            display_path = path
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not any(pattern.search(line) for pattern in STALE_PUBLICATION_PATTERNS):
                continue
            errors.append(
                f"{display_path}:{line_number} describes a published DataForge package "
                "or release surface as unpublished."
            )
    return errors


def design_partner_gate_not_met() -> bool:
    """Return whether the design-partner gate is explicitly marked unmet."""
    if not CONTRIBUTORS.exists():
        return False
    return DESIGN_PARTNER_NOT_MET_MARKER.lower() in CONTRIBUTORS.read_text(encoding="utf-8").lower()


def check_design_partner_claims(paths: list[Path]) -> list[str]:
    """Reject unqualified customer/design-partner claims while the gate is unmet."""
    if not design_partner_gate_not_met():
        return []

    errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            display_path = path.relative_to(PROJECT_ROOT)
        except ValueError:
            display_path = path
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            if not any(pattern.search(line) for pattern in DESIGN_PARTNER_CLAIM_PATTERNS):
                continue
            if any(qualifier in lowered for qualifier in DESIGN_PARTNER_QUALIFIERS):
                continue
            errors.append(
                f"{display_path}:{line_number} has an unqualified "
                "design-partner or customer-validation claim while the gate is not met."
            )
    return errors


def _generated_benchmark_lines(text: str) -> set[int]:
    """Return line numbers covered by generated benchmark marker blocks."""
    covered: set[int] = set()
    inside = False
    for line_number, line in enumerate(text.splitlines(), start=1):
        if BENCHMARK_BLOCK_START in line:
            inside = True
        if inside:
            covered.add(line_number)
        if BENCHMARK_BLOCK_END in line:
            inside = False
    return covered


def check_public_claim_boundaries(paths: list[Path]) -> list[str]:
    """Reject benchmark/model/live claims outside generated or qualified evidence text."""
    errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            display_path = path.relative_to(PROJECT_ROOT)
        except ValueError:
            display_path = path
        text = path.read_text(encoding="utf-8")
        generated_lines = _generated_benchmark_lines(text)
        lines = text.splitlines()
        for index, line in enumerate(lines):
            line_number = index + 1
            if line_number in generated_lines:
                continue
            if not any(pattern.search(line) for pattern in PUBLIC_CLAIM_PATTERNS):
                continue
            previous_line = lines[index - 1] if index > 0 else ""
            next_line = lines[index + 1] if index + 1 < len(lines) else ""
            context = f"{previous_line}\n{line}\n{next_line}".lower()
            if any(qualifier in context for qualifier in PUBLIC_CLAIM_QUALIFIERS):
                continue
            errors.append(
                f"{display_path}:{line_number} has a benchmark, model-quality, publication, "
                "or live-surface claim outside a generated evidence block."
            )
    return errors


def check_custom_domain_claims(paths: list[Path]) -> list[str]:
    """Reject any reference to the removed domain."""
    errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            display_path = path.relative_to(PROJECT_ROOT)
        except ValueError:
            display_path = path
        lines = path.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if not CUSTOM_DOMAIN_PATTERN.search(line):
                continue
            errors.append(
                f"{display_path}:{index + 1} references the removed domain. "
                "Use the Cloudflare workers.dev playground URL instead."
            )
    return errors


def check_unshipped_integration_claims(paths: list[Path]) -> list[str]:
    """Reject Airbyte/Databricks claims unless they are clearly roadmap-only."""
    errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            display_path = path.relative_to(PROJECT_ROOT)
        except ValueError:
            display_path = path
        lines = path.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if not any(pattern.search(line) for pattern in UNSHIPPED_INTEGRATION_PATTERNS):
                continue
            previous_line = lines[index - 1] if index > 0 else ""
            next_line = lines[index + 1] if index + 1 < len(lines) else ""
            context = f"{previous_line}\n{line}\n{next_line}".lower()
            if any(qualifier in context for qualifier in UNSHIPPED_INTEGRATION_QUALIFIERS):
                continue
            errors.append(
                f"{display_path}:{index + 1} has an unqualified Airbyte or Databricks "
                "integration claim without shipped package evidence."
            )
    return errors


def main() -> None:
    """Run all README truth checks."""
    readme_text = README.read_text(encoding="utf-8")
    errors: list[str] = []

    # Check subcommands
    claimed = extract_subcommands_from_readme(readme_text)
    registered = get_registered_typer_commands()
    claimed_release = extract_release_subcommands_from_readme(readme_text)
    registered_release = get_registered_release_commands()

    # Exclude known non-command references (e.g. version flags)
    non_commands = {"version", "help"}
    claimed_commands = claimed - non_commands

    if registered:
        missing = claimed_commands - registered
        if missing:
            errors.append(
                f"README claims these subcommands but they are not registered: {sorted(missing)}"
            )
    else:
        print("WARNING: could not resolve registered commands, skipping subcommand check.")

    if registered_release:
        missing_release = claimed_release - registered_release
        if missing_release:
            errors.append(
                "README claims these release subcommands but they are not registered: "
                f"{sorted(missing_release)}"
            )
    elif claimed_release:
        print(
            "WARNING: could not resolve release commands, skipping release subcommand check.",
            file=sys.stderr,
        )

    # Check playground URLs
    playground_urls = extract_playground_urls(readme_text)
    url_errors = check_playground_urls(playground_urls)
    errors.extend(url_errors)
    errors.extend(check_pypi_publish_report())
    errors.extend(check_stale_publication_claims(RELEASE_TRUTH_DOCS))
    errors.extend(check_design_partner_claims(DESIGN_PARTNER_TRUTH_DOCS))
    errors.extend(check_public_claim_boundaries(PUBLIC_CLAIM_TRUTH_DOCS))
    errors.extend(check_custom_domain_claims(CUSTOM_DOMAIN_TRUTH_DOCS))
    errors.extend(check_unshipped_integration_claims(PUBLIC_CLAIM_TRUTH_DOCS))
    errors.extend(check_claim_ledger())
    errors.extend(check_evidence_ledger())

    if errors:
        print("README truth check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)

    print(
        f"README truth check passed. "
        f"Claimed commands: {sorted(claimed_commands)}. "
        f"Claimed release commands: {sorted(claimed_release)}. "
        f"Playground URLs checked: {len(playground_urls)}."
    )


if __name__ == "__main__":
    main()
