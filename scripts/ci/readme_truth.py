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
#: Documents whose auto-apply claims are checked against the runtime allowlist. Wider than
#: PUBLIC_CLAIM_TRUTH_DOCS because the staleness of 2026-08-25 reached the architecture
#: document too, not only the marketing surface.
AUTOAPPLY_TRUTH_DOCS = [
    README,
    PROJECT_ROOT / "PRODUCT.md",
    PROJECT_ROOT / "ARCHITECTURE.md",
    PROJECT_ROOT / "docs" / "docs" / "index.md",
    PROJECT_ROOT / "docs" / "docs" / "quickstart.md",
    PROJECT_ROOT / "docs" / "docs" / "detectors.md",
    PROJECT_ROOT / "docs" / "docs" / "architecture.md",
]

#: Registered CLI commands that are deliberately absent from README.md, with the reason.
#: This exists so that "undocumented" is a DECLARATION rather than an oversight -- the
#: check below fails on any registered command that is neither documented nor listed here,
#: and also fails on a stale entry, so the list cannot rot into fake coverage.
#:
#: Empty on purpose as of 2026-09-01: `measure-on-my-table` was the only member, and it was
#: documented instead of exempted, because it is the design-partner entry point and its
#: discoverability is the bottleneck named in the 2026-09-01 DECISIONS entry.
UNDOCUMENTED_COMMANDS: frozenset[str] = frozenset()

#: Every list above defines a POPULATION this module polices. A list that shrinks to
#: nothing -- or that names a document somebody deleted -- does not make these checks
#: weaker, it makes them vacuous: they iterate nothing, report success, and gate nothing.
#: That is the failure mode retiring META_CONTEXT.md and FILE_STRUCTURE.md could have
#: introduced silently, so it is now an error rather than a green run. Checked at import
#: so it fires in CI before any individual check reports.
_TRUTH_DOC_POPULATIONS = {
    "RELEASE_TRUTH_DOCS": RELEASE_TRUTH_DOCS,
    "DESIGN_PARTNER_TRUTH_DOCS": DESIGN_PARTNER_TRUTH_DOCS,
    "PUBLIC_CLAIM_TRUTH_DOCS": PUBLIC_CLAIM_TRUTH_DOCS,
    "CUSTOM_DOMAIN_TRUTH_DOCS": CUSTOM_DOMAIN_TRUTH_DOCS,
    "AUTOAPPLY_TRUTH_DOCS": AUTOAPPLY_TRUTH_DOCS,
}


def assert_truth_doc_populations_are_non_vacuous() -> None:
    """Refuse to run if any policed document set is empty or names a missing file."""
    problems: list[str] = []
    for name, docs in _TRUTH_DOC_POPULATIONS.items():
        if not docs:
            problems.append(f"{name} is empty, so every check over it would pass vacuously")
            continue
        for doc in docs:
            if not doc.exists():
                rel = doc.relative_to(PROJECT_ROOT) if doc.is_relative_to(PROJECT_ROOT) else doc
                problems.append(f"{name} names a document that does not exist: {rel}")
    if problems:
        raise SystemExit(
            "readme_truth doc populations are not trustworthy:\n  " + "\n  ".join(problems)
        )


assert_truth_doc_populations_are_non_vacuous()
#: Phrases that assert write authority for a detector. A line merely naming a detector is
#: not a claim about what it may do, so the policed set is deliberately narrow.
_AUTHORITY_CLAIM_PHRASES = (
    "auto-correcting",
    "auto-corrects",
    "auto-applies",
    "auto-applied",
    "constraint-checkable",
    "may auto-apply",
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
    """List the CLI's command names, and verify each one actually resolves.

    Commands are registered lazily as of 2026-08-28 (see ``dataforge/cli/__init__.py``), so
    ``registered_commands`` is empty by design and reading it alone would make this check
    vacuous -- every command claimed in the README would look unregistered, or worse, the empty
    set would silently satisfy a subset test.

    So this asks the lazy table for the names and then RESOLVES each one, which imports the
    module and builds the click command. That is strictly stronger than what this function did
    before: previously a name being registered proved only that a name was registered, and now a
    name being returned proves its target imports and builds. A typo in the table fails here.

    The eager lists are still read, so a command added the ordinary way is still counted.
    """
    try:
        from dataforge import cli
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

    for name in cli.command_names():
        if cli.resolve_command(name) is None:
            print(
                f"WARNING: lazy command {name!r} does not resolve to a command; excluded",
                file=sys.stderr,
            )
            continue
        registered.add(name)

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
    """Check that playground URLs return 200.

    This is the only check in this file that verifies a LIVE EXTERNAL SURFACE, and until
    2026-09-01 it failed open on two independent axes -- an empty URL list returned ``[]``,
    and a missing ``httpx`` printed a warning to stderr and returned ``[]``. Either way the
    README could claim a live playground while nothing confirmed one existed, which is the
    honesty doctrine's "never pre-claim an external event" clause going unenforced by one of
    the gates written to enforce it.

    Both are now failures, on the reasoning this module already applies at import time in
    ``_assert_policed_docs_exist`` and that ``gate_population.py`` applies to its node ids:
    an empty population makes a gate vacuous, and vacuity must be reported, not passed.

    Args:
        urls: Playground URLs extracted from the README.

    Returns:
        Human-readable error strings, empty when every URL answered 200.
    """
    if not urls:
        # Not a "nothing to do" case. The README documents a hosted playground, so zero
        # matches means the extraction regex and the README have diverged -- most likely a
        # host change -- and the live-surface claim is now unchecked.
        return [
            "No playground URLs found in the README, so the live-surface claim is "
            "unverified. extract_playground_urls only matches workers.dev, pages.dev and "
            "hf.space; if the playground moved hosts, widen that pattern rather than "
            "letting this check pass on an empty list."
        ]

    errors: list[str] = []
    try:
        import httpx
    except ImportError:
        # httpx is a declared dependency in four groups in pyproject.toml, so this branch
        # cannot fire for the reason the old message gave. It firing means the environment
        # is broken, and a broken environment must not read as a pass.
        return [
            "httpx is not importable, so playground URLs could not be checked. It is a "
            "declared dependency (pyproject.toml), so this is a broken environment rather "
            "than a missing optional extra; fix the install instead of skipping the check."
        ]

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


def check_corpus_tier_claims(docs: list[Path]) -> list[str]:
    """Refuse a headline claim sourced from a non-headline corpus.

    A tier that is only documented rots. This is the mechanism that makes
    ``DatasetMetadata.tier`` real: a corpus whose errors are injected, synthetic or
    contested may appear in a public document, but only alongside a qualifier that
    tells the reader what they are looking at.

    The qualifier requirement rather than a ban is deliberate. ``hospital`` must remain
    discussable -- it is the project's regression tripwire and the subject of several
    trust documents -- but a reader meeting "F1 0.7926 on hospital" with no nearby word
    like "injected" or "tripwire" is being invited to read a benchmark artifact as a
    capability claim.

    Args:
        docs: Public documents to check.

    Returns:
        One error string per unqualified mention.
    """
    from dataforge.datasets.registry import DATASET_REGISTRY, non_headline_corpora

    non_headline = non_headline_corpora()
    qualifiers = (
        "injected",
        "synthetic",
        "contested",
        "tripwire",
        "diagnostic",
        "not a headline",
        "saturated",
        "demoted",
    )
    errors: list[str] = []
    for path in docs:
        if not path.exists():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            named = sorted(name for name in non_headline if name in lowered)
            if not named:
                continue
            # Only lines that also carry a number read as a claim.
            if not re.search(r"\d\.\d{2,}", line):
                continue
            if any(qualifier in lowered for qualifier in qualifiers):
                continue
            tiers = ", ".join(
                f"{name}={DATASET_REGISTRY[name].tier}"
                for name in named
                if name in DATASET_REGISTRY
            )
            errors.append(
                f"{path.name}:{number} quotes a number for non-headline corpus/corpora "
                f"{named} ({tiers}) with no qualifier. Add one of {list(qualifiers)}, or "
                "source the claim from a headline-tier corpus. See "
                "docs/trust/column-benchmark-scope.md."
            )
    return errors


def check_detector_family_count_claims(docs: list[Path]) -> list[str]:
    """Refuse a published count of detector families that disagrees with the closed vocabulary.

    Why this exists, dated 2026-08-26. `README.md` and `docs/docs/detectors.md` both said "Eight
    detector families" while ``IssueTypeLiteral`` defined eleven. The claim was stale for as long
    as :func:`check_autoapply_membership_claims` subtracted its allowlist from a hardcoded
    eight-name population -- so the prose and the gate agreed with each other and both disagreed
    with the code. Two mutually-consistent wrong artifacts read as verification.

    The count alone is weak evidence, so this also checks coverage: every issue type in the closed
    vocabulary must be named somewhere in the published set. A twelfth issue type is then
    undocumentable-but-shipped for exactly as long as CI is red, which is the property the count
    was supposed to provide and could not.

    Args:
        docs: Public documents to check.

    Returns:
        One error string per wrong count, plus one for any undocumented issue type.
    """
    from dataforge.detectors.base import ALL_ISSUE_TYPES

    expected = len(ALL_ISSUE_TYPES)
    words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
        "thirteen": 13,
        "fourteen": 14,
        "fifteen": 15,
    }
    # "Eight detector families", "Eight families ship", "11 issue families".
    #
    # Two deliberate narrowings, both found by running the check against the live docs before
    # trusting it. The number must not be glued to a preceding token, and an unqualified
    # "famil..." must be PLURAL: without either, "first-party GPT-5 family" in README.md parsed
    # as a claim of five detector families. A gate that fires on prose about something else
    # teaches its readers to ignore it.
    counted = r"(?<![-\w])(\d+|" + "|".join(words) + r")\b"
    pattern = re.compile(
        counted + r"\s+(?:(?:detector|issue)\s+famil(?:y|ies)|families)\b",
        re.IGNORECASE,
    )
    # Cues that the number counts a PART rather than the whole. Only a total can contradict the
    # vocabulary size; "ten of them come from the default ensemble" is a partition and is true.
    # Without this the check fired on the very sentences written to correct it -- the same failure
    # the `negations` tuple in :func:`check_autoapply_membership_claims` exists to prevent, and it
    # is worth stating twice: a gate that blocks its own remedy gets deleted rather than fixed.
    subset_cues = (
        "come from",
        "of them",
        "of these",
        "of the",
        "below",
        "above",
        "remain",
        "rest",
        "detection-only",
        "opt-in",
    )

    errors: list[str] = []
    documented: set[str] = set()
    for path in docs:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        documented.update(issue_type for issue_type in ALL_ISSUE_TYPES if issue_type in text)
        for number, line in enumerate(text.splitlines(), start=1):
            for match in pattern.finditer(line):
                trailing = line[match.end() : match.end() + 40].lower()
                if any(cue in trailing for cue in subset_cues):
                    continue
                token = match.group(1).lower()
                claimed = int(token) if token.isdigit() else words[token]
                if claimed != expected:
                    errors.append(
                        f"{path.name}:{number} claims {claimed} detector families; the closed "
                        f"vocabulary IssueTypeLiteral defines {expected}. Either the count is "
                        "stale or a detector shipped without updating the docs. Derived from "
                        "dataforge/detectors/base.py, never restated."
                    )

    undocumented = sorted(ALL_ISSUE_TYPES - documented)
    if undocumented:
        errors.append(
            f"these issue types exist in IssueTypeLiteral but no public doc names them: "
            f"{undocumented}. A detector families table that omits a shipped family understates "
            "the review surface a user is signing up for."
        )
    return errors


def check_autoapply_membership_claims(docs: list[Path]) -> list[str]:
    """Refuse a user-facing claim that a detector auto-applies unless the code agrees.

    Why this exists, dated 2026-08-25. `type_mismatch` was removed from
    ``CONSTRAINT_CHECKABLE_DETECTORS`` and TEN user-facing claims went stale in one commit.
    `README.md` described `type_mismatch` and `decimal_shift` as "auto-correcting" while
    withholding that label from `missing_value`, the only repairer measured at write
    precision 1.0000 -- wrong in both directions at once. `docs/docs/detectors.md` published
    a three-family table with a "Typical repair" column where two of the three could not
    write. None of it was caught by any gate, because this file checked that CLI commands
    exist and nothing checked what the product claims to *do*.

    Two directions, because the failure occurred in both. This function checks the first;
    :func:`check_autoapply_members_are_documented` checks the second. They are separate
    because they have different scopes -- a claim is wrong on the line it appears on, whereas
    coverage is only meaningful across the whole published set.

    Deliberately keyed on the small set of phrases that assert write authority rather than
    on any mention of a detector. Detectors must stay freely discussable -- the point is
    that claiming one *writes* is checkable against the code that decides whether it does.

    Args:
        docs: Public documents to check.

    Returns:
        One error string per contradicted claim.
    """
    from dataforge.detectors.base import ALL_ISSUE_TYPES
    from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS

    # DERIVED, not restated. Until 2026-08-26 this was an eight-name set literal while
    # `IssueTypeLiteral` had grown to eleven, so `date_transposition`, `entity_consensus` and
    # `semantic_domain_violation` were invisible to this check -- a doc could claim any of them
    # auto-applies and CI would pass. The allowlist was imported from source of truth and the
    # population it is subtracted from was frozen, which is the failure this file's own docstring
    # warns about, one level up. See dataforge/detectors/base.py for the general rule.
    non_writers = sorted(ALL_ISSUE_TYPES - set(CONSTRAINT_CHECKABLE_DETECTORS))
    # Phrases that assert write authority. A line merely naming a detector is not a claim.
    authority_claims = _AUTHORITY_CLAIM_PHRASES
    # A line that names a non-writer alongside an authority phrase is acceptable when it is
    # explicitly denying or historicising the claim. Without this, correcting a doc would
    # trip the very check that demanded the correction.
    negations = (
        "no |",
        "| no",
        "not ",
        "never",
        "removed",
        "withheld",
        "calibration-bound",
        "detection-only",
        "cannot",
        "no longer",
        "until",
        "would ",
    )

    errors: list[str] = []
    for path in docs:
        if not path.exists():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            if not any(phrase in lowered for phrase in authority_claims):
                continue
            if any(negation in lowered for negation in negations):
                continue
            named = sorted(name for name in non_writers if name in lowered)
            if named:
                errors.append(
                    f"{path.name}:{number} claims write authority for {named}, which is "
                    f"not in CONSTRAINT_CHECKABLE_DETECTORS "
                    f"({sorted(CONSTRAINT_CHECKABLE_DETECTORS)}). Either the claim is "
                    "stale or the allowlist changed without the docs. See "
                    "docs/trust/bypass-allowlist-evidence.md."
                )
    return errors


def check_autoapply_members_are_documented(docs: list[Path]) -> list[str]:
    """Refuse a detector that may auto-apply while no public document says so.

    The other half of the 2026-08-25 failure. `missing_value` holds the strongest measured
    write precision in the project and `README.md` listed it as merely "additive", implying
    it could not write. A detector that gains write authority silently is worse than one
    that loses it noisily.

    Scoped to a whole published set rather than a single file, because no one document has to
    name every member -- the union of them does.

    Args:
        docs: The full set of published documents.

    Returns:
        One error string naming every member no document mentions beside a write claim.
    """
    from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS

    documented: set[str] = set()
    for path in docs:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            lowered = line.lower()
            if not any(phrase in lowered for phrase in _AUTHORITY_CLAIM_PHRASES):
                continue
            documented.update(
                member for member in CONSTRAINT_CHECKABLE_DETECTORS if member in lowered
            )

    undocumented = sorted(set(CONSTRAINT_CHECKABLE_DETECTORS) - documented)
    if not undocumented:
        return []
    return [
        f"these detectors may auto-apply but no public doc says so: {undocumented}. "
        "A detector that gains write authority silently is the failure this check "
        "exists to prevent; name it in README.md or docs/docs/detectors.md."
    ]


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
        # The reverse direction, added 2026-09-01. Until then this check ran one way only --
        # every DOCUMENTED command had to resolve, but a REGISTERED command documented
        # nowhere was invisible. That is the allowlist shape this repository keeps
        # rediscovering: the failure mode is silence, not a red gate.
        #
        # It was live. `measure-on-my-table` shipped with zero README mentions, and it is
        # the design-partner instrument -- the one command whose entire purpose requires
        # somebody outside this repository to find it. A user-facing command nobody can
        # discover is not a shipped capability.
        undocumented = registered - claimed_commands - UNDOCUMENTED_COMMANDS
        if undocumented:
            errors.append(
                f"these commands are registered but documented nowhere in README.md: "
                f"{sorted(undocumented)}. Document them, or add them to "
                f"UNDOCUMENTED_COMMANDS with the reason. A command a user cannot discover "
                f"is not shipped."
            )
        stale_exemptions = UNDOCUMENTED_COMMANDS - registered
        if stale_exemptions:
            errors.append(
                f"UNDOCUMENTED_COMMANDS exempts commands that are not registered: "
                f"{sorted(stale_exemptions)}. Remove them -- a stale exemption reads as "
                f"coverage and hides the next undocumented command."
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
    errors.extend(check_corpus_tier_claims(PUBLIC_CLAIM_TRUTH_DOCS))
    errors.extend(check_detector_family_count_claims(AUTOAPPLY_TRUTH_DOCS))
    errors.extend(check_autoapply_membership_claims(AUTOAPPLY_TRUTH_DOCS))
    errors.extend(check_autoapply_members_are_documented(AUTOAPPLY_TRUTH_DOCS))

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
