"""CI check: verify README claims match shipped code.

Asserts that every `dataforge15 <subcommand>` or compatibility
`dataforge <subcommand>` shown in the root README resolves to a registered
Typer command. Also checks that the playground
URL (once added) returns HTTP 200.

Usage:
    python scripts/ci/readme_truth.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
README = PROJECT_ROOT / "README.md"
CONTRIBUTORS = PROJECT_ROOT / "CONTRIBUTORS.md"
CLAIM_LEDGER = PROJECT_ROOT / "docs" / "claims.yaml"
CLAIM_LEDGER_STATUSES = frozenset({"shipped", "beta", "experimental", "roadmap"})
RELEASE_TRUTH_DOCS = [
    README,
    PROJECT_ROOT / "META_CONTEXT.md",
    PROJECT_ROOT / "docs" / "docs" / "index.md",
    PROJECT_ROOT / "docs" / "docs" / "quickstart.md",
    PROJECT_ROOT / "dataforge-mcp" / "README.md",
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
UNPUBLISHED_DISTS = (
    "dataforge15",
    "dataforge15-dbt",
    "dataforge15-evals",
    "dataforge15-mcp",
    "dataforge15-agent-patterns",
)
PUBLISHED_QUALIFIERS = (
    "after publication",
    "after pypi publication",
    "once published",
    "when published",
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
CUSTOM_DOMAIN_PATTERN = re.compile(
    r"(?:https?://(?:www\.)?dataforge\.dev(?:/[^\s)]*)?|\bdataforge\.dev\b)"
)
CUSTOM_DOMAIN_QUALIFIERS = (
    "future",
    "optional",
    "deferred",
    "later",
    "planned",
    "not ",
    "not yet",
    "out of scope",
    "not a release",
    "after",
    "branding",
    "custom domain",
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


def extract_subcommands_from_readme(text: str) -> set[str]:
    """Find all DataForge15 CLI subcommand references in the README."""
    pattern = re.compile(r"\bdataforge(?:15)?\s+([a-z][a-z0-9_-]*)")
    return {m.group(1) for m in pattern.finditer(text)}


def extract_release_subcommands_from_readme(text: str) -> set[str]:
    """Find all nested ``dataforge15 release <command>`` references."""
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
    """Find live playground URLs in the README.

    The optional custom domain is intentionally excluded because it is future
    branding, not a release-readiness target.
    """
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


def check_unpublished_install_claims(paths: list[Path]) -> list[str]:
    """Reject unqualified PyPI install claims for packages not yet published."""
    errors: list[str] = []
    install_pattern = re.compile(
        rf"\bpip\s+install\b[^\n`]*(?:{'|'.join(re.escape(name) for name in UNPUBLISHED_DISTS)})"
    )
    for path in paths:
        if not path.exists():
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            if not install_pattern.search(line):
                continue
            if any(qualifier in lowered for qualifier in PUBLISHED_QUALIFIERS):
                continue
            errors.append(
                f"{path.relative_to(PROJECT_ROOT)}:{line_number} has an unqualified "
                "PyPI install claim for an unpublished DataForge15 package."
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
    """Reject unqualified claims that dataforge.dev is live or release-blocking."""
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
            previous_line = lines[index - 1] if index > 0 else ""
            next_line = lines[index + 1] if index + 1 < len(lines) else ""
            context = f"{previous_line}\n{line}\n{next_line}".lower()
            if any(qualifier in context for qualifier in CUSTOM_DOMAIN_QUALIFIERS):
                continue
            errors.append(
                f"{display_path}:{index + 1} presents dataforge.dev as a current live "
                "surface. It must be described only as a future optional custom domain."
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
    errors.extend(check_unpublished_install_claims(RELEASE_TRUTH_DOCS))
    errors.extend(check_design_partner_claims(DESIGN_PARTNER_TRUTH_DOCS))
    errors.extend(check_public_claim_boundaries(PUBLIC_CLAIM_TRUTH_DOCS))
    errors.extend(check_custom_domain_claims(CUSTOM_DOMAIN_TRUTH_DOCS))
    errors.extend(check_unshipped_integration_claims(PUBLIC_CLAIM_TRUTH_DOCS))
    errors.extend(check_claim_ledger())

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
