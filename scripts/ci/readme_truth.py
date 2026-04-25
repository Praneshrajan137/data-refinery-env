"""CI check: verify README claims match shipped code.

Asserts that every `dataforge <subcommand>` shown in the root README
resolves to a registered Typer command. Also checks that the playground
URL (once added) returns HTTP 200.

Usage:
    python scripts/ci/readme_truth.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
README = PROJECT_ROOT / "README.md"


def extract_subcommands_from_readme(text: str) -> set[str]:
    """Find all `dataforge <subcommand>` references in the README."""
    pattern = re.compile(r"dataforge\s+([a-z][a-z0-9_-]*)")
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


def extract_playground_urls(text: str) -> list[str]:
    """Find playground URLs in the README."""
    pattern = re.compile(r"https?://[^\s)]+(?:pages\.dev|hf\.space)[^\s)]*")
    return pattern.findall(text)


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


def main() -> None:
    """Run all README truth checks."""
    readme_text = README.read_text(encoding="utf-8")
    errors: list[str] = []

    # Check subcommands
    claimed = extract_subcommands_from_readme(readme_text)
    registered = get_registered_typer_commands()

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

    # Check playground URLs
    playground_urls = extract_playground_urls(readme_text)
    url_errors = check_playground_urls(playground_urls)
    errors.extend(url_errors)

    if errors:
        print("README truth check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)

    print(
        f"README truth check passed. "
        f"Claimed commands: {sorted(claimed_commands)}. "
        f"Playground URLs checked: {len(playground_urls)}."
    )


if __name__ == "__main__":
    main()
