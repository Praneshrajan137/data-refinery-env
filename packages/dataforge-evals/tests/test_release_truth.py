"""Release-truth checks for the dataforge-evals README."""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_readme_has_no_unqualified_unpublished_install_claims() -> None:
    """PyPI install snippets must be qualified while the package is unpublished."""
    text = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    pattern = re.compile(r"\bpip\s+install\b[^\n`]*dataforge_07_evals")
    errors = [
        line
        for line in text.splitlines()
        if pattern.search(line) and "after pypi publication" not in line.lower()
    ]

    assert errors == []
