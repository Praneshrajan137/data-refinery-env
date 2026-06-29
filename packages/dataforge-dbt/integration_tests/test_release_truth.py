"""Release-truth checks for the DataForge dbt integration README."""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_readme_documents_published_install_without_stale_release_qualifiers() -> None:
    """The README should describe the current PyPI package state."""
    text = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    assert re.search(r"\bpython\s+-m\s+pip\s+install\s+dataforge_07_dbt\b", text)
    assert "not published yet" not in text.lower()
    assert "after pypi publication" not in text.lower()
