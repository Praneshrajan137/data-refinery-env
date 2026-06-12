"""Release naming contract for the dataforge-agent-patterns distribution."""

from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_distribution_name() -> None:
    """The package publishes under the DataForge 07 distribution namespace."""
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = pyproject["project"]

    assert project["name"] == "dataforge_07_agent_patterns"
