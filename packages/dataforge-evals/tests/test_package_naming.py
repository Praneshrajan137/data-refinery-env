"""Release naming contract for the dataforge-evals distribution."""

from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_distribution_name_and_cli_aliases() -> None:
    """The package publishes as dataforge_07_evals and keeps the CLI aliases."""
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = pyproject["project"]

    assert project["name"] == "dataforge_07_evals"
    assert project["optional-dependencies"]["dataforge"] == ["dataforge_07>=0.1.0"]
    assert project["scripts"]["dataforge-evals"] == "dataforge_evals.cli:app"
    assert project["scripts"]["dataforge15-evals"] == "dataforge_evals.cli:app"
