"""Release naming contract for the dataforge-dbt distribution."""

from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_distribution_name_dependency_and_cli_aliases() -> None:
    """The package publishes as dataforge_07_dbt and keeps the CLI aliases."""
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = pyproject["project"]

    assert project["name"] == "dataforge_07_dbt"
    assert "dataforge_07>=0.1.0,<0.2" in project["dependencies"]
    assert project["scripts"]["dataforge-dbt"] == "dataforge_dbt.dispatch:main"
    assert project["scripts"]["dataforge15-dbt"] == "dataforge_dbt.dispatch:main"
