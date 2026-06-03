"""Release naming contract for the DataForge distributions."""

from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_pyproject(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def test_core_distribution_uses_dataforge07_distribution_and_dataforge_cli_aliases() -> None:
    """The core PyPI distribution avoids the occupied name while keeping the CLI."""
    pyproject = _load_pyproject(PROJECT_ROOT / "pyproject.toml")
    project = pyproject["project"]

    assert project["name"] == "dataforge_07"
    assert (
        "dataforge_07[bench,causal,dev,eval,pandas,playground,providers,train,openenv]"
        in project["optional-dependencies"]["all"]
    )
    assert project["scripts"]["dataforge"] == "dataforge.cli:app"
    assert project["scripts"]["dataforge15"] == "dataforge.cli:app"
    assert pyproject["tool"]["setuptools"]["packages"]["find"]["include"] == [
        "dataforge",
        "dataforge.*",
    ]
    assert pyproject["tool"]["setuptools"]["packages"]["find"]["exclude"] == [
        "data_quality_env",
        "data_quality_env.*",
    ]


def test_mcp_distribution_uses_dataforge07_distribution_and_legacy_alias() -> None:
    """The MCP side package publishes under the PyPI-safe distribution name."""
    pyproject = _load_pyproject(PROJECT_ROOT / "dataforge-mcp" / "pyproject.toml")
    project = pyproject["project"]

    assert project["name"] == "dataforge_07_mcp"
    assert "dataforge_07>=0.1.0,<0.2" in project["dependencies"]
    assert project["scripts"]["dataforge-mcp"] == "dataforge_mcp.server:main"
    assert project["scripts"]["dataforge15-mcp"] == "dataforge_mcp.server:main"
