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
    # `exclude` is deliberately absent. It used to name the deleted `data_quality_env`
    # hackathon package; `include` is already an allowlist, so re-adding an exclude for a
    # package that no longer exists would assert over an empty population.
    assert "exclude" not in pyproject["tool"]["setuptools"]["packages"]["find"]


def test_mcp_distribution_uses_dataforge07_distribution_and_legacy_alias() -> None:
    """The MCP side package publishes under the PyPI-safe distribution name."""
    pyproject = _load_pyproject(PROJECT_ROOT / "dataforge-mcp" / "pyproject.toml")
    project = pyproject["project"]

    assert project["name"] == "dataforge_07_mcp"
    assert "dataforge_07>=0.1.0,<0.2" in project["dependencies"]
    assert project["scripts"]["dataforge-mcp"] == "dataforge_mcp.server:main"
    assert project["scripts"]["dataforge15-mcp"] == "dataforge_mcp.server:main"


def test_monorepo_side_packages_use_dataforge07_distribution_names() -> None:
    """The imported side packages publish under the PyPI-safe distribution family."""
    package_expectations = {
        "dataforge-evals": {
            "name": "dataforge_07_evals",
            "script": ("dataforge-evals", "dataforge_evals.cli:app"),
            "dependency": "dataforge_07>=0.1.0",
        },
        "dataforge-dbt": {
            "name": "dataforge_07_dbt",
            "script": ("dataforge-dbt", "dataforge_dbt.dispatch:main"),
            "dependency": "dataforge_07>=0.1.0,<0.2",
        },
        "dataforge-agent-patterns": {
            "name": "dataforge_07_agent_patterns",
            "script": None,
            "dependency": None,
        },
    }
    for package_dir, expectation in package_expectations.items():
        pyproject = _load_pyproject(PROJECT_ROOT / "packages" / package_dir / "pyproject.toml")
        project = pyproject["project"]
        assert project["name"] == expectation["name"]
        script = expectation["script"]
        if script is not None:
            script_name, entrypoint = script
            assert project["scripts"][script_name] == entrypoint
        dependency = expectation["dependency"]
        if dependency is not None:
            dependency_text = "\n".join(project.get("dependencies", []))
            optional_text = "\n".join(
                dependency
                for dependencies in project.get("optional-dependencies", {}).values()
                for dependency in dependencies
            )
            assert dependency in f"{dependency_text}\n{optional_text}"
