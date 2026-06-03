"""Release workflow guard tests for PyPI and TestPyPI publishing."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_pypi_workflow_refuses_prerelease_versions() -> None:
    """The real PyPI workflow must not publish RC metadata by accident."""
    workflow = (PROJECT_ROOT / ".github" / "workflows" / "publish-dataforge.yml").read_text(
        encoding="utf-8"
    )

    assert "Refuse prerelease versions on PyPI" in workflow
    assert '("a", "b", "rc", ".dev")' in workflow
    assert "pypa/gh-action-pypi-publish@release/v1" in workflow
    assert "repository-url: https://test.pypi.org/legacy/" not in workflow


def test_testpypi_workflow_uses_trusted_publishing_and_installed_smoke() -> None:
    """The TestPyPI workflow publishes only to TestPyPI and smokes the artifact."""
    workflow = (PROJECT_ROOT / ".github" / "workflows" / "publish-testpypi.yml").read_text(
        encoding="utf-8"
    )

    assert "workflow_dispatch:" in workflow
    assert "environment: testpypi" in workflow
    assert "id-token: write" in workflow
    assert "repository-url: https://test.pypi.org/legacy/" in workflow
    assert "--extra-index-url https://pypi.org/simple/" in workflow
    assert "dataforge_07==0.1.0" in workflow
    assert "scripts/ci/installed_cli_smoke.py" in workflow
    assert "dataforge-testpypi-installed-cli-smoke" in workflow


def test_all_dataforge07_publish_workflows_exist_and_use_oidc() -> None:
    """Every public distribution must have TestPyPI and PyPI Trusted Publishing workflows."""
    workflows = {
        "dataforge_07": ("publish-testpypi.yml", "publish-dataforge.yml", "v*"),
        "dataforge_07_mcp": (
            "publish-dataforge-mcp-testpypi.yml",
            "publish-dataforge-mcp.yml",
            "dataforge-mcp-v*",
        ),
        "dataforge_07_evals": (
            "publish-dataforge-evals-testpypi.yml",
            "publish-dataforge-evals.yml",
            "dataforge-evals-v*",
        ),
        "dataforge_07_dbt": (
            "publish-dataforge-dbt-testpypi.yml",
            "publish-dataforge-dbt.yml",
            "dataforge-dbt-v*",
        ),
        "dataforge_07_agent_patterns": (
            "publish-dataforge-agent-patterns-testpypi.yml",
            "publish-dataforge-agent-patterns.yml",
            "dataforge-agent-patterns-v*",
        ),
    }
    for package, (testpypi_name, pypi_name, tag_glob) in workflows.items():
        testpypi = (PROJECT_ROOT / ".github" / "workflows" / testpypi_name).read_text(
            encoding="utf-8"
        )
        pypi = (PROJECT_ROOT / ".github" / "workflows" / pypi_name).read_text(encoding="utf-8")

        assert "environment: testpypi" in testpypi
        assert "id-token: write" in testpypi
        assert "repository-url: https://test.pypi.org/legacy/" in testpypi
        assert package in testpypi
        assert "workflow_dispatch:" in testpypi

        assert "environment: pypi" in pypi
        assert "id-token: write" in pypi
        assert "repository-url: https://test.pypi.org/legacy/" not in pypi
        assert package in pypi
        assert tag_glob in pypi
