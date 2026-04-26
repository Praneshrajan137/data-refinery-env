"""Regression smoke tests for the Week 0 scaffold."""

import importlib
from pathlib import Path


def test_import_dataforge_succeeds() -> None:
    """Verify that the top-level package imports successfully."""
    module = importlib.import_module("dataforge")
    assert module is not None


def test_import_legacy_compat_package_succeeds() -> None:
    """Verify that the supported compatibility package still imports successfully."""
    module = importlib.import_module("data_quality_env")
    assert module is not None


def test_import_legacy_submodules_succeed() -> None:
    """Verify that the legacy import surface remains available."""
    importlib.import_module("data_quality_env.client")
    importlib.import_module("data_quality_env.models")
    importlib.import_module("data_quality_env.server.data_quality_environment")


def test_repo_root_is_not_a_python_package() -> None:
    """Prevent reintroducing the repo-root package boundary that breaks CI checkouts."""
    repo_root = Path(__file__).resolve().parents[2]
    assert not (repo_root / "__init__.py").exists()
