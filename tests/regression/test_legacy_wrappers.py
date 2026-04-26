"""Regression tests for preserved root-level legacy wrappers."""

from __future__ import annotations

import importlib


def test_benchmark_wrapper_delegates_to_package_main() -> None:
    """Root benchmark.py remains an executable shim over the compatibility package."""
    module = importlib.import_module("benchmark")
    package_module = importlib.import_module("data_quality_env.benchmark")
    assert module.main is package_module.main


def test_random_baseline_wrapper_delegates_to_package_run_episode() -> None:
    """Root random_baseline.py remains a thin compatibility shim."""
    module = importlib.import_module("random_baseline")
    package_module = importlib.import_module("data_quality_env.random_baseline")
    assert module.run_episode is package_module.run_episode


def test_server_wrapper_exports_environment_from_compat_package() -> None:
    """Legacy server.data_quality_environment still resolves to the nested package."""
    module = importlib.import_module("server.data_quality_environment")
    package_module = importlib.import_module("data_quality_env.server.data_quality_environment")
    assert module.DataQualityEnvironment is package_module.DataQualityEnvironment
