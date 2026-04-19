"""Regression smoke tests for the Week 0 scaffold."""

import importlib


def test_import_dataforge_succeeds() -> None:
    """Verify that the top-level package imports successfully."""
    module = importlib.import_module("dataforge")
    assert module is not None
