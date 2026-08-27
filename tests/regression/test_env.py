"""Regression smoke tests for the package import boundary."""

import importlib
import importlib.util
from pathlib import Path


def test_import_dataforge_succeeds() -> None:
    """Verify that the top-level package imports successfully."""
    module = importlib.import_module("dataforge")
    assert module is not None


def test_legacy_hackathon_package_stays_deleted() -> None:
    """Refuse the reintroduction of the ``data_quality_env`` compatibility package.

    Until 2026-08-27 two tests here asserted the OPPOSITE -- that ``data_quality_env`` and
    its submodules still imported. That package was the original OpenEnv RL hackathon
    environment, kept alive as a frozen compatibility shim long after ``dataforge.env``
    superseded it. It was excluded from the wheel, excluded from the sdist, exempt from
    mypy, and blanket-exempt from twelve ruff rule families, so nothing checked it while
    everything carried it.

    Deleting it without replacing this test would have left the repo with no opinion at
    all about the name. A deletion that only removes the assertion keeping something alive
    is half a decision: the next person to add a root-level shim would meet no resistance.
    So the guard is inverted rather than dropped, which also makes the release gate's
    ``find_spec('data_quality_env') is None`` assertion true by construction rather than
    by luck.
    """
    assert importlib.util.find_spec("data_quality_env") is None


def test_repo_root_is_not_a_python_package() -> None:
    """Prevent reintroducing the repo-root package boundary that breaks CI checkouts."""
    repo_root = Path(__file__).resolve().parents[2]
    assert not (repo_root / "__init__.py").exists()


def test_repo_root_carries_no_loose_python_modules() -> None:
    """Refuse a new root-level ``.py`` shim.

    The deleted lineage was ~20 loose root scripts that existed only so ``import models``
    or ``import benchmark`` would resolve from the repo root, and the release gate policed
    them by enumerating fourteen filenames. This gates the CLASS instead, so a shim named
    something nobody predicted is caught too.

    Note on why the root stays importable: ``pyproject.toml`` keeps ``pythonpath = ["."]``.
    Removing it was considered as the structural fix and REJECTED on evidence -- 51 test
    modules do ``from scripts... import ...`` and ``tests/conftest.py`` performs no
    ``sys.path`` manipulation, so dropping it breaks the legitimate consumer while the
    shims it once enabled are already gone. Policing the files directly costs nothing and
    does not take the rest of the suite hostage.
    """
    repo_root = Path(__file__).resolve().parents[2]
    loose = sorted(p.name for p in repo_root.glob("*.py"))
    assert loose == [], f"unexpected loose root-level Python modules: {loose}"
