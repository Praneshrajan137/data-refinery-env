"""Guard: the archived subsystem must stay out of the product.

`training/` was moved to `archive/training/` on 2026-09-07 because it never passed its
own gate and never contributed a write (best `sft_f1` 0.0202; the v7 candidate proposed
nothing on 576 opportunities). A move alone does not keep it out: nothing stopped a
future edit from importing `archive.*` back into `dataforge/`, at which point the
excision would be undone silently and the product would once again depend on code that
is excluded from `ruff`, from `mypy --strict`, and from the distribution.

These tests are the mechanism. They are deliberately structural -- they read the source
tree rather than trusting a convention -- because PRODUCT.md 1.3 asks for the population
of a gate to be derived rather than restated.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_PRODUCT = _ROOT / "dataforge"
_ARCHIVE = _ROOT / "archive"


def _imported_modules(source: str) -> set[str]:
    """Return every module name imported by a Python source file."""
    names: set[str] = set()
    try:
        tree = ast.parse(source)
    except SyntaxError:  # pragma: no cover - the product must parse for other gates anyway
        return names
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _product_modules() -> list[Path]:
    """Every shipped Python module, derived from the packaged directory."""
    return sorted(_PRODUCT.rglob("*.py"))


def test_the_archive_exists_and_declares_itself() -> None:
    """Non-vacuity: the two tests below prove nothing if the archive is absent."""
    assert _ARCHIVE.is_dir(), "archive/ is missing; the excision tests would pass vacuously"
    readme = _ARCHIVE / "README.md"
    assert readme.is_file(), "archive/README.md is missing; the freeze must be legible"
    text = readme.read_text(encoding="utf-8")
    assert "is part of the DataForge product" in text, (
        "archive/README.md must state the product status explicitly"
    )
    assert "0.0202" in text, "the archive must carry the evidence that justified excising it"
    assert (_ARCHIVE / "training").is_dir()


def test_no_product_module_imports_the_archive() -> None:
    """The shipped package must not depend on archived code.

    `archive/` is excluded from lint, from strict typing, and from the sdist. A product
    module importing it would therefore depend on code that no gate checks and that no
    installed wheel contains -- an ImportError for every user, invisible here.
    """
    offenders: list[str] = []
    for module in _product_modules():
        for imported in _imported_modules(module.read_text(encoding="utf-8")):
            if imported == "archive" or imported.startswith("archive."):
                offenders.append(f"{module.relative_to(_ROOT).as_posix()} imports {imported}")

    assert not offenders, (
        "Product modules must not import archived code. The archive is excluded from "
        f"ruff, mypy --strict and the distribution: {offenders}"
    )


def test_the_archive_is_refused_by_the_sdist_gate() -> None:
    """`archive/` must be one of the prefixes the release gate refuses in an sdist."""
    from dataforge.release.gate import REJECTED_SDIST_PREFIXES

    assert "archive/" in REJECTED_SDIST_PREFIXES, (
        "archive/ must be refused in the sdist, or archived code ships to users"
    )


def test_the_import_scanner_can_actually_detect_an_offender() -> None:
    """A gate that cannot fail is indistinguishable from no gate."""
    assert _imported_modules("import archive.training.grpo_config") == {
        "archive.training.grpo_config"
    }
    assert _imported_modules("from archive.training import grpo_config") == {"archive.training"}
    # ...and it does not fire on an unrelated module whose name merely contains the word.
    assert not any(
        name == "archive" or name.startswith("archive.")
        for name in _imported_modules("from dataforge.archiver import thing")
    )


@pytest.mark.parametrize(
    "path",
    [
        "archive/training/grpo_contract.py",
        "archive/training/gigpo_advantage.py",
    ],
)
def test_modules_with_a_live_consumer_survived_the_move(path: str) -> None:
    """The two archived modules that DO have a consumer must still be present.

    `grpo_contract.py` and `gigpo_advantage.py` are imported by parity tests over
    `dataforge/repair_contract.py` and `dataforge/release/model_family.py`. Deleting them
    would remove product coverage under cover of a cleanup, which is the failure mode
    this whole task was meant to avoid rather than commit.
    """
    assert (_ROOT / path).is_file(), f"{path} is imported by a product parity test"
