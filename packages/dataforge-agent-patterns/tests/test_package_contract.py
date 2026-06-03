"""Package-level contract tests."""

from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src" / "dataforge_agent_patterns"


def test_no_main_dataforge_imports() -> None:
    """The package must not import the main dataforge package."""
    for path in SOURCE_ROOT.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            assert all(name != "dataforge" and not name.startswith("dataforge.") for name in names)


def test_primitive_files_under_200_loc() -> None:
    """Each primitive implementation remains small enough to audit."""
    primitive_files = [
        "progressive.py",
        "constitutional.py",
        "transaction.py",
        "smt.py",
        "cascade.py",
    ]
    for filename in primitive_files:
        path = SOURCE_ROOT / filename
        loc = len(path.read_text(encoding="utf-8").splitlines())
        assert loc < 200, f"{filename} has {loc} LOC"
