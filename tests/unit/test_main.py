"""Test the dataforge __main__ module can be invoked."""

from __future__ import annotations

import subprocess
import sys


def test_main_help_runs() -> None:
    """``python -m dataforge --help`` exits cleanly."""
    result = subprocess.run(
        [sys.executable, "-m", "dataforge", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "DataForge" in result.stdout or "profile" in result.stdout
