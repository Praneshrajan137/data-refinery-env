"""Tests for targeted Kaggle JSON evidence fetches."""

from __future__ import annotations

from pathlib import Path

from scripts.remote.fetch_kaggle_json_reports import (
    DEFAULT_REPORT_PATTERN,
    build_kaggle_output_command,
)


def test_kaggle_report_fetch_uses_file_pattern_without_model_weights(tmp_path: Path) -> None:
    command = build_kaggle_output_command(
        kernel="owner/kernel",
        output_dir=tmp_path / "reports",
        kaggle_cli=Path("kaggle"),
    )

    assert command[:3] == ["kaggle", "kernels", "output"]
    assert "--file-pattern" in command
    assert DEFAULT_REPORT_PATTERN in command
    assert "safetensors" not in DEFAULT_REPORT_PATTERN
    assert "bin" not in DEFAULT_REPORT_PATTERN
