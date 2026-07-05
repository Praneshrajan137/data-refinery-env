"""Contract tests for the DataForge 0.5B Gradio Space source tree."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
import yaml

SPACE_ROOT = Path(__file__).resolve().parents[2] / "playground-model"


def _load_space_app() -> ModuleType:
    """Load `playground-model/app.py` without requiring a package import name."""
    pytest.importorskip("gradio")  # app.py imports gradio at module level
    spec = importlib.util.spec_from_file_location(
        "dataforge_model_space_app", SPACE_ROOT / "app.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestModelSpaceContract:
    """Static and parser contracts for the model Space."""

    def test_readme_frontmatter_uses_supported_gradio_keys(self) -> None:
        readme = (SPACE_ROOT / "README.md").read_text(encoding="utf-8")
        frontmatter = readme.split("---", 2)[1]
        metadata = yaml.safe_load(frontmatter)

        assert metadata["sdk"] == "gradio"
        assert metadata["app_file"] == "app.py"
        assert "hardware" not in metadata
        assert metadata["models"] == [
            "Praneshrajan15/DataForge-0.5B-GRPO",
            "Praneshrajan15/DataForge-0.5B-SFT",
        ]

    def test_requirements_do_not_include_model_weights_or_caches(self) -> None:
        requirements = [
            line
            for line in (SPACE_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line
        ]

        assert requirements == ["transformers", "accelerate", "torch"]
        assert not list(SPACE_ROOT.rglob("*.safetensors"))
        assert not list(SPACE_ROOT.rglob("*.bin"))

    def test_csv_parser_rejects_more_than_50_rows(self) -> None:
        app = _load_space_app()
        csv_text = "id,amount\n" + "\n".join(f"{index},{index}" for index in range(51))

        ok, message, rows = app.parse_csv_snippet(csv_text)

        assert ok is False
        assert rows == []
        assert "at most 50" in message

    def test_empty_csv_returns_user_facing_error_row(self) -> None:
        app = _load_space_app()

        rows = app.detect_and_propose("")

        assert rows[0][0] == "error"
        assert "csv snippet" in rows[0][-1].lower()

    def test_model_output_parser_handles_non_json_text(self) -> None:
        app = _load_space_app()

        rows = app.parse_model_output("not json")

        assert rows == [["raw", "", "", "", "", "", "", "not json"]]

    def test_model_output_parser_returns_stable_json_rows(self) -> None:
        app = _load_space_app()

        rows = app.parse_model_output(
            '{"fixes":[{"row":3,"column":"amount","issue_type":"decimal_shift",'
            '"old_value":"1020","new_value":"102","confidence":0.91,'
            '"reason":"10x outlier"}]}'
        )

        assert rows == [
            ["proposed", "3", "amount", "decimal_shift", "1020", "102", "0.91", "10x outlier"]
        ]
