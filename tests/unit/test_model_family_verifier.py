"""Tests for model-family v2 report assembly and card repair."""

from __future__ import annotations

from pathlib import Path

from dataforge.release.model_family import FAMILY_REPORT_SCHEMA_VERSION, load_model_family_manifest
from scripts.model.repair_hf_model_card_metadata import repair_model_card_metadata
from scripts.model.verify_model_family import build_policy_report, validate_family_report


def test_policy_report_is_v2_and_not_complete_until_all_rows_verify() -> None:
    manifest = load_model_family_manifest()

    report = build_policy_report(manifest, source_git_commit="abc1234")
    errors = validate_family_report(report, manifest)

    assert report["schema_version"] == FAMILY_REPORT_SCHEMA_VERSION
    assert len(report["models"]) == len(manifest.entries)
    assert any("DataForge-1.5B-SFT" in error for error in errors)
    assert any("artifact_status must be public" in error for error in errors)


def test_model_card_repair_adds_dataset_base_license_and_model_index(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(
        "---\nlicense: apache-2.0\nbase_model: Qwen/Qwen2.5-0.5B-Instruct\n---\n\n# Card\n",
        encoding="utf-8",
    )

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        assert filename == "README.md"
        assert repo_type == "model"
        return str(readme)

    result = repair_model_card_metadata(
        repo_id="Praneshrajan15/DataForge-0.5B-SFT",
        apply=False,
        downloader=downloader,
    )

    assert result.changed is True
    assert result.applied is False
    assert "datasets" in result.changed_fields
    assert "model-index" in result.changed_fields
