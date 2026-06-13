"""Tests for the private Kaggle SFT-v5 handoff bundle."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from scripts.remote import prepare_kaggle_sft_v5_candidate


def _write(path: Path, payload: str = "{}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return path


def test_kaggle_sft_v5_bundle_is_private_and_clean(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(prepare_kaggle_sft_v5_candidate, "SOURCE_ROOTS", ("pyproject.toml",))
    dataset_dir = tmp_path / "dataset"
    kernel_dir = tmp_path / "kernel"

    report = prepare_kaggle_sft_v5_candidate.build_bundles(
        dataset_dir=dataset_dir,
        kernel_dir=kernel_dir,
        trajectory=_write(
            tmp_path / "expert_v5_repair_curriculum.jsonl",
            '{"schema_version":"expert_v4","curriculum_version":"expert_v5_repair_curriculum"}\n',
        ),
        split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
        curriculum_report=_write(
            tmp_path / "sft_v5_repair_curriculum_report.json",
            '{"schema_version":"dataforge_sft_v5_repair_curriculum_report_v1","ok":true}',
        ),
        sft_config=_write(
            tmp_path / "sft_05b_v5.yaml",
            "schema_version: sft_05b_v5\n"
            "repos:\n"
            "  trajectory_filename: expert_v5_repair_curriculum.jsonl\n"
            "  split_manifest_filename: split_manifest_v4.json\n",
        ),
    )

    dataset_metadata = json.loads((dataset_dir / "dataset-metadata.json").read_text())
    kernel_metadata = json.loads((kernel_dir / "kernel-metadata.json").read_text())
    sft_manifest = json.loads((dataset_dir / "sft_v5_manifest.json").read_text())

    assert report["dataset_id"] == "praneshrajan15/dataforge-sft-v5-handoff"
    assert report["kernel_id"] == "praneshrajan15/dataforge-0-5b-sft-v5-candidate"
    assert dataset_metadata["id"] == report["dataset_id"]
    assert kernel_metadata["is_private"] == "true"
    assert kernel_metadata["enable_gpu"] == "true"
    assert kernel_metadata["machine_shape"] == "NvidiaTeslaT4"
    assert kernel_metadata["dataset_sources"] == [report["dataset_id"]]
    assert sft_manifest["private_candidate_upload_allowed_after_gate"] is True
    assert sft_manifest["public_claim_update_allowed"] is False
    assert sft_manifest["trajectory_file"] == "expert_v5_repair_curriculum.jsonl"
    assert sft_manifest["split_manifest_file"] == "split_manifest_v4.json"
    assert (dataset_dir / "expert_v5_repair_curriculum.jsonl").exists()
    assert (dataset_dir / "split_manifest_v4.json").exists()
    assert (kernel_dir / "dataforge-0-5b-sft-v5-candidate.py").exists()
    with zipfile.ZipFile(dataset_dir / "source.zip") as archive:
        names = set(archive.namelist())
    assert "pyproject.toml" in names
    assert all(".kaggle" not in name and "credentials" not in name for name in names)


def test_kaggle_sft_v5_bundle_rejects_wrong_config_version(tmp_path: Path) -> None:
    config = _write(tmp_path / "sft.yaml", "schema_version: sft_05b_v4\n")

    try:
        prepare_kaggle_sft_v5_candidate.build_bundles(
            dataset_dir=tmp_path / "dataset",
            kernel_dir=tmp_path / "kernel",
            trajectory=_write(tmp_path / "expert_v5_repair_curriculum.jsonl"),
            split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
            curriculum_report=_write(tmp_path / "sft_v5_repair_curriculum_report.json"),
            sft_config=config,
        )
    except ValueError as exc:
        assert "schema_version=sft_05b_v5" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected wrong config version to be rejected.")
