"""Tests for the private Kaggle GRPO candidate handoff bundle."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from scripts.remote import prepare_kaggle_grpo_candidate


def _write(path: Path, payload: str = "{}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return path


def test_kaggle_grpo_candidate_bundle_is_private_publish_eligible_and_clean(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(prepare_kaggle_grpo_candidate, "SOURCE_ROOTS", ("pyproject.toml",))
    dataset_dir = tmp_path / "dataset"
    kernel_dir = tmp_path / "kernel"

    report = prepare_kaggle_grpo_candidate.build_bundles(
        dataset_dir=dataset_dir,
        kernel_dir=kernel_dir,
        trajectory=_write(
            tmp_path / "expert_v4_candidate.jsonl", '{"schema_version":"expert_v4"}\n'
        ),
        split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
        readiness_report=_write(tmp_path / "grpo_readiness_05b_candidate.json"),
        grpo_config=_write(tmp_path / "grpo_05b.yaml", "schema_version: grpo_05b_v1\n"),
        smoke_report=_write(
            tmp_path / "kaggle_grpo_smoke_report.json",
            '{"schema_version":"dataforge_kaggle_grpo_smoke_report_v1","status":"pass"}',
        ),
        smoke_validation=_write(
            tmp_path / "smoke_validation.json",
            '{"schema_version":"dataforge_grpo_smoke_validation_v1","ok":true,"blockers":[]}',
        ),
    )

    dataset_metadata = json.loads((dataset_dir / "dataset-metadata.json").read_text())
    kernel_metadata = json.loads((kernel_dir / "kernel-metadata.json").read_text())
    candidate_manifest = json.loads((dataset_dir / "candidate_manifest.json").read_text())

    assert report["dataset_id"] == "praneshrajan15/dataforge-grpo-candidate-handoff"
    assert report["kernel_id"] == "praneshrajan15/dataforge-0-5b-grpo-candidate"
    assert dataset_metadata["id"] == report["dataset_id"]
    assert kernel_metadata["is_private"] == "true"
    assert kernel_metadata["id"] == report["kernel_id"]
    assert kernel_metadata["id_no"] == 121775487
    assert kernel_metadata["code_file"] == "dataforge-0-5b-grpo-candidate.py"
    assert kernel_metadata["machine_shape"] == "NvidiaTeslaT4"
    assert kernel_metadata["dataset_sources"] == [report["dataset_id"]]
    assert candidate_manifest["model_upload_allowed_after_gate"] is True
    assert candidate_manifest["public_claim_update_allowed"] is False
    assert "kaggle_grpo_smoke_report.json" in candidate_manifest["files"]
    assert "smoke_validation.json" in candidate_manifest["files"]
    with zipfile.ZipFile(dataset_dir / "source.zip") as archive:
        names = set(archive.namelist())
    assert "pyproject.toml" in names
    assert all(".kaggle" not in name and "credentials" not in name for name in names)


def test_kaggle_grpo_candidate_bundle_uses_configured_v6_curriculum_and_predecessor(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(prepare_kaggle_grpo_candidate, "SOURCE_ROOTS", ("pyproject.toml",))
    dataset_dir = tmp_path / "dataset"
    kernel_dir = tmp_path / "kernel"

    prepare_kaggle_grpo_candidate.build_bundles(
        dataset_dir=dataset_dir,
        kernel_dir=kernel_dir,
        trajectory=_write(
            tmp_path / "expert_v6_contract_minimal.jsonl",
            '{"schema_version":"expert_v4","curriculum_version":"expert_v6_contract_minimal"}\n',
        ),
        split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
        readiness_report=_write(tmp_path / "grpo_readiness_05b_candidate.json"),
        grpo_config=_write(
            tmp_path / "grpo_05b_v3.yaml",
            "schema_version: grpo_05b_v3\n"
            "readiness:\n"
            "  trajectory_filename: expert_v6_contract_minimal.jsonl\n"
            "  split_manifest_filename: split_manifest_v4.json\n",
        ),
        sft_predecessor_report=_write(
            tmp_path / "sft_v6_candidate_eval_report.json",
            '{"ok":true,"status":"pass","promote_to_grpo":true}\n',
        ),
        smoke_report=_write(
            tmp_path / "kaggle_grpo_smoke_report.json",
            '{"schema_version":"dataforge_kaggle_grpo_smoke_report_v1","status":"pass"}',
        ),
        smoke_validation=_write(
            tmp_path / "smoke_validation.json",
            '{"schema_version":"dataforge_grpo_smoke_validation_v1","ok":true,"blockers":[]}',
        ),
    )

    candidate_manifest = json.loads((dataset_dir / "candidate_manifest.json").read_text())
    assert (dataset_dir / "expert_v6_contract_minimal.jsonl").exists()
    assert (dataset_dir / "sft_v6_candidate_eval_report.json").exists()
    assert not (dataset_dir / "expert_v4.jsonl").exists()
    assert candidate_manifest["trajectory_file"] == "expert_v6_contract_minimal.jsonl"
    assert candidate_manifest["sft_predecessor_report_file"] == "sft_v6_candidate_eval_report.json"
