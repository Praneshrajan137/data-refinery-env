"""Tests for the private Kaggle GRPO smoke handoff bundle."""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path

from scripts.remote import kaggle_grpo_smoke, prepare_kaggle_grpo_smoke


def _write(path: Path, payload: str = "{}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return path


def test_kaggle_grpo_smoke_bundle_is_private_and_no_upload(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(prepare_kaggle_grpo_smoke, "SOURCE_ROOTS", ("pyproject.toml",))
    dataset_dir = tmp_path / "dataset"
    kernel_dir = tmp_path / "kernel"

    report = prepare_kaggle_grpo_smoke.build_bundles(
        dataset_dir=dataset_dir,
        kernel_dir=kernel_dir,
        trajectory=_write(
            tmp_path / "expert_v4_candidate.jsonl", '{"schema_version":"expert_v4"}\n'
        ),
        split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
        readiness_report=_write(tmp_path / "grpo_readiness_05b_candidate.json"),
        grpo_config=_write(tmp_path / "grpo_05b.yaml", "schema_version: grpo_05b_v1\n"),
    )

    dataset_metadata = json.loads((dataset_dir / "dataset-metadata.json").read_text())
    kernel_metadata = json.loads((kernel_dir / "kernel-metadata.json").read_text())
    smoke_manifest = json.loads((dataset_dir / "smoke_manifest.json").read_text())

    assert report["dataset_id"] == "praneshrajan15/dataforge-grpo-smoke-handoff"
    assert report["kernel_id"] == "praneshrajan15/dataforge-0-5b-grpo-smoke"
    assert dataset_metadata["id"] == report["dataset_id"]
    assert kernel_metadata["is_private"] == "true"
    assert kernel_metadata["enable_gpu"] == "true"
    assert kernel_metadata["machine_shape"] == "NvidiaTeslaT4"
    assert kernel_metadata["dataset_sources"] == [report["dataset_id"]]
    assert smoke_manifest["model_upload_allowed"] is False
    assert smoke_manifest["public_claim_update_allowed"] is False
    assert "source.zip" in smoke_manifest["files"]
    with zipfile.ZipFile(dataset_dir / "source.zip") as archive:
        names = set(archive.namelist())
    assert "pyproject.toml" in names
    assert all(".kaggle" not in name and "credentials" not in name for name in names)


def test_kaggle_grpo_smoke_bundle_uses_configured_v7_curriculum_and_predecessor(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(prepare_kaggle_grpo_smoke, "SOURCE_ROOTS", ("pyproject.toml",))
    dataset_dir = tmp_path / "dataset"
    kernel_dir = tmp_path / "kernel"

    prepare_kaggle_grpo_smoke.build_bundles(
        dataset_dir=dataset_dir,
        kernel_dir=kernel_dir,
        trajectory=_write(
            tmp_path / "expert_v7_parse_latch.jsonl",
            '{"schema_version":"expert_v4","curriculum_version":"expert_v7_parse_latch"}\n',
        ),
        split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
        readiness_report=_write(tmp_path / "grpo_readiness_05b_candidate.json"),
        grpo_config=_write(
            tmp_path / "grpo_05b_v3.yaml",
            "schema_version: grpo_05b_v3\n"
            "readiness:\n"
            "  trajectory_filename: expert_v7_parse_latch.jsonl\n"
            "  split_manifest_filename: split_manifest_v4.json\n",
        ),
        sft_predecessor_report=_write(
            tmp_path / "sft_v7_candidate_eval_report.json",
            '{"ok":true,"status":"pass","promote_to_grpo":true}\n',
        ),
    )

    smoke_manifest = json.loads((dataset_dir / "smoke_manifest.json").read_text())
    assert (dataset_dir / "expert_v7_parse_latch.jsonl").exists()
    assert (dataset_dir / "sft_v7_candidate_eval_report.json").exists()
    assert not (dataset_dir / "expert_v4.jsonl").exists()
    assert smoke_manifest["trajectory_file"] == "expert_v7_parse_latch.jsonl"
    assert smoke_manifest["sft_predecessor_report_file"] == "sft_v7_candidate_eval_report.json"


def test_kaggle_grpo_smoke_bundle_uses_v9_predecessor_for_v4(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(prepare_kaggle_grpo_smoke, "SOURCE_ROOTS", ("pyproject.toml",))
    dataset_dir = tmp_path / "dataset"
    kernel_dir = tmp_path / "kernel"

    prepare_kaggle_grpo_smoke.build_bundles(
        dataset_dir=dataset_dir,
        kernel_dir=kernel_dir,
        trajectory=_write(
            tmp_path / "expert_v9_action_envelope.jsonl",
            '{"schema_version":"expert_v4","curriculum_version":"expert_v9_action_envelope"}\n',
        ),
        split_manifest=_write(tmp_path / "split_manifest_v4_candidate.json"),
        readiness_report=_write(tmp_path / "grpo_readiness_05b_candidate.json"),
        grpo_config=_write(
            tmp_path / "grpo_05b_v4.yaml",
            "schema_version: grpo_05b_v4\n"
            "readiness:\n"
            "  trajectory_filename: expert_v9_action_envelope.jsonl\n"
            "  split_manifest_filename: split_manifest_v4.json\n",
        ),
        sft_predecessor_report=_write(
            tmp_path / "sft_v9_candidate_eval_report.json",
            '{"ok":true,"status":"pass","promote_to_grpo":true}\n',
        ),
    )

    smoke_manifest = json.loads((dataset_dir / "smoke_manifest.json").read_text())
    assert (dataset_dir / "expert_v9_action_envelope.jsonl").exists()
    assert (dataset_dir / "sft_v9_candidate_eval_report.json").exists()
    assert smoke_manifest["trajectory_file"] == "expert_v9_action_envelope.jsonl"
    assert smoke_manifest["sft_predecessor_report_file"] == "sft_v9_candidate_eval_report.json"


def test_prompt_token_counter_handles_mapping_chat_template_output() -> None:
    class MappingTokenizer:
        chat_template = "template"

        def apply_chat_template(self, messages, *, tokenize, add_generation_prompt):
            assert messages
            assert tokenize is True
            assert add_generation_prompt is True
            return {"input_ids": [1, 2, 3, 4, 5]}

    assert (
        kaggle_grpo_smoke._count_prompt_tokens(
            MappingTokenizer(),
            [{"role": "user", "content": "hello"}],
        )
        == 5
    )


def test_prompt_token_counter_handles_object_chat_template_output() -> None:
    @dataclass
    class Tokenized:
        input_ids: list[int]

    class ObjectTokenizer:
        chat_template = "template"

        def apply_chat_template(self, messages, *, tokenize, add_generation_prompt):
            assert messages
            assert tokenize is True
            assert add_generation_prompt is True
            return Tokenized(input_ids=[1, 2, 3, 4])

    assert (
        kaggle_grpo_smoke._count_prompt_tokens(
            ObjectTokenizer(),
            [{"role": "user", "content": "hello"}],
        )
        == 4
    )


def test_prompt_token_counter_handles_plain_token_list_and_fallback() -> None:
    class PlainTemplateTokenizer:
        chat_template = "template"

        def apply_chat_template(self, messages, *, tokenize, add_generation_prompt):
            assert messages
            assert tokenize is True
            assert add_generation_prompt is True
            return [1, 2, 3]

    class FallbackTokenizer:
        chat_template = None

        def __call__(self, text, *, add_special_tokens):
            assert text
            assert add_special_tokens is True
            return {"input_ids": [1, 2, 3, 4, 5, 6]}

    assert (
        kaggle_grpo_smoke._count_prompt_tokens(
            PlainTemplateTokenizer(),
            [{"role": "user", "content": "hello"}],
        )
        == 3
    )
    assert (
        kaggle_grpo_smoke._count_prompt_tokens(
            FallbackTokenizer(),
            [{"role": "system", "content": "rules"}, {"role": "user", "content": "hello"}],
        )
        == 6
    )
