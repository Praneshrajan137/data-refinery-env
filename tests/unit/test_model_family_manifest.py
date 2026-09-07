"""Tests for the manifest-driven Hugging Face model-family policy."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace

from archive.training.gigpo_advantage import (
    EpisodeRollout,
    canonical_observation_hash,
    compute_gigpo_advantages,
)
from dataforge.release.model_family import (
    FAMILY_REPORT_SCHEMA_VERSION,
    MODEL_FAMILY_SIZES,
    MODEL_FAMILY_STAGES,
    ModelFamilyManifest,
    build_hub_upload_manifest,
    license_matches,
    load_model_family_manifest,
    render_stage_config,
    resolve_base_license,
)


@dataclass(frozen=True)
class _ModelInfo:
    card_data: dict[str, object]
    sha: str | None = "base-sha"

    def __getattr__(self, name: str) -> object:
        if name == "cardData":
            return self.card_data
        raise AttributeError(name)


class _FakeBaseModelApi:
    def model_info(self, repo_id: str, *, token: str | None = None) -> _ModelInfo:
        return _ModelInfo(
            card_data={
                "license": "other",
                "license_name": "qwen-research",
                "license_link": "https://huggingface.co/Qwen/Qwen2.5-3B-Instruct/blob/main/LICENSE",
            }
        )


def test_model_family_manifest_expands_complete_matrix_and_3b_license() -> None:
    manifest = load_model_family_manifest()

    assert len(manifest.entries) == len(MODEL_FAMILY_SIZES) * len(MODEL_FAMILY_STAGES)
    assert "Praneshrajan15/DataForge-7B-GiGPO" in manifest.repo_ids()
    row = manifest.entry_for(size="3B", stage="SFT")
    assert row.base_model == "Qwen/Qwen2.5-3B-Instruct"
    assert row.upstream_license == "qwen-research"
    assert row.hub_license == "other"
    assert row.license_name == "qwen-research"
    assert license_matches("other", "qwen-research", license_name="qwen-research")


def test_model_family_manifest_refuses_verified_child_before_parent() -> None:
    manifest = load_model_family_manifest()
    entries = list(manifest.entries)
    bad_entries = tuple(
        replace(entry, artifact_status="public", quality_status="quality_improved_verified")
        if entry.repo_id == "Praneshrajan15/DataForge-1.5B-GRPO"
        else entry
        for entry in entries
    )
    bad_manifest = ModelFamilyManifest(
        schema_version=manifest.schema_version,
        hf_owner=manifest.hf_owner,
        dataset_repo=manifest.dataset_repo,
        entries=bad_entries,
        source_path=None,
    )

    errors = bad_manifest.dependency_errors()

    assert errors
    assert "DataForge-1.5B-SFT" in errors[0]


def test_qwen_research_license_resolution_from_hub_card() -> None:
    license_info = resolve_base_license("Qwen/Qwen2.5-3B-Instruct", api=_FakeBaseModelApi())

    assert license_info.license == "other"
    assert license_info.license_name == "qwen-research"
    assert license_info.source_sha == "base-sha"


def test_rendered_configs_encode_backends_and_predecessors() -> None:
    manifest = load_model_family_manifest()
    grpo_3b = manifest.entry_for(size="3B", stage="GRPO")
    gigpo_7b = manifest.entry_for(size="7B", stage="GiGPO")

    grpo_config = render_stage_config(grpo_3b, dataset_repo=manifest.dataset_repo)
    gigpo_config = render_stage_config(gigpo_7b, dataset_repo=manifest.dataset_repo)

    assert grpo_config["model"]["model_license"] == "qwen-research"
    assert grpo_config["model"]["sft_checkpoint"] == "Praneshrajan15/DataForge-3B-SFT"
    assert grpo_config["training"]["report_to"] == "trackio"
    assert grpo_config["training"]["prompt_token_budget"] == 1280
    assert grpo_config["evaluation"]["source"] == "pinned_dataforge_registry"
    assert grpo_config["training_sequence"]["stages"][1]["max_steps"] == 500
    assert gigpo_config["model"]["grpo_checkpoint"] == "Praneshrajan15/DataForge-7B-GRPO"
    assert gigpo_config["training"]["anchor_state_grouping"] == "canonical_observation_hash"


def test_hub_upload_manifest_uses_v2_evidence_fields() -> None:
    manifest = load_model_family_manifest()
    entry = manifest.entry_for(size="0.5B", stage="SFT")

    row = build_hub_upload_manifest(
        entry,
        dataset_repo=manifest.dataset_repo,
        dataset_sha="dataset-sha",
        model_sha="model-sha",
        source_git_commit="abc1234",
        training_run_url="https://huggingface.co/jobs/Praneshrajan15/example",
        eval_report_path="models/DataForge-0.5B-SFT.eval.json",
        verification_report_path="models/DataForge-0.5B-SFT.verification.json",
        eval_metrics={"macro_f1": 0.1},
    )

    assert FAMILY_REPORT_SCHEMA_VERSION == "dataforge_model_family_report_v2"
    assert row["artifact_status"] == "public"
    assert row["quality_status"] == "quality_improved_verified"
    assert row["dataset_sha"] == "dataset-sha"
    assert row["eval_metrics"] == {"macro_f1": 0.1}


def test_gigpo_advantages_group_by_canonical_observation() -> None:
    observation = {"rows": [{"b": 2, "a": 1.0}], "columns": ["a", "b"]}
    same_observation = json.loads(json.dumps({"columns": ["a", "b"], "rows": [{"a": 1, "b": 2}]}))
    rollouts = (
        EpisodeRollout("a", observation, 1.0, (0.5, 1.0)),
        EpisodeRollout("b", same_observation, 0.0, (0.0, 0.0)),
    )

    advantages = compute_gigpo_advantages(rollouts)

    assert canonical_observation_hash(observation) == canonical_observation_hash(same_observation)
    assert [round(item.macro_episode_advantage, 4) for item in advantages] == [0.5, -0.5]
    assert [
        tuple(round(value, 4) for value in item.micro_step_advantages) for item in advantages
    ] == [
        (0.25, 0.5),
        (-0.25, -0.5),
    ]
