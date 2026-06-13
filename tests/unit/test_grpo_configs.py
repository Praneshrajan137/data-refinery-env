"""Contract tests for Week 12 GRPO training configuration."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from training.grpo_config import GrpoConfigError, build_grpo_config_kwargs, load_grpo_config

ROOT = Path(__file__).resolve().parents[2]
CONFIG_05B = ROOT / "training" / "configs" / "grpo_05b.yaml"
CONFIG_05B_V2 = ROOT / "training" / "configs" / "grpo_05b_v2.yaml"
CONFIG_05B_V3 = ROOT / "training" / "configs" / "grpo_05b_v3.yaml"
CONFIG_15B = ROOT / "training" / "configs" / "grpo_15b.yaml"


def test_grpo_05b_config_uses_supported_v1_stack_and_free_tier_hparams() -> None:
    """The 0.5B GRPO config should encode the corrected Week 12 defaults."""
    config = load_grpo_config(CONFIG_05B)

    packages = config["environment"]["pip_packages"]
    assert "torchao" in config["environment"]["uninstall_packages"]
    assert "trl==1.4.0" in packages
    assert all("==0.11" not in package for package in packages)
    assert "tensorboard==2.20.0" in packages
    assert config["model"]["sft_checkpoint"] == "Praneshrajan15/DataForge-0.5B-SFT"
    assert config["model"]["target_model_repo"].endswith("/DataForge-0.5B-GRPO")
    assert config["lora"]["r"] == 16
    assert config["training"]["fp16"] is True
    assert config["training"]["bf16"] is False
    assert config["training"]["num_generations"] == 4
    assert config["training"]["prompt_token_budget"] == 1280
    assert config["training"]["max_completion_length"] == 256
    assert config["training"]["per_device_train_batch_size"] == 1
    assert config["training"]["gradient_accumulation_steps"] == 16
    assert config["training"]["beta"] == 0.04
    assert config["training"]["learning_rate"] == pytest.approx(1e-5)
    assert config["training"]["num_iterations"] == 1
    assert config["training"]["save_steps"] == 50
    assert config["training"]["logging_steps"] == 5
    assert config["training"]["report_to"] == "tensorboard"
    assert config["reward"]["prompt_contract_version"] == "repair_contract_v2"
    assert config["readiness"]["trajectory_filename"] == "expert_v4.jsonl"
    assert config["readiness"]["split_manifest_filename"] == "split_manifest_v4.json"
    assert config["readiness"]["required_datasets"] == ["hospital", "flights", "beers"]
    assert config["readiness"]["auxiliary_datasets"] == ["hospital_synthetic_deterministic_v1"]
    assert config["readiness"]["require_source_provenance"] is True
    assert config["readiness"]["min_repair_records"] == 32
    assert config["readiness"]["min_repair_signal_domains"] == 2
    assert config["readiness"]["min_dirty_records_per_dataset"] == 0
    assert config["readiness"]["min_per_dataset_reward_std"] == pytest.approx(0.01)
    stages = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    assert stages["smoke"]["max_steps"] == 50
    assert stages["smoke"]["allow_upload"] is False
    assert stages["candidate"]["max_steps"] == 500
    assert stages["extended_candidate"]["max_steps"] == 1000
    assert config["training_sequence"]["selection_order"] == [
        "highest_strict_macro_f1",
        "lowest_schema_case_errors",
        "lowest_gpu_hours",
    ]
    assert config["release"]["min_absolute_f1_gain"] == pytest.approx(0.03)
    assert config["release"]["benchmark_seeds"] == [0, 1, 2]
    assert config["evaluation"]["heldout_tasks"] == 100
    assert config["evaluation"]["seeds_start"] == 10000
    assert config["evaluation"]["chunk_width"] == 4
    assert config["evaluation"]["max_new_tokens"] == 1024
    assert config["evaluation"]["source"] == "pinned_dataforge_registry"
    assert config["evaluation"]["datasets"] == ["hospital", "flights", "beers"]


def test_grpo_15b_config_requires_sft_warmup_and_qlora() -> None:
    """The 1.5B config must not silently start from a raw base model."""
    config = load_grpo_config(CONFIG_15B)

    assert "torchao" in config["environment"]["uninstall_packages"]
    assert config["model"]["sft_checkpoint"] == "Praneshrajan15/DataForge-1.5B-SFT"
    assert config["model"]["sft_checkpoint_required"] is True
    assert config["model"]["target_model_repo"].endswith("/DataForge-1.5B-GRPO")
    assert config["lora"]["r"] == 8
    assert config["quantization"]["load_in_4bit"] is True
    assert config["quantization"]["bnb_4bit_quant_type"] == "nf4"
    assert config["training"]["gradient_checkpointing"] is True
    assert config["training"]["use_cache"] is False
    assert config["training"]["num_generations"] == 4
    assert config["reward"]["prompt_contract_version"] == "repair_contract_v2"
    assert config["readiness"]["trajectory_filename"] == "expert_v4.jsonl"


def test_grpo_05b_v2_config_preserves_v1_stack_and_targets_balanced_recall() -> None:
    """The v2 config should isolate the next improvement cycle from verified v1."""
    config = load_grpo_config(CONFIG_05B_V2)

    assert config["schema_version"] == "grpo_05b_v2"
    assert config["kaggle"]["auth_mode"] == "oauth"
    assert config["kaggle"]["credentials_path"] == r"C:\Users\Pranesh\.kaggle\credentials.json"
    assert config["repos"]["config_filename"] == "grpo_05b_v2.yaml"
    assert config["model"]["target_model_repo"].endswith("/DataForge-0.5B-GRPO")
    assert config["training"]["output_dir"].endswith("dataforge-0.5b-grpo-v2")
    assert config["training"]["merged_dir"].endswith("DataForge-0.5B-GRPO-v2-merged")
    assert config["reward"]["posture"] == "balanced_recall"
    assert config["reward"]["shaping"]["recall_weight"] == pytest.approx(0.15)
    assert config["reward"]["shaping"]["precision_weight"] == pytest.approx(0.10)
    assert config["reward"]["shaping"]["empty_truth_positive_penalty"] == pytest.approx(0.05)
    assert config["release"]["target_strict_macro_f1"] == pytest.approx(0.25)
    assert config["release"]["require_not_inferable_slice_f1"] == pytest.approx(0.95)
    assert config["release"]["push_only_if_gate_passes"] is True
    assert config["release"]["publish_or_update_public_model_only_after_v2_gate"] is True


def test_grpo_05b_v3_config_uses_sft_v6_predecessor_and_stronger_gates() -> None:
    """The v3 config must consume SFT-v6 predecessor evidence before another 500-step run."""
    config = load_grpo_config(CONFIG_05B_V3)

    assert config["schema_version"] == "grpo_05b_v3"
    assert config["kaggle"]["auth_mode"] == "oauth"
    assert config["kaggle"]["credentials_path"] == r"C:\Users\Pranesh\.kaggle\credentials.json"
    assert config["repos"]["config_filename"] == "grpo_05b_v3.yaml"
    assert config["model"]["sft_checkpoint"].endswith("/DataForge-0.5B-SFT-v6-candidate")
    assert config["model"]["target_model_repo"].endswith("/DataForge-0.5B-GRPO-v3-candidate")
    assert config["training"]["output_dir"].endswith("dataforge-0.5b-grpo-v3")
    assert config["readiness"]["trajectory_filename"] == "expert_v6_contract_minimal.jsonl"
    assert config["readiness"]["min_repair_signal_domains"] == 3
    assert config["reward"]["posture"] == "inferability_aware_repair"
    stages = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    assert stages["smoke"]["max_steps"] == 50
    assert stages["diagnostic"]["max_steps"] == 250
    assert stages["diagnostic"]["allow_upload_after_gate"] is False
    assert stages["candidate"]["max_steps"] == 500
    assert stages["candidate"]["allow_upload_after_gate"] is True
    assert config["training_sequence"]["selection_order"] == [
        "highest_strict_macro_f1",
        "highest_deterministic_normalization_f1",
        "lowest_schema_case_errors",
        "lowest_gpu_hours",
    ]
    assert config["release"]["target_strict_macro_f1"] == pytest.approx(0.25)
    assert config["release"]["require_not_inferable_slice_f1"] == pytest.approx(0.95)
    assert config["release"]["require_deterministic_normalization_slice_f1"] == pytest.approx(0.50)
    assert config["release"]["upload_repo_private"] is True
    assert config["release"]["publish_or_update_public_model_only_after_v3_gate"] is True


def test_grpo_kwargs_map_prompt_budget_only_when_trl_supports_it() -> None:
    """`max_prompt_length` is optional because current local TRL lacks it."""
    config = load_grpo_config(CONFIG_05B)
    supported = {
        "output_dir",
        "num_generations",
        "max_completion_length",
        "per_device_train_batch_size",
        "gradient_accumulation_steps",
        "beta",
        "learning_rate",
        "num_iterations",
        "save_steps",
        "logging_steps",
        "report_to",
        "fp16",
        "bf16",
    }

    kwargs = build_grpo_config_kwargs(config, supported_keys=supported)

    assert kwargs["num_generations"] == 4
    assert kwargs["max_completion_length"] == 256
    assert "prompt_token_budget" not in kwargs
    assert "max_prompt_length" not in kwargs

    supported_with_prompt = set(supported) | {"max_prompt_length"}
    kwargs_with_prompt = build_grpo_config_kwargs(config, supported_keys=supported_with_prompt)
    assert kwargs_with_prompt["max_prompt_length"] == 1280


def test_grpo_config_loader_rejects_stale_trl_011(tmp_path: Path) -> None:
    """The corrected plan must fail fast on the stale TRL v0.11 assumption."""
    config = yaml.safe_load(CONFIG_05B.read_text(encoding="utf-8"))
    config["environment"]["pip_packages"] = ["trl==0.11.0", "transformers==5.7.0"]
    path = tmp_path / "bad_grpo.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(GrpoConfigError, match="TRL v0.11"):
        load_grpo_config(path)


def test_grpo_config_loader_rejects_stale_repair_contract_v1(tmp_path: Path) -> None:
    config = yaml.safe_load(CONFIG_05B.read_text(encoding="utf-8"))
    config["reward"]["prompt_contract_version"] = "repair_contract_v1"
    path = tmp_path / "bad_grpo_contract.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(GrpoConfigError, match="repair_contract_v2"):
        load_grpo_config(path)


def test_grpo_config_loader_rejects_missing_smoke_no_upload_stage(tmp_path: Path) -> None:
    config = yaml.safe_load(CONFIG_05B.read_text(encoding="utf-8"))
    config["training_sequence"]["stages"][0]["allow_upload"] = True
    path = tmp_path / "bad_grpo_sequence.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(GrpoConfigError, match="smoke stage"):
        load_grpo_config(path)
