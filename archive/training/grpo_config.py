"""Helpers for loading and adapting Week 12 GRPO YAML configs."""

from __future__ import annotations

import inspect
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from dataforge.repair_contract import CONTRACT_VERSION_V2, CONTRACT_VERSION_V3

REQUIRED_PIP_PACKAGES = {
    "trl==1.4.0",
    "transformers==5.7.0",
    "accelerate==1.13.0",
    "peft==0.19.1",
    "bitsandbytes==0.49.2",
    "datasets==4.8.5",
    "huggingface_hub==1.13.0",
    "pyyaml==6.0.3",
    "pandas==2.3.3",
    "tensorboard==2.20.0",
}

REQUIRED_TRAINING_VALUES: dict[str, object] = {
    "num_generations": 4,
    "max_completion_length": 256,
    "prompt_token_budget": 1280,
    "per_device_train_batch_size": 1,
    "gradient_accumulation_steps": 16,
    "beta": 0.04,
    "learning_rate": 1e-5,
    "num_iterations": 1,
    "save_steps": 50,
    "logging_steps": 5,
    "report_to": "tensorboard",
    "fp16": True,
    "bf16": False,
}

PASSTHROUGH_TRAINING_KEYS = {
    "output_dir",
    "per_device_train_batch_size",
    "learning_rate",
    "gradient_accumulation_steps",
    "bf16",
    "fp16",
    "gradient_checkpointing",
    "report_to",
    "save_steps",
    "logging_steps",
    "num_generations",
    "max_completion_length",
    "beta",
    "num_iterations",
    "max_steps",
    "warmup_ratio",
    "lr_scheduler_type",
    "weight_decay",
    "save_total_limit",
    "use_cache",
}


class GrpoConfigError(RuntimeError):
    """Raised when a GRPO handoff config is unsafe or stale."""


# `beers` was de-registered on 2026-07-12, but these validators still *required* it, so a
# config written against the current registry raised. Both sets are accepted: the legacy
# triple so frozen configs and their recorded runs stay replayable (rewriting frozen
# historical evidence is forbidden), and the current pair so a new config is writable.
#
# Deliberately not derived from DATASET_REGISTRY: these are training-corpus decisions
# recorded in a config, and silently widening them whenever a corpus is registered would
# let a new dataset enter a training contract without anyone choosing it.
_LEGACY_DATASET_SET = ["hospital", "flights", "beers"]
_CURRENT_DATASET_SET = ["hospital", "flights"]
_ACCEPTED_DATASET_SETS = (_CURRENT_DATASET_SET, _LEGACY_DATASET_SET)


def _as_mapping(value: object, *, name: str) -> dict[str, Any]:
    """Return a YAML object as a string-keyed mapping."""
    if not isinstance(value, dict):
        raise GrpoConfigError(f"{name} must be a mapping.")
    return dict(value)


def load_grpo_config(path: Path) -> dict[str, Any]:
    """Load and validate a Week 12 GRPO YAML config."""
    if not path.exists():
        raise GrpoConfigError(f"Missing GRPO config: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    config = _as_mapping(payload, name=str(path))
    for section in (
        "environment",
        "repos",
        "model",
        "lora",
        "training",
        "reward",
        "readiness",
        "training_sequence",
        "release",
        "evaluation",
    ):
        if section not in config:
            raise GrpoConfigError(f"GRPO config is missing required section: {section}")

    environment = _as_mapping(config["environment"], name="environment")
    packages = environment.get("pip_packages")
    if not isinstance(packages, list) or not all(isinstance(item, str) for item in packages):
        raise GrpoConfigError("environment.pip_packages must be a list of exact package pins.")
    uninstall_packages = environment.get("uninstall_packages")
    if not isinstance(uninstall_packages, list) or "torchao" not in uninstall_packages:
        raise GrpoConfigError("GRPO environment.uninstall_packages must include torchao.")
    unpinned = [package for package in packages if "==" not in package]
    if unpinned:
        raise GrpoConfigError("GRPO package pins must be exact: " + ", ".join(unpinned))
    if any(package.startswith("trl==0.11") for package in packages):
        raise GrpoConfigError("TRL v0.11 does not provide the required GRPOTrainer path.")
    missing = sorted(REQUIRED_PIP_PACKAGES - set(packages))
    if missing:
        raise GrpoConfigError("GRPO config missing package pin(s): " + ", ".join(missing))

    training = _as_mapping(config["training"], name="training")
    stale = {
        key: training.get(key)
        for key, expected in REQUIRED_TRAINING_VALUES.items()
        if training.get(key) != expected
    }
    if stale:
        raise GrpoConfigError(f"GRPO training settings are stale or unsafe: {stale}")
    if "max_prompt_length" in training:
        raise GrpoConfigError("Use training.prompt_token_budget instead of max_prompt_length.")

    reward = _as_mapping(config["reward"], name="reward")
    if reward.get("prompt_contract_version") != CONTRACT_VERSION_V2:
        raise GrpoConfigError("GRPO reward.prompt_contract_version must be repair_contract_v2.")
    if reward.get("local_stateless") is not True:
        raise GrpoConfigError("GRPO reward must remain local_stateless.")
    if reward.get("parse_failure_reward") != 0.0:
        raise GrpoConfigError("GRPO parse_failure_reward must be 0.0.")

    readiness = _as_mapping(config["readiness"], name="readiness")
    schema_version = str(config.get("schema_version", ""))
    allowed_trajectories = {
        "expert_v4.jsonl",
        "expert_v5_repair_curriculum.jsonl",
        "expert_v6_contract_minimal.jsonl",
        "expert_v7_parse_latch.jsonl",
        "expert_v8_schema_distill.jsonl",
        "expert_v9_action_envelope.jsonl",
    }
    if readiness.get("trajectory_filename") not in allowed_trajectories:
        raise GrpoConfigError(
            "GRPO readiness must use an approved v4/v5/v6/v7/v8 repair trajectory file."
        )
    if readiness.get("split_manifest_filename") != "split_manifest_v4.json":
        raise GrpoConfigError("GRPO readiness must use split_manifest_v4.json.")
    allowed_readiness_contracts = {CONTRACT_VERSION_V2, CONTRACT_VERSION_V3}
    if readiness.get("prompt_contract_version") not in allowed_readiness_contracts:
        raise GrpoConfigError(
            "GRPO readiness.prompt_contract_version must be repair_contract_v2 or repair_contract_v3."
        )
    required_datasets = readiness.get("required_datasets")
    if required_datasets not in _ACCEPTED_DATASET_SETS:
        raise GrpoConfigError(
            "GRPO readiness.required_datasets must be "
            f"{_CURRENT_DATASET_SET} (or the frozen legacy {_LEGACY_DATASET_SET})."
        )
    auxiliary_datasets = readiness.get("auxiliary_datasets")
    if "hospital_synthetic_deterministic_v1" not in (auxiliary_datasets or []):
        raise GrpoConfigError(
            "GRPO readiness.auxiliary_datasets must include hospital_synthetic_deterministic_v1."
        )
    if float(readiness.get("min_reward_std", 0.0)) <= 0.0:
        raise GrpoConfigError("GRPO readiness.min_reward_std must be positive.")
    if float(readiness.get("min_per_dataset_reward_std", 0.0)) <= 0.0:
        raise GrpoConfigError("GRPO readiness.min_per_dataset_reward_std must be positive.")
    if int(readiness.get("min_repair_records", 0)) < 1:
        raise GrpoConfigError("GRPO readiness.min_repair_records must be >= 1.")
    if int(readiness.get("min_repair_signal_domains", 0)) < 2:
        raise GrpoConfigError("GRPO readiness.min_repair_signal_domains must be >= 2.")
    if int(readiness.get("min_dirty_records_per_dataset", 0)) != 0:
        raise GrpoConfigError(
            "GRPO readiness.min_dirty_records_per_dataset must be 0; "
            "use min_repair_signal_domains for learnable repair coverage."
        )
    if readiness.get("require_source_provenance") is not True:
        raise GrpoConfigError("GRPO readiness.require_source_provenance must be true.")

    sequence = _as_mapping(config["training_sequence"], name="training_sequence")
    stages = sequence.get("stages")
    if not isinstance(stages, list) or len(stages) != 3:
        raise GrpoConfigError("GRPO training_sequence.stages must define smoke/candidate/extended.")
    stage_by_name = {
        str(stage.get("name")): _as_mapping(stage, name="training_sequence stage")
        for stage in stages
        if isinstance(stage, dict)
    }
    expected_steps = (
        {"smoke": 50, "diagnostic": 250, "candidate": 500}
        if schema_version in {"grpo_05b_v3", "grpo_05b_v4"}
        else {"smoke": 50, "candidate": 500, "extended_candidate": 1000}
    )
    for name, steps in expected_steps.items():
        stage = stage_by_name.get(name)
        if stage is None or int(stage.get("max_steps", 0)) != steps:
            raise GrpoConfigError(f"GRPO training_sequence missing {name} max_steps={steps}.")
    if stage_by_name["smoke"].get("allow_upload") is not False:
        raise GrpoConfigError("GRPO smoke stage must not allow upload.")
    selection_order = sequence.get("selection_order")
    expected_selection_order = (
        [
            "highest_strict_macro_f1",
            "highest_deterministic_normalization_f1",
            "lowest_schema_case_errors",
            "lowest_gpu_hours",
        ]
        if schema_version in {"grpo_05b_v3", "grpo_05b_v4"}
        else [
            "highest_strict_macro_f1",
            "lowest_schema_case_errors",
            "lowest_gpu_hours",
        ]
    )
    if selection_order != expected_selection_order:
        raise GrpoConfigError("GRPO training_sequence.selection_order is stale.")

    release = _as_mapping(config["release"], name="release")
    if release.get("benchmark_name") != "DataForge-Bench-light-verified":
        raise GrpoConfigError("release.benchmark_name must be DataForge-Bench-light-verified.")
    if float(release.get("min_absolute_f1_gain", 0.0)) < 0.03:
        raise GrpoConfigError("release.min_absolute_f1_gain must be at least 0.03.")
    if release.get("benchmark_seeds") != [0, 1, 2]:
        raise GrpoConfigError("release.benchmark_seeds must be [0, 1, 2].")

    if schema_version == "grpo_05b_v2":
        kaggle = _as_mapping(config.get("kaggle", {}), name="kaggle")
        if kaggle.get("auth_mode") != "oauth":
            raise GrpoConfigError("GRPO v2 requires Kaggle OAuth.")
        if (
            str(kaggle.get("credentials_path", "")).strip()
            != r"C:\Users\Pranesh\.kaggle\credentials.json"
        ):
            raise GrpoConfigError("GRPO v2 must use C:\\Users\\Pranesh\\.kaggle\\credentials.json.")
        if reward.get("posture") != "balanced_recall":
            raise GrpoConfigError("GRPO v2 reward.posture must be balanced_recall.")
        if float(release.get("target_strict_macro_f1", 0.0)) < 0.25:
            raise GrpoConfigError("GRPO v2 release.target_strict_macro_f1 must be >= 0.25.")
        if float(release.get("require_not_inferable_slice_f1", 0.0)) < 0.95:
            raise GrpoConfigError("GRPO v2 must preserve the not-inferable no-op slice.")
        if release.get("publish_or_update_public_model_only_after_v2_gate") is not True:
            raise GrpoConfigError("GRPO v2 public model updates must remain gate-blocked.")
    if schema_version == "grpo_05b_v3":
        kaggle = _as_mapping(config.get("kaggle", {}), name="kaggle")
        if kaggle.get("auth_mode") != "oauth":
            raise GrpoConfigError("GRPO v3 requires Kaggle OAuth.")
        if (
            str(kaggle.get("credentials_path", "")).strip()
            != r"C:\Users\Pranesh\.kaggle\credentials.json"
        ):
            raise GrpoConfigError("GRPO v3 must use C:\\Users\\Pranesh\\.kaggle\\credentials.json.")
        if readiness.get("trajectory_filename") != "expert_v7_parse_latch.jsonl":
            raise GrpoConfigError("GRPO v3 must use the expert_v7_parse_latch handoff.")
        if readiness.get("prompt_contract_version") != CONTRACT_VERSION_V3:
            raise GrpoConfigError("GRPO v3 readiness must use repair_contract_v3.")
        if reward.get("posture") != "inferability_aware_repair":
            raise GrpoConfigError("GRPO v3 reward.posture must be inferability_aware_repair.")
        if float(release.get("target_strict_macro_f1", 0.0)) < 0.25:
            raise GrpoConfigError("GRPO v3 release.target_strict_macro_f1 must be >= 0.25.")
        if float(release.get("require_not_inferable_slice_f1", 0.0)) < 0.95:
            raise GrpoConfigError("GRPO v3 must preserve the not-inferable no-op slice.")
        if float(release.get("require_deterministic_normalization_slice_f1", 0.0)) < 0.50:
            raise GrpoConfigError("GRPO v3 must gate deterministic-normalization active repair.")
        if release.get("upload_repo_private") is not True:
            raise GrpoConfigError("GRPO v3 candidate uploads must remain private.")
        if release.get("publish_or_update_public_model_only_after_v3_gate") is not True:
            raise GrpoConfigError("GRPO v3 public model updates must remain gate-blocked.")
    if schema_version == "grpo_05b_v4":
        kaggle = _as_mapping(config.get("kaggle", {}), name="kaggle")
        if kaggle.get("auth_mode") != "oauth":
            raise GrpoConfigError("GRPO v4 requires Kaggle OAuth.")
        if (
            str(kaggle.get("credentials_path", "")).strip()
            != r"C:\Users\Pranesh\.kaggle\credentials.json"
        ):
            raise GrpoConfigError("GRPO v4 must use C:\\Users\\Pranesh\\.kaggle\\credentials.json.")
        if readiness.get("trajectory_filename") != "expert_v9_action_envelope.jsonl":
            raise GrpoConfigError("GRPO v4 must use the expert_v9_action_envelope handoff.")
        if readiness.get("prompt_contract_version") != CONTRACT_VERSION_V3:
            raise GrpoConfigError("GRPO v4 readiness must use repair_contract_v3.")
        if reward.get("posture") != "inferability_aware_repair":
            raise GrpoConfigError("GRPO v4 reward.posture must be inferability_aware_repair.")
        if not str(config["model"].get("sft_checkpoint", "")).endswith(
            "/DataForge-0.5B-SFT-v9-candidate"
        ):
            raise GrpoConfigError("GRPO v4 must use the private SFT-v9 predecessor checkpoint.")
        if float(release.get("target_strict_macro_f1", 0.0)) < 0.25:
            raise GrpoConfigError("GRPO v4 release.target_strict_macro_f1 must be >= 0.25.")
        if float(release.get("require_not_inferable_slice_f1", 0.0)) < 0.95:
            raise GrpoConfigError("GRPO v4 must preserve the not-inferable no-op slice.")
        if float(release.get("require_deterministic_normalization_slice_f1", 0.0)) < 0.50:
            raise GrpoConfigError("GRPO v4 must gate deterministic-normalization active repair.")
        if release.get("upload_repo_private") is not True:
            raise GrpoConfigError("GRPO v4 candidate uploads must remain private.")
        if release.get("publish_or_update_public_model_only_after_v4_gate") is not True:
            raise GrpoConfigError("GRPO v4 public model updates must remain gate-blocked.")

    evaluation = _as_mapping(config["evaluation"], name="evaluation")
    if int(evaluation.get("heldout_tasks", 0)) != 100:
        raise GrpoConfigError("evaluation.heldout_tasks must be 100.")
    if int(evaluation.get("seeds_start", 0)) != 10000:
        raise GrpoConfigError("evaluation.seeds_start must be 10000.")
    if int(evaluation.get("chunk_width", 0)) != 4:
        raise GrpoConfigError("evaluation.chunk_width must be 4.")
    if int(evaluation.get("max_new_tokens", 0)) != 1024:
        raise GrpoConfigError("evaluation.max_new_tokens must be 1024.")
    if evaluation.get("source") != "pinned_dataforge_registry":
        raise GrpoConfigError("evaluation.source must be pinned_dataforge_registry.")
    if evaluation.get("datasets") not in _ACCEPTED_DATASET_SETS:
        raise GrpoConfigError(
            "evaluation.datasets must be "
            f"{_CURRENT_DATASET_SET} (or the frozen legacy {_LEGACY_DATASET_SET})."
        )
    return config


def build_grpo_config_kwargs(
    config: dict[str, Any],
    *,
    supported_keys: set[str] | None = None,
) -> dict[str, Any]:
    """Build kwargs safe to pass to TRL's ``GRPOConfig``.

    ``prompt_token_budget`` is a DataForge-local tokenizer constraint. It is
    mapped to ``max_prompt_length`` only for TRL versions that expose that
    parameter.
    """
    training = _as_mapping(config["training"], name="training")
    if supported_keys is None:
        supported_keys = installed_grpo_config_keys()
    kwargs: dict[str, Any] = {}
    for key in PASSTHROUGH_TRAINING_KEYS:
        if key in training and key in supported_keys:
            kwargs[key] = training[key]
    if "max_prompt_length" in supported_keys:
        kwargs["max_prompt_length"] = training["prompt_token_budget"]
    return kwargs


def installed_grpo_config_keys() -> set[str]:
    """Return the parameter names exposed by the installed TRL ``GRPOConfig``."""
    from trl import GRPOConfig

    return set(inspect.signature(GRPOConfig).parameters)


def run_grpo_import_preflight(python_executable: Path | None = None) -> None:
    """Verify ``GRPOTrainer`` imports under UTF-8 mode before launching Kaggle.

    Some Windows environments fail on TRL chat-template reads unless UTF-8 mode
    is enabled. The notebook mirrors this by setting ``PYTHONUTF8=1`` before
    importing TRL.
    """
    executable = str(python_executable or Path(sys.executable))
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    command = [
        executable,
        "-c",
        "from trl import GRPOConfig, GRPOTrainer; print('grpo-ok')",
    ]
    subprocess.run(command, check=True, capture_output=True, text=True, env=env, timeout=120)
