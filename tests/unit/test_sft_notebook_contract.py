"""Contract tests for the Kaggle SFT warmup notebook and YAML config."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "archive" / "training" / "kaggle" / "sft_warmup_kaggle.ipynb"
CONFIG = ROOT / "archive" / "training" / "configs" / "sft_05b.yaml"
CONFIG_V2 = ROOT / "archive" / "training" / "configs" / "sft_05b_v2.yaml"
CONFIG_V3 = ROOT / "archive" / "training" / "configs" / "sft_05b_v3.yaml"
CONFIG_V4 = ROOT / "archive" / "training" / "configs" / "sft_05b_v4.yaml"
CONFIG_V5 = ROOT / "archive" / "training" / "configs" / "sft_05b_v5.yaml"
CONFIG_V6 = ROOT / "archive" / "training" / "configs" / "sft_05b_v6.yaml"
CONFIG_V7 = ROOT / "archive" / "training" / "configs" / "sft_05b_v7.yaml"
CONFIG_V8 = ROOT / "archive" / "training" / "configs" / "sft_05b_v8.yaml"
CONFIG_V9 = ROOT / "archive" / "training" / "configs" / "sft_05b_v9.yaml"


def _notebook_source() -> str:
    payload = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell.get("source", ""))
        for cell in payload["cells"]
        if cell.get("cell_type") == "code"
    )


def test_config_owns_training_hyperparameters_and_exact_package_pins() -> None:
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

    assert config["model"]["base_model"] == "Qwen/Qwen2.5-0.5B-Instruct"
    assert config["training"]["fp16"] is True
    assert config["training"]["bf16"] is False
    assert config["training"]["save_steps"] == 100
    assert config["training"]["per_device_train_batch_size"] == 1
    assert config["training"]["gradient_accumulation_steps"] == 16
    assert config["training"]["max_seq_length"] == 1024
    assert config["training"]["loss_type"] == "chunked_nll"
    assert "trl==1.4.0" in config["environment"]["pip_packages"]
    assert config["lora"]["target_modules"] == ["q_proj", "k_proj", "v_proj", "o_proj"]
    assert config["evaluation"]["chunk_width"] == 4
    assert config["evaluation"]["max_new_tokens"] == 1024
    assert config["collection"]["oracle"]["include_noop_records"] is True
    assert all("==" in package for package in config["environment"]["pip_packages"])


def test_v2_config_owns_repair_contract_and_remote_only_gates() -> None:
    config = yaml.safe_load(CONFIG_V2.read_text(encoding="utf-8"))

    assert config["collection"]["schema_version"] == "expert_v2"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v1"
    assert config["repos"]["trajectory_filename"] == "expert_v2.jsonl"
    assert config["repos"]["split_manifest_filename"] == "split_manifest_v2.json"
    assert config["release"]["require_hf_token"] is True
    assert config["release"]["upload_eval_diagnostics"] is True
    assert config["release"]["promoted_status"] == "quality_improved_verified"
    assert config["collection"]["oracle"]["chunk_rows"] == 4
    assert config["collection"]["oracle"]["context_window_rows"] == 8
    assert config["collection"]["oracle"]["max_repairs_per_record"] == 8
    assert config["evaluation"]["remote_only"] is True


def test_v3_config_requires_inferable_repair_contract_v2_handoff() -> None:
    config = yaml.safe_load(CONFIG_V3.read_text(encoding="utf-8"))

    assert config["collection"]["schema_version"] == "expert_v3"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v2"
    assert config["repos"]["trajectory_filename"] == "expert_v3.jsonl"
    assert config["repos"]["split_manifest_filename"] == "split_manifest_v3.json"
    assert config["collection"]["oracle"]["inferability"] == "auto"
    assert config["collection"]["oracle"]["train_only_inferable"] is True
    assert config["evaluation"]["parse_success_min"] == 0.995


def test_v4_config_requires_contract_repair_handoff() -> None:
    config = yaml.safe_load(CONFIG_V4.read_text(encoding="utf-8"))

    assert config["collection"]["schema_version"] == "expert_v4"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v2"
    assert config["repos"]["trajectory_filename"] == "expert_v4.jsonl"
    assert config["repos"]["split_manifest_filename"] == "split_manifest_v4.json"
    assert config["collection"]["oracle"]["inferability"] == "auto"
    assert config["collection"]["oracle"]["train_only_inferable"] is False
    assert config["collection"]["oracle"]["abstain_noninferable"] is True
    assert config["collection"]["oracle"]["include_context_derivable"] is False
    assert config["collection"]["oracle"]["min_deterministic_records"] >= 32
    assert config["collection"]["oracle"]["min_abstention_records"] >= 32
    assert config["evaluation"]["promotion_slice"] == "deterministic_normalization"
    assert "context_derivable" in config["evaluation"]["auxiliary_slices"]


def test_v5_config_is_private_repair_curriculum_candidate() -> None:
    config = yaml.safe_load(CONFIG_V5.read_text(encoding="utf-8"))

    assert config["schema_version"] == "sft_05b_v5"
    assert config["collection"]["schema_version"] == "expert_v4"
    assert config["collection"]["curriculum_version"] == "expert_v5_repair_curriculum"
    assert config["repos"]["trajectory_filename"] == "expert_v5_repair_curriculum.jsonl"
    assert config["repos"]["split_manifest_filename"] == "split_manifest_v4.json"
    assert config["repos"]["model_repo_template"].endswith("/DataForge-0.5B-SFT-v5-candidate")
    assert config["release"]["private_candidate_only"] is True
    assert config["release"]["public_upload_allowed"] is False
    assert (
        config["release"]["promote_to_grpo_only_if"]["deterministic_normalization_slice_f1_min"]
        >= 0.30
    )
    assert config["evaluation"]["promotion_slice"] == "deterministic_normalization"


def test_v6_config_is_contract_first_private_candidate() -> None:
    config = yaml.safe_load(CONFIG_V6.read_text(encoding="utf-8"))

    assert config["schema_version"] == "sft_05b_v6"
    assert config["kaggle"]["auth_mode"] == "oauth"
    assert config["kaggle"]["credentials_path"] == r"C:\Users\Pranesh\.kaggle\credentials.json"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v3"
    assert config["collection"]["curriculum_version"] == "expert_v6_contract_minimal"
    assert (
        config["collection"]["curriculum"]["source_trajectory_filename"]
        == "expert_v5_repair_curriculum.jsonl"
    )
    assert config["collection"]["curriculum"]["max_repairs_per_record"] == 2
    assert config["repos"]["trajectory_filename"] == "expert_v6_contract_minimal.jsonl"
    assert config["repos"]["split_manifest_filename"] == "split_manifest_v4.json"
    assert config["repos"]["model_repo_template"].endswith("/DataForge-0.5B-SFT-v6-candidate")
    assert config["release"]["private_candidate_only"] is True
    assert config["release"]["public_upload_allowed"] is False
    assert config["release"]["require_hf_token_for_candidate"] is True
    stages = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    assert stages["smoke"]["max_steps"] == 20
    assert stages["smoke"]["allow_upload_after_gate"] is False
    assert stages["diagnostic"]["max_steps"] == 60
    assert stages["diagnostic"]["allow_upload_after_gate"] is False
    assert stages["candidate"]["max_steps"] == 140
    assert stages["candidate"]["allow_upload_after_gate"] is True
    assert stages["candidate"]["require_hf_token"] is True


def test_v7_config_is_parse_latch_private_candidate() -> None:
    config = yaml.safe_load(CONFIG_V7.read_text(encoding="utf-8"))

    assert config["schema_version"] == "sft_05b_v7"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v3"
    assert config["collection"]["curriculum_version"] == "expert_v7_parse_latch"
    assert config["collection"]["curriculum"]["source_trajectory_filename"] == (
        "expert_v6_contract_minimal.jsonl"
    )
    assert config["collection"]["curriculum"]["submit_repair_copies"] == 2
    assert config["collection"]["curriculum"]["finish_copies"] == 1
    assert config["repos"]["trajectory_filename"] == "expert_v7_parse_latch.jsonl"
    assert config["repos"]["model_repo_template"].endswith("/DataForge-0.5B-SFT-v7-candidate")
    assert config["release"]["private_candidate_only"] is True
    assert config["release"]["public_upload_allowed"] is False
    assert (
        config["release"]["promotion_report_schema"] == "dataforge_sft_v7_candidate_eval_report_v1"
    )
    stages = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    assert stages["smoke"]["max_steps"] == 20
    assert stages["diagnostic"]["max_steps"] == 120
    assert stages["candidate"]["max_steps"] == 240
    assert stages["candidate"]["require_hf_token"] is True


def test_v8_config_is_schema_distill_prompt_completion_candidate() -> None:
    config = yaml.safe_load(CONFIG_V8.read_text(encoding="utf-8"))

    assert config["schema_version"] == "sft_05b_v8"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v3"
    assert config["collection"]["curriculum_version"] == "expert_v8_schema_distill"
    assert config["collection"]["curriculum"]["source_trajectory_filename"] == (
        "expert_v6_contract_minimal.jsonl"
    )
    assert config["collection"]["curriculum"]["training_format"] == "prompt_completion"
    assert config["training"]["dataset_format"] == "prompt_completion"
    assert config["training"]["completion_only_loss"] is True
    assert config["training"]["assistant_only_loss"] is False
    assert config["training"]["packing"] is False
    assert config["repos"]["trajectory_filename"] == "expert_v8_schema_distill.jsonl"
    assert config["repos"]["model_repo_template"].endswith("/DataForge-0.5B-SFT-v8-candidate")
    assert config["release"]["private_candidate_only"] is True
    assert config["release"]["public_upload_allowed"] is False
    assert config["release"]["grpo_consumer_label"] == "GRPO-v4"
    assert (
        config["release"]["promotion_report_schema"] == "dataforge_sft_v8_candidate_eval_report_v1"
    )
    tracks = config["evaluation"]["report_tracks"]
    assert tracks["raw_research"]["decoding"] == "unconstrained_greedy"
    assert tracks["product_constrained"]["decoding"] == "json_schema_or_grammar_constrained"
    stages = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    assert stages["smoke"]["max_steps"] == 40
    assert stages["diagnostic"]["max_steps"] == 180
    assert stages["candidate"]["max_steps"] == 360
    assert stages["candidate"]["require_hf_token"] is True


def test_v9_config_is_action_envelope_prompt_completion_candidate() -> None:
    config = yaml.safe_load(CONFIG_V9.read_text(encoding="utf-8"))

    assert config["schema_version"] == "sft_05b_v9"
    assert config["collection"]["prompt_contract_version"] == "repair_contract_v3"
    assert config["collection"]["curriculum_version"] == "expert_v9_action_envelope"
    assert config["collection"]["curriculum"]["source_trajectory_filename"] == (
        "expert_v8_schema_distill.jsonl"
    )
    assert config["collection"]["curriculum"]["output_trajectory_filename"] == (
        "expert_v9_action_envelope.jsonl"
    )
    assert config["collection"]["curriculum"]["training_format"] == "prompt_completion"
    assert config["collection"]["curriculum"]["require_negative_contrast_examples"] is True
    assert config["collection"]["curriculum"]["require_negative_contrast_not_supervised"] is True
    assert config["training"]["dataset_format"] == "prompt_completion"
    assert config["training"]["completion_only_loss"] is True
    assert config["training"]["assistant_only_loss"] is False
    assert config["training"]["packing"] is False
    assert config["repos"]["trajectory_filename"] == "expert_v9_action_envelope.jsonl"
    assert config["repos"]["model_repo_template"].endswith("/DataForge-0.5B-SFT-v9-candidate")
    assert config["release"]["private_candidate_only"] is True
    assert config["release"]["public_upload_allowed"] is False
    assert config["release"]["grpo_consumer_label"] == "GRPO-v4"
    assert (
        config["release"]["promotion_report_schema"] == "dataforge_sft_v9_candidate_eval_report_v1"
    )
    tracks = config["evaluation"]["report_tracks"]
    assert tracks["raw_research"]["decoding"] == "unconstrained_greedy"
    assert tracks["product_constrained"]["decoding"] == "json_schema_or_grammar_constrained"
    assert tracks["product_constrained"]["prototype_status"].endswith("_runs_locally")
    assert config["evaluation"]["smoke_gate"]["constrained_product_parse_min"] == 0.99
    stages = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    assert stages["smoke"]["max_steps"] == 40
    assert stages["diagnostic"]["max_steps"] == 180
    assert stages["candidate"]["max_steps"] == 360
    assert stages["candidate"]["require_hf_token"] is True


def test_notebook_has_six_main_cells_and_no_placeholder_owner() -> None:
    payload = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    main_cells = [
        cell
        for cell in payload["cells"]
        if cell.get("cell_type") == "code"
        and "week9_main" in cell.get("metadata", {}).get("tags", [])
    ]
    source = _notebook_source()

    assert len(main_cells) == 6
    assert "<you>" not in source
    assert "bf16=True" not in source
    assert '"pending"' not in source
    assert "kaggle_secrets" in source
    assert 'secrets.get_secret("HF_TOKEN")' in source
    assert "HF_TOKEN Kaggle secret is required for this release notebook" in source
    assert 'os.environ["HF_TOKEN"] = HF_TOKEN' in source
    assert 'os.environ["HUGGING_FACE_HUB_TOKEN"] = HF_TOKEN' in source
    assert "HF upload skipped" not in source
    assert "No HF_TOKEN secret found" not in source
    assert "uninstall_remote_conflicts" in source
    assert "torchao: not installed" in source


def test_notebook_loads_yaml_and_exercises_required_training_flow() -> None:
    source = _notebook_source()

    assert "yaml.safe_load" in source
    assert "hf_hub_download" in source
    assert "BitsAndBytesConfig" in source
    assert "LoraConfig" in source
    assert "SFTTrainer" in source
    assert "expected_remote_training" in source
    assert 'CONFIG_FILENAME = os.environ.get("DATAFORGE_SFT_CONFIG", "sft_05b_v4.yaml")' in source
    assert "Downloaded SFT YAML is stale for this remote run" in source
    assert 'loss_type=train_cfg["loss_type"]' in source
    assert "Loaded remote training memory settings:" in source
    assert "Installed TRL does not support loss_type='chunked_nll'" in source
    assert 'importlib_metadata.version("trl")' in source
    assert 'device_map={"": 0}' in source
    assert "Trainable parameter dtypes:" in source
    assert "bf16 trainable parameters remain" in source
    assert "param.dtype != torch.float32" in source
    assert "latest_checkpoint = None" in source
    assert "resume_from_checkpoint=latest_checkpoint" in source
    assert "merge_and_unload" in source
    assert "training_metrics.json" in source
    assert "eval_diagnostics.json" in source
    assert "evaluate_model(" in source
    assert "torch.cuda.empty_cache()" in source
    assert source.count("api.upload_folder(") == 1
    assert "collection_methods" in source
    assert "oracle_from_clean_diff" in source
    assert "accepted_teachers" in source
    assert "parse_json_payload" in source
    assert "submit_repairs" in source
    assert "chunk_width" in source
    assert "split_manifest.json" in source
    assert "validate_split_manifest" in source
    assert "DATASET_SHA" in source
    assert '"dataset_sha": DATASET_SHA' in source
    assert "base_eval" in source
    assert "sft_eval" in source
    assert '"macro_f1"' in source
    assert '"quality_gate_failures": gate_failures' in source
    assert "quality_milestone" in source
    assert "diagnostic_complete_no_gain" in source
    assert "quality_improved_verified" in source
    assert '"parse_success_rate": parse_success_rate' in source
    assert '"schema_case_error_count": schema_case_error_count' in source
    assert '"promotion_slice": PROMOTION_SLICE' in source
    assert (
        '"slice_scores": {"base": base_eval.get("slice_scores", {}), "sft": sft_eval.get("slice_scores", {})}'
        in source
    )
    assert "failure_samples_by_slice" in source
    assert '"valid_rows": task["valid_rows"]' in source
    assert '"dataset": dataset_name' in source
    assert (
        'dataset_name = task.get("dataset") or task.get("schema_summary", {}).get("dataset", "unknown")'
        in source
    )
    assert '"target_rows": task["target_rows"]' in source
