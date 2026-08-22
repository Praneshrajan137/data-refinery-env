"""Kaggle entrypoint for gated DataForge 0.5B SFT predecessor candidates."""

from __future__ import annotations

import gc
import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

DEFAULT_SFT_VERSION = os.environ.get("DATAFORGE_SFT_VERSION", "v6")
DEFAULT_INPUT_ROOT = Path(f"/kaggle/input/dataforge-sft-{DEFAULT_SFT_VERSION}-handoff")
INPUT_ROOT = Path(os.environ.get("DATAFORGE_SFT_INPUT", str(DEFAULT_INPUT_ROOT)))
WORK_ROOT = Path("/kaggle/working")
SOURCE_ROOT = WORK_ROOT / "dataforge-src"
REPORT_PATH = WORK_ROOT / f"kaggle_sft_{DEFAULT_SFT_VERSION}_candidate_report.json"
PROMOTION_REPORT_FILENAME = f"sft_{DEFAULT_SFT_VERSION}_candidate_eval_report.json"
KAGGLE_REPORT_SCHEMA = f"dataforge_kaggle_sft_{DEFAULT_SFT_VERSION}_candidate_report_v1"
HF_SECRET_LABELS = (
    "HF_TOKEN",
    "HF",
    "HUGGING_FACE_HUB_TOKEN",
    "HUGGINGFACE_HUB_TOKEN",
    "HUGGINGFACE_TOKEN",
    "HUGGING_FACE_TOKEN",
)
HF_UPLOAD_CREDENTIAL_UNAVAILABLE = "hf_hub_upload_credential_unavailable"


def _input_spec() -> dict[str, Any]:
    if DEFAULT_SFT_VERSION == "v9":
        return {
            "schema_version": "sft_05b_v9",
            "config_file": "sft_05b_v9.yaml",
            "trajectory_file": "expert_v9_action_envelope.jsonl",
            "curriculum_report_file": "sft_v9_action_envelope_curriculum_report.json",
            "curriculum_report_schema": "dataforge_sft_v9_action_envelope_curriculum_report_v1",
            "candidate_label": "SFT-v9",
            "training_format": "prompt_completion",
            "min_submit": 1000,
            "min_finish": 1000,
            "submit_ratio_min": 0.45,
            "submit_ratio_max": 0.60,
            "requires_product_constrained_preflight": True,
        }
    if DEFAULT_SFT_VERSION == "v8":
        return {
            "schema_version": "sft_05b_v8",
            "config_file": "sft_05b_v8.yaml",
            "trajectory_file": "expert_v8_schema_distill.jsonl",
            "curriculum_report_file": "sft_v8_schema_distill_curriculum_report.json",
            "curriculum_report_schema": "dataforge_sft_v8_schema_distill_curriculum_report_v1",
            "candidate_label": "SFT-v8",
            "training_format": "prompt_completion",
            "min_submit": 1000,
            "min_finish": 900,
            "submit_ratio_min": 0.50,
            "submit_ratio_max": 0.60,
        }
    if DEFAULT_SFT_VERSION == "v7":
        return {
            "schema_version": "sft_05b_v7",
            "config_file": "sft_05b_v7.yaml",
            "trajectory_file": "expert_v7_parse_latch.jsonl",
            "curriculum_report_file": "sft_v7_parse_latch_curriculum_report.json",
            "curriculum_report_schema": "dataforge_sft_v7_parse_latch_curriculum_report_v1",
            "candidate_label": "SFT-v7",
            "training_format": "messages",
            "min_submit": 1800,
            "min_finish": 450,
        }
    return {
        "schema_version": "sft_05b_v6",
        "config_file": "sft_05b_v6.yaml",
        "trajectory_file": "expert_v6_contract_minimal.jsonl",
        "curriculum_report_file": "sft_v6_contract_minimal_curriculum_report.json",
        "curriculum_report_schema": "dataforge_sft_v6_contract_minimal_curriculum_report_v1",
        "candidate_label": "SFT-v6",
        "training_format": "messages",
        "min_submit": 900,
        "min_finish": 450,
    }


def _has_sft_inputs(file_names: set[str]) -> bool:
    spec = _input_spec()
    return {
        spec["trajectory_file"],
        "split_manifest_v4.json",
        spec["config_file"],
        spec["curriculum_report_file"],
    } <= file_names


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_report(payload: dict[str, Any]) -> None:
    _write_json(REPORT_PATH, payload)


def _log_event(event: str, **payload: Any) -> None:
    print(json.dumps({"event": event, **payload}, sort_keys=True), flush=True)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path} must contain a JSON object.")
    return cast(dict[str, Any], payload)


def _source_zip_candidates() -> list[Path]:
    input_root = Path("/kaggle/input")
    if not input_root.exists():
        return []
    candidates: list[Path] = []
    for root, _, files in os.walk(input_root, followlinks=True):
        if "source.zip" in files:
            candidates.append(Path(root) / "source.zip")
    return sorted(candidates)


def _source_dir_candidates() -> list[Path]:
    input_root = Path("/kaggle/input")
    if not input_root.exists():
        return []
    candidates: list[Path] = []
    for root, dirs, _files in os.walk(input_root, followlinks=True):
        if "source" not in dirs:
            continue
        candidate = Path(root) / "source"
        child_names = {path.name for path in candidate.iterdir()}
        if {"dataforge", "scripts", "training", "pyproject.toml"} <= child_names:
            candidates.append(candidate)
    return sorted(candidates)


def _visible_input_listing(limit: int = 80) -> list[str]:
    input_root = Path("/kaggle/input")
    if not input_root.exists():
        return []
    listing: list[str] = []
    for root, dirs, files in os.walk(input_root, followlinks=True):
        rel_root = Path(root).relative_to(input_root)
        for name in sorted(dirs + files):
            listing.append((rel_root / name).as_posix())
            if len(listing) >= limit:
                return listing
    return listing


def _extract_source() -> None:
    global INPUT_ROOT
    source_zip = INPUT_ROOT / "source.zip"
    source_dir = INPUT_ROOT / "source"
    if not source_zip.exists():
        for candidate in _source_zip_candidates():
            sibling_names = {path.name for path in candidate.parent.iterdir() if path.is_file()}
            if _has_sft_inputs(sibling_names):
                INPUT_ROOT = candidate.parent
                source_zip = candidate
                break
    if not source_zip.exists() and not source_dir.exists():
        for candidate in _source_dir_candidates():
            sibling_names = {path.name for path in candidate.parent.iterdir() if path.is_file()}
            if _has_sft_inputs(sibling_names):
                INPUT_ROOT = candidate.parent
                source_dir = candidate
                break
    if not source_zip.exists() and not source_dir.exists():
        raise RuntimeError(
            f"Missing {_input_spec()['candidate_label']} source bundle: {source_zip}. Visible inputs: "
            f"{_visible_input_listing()}"
        )
    if SOURCE_ROOT.exists():
        shutil.rmtree(SOURCE_ROOT)
    if source_zip.exists():
        SOURCE_ROOT.mkdir(parents=True)
        with zipfile.ZipFile(source_zip) as archive:
            archive.extractall(SOURCE_ROOT)
    else:
        shutil.copytree(source_dir, SOURCE_ROOT)
    os.chdir(SOURCE_ROOT)
    sys.path.insert(0, str(SOURCE_ROOT))


def _load_hf_token() -> tuple[str | None, str, str]:
    for label in HF_SECRET_LABELS:
        token = (os.environ.get(label) or "").strip()
        if token:
            return token, "environment", ""
    try:
        from kaggle_secrets import UserSecretsClient
    except Exception:  # pragma: no cover - exercised only inside Kaggle
        return None, "unavailable", HF_UPLOAD_CREDENTIAL_UNAVAILABLE
    client = UserSecretsClient()
    for label in HF_SECRET_LABELS:
        try:
            token = (client.get_secret(label) or "").strip()
        except Exception:  # pragma: no cover - exercised only inside Kaggle
            continue
        if token:
            return token, "kaggle_secrets", ""
    return None, "unavailable", HF_UPLOAD_CREDENTIAL_UNAVAILABLE


def _install_stack(config: dict[str, Any]) -> None:
    env_cfg = config["environment"]
    uninstall_packages = [
        str(package)
        for package in env_cfg.get(
            "uninstall_packages", ["torchvision", "torchaudio", "torchtext", "torchao"]
        )
    ]
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pyyaml==6.0.3"])
    if uninstall_packages:
        subprocess.run(
            [sys.executable, "-m", "pip", "uninstall", "-y", "-q", *uninstall_packages],
            check=False,
        )
    torch_packages = [str(package) for package in env_cfg.get("torch_pip_packages", [])]
    torch_index_url = str(env_cfg.get("torch_index_url", "")).strip()
    if torch_packages:
        command = [sys.executable, "-m", "pip", "install", "-q"]
        if torch_index_url:
            command.extend(["--index-url", torch_index_url])
        command.extend(torch_packages)
        subprocess.check_call(command)
    packages = [str(package) for package in env_cfg["pip_packages"]]
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *packages])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-e", str(SOURCE_ROOT)])


def _require_supported_gpu(torch_module: Any, candidate_label: str) -> dict[str, str]:
    """Validate the actual Kaggle GPU, not the requested notebook metadata."""
    if not torch_module.cuda.is_available():
        raise RuntimeError(f"Kaggle GPU runtime is required for {candidate_label} candidate.")
    capability = torch_module.cuda.get_device_capability(0)
    gpu_name = torch_module.cuda.get_device_name(0)
    capability_label = f"sm_{capability[0]}{capability[1]}"
    if capability[0] < 6:
        raise RuntimeError(
            f"{candidate_label} candidate requires a Pascal-or-newer Kaggle GPU; "
            f"received {gpu_name} with capability {capability_label}."
        )
    precision_mode = "fp16_pascal" if capability[0] == 6 else "fp16_tensor_core"
    return {
        "gpu_name": gpu_name,
        "capability": capability_label,
        "precision_mode": precision_mode,
    }


def _load_config() -> dict[str, Any]:
    import yaml

    spec = _input_spec()
    config_name = os.environ.get("DATAFORGE_SFT_CONFIG", spec["config_file"])
    config_path = INPUT_ROOT / config_name
    if not config_path.exists():
        raise RuntimeError(
            f"Missing {spec['candidate_label']} config in Kaggle input: {config_path}"
        )
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{config_path} must contain a YAML mapping.")
    if payload.get("schema_version") != spec["schema_version"]:
        raise RuntimeError(f"This runner requires schema_version={spec['schema_version']}.")
    release = payload.get("release", {})
    if not isinstance(release, dict) or release.get("public_upload_allowed") is not False:
        raise RuntimeError(f"{spec['candidate_label']} must keep public_upload_allowed=false.")
    return cast(dict[str, Any], payload)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise RuntimeError(f"{path}:{line_number} must contain a JSON object.")
        records.append(cast(dict[str, Any], payload))
    return records


def _validate_curriculum_report(path: Path) -> dict[str, Any]:
    spec = _input_spec()
    report = _load_json(path)
    blockers: list[str] = []
    if report.get("schema_version") != spec["curriculum_report_schema"]:
        blockers.append("curriculum_report_wrong_schema")
    if report.get("ok") is not True:
        blockers.append("curriculum_report_not_pass")
    metrics = report.get("metrics", {})
    if not isinstance(metrics, dict):
        blockers.append("curriculum_report_missing_metrics")
        metrics = {}
    min_submit = int(spec.get("min_submit", 900))
    if int(metrics.get("submit_repair_records", 0)) < min_submit:
        blockers.append(f"submit_repair_records_under_{min_submit}")
    min_finish = int(spec.get("min_finish", 450))
    if int(metrics.get("finish_records", 0)) < min_finish:
        blockers.append(f"finish_records_under_{min_finish}")
    if spec.get("training_format") != "prompt_completion":
        if int(metrics.get("assistant_reason_fields", -1)) != 0:
            blockers.append("assistant_reason_fields_present")
        if int(metrics.get("system_reason_field_mentions", -1)) != 0:
            blockers.append("system_reason_field_mentions_present")
        if int(metrics.get("system_wrapper_mentions", -1)) != 0:
            blockers.append("system_wrapper_mentions_present")
    if int(metrics.get("finish_with_repairs", 0)) != 0:
        blockers.append("finish_with_repairs_present")
    if int(metrics.get("user_contract_version_mismatches", -1)) != 0:
        blockers.append("user_contract_version_mismatches_present")
    if int(metrics.get("record_contract_version_mismatches", -1)) != 0:
        blockers.append("record_contract_version_mismatches_present")
    if (
        spec.get("training_format") != "prompt_completion"
        and int(metrics.get("parse_failure_count", -1)) != 0
    ):
        blockers.append("parse_failures_present")
    if spec.get("training_format") == "prompt_completion":
        if int(metrics.get("prompt_completion_records", 0)) <= 0:
            blockers.append("prompt_completion_records_missing")
        if int(metrics.get("completion_parse_failure_count", -1)) != 0:
            blockers.append("completion_parse_failures_present")
        if int(metrics.get("completion_code_fence_count", -1)) != 0:
            blockers.append("completion_code_fences_present")
        if int(metrics.get("completion_reason_text_count", -1)) != 0:
            blockers.append("completion_reason_text_present")
        if int(metrics.get("legacy_messages_present", -1)) != 0:
            blockers.append("legacy_messages_present")
        submit_ratio = float(metrics.get("submit_ratio", 0.0))
        if not (
            float(spec.get("submit_ratio_min", 0.0))
            <= submit_ratio
            <= float(spec.get("submit_ratio_max", 1.0))
        ):
            blockers.append("submit_ratio_outside_configured_range")
        label_mask_audit = report.get("label_mask_audit", {})
        if not isinstance(label_mask_audit, dict) or label_mask_audit.get("ok") is not True:
            blockers.append("curriculum_label_mask_audit_not_pass")
        if spec.get("requires_product_constrained_preflight"):
            constrained_preflight = report.get("product_constrained_preflight", {})
            if not isinstance(constrained_preflight, dict):
                blockers.append("product_constrained_preflight_missing")
            elif float(constrained_preflight.get("parse_structural_success_rate", 0.0)) < 0.99:
                blockers.append("product_constrained_preflight_parse_under_0.99")
            if int(metrics.get("negative_contrast_target_leakage_count", -1)) != 0:
                blockers.append("negative_contrast_targets_supervised")
    if blockers:
        raise RuntimeError(
            f"{spec['candidate_label']} curriculum report blocked: " + ", ".join(blockers)
        )
    return report


def _select_stage(config: dict[str, Any]) -> dict[str, Any]:
    stage_name = os.environ.get("DATAFORGE_SFT_STAGE", "smoke")
    sequence = config.get("training_sequence", {})
    stages = sequence.get("stages", []) if isinstance(sequence, dict) else []
    stage_by_name = {
        str(stage.get("name")): stage
        for stage in stages
        if isinstance(stage, dict) and stage.get("name")
    }
    selected = stage_by_name.get(stage_name)
    if selected is None:
        valid = ", ".join(sorted(stage_by_name)) or "none"
        raise RuntimeError(f"Unsupported DATAFORGE_SFT_STAGE={stage_name!r}; valid stages: {valid}")
    config["training"]["max_steps"] = int(selected["max_steps"])
    return {
        "name": stage_name,
        "max_steps": int(selected["max_steps"]),
        "allow_upload_after_gate": bool(selected.get("allow_upload_after_gate", False)),
        "require_hf_token": bool(selected.get("require_hf_token", False)),
    }


def _records_to_dataset(records: list[dict[str, Any]]) -> tuple[Any, dict[str, Any]]:
    from collections import Counter

    from datasets import Dataset

    rows: list[dict[str, Any]] = []
    shape: Counter[str] = Counter()
    invalid = 0
    spec = _input_spec()
    for record in records:
        if spec.get("training_format") == "prompt_completion":
            prompt = record.get("prompt")
            completion = record.get("completion")
            if not isinstance(prompt, list) or not isinstance(completion, str) or not completion:
                invalid += 1
                continue
            prompt_roles = [
                str(message.get("role"))
                for message in prompt
                if isinstance(message, dict) and message.get("role")
            ]
            if prompt_roles != ["system", "user"]:
                invalid += 1
                continue
            rows.append(
                {
                    "prompt": prompt,
                    "completion": [{"role": "assistant", "content": completion}],
                }
            )
        else:
            messages = record.get("messages")
            if not isinstance(messages, list) or not messages:
                invalid += 1
                continue
            assistant_messages = [
                message
                for message in messages
                if isinstance(message, dict) and message.get("role") == "assistant"
            ]
            if not assistant_messages:
                invalid += 1
                continue
            rows.append({"messages": messages})
        dataset = str(record.get("dataset", "unknown"))
        inferability = str(record.get("inferability", "unknown"))
        repair_label = "repair" if record.get("fix") else "noop"
        shape[f"{dataset}:{inferability}:{repair_label}"] += 1
    if invalid:
        raise RuntimeError(
            f"{_input_spec()['candidate_label']} curriculum contains invalid message records: {invalid}"
        )
    if not rows:
        raise RuntimeError(
            f"{_input_spec()['candidate_label']} curriculum produced no trainable rows."
        )
    return Dataset.from_list(rows), {
        "records": len(rows),
        "shape": dict(sorted(shape.items())),
        "training_format": str(spec.get("training_format", "messages")),
    }


def _prompt_completion_label_mask_audit(
    records: list[dict[str, Any]],
    tokenizer: Any,
    *,
    max_seq_length: int,
    sample_size: int = 32,
) -> dict[str, Any]:
    """Audit that v8 prompt-completion rows keep assistant targets supervised."""
    if _input_spec().get("training_format") != "prompt_completion":
        return {"ok": True, "skipped": True, "reason": "messages_training_format"}
    samples = records[:sample_size]
    failures: list[dict[str, Any]] = []
    prompt_token_counts: list[int] = []
    completion_token_counts: list[int] = []
    full_token_counts: list[int] = []
    for index, record in enumerate(samples):
        prompt = record.get("prompt")
        completion = record.get("completion")
        if not isinstance(prompt, list) or not isinstance(completion, str):
            failures.append({"index": index, "kind": "bad_prompt_completion_shape"})
            continue
        completion_message = [{"role": "assistant", "content": completion}]
        try:
            prompt_text = tokenizer.apply_chat_template(
                prompt,
                tokenize=False,
                add_generation_prompt=True,
            )
            full_text = tokenizer.apply_chat_template(
                [*prompt, *completion_message],
                tokenize=False,
                add_generation_prompt=False,
            )
            prompt_ids = tokenizer(prompt_text, add_special_tokens=False)["input_ids"]
            full_ids = tokenizer(full_text, add_special_tokens=False)["input_ids"]
        except Exception as exc:  # pragma: no cover - exercised inside Kaggle tokenizer stack
            failures.append({"index": index, "kind": "chat_template_error", "error": str(exc)})
            continue
        if not isinstance(prompt_ids, list) or not isinstance(full_ids, list):
            failures.append({"index": index, "kind": "tokenizer_output_shape"})
            continue
        if len(full_ids) <= len(prompt_ids):
            failures.append({"index": index, "kind": "completion_has_no_supervised_tokens"})
            continue
        completion_ids = full_ids[len(prompt_ids) :]
        prompt_token_counts.append(len(prompt_ids))
        completion_token_counts.append(len(completion_ids))
        full_token_counts.append(len(full_ids))
        truncated_ids = full_ids[-max_seq_length:] if len(full_ids) > max_seq_length else full_ids
        completion_retained = (
            len(completion_ids) <= len(truncated_ids)
            and truncated_ids[-len(completion_ids) :] == completion_ids
        )
        if not completion_retained:
            failures.append(
                {
                    "index": index,
                    "kind": "completion_removed_by_left_truncation",
                    "prompt_tokens": len(prompt_ids),
                    "completion_tokens": len(completion_ids),
                    "full_tokens": len(full_ids),
                    "max_seq_length": max_seq_length,
                }
            )
        if completion in prompt_text:
            failures.append({"index": index, "kind": "completion_text_in_prompt"})
    return {
        "ok": not failures and bool(samples),
        "sampled_records": len(samples),
        "training_format": "prompt_completion",
        "completion_only_loss_required": True,
        "prompt_label_policy": "ignored_by_trl_prompt_completion_collator",
        "completion_label_policy": "supervised",
        "max_seq_length": max_seq_length,
        "max_prompt_tokens": max(prompt_token_counts) if prompt_token_counts else 0,
        "max_completion_tokens": max(completion_token_counts) if completion_token_counts else 0,
        "max_full_tokens": max(full_token_counts) if full_token_counts else 0,
        "failures": failures[:10],
    }


def _cast_trainable_parameters_to_float32(model: Any) -> list[dict[str, str]]:
    """Avoid AMP unscale failures from bf16 trainable adapter parameters."""
    changed: list[dict[str, str]] = []
    for name, parameter in model.named_parameters():
        if not getattr(parameter, "requires_grad", False):
            continue
        dtype = str(getattr(parameter, "dtype", ""))
        if dtype == "torch.float32":
            continue
        parameter.data = parameter.data.float()
        if parameter.grad is not None:
            parameter.grad.data = parameter.grad.data.float()
        changed.append({"name": str(name), "from_dtype": dtype, "to_dtype": "torch.float32"})
    return changed


def _resolve_model_repo(config: dict[str, Any], hf_token: str | None) -> str:
    template = str(config["repos"]["model_repo_template"])
    hf_user = os.environ.get("DATAFORGE_HF_USER", "Praneshrajan15").strip() or "Praneshrajan15"
    if hf_token:
        try:
            from huggingface_hub import HfApi

            whoami = HfApi(token=hf_token).whoami(token=hf_token)
            if isinstance(whoami, dict) and whoami.get("name"):
                hf_user = str(whoami["name"])
        except Exception:
            pass
    return template.format(hf_user=hf_user)


def _sft_config_kwargs(config: dict[str, Any], supported_keys: set[str]) -> dict[str, Any]:
    train_cfg = config["training"]
    if _input_spec().get("training_format") == "prompt_completion":
        if train_cfg.get("completion_only_loss") is not True:
            raise RuntimeError(
                f"{_input_spec()['candidate_label']} requires training.completion_only_loss=true."
            )
        if train_cfg.get("packing") is not False:
            raise RuntimeError(
                f"{_input_spec()['candidate_label']} requires packing=false so prompt/completion labels remain auditable."
            )
        if "completion_only_loss" not in supported_keys:
            raise RuntimeError("Installed TRL SFTConfig does not expose completion_only_loss.")
    wanted = {
        "output_dir": train_cfg["output_dir"],
        "num_train_epochs": train_cfg["num_train_epochs"],
        "per_device_train_batch_size": train_cfg["per_device_train_batch_size"],
        "gradient_accumulation_steps": train_cfg["gradient_accumulation_steps"],
        "learning_rate": train_cfg["learning_rate"],
        "lr_scheduler_type": train_cfg["lr_scheduler_type"],
        "warmup_ratio": train_cfg["warmup_ratio"],
        "weight_decay": train_cfg["weight_decay"],
        "logging_steps": train_cfg["logging_steps"],
        "save_steps": train_cfg["save_steps"],
        "save_total_limit": train_cfg["save_total_limit"],
        "fp16": train_cfg["fp16"],
        "bf16": train_cfg["bf16"],
        "gradient_checkpointing": train_cfg["gradient_checkpointing"],
        "report_to": train_cfg["report_to"],
        "packing": train_cfg["packing"],
    }
    if "max_steps" in supported_keys and "max_steps" in train_cfg:
        wanted["max_steps"] = train_cfg["max_steps"]
    if "max_length" in supported_keys:
        wanted["max_length"] = train_cfg["max_seq_length"]
    elif "max_seq_length" in supported_keys:
        wanted["max_seq_length"] = train_cfg["max_seq_length"]
    if "loss_type" in supported_keys:
        wanted["loss_type"] = train_cfg["loss_type"]
    for optional_key in ("completion_only_loss", "assistant_only_loss"):
        if optional_key in supported_keys and optional_key in train_cfg:
            wanted[optional_key] = train_cfg[optional_key]
    return {key: value for key, value in wanted.items() if key in supported_keys}


def _render_model_card(metrics: dict[str, Any]) -> str:
    candidate_label = str(metrics.get("candidate_label", _input_spec()["candidate_label"]))
    return (
        "---\n"
        f"license: {metrics['model_license']}\n"
        f"base_model: {metrics['base_model']}\n"
        "library_name: transformers\n"
        "private_candidate: true\n"
        "---\n\n"
        f"# {metrics['model_name']}\n\n"
        f"Private {candidate_label} predecessor candidate for DataForge 0.5B. This checkpoint is "
        f"eligible to seed {metrics.get('grpo_consumer_label', 'GRPO-v3')} only when `{PROMOTION_REPORT_FILENAME}` has "
        "`promote_to_grpo: true`.\n\n"
        "## Evidence\n\n"
        f"- Strict macro F1: `{metrics['sft_f1']}`\n"
        f"- Base strict macro F1: `{metrics['base_f1']}`\n"
        f"- Parse success: `{metrics['parse_success_rate']}`\n"
        f"- Schema-case errors: `{metrics['schema_case_error_count']}`\n"
        f"- Promotion gate passed: `{metrics['promotion_gate_passed']}`\n\n"
        "## Limits\n\n"
        "This is private predecessor evidence, not a public release and not production-quality "
        "autonomous data repair.\n"
    )


def _save_promotion_report(merged_dir: Path, report: dict[str, Any]) -> None:
    _write_json(WORK_ROOT / PROMOTION_REPORT_FILENAME, report)
    _write_json(merged_dir / PROMOTION_REPORT_FILENAME, report)


def main() -> int:
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    input_spec = _input_spec()
    started_at = time.time()
    _log_event(f"sft_{DEFAULT_SFT_VERSION}_candidate_start")
    _extract_source()
    _log_event("source_extracted", input_root=str(INPUT_ROOT))
    config = _load_config()
    selected_stage = _select_stage(config)
    _log_event(
        "stage_selected",
        stage=selected_stage["name"],
        max_steps=selected_stage["max_steps"],
        allow_upload_after_gate=selected_stage["allow_upload_after_gate"],
    )
    curriculum_report = _validate_curriculum_report(
        INPUT_ROOT / input_spec["curriculum_report_file"]
    )
    _write_report(
        {
            "schema_version": KAGGLE_REPORT_SCHEMA,
            "status": "preflight",
            "model_upload_attempted": False,
            "public_claim_updated": False,
            "curriculum_report": curriculum_report,
        }
    )
    hf_token, hf_auth_origin, hf_token_error = _load_hf_token()
    if hf_token:
        os.environ["HF_TOKEN"] = hf_token
        os.environ["HUGGING_FACE_HUB_TOKEN"] = hf_token
        _log_event("hf_token_available_preflight", origin=hf_auth_origin)
    else:
        _log_event("hf_token_unavailable_preflight")
    if selected_stage["require_hf_token"] and not hf_token:
        _write_report(
            {
                "schema_version": KAGGLE_REPORT_SCHEMA,
                "status": "blocked_missing_hf_token_no_gpu",
                "training_stage": selected_stage["name"],
                "configured_max_steps": selected_stage["max_steps"],
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "upload_blocker": HF_UPLOAD_CREDENTIAL_UNAVAILABLE,
                "note": f"{input_spec['candidate_label']} candidate stage requires a visible HF token before GPU work because promotion requires a private checkpoint upload.",
            }
        )
        _log_event("blocked_missing_hf_token_no_gpu")
        return 0
    _log_event("install_stack_start")
    _install_stack(config)
    _log_event("install_stack_done")

    import inspect

    import torch
    from huggingface_hub import HfApi
    from peft import LoraConfig, PeftModel, prepare_model_for_kbit_training
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
    from trl import SFTConfig, SFTTrainer

    from training.grpo_eval import (
        build_heldout_tasks,
        evaluate_causal_lm,
        evaluate_product_constrained_finish_baseline,
    )
    from training.sft_promotion import build_sft_promotion_report, sft_promotion_gate_failures

    gpu_metadata = _require_supported_gpu(torch, input_spec["candidate_label"])
    _log_event("gpu_ready", **gpu_metadata)

    model_repo = _resolve_model_repo(config, hf_token)
    base_model_id = str(config["model"]["base_model"])
    records = _load_jsonl(INPUT_ROOT / str(config["repos"]["trajectory_filename"]))
    train_dataset, dataset_shape = _records_to_dataset(records)
    _log_event("dataset_ready", records=dataset_shape["records"])

    tokenizer = AutoTokenizer.from_pretrained(
        base_model_id,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.truncation_side = "left"
    tokenizer.padding_side = "left"
    label_mask_audit = _prompt_completion_label_mask_audit(
        records,
        tokenizer,
        max_seq_length=int(config["training"]["max_seq_length"]),
    )
    _log_event(
        "label_mask_audit",
        ok=label_mask_audit["ok"],
        training_format=label_mask_audit.get("training_format"),
    )
    if not label_mask_audit["ok"]:
        raise RuntimeError(
            f"{input_spec['candidate_label']} label-mask audit failed: {label_mask_audit['failures']}"
        )

    quant_cfg = None
    quant = config["model"].get("quantization", {})
    if isinstance(quant, dict) and quant.get("load_in_4bit"):
        quant_cfg = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type=quant["bnb_4bit_quant_type"],
            bnb_4bit_use_double_quant=quant["bnb_4bit_use_double_quant"],
            bnb_4bit_compute_dtype=torch.float16,
        )
    model = AutoModelForCausalLM.from_pretrained(
        base_model_id,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
        torch_dtype=torch.float16,
        quantization_config=quant_cfg,
        device_map={"": 0},
    )
    model.config.use_cache = False
    if quant_cfg is not None:
        prepare_kwargs: dict[str, Any] = {}
        if (
            "use_gradient_checkpointing"
            in inspect.signature(prepare_model_for_kbit_training).parameters
        ):
            prepare_kwargs["use_gradient_checkpointing"] = bool(
                config["training"].get("gradient_checkpointing", True)
            )
        model = prepare_model_for_kbit_training(model, **prepare_kwargs)
    peft_config = LoraConfig(
        r=config["lora"]["r"],
        lora_alpha=config["lora"]["alpha"],
        lora_dropout=config["lora"]["dropout"],
        bias=config["lora"]["bias"],
        task_type=config["lora"]["task_type"],
        target_modules=config["lora"]["target_modules"],
    )
    sft_kwargs = _sft_config_kwargs(
        config,
        supported_keys=set(inspect.signature(SFTConfig).parameters),
    )
    training_args = SFTConfig(**sft_kwargs)
    trainer_kwargs = {
        "model": model,
        "args": training_args,
        "train_dataset": train_dataset,
        "peft_config": peft_config,
    }
    trainer_params = set(inspect.signature(SFTTrainer).parameters)
    if "processing_class" in trainer_params:
        trainer_kwargs["processing_class"] = tokenizer
    else:
        trainer_kwargs["tokenizer"] = tokenizer
    trainer = SFTTrainer(**trainer_kwargs)
    trainable_dtype_changes = _cast_trainable_parameters_to_float32(trainer.model)
    _log_event(
        "trainable_dtype_prepared",
        changed_count=len(trainable_dtype_changes),
        changed_dtypes=sorted({row["from_dtype"] for row in trainable_dtype_changes}),
    )
    _log_event("training_start")
    train_started = time.time()
    train_result = trainer.train()
    _log_event("training_done")
    train_metrics = getattr(train_result, "metrics", {}) or {}
    adapter_dir = Path(config["training"]["adapter_dir"])
    trainer.save_model(str(adapter_dir))
    attempted_steps = int(getattr(train_result, "global_step", 0))
    gpu_hours = round((time.time() - train_started) / 3600.0, 4)
    del trainer, model
    gc.collect()
    torch.cuda.empty_cache()

    eval_cfg = config["evaluation"]
    tasks, task_manifest = build_heldout_tasks(
        datasets=eval_cfg["datasets"],
        heldout_tasks=int(eval_cfg["heldout_tasks"]),
        benchmark_seeds=(0, 1, 2),
        seeds_start=int(eval_cfg["seeds_start"]),
        chunk_width=int(eval_cfg["chunk_width"]),
        cache_root=WORK_ROOT / "dataforge-cache",
        contract_version=str(config["collection"]["prompt_contract_version"]),
    )
    _log_event("heldout_tasks_ready", task_count=len(tasks))
    merged_dir = Path(config["training"]["merged_dir"])
    merged_dir.mkdir(parents=True, exist_ok=True)
    task_manifest["max_new_tokens"] = int(eval_cfg["max_new_tokens"])
    _write_json(merged_dir / "eval_task_manifest.json", task_manifest)

    base_eval_model = AutoModelForCausalLM.from_pretrained(
        base_model_id,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
        torch_dtype=torch.float16,
        device_map="auto",
    )
    base_eval, base_diagnostics = evaluate_causal_lm(
        base_eval_model,
        tokenizer,
        tasks,
        model_label="base",
        max_new_tokens=int(eval_cfg["max_new_tokens"]),
    )
    _log_event("base_eval_done", macro_f1=float(base_eval["macro_f1"]))
    del base_eval_model
    gc.collect()
    torch.cuda.empty_cache()

    base_for_merge = AutoModelForCausalLM.from_pretrained(
        base_model_id,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
        torch_dtype=torch.float16,
        device_map="auto",
    )
    merged_model = PeftModel.from_pretrained(base_for_merge, str(adapter_dir))
    merged_model = merged_model.merge_and_unload()
    merged_model.save_pretrained(merged_dir, safe_serialization=True)
    tokenizer.save_pretrained(merged_dir)
    sft_eval, sft_diagnostics = evaluate_causal_lm(
        merged_model,
        tokenizer,
        tasks,
        model_label=f"sft_{DEFAULT_SFT_VERSION}",
        max_new_tokens=int(eval_cfg["max_new_tokens"]),
    )
    _log_event("sft_eval_done", macro_f1=float(sft_eval["macro_f1"]))
    product_constrained_track, product_constrained_diagnostics = (
        evaluate_product_constrained_finish_baseline(
            tasks,
            raw_research_summary=sft_eval,
            max_failure_samples=int(config["release"].get("max_failure_samples", 25)),
        )
    )
    _log_event(
        "product_constrained_eval_done",
        parse_structural_success_rate=float(
            product_constrained_track["parse_structural_success_rate"]
        ),
        strict_macro_f1=float(product_constrained_track["strict_macro_f1"]),
    )
    del merged_model
    gc.collect()
    torch.cuda.empty_cache()

    threshold_config = config["release"]["promote_to_grpo_only_if"]
    gate_failures = sft_promotion_gate_failures(
        sft_eval,
        threshold_config=threshold_config,
    )
    promotion_gate_passed = not gate_failures
    evaluation_tracks = {
        "raw_research": {
            "enabled": True,
            "decoding": "unconstrained_greedy",
            "base_eval": base_eval,
            "sft_eval": sft_eval,
            "claim_policy": "research evidence only",
        },
        "product_constrained": {
            **product_constrained_track,
            "enabled": bool(
                config.get("evaluation", {})
                .get("report_tracks", {})
                .get("product_constrained", {})
                .get("enabled", False)
            ),
        },
    }
    eval_diagnostics = {
        "schema_version": f"dataforge_sft_{DEFAULT_SFT_VERSION}_eval_diagnostics_v1",
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "source_audit": task_manifest["source_audit"],
        "evaluation_tracks": evaluation_tracks,
        "base_eval": base_eval,
        "sft_eval": sft_eval,
        "base": base_diagnostics,
        "sft": sft_diagnostics,
        "product_constrained": product_constrained_diagnostics,
        "failure_samples": sft_diagnostics["failure_samples"][:25],
        "gate_failures": gate_failures,
        "limitations": [
            "Strict held-out eval verifies only DataForge repair research tasks.",
            f"{input_spec['candidate_label']} is private predecessor evidence, not a public release.",
        ],
    }
    _write_json(merged_dir / "eval_diagnostics.json", eval_diagnostics)
    metrics = {
        "model_name": model_repo.split("/")[-1],
        "model_license": config["model"]["model_license"],
        "base_model": base_model_id,
        "model_repo": model_repo,
        "gpu_hours": gpu_hours,
        "attempted_steps": attempted_steps,
        "training_stage": selected_stage["name"],
        "configured_max_steps": selected_stage["max_steps"],
        "base_f1": float(base_eval["macro_f1"]),
        "sft_f1": float(sft_eval["macro_f1"]),
        "f1_delta": round(float(sft_eval["macro_f1"]) - float(base_eval["macro_f1"]), 4),
        "parse_success_rate": float(sft_eval["parse_success_rate"]),
        "schema_case_error_count": int(sft_eval["schema_case_error_count"]),
        "promotion_gate_passed": promotion_gate_passed,
        "quality_gate_failures": gate_failures,
        "run_date_utc": datetime.now(UTC).isoformat(),
        "records_seen": len(records),
        "valid_train_records": int(dataset_shape["records"]),
        "dataset_shape": dataset_shape["shape"],
        "curriculum_report": curriculum_report,
        "label_mask_audit": label_mask_audit,
        "evaluation_tracks": evaluation_tracks,
        "train_metrics": train_metrics,
        "trainable_dtype_changes": trainable_dtype_changes[:50],
        "hf_hub_upload_credential": "available" if hf_token else "unavailable",
        "candidate_label": input_spec["candidate_label"],
        "grpo_consumer_label": str(config["release"].get("grpo_consumer_label", "GRPO-v3")),
        "private_candidate_only": True,
        "public_claim_updated": False,
    }
    _write_json(merged_dir / "training_metrics.json", metrics)
    (merged_dir / "README.md").write_text(_render_model_card(metrics), encoding="utf-8")

    if not promotion_gate_passed:
        promotion_report = build_sft_promotion_report(
            status="quality_gate_failed_no_upload",
            model_repo=model_repo,
            checkpoint=model_repo,
            base_eval=base_eval,
            sft_eval=sft_eval,
            sft_diagnostics=sft_diagnostics,
            threshold_config=threshold_config,
            model_uploaded=False,
            report_schema_version=str(
                config["release"].get(
                    "promotion_report_schema",
                    f"dataforge_sft_{DEFAULT_SFT_VERSION}_candidate_eval_report_v1",
                )
            ),
            candidate_label=str(
                config["release"].get("candidate_label", input_spec["candidate_label"])
            ),
            candidate_kind=str(config["release"].get("candidate_kind", "predecessor")),
            grpo_consumer_label=str(config["release"].get("grpo_consumer_label", "GRPO-v3")),
            training_metrics=metrics,
            artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
        )
        _save_promotion_report(merged_dir, promotion_report)
        _write_report(
            {
                "schema_version": KAGGLE_REPORT_SCHEMA,
                "status": "quality_gate_failed_no_upload",
                "gate_failures": gate_failures,
                "training_stage": selected_stage["name"],
                "configured_max_steps": selected_stage["max_steps"],
                "attempted_steps": attempted_steps,
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_hours": gpu_hours,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "merged_dir": str(merged_dir),
                "promotion_report": promotion_report,
            }
        )
        _log_event("quality_gate_failed_no_upload", gate_failures=gate_failures)
        return 0

    if not selected_stage["allow_upload_after_gate"]:
        status = f"{selected_stage['name']}_complete_no_upload"
        promotion_report = build_sft_promotion_report(
            status=status,
            model_repo=model_repo,
            checkpoint=model_repo,
            base_eval=base_eval,
            sft_eval=sft_eval,
            sft_diagnostics=sft_diagnostics,
            threshold_config=threshold_config,
            model_uploaded=False,
            report_schema_version=str(
                config["release"].get(
                    "promotion_report_schema",
                    f"dataforge_sft_{DEFAULT_SFT_VERSION}_candidate_eval_report_v1",
                )
            ),
            candidate_label=str(
                config["release"].get("candidate_label", input_spec["candidate_label"])
            ),
            candidate_kind=str(config["release"].get("candidate_kind", "predecessor")),
            grpo_consumer_label=str(config["release"].get("grpo_consumer_label", "GRPO-v3")),
            training_metrics=metrics,
            artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
        )
        _save_promotion_report(merged_dir, promotion_report)
        _write_report(
            {
                "schema_version": KAGGLE_REPORT_SCHEMA,
                "status": status,
                "training_stage": selected_stage["name"],
                "configured_max_steps": selected_stage["max_steps"],
                "attempted_steps": attempted_steps,
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_hours": gpu_hours,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "model_repo": model_repo,
                "merged_dir": str(merged_dir),
                "promotion_report": promotion_report,
            }
        )
        _log_event(status)
        return 0

    if not hf_token:
        promotion_report = build_sft_promotion_report(
            status="pass_upload_blocked_missing_hf_token",
            model_repo=model_repo,
            checkpoint=model_repo,
            base_eval=base_eval,
            sft_eval=sft_eval,
            sft_diagnostics=sft_diagnostics,
            threshold_config=threshold_config,
            model_uploaded=False,
            report_schema_version=str(
                config["release"].get(
                    "promotion_report_schema",
                    f"dataforge_sft_{DEFAULT_SFT_VERSION}_candidate_eval_report_v1",
                )
            ),
            candidate_label=str(
                config["release"].get("candidate_label", input_spec["candidate_label"])
            ),
            candidate_kind=str(config["release"].get("candidate_kind", "predecessor")),
            grpo_consumer_label=str(config["release"].get("grpo_consumer_label", "GRPO-v3")),
            training_metrics=metrics,
            artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
            upload_blocker=HF_UPLOAD_CREDENTIAL_UNAVAILABLE,
        )
        _save_promotion_report(merged_dir, promotion_report)
        _write_report(
            {
                "schema_version": KAGGLE_REPORT_SCHEMA,
                "status": "pass_upload_blocked_missing_hf_token",
                "training_stage": selected_stage["name"],
                "configured_max_steps": selected_stage["max_steps"],
                "attempted_steps": attempted_steps,
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_hours": gpu_hours,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "model_repo": model_repo,
                "merged_dir": str(merged_dir),
                "upload_blocker": HF_UPLOAD_CREDENTIAL_UNAVAILABLE,
                "promotion_report": promotion_report,
            }
        )
        _log_event("pass_upload_blocked_missing_hf_token")
        return 0

    api = HfApi(token=hf_token)
    _log_event("private_upload_start", repo=model_repo)
    api.create_repo(
        repo_id=model_repo,
        repo_type="model",
        exist_ok=True,
        private=True,
        token=hf_token,
    )
    promotion_report = build_sft_promotion_report(
        status="pass_uploaded_private_candidate",
        model_repo=model_repo,
        checkpoint=model_repo,
        base_eval=base_eval,
        sft_eval=sft_eval,
        sft_diagnostics=sft_diagnostics,
        threshold_config=threshold_config,
        model_uploaded=True,
        report_schema_version=str(
            config["release"].get(
                "promotion_report_schema",
                f"dataforge_sft_{DEFAULT_SFT_VERSION}_candidate_eval_report_v1",
            )
        ),
        candidate_label=str(
            config["release"].get("candidate_label", input_spec["candidate_label"])
        ),
        candidate_kind=str(config["release"].get("candidate_kind", "predecessor")),
        grpo_consumer_label=str(config["release"].get("grpo_consumer_label", "GRPO-v3")),
        training_metrics=metrics,
        artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
    )
    _save_promotion_report(merged_dir, promotion_report)
    api.upload_folder(
        folder_path=str(merged_dir),
        repo_id=model_repo,
        repo_type="model",
        token=hf_token,
        commit_message=f"Upload private DataForge 0.5B {input_spec['candidate_label']} candidate after promotion gate",
    )
    _log_event("private_upload_done", repo=model_repo)
    _write_report(
        {
            "schema_version": KAGGLE_REPORT_SCHEMA,
            "status": "pass_uploaded_private_candidate",
            "training_stage": selected_stage["name"],
            "configured_max_steps": selected_stage["max_steps"],
            "attempted_steps": attempted_steps,
            "gpu_name": torch.cuda.get_device_name(0),
            "gpu_hours": gpu_hours,
            "model_upload_attempted": True,
            "model_repo_created": True,
            "model_repo_private": True,
            "public_claim_updated": False,
            "model_repo": model_repo,
            "merged_dir": str(merged_dir),
            "promotion_report": promotion_report,
            "total_runtime_hours": round((time.time() - started_at) / 3600.0, 4),
        }
    )
    print(
        json.dumps(
            {"status": "pass_uploaded_private_candidate", "repo": model_repo}, sort_keys=True
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        _write_report(
            {
                "schema_version": KAGGLE_REPORT_SCHEMA,
                "status": "runtime_error",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
            }
        )
        raise
