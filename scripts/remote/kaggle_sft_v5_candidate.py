"""Kaggle entrypoint for the gated DataForge 0.5B SFT-v5 candidate run."""

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

DEFAULT_INPUT_ROOT = Path("/kaggle/input/dataforge-sft-v5-handoff")
INPUT_ROOT = Path(os.environ.get("DATAFORGE_SFT_INPUT", str(DEFAULT_INPUT_ROOT)))
WORK_ROOT = Path("/kaggle/working")
SOURCE_ROOT = WORK_ROOT / "dataforge-src"
REPORT_PATH = WORK_ROOT / "kaggle_sft_v5_candidate_report.json"
PROMOTION_REPORT_PATH = WORK_ROOT / "sft_v5_candidate_eval_report.json"
HF_SECRET_LABELS = (
    "HF_TOKEN",
    "HF",
    "HUGGING_FACE_HUB_TOKEN",
    "HUGGINGFACE_HUB_TOKEN",
    "HUGGINGFACE_TOKEN",
    "HUGGING_FACE_TOKEN",
)
HF_UPLOAD_CREDENTIAL_UNAVAILABLE = "hf_hub_upload_credential_unavailable"


def _has_sft_inputs(file_names: set[str]) -> bool:
    return {
        "expert_v5_repair_curriculum.jsonl",
        "split_manifest_v4.json",
        "sft_05b_v5.yaml",
        "sft_v5_repair_curriculum_report.json",
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
            f"Missing SFT-v5 source bundle: {source_zip}. Visible inputs: "
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


def _load_config() -> dict[str, Any]:
    import yaml

    config_name = os.environ.get("DATAFORGE_SFT_CONFIG", "sft_05b_v5.yaml")
    config_path = INPUT_ROOT / config_name
    if not config_path.exists():
        raise RuntimeError(f"Missing SFT-v5 config in Kaggle input: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{config_path} must contain a YAML mapping.")
    if payload.get("schema_version") != "sft_05b_v5":
        raise RuntimeError("This runner requires schema_version=sft_05b_v5.")
    release = payload.get("release", {})
    if not isinstance(release, dict) or release.get("public_upload_allowed") is not False:
        raise RuntimeError("SFT-v5 must keep public_upload_allowed=false.")
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
    report = _load_json(path)
    blockers: list[str] = []
    if report.get("schema_version") != "dataforge_sft_v5_repair_curriculum_report_v1":
        blockers.append("curriculum_report_wrong_schema")
    if report.get("ok") is not True:
        blockers.append("curriculum_report_not_pass")
    metrics = report.get("metrics", {})
    if not isinstance(metrics, dict):
        blockers.append("curriculum_report_missing_metrics")
        metrics = {}
    if int(metrics.get("deterministic_repair_records", 0)) < 512:
        blockers.append("deterministic_repair_records_under_512")
    if int(metrics.get("hard_negative_noop_records", 0)) < 128:
        blockers.append("hard_negative_noop_records_under_128")
    if blockers:
        raise RuntimeError("SFT-v5 curriculum report blocked: " + ", ".join(blockers))
    return report


def _records_to_dataset(records: list[dict[str, Any]]) -> tuple[Any, dict[str, Any]]:
    from collections import Counter

    from datasets import Dataset

    rows: list[dict[str, Any]] = []
    shape: Counter[str] = Counter()
    invalid = 0
    for record in records:
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
        raise RuntimeError(f"SFT-v5 curriculum contains invalid message records: {invalid}")
    if not rows:
        raise RuntimeError("SFT-v5 curriculum produced no trainable rows.")
    return Dataset.from_list(rows), {"records": len(rows), "shape": dict(sorted(shape.items()))}


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
    if "max_length" in supported_keys:
        wanted["max_length"] = train_cfg["max_seq_length"]
    elif "max_seq_length" in supported_keys:
        wanted["max_seq_length"] = train_cfg["max_seq_length"]
    if "loss_type" in supported_keys:
        wanted["loss_type"] = train_cfg["loss_type"]
    return {key: value for key, value in wanted.items() if key in supported_keys}


def _render_model_card(metrics: dict[str, Any]) -> str:
    return (
        "---\n"
        f"license: {metrics['model_license']}\n"
        f"base_model: {metrics['base_model']}\n"
        "library_name: transformers\n"
        "private_candidate: true\n"
        "---\n\n"
        f"# {metrics['model_name']}\n\n"
        "Private SFT-v5 repair-curriculum candidate for DataForge 0.5B. This checkpoint is "
        "eligible to seed GRPO-v3 only when `sft_v5_candidate_eval_report.json` has "
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
    _write_json(PROMOTION_REPORT_PATH, report)
    _write_json(merged_dir / "sft_v5_candidate_eval_report.json", report)


def main() -> int:
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    started_at = time.time()
    _log_event("sft_v5_candidate_start")
    _extract_source()
    _log_event("source_extracted", input_root=str(INPUT_ROOT))
    config = _load_config()
    curriculum_report = _validate_curriculum_report(
        INPUT_ROOT / "sft_v5_repair_curriculum_report.json"
    )
    _write_report(
        {
            "schema_version": "dataforge_kaggle_sft_v5_candidate_report_v1",
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
    _log_event("install_stack_start")
    _install_stack(config)
    _log_event("install_stack_done")

    import inspect

    import torch
    from huggingface_hub import HfApi
    from peft import LoraConfig, PeftModel, prepare_model_for_kbit_training
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
    from trl import SFTConfig, SFTTrainer

    from training.grpo_eval import build_heldout_tasks, evaluate_causal_lm
    from training.sft_promotion import build_sft_v5_promotion_report, sft_v5_promotion_gate_failures

    if not torch.cuda.is_available():
        raise RuntimeError("Kaggle GPU runtime is required for SFT-v5 candidate.")
    capability = torch.cuda.get_device_capability(0)
    if capability[0] < 7:
        raise RuntimeError(
            "SFT-v5 candidate requires a T4-or-newer Kaggle GPU for the installed PyTorch wheel; "
            f"received {torch.cuda.get_device_name(0)} with capability sm_{capability[0]}{capability[1]}."
        )
    _log_event(
        "gpu_ready",
        gpu_name=torch.cuda.get_device_name(0),
        capability=f"sm_{capability[0]}{capability[1]}",
    )

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
        model_label="sft_v5",
        max_new_tokens=int(eval_cfg["max_new_tokens"]),
    )
    _log_event("sft_eval_done", macro_f1=float(sft_eval["macro_f1"]))
    del merged_model
    gc.collect()
    torch.cuda.empty_cache()

    threshold_config = config["release"]["promote_to_grpo_only_if"]
    gate_failures = sft_v5_promotion_gate_failures(
        sft_eval,
        threshold_config=threshold_config,
    )
    promotion_gate_passed = not gate_failures
    eval_diagnostics = {
        "schema_version": "dataforge_sft_v5_eval_diagnostics_v1",
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "source_audit": task_manifest["source_audit"],
        "base_eval": base_eval,
        "sft_eval": sft_eval,
        "base": base_diagnostics,
        "sft": sft_diagnostics,
        "failure_samples": sft_diagnostics["failure_samples"][:25],
        "gate_failures": gate_failures,
        "limitations": [
            "Strict held-out eval verifies only DataForge repair research tasks.",
            "SFT-v5 is private predecessor evidence, not a public release.",
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
        "train_metrics": train_metrics,
        "trainable_dtype_changes": trainable_dtype_changes[:50],
        "hf_hub_upload_credential": "available" if hf_token else "unavailable",
        "private_candidate_only": True,
        "public_claim_updated": False,
    }
    _write_json(merged_dir / "training_metrics.json", metrics)
    (merged_dir / "README.md").write_text(_render_model_card(metrics), encoding="utf-8")

    if not promotion_gate_passed:
        promotion_report = build_sft_v5_promotion_report(
            status="quality_gate_failed_no_upload",
            model_repo=model_repo,
            checkpoint=model_repo,
            base_eval=base_eval,
            sft_eval=sft_eval,
            sft_diagnostics=sft_diagnostics,
            threshold_config=threshold_config,
            model_uploaded=False,
            training_metrics=metrics,
            artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
        )
        _save_promotion_report(merged_dir, promotion_report)
        _write_report(
            {
                "schema_version": "dataforge_kaggle_sft_v5_candidate_report_v1",
                "status": "quality_gate_failed_no_upload",
                "gate_failures": gate_failures,
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

    if not hf_token:
        promotion_report = build_sft_v5_promotion_report(
            status="pass_upload_blocked_missing_hf_token",
            model_repo=model_repo,
            checkpoint=model_repo,
            base_eval=base_eval,
            sft_eval=sft_eval,
            sft_diagnostics=sft_diagnostics,
            threshold_config=threshold_config,
            model_uploaded=False,
            training_metrics=metrics,
            artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
            upload_blocker=HF_UPLOAD_CREDENTIAL_UNAVAILABLE,
        )
        _save_promotion_report(merged_dir, promotion_report)
        _write_report(
            {
                "schema_version": "dataforge_kaggle_sft_v5_candidate_report_v1",
                "status": "pass_upload_blocked_missing_hf_token",
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
    promotion_report = build_sft_v5_promotion_report(
        status="pass_uploaded_private_candidate",
        model_repo=model_repo,
        checkpoint=model_repo,
        base_eval=base_eval,
        sft_eval=sft_eval,
        sft_diagnostics=sft_diagnostics,
        threshold_config=threshold_config,
        model_uploaded=True,
        training_metrics=metrics,
        artifacts={"merged_dir": str(merged_dir), "adapter_dir": str(adapter_dir)},
    )
    _save_promotion_report(merged_dir, promotion_report)
    api.upload_folder(
        folder_path=str(merged_dir),
        repo_id=model_repo,
        repo_type="model",
        token=hf_token,
        commit_message="Upload private DataForge 0.5B SFT-v5 candidate after promotion gate",
    )
    _log_event("private_upload_done", repo=model_repo)
    _write_report(
        {
            "schema_version": "dataforge_kaggle_sft_v5_candidate_report_v1",
            "status": "pass_uploaded_private_candidate",
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
                "schema_version": "dataforge_kaggle_sft_v5_candidate_report_v1",
                "status": "runtime_error",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
            }
        )
        raise
