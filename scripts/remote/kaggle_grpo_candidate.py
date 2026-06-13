"""Kaggle entrypoint for the gated DataForge 0.5B GRPO candidate run."""

from __future__ import annotations

import gc
import hashlib
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

DEFAULT_INPUT_ROOT = Path("/kaggle/input/dataforge-grpo-candidate-handoff")
INPUT_ROOT = Path(os.environ.get("DATAFORGE_GRPO_INPUT", str(DEFAULT_INPUT_ROOT)))
WORK_ROOT = Path("/kaggle/working")
SOURCE_ROOT = WORK_ROOT / "dataforge-src"
REPORT_PATH = WORK_ROOT / "kaggle_grpo_candidate_report.json"
HF_SECRET_LABELS = (
    "HF_TOKEN",
    "HF",
    "HUGGING_FACE_HUB_TOKEN",
    "HUGGINGFACE_HUB_TOKEN",
    "HUGGINGFACE_TOKEN",
    "HUGGING_FACE_TOKEN",
)

TRAJECTORY_INPUT_FILES = {
    "expert_v4.jsonl",
    "expert_v5_repair_curriculum.jsonl",
    "expert_v6_contract_minimal.jsonl",
}
SFT_V6_PREDECESSOR_REPORT = "sft_v6_candidate_eval_report.json"


def _has_candidate_inputs(file_names: set[str]) -> bool:
    return {
        "split_manifest_v4.json",
        "grpo_05b.yaml",
        "kaggle_grpo_smoke_report.json",
        "smoke_validation.json",
    } <= file_names and bool(file_names & TRAJECTORY_INPUT_FILES)


def _write_report(payload: dict[str, Any]) -> None:
    REPORT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _log_event(event: str, **payload: Any) -> None:
    print(
        json.dumps(
            {
                "event": event,
                **payload,
            },
            sort_keys=True,
        ),
        flush=True,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _load_hf_token() -> tuple[str, str]:
    for label in HF_SECRET_LABELS:
        token = (os.environ.get(label) or "").strip()
        if token:
            return token, f"environment:{label}"
    try:
        from kaggle_secrets import UserSecretsClient
    except Exception as exc:  # pragma: no cover - exercised only inside Kaggle
        raise RuntimeError("HF_TOKEN or HF Kaggle secret is required for candidate upload.") from exc
    client = UserSecretsClient()
    token = ""
    last_error: Exception | None = None
    for label in HF_SECRET_LABELS:
        try:
            token = (client.get_secret(label) or "").strip()
        except Exception as exc:  # pragma: no cover - exercised only inside Kaggle
            last_error = exc
            continue
        if token:
            return token, f"kaggle_secret:{label}"
    if not token:
        labels = ", ".join(HF_SECRET_LABELS)
        raise RuntimeError(f"HF_TOKEN or HF Kaggle secret is required for candidate upload. Tried: {labels}.") from last_error
    return token, "kaggle_secret"


def _extract_source() -> None:
    global INPUT_ROOT
    source_zip = INPUT_ROOT / "source.zip"
    source_dir = INPUT_ROOT / "source"
    if not source_zip.exists():
        for candidate in _source_zip_candidates():
            sibling_names = {path.name for path in candidate.parent.iterdir() if path.is_file()}
            if _has_candidate_inputs(sibling_names):
                INPUT_ROOT = candidate.parent
                source_zip = candidate
                break
    if not source_zip.exists() and not source_dir.exists():
        for candidate in _source_dir_candidates():
            sibling_names = {path.name for path in candidate.parent.iterdir() if path.is_file()}
            if _has_candidate_inputs(sibling_names):
                INPUT_ROOT = candidate.parent
                source_dir = candidate
                break
    if not source_zip.exists() and not source_dir.exists():
        raise RuntimeError(
            f"Missing candidate source bundle: {source_zip}. Visible inputs: "
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


def _install_stack(config: dict[str, Any]) -> None:
    packages = [str(package) for package in config["environment"]["pip_packages"]]
    uninstall_packages = [
        str(package)
        for package in config["environment"].get("uninstall_packages", ["torchao"])
    ]
    if any(package.startswith("trl==") and package.split("==", 1)[1].startswith("0.11") for package in packages):
        raise RuntimeError("TRL v0.11 is not a valid GRPOTrainer target.")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pyyaml==6.0.3"])
    if uninstall_packages:
        subprocess.run(
            [sys.executable, "-m", "pip", "uninstall", "-y", "-q", *uninstall_packages],
            check=False,
        )
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *packages])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-e", str(SOURCE_ROOT)])


def _load_config() -> dict[str, Any]:
    import yaml

    config_name = os.environ.get("DATAFORGE_GRPO_CONFIG", "grpo_05b.yaml")
    config_path = INPUT_ROOT / config_name
    if not config_path.exists():
        raise RuntimeError(f"Missing GRPO config in Kaggle input: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{config_path} must contain a YAML mapping.")
    return cast(dict[str, Any], payload)


def _sft_v6_predecessor_blockers(config: dict[str, Any]) -> list[str]:
    if str(config.get("schema_version", "")) != "grpo_05b_v3":
        return []
    report_path = INPUT_ROOT / SFT_V6_PREDECESSOR_REPORT
    if not report_path.exists():
        return ["missing_sft_v6_candidate_eval_report"]
    report = _load_json(report_path)
    status = str(report.get("status", ""))
    ok = report.get("ok")
    promotion_gate = report.get("promote_to_grpo", report.get("promotion_gate_passed"))
    blockers: list[str] = []
    if ok is not True and status not in {"pass", "promote_to_grpo", "promotion_gate_passed"}:
        blockers.append("sft_v6_candidate_report_not_pass")
    if promotion_gate is not True:
        blockers.append("sft_v6_candidate_not_promoted_to_grpo")
    return blockers


def _validate_smoke_prereq() -> tuple[str, dict[str, Any]]:
    smoke_report_path = INPUT_ROOT / "kaggle_grpo_smoke_report.json"
    smoke_validation_path = INPUT_ROOT / "smoke_validation.json"
    smoke_report = _load_json(smoke_report_path)
    smoke_validation = _load_json(smoke_validation_path)
    blockers: list[str] = []
    if smoke_report.get("schema_version") != "dataforge_kaggle_grpo_smoke_report_v1":
        blockers.append("smoke_wrong_schema")
    if smoke_report.get("status") != "pass":
        blockers.append("smoke_not_pass")
    if smoke_report.get("training_stage") != "smoke":
        blockers.append("smoke_wrong_stage")
    if smoke_validation.get("schema_version") != "dataforge_grpo_smoke_validation_v1":
        blockers.append("smoke_validation_wrong_schema")
    if smoke_validation.get("ok") is not True or smoke_validation.get("blockers"):
        blockers.append("smoke_validation_not_pass")
    if blockers:
        raise RuntimeError("GRPO candidate blocked by smoke evidence: " + ", ".join(blockers))
    return _sha256_file(smoke_report_path), smoke_validation


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


def _prepare_dataset(config: dict[str, Any]) -> tuple[Any, dict[str, Any], int]:
    from datasets import Dataset
    from training.grpo_readiness import (
        GrpoReadinessSettings,
        analyze_grpo_readiness,
        build_prompt_example,
        load_split_manifest,
    )

    trajectory_name = str(config["readiness"].get("trajectory_filename", "expert_v4.jsonl"))
    manifest_name = str(config["readiness"].get("split_manifest_filename", "split_manifest_v4.json"))
    records = _load_jsonl(INPUT_ROOT / trajectory_name)
    split_eval_rows, split_source = load_split_manifest(INPUT_ROOT / manifest_name)
    readiness_report = analyze_grpo_readiness(
        records,
        split_eval_rows=split_eval_rows,
        split_source_provenance=split_source,
        settings=GrpoReadinessSettings.from_config(config),
    )
    if not readiness_report["ok"]:
        raise RuntimeError(f"GRPO readiness blocked: {readiness_report['blockers']}")
    prompt_rows: list[dict[str, Any]] = []
    for record in records:
        example = build_prompt_example(record)
        prompt_rows.append(
            {
                "prompt": example["prompt"],
                "ground_truth": example["ground_truth"],
                "allowed_columns": example["allowed_columns"],
                "valid_rows": example["valid_rows"],
                "dataset": example["dataset"],
                "base_dataset": example["base_dataset"],
                "inferability": example["inferability"],
                "prompt_contract_version": example["prompt_contract_version"],
            }
        )
    return Dataset.from_list(prompt_rows), readiness_report, len(records)


def _input_id_count(input_ids: Any) -> int:
    shape = getattr(input_ids, "shape", None)
    if shape is not None and len(shape) > 0:
        return int(shape[-1])
    if isinstance(input_ids, list | tuple):
        if not input_ids:
            return 0
        first = input_ids[0]
        if isinstance(first, list | tuple):
            return len(first)
        return len(input_ids)
    return len(input_ids)


def _count_prompt_tokens(tokenizer: Any, messages: list[dict[str, str]]) -> int:
    if getattr(tokenizer, "chat_template", None):
        rendered = tokenizer.apply_chat_template(
            messages,
            tokenize=True,
            add_generation_prompt=True,
        )
        if isinstance(rendered, dict):
            return _input_id_count(rendered["input_ids"])
        input_ids = getattr(rendered, "input_ids", None)
        if input_ids is not None:
            return _input_id_count(input_ids)
        return _input_id_count(rendered)
    rendered_text = "\n".join(f"{message['role']}: {message['content']}" for message in messages)
    encoded = tokenizer(rendered_text, add_special_tokens=True)
    if isinstance(encoded, dict):
        return _input_id_count(encoded["input_ids"])
    input_ids = getattr(encoded, "input_ids", None)
    if input_ids is not None:
        return _input_id_count(input_ids)
    return _input_id_count(encoded)


def _enforce_prompt_budget(train_dataset: Any, tokenizer: Any, prompt_token_budget: int) -> tuple[Any, int]:
    def enforce(record: dict[str, Any]) -> dict[str, Any]:
        messages = [dict(message) for message in record["prompt"]]
        payload = json.loads(messages[1]["content"])
        while _count_prompt_tokens(tokenizer, messages) > prompt_token_budget and payload.get(
            "context_rows"
        ):
            payload["context_rows"].pop()
            messages[1]["content"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        token_count = _count_prompt_tokens(tokenizer, messages)
        if token_count > prompt_token_budget:
            raise RuntimeError(
                f"Prompt exceeds prompt_token_budget={prompt_token_budget}: {token_count} tokens"
            )
        record["prompt"] = messages
        record["prompt_token_count"] = token_count
        return record

    train_dataset = train_dataset.map(enforce)
    max_prompt_tokens = max(train_dataset["prompt_token_count"]) if len(train_dataset) else 0
    return train_dataset, int(max_prompt_tokens)


def _render_model_card(metrics: dict[str, Any]) -> str:
    return (
        "---\n"
        f"license: {metrics['model_license']}\n"
        "datasets:\n"
        f"- {metrics['dataset_repo']}\n"
        f"base_model: {metrics['base_model']}\n"
        "library_name: transformers\n"
        "model-index:\n"
        f"- name: {metrics['model_name']}\n"
        "  results:\n"
        "  - task:\n"
        "      type: data-cleaning\n"
        "      name: Data quality repair\n"
        "    dataset:\n"
        f"      name: {metrics['benchmark_name']}\n"
        "      type: dataforge-bench-light-verified\n"
        "    metrics:\n"
        f"    - type: macro_f1\n      value: {metrics['grpo_f1']}\n      name: Strict macro F1\n"
        f"    - type: f1_delta\n      value: {metrics['f1_delta']}\n      name: Delta over predecessor\n"
        "---\n\n"
        f"# {metrics['model_name']}\n\n"
        f"GRPO checkpoint trained from predecessor `{metrics['sft_model']}` and uploaded only after "
        f"the strict held-out gate passed with F1 delta `{metrics['f1_delta']}`.\n\n"
        "## Evidence\n\n"
        f"- Benchmark: `{metrics['benchmark_name']}` over seeds `{metrics['benchmark_seeds']}`\n"
        f"- SFT strict macro F1: `{metrics['sft_f1']}`\n"
        f"- GRPO strict macro F1: `{metrics['grpo_f1']}`\n"
        f"- Parse success: `{metrics['parse_success_rate']}`\n"
        f"- Schema-case errors: `{metrics['schema_case_error_count']}`\n"
        f"- Training stage: `{metrics['training_stage']}` with `{metrics['attempted_steps']}` steps\n\n"
        "## Limitations\n\n"
        "This is a research artifact for DataForge repair evaluation. It is not production autonomous "
        "repair software and must not mutate data without DataForge verification, receipts, and human "
        "approval.\n"
    )


def main() -> int:
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    started_at = time.time()
    upload_attempted = False
    hf_token: str | None = None
    hf_token_source = "unavailable"
    hf_token_error = ""
    _log_event("candidate_start", stage=os.environ.get("DATAFORGE_GRPO_STAGE", "candidate"))
    _extract_source()
    _log_event("source_extracted", input_root=str(INPUT_ROOT))
    smoke_report_sha256, smoke_validation = _validate_smoke_prereq()
    _log_event("smoke_validated", smoke_report_sha256=smoke_report_sha256)
    config = _load_config()
    selected_stage_name = os.environ.get("DATAFORGE_GRPO_STAGE", "candidate")
    stage_by_name = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    selected_stage = stage_by_name.get(selected_stage_name)
    if selected_stage_name not in {"diagnostic", "candidate"} or selected_stage is None:
        raise RuntimeError(
            "This entrypoint is restricted to DATAFORGE_GRPO_STAGE=diagnostic or candidate."
        )
    configured_steps = int(selected_stage["max_steps"])
    if selected_stage_name == "diagnostic" and configured_steps != 250:
        raise RuntimeError("GRPO diagnostic stage must run exactly 250 configured steps.")
    if selected_stage_name == "candidate" and configured_steps != 500:
        raise RuntimeError("GRPO candidate stage must run exactly 500 configured steps.")
    upload_allowed_after_gate = bool(selected_stage.get("allow_upload_after_gate", False))
    if selected_stage_name == "candidate" and not upload_allowed_after_gate:
        raise RuntimeError("GRPO candidate stage must allow upload only after the gate.")
    if selected_stage_name == "diagnostic" and upload_allowed_after_gate:
        raise RuntimeError("GRPO diagnostic stage must not allow upload.")
    config["training"]["max_steps"] = configured_steps
    predecessor_blockers = _sft_v6_predecessor_blockers(config)
    if predecessor_blockers:
        _write_report(
            {
                "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
                "status": "blocked_missing_sft_v6_predecessor",
                "blockers": predecessor_blockers,
                "training_stage": selected_stage_name,
                "configured_max_steps": configured_steps,
                "smoke_report_sha256": smoke_report_sha256,
                "smoke_validation": smoke_validation,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "note": "GRPO-v3 requires a verified SFT-v6 candidate eval report before spending GPU time.",
            }
        )
        _log_event("blocked_missing_sft_v6_predecessor", blockers=predecessor_blockers)
        return 0
    _write_report(
        {
            "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
            "status": "preflight",
            "training_stage": selected_stage_name,
            "smoke_report_sha256": smoke_report_sha256,
            "smoke_validation": smoke_validation,
            "model_upload_attempted": False,
        }
    )
    try:
        hf_token, hf_token_source = _load_hf_token()
    except RuntimeError as exc:
        hf_token_error = str(exc)
        _log_event("hf_token_unavailable_preflight")
        _write_report(
            {
                "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
                "status": "preflight_no_hf_token",
                "training_stage": selected_stage_name,
                "smoke_report_sha256": smoke_report_sha256,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "upload_blocker": hf_token_error,
                "note": "Training and strict held-out eval will continue; upload remains blocked unless a token is visible after the gate.",
            }
        )
    if hf_token:
        os.environ["HF_TOKEN"] = hf_token
        os.environ["HUGGING_FACE_HUB_TOKEN"] = hf_token
        _log_event("hf_token_available_preflight", source=hf_token_source)
    _log_event("install_stack_start")
    _install_stack(config)
    _log_event("install_stack_done")

    import inspect

    import torch
    from huggingface_hub import HfApi
    from peft import LoraConfig, PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
    from trl import GRPOConfig, GRPOTrainer

    from scripts.model.verify_grpo_release import verify_local_grpo_artifact_dir
    from training.grpo_config import build_grpo_config_kwargs
    from training.grpo_eval import (
        build_heldout_tasks,
        evaluate_causal_lm,
        grpo_gate_failures,
    )
    from training.rewards.dataforge_reward import dataforge_reward

    if not torch.cuda.is_available():
        raise RuntimeError("Kaggle GPU runtime is required for GRPO candidate.")
    capability = torch.cuda.get_device_capability(0)
    if capability[0] < 7:
        raise RuntimeError(
            "GRPO candidate requires a T4-or-newer Kaggle GPU for the installed PyTorch wheel; "
            f"received {torch.cuda.get_device_name(0)} with capability sm_{capability[0]}{capability[1]}."
        )
    api = HfApi(token=hf_token)
    _log_event(
        "gpu_ready",
        gpu_name=torch.cuda.get_device_name(0),
        capability=f"sm_{capability[0]}{capability[1]}",
    )
    dataset_repo = str(config["repos"]["source_dataset_repo"])
    target_model_repo = str(config["model"]["target_model_repo"])
    sft_checkpoint = str(config["model"]["sft_checkpoint"])
    dataset_info = api.repo_info(dataset_repo, repo_type="dataset", token=hf_token)
    api.repo_info(sft_checkpoint, repo_type="model", token=hf_token)

    train_dataset, readiness_report, records_seen = _prepare_dataset(config)
    _log_event(
        "dataset_ready",
        records_seen=records_seen,
        valid_prompt_records=len(train_dataset),
    )
    tokenizer = AutoTokenizer.from_pretrained(
        sft_checkpoint,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.truncation_side = "left"
    tokenizer.padding_side = "left"
    prompt_token_budget = int(config["training"]["prompt_token_budget"])
    train_dataset, max_prompt_tokens = _enforce_prompt_budget(
        train_dataset,
        tokenizer,
        prompt_token_budget,
    )

    quant_cfg = None
    if config.get("quantization", {}).get("load_in_4bit"):
        quant_cfg = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type=config["quantization"]["bnb_4bit_quant_type"],
            bnb_4bit_use_double_quant=config["quantization"]["bnb_4bit_use_double_quant"],
            bnb_4bit_compute_dtype=torch.float16,
        )
    model = AutoModelForCausalLM.from_pretrained(
        sft_checkpoint,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
        torch_dtype=torch.float16,
        quantization_config=quant_cfg,
        device_map={"": 0},
    )
    model.config.use_cache = bool(config["training"].get("use_cache", False))
    grpo_kwargs = build_grpo_config_kwargs(
        config,
        supported_keys=set(inspect.signature(GRPOConfig).parameters),
    )
    grpo_kwargs["output_dir"] = config["training"]["output_dir"]
    training_args = GRPOConfig(**grpo_kwargs)
    peft_config = LoraConfig(
        r=config["lora"]["r"],
        lora_alpha=config["lora"]["alpha"],
        lora_dropout=config["lora"]["dropout"],
        bias=config["lora"]["bias"],
        task_type=config["lora"]["task_type"],
        target_modules=config["lora"]["target_modules"],
    )
    trainer = GRPOTrainer(
        model=model,
        reward_funcs=dataforge_reward,
        args=training_args,
        train_dataset=train_dataset,
        processing_class=tokenizer,
        peft_config=peft_config,
    )
    _log_event("training_start", max_steps=int(config["training"]["max_steps"]))
    train_started = time.time()
    train_result = trainer.train()
    _log_event("training_done")
    train_metrics = getattr(train_result, "metrics", {}) or {}
    adapter_dir = Path(config["training"]["adapter_dir"])
    trainer.save_model(str(adapter_dir))
    attempted_steps = int(getattr(train_result, "global_step", config["training"]["max_steps"]))
    gpu_hours = round((time.time() - train_started) / 3600.0, 4)
    del trainer, model
    gc.collect()
    torch.cuda.empty_cache()

    eval_cfg = config["evaluation"]
    tasks, task_manifest = build_heldout_tasks(
        datasets=eval_cfg["datasets"],
        heldout_tasks=int(eval_cfg["heldout_tasks"]),
        benchmark_seeds=config["release"]["benchmark_seeds"],
        seeds_start=int(eval_cfg["seeds_start"]),
        chunk_width=int(eval_cfg["chunk_width"]),
        cache_root=WORK_ROOT / "dataforge-cache",
    )
    _log_event("heldout_tasks_ready", task_count=len(tasks))
    merged_dir = Path(config["training"]["merged_dir"])
    merged_dir.mkdir(parents=True, exist_ok=True)
    task_manifest["max_new_tokens"] = int(eval_cfg["max_new_tokens"])
    (merged_dir / "eval_task_manifest.json").write_text(
        json.dumps(task_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    eval_task_manifest_sha256 = _sha256_file(merged_dir / "eval_task_manifest.json")

    sft_eval_model = AutoModelForCausalLM.from_pretrained(
        sft_checkpoint,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
        torch_dtype=torch.float16,
        device_map="auto",
    )
    sft_eval, sft_diagnostics = evaluate_causal_lm(
        sft_eval_model,
        tokenizer,
        tasks,
        model_label="sft",
        max_new_tokens=int(eval_cfg["max_new_tokens"]),
    )
    _log_event("sft_eval_done", macro_f1=float(sft_eval["macro_f1"]))
    del sft_eval_model
    gc.collect()
    torch.cuda.empty_cache()

    base_for_merge = AutoModelForCausalLM.from_pretrained(
        sft_checkpoint,
        token=hf_token,
        trust_remote_code=bool(config["model"].get("trust_remote_code", True)),
        torch_dtype=torch.float16,
        device_map="auto",
    )
    merged_model = PeftModel.from_pretrained(base_for_merge, str(adapter_dir))
    merged_model = merged_model.merge_and_unload()
    merged_model.save_pretrained(merged_dir, safe_serialization=True)
    tokenizer.save_pretrained(merged_dir)
    grpo_eval, grpo_diagnostics = evaluate_causal_lm(
        merged_model,
        tokenizer,
        tasks,
        model_label="grpo",
        max_new_tokens=int(eval_cfg["max_new_tokens"]),
    )
    _log_event("grpo_eval_done", macro_f1=float(grpo_eval["macro_f1"]))
    del merged_model
    gc.collect()
    torch.cuda.empty_cache()

    gate_failures = grpo_gate_failures(
        sft_eval=sft_eval,
        grpo_eval=grpo_eval,
        min_absolute_f1_gain=float(config["release"]["min_absolute_f1_gain"]),
        min_parse_success_rate=float(config["release"]["require_parse_success_rate"]),
        required_schema_case_error_count=int(config["release"]["require_schema_case_error_count"]),
        target_strict_macro_f1=(
            float(config["release"]["target_strict_macro_f1"])
            if "target_strict_macro_f1" in config["release"]
            else None
        ),
        min_not_inferable_slice_f1=(
            float(config["release"]["require_not_inferable_slice_f1"])
            if "require_not_inferable_slice_f1" in config["release"]
            else None
        ),
        min_deterministic_normalization_slice_f1=(
            float(config["release"]["require_deterministic_normalization_slice_f1"])
            if "require_deterministic_normalization_slice_f1" in config["release"]
            else None
        ),
    )
    acceptance_gate_passed = not gate_failures
    _log_event("gate_evaluated", passed=acceptance_gate_passed, gate_failures=gate_failures)
    sft_f1 = float(sft_eval["macro_f1"])
    grpo_f1 = float(grpo_eval["macro_f1"])
    f1_delta = round(grpo_f1 - sft_f1, 4)
    failure_samples = grpo_diagnostics["failure_samples"][:25]
    eval_diagnostics = {
        "schema_version": "dataforge_grpo_eval_diagnostics_v1",
        "benchmark_name": config["release"]["benchmark_name"],
        "benchmark_seeds": config["release"]["benchmark_seeds"],
        "task_manifest_sha256": eval_task_manifest_sha256,
        "source_audit": task_manifest["source_audit"],
        "sft_eval": sft_eval,
        "grpo_eval": grpo_eval,
        "sft": sft_diagnostics,
        "grpo": grpo_diagnostics,
        "failure_samples": failure_samples,
        "gate_failures": gate_failures,
        "limitations": [
            "Strict held-out eval verifies this model only for DataForge repair research tasks.",
            "This is not production autonomous repair evidence.",
        ],
    }
    (merged_dir / "eval_diagnostics.json").write_text(
        json.dumps(eval_diagnostics, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    metrics = {
        "model_name": target_model_repo.split("/")[-1],
        "model_license": config["model"]["model_license"],
        "base_model": config["model"]["base_model"],
        "sft_model": sft_checkpoint,
        "dataset_repo": dataset_repo,
        "dataset_sha": dataset_info.sha,
        "source_git_commit": os.environ.get("DATAFORGE_SOURCE_COMMIT", "unknown"),
        "benchmark_name": config["release"]["benchmark_name"],
        "benchmark_seeds": config["release"]["benchmark_seeds"],
        "gpu_hours": gpu_hours,
        "attempted_steps": attempted_steps,
        "training_stage": selected_stage_name,
        "smoke_report_sha256": smoke_report_sha256,
        "eval_task_manifest_sha256": eval_task_manifest_sha256,
        "sft_f1": sft_f1,
        "grpo_f1": grpo_f1,
        "f1_delta": f1_delta,
        "parse_success_rate": float(grpo_eval["parse_success_rate"]),
        "schema_case_error_count": int(grpo_eval["schema_case_error_count"]),
        "failure_samples": failure_samples,
        "failure_sample_count": len(failure_samples),
        "quality_gate_failures": gate_failures,
        "acceptance_gate_passed": acceptance_gate_passed,
        "run_date_utc": datetime.now(UTC).isoformat(),
        "records_seen": records_seen,
        "valid_prompt_records": len(train_dataset),
        "max_prompt_tokens": max_prompt_tokens,
        "prompt_token_budget": prompt_token_budget,
        "readiness_status": readiness_report["status"],
        "readiness_blockers": readiness_report["blockers"],
        "smoke_validation": smoke_validation,
        "train_metrics": train_metrics,
        "hf_token_source": hf_token_source,
        "upload_repo_private": bool(config["release"].get("upload_repo_private", False)),
    }
    (merged_dir / "training_metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (merged_dir / "README.md").write_text(_render_model_card(metrics), encoding="utf-8")
    if not acceptance_gate_passed:
        _write_report(
            {
                "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
                "status": "quality_gate_failed_no_upload",
                "gate_failures": gate_failures,
                "training_stage": selected_stage_name,
                "attempted_steps": attempted_steps,
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_hours": gpu_hours,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "merged_dir": str(merged_dir),
                "training_metrics": metrics,
            }
        )
        _log_event("quality_gate_failed_no_upload", gate_failures=gate_failures)
        return 0

    if not upload_allowed_after_gate:
        _write_report(
            {
                "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
                "status": "diagnostic_complete_no_upload",
                "training_stage": selected_stage_name,
                "attempted_steps": attempted_steps,
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_hours": gpu_hours,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "merged_dir": str(merged_dir),
                "training_metrics": metrics,
            }
        )
        _log_event("diagnostic_complete_no_upload")
        return 0

    verify_local_grpo_artifact_dir(merged_dir, model_repo=target_model_repo)
    _log_event("local_artifact_verified")
    upload_token = hf_token
    upload_token_source = hf_token_source
    upload_blocker = hf_token_error
    if not upload_token:
        try:
            upload_token, upload_token_source = _load_hf_token()
        except RuntimeError as exc:
            upload_blocker = str(exc)
    if not upload_token:
        metrics["hf_token_source"] = "unavailable"
        metrics["upload_blocker"] = upload_blocker
        (merged_dir / "training_metrics.json").write_text(
            json.dumps(metrics, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _write_report(
            {
                "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
                "status": "pass_upload_blocked_missing_hf_token",
                "training_stage": selected_stage_name,
                "attempted_steps": attempted_steps,
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_hours": gpu_hours,
                "model_upload_attempted": False,
                "model_repo_created": False,
                "public_claim_updated": False,
                "model_repo": target_model_repo,
                "merged_dir": str(merged_dir),
                "upload_blocker": upload_blocker,
                "training_metrics": metrics,
                "total_runtime_hours": round((time.time() - started_at) / 3600.0, 4),
            }
        )
        print(
            json.dumps(
                {
                    "status": "pass_upload_blocked_missing_hf_token",
                    "repo": target_model_repo,
                },
                sort_keys=True,
            )
        )
        return 0
    metrics["hf_token_source"] = upload_token_source
    (merged_dir / "training_metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    upload_attempted = True
    _log_event("upload_start", repo=target_model_repo)
    upload_api = HfApi(token=upload_token)
    upload_private = bool(config["release"].get("upload_repo_private", False))
    upload_api.create_repo(
        repo_id=target_model_repo,
        repo_type="model",
        exist_ok=True,
        private=upload_private,
        token=upload_token,
    )
    upload_api.upload_folder(
        folder_path=str(merged_dir),
        repo_id=target_model_repo,
        repo_type="model",
        token=upload_token,
        commit_message=f"Publish {metrics['model_name']} after strict GRPO gate",
    )
    _log_event("upload_done", repo=target_model_repo)
    _write_report(
        {
            "schema_version": "dataforge_kaggle_grpo_candidate_report_v1",
            "status": "pass_uploaded",
            "training_stage": selected_stage_name,
            "attempted_steps": attempted_steps,
            "gpu_name": torch.cuda.get_device_name(0),
            "gpu_hours": gpu_hours,
            "model_upload_attempted": upload_attempted,
            "model_repo_created": True,
            "public_claim_updated": False,
            "model_repo": target_model_repo,
            "model_repo_private": upload_private,
            "training_metrics": metrics,
            "total_runtime_hours": round((time.time() - started_at) / 3600.0, 4),
        }
    )
    print(json.dumps({"status": "pass_uploaded", "repo": target_model_repo}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
