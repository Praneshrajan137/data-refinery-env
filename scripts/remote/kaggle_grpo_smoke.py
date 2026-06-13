"""Kaggle entrypoint for the DataForge 0.5B GRPO no-upload smoke run.

This script is intentionally a smoke runner, not a release publisher. It trains
the configured GRPO stage, writes bounded diagnostics under /kaggle/working,
and exits successfully only if no upload was attempted.
"""

from __future__ import annotations

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

DEFAULT_INPUT_ROOT = Path("/kaggle/input/dataforge-grpo-smoke-handoff")
INPUT_ROOT = Path(os.environ.get("DATAFORGE_GRPO_INPUT", str(DEFAULT_INPUT_ROOT)))
WORK_ROOT = Path("/kaggle/working")
SOURCE_ROOT = WORK_ROOT / "dataforge-src"
REPORT_PATH = WORK_ROOT / "kaggle_grpo_smoke_report.json"
TRAJECTORY_INPUT_FILES = {
    "expert_v4.jsonl",
    "expert_v5_repair_curriculum.jsonl",
    "expert_v6_contract_minimal.jsonl",
}
SFT_V6_PREDECESSOR_REPORT = "sft_v6_candidate_eval_report.json"


def _has_smoke_inputs(file_names: set[str]) -> bool:
    return {"split_manifest_v4.json", "grpo_05b.yaml"} <= file_names and bool(
        file_names & TRAJECTORY_INPUT_FILES
    )


def _write_report(payload: dict[str, Any]) -> None:
    REPORT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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
            rel = (rel_root / name).as_posix()
            listing.append(rel)
            if len(listing) >= limit:
                return listing
    return listing


def _load_hf_token(*, required: bool = False) -> tuple[str | None, str]:
    token = (
        os.environ.get("HF_TOKEN")
        or os.environ.get("HUGGING_FACE_HUB_TOKEN")
        or os.environ.get("HUGGINGFACE_HUB_TOKEN")
        or ""
    ).strip()
    if token:
        return token, "environment"
    try:
        from kaggle_secrets import UserSecretsClient

        token = (UserSecretsClient().get_secret("HF_TOKEN") or "").strip()
    except Exception as exc:  # pragma: no cover - exercised only inside Kaggle
        if required:
            raise RuntimeError("HF_TOKEN Kaggle secret is required for GRPO smoke.") from exc
        return None, "not_available_public_download"
    if not token:
        if required:
            raise RuntimeError("HF_TOKEN Kaggle secret is empty.")
        return None, "not_available_public_download"
    return token, "kaggle_secret"


def _extract_source() -> None:
    global INPUT_ROOT
    source_zip = INPUT_ROOT / "source.zip"
    source_dir = INPUT_ROOT / "source"
    if not source_zip.exists():
        for candidate in _source_zip_candidates():
            sibling_names = {path.name for path in candidate.parent.iterdir() if path.is_file()}
            if _has_smoke_inputs(sibling_names):
                INPUT_ROOT = candidate.parent
                source_zip = candidate
                source_dir = INPUT_ROOT / "source"
                break
    if not source_zip.exists() and not source_dir.exists():
        for candidate in _source_dir_candidates():
            sibling_names = {path.name for path in candidate.parent.iterdir() if path.is_file()}
            if _has_smoke_inputs(sibling_names):
                INPUT_ROOT = candidate.parent
                source_dir = candidate
                break
    if not source_zip.exists() and not source_dir.exists():
        raise RuntimeError(
            f"Missing source bundle: {source_zip}. Visible /kaggle/input entries: "
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


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path} must contain a JSON object.")
    return cast(dict[str, Any], payload)


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
    readiness_settings = GrpoReadinessSettings.from_config(config)
    readiness_report = analyze_grpo_readiness(
        records,
        split_eval_rows=split_eval_rows,
        split_source_provenance=split_source,
        settings=readiness_settings,
    )
    _write_report(
        {
            "schema_version": "dataforge_kaggle_grpo_smoke_report_v1",
            "status": "preflight",
            "readiness": readiness_report,
            "model_upload_attempted": False,
        }
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
    try:
        return len(input_ids)
    except TypeError as exc:
        raise RuntimeError("Tokenizer returned input_ids without a countable length.") from exc


def _count_prompt_tokens(tokenizer: Any, messages: list[dict[str, str]]) -> int:
    if getattr(tokenizer, "chat_template", None):
        rendered = tokenizer.apply_chat_template(
            messages,
            tokenize=True,
            add_generation_prompt=True,
        )
        if isinstance(rendered, dict):
            if "input_ids" not in rendered:
                raise RuntimeError("Tokenizer chat template returned a mapping without input_ids.")
            return _input_id_count(rendered["input_ids"])
        input_ids = getattr(rendered, "input_ids", None)
        if input_ids is not None:
            return _input_id_count(input_ids)
        return _input_id_count(rendered)
    rendered_text = "\n".join(f"{message['role']}: {message['content']}" for message in messages)
    encoded = tokenizer(rendered_text, add_special_tokens=True)
    if isinstance(encoded, dict):
        if "input_ids" not in encoded:
            raise RuntimeError("Tokenizer returned a mapping without input_ids.")
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


def main() -> int:
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    started_at = time.time()
    _extract_source()
    config = _load_config()
    selected_stage_name = os.environ.get("DATAFORGE_GRPO_STAGE", "smoke")
    stage_by_name = {stage["name"]: stage for stage in config["training_sequence"]["stages"]}
    if selected_stage_name != "smoke":
        raise RuntimeError("This entrypoint is restricted to the no-upload smoke stage.")
    selected_stage = stage_by_name[selected_stage_name]
    if bool(selected_stage.get("allow_upload_after_gate", selected_stage.get("allow_upload", False))):
        raise RuntimeError("Smoke stage must not allow upload.")
    config["training"]["max_steps"] = int(selected_stage["max_steps"])
    predecessor_blockers = _sft_v6_predecessor_blockers(config)
    if predecessor_blockers:
        report = {
            "schema_version": "dataforge_kaggle_grpo_smoke_report_v1",
            "status": "blocked_missing_sft_v6_predecessor",
            "blockers": predecessor_blockers,
            "training_stage": selected_stage_name,
            "configured_max_steps": int(config["training"]["max_steps"]),
            "model_upload_attempted": False,
            "model_repo_created": False,
            "public_claim_updated": False,
            "note": "GRPO-v3 requires a verified SFT-v6 candidate eval report before spending GPU time.",
        }
        _write_report(report)
        print(
            json.dumps(
                {"status": report["status"], "blockers": predecessor_blockers},
                sort_keys=True,
            )
        )
        return 0
    _install_stack(config)

    import inspect

    import torch
    from peft import LoraConfig
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
    from trl import GRPOConfig, GRPOTrainer

    from training.grpo_config import build_grpo_config_kwargs
    from training.rewards.dataforge_reward import dataforge_reward

    if not torch.cuda.is_available():
        raise RuntimeError("Kaggle GPU runtime is required for GRPO smoke.")
    capability = torch.cuda.get_device_capability(0)
    if capability[0] < 7:
        raise RuntimeError(
            "GRPO smoke requires a T4-or-newer Kaggle GPU for the installed PyTorch wheel; "
            f"received {torch.cuda.get_device_name(0)} with capability sm_{capability[0]}{capability[1]}."
        )
    hf_token, hf_token_source = _load_hf_token(required=False)
    if hf_token:
        os.environ["HF_TOKEN"] = hf_token
        os.environ["HUGGING_FACE_HUB_TOKEN"] = hf_token

    train_dataset, readiness_report, records_seen = _prepare_dataset(config)
    sft_checkpoint = str(config["model"]["sft_checkpoint"])
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

    supported_grpo_keys = set(inspect.signature(GRPOConfig).parameters)
    grpo_kwargs = build_grpo_config_kwargs(config, supported_keys=supported_grpo_keys)
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
    train_result = trainer.train()
    adapter_dir = Path(config["training"]["adapter_dir"])
    trainer.save_model(str(adapter_dir))

    metrics = getattr(train_result, "metrics", {}) or {}
    report = {
        "schema_version": "dataforge_kaggle_grpo_smoke_report_v1",
        "status": "pass",
        "run_date_utc": datetime.now(UTC).isoformat(),
        "training_stage": selected_stage_name,
        "attempted_steps": int(getattr(train_result, "global_step", config["training"]["max_steps"])),
        "configured_max_steps": int(config["training"]["max_steps"]),
        "gpu_name": torch.cuda.get_device_name(0),
        "gpu_hours": round((time.time() - started_at) / 3600.0, 4),
        "records_seen": records_seen,
        "valid_prompt_records": len(train_dataset),
        "max_prompt_tokens": max_prompt_tokens,
        "prompt_token_budget": prompt_token_budget,
        "readiness_status": readiness_report["status"],
        "readiness_blockers": readiness_report["blockers"],
        "train_metrics": metrics,
        "adapter_dir": str(adapter_dir),
        "model_upload_attempted": False,
        "model_repo_created": False,
        "public_claim_updated": False,
        "hf_token_source": hf_token_source,
    }
    _write_report(report)
    print(json.dumps({key: report[key] for key in ("status", "attempted_steps", "gpu_name", "gpu_hours")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
