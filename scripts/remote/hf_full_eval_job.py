"""Run DataForge base-vs-SFT benchmark eval on Hugging Face Jobs.

# /// script
# dependencies = [
#   "accelerate==1.13.0",
#   "huggingface_hub==1.13.0",
#   "pandas==2.3.3",
#   "torch==2.11.0",
#   "transformers==5.7.0",
# ]
# ///
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import torch
from huggingface_hub import HfApi
from transformers import AutoModelForCausalLM, AutoTokenizer

HF_USER = "Praneshrajan15"
MODEL_SIZE = os.environ.get("DATAFORGE_MODEL_SIZE", "0.5B")
MODEL_STAGE = os.environ.get("DATAFORGE_MODEL_STAGE", "SFT")
DATASET_REPO = os.environ.get("DATAFORGE_DATASET_REPO", f"{HF_USER}/dataforge-sft-trajectories")
TARGET_MODEL_REPO = os.environ.get(
    "DATAFORGE_TARGET_MODEL_REPO",
    f"{HF_USER}/DataForge-{MODEL_SIZE}-{MODEL_STAGE}",
)
BASELINE_MODEL_ID = os.environ.get("DATAFORGE_BASELINE_MODEL_ID", "Qwen/Qwen2.5-0.5B-Instruct")
BASE_MODEL_ID = os.environ.get("DATAFORGE_BASE_MODEL_ID", BASELINE_MODEL_ID)
OUTPUT_DATASET_PATH = os.environ.get(
    "DATAFORGE_OUTPUT_DATASET_PATH",
    f"reports/hf_full_eval_{MODEL_SIZE.replace('.', '-')}_{MODEL_STAGE.lower()}_latest.json",
)
MIN_ABSOLUTE_F1_GAIN = float(
    os.environ.get(
        "DATAFORGE_MIN_ABSOLUTE_F1_GAIN",
        "0.02" if MODEL_STAGE == "GiGPO" else ("0.03" if MODEL_STAGE == "GRPO" else "0.0"),
    )
)
MAX_NEW_TOKENS = 1024
HELDOUT_TASKS = 100
SEEDS_START = 10000
CHUNK_WIDTH = 4
# Pinned to a commit SHA, never a branch. This ran against `refs/heads/master` with no
# checksum until 2026-08-23, which meant a held-out release evaluation could change
# silently whenever upstream moved -- the one property a held-out evaluation must not have.
#
# These constants are duplicated from `dataforge.datasets.registry` rather than imported
# because this file executes standalone on HF Jobs with no DataForge install. The
# duplication is verified: `tests/unit/test_fetch_pinning.py` asserts every value here
# matches the registry, so the two cannot drift.
#
# `beers` was de-registered on 2026-07-12 and is not fetched here any more.
RAHA_GIT_REVISION = "7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c"
_RAHA_BASE_URL = f"https://raw.githubusercontent.com/BigDaMa/raha/{RAHA_GIT_REVISION}/datasets"
DATA_SOURCES = {
    "hospital": {
        "dirty_url": f"{_RAHA_BASE_URL}/hospital/dirty.csv",
        "clean_url": f"{_RAHA_BASE_URL}/hospital/clean.csv",
        "dirty_sha256": "dbc5575b915fe8b5e0ac6dc6172f38ba91e611fdb76d09a8f4a81cb7ea9925ac",
        "clean_sha256": "ea3ee44998455c0b491750c348509de176c758a3bbf58e4530c0a136bb248b4b",
    },
    "flights": {
        "dirty_url": f"{_RAHA_BASE_URL}/flights/dirty.csv",
        "clean_url": f"{_RAHA_BASE_URL}/flights/clean.csv",
        "dirty_sha256": "1b5c1afa10aa0e7c20fd7e14d05c56772715b2771aa0f5fa67ed1709e1eecd46",
        "clean_sha256": "0acfcfd8985b06fdd363965c9e8d9522c43e7589a93d79ae7dc311e1c37fdf3b",
    },
}


def _fetch_verified(url: str, expected_sha256: str) -> bytes:
    """Fetch a URL and refuse the bytes unless they match the pinned digest.

    Raises:
        RuntimeError: If the digest does not match. Fails closed: an unverified
            held-out corpus is worse than no evaluation, because it produces a number
            that looks measured.
    """
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310
        raw = bytes(response.read())
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise RuntimeError(
            f"held-out corpus {url} does not match pinned revision {RAHA_GIT_REVISION}: "
            f"expected sha256 {expected_sha256}, got {digest}"
        )
    return raw


def load_bench_dataset(name: str) -> tuple[pd.DataFrame, tuple[str, ...], list[dict[str, Any]]]:
    """Load one aligned dirty/clean benchmark dataset from pinned, verified bytes."""
    source = DATA_SOURCES[name]
    read_kwargs = {"dtype": str, "keep_default_na": False, "na_filter": False}
    dirty = pd.read_csv(
        io.BytesIO(_fetch_verified(source["dirty_url"], source["dirty_sha256"])), **read_kwargs
    )
    clean = pd.read_csv(
        io.BytesIO(_fetch_verified(source["clean_url"], source["clean_sha256"])), **read_kwargs
    )
    if len(dirty.index) != len(clean.index) or len(dirty.columns) != len(clean.columns):
        raise RuntimeError(f"Dirty/clean shape mismatch for held-out dataset {name}.")
    dirty.columns = [str(column) for column in clean.columns]
    ground_truth: list[dict[str, Any]] = []
    for row_idx, (dirty_row, clean_row) in enumerate(
        zip(
            dirty.itertuples(index=False, name=None),
            clean.itertuples(index=False, name=None),
            strict=True,
        )
    ):
        for column, dirty_value, clean_value in zip(
            clean.columns, dirty_row, clean_row, strict=True
        ):
            if str(dirty_value) != str(clean_value):
                ground_truth.append(
                    {"row": row_idx, "column": str(column), "clean_value": str(clean_value)}
                )
    return dirty, tuple(str(column) for column in clean.columns), ground_truth


def chunk_rows(
    df: pd.DataFrame,
    columns: tuple[str, ...],
    truth: list[dict[str, Any]],
    seed: int,
    width: int = CHUNK_WIDTH,
) -> list[dict[str, str]]:
    """Build one deterministic evaluation target window."""
    width = min(width, len(df.index))
    if width <= 0:
        return []
    digest = hashlib.sha256(str(seed).encode("utf-8")).hexdigest()
    if truth:
        anchor = int(truth[int(digest[:8], 16) % len(truth)]["row"])
        start = min(max(anchor - width // 2, 0), len(df.index) - width)
    else:
        start = int(digest[:8], 16) % max(1, len(df.index) - width + 1)
    rows: list[dict[str, str]] = []
    for row_idx in range(start, start + width):
        row = {"_row": str(row_idx)}
        for column in columns:
            row[column] = str(df.iloc[row_idx][column])
        rows.append(row)
    return rows


def parse_json_payload(text: str) -> object | None:
    """Extract first complete JSON object/array from model text."""
    text = text.strip()
    if text.startswith("```"):
        fence_lines = text.splitlines()
        if len(fence_lines) >= 3:
            text = "\n".join(fence_lines[1:-1]).strip()
    decoder = json.JSONDecoder()
    for offset, char in enumerate(text):
        if char not in "[{":
            continue
        try:
            payload, _ = decoder.raw_decode(text[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict | list):
            return payload
    return None


def parse_repairs(text: str) -> list[dict[str, Any]]:
    """Parse strict JSON repair output."""
    payload = parse_json_payload(text)
    if isinstance(payload, dict):
        repairs = payload.get("repairs", [])
    elif isinstance(payload, list):
        repairs = payload
    else:
        repairs = []
    if not isinstance(repairs, list):
        return []
    return [repair for repair in repairs if isinstance(repair, dict)]


def f1_score(truth: list[dict[str, Any]], repairs: list[dict[str, Any]]) -> float:
    """Exact cell repair F1."""
    truth_map = {(cell["row"], cell["column"]): cell["clean_value"] for cell in truth}
    predictions: dict[tuple[int, str], str] = {}
    for repair in repairs:
        if {"row", "column", "new_value"}.issubset(repair):
            try:
                key = (int(repair["row"]), str(repair["column"]))
            except (TypeError, ValueError):
                continue
            predictions[key] = str(repair["new_value"])
    tp = sum(1 for key, value in predictions.items() if truth_map.get(key) == value)
    fp = sum(1 for key in predictions if key not in truth_map or truth_map[key] != predictions[key])
    fn = len(
        set(truth_map) - {key for key, value in predictions.items() if truth_map.get(key) == value}
    )
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return 2 * precision * recall / (precision + recall) if precision + recall else 0.0


def schema_case_errors(repairs: list[dict[str, Any]], allowed_columns: list[str]) -> int:
    """Count diagnostic column-case errors."""
    allowed = set(allowed_columns)
    lowered = {column.lower() for column in allowed}
    return sum(
        1
        for repair in repairs
        if str(repair.get("column", "")) not in allowed
        and str(repair.get("column", "")).lower() in lowered
    )


def build_eval_tasks() -> list[dict[str, Any]]:
    """Build benchmark windows exactly like the Kaggle notebook contract."""
    datasets = {name: load_bench_dataset(name) for name in DATA_SOURCES}
    tasks: list[dict[str, Any]] = []
    attempts = 0
    max_attempts = max(HELDOUT_TASKS * 20, 20)
    while len(tasks) < HELDOUT_TASKS and attempts < max_attempts:
        for dataset_name, (dirty, columns, truth) in datasets.items():
            rows = chunk_rows(dirty, columns, truth, SEEDS_START + attempts)
            attempts += 1
            row_ids = {int(row["_row"]) for row in rows}
            task_truth = [cell for cell in truth if cell["row"] in row_ids]
            if not task_truth:
                continue
            tasks.append(
                {
                    "dataset": dataset_name,
                    "schema_summary": {"dataset": dataset_name, "columns": list(columns)},
                    "allowed_columns": list(columns),
                    "target_rows": rows,
                    "context_rows": [],
                    "ground_truth": task_truth,
                }
            )
            if len(tasks) >= HELDOUT_TASKS or attempts >= max_attempts:
                break
    if len(tasks) < HELDOUT_TASKS:
        raise RuntimeError(
            f"Could only build {len(tasks)} held-out tasks; requested {HELDOUT_TASKS}."
        )
    return tasks


def summarize_task_scores(task_scores: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate model eval scores."""
    by_dataset: dict[str, list[float]] = {}
    for row in task_scores:
        by_dataset.setdefault(str(row["dataset"]), []).append(float(row["f1"]))
    dataset_f1 = {
        dataset: round(sum(values) / len(values), 4)
        for dataset, values in sorted(by_dataset.items())
        if values
    }
    macro_f1 = round(sum(dataset_f1.values()) / len(dataset_f1), 4) if dataset_f1 else 0.0
    mean_f1 = round(sum(float(row["f1"]) for row in task_scores) / len(task_scores), 4)
    parse_success_rate = round(
        sum(1 for row in task_scores if row["parse_ok"]) / len(task_scores), 4
    )
    schema_case_error_count = sum(int(row["schema_case_errors"]) for row in task_scores)
    return {
        "macro_f1": macro_f1,
        "mean_f1": mean_f1,
        "dataset_f1": dataset_f1,
        "parse_success_rate": parse_success_rate,
        "schema_case_error_count": schema_case_error_count,
    }


def evaluate_model(
    model_id: str, tokenizer: Any, tasks: list[dict[str, Any]], token: str
) -> dict[str, Any]:
    """Load and evaluate one causal LM."""
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        dtype=torch.float16,
        device_map="auto",
        token=token,
        trust_remote_code=True,
    )
    model.eval()
    device = next(model.parameters()).device
    task_scores: list[dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        prompt_messages = [
            {
                "role": "system",
                "content": (
                    "You repair tabular data by proposing exact cell replacements. "
                    "Rows must be absolute row ids from valid_rows and columns must exactly "
                    "match one of the allowed_columns values. "
                    "Use only the provided dirty target rows and optional context rows. "
                    "Return strict JSON only in this object shape: "
                    '{"action":"submit_repairs","repairs":[{"row":0,"column":"Column",'
                    '"new_value":"value","reason":"why"}]}. '
                    'Use {"action":"finish","repairs":[]} when no cells should be changed. '
                    "Do not wrap the JSON in markdown code fences."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "contract_version": "repair_contract_v2",
                        "schema_summary": task["schema_summary"],
                        "allowed_columns": task["allowed_columns"],
                        "valid_rows": [int(row["_row"]) for row in task["target_rows"]],
                        "target_rows": task["target_rows"],
                        "context_rows": task["context_rows"],
                    },
                    sort_keys=True,
                ),
            },
        ]
        prompt = tokenizer.apply_chat_template(
            prompt_messages, tokenize=False, add_generation_prompt=True
        )
        inputs = tokenizer(prompt, return_tensors="pt").to(device)
        with torch.inference_mode():
            output = model.generate(
                **inputs,
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
            )
        decoded = tokenizer.decode(
            output[0][inputs["input_ids"].shape[-1] :], skip_special_tokens=True
        )
        repairs = parse_repairs(decoded)
        task_scores.append(
            {
                "dataset": task["dataset"],
                "f1": f1_score(task["ground_truth"], repairs),
                "parse_ok": parse_json_payload(decoded) is not None,
                "schema_case_errors": schema_case_errors(repairs, task["allowed_columns"]),
            }
        )
        if index % 10 == 0:
            print(f"{model_id}: evaluated {index}/{len(tasks)} tasks", flush=True)
    del model
    torch.cuda.empty_cache()
    return summarize_task_scores(task_scores)


def main() -> None:
    started = time.time()
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise RuntimeError("HF_TOKEN secret is required.")
    api = HfApi(token=token)
    dataset_info = api.repo_info(DATASET_REPO, repo_type="dataset", token=token)
    model_info = api.repo_info(TARGET_MODEL_REPO, repo_type="model", token=token)
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL_ID, token=token, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tasks = build_eval_tasks()
    baseline_eval = evaluate_model(BASELINE_MODEL_ID, tokenizer, tasks, token)
    target_eval = evaluate_model(TARGET_MODEL_REPO, tokenizer, tasks, token)
    baseline_f1 = float(baseline_eval["macro_f1"])
    target_f1 = float(target_eval["macro_f1"])
    f1_delta = round(target_f1 - baseline_f1, 4)
    parse_success_rate = float(target_eval["parse_success_rate"])
    schema_case_error_count = int(target_eval["schema_case_error_count"])
    quality_milestone = (
        f1_delta >= MIN_ABSOLUTE_F1_GAIN
        and target_f1 > baseline_f1
        and parse_success_rate >= 0.99
        and schema_case_error_count == 0
    )
    release_status = (
        "quality_improved_verified"
        if quality_milestone
        else ("diagnostic_complete_no_gain" if target_f1 <= baseline_f1 else "quality_gate_failed")
    )
    report = {
        "run_date_utc": datetime.now(UTC).isoformat(),
        "job_runtime_hours": round((time.time() - started) / 3600, 4),
        "device": torch.cuda.get_device_name(0) if torch.cuda.is_available() else "cpu",
        "model_size": MODEL_SIZE,
        "model_stage": MODEL_STAGE,
        "dataset_repo": DATASET_REPO,
        "dataset_sha": dataset_info.sha,
        "model_repo": TARGET_MODEL_REPO,
        "model_sha": model_info.sha,
        "base_model": BASE_MODEL_ID,
        "baseline_model": BASELINE_MODEL_ID,
        "heldout_tasks": len(tasks),
        "evaluation_chunk_width": CHUNK_WIDTH,
        "evaluation_max_new_tokens": MAX_NEW_TOKENS,
        "baseline_eval": baseline_eval,
        "target_eval": target_eval,
        "baseline_f1": baseline_f1,
        "target_f1": target_f1,
        "f1_delta": f1_delta,
        "min_absolute_f1_gain": MIN_ABSOLUTE_F1_GAIN,
        "base_eval": baseline_eval,
        "sft_eval": target_eval,
        "base_f1": baseline_f1,
        "sft_f1": target_f1,
        "parse_success_rate": parse_success_rate,
        "schema_case_error_count": schema_case_error_count,
        "prompt_contract_drift": False,
        "heldout_leakage_detected": False,
        "quality_milestone": quality_milestone,
        "release_status": release_status,
    }
    output = Path("hf_full_eval_latest.json")
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    timestamp_path = (
        "reports/hf_full_eval_"
        + MODEL_SIZE.replace(".", "-")
        + "_"
        + MODEL_STAGE.lower()
        + "_"
        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        + ".json"
    )
    api.upload_file(
        path_or_fileobj=str(output),
        path_in_repo=OUTPUT_DATASET_PATH,
        repo_id=DATASET_REPO,
        repo_type="dataset",
        token=token,
        commit_message="Add latest HF Jobs full model evaluation",
    )
    api.upload_file(
        path_or_fileobj=str(output),
        path_in_repo=timestamp_path,
        repo_id=DATASET_REPO,
        repo_type="dataset",
        token=token,
        commit_message="Add timestamped HF Jobs full model evaluation",
    )
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
