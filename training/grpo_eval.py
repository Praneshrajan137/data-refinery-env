"""Strict held-out GRPO evaluation helpers for DataForge release gates."""

from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dataforge.datasets.real_world import RealWorldDataset, load_real_world_dataset
from dataforge.evaluation_contract import prompt_sha256
from dataforge.repair_contract import (
    CONTRACT_VERSION_V2,
    RepairFix,
    parse_repair_action,
    render_repair_messages,
    repair_failure_taxonomy,
    score_repair_fixes,
    score_repair_fixes_canonicalized,
)
from training.grpo_contract import TruthCell

BENCHMARK_NAME = "DataForge-Bench-light-verified"
TASK_MANIFEST_SCHEMA = "dataforge_grpo_eval_task_manifest_v1"
EVAL_DIAGNOSTICS_SCHEMA = "dataforge_grpo_eval_diagnostics_v1"
DEFAULT_DATASETS = ("hospital", "flights", "beers")
DEFAULT_BENCHMARK_SEEDS = (0, 1, 2)
DEFAULT_SEEDS_START = 10000
DEFAULT_HELDOUT_TASKS = 100
DEFAULT_CHUNK_WIDTH = 4
DEFAULT_MAX_NEW_TOKENS = 1024
MAX_FAILURE_SAMPLES = 25


@dataclass(frozen=True, slots=True)
class GrpoEvalTask:
    """One hidden-label repair task used for strict GRPO release evaluation."""

    task_id: str
    dataset: str
    seed: int
    prompt: dict[str, Any]
    messages: list[dict[str, str]]
    allowed_columns: list[str]
    valid_rows: list[int]
    target_rows: list[dict[str, str]]
    context_rows: list[dict[str, str]]
    ground_truth: list[TruthCell]
    inferability: str
    source: dict[str, Any]


def _rows_from_window(
    dataset: RealWorldDataset,
    *,
    start: int,
    width: int,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row_idx in range(start, start + width):
        row = {"_row": str(row_idx)}
        for column in dataset.canonical_columns:
            row[column] = str(dataset.dirty_df.iloc[row_idx][column])
        rows.append(row)
    return rows


def _chunk_rows(
    dataset: RealWorldDataset,
    *,
    seed: int,
    width: int,
    prefer_truth: bool,
) -> list[dict[str, str]]:
    width = min(width, len(dataset.dirty_df.index))
    if width <= 0:
        return []
    digest = hashlib.sha256(str(seed).encode("utf-8")).hexdigest()
    truth_rows = {cell.row for cell in dataset.ground_truth}
    max_start = max(0, len(dataset.dirty_df.index) - width)
    if dataset.ground_truth and prefer_truth:
        anchor = dataset.ground_truth[int(digest[:8], 16) % len(dataset.ground_truth)].row
        start = min(max(anchor - width // 2, 0), max_start)
    else:
        clean_starts = [
            start
            for start in range(max_start + 1)
            if not truth_rows.intersection(range(start, start + width))
        ]
        candidates = clean_starts if clean_starts else list(range(max_start + 1))
        start = candidates[int(digest[:8], 16) % len(candidates)]
    return _rows_from_window(dataset, start=start, width=width)


def _truth_for_rows(dataset: RealWorldDataset, row_ids: set[int]) -> list[TruthCell]:
    return [
        TruthCell(row=cell.row, column=cell.column, clean_value=cell.clean_value)
        for cell in dataset.ground_truth
        if cell.row in row_ids
    ]


def _deterministic_candidate(dataset_name: str, column: str, raw_value: str) -> str | None:
    value = str(raw_value).strip()
    if dataset_name == "beers":
        if column == "ounces":
            import re

            match = re.search(r"[-+]?\d+(?:\.\d+)?", value)
            if match and re.search(r"\boz\.?\b|\bounce\b", value, re.IGNORECASE):
                number = float(match.group(0))
                return str(int(number)) if number.is_integer() else str(number).rstrip("0").rstrip(".")
        if column == "abv" and value.endswith("%"):
            return value.rstrip("%").strip()
        if column in {"ibu", "abv"} and value.upper() in {"N/A", "NA", "NULL", "NONE"}:
            return ""
    if dataset_name == "flights" and column in {
        "sched_dep_time",
        "act_dep_time",
        "sched_arr_time",
        "act_arr_time",
    }:
        import re

        if not value or "(" in value or ")" in value:
            return None
        without_status = re.sub(
            r"\s+(?:on\s+time|delayed|cancelled|canceled|arrived|departed|early|late)\b.*$",
            "",
            value,
            flags=re.IGNORECASE,
        ).strip()
        if without_status != value and re.fullmatch(
            r"\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.)",
            without_status,
            flags=re.IGNORECASE,
        ):
            return without_status
        if re.search(
            r"\b(?:mon|tue|wed|thu|fri|sat|sun|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b"
            r"|\d{1,2}[-/][A-Za-z]{3}[-/]\d{2,4}"
            r"|[A-Za-z]{3}\s+\d{1,2},?\s+\d{2,4}",
            value,
            flags=re.IGNORECASE,
        ):
            matches = re.findall(r"\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.)", value, flags=re.IGNORECASE)
            if len(matches) == 1:
                return str(matches[0]).strip()
    return None


def _inferability(dataset_name: str, rows: Sequence[dict[str, str]], truth: Sequence[TruthCell]) -> str:
    if not truth:
        return "not_inferable_from_prompt"
    rows_by_id = {int(row["_row"]): row for row in rows}
    for cell in truth:
        row = rows_by_id.get(cell.row)
        if row is None:
            return "external_reference_required"
        if _deterministic_candidate(dataset_name, cell.column, row.get(cell.column, "")) != cell.clean_value:
            return "external_reference_required"
    return "deterministic_normalization"


def _source_audit_row(dataset: RealWorldDataset) -> dict[str, Any]:
    metadata = dataset.metadata
    status = "pass"
    errors: list[str] = []
    if dataset.dirty_sha256 != metadata.dirty_sha256:
        status = "block"
        errors.append("dirty_sha256_mismatch")
    if dataset.clean_sha256 != metadata.clean_sha256:
        status = "block"
        errors.append("clean_sha256_mismatch")
    if len(dataset.dirty_df.index) != metadata.n_rows:
        status = "block"
        errors.append("row_count_mismatch")
    if len(dataset.canonical_columns) != metadata.n_columns:
        status = "block"
        errors.append("column_count_mismatch")
    return {
        "dataset": metadata.name,
        "status": status,
        "errors": errors,
        "source_revision": metadata.source_revision,
        "dirty_sha256": dataset.dirty_sha256,
        "clean_sha256": dataset.clean_sha256,
        "rows": len(dataset.dirty_df.index),
        "columns": len(dataset.canonical_columns),
        "ground_truth_cells": len(dataset.ground_truth),
    }


def build_heldout_tasks(
    *,
    datasets: Sequence[str] = DEFAULT_DATASETS,
    heldout_tasks: int = DEFAULT_HELDOUT_TASKS,
    benchmark_seeds: Sequence[int] = DEFAULT_BENCHMARK_SEEDS,
    seeds_start: int = DEFAULT_SEEDS_START,
    chunk_width: int = DEFAULT_CHUNK_WIDTH,
    cache_root: Path | None = None,
    contract_version: str = CONTRACT_VERSION_V2,
) -> tuple[list[GrpoEvalTask], dict[str, Any]]:
    """Build deterministic strict held-out tasks from pinned source bytes."""
    if heldout_tasks < 1:
        raise ValueError("heldout_tasks must be positive.")
    if not benchmark_seeds:
        raise ValueError("benchmark_seeds must not be empty.")
    loaded = {
        name: load_real_world_dataset(
            name,
            cache_root=cache_root,
            verify_hashes=True,
            allow_embedded_fallback=False,
        )
        for name in datasets
    }
    source_audit = [_source_audit_row(dataset) for dataset in loaded.values()]
    blockers = [row["dataset"] for row in source_audit if row["status"] != "pass"]
    if blockers:
        raise ValueError("Held-out source audit blocked: " + ", ".join(blockers))

    tasks: list[GrpoEvalTask] = []
    attempts = 0
    max_attempts = max(heldout_tasks * 20, 20)
    while len(tasks) < heldout_tasks and attempts < max_attempts:
        for dataset_name, dataset in loaded.items():
            seed = seeds_start + attempts * len(benchmark_seeds) + int(
                benchmark_seeds[attempts % len(benchmark_seeds)]
            )
            rows = _chunk_rows(
                dataset,
                seed=seed,
                width=chunk_width,
                prefer_truth=attempts % 5 != 4,
            )
            attempts += 1
            row_ids = {int(row["_row"]) for row in rows}
            truth = _truth_for_rows(dataset, row_ids)
            valid_rows = sorted(row_ids)
            schema_summary = {
                "dataset": dataset_name,
                "columns": list(dataset.canonical_columns),
                "source_revision": dataset.metadata.source_revision,
            }
            messages = render_repair_messages(
                schema_summary=schema_summary,
                target_rows=rows,
                context_rows=[],
                allowed_columns=dataset.canonical_columns,
                valid_rows=valid_rows,
                contract_version=contract_version,
            )
            user_payload = json.loads(messages[1]["content"])
            task_id = f"{BENCHMARK_NAME}:{dataset_name}:{seed}:{len(tasks):04d}"
            tasks.append(
                GrpoEvalTask(
                    task_id=task_id,
                    dataset=dataset_name,
                    seed=seed,
                    prompt=user_payload,
                    messages=messages,
                    allowed_columns=list(dataset.canonical_columns),
                    valid_rows=valid_rows,
                    target_rows=rows,
                    context_rows=[],
                    ground_truth=truth,
                    inferability=_inferability(dataset_name, rows, truth),
                    source={
                        "source_revision": dataset.metadata.source_revision,
                        "dirty_sha256": dataset.dirty_sha256,
                        "clean_sha256": dataset.clean_sha256,
                    },
                )
            )
            if len(tasks) >= heldout_tasks or attempts >= max_attempts:
                break
    if len(tasks) < heldout_tasks:
        raise RuntimeError(f"Could only build {len(tasks)} held-out tasks; requested {heldout_tasks}.")
    manifest = render_eval_task_manifest(
        tasks,
        benchmark_seeds=benchmark_seeds,
        source_audit=source_audit,
        chunk_width=chunk_width,
    )
    return tasks, manifest


def render_eval_task_manifest(
    tasks: Sequence[GrpoEvalTask],
    *,
    benchmark_seeds: Sequence[int],
    source_audit: Sequence[dict[str, Any]],
    chunk_width: int,
    max_new_tokens: int = DEFAULT_MAX_NEW_TOKENS,
) -> dict[str, Any]:
    """Render a public task manifest without exposing full hidden labels."""
    return {
        "schema_version": TASK_MANIFEST_SCHEMA,
        "benchmark_name": BENCHMARK_NAME,
        "benchmark_seeds": [int(seed) for seed in benchmark_seeds],
        "heldout_tasks": len(tasks),
        "chunk_width": int(chunk_width),
        "max_new_tokens": int(max_new_tokens),
        "source_audit": {
            "schema_version": "dataforge_real_world_source_audit_v1",
            "status": "pass",
            "ok": True,
            "datasets": list(source_audit),
            "blockers": [],
        },
        "tasks": [
            {
                "task_id": task.task_id,
                "dataset": task.dataset,
                "seed": task.seed,
                "prompt_hash": prompt_sha256(task.prompt),
                "allowed_columns": task.allowed_columns,
                "valid_rows": task.valid_rows,
                "truth_cell_count": len(task.ground_truth),
                "truth_hash": hashlib.sha256(
                    json.dumps(
                        [
                            {
                                "row": cell.row,
                                "column": cell.column,
                                "clean_value": cell.clean_value,
                            }
                            for cell in task.ground_truth
                        ],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
                "inferability": task.inferability,
                "source": task.source,
            }
            for task in tasks
        ],
    }


def _score_completion(task: GrpoEvalTask, completion: str) -> dict[str, Any]:
    parse_result = parse_repair_action(
        completion,
        allowed_columns=task.allowed_columns,
        valid_rows=task.valid_rows,
        require_explicit_action=True,
    )
    repairs: list[RepairFix] = []
    taxonomy: dict[str, int] = {}
    if parse_result.ok and parse_result.action is not None:
        repairs = parse_result.action.repairs
        if not task.ground_truth and not repairs:
            score = {"tp": 0, "fp": 0, "fn": 0, "precision": 1.0, "recall": 1.0, "f1": 1.0}
            canonicalized_score = score
        else:
            strict_score = score_repair_fixes(task.ground_truth, repairs)
            fuzzy_score = score_repair_fixes_canonicalized(task.ground_truth, repairs)
            score = strict_score.model_dump(mode="json")
            canonicalized_score = fuzzy_score.model_dump(mode="json")
        taxonomy = repair_failure_taxonomy(
            ground_truth=task.ground_truth,
            fixes=repairs,
            allowed_columns=task.allowed_columns,
            valid_rows=task.valid_rows,
        )
    else:
        score = {"tp": 0, "fp": 0, "fn": len(task.ground_truth), "precision": 0.0, "recall": 0.0, "f1": 0.0}
        canonicalized_score = dict(score)
        relaxed = parse_repair_action(completion, require_explicit_action=False)
        if relaxed.ok and relaxed.action is not None:
            repairs = relaxed.action.repairs
        taxonomy = repair_failure_taxonomy(
            ground_truth=task.ground_truth,
            fixes=repairs,
            allowed_columns=task.allowed_columns,
            valid_rows=task.valid_rows,
        )
        if parse_result.error_kind == "invalid_column" and parse_result.diagnostics.get("schema_case_error"):
            taxonomy["schema_case_error"] = max(1, taxonomy.get("schema_case_error", 0))
    return {
        "task_id": task.task_id,
        "dataset": task.dataset,
        "inferability": task.inferability,
        "f1": float(score["f1"]),
        "canonicalized_f1": float(canonicalized_score["f1"]),
        "precision": float(score["precision"]),
        "recall": float(score["recall"]),
        "tp": int(score["tp"]),
        "fp": int(score["fp"]),
        "fn": int(score["fn"]),
        "parse_ok": parse_result.ok,
        "parse_error_kind": parse_result.error_kind,
        "schema_case_errors": int(taxonomy.get("schema_case_error", 0)),
        "failure_taxonomy": taxonomy,
        "predicted_repairs": [repair.model_dump(mode="json") for repair in repairs[:20]],
        "decoded_preview": completion[:1500],
    }


def summarize_task_scores(task_scores: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate strict GRPO eval rows into release-gate metrics."""
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    by_slice: dict[str, list[dict[str, Any]]] = {}
    failures: Counter[str] = Counter()
    for row in task_scores:
        by_dataset.setdefault(str(row["dataset"]), []).append(row)
        by_slice.setdefault(str(row["inferability"]), []).append(row)
        failures.update(row.get("failure_taxonomy", {}))
        if not row["parse_ok"]:
            failures[str(row.get("parse_error_kind") or "parse_failure")] += 1
    dataset_f1 = {
        dataset: round(sum(float(row["f1"]) for row in rows) / len(rows), 4)
        for dataset, rows in sorted(by_dataset.items())
        if rows
    }
    canonicalized_dataset_f1 = {
        dataset: round(sum(float(row["canonicalized_f1"]) for row in rows) / len(rows), 4)
        for dataset, rows in sorted(by_dataset.items())
        if rows
    }
    slice_scores = {}
    for label, rows in sorted(by_slice.items()):
        slice_scores[label] = {
            "tasks": len(rows),
            "macro_f1": round(sum(float(row["f1"]) for row in rows) / len(rows), 4),
            "parse_success_rate": round(sum(1 for row in rows if row["parse_ok"]) / len(rows), 4),
            "schema_case_error_count": sum(int(row["schema_case_errors"]) for row in rows),
        }
    return {
        "macro_f1": round(sum(dataset_f1.values()) / len(dataset_f1), 4) if dataset_f1 else 0.0,
        "canonicalized_macro_f1": round(sum(canonicalized_dataset_f1.values()) / len(canonicalized_dataset_f1), 4)
        if canonicalized_dataset_f1
        else 0.0,
        "mean_f1": round(sum(float(row["f1"]) for row in task_scores) / len(task_scores), 4)
        if task_scores
        else 0.0,
        "dataset_f1": dataset_f1,
        "parse_success_rate": round(sum(1 for row in task_scores if row["parse_ok"]) / len(task_scores), 4)
        if task_scores
        else 0.0,
        "schema_case_error_count": sum(int(row["schema_case_errors"]) for row in task_scores),
        "failure_taxonomy": {kind: failures[kind] for kind in sorted(failures)},
        "slice_scores": slice_scores,
        "tasks": len(task_scores),
    }


def evaluate_completions(
    tasks: Sequence[GrpoEvalTask],
    completion_fn: Callable[[GrpoEvalTask], str],
    *,
    model_label: str,
    max_failure_samples: int = MAX_FAILURE_SAMPLES,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Evaluate caller-provided completions against strict held-out tasks."""
    task_scores: list[dict[str, Any]] = []
    failure_samples: list[dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        row = _score_completion(task, completion_fn(task))
        row["task_index"] = index
        task_scores.append(row)
        if (
            row["f1"] < 1.0 or row["failure_taxonomy"] or not row["parse_ok"]
        ) and len(failure_samples) < max_failure_samples:
            failure_samples.append(
                {
                    **row,
                    "target_rows": task.target_rows,
                    "ground_truth_count": len(task.ground_truth),
                }
            )
    summary = summarize_task_scores(task_scores)
    diagnostics = {
        "model_label": model_label,
        "task_scores": task_scores,
        "failure_samples": failure_samples,
    }
    return summary, diagnostics


def evaluate_causal_lm(
    model: Any,
    tokenizer: Any,
    tasks: Sequence[GrpoEvalTask],
    *,
    model_label: str,
    max_new_tokens: int = DEFAULT_MAX_NEW_TOKENS,
    max_failure_samples: int = MAX_FAILURE_SAMPLES,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Evaluate one causal LM with greedy repair-contract decoding."""
    import torch

    model.eval()
    device = next(model.parameters()).device

    def complete(task: GrpoEvalTask) -> str:
        prompt = tokenizer.apply_chat_template(
            task.messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        inputs = tokenizer(prompt, return_tensors="pt").to(device)
        with torch.inference_mode():
            output = model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
            )
        return tokenizer.decode(output[0][inputs["input_ids"].shape[-1] :], skip_special_tokens=True)

    started = time.time()
    summary, diagnostics = evaluate_completions(
        tasks,
        complete,
        model_label=model_label,
        max_failure_samples=max_failure_samples,
    )
    diagnostics["runtime_seconds"] = round(time.time() - started, 3)
    return summary, diagnostics


def grpo_gate_failures(
    *,
    sft_eval: dict[str, Any],
    grpo_eval: dict[str, Any],
    min_absolute_f1_gain: float = 0.03,
    min_parse_success_rate: float = 0.99,
    required_schema_case_error_count: int = 0,
    target_strict_macro_f1: float | None = None,
    min_not_inferable_slice_f1: float | None = None,
    min_deterministic_normalization_slice_f1: float | None = None,
) -> list[str]:
    """Return release-gate failures for one SFT-vs-GRPO eval pair."""
    failures: list[str] = []
    if float(grpo_eval.get("macro_f1", 0.0)) - float(sft_eval.get("macro_f1", 0.0)) < min_absolute_f1_gain:
        failures.append("grpo_f1-sft_f1>=0.03")
    if target_strict_macro_f1 is not None and float(grpo_eval.get("macro_f1", 0.0)) < target_strict_macro_f1:
        failures.append(f"grpo_f1>={target_strict_macro_f1:g}")
    if float(grpo_eval.get("parse_success_rate", 0.0)) < min_parse_success_rate:
        failures.append("parse_success_rate>=0.99")
    if int(grpo_eval.get("schema_case_error_count", -1)) != required_schema_case_error_count:
        failures.append("schema_case_error_count==0")
    if min_not_inferable_slice_f1 is not None:
        slice_scores = grpo_eval.get("slice_scores", {})
        not_inferable = slice_scores.get("not_inferable_from_prompt", {}) if isinstance(slice_scores, dict) else {}
        if float(not_inferable.get("macro_f1", 0.0)) < min_not_inferable_slice_f1:
            failures.append(f"not_inferable_from_prompt_f1>={min_not_inferable_slice_f1:g}")
    if min_deterministic_normalization_slice_f1 is not None:
        slice_scores = grpo_eval.get("slice_scores", {})
        deterministic = (
            slice_scores.get("deterministic_normalization", {})
            if isinstance(slice_scores, dict)
            else {}
        )
        if float(deterministic.get("macro_f1", 0.0)) < min_deterministic_normalization_slice_f1:
            failures.append(
                f"deterministic_normalization_f1>={min_deterministic_normalization_slice_f1:g}"
            )
    return failures
