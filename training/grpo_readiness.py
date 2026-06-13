"""Readiness checks and prompt-dataset construction for DataForge GRPO."""

from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from dataforge.datasets.registry import DATASET_REGISTRY
from dataforge.repair_contract import CONTRACT_VERSION_V2, SYSTEM_PROMPT
from training.grpo_contract import score_grpo_completion

GRPO_BALANCED_RECALL_SYSTEM_PROMPT = (
    SYSTEM_PROMPT
    + "\nFor GRPO reward training, preserve clean no-op behavior, but do not return "
    + "empty repairs when the prompt evidence supports a valid dirty-cell repair. "
    + "Prefer precise repairs over abstention on truth-positive chunks."
)

DEFAULT_REQUIRED_DATASETS = ("hospital", "flights", "beers")
REPORT_SCHEMA_VERSION = "dataforge_grpo_readiness_report_v1"
HOSPITAL_SYNTHETIC_DATASET = "hospital_synthetic_deterministic_v1"
DEFAULT_AUXILIARY_DATASETS = (HOSPITAL_SYNTHETIC_DATASET,)


@dataclass(frozen=True, slots=True)
class GrpoReadinessSettings:
    """Thresholds for deciding whether GRPO rollout data is worth GPU time."""

    trajectory_filename: str = "expert_v4.jsonl"
    split_manifest_filename: str = "split_manifest_v4.json"
    prompt_contract_version: str = CONTRACT_VERSION_V2
    required_datasets: tuple[str, ...] = DEFAULT_REQUIRED_DATASETS
    auxiliary_datasets: tuple[str, ...] = DEFAULT_AUXILIARY_DATASETS
    min_records: int = 128
    min_records_per_dataset: int = 16
    min_repair_records: int = 32
    min_repair_signal_domains: int = 2
    min_dirty_records: int = 0
    min_dirty_records_per_dataset: int = 0
    min_clean_records: int = 32
    min_reward_std: float = 0.05
    min_per_dataset_reward_std: float = 0.01
    max_failure_samples: int = 25
    require_source_provenance: bool = False

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> GrpoReadinessSettings:
        """Build readiness thresholds from a GRPO YAML config mapping."""
        defaults = cls()
        readiness = config.get("readiness", {})
        if not isinstance(readiness, Mapping):
            readiness = {}
        required = readiness.get("required_datasets", DEFAULT_REQUIRED_DATASETS)
        if not isinstance(required, list | tuple):
            required = DEFAULT_REQUIRED_DATASETS
        auxiliary = readiness.get("auxiliary_datasets", DEFAULT_AUXILIARY_DATASETS)
        if not isinstance(auxiliary, list | tuple):
            auxiliary = DEFAULT_AUXILIARY_DATASETS
        return cls(
            trajectory_filename=str(
                readiness.get("trajectory_filename", defaults.trajectory_filename)
            ),
            split_manifest_filename=str(
                readiness.get("split_manifest_filename", defaults.split_manifest_filename)
            ),
            prompt_contract_version=str(
                readiness.get("prompt_contract_version", CONTRACT_VERSION_V2)
            ),
            required_datasets=tuple(str(dataset) for dataset in required),
            auxiliary_datasets=tuple(str(dataset) for dataset in auxiliary),
            min_records=int(readiness.get("min_records", defaults.min_records)),
            min_records_per_dataset=int(
                readiness.get("min_records_per_dataset", defaults.min_records_per_dataset)
            ),
            min_repair_records=int(
                readiness.get(
                    "min_repair_records",
                    readiness.get("min_dirty_records", defaults.min_repair_records),
                )
            ),
            min_repair_signal_domains=int(
                readiness.get(
                    "min_repair_signal_domains",
                    defaults.min_repair_signal_domains,
                )
            ),
            min_dirty_records=int(readiness.get("min_dirty_records", defaults.min_dirty_records)),
            min_dirty_records_per_dataset=int(
                readiness.get(
                    "min_dirty_records_per_dataset",
                    defaults.min_dirty_records_per_dataset,
                )
            ),
            min_clean_records=int(readiness.get("min_clean_records", defaults.min_clean_records)),
            min_reward_std=float(readiness.get("min_reward_std", defaults.min_reward_std)),
            min_per_dataset_reward_std=float(
                readiness.get(
                    "min_per_dataset_reward_std",
                    defaults.min_per_dataset_reward_std,
                )
            ),
            max_failure_samples=int(
                readiness.get("max_failure_samples", defaults.max_failure_samples)
            ),
            require_source_provenance=bool(
                readiness.get(
                    "require_source_provenance",
                    defaults.require_source_provenance,
                )
            ),
        )


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load a JSONL trajectory file as object records."""
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"{path}:{line_number} must contain a JSON object.")
        records.append(cast(dict[str, Any], payload))
    return records


def load_eval_rows(split_manifest_path: Path) -> dict[str, set[int]]:
    """Load held-out eval row ids from a split manifest."""
    payload = json.loads(split_manifest_path.read_text(encoding="utf-8"))
    return split_manifest_eval_rows(payload)


def split_manifest_eval_rows(payload: Mapping[str, Any]) -> dict[str, set[int]]:
    """Return held-out eval row ids from a split-manifest object."""
    eval_rows: dict[str, set[int]] = {}
    datasets = payload.get("datasets", []) if isinstance(payload, dict) else []
    if isinstance(datasets, list):
        for dataset_payload in datasets:
            if not isinstance(dataset_payload, dict):
                continue
            dataset = str(dataset_payload.get("dataset", ""))
            rows: set[int] = set()
            for row_payload in dataset_payload.get("eval", []):
                if isinstance(row_payload, dict) and "row" in row_payload:
                    rows.add(int(row_payload["row"]))
            if dataset:
                eval_rows[dataset] = rows
    return eval_rows


def split_manifest_source_provenance(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Return dataset-level source provenance from a split-manifest object."""
    source: dict[str, dict[str, Any]] = {}
    datasets = payload.get("datasets", []) if isinstance(payload, dict) else []
    if isinstance(datasets, list):
        for dataset_payload in datasets:
            if not isinstance(dataset_payload, dict):
                continue
            dataset = str(dataset_payload.get("dataset", ""))
            if dataset:
                source[dataset] = {
                    key: dataset_payload.get(key)
                    for key in (
                        "domain",
                        "base_dataset",
                        "collection_method",
                        "synthetic_policy",
                        "source_revision",
                        "dirty_sha256",
                        "clean_sha256",
                        "n_rows",
                        "n_columns",
                        "ground_truth_cells",
                        "train_rows",
                        "eval_rows",
                    )
                    if key in dataset_payload
                }
    return source


def load_split_manifest(path: Path) -> tuple[dict[str, set[int]], dict[str, dict[str, Any]]]:
    """Load split-manifest eval rows and dataset source provenance."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    manifest = cast(dict[str, Any], payload)
    return split_manifest_eval_rows(manifest), split_manifest_source_provenance(manifest)


def _message_with_role(messages: object, role: str) -> dict[str, Any]:
    """Return the first chat message with ``role``."""
    if not isinstance(messages, list):
        raise ValueError("record.messages must be a list.")
    for message in messages:
        if isinstance(message, dict) and message.get("role") == role:
            return cast(dict[str, Any], message)
    raise ValueError(f"record.messages is missing role={role!r}.")


def _user_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    """Parse the canonical JSON user payload from a trajectory record."""
    message = _message_with_role(record.get("messages"), "user")
    content = message.get("content")
    if not isinstance(content, str):
        raise ValueError("user message content must be a JSON string.")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("user message content must decode to a JSON object.")
    return cast(dict[str, Any], payload)


def _rows_from_payload(payload: Mapping[str, Any], field: str) -> set[int]:
    """Extract absolute ``_row`` ids from target/context rows."""
    rows: set[int] = set()
    for row_payload in payload.get(field, []):
        if isinstance(row_payload, Mapping) and "_row" in row_payload:
            rows.add(int(str(row_payload["_row"])))
    return rows


def _fixes(record: Mapping[str, Any]) -> list[dict[str, object]]:
    """Return normalized assistant repair dictionaries."""
    fixes: list[dict[str, object]] = []
    raw_fixes = record.get("fix", [])
    if not isinstance(raw_fixes, list):
        raise ValueError("record.fix must be a list.")
    for raw_fix in raw_fixes:
        if not isinstance(raw_fix, Mapping):
            raise ValueError("record.fix entries must be objects.")
        new_value = raw_fix.get("new_value", raw_fix.get("clean_value", raw_fix.get("expected")))
        fixes.append(
            {
                "row": int(raw_fix["row"]),
                "column": str(raw_fix["column"]),
                "new_value": "" if new_value is None else str(new_value),
                "reason": str(raw_fix.get("reason", "oracle repair")),
            }
        )
    return fixes


def _oracle_completion(fixes: list[dict[str, object]]) -> str:
    """Return strict JSON assistant output for oracle reward probes."""
    action = "submit_repairs" if fixes else "finish"
    return json.dumps({"action": action, "repairs": fixes}, sort_keys=True, separators=(",", ":"))


def _finish_completion() -> str:
    """Return strict JSON assistant output for no-op finish reward probes."""
    return '{"action":"finish","repairs":[]}'


def _adversarial_repair_completion(example: Mapping[str, Any]) -> str | None:
    """Return one valid-but-wrong repair to test reward contrast."""
    allowed_columns = example.get("allowed_columns")
    valid_rows = example.get("valid_rows")
    if not isinstance(allowed_columns, list) or not allowed_columns:
        return None
    if not isinstance(valid_rows, list) or not valid_rows:
        return None
    truth = example.get("ground_truth")
    if isinstance(truth, list) and truth:
        first_truth = truth[0]
        if not isinstance(first_truth, Mapping):
            return None
        row = int(first_truth["row"])
        column = str(first_truth["column"])
        correct = str(first_truth.get("new_value", first_truth.get("clean_value", "")))
        wrong_value = f"{correct}__wrong_probe__"
    else:
        row = int(valid_rows[0])
        column = str(allowed_columns[0])
        wrong_value = "__unjustified_repair_probe__"
    return json.dumps(
        {
            "action": "submit_repairs",
            "repairs": [
                {
                    "row": row,
                    "column": column,
                    "new_value": wrong_value,
                    "reason": "readiness adversarial reward probe",
                }
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def build_prompt_example(record: Mapping[str, Any]) -> dict[str, Any]:
    """Build one leak-checkable GRPO prompt example from an expert-v4 record."""
    if record.get("schema_version") != "expert_v4":
        raise ValueError("GRPO readiness requires schema_version=expert_v4.")
    if record.get("prompt_contract_version") != CONTRACT_VERSION_V2:
        raise ValueError("GRPO readiness requires prompt_contract_version=repair_contract_v2.")
    payload = _user_payload(record)
    if payload.get("contract_version") != CONTRACT_VERSION_V2:
        raise ValueError("GRPO prompt payload must use repair_contract_v2.")
    dataset = str(record.get("dataset", payload.get("schema_summary", {}).get("dataset", "")))
    if not dataset:
        raise ValueError("trajectory record is missing dataset.")
    inferability = str(record.get("inferability", ""))
    schema_summary = payload.get("schema_summary", {})
    base_dataset = dataset
    if isinstance(schema_summary, Mapping):
        base_dataset = str(schema_summary.get("base_dataset", dataset))
    allowed_columns = payload.get("allowed_columns")
    valid_rows = payload.get("valid_rows")
    if not isinstance(allowed_columns, list) or not allowed_columns:
        raise ValueError("GRPO prompt payload must include allowed_columns.")
    if not isinstance(valid_rows, list) or not valid_rows:
        raise ValueError("GRPO prompt payload must include valid_rows.")
    fixes = _fixes(record)
    prompt = [
        {"role": "system", "content": GRPO_BALANCED_RECALL_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        },
    ]
    return {
        "prompt": prompt,
        "ground_truth": fixes,
        "allowed_columns": [str(column) for column in allowed_columns],
        "valid_rows": [int(row) for row in valid_rows],
        "dataset": dataset,
        "base_dataset": base_dataset,
        "inferability": inferability,
        "trajectory_id": str(record.get("trajectory_id", "")),
        "dirty": bool(fixes),
        "target_rows": sorted(_rows_from_payload(payload, "target_rows")),
        "context_rows": sorted(_rows_from_payload(payload, "context_rows")),
        "prompt_contract_version": CONTRACT_VERSION_V2,
    }


def _balanced_examples(examples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return deterministic balanced examples across dataset and clean/dirty slices."""
    groups: dict[tuple[str, bool], list[dict[str, Any]]] = defaultdict(list)
    for example in examples:
        groups[(str(example["dataset"]), bool(example["dirty"]))].append(example)
    non_empty_groups = [group for group in groups.values() if group]
    if not non_empty_groups:
        return []
    cap = min(len(group) for group in non_empty_groups)
    balanced: list[dict[str, Any]] = []
    for key in sorted(groups):
        balanced.extend(groups[key][:cap])
    return balanced


def _reward_probe_metrics(examples: list[dict[str, Any]]) -> dict[str, Any]:
    """Return reward-diversity diagnostics using oracle, finish, and wrong probes."""
    rewards: list[float] = []
    deltas: list[float] = []
    parse_failures = 0
    schema_case_errors = 0
    adversarial_probe_count = 0
    for example in examples:
        oracle_reward, oracle_diag = score_grpo_completion(
            _oracle_completion(cast(list[dict[str, object]], example["ground_truth"])),
            raw_truth=example["ground_truth"],
            raw_allowed_columns=example["allowed_columns"],
            raw_valid_rows=example["valid_rows"],
            raw_inferability=example.get("inferability"),
        )
        finish_reward, finish_diag = score_grpo_completion(
            _finish_completion(),
            raw_truth=example["ground_truth"],
            raw_allowed_columns=example["allowed_columns"],
            raw_valid_rows=example["valid_rows"],
            raw_inferability=example.get("inferability"),
        )
        rewards.extend([oracle_reward, finish_reward])
        adversarial_completion = _adversarial_repair_completion(example)
        if adversarial_completion is not None:
            adversarial_reward, _ = score_grpo_completion(
                adversarial_completion,
                raw_truth=example["ground_truth"],
                raw_allowed_columns=example["allowed_columns"],
                raw_valid_rows=example["valid_rows"],
                raw_inferability=example.get("inferability"),
            )
            rewards.append(adversarial_reward)
            adversarial_probe_count += 1
        deltas.append(round(oracle_reward - finish_reward, 6))
        if not oracle_diag.get("parse_ok", False):
            parse_failures += 1
        schema_case_errors += int(
            oracle_diag.get("failure_taxonomy", {}).get("schema_case_error", 0)
        )
        schema_case_errors += int(
            finish_diag.get("failure_taxonomy", {}).get("schema_case_error", 0)
        )
    reward_std = statistics.pstdev(rewards) if len(rewards) > 1 else 0.0
    return {
        "reward_count": len(rewards),
        "reward_std": round(float(reward_std), 6),
        "reward_unique_values": sorted({round(float(reward), 6) for reward in rewards}),
        "mean_oracle_minus_finish": round(statistics.fmean(deltas), 6) if deltas else 0.0,
        "adversarial_probe_count": adversarial_probe_count,
        "oracle_parse_failures": parse_failures,
        "schema_case_error_count": schema_case_errors,
    }


def analyze_grpo_readiness(
    records: Iterable[Mapping[str, Any]],
    *,
    split_eval_rows: Mapping[str, set[int]] | None = None,
    split_source_provenance: Mapping[str, Mapping[str, Any]] | None = None,
    settings: GrpoReadinessSettings | None = None,
) -> dict[str, Any]:
    """Build a JSON-ready GRPO readiness report without making public claims."""
    resolved_settings = settings or GrpoReadinessSettings()
    records_list = list(records)
    examples: list[dict[str, Any]] = []
    errors: list[dict[str, object]] = []
    leakage_count = 0
    eval_rows = split_eval_rows or {}

    for index, record in enumerate(records_list):
        try:
            example = build_prompt_example(record)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            if len(errors) < resolved_settings.max_failure_samples:
                errors.append({"index": index, "error": str(exc)})
            continue
        dataset = str(example["dataset"])
        base_dataset = str(example.get("base_dataset", dataset))
        heldout_rows = set(eval_rows.get(dataset, set())) | set(eval_rows.get(base_dataset, set()))
        leaked_rows = (set(example["target_rows"]) | set(example["context_rows"])) & heldout_rows
        if leaked_rows:
            leakage_count += 1
            if len(errors) < resolved_settings.max_failure_samples:
                errors.append(
                    {
                        "index": index,
                        "dataset": dataset,
                        "error": "held-out row leakage",
                        "rows": sorted(leaked_rows),
                    }
                )
            continue
        examples.append(example)

    counts_by_dataset = Counter(str(example["dataset"]) for example in examples)
    counts_by_base_dataset = Counter(
        str(example.get("base_dataset", example["dataset"])) for example in examples
    )
    dirty_by_dataset = Counter(
        str(example["dataset"]) for example in examples if bool(example["dirty"])
    )
    dirty_by_base_dataset = Counter(
        str(example.get("base_dataset", example["dataset"]))
        for example in examples
        if bool(example["dirty"])
    )
    clean_by_dataset = Counter(
        str(example["dataset"]) for example in examples if not bool(example["dirty"])
    )
    clean_by_base_dataset = Counter(
        str(example.get("base_dataset", example["dataset"]))
        for example in examples
        if not bool(example["dirty"])
    )
    dirty_records = sum(dirty_by_dataset.values())
    clean_records = sum(clean_by_dataset.values())
    repair_signal_domains = sorted(
        dataset for dataset, count in dirty_by_base_dataset.items() if count > 0
    )
    balanced = _balanced_examples(examples)
    reward_metrics = _reward_probe_metrics(examples)
    examples_by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    examples_by_base_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    inferability_by_dataset: dict[str, Counter[str]] = defaultdict(Counter)
    for example in examples:
        dataset = str(example["dataset"])
        examples_by_dataset[dataset].append(example)
        examples_by_base_dataset[str(example.get("base_dataset", dataset))].append(example)
    for record in records_list:
        dataset = str(record.get("dataset", ""))
        inferability = record.get("inferability")
        if dataset and isinstance(inferability, str):
            inferability_by_dataset[dataset][inferability] += 1
    per_dataset: dict[str, dict[str, Any]] = {}
    for dataset in sorted(set(counts_by_dataset) | set(resolved_settings.required_datasets)):
        dataset_examples = examples_by_dataset.get(dataset, [])
        per_dataset[dataset] = {
            "records": counts_by_dataset[dataset],
            "dirty_records": dirty_by_dataset[dataset],
            "clean_records": clean_by_dataset[dataset],
            "inferability_counts": dict(sorted(inferability_by_dataset[dataset].items())),
            "reward_probe": _reward_probe_metrics(dataset_examples) if dataset_examples else None,
            "source_provenance": dict(split_source_provenance.get(dataset, {}))
            if split_source_provenance
            else {},
        }
    per_base_dataset: dict[str, dict[str, Any]] = {}
    for dataset in sorted(
        set(counts_by_base_dataset)
        | set(resolved_settings.required_datasets)
        | set(resolved_settings.auxiliary_datasets)
    ):
        dataset_examples = examples_by_base_dataset.get(dataset, [])
        per_base_dataset[dataset] = {
            "records": counts_by_base_dataset[dataset],
            "dirty_records": dirty_by_base_dataset[dataset],
            "clean_records": clean_by_base_dataset[dataset],
            "reward_probe": _reward_probe_metrics(dataset_examples) if dataset_examples else None,
        }

    blockers: list[str] = []
    if errors:
        blockers.append("invalid_records")
    if leakage_count:
        blockers.append("heldout_leakage")
    if len(examples) < resolved_settings.min_records:
        blockers.append("too_few_records")
    for dataset in resolved_settings.required_datasets:
        if counts_by_dataset[dataset] < resolved_settings.min_records_per_dataset:
            blockers.append(f"dataset_{dataset}_under_min_records")
        if (
            resolved_settings.min_dirty_records_per_dataset
            and dirty_by_base_dataset[dataset] < resolved_settings.min_dirty_records_per_dataset
        ):
            blockers.append(f"dataset_{dataset}_under_min_dirty_records")
    if resolved_settings.min_dirty_records and dirty_records < resolved_settings.min_dirty_records:
        blockers.append("too_few_dirty_records")
    if dirty_records < resolved_settings.min_repair_records:
        blockers.append("too_few_repair_records")
    if len(repair_signal_domains) < resolved_settings.min_repair_signal_domains:
        blockers.append("too_few_repair_signal_domains")
    if clean_records < resolved_settings.min_clean_records:
        blockers.append("too_few_clean_records")
    if reward_metrics["reward_std"] < resolved_settings.min_reward_std:
        blockers.append("reward_variance_too_low")
    for dataset in resolved_settings.required_datasets:
        dataset_reward_probe = per_dataset.get(dataset, {}).get("reward_probe")
        dataset_reward_std = (
            float(dataset_reward_probe["reward_std"])
            if isinstance(dataset_reward_probe, dict)
            else 0.0
        )
        if dataset_reward_std < resolved_settings.min_per_dataset_reward_std:
            blockers.append(f"dataset_{dataset}_reward_variance_too_low")
    if reward_metrics["oracle_parse_failures"]:
        blockers.append("oracle_contract_parse_failures")
    if reward_metrics["schema_case_error_count"]:
        blockers.append("schema_case_errors_in_reward_probe")
    if resolved_settings.require_source_provenance:
        source = split_source_provenance or {}
        for dataset in resolved_settings.required_datasets:
            expected = DATASET_REGISTRY[dataset]
            actual = source.get(dataset)
            if not actual:
                blockers.append(f"dataset_{dataset}_source_provenance_missing")
                continue
            expected_values = {
                "source_revision": expected.source_revision,
                "dirty_sha256": expected.dirty_sha256,
                "clean_sha256": expected.clean_sha256,
                "n_rows": expected.n_rows,
                "n_columns": expected.n_columns,
            }
            for field, expected_value in expected_values.items():
                if actual.get(field) != expected_value:
                    blockers.append(f"dataset_{dataset}_{field}_mismatch")

    status = "pass" if not blockers else "block"
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "ok": status == "pass",
        "settings": asdict(resolved_settings),
        "metrics": {
            "records_seen": len(records_list),
            "valid_prompt_records": len(examples),
            "balanced_prompt_records": len(balanced),
            "dirty_records": dirty_records,
            "repair_records": dirty_records,
            "clean_records": clean_records,
            "leakage_count": leakage_count,
            "repair_signal_domains": repair_signal_domains,
            "counts_by_dataset": dict(sorted(counts_by_dataset.items())),
            "counts_by_base_dataset": dict(sorted(counts_by_base_dataset.items())),
            "dirty_by_dataset": dict(sorted(dirty_by_dataset.items())),
            "dirty_by_base_dataset": dict(sorted(dirty_by_base_dataset.items())),
            "clean_by_dataset": dict(sorted(clean_by_dataset.items())),
            "clean_by_base_dataset": dict(sorted(clean_by_base_dataset.items())),
            "per_dataset": per_dataset,
            "per_base_dataset": per_base_dataset,
            "reward_probe": reward_metrics,
        },
        "blockers": sorted(set(blockers)),
        "failure_samples": errors[: resolved_settings.max_failure_samples],
        "prompt_dataset_preview": [
            {
                "dataset": example["dataset"],
                "trajectory_id": example["trajectory_id"],
                "dirty": example["dirty"],
            }
            for example in balanced[: min(10, len(balanced))]
        ],
        "limitations": [
            "Readiness is local diagnostic evidence only; it does not create a model-quality claim.",
            "Tokenizer-specific prompt-token enforcement still runs inside the Kaggle notebook.",
        ],
    }


def analyze_grpo_readiness_paths(
    *,
    trajectory_path: Path,
    split_manifest_path: Path | None = None,
    settings: GrpoReadinessSettings | None = None,
) -> dict[str, Any]:
    """Load trajectory files and return a GRPO readiness report."""
    records = load_jsonl(trajectory_path)
    split_eval_rows: dict[str, set[int]] | None = None
    split_source_provenance: dict[str, dict[str, Any]] | None = None
    if split_manifest_path:
        split_eval_rows, split_source_provenance = load_split_manifest(split_manifest_path)
    report = analyze_grpo_readiness(
        records,
        split_eval_rows=split_eval_rows,
        split_source_provenance=split_source_provenance,
        settings=settings,
    )
    report["artifacts"] = {
        "trajectory_path": str(trajectory_path),
        "split_manifest_path": str(split_manifest_path) if split_manifest_path else None,
    }
    return report
