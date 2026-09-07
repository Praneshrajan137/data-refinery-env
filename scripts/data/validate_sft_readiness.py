"""Validate that Week 9 SFT artifacts are ready before launching Kaggle."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import yaml
from rich.console import Console
from rich.table import Table

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.evaluation_contract import (  # noqa: E402
    ABSTENTION_SLICES,
    AUXILIARY_SLICES,
    PROMOTION_SLICE,
    InferabilityLabel,
)
from dataforge.repair_contract import (  # noqa: E402
    CONTRACT_VERSION,
    CONTRACT_VERSION_V1,
    CONTRACT_VERSION_V2,
    CONTRACT_VERSION_V3,
    parse_repair_action,
)
from scripts.data.collect_sft_trajectories import validate_trajectory_record  # noqa: E402

DEFAULT_CONFIG = Path("archive/training/configs/sft_05b.yaml")
DEFAULT_JSONL = Path("data/sft_traj/expert_v1.jsonl")
DEFAULT_SPLIT_MANIFEST = Path("data/sft_traj/split_manifest.json")
DEFAULT_MIN_RECORDS = 32
DEFAULT_HOLDOUT_RECORDS = 100
EXPECTED_TRL_PIN = "trl==1.4.0"
SPLIT_SAFE_COLLECTION_METHODS = {
    "oracle_from_clean_diff",
    "synthetic_from_canonical_train_rows",
}
INFERABLE_LABELS = {"deterministic_normalization", "context_derivable"}
INFERABILITY_LABELS = INFERABLE_LABELS | {
    "external_reference_required",
    "not_inferable_from_prompt",
}
EXPECTED_T4_TRAINING = {
    "per_device_train_batch_size": 1,
    "gradient_accumulation_steps": 16,
    "max_seq_length": 1024,
    "loss_type": "chunked_nll",
}


class SftReadinessError(RuntimeError):
    """Raised when the SFT handoff is not ready for Kaggle."""


@dataclass(frozen=True, slots=True)
class SftReadinessReport:
    """Small summary of the validated SFT handoff."""

    records: int
    train_records: int
    heldout_records: int
    train_rows: int
    heldout_rows: int
    datasets: tuple[str, ...]
    difficulties: tuple[str, ...]
    teacher_model: str
    collection_methods: tuple[str, ...]
    package_count: int
    split_manifest_present: bool = False
    split_manifest_train_rows: int = 0
    split_manifest_eval_rows: int = 0
    prompt_contract_version: str | None = None
    noop_records: int = 0
    max_repairs_per_record: int = 0
    inferability_counts: dict[InferabilityLabel, int] | None = None
    inferable_records: int = 0
    noninferable_records: int = 0
    deterministic_records: int = 0
    abstention_records: int = 0


def _as_mapping(value: object, *, name: str) -> dict[str, Any]:
    """Return a JSON/YAML object as a string-keyed mapping."""
    if not isinstance(value, dict):
        raise SftReadinessError(f"{name} must be a mapping.")
    return cast(dict[str, Any], value)


def load_config(path: Path) -> dict[str, Any]:
    """Load and minimally validate the Kaggle SFT YAML config."""
    if not path.exists():
        raise SftReadinessError(f"Missing SFT config: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    config = _as_mapping(payload, name=str(path))
    for section in ("environment", "repos", "model", "lora", "training", "collection"):
        if section not in config:
            raise SftReadinessError(f"Config is missing required section: {section}")

    packages = _as_mapping(config["environment"], name="environment").get("pip_packages")
    if not isinstance(packages, list) or not packages:
        raise SftReadinessError("environment.pip_packages must be a non-empty list.")
    bad_packages = [package for package in packages if not isinstance(package, str)]
    if bad_packages:
        raise SftReadinessError("environment.pip_packages must contain only strings.")
    unpinned = [package for package in packages if "==" not in package]
    if unpinned:
        raise SftReadinessError(
            "Kaggle package pins must be exact. Unpinned entries: " + ", ".join(map(str, unpinned))
        )
    if EXPECTED_TRL_PIN not in packages:
        raise SftReadinessError(
            f"Kaggle config must pin {EXPECTED_TRL_PIN} for loss_type='chunked_nll'."
        )

    training = _as_mapping(config["training"], name="training")
    # The fp16/T4 hardware assertions below are a Week-9 Kaggle-T4 relic. They
    # only apply to fp16/T4 configs; a config that declares bf16=True is a
    # paid-GPU config (e.g. the 1.5B run) whose authoritative validation is
    # structural parity with the proven config, not these stale hardware pins.
    if training.get("bf16") is not True:
        if training.get("fp16") is not True or training.get("bf16") is not False:
            raise SftReadinessError("Kaggle config must use fp16=True and bf16=False.")
        stale_settings = {
            key: training.get(key)
            for key, expected in EXPECTED_T4_TRAINING.items()
            if training.get(key) != expected
        }
        if stale_settings:
            raise SftReadinessError(
                "Kaggle T4 training settings are stale. Expected "
                f"{EXPECTED_T4_TRAINING}; got {stale_settings}."
            )
    _validate_dataset_readme(path=path, config=config)
    return config


def _validate_dataset_readme(*, path: Path, config: dict[str, Any]) -> None:
    """Check that the dataset README agrees with core config filenames and provenance."""
    readme = path.parent.parent / "DATASET_README.md"
    if not readme.exists():
        if path == DEFAULT_CONFIG:
            raise SftReadinessError(f"Missing dataset README: {readme}")
        return
    text = readme.read_text(encoding="utf-8")
    repos = _as_mapping(config["repos"], name="repos")
    trajectory_filename = str(repos.get("trajectory_filename", "expert_v1.jsonl"))
    if trajectory_filename not in text:
        raise SftReadinessError(
            f"{readme} must document configured trajectory file {trajectory_filename!r}."
        )
    split_manifest_filename = str(repos.get("split_manifest_filename", "split_manifest.json"))
    if split_manifest_filename not in text:
        raise SftReadinessError(
            f"{readme} must document configured split manifest {split_manifest_filename!r}."
        )
    collection_methods = _collection_methods(config)
    missing_methods = [method for method in collection_methods if method not in text]
    if missing_methods:
        raise SftReadinessError(
            f"{readme} must document collection method(s): {', '.join(missing_methods)}."
        )


def load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    """Load and schema-validate trajectory JSONL records."""
    if not path.exists():
        raise SftReadinessError(
            f"Missing trajectory JSONL: {path}. Run collect_sft_trajectories.py "
            "with --push-to-hub before starting Kaggle."
        )
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            raw_record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SftReadinessError(f"{path}:{line_number} is not valid JSON: {exc}") from exc
        if not isinstance(raw_record, dict):
            raise SftReadinessError(f"{path}:{line_number} must contain a JSON object.")
        records.append(validate_trajectory_record(cast(dict[str, Any], raw_record)))
    if not records:
        raise SftReadinessError(f"Trajectory JSONL is empty: {path}")
    return records


def load_split_manifest(path: Path | None) -> dict[str, Any] | None:
    """Load an optional split manifest for oracle records."""
    if path is None:
        return None
    if not path.exists():
        raise SftReadinessError(f"Missing split manifest: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _as_mapping(payload, name=str(path))


def _split_sizes(record_count: int, holdout_records: int) -> tuple[int, int]:
    """Return deterministic train/held-out sizes matching the Kaggle notebook."""
    test_size = min(holdout_records, max(1, record_count // 10))
    train_size = record_count - test_size
    if train_size < 1 or test_size < 1:
        raise SftReadinessError(
            "Need at least two trajectory records so train/held-out split is non-empty."
        )
    return train_size, test_size


def _collection_methods(config: dict[str, Any]) -> set[str]:
    """Return configured collection methods, defaulting to legacy ReAct records."""
    collection = _as_mapping(config["collection"], name="collection")
    raw_methods = collection.get("collection_methods")
    if raw_methods is None:
        return {"llm_react_chunk"}
    if not isinstance(raw_methods, list) or not raw_methods:
        raise SftReadinessError("collection.collection_methods must be a non-empty list.")
    methods: set[str] = set()
    for method in raw_methods:
        if not isinstance(method, str) or not method.strip():
            raise SftReadinessError("collection.collection_methods entries must be strings.")
        methods.add(method)
    return methods


def _prompt_contract_version(config: dict[str, Any]) -> str | None:
    """Return the configured repair prompt contract version, if any."""
    collection = _as_mapping(config["collection"], name="collection")
    value = collection.get("prompt_contract_version")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SftReadinessError("collection.prompt_contract_version must be a string.")
    return value


def _max_repairs_per_record(config: dict[str, Any]) -> int | None:
    """Return the configured v2 repair-density cap."""
    oracle = _as_mapping(config["collection"], name="collection").get("oracle", {})
    oracle_config = _as_mapping(oracle, name="collection.oracle") if oracle else {}
    value = oracle_config.get("max_repairs_per_record")
    if value is None:
        return None
    if not isinstance(value, int) or value < 1:
        raise SftReadinessError("collection.oracle.max_repairs_per_record must be >= 1.")
    return value


def _min_noop_ratio(config: dict[str, Any]) -> float:
    """Return the configured minimum no-op ratio."""
    oracle = _as_mapping(config["collection"], name="collection").get("oracle", {})
    oracle_config = _as_mapping(oracle, name="collection.oracle") if oracle else {}
    value = float(oracle_config.get("min_noop_ratio", 0.0))
    if value < 0.0 or value >= 1.0:
        raise SftReadinessError("collection.oracle.min_noop_ratio must be >= 0 and < 1.")
    return value


def _min_inferable_records(config: dict[str, Any]) -> int:
    """Return the configured minimum v3 trainable inferability count."""
    oracle = _as_mapping(config["collection"], name="collection").get("oracle", {})
    oracle_config = _as_mapping(oracle, name="collection.oracle") if oracle else {}
    value = oracle_config.get("min_inferable_records", 0)
    if not isinstance(value, int) or value < 0:
        raise SftReadinessError("collection.oracle.min_inferable_records must be >= 0.")
    return value


def _min_deterministic_records(config: dict[str, Any]) -> int:
    """Return the configured minimum v4 promotion-slice examples."""
    oracle = _as_mapping(config["collection"], name="collection").get("oracle", {})
    oracle_config = _as_mapping(oracle, name="collection.oracle") if oracle else {}
    value = oracle_config.get("min_deterministic_records", 0)
    if not isinstance(value, int) or value < 0:
        raise SftReadinessError("collection.oracle.min_deterministic_records must be >= 0.")
    return value


def _min_abstention_records(config: dict[str, Any]) -> int:
    """Return the configured minimum v4 abstention examples."""
    oracle = _as_mapping(config["collection"], name="collection").get("oracle", {})
    oracle_config = _as_mapping(oracle, name="collection.oracle") if oracle else {}
    value = oracle_config.get("min_abstention_records", 0)
    if not isinstance(value, int) or value < 0:
        raise SftReadinessError("collection.oracle.min_abstention_records must be >= 0.")
    return value


def _record_inferability(record: dict[str, Any], *, index: int) -> InferabilityLabel | None:
    """Return and validate a trajectory record's inferability label when present."""
    raw_label = record.get("inferability")
    if raw_label is None:
        raw_label = _as_mapping(record["provenance"], name=f"record {index} provenance").get(
            "inferability"
        )
    if raw_label is None:
        return None
    if raw_label not in INFERABILITY_LABELS:
        raise SftReadinessError(f"Record {index} has unknown inferability label {raw_label!r}.")
    return cast(InferabilityLabel, raw_label)


def _oracle_teacher_key(config: dict[str, Any]) -> tuple[str, str]:
    """Return the configured oracle provider/model pair."""
    collection = _as_mapping(config["collection"], name="collection")
    oracle = collection.get("oracle", {})
    oracle_config = _as_mapping(oracle, name="collection.oracle") if oracle else {}
    provider = str(oracle_config.get("provider", "oracle"))
    model = str(oracle_config.get("model", "clean-diff-v1"))
    return provider, model


def _rows_from_payload(rows: object, *, name: str) -> set[int]:
    """Extract integer row ids from serialized prompt rows."""
    if not isinstance(rows, list):
        raise SftReadinessError(f"{name} must be a list.")
    row_ids: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise SftReadinessError(f"{name} entries must be mappings.")
        raw_row = row.get("_row")
        try:
            row_ids.add(int(str(raw_row)))
        except (TypeError, ValueError) as exc:
            raise SftReadinessError(f"{name} entries must include integer _row values.") from exc
    return row_ids


def _rows_from_repairs(repairs: object, *, name: str) -> set[int]:
    """Extract integer row ids from serialized fixes or candidates."""
    if not isinstance(repairs, list):
        raise SftReadinessError(f"{name} must be a list.")
    row_ids: set[int] = set()
    for repair in repairs:
        if not isinstance(repair, dict):
            raise SftReadinessError(f"{name} entries must be mappings.")
        row = repair.get("row")
        if not isinstance(row, int):
            raise SftReadinessError(f"{name} entries must include integer row values.")
        row_ids.add(row)
    return row_ids


def _oracle_eval_rows(record: dict[str, Any], *, index: int) -> set[int]:
    """Return held-out row ids recorded for an oracle trajectory."""
    provenance = _as_mapping(record["provenance"], name=f"record {index} provenance")
    raw_eval_rows = provenance.get("eval_rows")
    if not isinstance(raw_eval_rows, list) or not all(
        isinstance(row, int) for row in raw_eval_rows
    ):
        raise SftReadinessError(f"Record {index} oracle provenance must include integer eval_rows.")
    return set(cast(list[int], raw_eval_rows))


def _validate_oracle_no_eval_leak(
    record: dict[str, Any], *, index: int
) -> tuple[set[int], set[int]]:
    """Reject oracle records that expose held-out rows anywhere in the SFT example."""
    eval_rows = _oracle_eval_rows(record, index=index)
    if not eval_rows:
        raise SftReadinessError(f"Record {index} oracle provenance has no held-out rows.")
    state = _as_mapping(record["state"], name=f"record {index} state")
    provenance = _as_mapping(record["provenance"], name=f"record {index} provenance")
    if state.get("split") != "train" or provenance.get("split") != "train":
        raise SftReadinessError(f"Record {index} oracle record must be marked split='train'.")
    exposed_rows = set()
    exposed_rows.update(
        _rows_from_payload(state.get("target_rows"), name=f"record {index} target_rows")
    )
    exposed_rows.update(
        _rows_from_payload(state.get("context_rows"), name=f"record {index} context_rows")
    )
    exposed_rows.update(_rows_from_repairs(record.get("fix"), name=f"record {index} fix"))
    candidates = state.get("normalization_candidates", [])
    if candidates:
        exposed_rows.update(
            _rows_from_repairs(candidates, name=f"record {index} normalization_candidates")
        )
        fix_values = {
            (repair["row"], repair["column"], repair["new_value"])
            for repair in record.get("fix", [])
            if isinstance(repair, dict)
            and isinstance(repair.get("row"), int)
            and isinstance(repair.get("column"), str)
            and isinstance(repair.get("new_value"), str)
        }
        leaked_candidates = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            key = (
                candidate.get("row"),
                candidate.get("column"),
                candidate.get("suggested_value"),
            )
            if key in fix_values:
                leaked_candidates.append(key)
        if leaked_candidates:
            raise SftReadinessError(
                "Record "
                f"{index} leaks oracle clean labels through normalization_candidates: "
                f"{leaked_candidates[:5]}"
            )
    leaked = sorted(exposed_rows & eval_rows)
    if leaked:
        raise SftReadinessError(
            f"Record {index} leaks held-out eval row(s) into SFT payload: {leaked[:10]}"
        )
    return exposed_rows, eval_rows


def _assert_manifest_has_no_label_fields(value: object, *, path: str = "manifest") -> None:
    """Reject split manifests that accidentally contain clean labels or repairs."""
    forbidden = {
        "clean",
        "clean_value",
        "ground_truth",
        "new_value",
        "normalization_candidates",
        "repairs",
        "suggested_value",
    }
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            if key_text in forbidden:
                raise SftReadinessError(
                    f"Split manifest leaks label-bearing field {path}.{key_text!s}."
                )
            _assert_manifest_has_no_label_fields(child, path=f"{path}.{key_text}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_manifest_has_no_label_fields(child, path=f"{path}[{index}]")


def _manifest_row_entries(entries: object, *, name: str) -> set[int]:
    """Parse manifest row entries and verify their dirty-row hash shape."""
    if not isinstance(entries, list):
        raise SftReadinessError(f"{name} must be a list.")
    rows: set[int] = set()
    for index, entry in enumerate(entries):
        item = _as_mapping(entry, name=f"{name}[{index}]")
        row = item.get("row")
        digest = item.get("dirty_row_sha256")
        if not isinstance(row, int):
            raise SftReadinessError(f"{name}[{index}].row must be an integer.")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise SftReadinessError(
                f"{name}[{index}].dirty_row_sha256 must be a lowercase SHA-256 hex digest."
            )
        if row in rows:
            raise SftReadinessError(f"{name} contains duplicate row id {row}.")
        rows.add(row)
    return rows


def _split_manifest_rows(
    manifest: dict[str, Any],
) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """Return train/eval row maps after validating split-manifest structure."""
    _assert_manifest_has_no_label_fields(manifest)
    if manifest.get("schema_version") != "split_manifest_v1":
        raise SftReadinessError("Split manifest schema_version must be split_manifest_v1.")
    if manifest.get("collection_method") != "oracle_from_clean_diff":
        raise SftReadinessError("Split manifest collection_method must be oracle_from_clean_diff.")
    raw_datasets = manifest.get("datasets")
    if not isinstance(raw_datasets, list) or not raw_datasets:
        raise SftReadinessError("Split manifest must include a non-empty datasets list.")
    train_by_dataset: dict[str, set[int]] = {}
    eval_by_dataset: dict[str, set[int]] = {}
    for index, raw_dataset in enumerate(raw_datasets):
        dataset_entry = _as_mapping(raw_dataset, name=f"split_manifest.datasets[{index}]")
        dataset = dataset_entry.get("dataset")
        if not isinstance(dataset, str) or not dataset:
            raise SftReadinessError(f"split_manifest.datasets[{index}].dataset must be a string.")
        if dataset in train_by_dataset:
            raise SftReadinessError(f"Split manifest contains duplicate dataset {dataset!r}.")
        train_rows = _manifest_row_entries(
            dataset_entry.get("train"), name=f"split_manifest.{dataset}.train"
        )
        eval_rows = _manifest_row_entries(
            dataset_entry.get("eval"), name=f"split_manifest.{dataset}.eval"
        )
        if train_rows & eval_rows:
            raise SftReadinessError(
                f"Split manifest dataset {dataset!r} has rows in both train and eval."
            )
        if dataset_entry.get("train_rows") != len(train_rows):
            raise SftReadinessError(f"Split manifest dataset {dataset!r} train_rows mismatch.")
        if dataset_entry.get("eval_rows") != len(eval_rows):
            raise SftReadinessError(f"Split manifest dataset {dataset!r} eval_rows mismatch.")
        train_by_dataset[dataset] = train_rows
        eval_by_dataset[dataset] = eval_rows
    return train_by_dataset, eval_by_dataset


def _validate_record_against_manifest(
    record: dict[str, Any],
    *,
    index: int,
    manifest_train: dict[str, set[int]],
    manifest_eval: dict[str, set[int]],
) -> None:
    """Ensure an oracle record agrees with the published split manifest."""
    dataset = str(record["dataset"])
    if dataset not in manifest_train or dataset not in manifest_eval:
        raise SftReadinessError(
            f"Record {index} dataset {dataset!r} is absent from split manifest."
        )
    eval_rows = _oracle_eval_rows(record, index=index)
    if eval_rows != manifest_eval[dataset]:
        raise SftReadinessError(
            f"Record {index} oracle eval rows disagree with split_manifest.json."
        )
    state = _as_mapping(record["state"], name=f"record {index} state")
    exposed_rows = set()
    exposed_rows.update(
        _rows_from_payload(state.get("target_rows"), name=f"record {index} target_rows")
    )
    exposed_rows.update(
        _rows_from_payload(state.get("context_rows"), name=f"record {index} context_rows")
    )
    exposed_rows.update(_rows_from_repairs(record.get("fix"), name=f"record {index} fix"))
    outside_train = sorted(exposed_rows - manifest_train[dataset])
    if outside_train:
        raise SftReadinessError(
            f"Record {index} exposes rows outside the split manifest train set: "
            f"{outside_train[:10]}"
        )


def _validate_prompt_contract(
    record: dict[str, Any],
    *,
    index: int,
    expected_contract: str | None,
    max_repairs_per_record: int | None,
) -> tuple[int, bool]:
    """Validate one record's prompt/action contract and repair density."""
    repairs = record.get("fix")
    if not isinstance(repairs, list):
        raise SftReadinessError(f"Record {index} fix must be a list.")
    if max_repairs_per_record is not None and len(repairs) > max_repairs_per_record:
        raise SftReadinessError(
            f"Record {index} has {len(repairs)} repairs; configured cap is "
            f"{max_repairs_per_record}."
        )
    if expected_contract is None:
        return len(repairs), not repairs

    record_contract = record.get("prompt_contract_version") or _as_mapping(
        record["provenance"], name=f"record {index} provenance"
    ).get("prompt_contract_version")
    if record_contract != expected_contract:
        raise SftReadinessError(
            f"Record {index} prompt contract {record_contract!r}; expected {expected_contract!r}."
        )
    if expected_contract not in {
        CONTRACT_VERSION,
        CONTRACT_VERSION_V1,
        CONTRACT_VERSION_V2,
        CONTRACT_VERSION_V3,
    }:
        raise SftReadinessError(f"Unsupported configured prompt contract {expected_contract!r}.")

    messages = record.get("messages")
    if not isinstance(messages, list) or len(messages) < 3:
        raise SftReadinessError(f"Record {index} must include system/user/assistant messages.")
    user_message = messages[1]
    assistant_message = messages[-1]
    if not isinstance(user_message, dict) or not isinstance(assistant_message, dict):
        raise SftReadinessError(f"Record {index} messages must be mappings.")
    try:
        user_payload = json.loads(str(user_message["content"]))
    except (KeyError, json.JSONDecodeError) as exc:
        raise SftReadinessError(f"Record {index} user message is not contract JSON.") from exc
    user_map = _as_mapping(user_payload, name=f"record {index} user payload")
    required_user_keys = {
        "contract_version",
        "schema_summary",
        "allowed_columns",
        "target_rows",
        "context_rows",
    }
    strict_contract = expected_contract in {CONTRACT_VERSION_V2, CONTRACT_VERSION_V3}
    if strict_contract:
        required_user_keys.add("valid_rows")
    missing = sorted(required_user_keys - set(user_map))
    if missing:
        raise SftReadinessError(
            f"Record {index} user payload missing contract key(s): {', '.join(missing)}."
        )
    if user_map["contract_version"] != expected_contract:
        raise SftReadinessError(f"Record {index} user payload contract mismatch.")
    if (
        _as_mapping(record["state"], name=f"record {index} state").get("target_rows")
        != user_map["target_rows"]
    ):
        raise SftReadinessError(f"Record {index} target_rows drift between state and prompt.")
    parse_result = parse_repair_action(
        str(assistant_message.get("content", "")),
        allowed_columns=cast(list[str], user_map["allowed_columns"]) if strict_contract else None,
        valid_rows=cast(list[int], user_map["valid_rows"])
        if strict_contract and isinstance(user_map.get("valid_rows"), list)
        else None,
        require_explicit_action=strict_contract,
    )
    if not parse_result.ok or parse_result.action is None:
        raise SftReadinessError(
            f"Record {index} assistant message violates repair contract: "
            f"{parse_result.error_message}"
        )
    if expected_contract == CONTRACT_VERSION_V3:
        parsed_repairs = [
            {"row": repair.row, "column": repair.column, "new_value": repair.new_value}
            for repair in parse_result.action.repairs
        ]
    else:
        parsed_repairs = [repair.model_dump(mode="json") for repair in parse_result.action.repairs]
    if parsed_repairs != repairs:
        raise SftReadinessError(f"Record {index} assistant repairs drift from fix field.")
    return len(repairs), not repairs


def validate_records(
    records: list[dict[str, Any]],
    *,
    config: dict[str, Any],
    split_manifest: dict[str, Any] | None = None,
    min_records: int = DEFAULT_MIN_RECORDS,
    holdout_records: int = DEFAULT_HOLDOUT_RECORDS,
) -> SftReadinessReport:
    """Validate trajectory content against the YAML contract."""
    if min_records < 2:
        raise ValueError("min_records must be >= 2")
    if holdout_records < 1:
        raise ValueError("holdout_records must be >= 1")
    if len(records) < min_records:
        raise SftReadinessError(
            f"Only {len(records)} trajectory records found; need at least {min_records} "
            "before Kaggle training is worth launching."
        )

    collection = _as_mapping(config["collection"], name="collection")
    expected_schema = str(collection["schema_version"])
    expected_min_f1 = float(collection["min_episode_f1"])
    expected_teacher = str(collection["teacher_model"])
    expected_provider = collection.get("teacher_provider")
    allowed_methods = _collection_methods(config)
    oracle_teacher = _oracle_teacher_key(config)
    expected_prompt_contract = _prompt_contract_version(config)
    max_repairs_per_record = _max_repairs_per_record(config)
    min_noop_ratio = _min_noop_ratio(config)
    min_inferable_records = _min_inferable_records(config)
    min_deterministic_records = _min_deterministic_records(config)
    min_abstention_records = _min_abstention_records(config)
    accepted_teachers = collection.get("accepted_teachers")
    allowed_teachers: set[tuple[str | None, str]] = set()
    if isinstance(expected_provider, str):
        allowed_teachers.add((expected_provider, expected_teacher))
    else:
        allowed_teachers.add((None, expected_teacher))
    if isinstance(accepted_teachers, list):
        for raw_teacher in accepted_teachers:
            if not isinstance(raw_teacher, dict):
                raise SftReadinessError("collection.accepted_teachers entries must be mappings.")
            provider = raw_teacher.get("provider")
            model = raw_teacher.get("model")
            if not isinstance(provider, str) or not isinstance(model, str):
                raise SftReadinessError(
                    "collection.accepted_teachers entries require provider and model strings."
                )
            allowed_teachers.add((provider, model))

    trajectory_ids: set[str] = set()
    chunk_keys: set[tuple[str, int, int]] = set()
    datasets: set[str] = set()
    difficulties: set[str] = set()
    collection_methods: set[str] = set()
    inferability_counts: dict[InferabilityLabel, int] = {}
    noop_records = 0
    max_repairs_seen = 0
    train_row_ids: set[tuple[str, int]] = set()
    heldout_row_ids: set[tuple[str, int]] = set()
    manifest_train: dict[str, set[int]] = {}
    manifest_eval: dict[str, set[int]] = {}
    if split_manifest is not None:
        manifest_train, manifest_eval = _split_manifest_rows(split_manifest)
    for index, record in enumerate(records, start=1):
        if record["schema_version"] != expected_schema:
            raise SftReadinessError(
                f"Record {index} has schema_version={record['schema_version']!r}; "
                f"expected {expected_schema!r}."
            )
        inferability = _record_inferability(record, index=index)
        if expected_schema in {"expert_v3", "expert_v4"} and inferability is None:
            raise SftReadinessError(f"Record {index} {expected_schema} requires inferability.")
        if inferability is not None:
            inferability_counts[inferability] = inferability_counts.get(inferability, 0) + 1
        trajectory_id = str(record["trajectory_id"])
        if trajectory_id in trajectory_ids:
            raise SftReadinessError(f"Duplicate trajectory_id: {trajectory_id}")
        trajectory_ids.add(trajectory_id)

        chunk_key = (str(record["task_id"]), int(record["seed"]), int(record["chunk_index"]))
        if chunk_key in chunk_keys:
            raise SftReadinessError(
                "Duplicate chunk key: "
                f"task_id={chunk_key[0]} seed={chunk_key[1]} chunk_index={chunk_key[2]}"
            )
        chunk_keys.add(chunk_key)

        metrics = _as_mapping(record["metrics"], name=f"record {index} metrics")
        episode_f1 = float(metrics.get("episode_f1", -1.0))
        if episode_f1 < expected_min_f1:
            raise SftReadinessError(
                f"Record {index} has episode_f1={episode_f1}; expected >= {expected_min_f1}."
            )

        teacher = _as_mapping(record["teacher"], name=f"record {index} teacher")
        provenance = _as_mapping(record["provenance"], name=f"record {index} provenance")
        collection_method = str(provenance.get("collection_method", "llm_react_chunk"))
        if collection_method not in allowed_methods:
            raise SftReadinessError(
                f"Record {index} collection_method={collection_method!r}; "
                f"expected one of {sorted(allowed_methods)}."
            )
        collection_methods.add(collection_method)
        record_provider = teacher.get("provider")
        record_model = str(teacher.get("model", ""))
        record_teacher_key = (
            str(record_provider)
            if isinstance(record_provider, str) and isinstance(expected_provider, str)
            else None,
            record_model,
        )
        if collection_method in SPLIT_SAFE_COLLECTION_METHODS:
            if (str(record_provider), record_model) != oracle_teacher:
                raise SftReadinessError(
                    f"Record {index} oracle teacher {(record_provider, record_model)!r} "
                    f"does not match configured {oracle_teacher!r}."
                )
            exposed_rows, heldout_rows = _validate_oracle_no_eval_leak(record, index=index)
            if split_manifest is not None:
                _validate_record_against_manifest(
                    record,
                    index=index,
                    manifest_train=manifest_train,
                    manifest_eval=manifest_eval,
                )
            dataset = str(record["dataset"])
            train_row_ids.update((dataset, row) for row in exposed_rows)
            heldout_row_ids.update((dataset, row) for row in heldout_rows)
        elif record_teacher_key not in allowed_teachers:
            raise SftReadinessError(
                f"Record {index} teacher {record_teacher_key!r} does not match "
                f"configured accepted teachers."
            )
        if not record.get("messages"):
            raise SftReadinessError(f"Record {index} has no chat messages.")
        repair_count, is_noop = _validate_prompt_contract(
            record,
            index=index,
            expected_contract=expected_prompt_contract,
            max_repairs_per_record=max_repairs_per_record,
        )
        if expected_schema == "expert_v4":
            if inferability == PROMOTION_SLICE and repair_count < 1:
                raise SftReadinessError(
                    f"Record {index} expert_v4 deterministic_normalization must carry repairs."
                )
            if inferability in ABSTENTION_SLICES | AUXILIARY_SLICES and repair_count:
                raise SftReadinessError(
                    f"Record {index} expert_v4 {inferability} must be a finish/no-repair example."
                )
        max_repairs_seen = max(max_repairs_seen, repair_count)
        if is_noop:
            noop_records += 1
        datasets.add(str(record["dataset"]))
        difficulties.add(str(record["difficulty"]))

    train_size, test_size = _split_sizes(len(records), holdout_records)
    if min_noop_ratio > 0.0 and noop_records / len(records) < min_noop_ratio:
        raise SftReadinessError(
            "No-op ratio below configured gate: "
            f"{noop_records / len(records):.3f} < {min_noop_ratio:.3f}."
        )
    inferable_records = sum(
        count for label, count in inferability_counts.items() if label in INFERABLE_LABELS
    )
    noninferable_records = sum(inferability_counts.values()) - inferable_records
    deterministic_records = inferability_counts.get(PROMOTION_SLICE, 0)
    abstention_records = sum(
        count for label, count in inferability_counts.items() if label in ABSTENTION_SLICES
    )
    if min_inferable_records and inferable_records < min_inferable_records:
        raise SftReadinessError(
            "Inferable v3 record count below configured gate: "
            f"{inferable_records} < {min_inferable_records}."
        )
    if expected_schema == "expert_v4":
        if min_deterministic_records and deterministic_records < min_deterministic_records:
            raise SftReadinessError(
                "Deterministic v4 record count below configured gate: "
                f"{deterministic_records} < {min_deterministic_records}."
            )
        if min_abstention_records and abstention_records < min_abstention_records:
            raise SftReadinessError(
                "Abstention v4 record count below configured gate: "
                f"{abstention_records} < {min_abstention_records}."
            )
    packages = cast(
        list[str], _as_mapping(config["environment"], name="environment")["pip_packages"]
    )
    if split_manifest is not None and "oracle_from_clean_diff" in collection_methods:
        configured_datasets = set(datasets)
        missing_manifest_datasets = configured_datasets - set(manifest_train)
        if missing_manifest_datasets:
            raise SftReadinessError(
                "Split manifest is missing oracle dataset(s): "
                + ", ".join(sorted(missing_manifest_datasets))
            )
    manifest_train_rows = sum(len(rows) for rows in manifest_train.values())
    manifest_eval_rows = sum(len(rows) for rows in manifest_eval.values())
    return SftReadinessReport(
        records=len(records),
        train_records=train_size,
        heldout_records=test_size,
        train_rows=manifest_train_rows or len(train_row_ids),
        heldout_rows=manifest_eval_rows or len(heldout_row_ids),
        datasets=tuple(sorted(datasets)),
        difficulties=tuple(sorted(difficulties)),
        teacher_model=expected_teacher,
        collection_methods=tuple(sorted(collection_methods)),
        package_count=len(packages),
        split_manifest_present=split_manifest is not None,
        split_manifest_train_rows=manifest_train_rows,
        split_manifest_eval_rows=manifest_eval_rows,
        prompt_contract_version=expected_prompt_contract,
        noop_records=noop_records,
        max_repairs_per_record=max_repairs_seen,
        inferability_counts=inferability_counts or None,
        inferable_records=inferable_records,
        noninferable_records=noninferable_records,
        deterministic_records=deterministic_records,
        abstention_records=abstention_records,
    )


def validate_sft_readiness(
    *,
    jsonl: Path = DEFAULT_JSONL,
    config_path: Path = DEFAULT_CONFIG,
    split_manifest: Path | None = None,
    min_records: int = DEFAULT_MIN_RECORDS,
    holdout_records: int = DEFAULT_HOLDOUT_RECORDS,
) -> SftReadinessReport:
    """Validate the local SFT handoff and return a compact report."""
    config = load_config(config_path)
    records = load_jsonl_records(jsonl)
    manifest = load_split_manifest(split_manifest)
    return validate_records(
        records,
        config=config,
        split_manifest=manifest,
        min_records=min_records,
        holdout_records=holdout_records,
    )


def _build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", type=Path, default=DEFAULT_JSONL)
    parser.add_argument("--config", dest="config_path", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT_MANIFEST)
    parser.add_argument("--min-records", type=int, default=DEFAULT_MIN_RECORDS)
    parser.add_argument("--holdout-records", type=int, default=DEFAULT_HOLDOUT_RECORDS)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the readiness validator CLI."""
    args = _build_parser().parse_args(argv)
    report = validate_sft_readiness(
        jsonl=args.jsonl,
        config_path=args.config_path,
        split_manifest=args.split_manifest,
        min_records=args.min_records,
        holdout_records=args.holdout_records,
    )
    table = Table(title="Week 9 SFT Kaggle Readiness")
    table.add_column("Check")
    table.add_column("Value")
    table.add_row("Trajectory records", str(report.records))
    table.add_row("Train records", str(report.train_records))
    table.add_row("Held-out records", str(report.heldout_records))
    table.add_row("Train rows seen", str(report.train_rows))
    table.add_row("Held-out rows reserved", str(report.heldout_rows))
    table.add_row("Datasets", ", ".join(report.datasets))
    table.add_row("Difficulties", ", ".join(report.difficulties))
    table.add_row("Teacher model", report.teacher_model)
    table.add_row("Collection methods", ", ".join(report.collection_methods))
    table.add_row("Prompt contract", report.prompt_contract_version or "legacy")
    if report.inferability_counts:
        table.add_row(
            "Inferability",
            ", ".join(
                f"{label}={count}" for label, count in sorted(report.inferability_counts.items())
            ),
        )
        table.add_row("Inferable records", str(report.inferable_records))
        table.add_row("Non-inferable records", str(report.noninferable_records))
        table.add_row("Deterministic records", str(report.deterministic_records))
        table.add_row("Abstention records", str(report.abstention_records))
    table.add_row("No-op records", str(report.noop_records))
    table.add_row("Max repairs/record", str(report.max_repairs_per_record))
    table.add_row("Split manifest", "present" if report.split_manifest_present else "not checked")
    table.add_row("Pinned packages", str(report.package_count))
    Console().print(table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
