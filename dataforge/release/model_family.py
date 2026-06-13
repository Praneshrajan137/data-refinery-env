"""Manifest-driven policy for DataForge Hugging Face model-family releases."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, cast

import yaml

FAMILY_MANIFEST_SCHEMA_VERSION = "dataforge_model_family_manifest_v1"
FAMILY_REPORT_SCHEMA_VERSION = "dataforge_model_family_report_v2"
DEFAULT_HF_OWNER = "Praneshrajan15"
DEFAULT_DATASET_REPO = "Praneshrajan15/dataforge-sft-trajectories"
MODEL_FAMILY_SIZES = ("0.5B", "1.5B", "3B", "7B")
MODEL_FAMILY_STAGES = ("SFT", "GRPO", "GiGPO")
PASSING_QUALITY_STATUSES = frozenset({"quality_improved_verified", "quality_verified"})
PUBLIC_ARTIFACT_STATUSES = frozenset({"public", "public_verified"})

_STAGE_PREDECESSOR = {"SFT": None, "GRPO": "SFT", "GiGPO": "GRPO"}
_SIZE_CONFIG_SLUGS = {"0.5B": "05b", "1.5B": "15b", "3B": "3b", "7B": "7b"}
_SIZE_PATH_SLUGS = {"0.5B": "0.5b", "1.5B": "1.5b", "3B": "3b", "7B": "7b"}


class ModelFamilyError(RuntimeError):
    """Raised when the model-family manifest or evidence is internally unsafe."""


class BaseModelInfo(Protocol):
    """Minimal Hugging Face model-info shape used for license resolution."""

    @property
    def sha(self) -> str | None:
        """Return the Hub commit SHA."""
        ...


class BaseModelApi(Protocol):
    """Subset of HfApi used by the family policy module."""

    def model_info(self, repo_id: str, *, token: str | None = None) -> BaseModelInfo:
        """Return model metadata from the Hub."""


@dataclass(frozen=True, slots=True)
class HubLicense:
    """Normalized license metadata from a Hugging Face model card."""

    license: str
    license_name: str | None
    license_link: str | None
    source_sha: str | None = None


@dataclass(frozen=True, slots=True)
class ModelFamilyEntry:
    """One expected DataForge model-family row."""

    size: str
    stage: str
    base_model: str
    upstream_license: str
    hub_license: str
    license_name: str | None
    license_link: str | None
    repo_id: str
    predecessor_repo: str | None
    compute_backend: str
    training_backend: str
    config_path: str
    verifier: str
    eval_gate: dict[str, Any]
    artifact_status: str
    quality_status: str
    limitations: tuple[str, ...]

    @property
    def slug(self) -> str:
        """Return the repository slug without the owner namespace."""
        return self.repo_id.rsplit("/", 1)[-1]

    @property
    def size_config_slug(self) -> str:
        """Return the compact file-name slug for this size."""
        return _SIZE_CONFIG_SLUGS[self.size]

    @property
    def size_path_slug(self) -> str:
        """Return the lowercase path slug for this size."""
        return _SIZE_PATH_SLUGS[self.size]

    @property
    def is_public_quality_verified(self) -> bool:
        """Return whether this row may satisfy the full-family quality gate."""
        return (
            self.artifact_status in PUBLIC_ARTIFACT_STATUSES
            and self.quality_status in PASSING_QUALITY_STATUSES
        )

    def model_card_metadata(self, dataset_repo: str) -> dict[str, Any]:
        """Return required Hugging Face model-card front-matter for this row."""
        metadata: dict[str, Any] = {
            "license": self.hub_license,
            "base_model": self.base_model,
            "library_name": "transformers",
            "datasets": [dataset_repo],
            "metrics": ["f1"],
            "tags": ["dataforge", "data-quality", self.stage.lower(), self.size.lower()],
        }
        if self.license_name:
            metadata["license_name"] = self.license_name
        if self.license_link:
            metadata["license_link"] = self.license_link
        metadata["model-index"] = [
            {
                "name": self.slug,
                "results": [
                    {
                        "task": {
                            "type": "text-generation",
                            "name": "DataForge repair planning",
                        },
                        "dataset": {"name": "DataForge SFT trajectories", "type": dataset_repo},
                        "metrics": [
                            {
                                "type": "macro_f1",
                                "name": "Held-out macro F1",
                                "value": None,
                            }
                        ],
                    }
                ],
            }
        ]
        return metadata


@dataclass(frozen=True, slots=True)
class ModelFamilyManifest:
    """Loaded model-family manifest."""

    schema_version: str
    hf_owner: str
    dataset_repo: str
    entries: tuple[ModelFamilyEntry, ...]
    source_path: Path | None = None

    @property
    def manifest_hash(self) -> str:
        """Return a stable hash over the manifest entries."""
        payload = {
            "schema_version": self.schema_version,
            "hf_owner": self.hf_owner,
            "dataset_repo": self.dataset_repo,
            "entries": [asdict(entry) for entry in self.entries],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def repo_ids(self) -> tuple[str, ...]:
        """Return expected repo ids in manifest order."""
        return tuple(entry.repo_id for entry in self.entries)

    def entry_for(self, *, size: str, stage: str) -> ModelFamilyEntry:
        """Return the manifest row for a size and stage."""
        for entry in self.entries:
            if entry.size == size and entry.stage == stage:
                return entry
        raise ModelFamilyError(f"Missing model-family row for {size}-{stage}.")

    def entry_for_repo(self, repo_id: str) -> ModelFamilyEntry | None:
        """Return the manifest row for a repo id, if present."""
        for entry in self.entries:
            if entry.repo_id == repo_id:
                return entry
        return None

    def dependency_errors(self) -> list[str]:
        """Return quality-status dependency violations across the family."""
        by_repo = {entry.repo_id: entry for entry in self.entries}
        errors: list[str] = []
        for entry in self.entries:
            if entry.stage == "SFT":
                continue
            if not entry.predecessor_repo:
                errors.append(f"{entry.repo_id}: predecessor_repo is required for {entry.stage}.")
                continue
            predecessor = by_repo.get(entry.predecessor_repo)
            if predecessor is None:
                errors.append(f"{entry.repo_id}: predecessor {entry.predecessor_repo} is absent.")
                continue
            if entry.is_public_quality_verified and not predecessor.is_public_quality_verified:
                errors.append(
                    f"{entry.repo_id}: cannot be quality-verified before "
                    f"{predecessor.repo_id} is quality-verified."
                )
        return errors


def project_root() -> Path:
    """Return the repository root."""
    return Path(__file__).resolve().parents[2]


def default_manifest_path() -> Path:
    """Return the default model-family manifest path."""
    return project_root() / "training" / "configs" / "model_family.yaml"


def repo_id_for(owner: str, size: str, stage: str) -> str:
    """Return the canonical DataForge Hugging Face repo id."""
    _validate_size_stage(size=size, stage=stage)
    return f"{owner}/DataForge-{size}-{stage}"


def previous_stage(stage: str) -> str | None:
    """Return the required predecessor stage for a training stage."""
    if stage not in MODEL_FAMILY_STAGES:
        raise ModelFamilyError(f"Unknown model-family stage: {stage}")
    return _STAGE_PREDECESSOR[stage]


def expected_license_for_repo(
    repo_id: str,
    *,
    manifest: ModelFamilyManifest | None = None,
) -> str:
    """Return the expected downstream license name for a repo."""
    manifest = manifest or load_model_family_manifest()
    entry = manifest.entry_for_repo(repo_id)
    return entry.upstream_license if entry is not None else "apache-2.0"


def expected_predecessor_for_repo(
    repo_id: str,
    *,
    manifest: ModelFamilyManifest | None = None,
) -> str | None:
    """Return the expected predecessor repo for a repo."""
    manifest = manifest or load_model_family_manifest()
    entry = manifest.entry_for_repo(repo_id)
    return entry.predecessor_repo if entry is not None else None


def license_matches(actual: Any, expected: str, *, license_name: Any = None) -> bool:
    """Return whether Hub or metric license metadata satisfies the manifest policy."""
    actual_text = str(actual or "").strip().lower()
    expected_text = expected.strip().lower()
    license_name_text = str(license_name or "").strip().lower()
    return actual_text == expected_text or license_name_text == expected_text


def hub_license_from_card_data(card_data: Any, *, source_sha: str | None = None) -> HubLicense:
    """Normalize license fields from Hugging Face cardData."""
    card = card_data if isinstance(card_data, dict) else {}
    return HubLicense(
        license=str(card.get("license", "")).strip(),
        license_name=_optional_str(card.get("license_name")),
        license_link=_optional_str(card.get("license_link")),
        source_sha=source_sha,
    )


def resolve_base_license(
    base_model: str,
    *,
    api: BaseModelApi | None = None,
    token: str | None = None,
) -> HubLicense:
    """Resolve current license metadata for a base model from the Hub."""
    if api is None:
        from huggingface_hub import HfApi

        api = cast(BaseModelApi, HfApi())
    info = api.model_info(base_model, token=token)
    return hub_license_from_card_data(getattr(info, "cardData", {}), source_sha=info.sha)


def load_model_family_manifest(path: Path | None = None) -> ModelFamilyManifest:
    """Load and validate the model-family manifest."""
    manifest_path = path or default_manifest_path()
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    payload = _mapping(raw, name=str(manifest_path))
    schema_version = _required_str(payload, "schema_version")
    if schema_version != FAMILY_MANIFEST_SCHEMA_VERSION:
        raise ModelFamilyError(
            f"model-family manifest schema_version must be {FAMILY_MANIFEST_SCHEMA_VERSION}."
        )
    hf_owner = _required_str(payload, "hf_owner")
    dataset_repo = _required_str(payload, "dataset_repo")
    sizes = _size_metadata(payload.get("sizes"))
    stage_gates = _stage_gates(payload.get("stage_gates"))
    rows = _required_list(payload, "models")
    entries: list[ModelFamilyEntry] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        row_mapping = _mapping(row, name="models[]")
        size = _required_str(row_mapping, "size")
        stage = _required_str(row_mapping, "stage")
        _validate_size_stage(size=size, stage=stage)
        if (size, stage) in seen:
            raise ModelFamilyError(f"Duplicate model-family row: {size}-{stage}.")
        seen.add((size, stage))
        size_info = sizes[size]
        repo_id = _optional_str(row_mapping.get("repo_id")) or repo_id_for(hf_owner, size, stage)
        predecessor = _optional_str(row_mapping.get("predecessor_repo"))
        predecessor_stage = previous_stage(stage)
        if predecessor is None and predecessor_stage is not None:
            predecessor = repo_id_for(hf_owner, size, predecessor_stage)
        gate = dict(stage_gates[stage])
        gate.update(_optional_mapping(row_mapping.get("eval_gate"), name="eval_gate"))
        entries.append(
            ModelFamilyEntry(
                size=size,
                stage=stage,
                base_model=size_info["base_model"],
                upstream_license=size_info["upstream_license"],
                hub_license=size_info["hub_license"],
                license_name=_optional_str(size_info.get("license_name")),
                license_link=_optional_str(size_info.get("license_link")),
                repo_id=repo_id,
                predecessor_repo=predecessor,
                compute_backend=_required_str(row_mapping, "compute_backend"),
                training_backend=_required_str(row_mapping, "training_backend"),
                config_path=_required_str(row_mapping, "config_path"),
                verifier=_required_str(row_mapping, "verifier"),
                eval_gate=gate,
                artifact_status=_required_str(row_mapping, "artifact_status"),
                quality_status=_required_str(row_mapping, "quality_status"),
                limitations=tuple(_string_list(row_mapping.get("limitations"))),
            )
        )
    expected = {(size, stage) for size in MODEL_FAMILY_SIZES for stage in MODEL_FAMILY_STAGES}
    missing = sorted(f"{size}-{stage}" for size, stage in expected - seen)
    if missing:
        raise ModelFamilyError("model-family manifest missing rows: " + ", ".join(missing))
    manifest = ModelFamilyManifest(
        schema_version=schema_version,
        hf_owner=hf_owner,
        dataset_repo=dataset_repo,
        entries=tuple(entries),
        source_path=manifest_path,
    )
    dependency_errors = manifest.dependency_errors()
    if dependency_errors:
        raise ModelFamilyError("; ".join(dependency_errors))
    return manifest


def render_stage_config(
    entry: ModelFamilyEntry, *, dataset_repo: str = DEFAULT_DATASET_REPO
) -> dict[str, Any]:
    """Render a training config dictionary for one manifest row."""
    if entry.stage == "SFT":
        return _render_sft_config(entry, dataset_repo=dataset_repo)
    if entry.stage == "GRPO":
        return _render_grpo_config(entry, dataset_repo=dataset_repo)
    if entry.stage == "GiGPO":
        return _render_gigpo_config(entry, dataset_repo=dataset_repo)
    raise ModelFamilyError(f"Unknown model-family stage: {entry.stage}")


def build_hub_upload_manifest(
    entry: ModelFamilyEntry,
    *,
    dataset_repo: str,
    dataset_sha: str,
    source_git_commit: str,
    model_sha: str = "",
    training_run_url: str = "",
    eval_report_path: str = "",
    verification_report_path: str = "",
    eval_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a v2 evidence row skeleton derived from a verified model artifact."""
    return {
        "size": entry.size,
        "stage": entry.stage,
        "repo_id": entry.repo_id,
        "artifact_status": entry.artifact_status,
        "quality_status": entry.quality_status,
        "verifier_passed": entry.quality_status in PASSING_QUALITY_STATUSES,
        "upstream_license": entry.upstream_license,
        "hub_license": entry.hub_license,
        "license_name": entry.license_name,
        "base_model": entry.base_model,
        "predecessor_repo": entry.predecessor_repo,
        "dataset_repo": dataset_repo,
        "training_backend": entry.training_backend,
        "training_run_url": training_run_url,
        "source_git_commit": source_git_commit,
        "dataset_sha": dataset_sha,
        "model_sha": model_sha,
        "model_card_url": f"https://huggingface.co/{entry.repo_id}",
        "eval_report_path": eval_report_path,
        "verification_report_path": verification_report_path,
        "eval_metrics": eval_metrics or {},
        "limitations": list(entry.limitations),
    }


def _render_common_environment(entry: ModelFamilyEntry) -> dict[str, Any]:
    packages = [
        "trl==1.4.0",
        "transformers==5.7.0",
        "accelerate==1.13.0",
        "peft==0.19.1",
        "bitsandbytes==0.49.2",
        "datasets==4.8.5",
        "huggingface_hub==1.13.0",
        "pyyaml==6.0.3",
        "pandas==2.3.3",
    ]
    if entry.stage in {"GRPO", "GiGPO"}:
        packages.append("tensorboard==2.20.0")
    if _uses_hf_jobs(entry):
        packages.append("trackio")
    return {
        "resolved_on": "2026-06-04",
        "runtime_target": entry.compute_backend,
        "pip_packages": packages,
    }


def _render_sft_config(entry: ModelFamilyEntry, *, dataset_repo: str) -> dict[str, Any]:
    return {
        "schema_version": f"sft_{entry.size_config_slug}_v4",
        "environment": _render_common_environment(entry),
        "repos": {
            "dataset_repo_template": "{hf_user}/dataforge-sft-trajectories",
            "source_dataset_repo": dataset_repo,
            "model_repo_template": f"{{hf_user}}/{entry.slug}",
            "trajectory_filename": "expert_v4.jsonl",
            "split_manifest_filename": "split_manifest_v4.json",
            "config_filename": Path(entry.config_path).name,
            "model_card_template_filename": "MODEL_CARD_TEMPLATE.md",
        },
        "release": {
            "require_hf_token": True,
            "upload_eval_diagnostics": True,
            "max_failure_samples": 25,
            "push_only_if_gate_passes": True,
            "promoted_status": "quality_improved_verified",
        },
        "model": {
            "base_model": entry.base_model,
            "model_license": entry.upstream_license,
            "hub_license": entry.hub_license,
            "license_name": entry.license_name,
            "trust_remote_code": True,
            "torch_dtype": "float16",
            "target_model_repo": entry.repo_id,
            "quantization": {
                "load_in_4bit": True,
                "bnb_4bit_quant_type": "nf4",
                "bnb_4bit_use_double_quant": True,
                "bnb_4bit_compute_dtype": "float16",
            },
        },
        "lora": _lora_defaults(entry),
        "training": {
            "output_dir": f"/kaggle/working/dataforge-{entry.size_path_slug}-sft-v4",
            "adapter_dir": f"/kaggle/working/dataforge-{entry.size_path_slug}-sft-v4-adapter",
            "merged_dir": f"/kaggle/working/{entry.slug}-v4-merged",
            "num_train_epochs": 2,
            "per_device_train_batch_size": 1,
            "gradient_accumulation_steps": 16 if entry.size in {"0.5B", "1.5B"} else 32,
            "learning_rate": 0.00002,
            "lr_scheduler_type": "cosine",
            "warmup_ratio": 0.03,
            "weight_decay": 0.0,
            "max_length": 1024,
            "packing": False,
            "logging_steps": 10,
            "save_steps": 100,
            "save_total_limit": 3,
            "fp16": True,
            "bf16": False,
            "gradient_checkpointing": True,
            "report_to": "trackio" if _uses_hf_jobs(entry) else "none",
        },
        "evaluation": _evaluation_defaults(entry),
        "model_card": entry.model_card_metadata(dataset_repo),
    }


def _render_grpo_config(entry: ModelFamilyEntry, *, dataset_repo: str) -> dict[str, Any]:
    return {
        "schema_version": f"grpo_{entry.size_config_slug}_v1",
        "environment": _render_common_environment(entry),
        "repos": {
            "dataset_repo_template": "{hf_user}/dataforge-sft-trajectories",
            "source_dataset_repo": dataset_repo,
            "sft_model_repo": entry.predecessor_repo,
            "grpo_model_repo_template": f"{{hf_user}}/{entry.slug}",
            "config_filename": Path(entry.config_path).name,
            "reward_module": "training/rewards/dataforge_reward.py",
        },
        "model": {
            "base_model": entry.base_model,
            "sft_checkpoint": entry.predecessor_repo,
            "sft_checkpoint_required": True,
            "target_model_repo": entry.repo_id,
            "model_license": entry.upstream_license,
            "hub_license": entry.hub_license,
            "license_name": entry.license_name,
            "trust_remote_code": True,
            "torch_dtype": "float16",
        },
        "lora": _lora_defaults(entry),
        "quantization": _quantization_defaults(entry),
        "training": {
            "output_dir": f"/kaggle/working/dataforge-{entry.size_path_slug}-grpo",
            "adapter_dir": f"/kaggle/working/dataforge-{entry.size_path_slug}-grpo-adapter",
            "merged_dir": f"/kaggle/working/{entry.slug}-merged",
            "max_steps": 500,
            "num_generations": 4,
            "max_completion_length": 256,
            "prompt_token_budget": 1280,
            "per_device_train_batch_size": 1,
            "gradient_accumulation_steps": 16,
            "beta": 0.04,
            "learning_rate": 0.00001,
            "num_iterations": 1,
            "save_steps": 50,
            "logging_steps": 5,
            "report_to": "trackio" if _uses_hf_jobs(entry) else "tensorboard",
            "save_total_limit": 4,
            "lr_scheduler_type": "cosine",
            "warmup_ratio": 0.03,
            "weight_decay": 0.0,
            "fp16": True,
            "bf16": False,
            "gradient_checkpointing": True,
            "use_cache": False,
        },
        "reward": {
            "function": "training.rewards.dataforge_reward:dataforge_reward",
            "prompt_contract_version": "repair_contract_v2",
            "local_stateless": True,
            "endpoint_healthcheck_only": True,
            "parse_failure_reward": 0.0,
        },
        "readiness": {
            "trajectory_filename": "expert_v4.jsonl",
            "split_manifest_filename": "split_manifest_v4.json",
            "prompt_contract_version": "repair_contract_v2",
            "required_datasets": ["hospital", "flights", "beers"],
            "auxiliary_datasets": ["hospital_synthetic_deterministic_v1"],
            "min_records": 128,
            "min_records_per_dataset": 16,
            "min_repair_records": 32,
            "min_repair_signal_domains": 2,
            "min_dirty_records": 0,
            "min_dirty_records_per_dataset": 0,
            "min_clean_records": 32,
            "min_reward_std": 0.05,
            "min_per_dataset_reward_std": 0.01,
            "max_failure_samples": 25,
            "block_on_heldout_leakage": True,
            "require_source_provenance": True,
        },
        "training_sequence": _grpo_training_sequence_defaults(),
        "release": _release_defaults(entry),
        "evaluation": _grpo_evaluation_defaults(),
        "model_card": entry.model_card_metadata(dataset_repo),
    }


def _render_gigpo_config(entry: ModelFamilyEntry, *, dataset_repo: str) -> dict[str, Any]:
    return {
        "schema_version": f"gigpo_{entry.size_config_slug}_v1",
        "environment": _render_common_environment(entry),
        "repos": {
            "dataset_repo_template": "{hf_user}/dataforge-sft-trajectories",
            "source_dataset_repo": dataset_repo,
            "grpo_model_repo": entry.predecessor_repo,
            "gigpo_model_repo_template": f"{{hf_user}}/{entry.slug}",
            "config_filename": Path(entry.config_path).name,
            "advantage_module": "training/gigpo_advantage.py",
        },
        "model": {
            "base_model": entry.base_model,
            "grpo_checkpoint": entry.predecessor_repo,
            "grpo_checkpoint_required": True,
            "target_model_repo": entry.repo_id,
            "model_license": entry.upstream_license,
            "hub_license": entry.hub_license,
            "license_name": entry.license_name,
            "trust_remote_code": True,
            "torch_dtype": "float16",
        },
        "lora": _lora_defaults(entry),
        "quantization": _quantization_defaults(entry),
        "training": {
            "output_dir": f"/kaggle/working/dataforge-{entry.size_path_slug}-gigpo",
            "adapter_dir": f"/kaggle/working/dataforge-{entry.size_path_slug}-gigpo-adapter",
            "merged_dir": f"/kaggle/working/{entry.slug}-merged",
            "max_steps": 500,
            "num_rollouts_per_anchor": 4,
            "max_completion_length": 256,
            "prompt_token_budget": 1024,
            "per_device_train_batch_size": 1,
            "gradient_accumulation_steps": 16,
            "macro_episode_advantage": True,
            "micro_step_advantage": True,
            "anchor_state_grouping": "canonical_observation_hash",
            "learning_rate": 0.000005,
            "save_steps": 50,
            "logging_steps": 5,
            "report_to": "trackio",
            "fp16": True,
            "bf16": False,
            "gradient_checkpointing": True,
            "use_cache": False,
        },
        "reward": {
            "function": "training.rewards.dataforge_reward:dataforge_reward",
            "local_stateless": True,
            "canonical_observation_hashes": True,
        },
        "release": _release_defaults(entry),
        "model_card": entry.model_card_metadata(dataset_repo),
    }


def _lora_defaults(entry: ModelFamilyEntry) -> dict[str, Any]:
    rank = 16 if entry.size == "0.5B" else 8
    if entry.size == "7B":
        rank = 4
    return {
        "enabled": True,
        "r": rank,
        "alpha": 16,
        "dropout": 0.05,
        "bias": "none",
        "task_type": "CAUSAL_LM",
        "target_modules": ["q_proj", "k_proj", "v_proj", "o_proj"],
    }


def _uses_hf_jobs(entry: ModelFamilyEntry) -> bool:
    return entry.training_backend == "hf_jobs" or entry.compute_backend == "hf_jobs_gpu"


def _quantization_defaults(entry: ModelFamilyEntry) -> dict[str, Any]:
    if entry.size == "0.5B":
        return {"load_in_4bit": _uses_hf_jobs(entry)}
    return {
        "load_in_4bit": True,
        "bnb_4bit_quant_type": "nf4",
        "bnb_4bit_use_double_quant": True,
        "bnb_4bit_compute_dtype": "float16",
    }


def _evaluation_defaults(entry: ModelFamilyEntry) -> dict[str, Any]:
    return {
        "heldout_tasks": 100,
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "parse_success_min": entry.eval_gate["parse_success_min"],
        "schema_case_error_max": entry.eval_gate["schema_case_error_max"],
        "min_absolute_f1_gain": entry.eval_gate["min_absolute_f1_gain"],
        "require_eval_diagnostics": True,
        "remote_only": True,
    }


def _grpo_evaluation_defaults() -> dict[str, Any]:
    return {
        "heldout_tasks": 100,
        "seeds_start": 10000,
        "chunk_width": 4,
        "max_new_tokens": 1024,
        "source": "pinned_dataforge_registry",
        "datasets": ["hospital", "flights", "beers"],
    }


def _grpo_training_sequence_defaults() -> dict[str, Any]:
    return {
        "stages": [
            {
                "name": "smoke",
                "max_steps": 50,
                "allow_upload": False,
                "purpose": "Validate imports, dataset contract, reward variance, memory, and logging.",
            },
            {
                "name": "candidate",
                "max_steps": 500,
                "allow_upload_after_gate": True,
                "purpose": "First publish-eligible candidate if strict held-out gate passes.",
            },
            {
                "name": "extended_candidate",
                "max_steps": 1000,
                "run_if": "500_step_improves_trend_but_misses_gate",
                "allow_upload_after_gate": True,
                "purpose": "Only run when the 500-step candidate trends upward but misses +0.03 F1.",
            },
        ],
        "selection_order": [
            "highest_strict_macro_f1",
            "lowest_schema_case_errors",
            "lowest_gpu_hours",
        ],
    }


def _release_defaults(entry: ModelFamilyEntry) -> dict[str, Any]:
    return {
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "min_absolute_f1_gain": entry.eval_gate["min_absolute_f1_gain"],
        "require_parse_success_rate": entry.eval_gate["parse_success_min"],
        "require_schema_case_error_count": entry.eval_gate["schema_case_error_max"],
        "require_eval_diagnostics": True,
        "push_only_if_gate_passes": True,
        "compute_cost_unit": "gpu_hours",
        "training_backend": entry.training_backend,
    }


def _validate_size_stage(*, size: str, stage: str) -> None:
    if size not in MODEL_FAMILY_SIZES:
        raise ModelFamilyError(f"Unknown model-family size: {size}")
    if stage not in MODEL_FAMILY_STAGES:
        raise ModelFamilyError(f"Unknown model-family stage: {stage}")


def _mapping(value: object, *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ModelFamilyError(f"{name} must be a mapping.")
    return cast(dict[str, Any], value)


def _optional_mapping(value: object, *, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return _mapping(value, name=name)


def _required_list(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ModelFamilyError(f"{key} must be a list.")
    return value


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ModelFamilyError(f"{key} is required.")
    return value.strip()


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ModelFamilyError("limitations must be a list of strings.")
    return [item.strip() for item in value if item.strip()]


def _size_metadata(value: object) -> dict[str, dict[str, Any]]:
    sizes = _required_sequence_mappings(value, name="sizes")
    by_size: dict[str, dict[str, Any]] = {}
    for item in sizes:
        size = _required_str(item, "size")
        if size not in MODEL_FAMILY_SIZES:
            raise ModelFamilyError(f"Unknown size metadata row: {size}")
        by_size[size] = {
            "base_model": _required_str(item, "base_model"),
            "upstream_license": _required_str(item, "upstream_license"),
            "hub_license": _required_str(item, "hub_license"),
            "license_name": _optional_str(item.get("license_name")),
            "license_link": _optional_str(item.get("license_link")),
        }
    missing = sorted(set(MODEL_FAMILY_SIZES) - set(by_size))
    if missing:
        raise ModelFamilyError("sizes metadata missing: " + ", ".join(missing))
    return by_size


def _stage_gates(value: object) -> dict[str, dict[str, Any]]:
    gates = _required_sequence_mappings(value, name="stage_gates")
    by_stage: dict[str, dict[str, Any]] = {}
    for item in gates:
        stage = _required_str(item, "stage")
        if stage not in MODEL_FAMILY_STAGES:
            raise ModelFamilyError(f"Unknown stage gate row: {stage}")
        by_stage[stage] = {
            "metric": _required_str(item, "metric"),
            "predecessor_metric": _optional_str(item.get("predecessor_metric")),
            "min_absolute_f1_gain": float(item.get("min_absolute_f1_gain", 0.0)),
            "parse_success_min": float(item.get("parse_success_min", 0.99)),
            "schema_case_error_max": int(item.get("schema_case_error_max", 0)),
        }
    missing = sorted(set(MODEL_FAMILY_STAGES) - set(by_stage))
    if missing:
        raise ModelFamilyError("stage_gates missing: " + ", ".join(missing))
    return by_stage


def _required_sequence_mappings(value: object, *, name: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ModelFamilyError(f"{name} must be a list.")
    return [_mapping(item, name=f"{name}[]") for item in value]
