"""Prepare private Kaggle bundles for the DataForge 0.5B SFT-v8 candidate."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.remote.prepare_kaggle_sft_v6_candidate import (
    DEFAULT_STAGE,
    SFT_SCRIPT,
    VALID_STAGES,
    _copy_file,
    _reset_dir,
    _sha256,
    _write_kernel_script,
    _write_source_zip,
)

DEFAULT_DATASET_DIR = ROOT / "training" / "kaggle_sft_v8_handoff"
DEFAULT_KERNEL_DIR = ROOT / "training" / "kaggle_sft_v8_kernel"
DEFAULT_TRAJECTORY = ROOT / "data" / "sft_traj" / "expert_v8_schema_distill.jsonl"
DEFAULT_SPLIT_MANIFEST = ROOT / "data" / "sft_traj" / "split_manifest_v4_candidate.json"
DEFAULT_CURRICULUM_REPORT = (
    ROOT / "eval" / "results" / "sft_v8_schema_distill_curriculum_report.json"
)
DEFAULT_SFT_CONFIG = ROOT / "training" / "configs" / "sft_05b_v8.yaml"
DATASET_ID = "praneshrajan15/dataforge-sft-v8-handoff"
KERNEL_ID = "praneshrajan15/dataforge-0-5b-sft-v8-candidate"
KERNEL_CODE_FILE = "dataforge-0-5b-sft-v8-candidate.py"


def _config_handoff_names(config_path: Path) -> tuple[str, str]:
    if not config_path.exists():
        raise FileNotFoundError(config_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{config_path} must contain a YAML mapping.")
    if payload.get("schema_version") != "sft_05b_v8":
        raise ValueError("SFT-v8 handoff requires schema_version=sft_05b_v8.")
    repos = payload.get("repos", {})
    if not isinstance(repos, dict):
        repos = {}
    trajectory_name = str(repos.get("trajectory_filename", "expert_v8_schema_distill.jsonl"))
    split_manifest_name = str(repos.get("split_manifest_filename", "split_manifest_v4.json"))
    if trajectory_name != "expert_v8_schema_distill.jsonl":
        raise ValueError(f"Unsupported SFT-v8 trajectory handoff file: {trajectory_name}")
    if split_manifest_name != "split_manifest_v4.json":
        raise ValueError(f"Unsupported SFT-v8 split manifest handoff file: {split_manifest_name}")
    return trajectory_name, split_manifest_name


def build_bundles(
    *,
    dataset_dir: Path = DEFAULT_DATASET_DIR,
    kernel_dir: Path = DEFAULT_KERNEL_DIR,
    trajectory: Path = DEFAULT_TRAJECTORY,
    split_manifest: Path = DEFAULT_SPLIT_MANIFEST,
    curriculum_report: Path = DEFAULT_CURRICULUM_REPORT,
    sft_config: Path = DEFAULT_SFT_CONFIG,
    default_stage: str = DEFAULT_STAGE,
) -> dict[str, Any]:
    """Build private Kaggle handoff folders for the gated SFT-v8 candidate."""
    _reset_dir(dataset_dir)
    _reset_dir(kernel_dir)
    dataset_dir.mkdir(parents=True)
    kernel_dir.mkdir(parents=True)

    trajectory_name, split_manifest_name = _config_handoff_names(sft_config)
    _copy_file(trajectory, dataset_dir / trajectory_name)
    _copy_file(split_manifest, dataset_dir / split_manifest_name)
    _copy_file(curriculum_report, dataset_dir / "sft_v8_schema_distill_curriculum_report.json")
    _copy_file(sft_config, dataset_dir / "sft_05b_v8.yaml")
    source_file_count = _write_source_zip(dataset_dir / "source.zip")

    files = {
        path.name: {
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(dataset_dir.iterdir())
        if path.is_file()
    }
    candidate_manifest = {
        "schema_version": "dataforge_kaggle_sft_v8_handoff_v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "purpose": "0.5B-SFT-v8 private schema-distill prompt-completion candidate with strict held-out eval",
        "dataset_id": DATASET_ID,
        "kernel_id": KERNEL_ID,
        "trajectory_file": trajectory_name,
        "split_manifest_file": split_manifest_name,
        "config_file": "sft_05b_v8.yaml",
        "curriculum_report_file": "sft_v8_schema_distill_curriculum_report.json",
        "source_file_count": source_file_count,
        "default_stage": default_stage,
        "files": files,
        "training_format": "prompt_completion",
        "completion_only_loss_required": True,
        "private_candidate_upload_allowed_after_gate": True,
        "public_claim_update_allowed": False,
    }
    (dataset_dir / "sft_v8_manifest.json").write_text(
        json.dumps(candidate_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (dataset_dir / "dataset-metadata.json").write_text(
        json.dumps(
            {
                "title": "DataForge SFT v8 Handoff",
                "id": DATASET_ID,
                "licenses": [{"name": "Apache 2.0"}],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    _write_kernel_script(
        SFT_SCRIPT,
        kernel_dir / KERNEL_CODE_FILE,
        default_stage=default_stage,
        default_version="v8",
    )
    (kernel_dir / "kernel-metadata.json").write_text(
        json.dumps(
            {
                "id": KERNEL_ID,
                "title": "DataForge 0.5B SFT v8 Candidate",
                "code_file": KERNEL_CODE_FILE,
                "language": "python",
                "kernel_type": "script",
                "is_private": "true",
                "enable_gpu": "true",
                "enable_tpu": "false",
                "enable_internet": "true",
                "machine_shape": "NvidiaTeslaT4",
                "dataset_sources": [DATASET_ID],
                "competition_sources": [],
                "kernel_sources": [],
                "model_sources": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return {
        "dataset_dir": str(dataset_dir),
        "kernel_dir": str(kernel_dir),
        "dataset_id": DATASET_ID,
        "kernel_id": KERNEL_ID,
        "source_file_count": source_file_count,
        "default_stage": default_stage,
        "files": candidate_manifest["files"],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--kernel-dir", type=Path, default=DEFAULT_KERNEL_DIR)
    parser.add_argument("--trajectory", type=Path, default=DEFAULT_TRAJECTORY)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT_MANIFEST)
    parser.add_argument("--curriculum-report", type=Path, default=DEFAULT_CURRICULUM_REPORT)
    parser.add_argument("--sft-config", type=Path, default=DEFAULT_SFT_CONFIG)
    parser.add_argument(
        "--default-stage",
        choices=VALID_STAGES,
        default=DEFAULT_STAGE,
        help="Default DATAFORGE_SFT_STAGE baked into the generated Kaggle script.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = build_bundles(
        dataset_dir=args.dataset_dir,
        kernel_dir=args.kernel_dir,
        trajectory=args.trajectory,
        split_manifest=args.split_manifest,
        curriculum_report=args.curriculum_report,
        sft_config=args.sft_config,
        default_stage=args.default_stage,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
