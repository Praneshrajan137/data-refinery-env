"""Prepare private Kaggle bundles for the DataForge 0.5B SFT-v5 candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET_DIR = ROOT / "training" / "kaggle_sft_v5_handoff"
DEFAULT_KERNEL_DIR = ROOT / "training" / "kaggle_sft_v5_kernel"
DEFAULT_TRAJECTORY = ROOT / "data" / "sft_traj" / "expert_v5_repair_curriculum.jsonl"
DEFAULT_SPLIT_MANIFEST = ROOT / "data" / "sft_traj" / "split_manifest_v4_candidate.json"
DEFAULT_CURRICULUM_REPORT = ROOT / "eval" / "results" / "sft_v5_repair_curriculum_report.json"
DEFAULT_SFT_CONFIG = ROOT / "training" / "configs" / "sft_05b_v5.yaml"
SFT_SCRIPT = ROOT / "scripts" / "remote" / "kaggle_sft_v5_candidate.py"
DATASET_ID = "praneshrajan15/dataforge-sft-v5-handoff"
KERNEL_ID = "praneshrajan15/dataforge-0-5b-sft-v5-candidate"
KERNEL_CODE_FILE = "dataforge-0-5b-sft-v5-candidate.py"
SOURCE_ROOTS = (
    "dataforge",
    "scripts",
    "training",
    "pyproject.toml",
    "README.md",
)
SOURCE_EXCLUDES = (
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "training/kaggle_grpo_smoke_handoff",
    "training/kaggle_grpo_smoke_kernel",
    "training/kaggle_grpo_candidate_handoff",
    "training/kaggle_grpo_candidate_kernel",
    "training/kaggle_sft_v5_handoff",
    "training/kaggle_sft_v5_kernel",
    "training/kaggle_sft_v6_handoff",
    "training/kaggle_sft_v6_kernel",
    "training/kaggle_sft_v7_handoff",
    "training/kaggle_sft_v7_kernel",
    "training/kaggle_dataset_v3.zip",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_file(source: Path, target: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _remove_readonly(func: Any, path: str, _exc_info: Any) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)


def _reset_dir(path: Path) -> None:
    if not path.exists():
        return
    resolved = path.resolve()
    root = ROOT.resolve()
    if root not in resolved.parents:
        raise RuntimeError(f"Refusing to remove directory outside project root: {path}")
    shutil.rmtree(path, onexc=_remove_readonly)


def _is_excluded(path: Path) -> bool:
    relative = path.relative_to(ROOT).as_posix()
    parts = set(Path(relative).parts)
    if any(part in parts for part in SOURCE_EXCLUDES):
        return True
    return any(
        relative == exclude or relative.startswith(f"{exclude}/") for exclude in SOURCE_EXCLUDES
    )


def _write_source_zip(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for root_name in SOURCE_ROOTS:
            root = ROOT / root_name
            if root.is_file():
                archive.write(root, root.relative_to(ROOT).as_posix())
                written += 1
                continue
            for candidate in root.rglob("*"):
                if candidate.is_dir() or _is_excluded(candidate):
                    continue
                archive.write(candidate, candidate.relative_to(ROOT).as_posix())
                written += 1
    return written


def _config_handoff_names(config_path: Path) -> tuple[str, str]:
    if not config_path.exists():
        raise FileNotFoundError(config_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{config_path} must contain a YAML mapping.")
    if payload.get("schema_version") != "sft_05b_v5":
        raise ValueError("SFT-v5 handoff requires schema_version=sft_05b_v5.")
    repos = payload.get("repos", {})
    if not isinstance(repos, dict):
        repos = {}
    trajectory_name = str(repos.get("trajectory_filename", "expert_v5_repair_curriculum.jsonl"))
    split_manifest_name = str(repos.get("split_manifest_filename", "split_manifest_v4.json"))
    if trajectory_name != "expert_v5_repair_curriculum.jsonl":
        raise ValueError(f"Unsupported SFT-v5 trajectory handoff file: {trajectory_name}")
    if split_manifest_name != "split_manifest_v4.json":
        raise ValueError(f"Unsupported SFT-v5 split manifest handoff file: {split_manifest_name}")
    return trajectory_name, split_manifest_name


def build_bundles(
    *,
    dataset_dir: Path = DEFAULT_DATASET_DIR,
    kernel_dir: Path = DEFAULT_KERNEL_DIR,
    trajectory: Path = DEFAULT_TRAJECTORY,
    split_manifest: Path = DEFAULT_SPLIT_MANIFEST,
    curriculum_report: Path = DEFAULT_CURRICULUM_REPORT,
    sft_config: Path = DEFAULT_SFT_CONFIG,
) -> dict[str, Any]:
    """Build private Kaggle handoff folders for the gated SFT-v5 candidate."""
    _reset_dir(dataset_dir)
    _reset_dir(kernel_dir)
    dataset_dir.mkdir(parents=True)
    kernel_dir.mkdir(parents=True)

    trajectory_name, split_manifest_name = _config_handoff_names(sft_config)
    _copy_file(trajectory, dataset_dir / trajectory_name)
    _copy_file(split_manifest, dataset_dir / split_manifest_name)
    _copy_file(curriculum_report, dataset_dir / "sft_v5_repair_curriculum_report.json")
    _copy_file(sft_config, dataset_dir / "sft_05b_v5.yaml")
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
        "schema_version": "dataforge_kaggle_sft_v5_handoff_v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "purpose": "0.5B-SFT-v5 private repair-curriculum candidate with strict held-out eval",
        "dataset_id": DATASET_ID,
        "kernel_id": KERNEL_ID,
        "trajectory_file": trajectory_name,
        "split_manifest_file": split_manifest_name,
        "config_file": "sft_05b_v5.yaml",
        "curriculum_report_file": "sft_v5_repair_curriculum_report.json",
        "source_file_count": source_file_count,
        "files": files,
        "private_candidate_upload_allowed_after_gate": True,
        "public_claim_update_allowed": False,
    }
    (dataset_dir / "sft_v5_manifest.json").write_text(
        json.dumps(candidate_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (dataset_dir / "dataset-metadata.json").write_text(
        json.dumps(
            {
                "title": "DataForge SFT v5 Handoff",
                "id": DATASET_ID,
                "licenses": [{"name": "Apache 2.0"}],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    _copy_file(SFT_SCRIPT, kernel_dir / KERNEL_CODE_FILE)
    (kernel_dir / "kernel-metadata.json").write_text(
        json.dumps(
            {
                "id": KERNEL_ID,
                "title": "DataForge 0.5B SFT v5 Candidate",
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
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
