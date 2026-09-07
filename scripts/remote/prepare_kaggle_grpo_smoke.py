"""Prepare private Kaggle dataset/kernel bundles for the GRPO smoke milestone."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET_DIR = ROOT / "archive" / "training" / "kaggle_grpo_smoke_handoff"
DEFAULT_KERNEL_DIR = ROOT / "archive" / "training" / "kaggle_grpo_smoke_kernel"
DEFAULT_TRAJECTORY = ROOT / "data" / "sft_traj" / "expert_v4_candidate.jsonl"
DEFAULT_SPLIT_MANIFEST = ROOT / "data" / "sft_traj" / "split_manifest_v4_candidate.json"
DEFAULT_READINESS_REPORT = ROOT / "eval" / "results" / "grpo_readiness_05b_candidate.json"
DEFAULT_GRPO_CONFIG = ROOT / "archive" / "training" / "configs" / "grpo_05b.yaml"
DEFAULT_SFT_PREDECESSOR_REPORT = ROOT / "eval" / "results" / "sft_v7_candidate_eval_report.json"
SFT_PREDECESSOR_REPORTS = {
    "grpo_05b_v3": (
        ROOT / "eval" / "results" / "sft_v7_candidate_eval_report.json",
        "sft_v7_candidate_eval_report.json",
        "SFT-v7",
    ),
    "grpo_05b_v4": (
        ROOT / "eval" / "results" / "sft_v9_candidate_eval_report.json",
        "sft_v9_candidate_eval_report.json",
        "SFT-v9",
    ),
}
SMOKE_SCRIPT = ROOT / "scripts" / "remote" / "kaggle_grpo_smoke.py"
DATASET_ID = "praneshrajan15/dataforge-grpo-smoke-handoff"
KERNEL_ID = "praneshrajan15/dataforge-0-5b-grpo-smoke"
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
    "archive/training/kaggle_grpo_smoke_handoff",
    "archive/training/kaggle_grpo_smoke_kernel",
    "archive/training/kaggle_grpo_candidate_handoff",
    "archive/training/kaggle_grpo_candidate_kernel",
    "archive/training/kaggle_sft_v5_handoff",
    "archive/training/kaggle_sft_v5_kernel",
    "archive/training/kaggle_sft_v6_handoff",
    "archive/training/kaggle_sft_v6_kernel",
    "archive/training/kaggle_sft_v7_handoff",
    "archive/training/kaggle_sft_v7_kernel",
    "archive/training/kaggle_sft_v8_handoff",
    "archive/training/kaggle_sft_v8_kernel",
    "archive/training/kaggle_sft_v9_handoff",
    "archive/training/kaggle_sft_v9_kernel",
    "archive/training/kaggle_dataset_v3.zip",
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
    readiness = payload.get("readiness", {})
    if not isinstance(readiness, dict):
        readiness = {}
    trajectory_name = str(readiness.get("trajectory_filename", "expert_v4.jsonl"))
    split_manifest_name = str(readiness.get("split_manifest_filename", "split_manifest_v4.json"))
    if trajectory_name not in {
        "expert_v4.jsonl",
        "expert_v5_repair_curriculum.jsonl",
        "expert_v6_contract_minimal.jsonl",
        "expert_v7_parse_latch.jsonl",
        "expert_v8_schema_distill.jsonl",
        "expert_v9_action_envelope.jsonl",
    }:
        raise ValueError(f"Unsupported GRPO trajectory handoff file: {trajectory_name}")
    if split_manifest_name != "split_manifest_v4.json":
        raise ValueError(f"Unsupported GRPO split manifest handoff file: {split_manifest_name}")
    return trajectory_name, split_manifest_name


def _schema_version(config_path: Path) -> str:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{config_path} must contain a YAML mapping.")
    return str(payload.get("schema_version", ""))


def _copy_predecessor_report(
    *,
    config_path: Path,
    report_path: Path,
    dataset_dir: Path,
) -> str | None:
    schema_version = _schema_version(config_path)
    predecessor = SFT_PREDECESSOR_REPORTS.get(schema_version)
    if predecessor is None:
        return None
    default_report_path, report_name, label = predecessor
    if report_path == DEFAULT_SFT_PREDECESSOR_REPORT:
        report_path = default_report_path
    if not report_path.exists():
        raise FileNotFoundError(
            f"{schema_version} handoff requires a {label} eval report before launch: {report_path}"
        )
    _copy_file(report_path, dataset_dir / report_name)
    return report_name


def build_bundles(
    *,
    dataset_dir: Path = DEFAULT_DATASET_DIR,
    kernel_dir: Path = DEFAULT_KERNEL_DIR,
    trajectory: Path = DEFAULT_TRAJECTORY,
    split_manifest: Path = DEFAULT_SPLIT_MANIFEST,
    readiness_report: Path = DEFAULT_READINESS_REPORT,
    grpo_config: Path = DEFAULT_GRPO_CONFIG,
    sft_predecessor_report: Path = DEFAULT_SFT_PREDECESSOR_REPORT,
) -> dict[str, Any]:
    """Build the private Kaggle handoff dataset and kernel folders."""
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    if kernel_dir.exists():
        shutil.rmtree(kernel_dir)
    dataset_dir.mkdir(parents=True)
    kernel_dir.mkdir(parents=True)

    trajectory_name, split_manifest_name = _config_handoff_names(grpo_config)
    _copy_file(trajectory, dataset_dir / trajectory_name)
    _copy_file(split_manifest, dataset_dir / split_manifest_name)
    _copy_file(readiness_report, dataset_dir / "grpo_readiness_05b_candidate.json")
    _copy_file(grpo_config, dataset_dir / "grpo_05b.yaml")
    predecessor_report_file = _copy_predecessor_report(
        config_path=grpo_config,
        report_path=sft_predecessor_report,
        dataset_dir=dataset_dir,
    )
    source_file_count = _write_source_zip(dataset_dir / "source.zip")

    files = {
        path.name: {
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(dataset_dir.iterdir())
        if path.is_file()
    }
    smoke_manifest = {
        "schema_version": "dataforge_kaggle_grpo_smoke_handoff_v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "purpose": "0.5B-GRPO no-upload Kaggle smoke run",
        "dataset_id": DATASET_ID,
        "kernel_id": KERNEL_ID,
        "trajectory_file": trajectory_name,
        "split_manifest_file": split_manifest_name,
        "config_file": "grpo_05b.yaml",
        "readiness_report_file": "grpo_readiness_05b_candidate.json",
        "sft_predecessor_report_file": predecessor_report_file,
        "source_file_count": source_file_count,
        "files": files,
        "model_upload_allowed": False,
        "public_claim_update_allowed": False,
    }
    (dataset_dir / "smoke_manifest.json").write_text(
        json.dumps(smoke_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (dataset_dir / "dataset-metadata.json").write_text(
        json.dumps(
            {
                "title": "DataForge GRPO Smoke Handoff",
                "id": DATASET_ID,
                "licenses": [{"name": "Apache 2.0"}],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    _copy_file(SMOKE_SCRIPT, kernel_dir / "dataforge_grpo_smoke.py")
    (kernel_dir / "kernel-metadata.json").write_text(
        json.dumps(
            {
                "id": KERNEL_ID,
                "title": "DataForge 0.5B GRPO Smoke",
                "code_file": "dataforge_grpo_smoke.py",
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
        "files": smoke_manifest["files"],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--kernel-dir", type=Path, default=DEFAULT_KERNEL_DIR)
    parser.add_argument("--trajectory", type=Path, default=DEFAULT_TRAJECTORY)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT_MANIFEST)
    parser.add_argument("--readiness-report", type=Path, default=DEFAULT_READINESS_REPORT)
    parser.add_argument("--grpo-config", type=Path, default=DEFAULT_GRPO_CONFIG)
    parser.add_argument(
        "--sft-predecessor-report", type=Path, default=DEFAULT_SFT_PREDECESSOR_REPORT
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = build_bundles(
        dataset_dir=args.dataset_dir,
        kernel_dir=args.kernel_dir,
        trajectory=args.trajectory,
        split_manifest=args.split_manifest,
        readiness_report=args.readiness_report,
        grpo_config=args.grpo_config,
        sft_predecessor_report=args.sft_predecessor_report,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
