"""Unit tests for GRPO release verification evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts.model.verify_grpo_release import (
    GrpoReleaseVerificationError,
    verify_grpo_release,
    verify_local_grpo_artifact_dir,
)


@dataclass(frozen=True)
class _Sibling:
    rfilename: str


@dataclass(frozen=True)
class _RepoInfo:
    sha: str
    siblings: list[_Sibling]


class _FakeApi:
    def __init__(self, files: set[str]) -> None:
        self.files = files

    def repo_info(
        self,
        repo_id: str,
        *,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> _RepoInfo:
        return _RepoInfo(sha="model-sha", siblings=[_Sibling(name) for name in self.files])


def _files(tmp_path: Path, *, metrics: dict[str, object]) -> dict[str, Path]:
    manifest = tmp_path / "eval_task_manifest.json"
    manifest.write_text(json.dumps(_manifest()), encoding="utf-8")
    metrics["eval_task_manifest_sha256"] = _sha256(manifest)
    metrics_path = tmp_path / "training_metrics.json"
    metrics_path.write_text(json.dumps(metrics), encoding="utf-8")
    readme = tmp_path / "README.md"
    readme.write_text("# DataForge-0.5B-GRPO\n", encoding="utf-8")
    diagnostics = tmp_path / "eval_diagnostics.json"
    diagnostics.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_grpo_eval_diagnostics_v1",
                "sft_eval": {"dataset_f1": {"hospital": 0.42}},
                "grpo_eval": {"dataset_f1": {"hospital": 0.46}},
                "failure_samples": [],
            }
        ),
        encoding="utf-8",
    )
    return {
        "training_metrics.json": metrics_path,
        "README.md": readme,
        "eval_diagnostics.json": diagnostics,
        "eval_task_manifest.json": manifest,
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()


def _manifest() -> dict[str, object]:
    return {
        "schema_version": "dataforge_grpo_eval_task_manifest_v1",
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "heldout_tasks": 1,
        "source_audit": {"ok": True, "status": "pass", "datasets": [], "blockers": []},
        "tasks": [
            {
                "task_id": "task-1",
                "dataset": "hospital",
                "prompt_hash": "a" * 64,
                "allowed_columns": ["Name"],
                "valid_rows": [0],
                "truth_cell_count": 1,
                "truth_hash": "b" * 64,
                "source": {"dirty_sha256": "c" * 64, "clean_sha256": "d" * 64},
            }
        ],
    }


def _metrics() -> dict[str, object]:
    return {
        "model_name": "DataForge-0.5B-GRPO",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "sft_model": "Praneshrajan15/DataForge-0.5B-SFT",
        "dataset_repo": "Praneshrajan15/dataforge-sft-trajectories",
        "dataset_sha": "dataset-sha",
        "source_git_commit": "abc1234",
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "gpu_hours": 5.75,
        "attempted_steps": 500,
        "sft_f1": 0.42,
        "grpo_f1": 0.46,
        "f1_delta": 0.04,
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "failure_samples": [],
        "acceptance_gate_passed": True,
        "training_stage": "candidate",
        "smoke_report_sha256": "f" * 64,
        "eval_task_manifest_sha256": "e" * 64,
    }


def _required_files() -> set[str]:
    return {
        "README.md",
        "config.json",
        "model.safetensors",
        "tokenizer.json",
        "tokenizer_config.json",
        "training_metrics.json",
        "eval_diagnostics.json",
        "eval_task_manifest.json",
    }


def test_grpo_release_verifier_accepts_complete_gated_release(tmp_path: Path) -> None:
    files = _files(tmp_path, metrics=_metrics())

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    evidence = verify_grpo_release(
        model_repo="Praneshrajan15/DataForge-0.5B-GRPO",
        api=_FakeApi(_required_files()),
        downloader=downloader,
    )

    assert evidence.release_status == "quality_improved_verified"
    assert evidence.metrics["f1_delta"] == 0.04


def test_grpo_release_verifier_rejects_failed_gate(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["f1_delta"] = 0.01
    metrics["grpo_f1"] = 0.43
    metrics["acceptance_gate_passed"] = False
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GrpoReleaseVerificationError, match="acceptance gate"):
        verify_grpo_release(
            model_repo="Praneshrajan15/DataForge-0.5B-GRPO",
            api=_FakeApi(_required_files()),
            downloader=downloader,
        )


def test_grpo_release_verifier_rejects_wrong_predecessor(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["sft_model"] = "Praneshrajan15/DataForge-0.5B-SFT"
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GrpoReleaseVerificationError, match="predecessor"):
        verify_grpo_release(
            model_repo="Praneshrajan15/DataForge-1.5B-GRPO",
            api=_FakeApi(_required_files()),
            downloader=downloader,
        )


def test_grpo_release_verifier_uses_qwen_research_license_for_3b(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["model_name"] = "DataForge-3B-GRPO"
    metrics["model_license"] = "apache-2.0"
    metrics["base_model"] = "Qwen/Qwen2.5-3B-Instruct"
    metrics["sft_model"] = "Praneshrajan15/DataForge-3B-SFT"
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GrpoReleaseVerificationError, match="qwen-research"):
        verify_grpo_release(
            model_repo="Praneshrajan15/DataForge-3B-GRPO",
            api=_FakeApi(_required_files()),
            downloader=downloader,
        )


def test_grpo_release_verifier_bounds_metric_failure_samples(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["failure_samples"] = [{"i": i} for i in range(26)]
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GrpoReleaseVerificationError, match="bounded"):
        verify_grpo_release(
            model_repo="Praneshrajan15/DataForge-0.5B-GRPO",
            api=_FakeApi(_required_files()),
            downloader=downloader,
        )


def test_local_grpo_artifact_verifier_checks_manifest_hash_and_files(tmp_path: Path) -> None:
    metrics = _metrics()
    files = _files(tmp_path, metrics=metrics)
    for name in ("config.json", "tokenizer.json", "tokenizer_config.json"):
        (tmp_path / name).write_text("{}", encoding="utf-8")
    (tmp_path / "model.safetensors").write_bytes(b"model")
    (tmp_path / "training_metrics.json").write_text(json.dumps(metrics), encoding="utf-8")

    evidence = verify_local_grpo_artifact_dir(tmp_path)

    assert evidence.release_status == "quality_improved_verified"
    assert "eval_task_manifest.json" in evidence.model.files
    assert files["eval_task_manifest.json"].exists()


def test_release_verifier_rejects_manifest_with_hidden_labels(tmp_path: Path) -> None:
    metrics = _metrics()
    files = _files(tmp_path, metrics=metrics)
    manifest = _manifest()
    manifest["tasks"][0]["ground_truth"] = [{"row": 0, "column": "Name", "clean_value": "Alice"}]
    files["eval_task_manifest.json"].write_text(json.dumps(manifest), encoding="utf-8")
    metrics["eval_task_manifest_sha256"] = _sha256(files["eval_task_manifest.json"])
    files["training_metrics.json"].write_text(json.dumps(metrics), encoding="utf-8")

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GrpoReleaseVerificationError, match="hidden labels"):
        verify_grpo_release(
            model_repo="Praneshrajan15/DataForge-0.5B-GRPO",
            api=_FakeApi(_required_files()),
            downloader=downloader,
        )
