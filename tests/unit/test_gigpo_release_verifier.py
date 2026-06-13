"""Unit tests for GiGPO release verification evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts.model.verify_gigpo_release import GigpoReleaseVerificationError, verify_gigpo_release


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
    metrics_path = tmp_path / "training_metrics.json"
    metrics_path.write_text(json.dumps(metrics), encoding="utf-8")
    readme = tmp_path / "README.md"
    readme.write_text("# DataForge-0.5B-GiGPO\n", encoding="utf-8")
    diagnostics = tmp_path / "eval_diagnostics.json"
    diagnostics.write_text(
        json.dumps(
            {"schema_version": "dataforge_gigpo_eval_diagnostics_v1", "failure_samples": []}
        ),
        encoding="utf-8",
    )
    return {
        "training_metrics.json": metrics_path,
        "README.md": readme,
        "eval_diagnostics.json": diagnostics,
    }


def _metrics() -> dict[str, object]:
    return {
        "model_name": "DataForge-0.5B-GiGPO",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "grpo_model": "Praneshrajan15/DataForge-0.5B-GRPO",
        "dataset_repo": "Praneshrajan15/dataforge-sft-trajectories",
        "dataset_sha": "dataset-sha",
        "source_git_commit": "abc1234",
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "gpu_hours": 8.25,
        "attempted_steps": 500,
        "grpo_f1": 0.46,
        "gigpo_f1": 0.49,
        "f1_delta": 0.03,
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "failure_samples": [],
        "acceptance_gate_passed": True,
    }


def test_gigpo_release_verifier_accepts_complete_gated_release(tmp_path: Path) -> None:
    files = _files(tmp_path, metrics=_metrics())

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    evidence = verify_gigpo_release(
        model_repo="Praneshrajan15/DataForge-0.5B-GiGPO",
        api=_FakeApi(
            {
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
                "eval_diagnostics.json",
            }
        ),
        downloader=downloader,
    )

    assert evidence.release_status == "quality_improved_verified"
    assert evidence.metrics["f1_delta"] == 0.03


def test_gigpo_release_verifier_rejects_failed_gate(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["f1_delta"] = 0.01
    metrics["gigpo_f1"] = 0.47
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

    with pytest.raises(GigpoReleaseVerificationError, match="acceptance gate"):
        verify_gigpo_release(
            model_repo="Praneshrajan15/DataForge-0.5B-GiGPO",
            api=_FakeApi(
                {
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                    "eval_diagnostics.json",
                }
            ),
            downloader=downloader,
        )


def test_gigpo_release_verifier_rejects_wrong_predecessor(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["grpo_model"] = "Praneshrajan15/DataForge-0.5B-GRPO"
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GigpoReleaseVerificationError, match="predecessor"):
        verify_gigpo_release(
            model_repo="Praneshrajan15/DataForge-1.5B-GiGPO",
            api=_FakeApi(
                {
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                    "eval_diagnostics.json",
                }
            ),
            downloader=downloader,
        )


def test_gigpo_release_verifier_uses_qwen_research_license_for_3b(tmp_path: Path) -> None:
    metrics = _metrics()
    metrics["model_name"] = "DataForge-3B-GiGPO"
    metrics["model_license"] = "apache-2.0"
    metrics["base_model"] = "Qwen/Qwen2.5-3B-Instruct"
    metrics["grpo_model"] = "Praneshrajan15/DataForge-3B-GRPO"
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[filename])

    with pytest.raises(GigpoReleaseVerificationError, match="qwen-research"):
        verify_gigpo_release(
            model_repo="Praneshrajan15/DataForge-3B-GiGPO",
            api=_FakeApi(
                {
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                    "eval_diagnostics.json",
                }
            ),
            downloader=downloader,
        )


def test_gigpo_release_verifier_bounds_metric_failure_samples(tmp_path: Path) -> None:
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

    with pytest.raises(GigpoReleaseVerificationError, match="bounded"):
        verify_gigpo_release(
            model_repo="Praneshrajan15/DataForge-0.5B-GiGPO",
            api=_FakeApi(
                {
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                    "eval_diagnostics.json",
                }
            ),
            downloader=downloader,
        )
