"""Unit tests for the Hugging Face SFT release verifier."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

import scripts.model.publish_dataset_readme as dataset_readme_publisher
from scripts.model.publish_dataset_readme import publish_dataset_readme
from scripts.model.verify_sft_release import (
    DEFAULT_DATASET_REPO,
    DEFAULT_MODEL_REPO,
    ReleaseVerificationError,
    verify_sft_release,
)


@dataclass(frozen=True)
class _Sibling:
    rfilename: str


@dataclass(frozen=True)
class _RepoInfo:
    sha: str
    siblings: list[_Sibling]


class _FakeApi:
    def __init__(self, *, model_files: set[str], dataset_files: set[str]) -> None:
        self.model_files = model_files
        self.dataset_files = dataset_files

    def repo_info(
        self,
        repo_id: str,
        *,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> _RepoInfo:
        files = self.model_files if repo_type == "model" else self.dataset_files
        return _RepoInfo(
            sha=revision or f"{repo_type}-sha", siblings=[_Sibling(name) for name in files]
        )


def _files(
    tmp_path: Path, *, metrics: dict[str, object] | None = None
) -> dict[tuple[str, str], Path]:
    metrics_payload = metrics or {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "llama-3.3-70b-versatile",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0,
        "sft_f1": 0.0,
        "repo_id": DEFAULT_MODEL_REPO,
    }
    model_metrics = tmp_path / "training_metrics.json"
    model_metrics.write_text(json.dumps(metrics_payload), encoding="utf-8")
    model_readme = tmp_path / "model_README.md"
    model_readme.write_text("# DataForge-0.5B-SFT\n\nHeld-out F1: 0.0\n", encoding="utf-8")
    dataset_readme = tmp_path / "dataset_README.md"
    dataset_readme.write_text(
        "# DataForge SFT Trajectories\n\nSchema: expert_v1.\n",
        encoding="utf-8",
    )
    jsonl = tmp_path / "expert_v1.jsonl"
    jsonl.write_text(
        "".join(json.dumps({"schema_version": "expert_v1", "i": i}) + "\n" for i in range(32)),
        encoding="utf-8",
    )
    split_manifest = tmp_path / "split_manifest.json"
    split_manifest.write_text(
        json.dumps({"schema_version": "split_manifest_v1", "datasets": []}),
        encoding="utf-8",
    )
    return {
        ("model", "training_metrics.json"): model_metrics,
        ("model", "README.md"): model_readme,
        ("dataset", "README.md"): dataset_readme,
        ("dataset", "expert_v1.jsonl"): jsonl,
        ("dataset", "split_manifest.json"): split_manifest,
    }


def test_release_verifier_accepts_complete_smoke_release(tmp_path: Path) -> None:
    files = _files(tmp_path)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
            },
            dataset_files={
                "README.md",
                "expert_v1.jsonl",
                "split_manifest.json",
                "sft_05b.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
    )

    assert evidence.model.sha == "model-sha"
    assert evidence.dataset.sha == "dataset-sha"
    assert evidence.dataset_records == 32
    assert evidence.metrics["sft_f1"] == 0.0
    assert evidence.quality_milestone is False
    assert evidence.release_status == "diagnostic_complete_no_gain"
    assert "sft_f1>base_f1" in evidence.quality_gate_failures


def test_release_verifier_accepts_archived_kaggle_dataset_metric(tmp_path: Path) -> None:
    metrics = {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "clean-diff-v1",
        "dataset_repo": "kaggle://praneshrajan15/dataforge-sft-v3-handoff",
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0,
        "sft_f1": 0.0,
        "repo_id": DEFAULT_MODEL_REPO,
        "dataset_sha": "bf99494539c8346676488bb982cd72d3ce9fea494fc63695ab08cb3fb1c98ac3",
    }
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
            },
            dataset_files={
                "README.md",
                "expert_v1.jsonl",
                "split_manifest.json",
                "sft_05b.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
    )

    assert evidence.metrics["dataset_repo"].startswith("kaggle://")
    assert evidence.dataset_sha_metric_checked is False


def test_release_verifier_detects_v4_dataset_contract(tmp_path: Path) -> None:
    files = _files(tmp_path)
    v4_jsonl = tmp_path / "expert_v4.jsonl"
    v4_jsonl.write_text(
        "".join(json.dumps({"schema_version": "expert_v4", "i": i}) + "\n" for i in range(32)),
        encoding="utf-8",
    )
    v4_split_manifest = tmp_path / "split_manifest_v4.json"
    v4_split_manifest.write_text(
        json.dumps({"schema_version": "split_manifest_v1", "datasets": []}),
        encoding="utf-8",
    )
    files[("dataset", "expert_v4.jsonl")] = v4_jsonl
    files[("dataset", "split_manifest_v4.json")] = v4_split_manifest

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
            },
            dataset_files={
                "README.md",
                "expert_v4.jsonl",
                "split_manifest_v4.json",
                "sft_05b_v4.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
    )

    assert evidence.trajectory_filename == "expert_v4.jsonl"
    assert evidence.split_manifest_filename == "split_manifest_v4.json"
    assert evidence.dataset_records == 32


def test_release_verifier_keeps_legacy_diagnostic_metrics_on_v3_contract(
    tmp_path: Path,
) -> None:
    metrics = {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "clean-diff-v1",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0094,
        "sft_f1": 0.0083,
        "repo_id": DEFAULT_MODEL_REPO,
        "release_status": "diagnostic_complete_no_gain",
    }
    files = _files(tmp_path, metrics=metrics)
    for version in ("v3", "v4"):
        jsonl = tmp_path / f"expert_{version}.jsonl"
        jsonl.write_text(
            "".join(
                json.dumps({"schema_version": f"expert_{version}", "i": i}) + "\n"
                for i in range(32)
            ),
            encoding="utf-8",
        )
        split_manifest = tmp_path / f"split_manifest_{version}.json"
        split_manifest.write_text(
            json.dumps({"schema_version": "split_manifest_v1", "datasets": []}),
            encoding="utf-8",
        )
        files[("dataset", f"expert_{version}.jsonl")] = jsonl
        files[("dataset", f"split_manifest_{version}.json")] = split_manifest

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
            },
            dataset_files={
                "README.md",
                "expert_v3.jsonl",
                "split_manifest_v3.json",
                "sft_05b_v3.yaml",
                "expert_v4.jsonl",
                "split_manifest_v4.json",
                "sft_05b_v4.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
    )

    assert evidence.trajectory_filename == "expert_v3.jsonl"
    assert evidence.split_manifest_filename == "split_manifest_v3.json"


def test_release_verifier_rejects_missing_dataset_readme(tmp_path: Path) -> None:
    files = _files(tmp_path)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    with pytest.raises(ReleaseVerificationError, match="README.md"):
        verify_sft_release(
            api=_FakeApi(
                model_files={
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                },
                dataset_files={"expert_v1.jsonl", "sft_05b.yaml", "MODEL_CARD_TEMPLATE.md"},
            ),
            downloader=downloader,
        )


def test_release_verifier_rejects_unresolved_readme_placeholder(tmp_path: Path) -> None:
    files = _files(tmp_path)
    files[("model", "README.md")].write_text("# {model_name}\n", encoding="utf-8")

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    with pytest.raises(ReleaseVerificationError, match="placeholder"):
        verify_sft_release(
            api=_FakeApi(
                model_files={
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                },
                dataset_files={
                    "README.md",
                    "expert_v1.jsonl",
                    "split_manifest.json",
                    "sft_05b.yaml",
                    "MODEL_CARD_TEMPLATE.md",
                },
            ),
            downloader=downloader,
        )


def test_release_verifier_rejects_invalid_metric_range(tmp_path: Path) -> None:
    metrics = {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "llama-3.3-70b-versatile",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 2.0,
        "sft_f1": 0.0,
        "repo_id": DEFAULT_MODEL_REPO,
    }
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    with pytest.raises(ReleaseVerificationError, match="base_f1"):
        verify_sft_release(
            api=_FakeApi(
                model_files={
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                },
                dataset_files={
                    "README.md",
                    "expert_v1.jsonl",
                    "split_manifest.json",
                    "sft_05b.yaml",
                    "MODEL_CARD_TEMPLATE.md",
                },
            ),
            downloader=downloader,
        )


def test_release_verifier_checks_dataset_sha_when_required(tmp_path: Path) -> None:
    metrics = {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "llama-3.3-70b-versatile",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0,
        "sft_f1": 0.1,
        "repo_id": DEFAULT_MODEL_REPO,
        "dataset_sha": "dataset-sha",
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "prompt_contract_drift": False,
        "heldout_leakage_detected": False,
    }
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
            },
            dataset_files={
                "README.md",
                "expert_v1.jsonl",
                "split_manifest.json",
                "sft_05b.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
        require_sha_metrics=True,
    )

    assert evidence.dataset_sha_metric_checked is True
    assert evidence.quality_milestone is True
    assert evidence.release_status == "quality_improved_verified"
    assert evidence.quality_gate_failures == ()


def test_release_verifier_requires_eval_diagnostics_when_requested(tmp_path: Path) -> None:
    metrics = {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "clean-diff-v1",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0,
        "sft_f1": 0.1,
        "repo_id": DEFAULT_MODEL_REPO,
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "prompt_contract_drift": False,
        "heldout_leakage_detected": False,
    }
    files = _files(tmp_path, metrics=metrics)
    diagnostics = tmp_path / "eval_diagnostics.json"
    diagnostics.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_eval_diagnostics_v1",
                "base": {"task_scores": []},
                "sft": {"task_scores": []},
            }
        ),
        encoding="utf-8",
    )
    files[("model", "eval_diagnostics.json")] = diagnostics

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
                "eval_diagnostics.json",
            },
            dataset_files={
                "README.md",
                "expert_v1.jsonl",
                "split_manifest.json",
                "sft_05b.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
        require_eval_diagnostics=True,
    )

    assert evidence.eval_diagnostics_checked is True


def test_release_verifier_can_require_quality_improvement(tmp_path: Path) -> None:
    files = _files(tmp_path)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    with pytest.raises(ReleaseVerificationError, match="quality improvement gate failed"):
        verify_sft_release(
            api=_FakeApi(
                model_files={
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                },
                dataset_files={
                    "README.md",
                    "expert_v1.jsonl",
                    "split_manifest.json",
                    "sft_05b.yaml",
                    "MODEL_CARD_TEMPLATE.md",
                },
            ),
            downloader=downloader,
            require_quality_improvement=True,
        )


def test_release_verifier_uses_manifest_license_for_3b_sft(tmp_path: Path) -> None:
    metrics = {
        "model_name": "DataForge-3B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-3B-Instruct",
        "teacher_model": "clean-diff-v1",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0,
        "sft_f1": 0.1,
        "repo_id": "Praneshrajan15/DataForge-3B-SFT",
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "prompt_contract_drift": False,
        "heldout_leakage_detected": False,
    }
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    with pytest.raises(ReleaseVerificationError, match="qwen-research"):
        verify_sft_release(
            model_repo="Praneshrajan15/DataForge-3B-SFT",
            api=_FakeApi(
                model_files={
                    "README.md",
                    "config.json",
                    "model.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                    "training_metrics.json",
                },
                dataset_files={
                    "README.md",
                    "expert_v1.jsonl",
                    "split_manifest.json",
                    "sft_05b.yaml",
                    "MODEL_CARD_TEMPLATE.md",
                },
            ),
            downloader=downloader,
        )


def test_release_verifier_marks_f1_gain_without_gates_as_failed(tmp_path: Path) -> None:
    metrics = {
        "model_name": "DataForge-0.5B-SFT",
        "model_license": "apache-2.0",
        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
        "teacher_model": "clean-diff-v1",
        "dataset_repo": DEFAULT_DATASET_REPO,
        "training_examples": 29,
        "kaggle_hours": 0.906,
        "base_f1": 0.0,
        "sft_f1": 0.1,
        "repo_id": DEFAULT_MODEL_REPO,
    }
    files = _files(tmp_path, metrics=metrics)

    def downloader(
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        return str(files[(str(repo_type), filename)])

    evidence = verify_sft_release(
        api=_FakeApi(
            model_files={
                "README.md",
                "config.json",
                "model.safetensors",
                "tokenizer.json",
                "tokenizer_config.json",
                "training_metrics.json",
            },
            dataset_files={
                "README.md",
                "expert_v1.jsonl",
                "split_manifest.json",
                "sft_05b.yaml",
                "MODEL_CARD_TEMPLATE.md",
            },
        ),
        downloader=downloader,
    )

    assert evidence.quality_milestone is False
    assert evidence.release_status == "quality_gate_failed"


def test_publish_dataset_readme_uploads_readme(tmp_path: Path) -> None:
    class _FakeDatasetCardApi:
        def __init__(self) -> None:
            self.uploaded: list[tuple[str, str]] = []

        def upload_file(
            self,
            *,
            path_or_fileobj: str,
            path_in_repo: str,
            repo_id: str,
            repo_type: str,
            token: str | None = None,
            commit_message: str,
        ) -> object:
            self.uploaded.append((repo_id, path_in_repo))
            return object()

    readme = tmp_path / "README.md"
    readme.write_text("# Dataset\n", encoding="utf-8")
    api = _FakeDatasetCardApi()

    repo_id = publish_dataset_readme(
        repo_id="tester/dataforge-sft-trajectories",
        readme=readme,
        token=None,
        api=api,
    )

    assert repo_id == "tester/dataforge-sft-trajectories"
    assert api.uploaded == [("tester/dataforge-sft-trajectories", "README.md")]


def test_publish_dataset_readme_cli_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr(dataset_readme_publisher, "load_dotenv", lambda: False)
    monkeypatch.setattr(dataset_readme_publisher, "_resolve_hf_token", lambda: None)

    with pytest.raises(RuntimeError, match="HF_TOKEN"):
        dataset_readme_publisher.main([])
