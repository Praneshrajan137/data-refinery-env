"""Contract tests for the gated Kaggle GRPO candidate runner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.remote import kaggle_grpo_candidate


def test_candidate_runner_requires_passed_smoke_validation(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "kaggle_grpo_smoke_report.json").write_text(
        json.dumps(
            {
                "schema_version": "dataforge_kaggle_grpo_smoke_report_v1",
                "status": "pass",
                "training_stage": "smoke",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "smoke_validation.json").write_text(
        json.dumps(
            {
                "schema_version": "dataforge_grpo_smoke_validation_v1",
                "ok": False,
                "blockers": ["candidate_runtime_over_budget"],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(kaggle_grpo_candidate, "INPUT_ROOT", tmp_path)

    with pytest.raises(RuntimeError, match="smoke evidence"):
        kaggle_grpo_candidate._validate_smoke_prereq()


def test_candidate_runner_upload_is_after_local_verifier() -> None:
    source = Path(kaggle_grpo_candidate.__file__).read_text(encoding="utf-8")

    assert 'selected_stage_name not in {"diagnostic", "candidate"}' in source
    assert "GRPO diagnostic stage must run exactly 250" in source
    assert "GRPO candidate stage must run exactly 500" in source
    assert "verify_local_grpo_artifact_dir(merged_dir" in source
    assert source.index("verify_local_grpo_artifact_dir(merged_dir") < source.index("api.upload_folder(")
    assert "quality_gate_failed_no_upload" in source
    assert "return 0" in source[source.index("quality_gate_failed_no_upload") :]
    assert "diagnostic_complete_no_upload" in source
    assert "blocked_missing_sft_v6_predecessor" in source
    assert "sft_v6_candidate_eval_report.json" in source
    assert "pass_upload_blocked_missing_hf_token" in source
    assert "HF_TOKEN or HF Kaggle secret is required for candidate upload" in source
    assert '"HF"' in source
    assert "target_strict_macro_f1" in source
    assert "require_not_inferable_slice_f1" in source
    assert "require_deterministic_normalization_slice_f1" in source
    assert "private=upload_private" in source


def test_candidate_runner_accepts_hf_secret_env_label(monkeypatch) -> None:
    for label in kaggle_grpo_candidate.HF_SECRET_LABELS:
        monkeypatch.delenv(label, raising=False)
    monkeypatch.setenv("HF", "hf_example")

    token, source = kaggle_grpo_candidate._load_hf_token()

    assert token == "hf_example"
    assert source == "environment:HF"
