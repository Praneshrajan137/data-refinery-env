"""Tests for Kaggle GRPO smoke evidence validation."""

from __future__ import annotations

from scripts.model.validate_grpo_smoke_report import validate_smoke_report


def _report(**overrides):
    payload = {
        "schema_version": "dataforge_kaggle_grpo_smoke_report_v1",
        "status": "pass",
        "training_stage": "smoke",
        "attempted_steps": 50,
        "configured_max_steps": 50,
        "gpu_hours": 0.5,
        "max_prompt_tokens": 512,
        "prompt_token_budget": 1024,
        "readiness_status": "pass",
        "readiness_blockers": [],
        "model_upload_attempted": False,
        "model_repo_created": False,
        "public_claim_updated": False,
        "train_metrics": {
            "train_runtime": 1800.0,
            "train_loss": 0.05,
            "train_steps_per_second": 0.02,
        },
    }
    payload.update(overrides)
    return payload


def test_smoke_report_validator_accepts_no_upload_smoke_with_plausible_runtime() -> None:
    report = validate_smoke_report(_report(), max_candidate_gpu_hours=12.0)

    assert report["ok"] is True
    assert report["blockers"] == []
    assert report["checks"]["projected_candidate_gpu_hours"] == 5.0


def test_smoke_report_validator_blocks_implausible_prompt_token_telemetry() -> None:
    report = validate_smoke_report(_report(max_prompt_tokens=2), max_candidate_gpu_hours=12.0)

    assert report["ok"] is False
    assert "prompt_token_telemetry_implausible" in report["blockers"]


def test_smoke_report_validator_blocks_candidate_runtime_over_budget() -> None:
    report = validate_smoke_report(_report(gpu_hours=1.5432), max_candidate_gpu_hours=12.0)

    assert report["ok"] is False
    assert "candidate_runtime_over_budget" in report["blockers"]
    assert report["checks"]["projected_candidate_gpu_hours"] == 15.432


def test_smoke_report_validator_blocks_any_public_mutation() -> None:
    report = validate_smoke_report(_report(model_upload_attempted=True))

    assert report["ok"] is False
    assert "model_upload_attempted" in report["blockers"]
