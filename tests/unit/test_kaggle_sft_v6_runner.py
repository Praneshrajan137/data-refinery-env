"""Contract tests for the gated Kaggle SFT-v6 candidate runner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.remote import kaggle_sft_v6_candidate


def test_sft_v6_runner_validates_contract_minimal_curriculum_report(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_sft_v6_contract_minimal_curriculum_report_v1",
                "ok": True,
                "metrics": {
                    "submit_repair_records": 899,
                    "finish_records": 450,
                    "assistant_reason_fields": 0,
                    "system_reason_field_mentions": 0,
                    "system_wrapper_mentions": 0,
                    "parse_failure_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="submit_repair_records_under_900"):
        kaggle_sft_v6_candidate._validate_curriculum_report(report)


def test_sft_v6_runner_accepts_generated_curriculum_report_shape(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_sft_v6_contract_minimal_curriculum_report_v1",
                "ok": True,
                "metrics": {
                    "submit_repair_records": 974,
                    "finish_records": 487,
                    "assistant_reason_fields": 0,
                    "system_reason_field_mentions": 0,
                    "system_wrapper_mentions": 0,
                    "parse_failure_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    assert kaggle_sft_v6_candidate._validate_curriculum_report(report)["ok"] is True


def test_sft_v6_runner_selects_smoke_by_default_and_candidate_requires_token(monkeypatch) -> None:
    config = {
        "training": {},
        "training_sequence": {
            "stages": [
                {"name": "smoke", "max_steps": 20, "allow_upload_after_gate": False},
                {
                    "name": "candidate",
                    "max_steps": 140,
                    "allow_upload_after_gate": True,
                    "require_hf_token": True,
                },
            ]
        },
    }

    monkeypatch.delenv("DATAFORGE_SFT_STAGE", raising=False)
    smoke = kaggle_sft_v6_candidate._select_stage(config)
    assert smoke == {
        "name": "smoke",
        "max_steps": 20,
        "allow_upload_after_gate": False,
        "require_hf_token": False,
    }
    assert config["training"]["max_steps"] == 20

    monkeypatch.setenv("DATAFORGE_SFT_STAGE", "candidate")
    candidate = kaggle_sft_v6_candidate._select_stage(config)
    assert candidate["require_hf_token"] is True
    assert candidate["allow_upload_after_gate"] is True
    assert config["training"]["max_steps"] == 140


def test_sft_v6_runner_upload_is_private_after_promotion_gate() -> None:
    source = Path(kaggle_sft_v6_candidate.__file__).read_text(encoding="utf-8")

    assert "DATAFORGE_SFT_STAGE" in source
    assert "blocked_missing_hf_token_no_gpu" in source
    assert "sft_v6_candidate_eval_report.json" in source
    assert "runtime_error" in source
    assert "quality_gate_failed_no_upload" in source
    assert "return 0" in source[source.index("quality_gate_failed_no_upload") :]
    assert "smoke_complete_no_upload" in source or "complete_no_upload" in source
    assert "pass_upload_blocked_missing_hf_token" in source
    assert "pass_uploaded_private_candidate" in source
    assert "private=True" in source
    assert "public_claim_updated" in source
    assert source.index("sft_v6_promotion_gate_failures(") < source.index("api.upload_folder(")
    assert source.index("build_sft_v6_promotion_report(") < source.index("api.upload_folder(")
    assert "_cast_trainable_parameters_to_float32(trainer.model)" in source
    assert "max_steps" in source


def test_sft_v6_runner_accepts_hf_secret_env_label(monkeypatch) -> None:
    for label in kaggle_sft_v6_candidate.HF_SECRET_LABELS:
        monkeypatch.delenv(label, raising=False)
    monkeypatch.setenv("HF", "hf_example")

    token, source, error = kaggle_sft_v6_candidate._load_hf_token()

    assert token == "hf_example"
    assert source == "environment"
    assert error == ""
