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
                    "user_contract_version_mismatches": 0,
                    "record_contract_version_mismatches": 0,
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
                    "user_contract_version_mismatches": 0,
                    "record_contract_version_mismatches": 0,
                    "parse_failure_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    assert kaggle_sft_v6_candidate._validate_curriculum_report(report)["ok"] is True


def test_sft_runner_accepts_v7_parse_latch_curriculum_report(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(kaggle_sft_v6_candidate, "DEFAULT_SFT_VERSION", "v7")
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_sft_v7_parse_latch_curriculum_report_v1",
                "ok": True,
                "metrics": {
                    "submit_repair_records": 1948,
                    "finish_records": 487,
                    "assistant_reason_fields": 0,
                    "system_reason_field_mentions": 0,
                    "system_wrapper_mentions": 0,
                    "finish_with_repairs": 0,
                    "user_contract_version_mismatches": 0,
                    "record_contract_version_mismatches": 0,
                    "parse_failure_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    assert kaggle_sft_v6_candidate._validate_curriculum_report(report)["ok"] is True


def test_sft_runner_accepts_v8_schema_distill_curriculum_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(kaggle_sft_v6_candidate, "DEFAULT_SFT_VERSION", "v8")
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_sft_v8_schema_distill_curriculum_report_v1",
                "ok": True,
                "label_mask_audit": {"ok": True},
                "metrics": {
                    "prompt_completion_records": 2448,
                    "submit_repair_records": 1274,
                    "finish_records": 1174,
                    "submit_ratio": 0.5204,
                    "assistant_reason_fields": 0,
                    "system_reason_field_mentions": 0,
                    "system_wrapper_mentions": 0,
                    "finish_with_repairs": 0,
                    "user_contract_version_mismatches": 0,
                    "record_contract_version_mismatches": 0,
                    "parse_failure_count": 0,
                    "completion_parse_failure_count": 0,
                    "completion_code_fence_count": 0,
                    "completion_reason_text_count": 0,
                    "legacy_messages_present": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    assert kaggle_sft_v6_candidate._validate_curriculum_report(report)["ok"] is True


def test_sft_runner_accepts_v9_action_envelope_curriculum_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(kaggle_sft_v6_candidate, "DEFAULT_SFT_VERSION", "v9")
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_sft_v9_action_envelope_curriculum_report_v1",
                "ok": True,
                "label_mask_audit": {"ok": True},
                "product_constrained_preflight": {"parse_structural_success_rate": 1.0},
                "metrics": {
                    "prompt_completion_records": 3848,
                    "submit_repair_records": 1974,
                    "finish_records": 1874,
                    "submit_ratio": 0.513,
                    "finish_with_repairs": 0,
                    "user_contract_version_mismatches": 0,
                    "record_contract_version_mismatches": 0,
                    "completion_parse_failure_count": 0,
                    "completion_code_fence_count": 0,
                    "completion_reason_text_count": 0,
                    "legacy_messages_present": 0,
                    "negative_contrast_target_leakage_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    assert kaggle_sft_v6_candidate._validate_curriculum_report(report)["ok"] is True


def test_sft_v8_records_to_dataset_uses_conversational_prompt_completion(monkeypatch) -> None:
    monkeypatch.setattr(kaggle_sft_v6_candidate, "DEFAULT_SFT_VERSION", "v8")
    records = [
        {
            "dataset": "beers",
            "inferability": "deterministic_normalization",
            "fix": [{"row": 1, "column": "ounces", "new_value": "12"}],
            "prompt": [
                {"role": "system", "content": "system"},
                {"role": "user", "content": "{}"},
            ],
            "completion": '{"action":"submit_repairs","repairs":[{"row":1,"column":"ounces","new_value":"12"}]}',
        }
    ]

    dataset, shape = kaggle_sft_v6_candidate._records_to_dataset(records)

    row = dataset[0]
    assert shape["training_format"] == "prompt_completion"
    assert row["prompt"][0]["role"] == "system"
    assert row["completion"] == [
        {
            "role": "assistant",
            "content": '{"action":"submit_repairs","repairs":[{"row":1,"column":"ounces","new_value":"12"}]}',
        }
    ]


def test_sft_v8_sft_config_requires_completion_only_loss(monkeypatch) -> None:
    monkeypatch.setattr(kaggle_sft_v6_candidate, "DEFAULT_SFT_VERSION", "v8")
    config = {
        "training": {
            "output_dir": "out",
            "num_train_epochs": 1,
            "per_device_train_batch_size": 1,
            "gradient_accumulation_steps": 1,
            "learning_rate": 1e-5,
            "lr_scheduler_type": "cosine",
            "warmup_ratio": 0.0,
            "weight_decay": 0.0,
            "logging_steps": 1,
            "save_steps": 10,
            "save_total_limit": 1,
            "fp16": True,
            "bf16": False,
            "gradient_checkpointing": False,
            "report_to": "none",
            "packing": False,
            "max_steps": 1,
            "max_seq_length": 64,
            "loss_type": "chunked_nll",
            "completion_only_loss": True,
            "assistant_only_loss": False,
        }
    }

    kwargs = kaggle_sft_v6_candidate._sft_config_kwargs(
        config,
        supported_keys={
            "output_dir",
            "packing",
            "max_steps",
            "max_length",
            "completion_only_loss",
            "assistant_only_loss",
        },
    )

    assert kwargs["completion_only_loss"] is True
    assert kwargs["assistant_only_loss"] is False

    config["training"]["completion_only_loss"] = False
    with pytest.raises(RuntimeError, match="completion_only_loss=true"):
        kaggle_sft_v6_candidate._sft_config_kwargs(
            config,
            supported_keys={"completion_only_loss"},
        )


class _FakeTokenizer:
    def apply_chat_template(self, messages, *, tokenize=False, add_generation_prompt=False):
        assert tokenize is False
        text = "".join(f"<{message['role']}>{message['content']}</{message['role']}>" for message in messages)
        if add_generation_prompt:
            text += "<assistant>"
        return text

    def __call__(self, text, *, add_special_tokens=False):
        assert add_special_tokens is False
        return {"input_ids": list(text.encode("utf-8"))}


def test_sft_v8_label_mask_audit_keeps_completion_after_left_truncation(monkeypatch) -> None:
    monkeypatch.setattr(kaggle_sft_v6_candidate, "DEFAULT_SFT_VERSION", "v8")
    records = [
        {
            "prompt": [
                {"role": "system", "content": "s" * 20},
                {"role": "user", "content": "u" * 20},
            ],
            "completion": '{"action":"finish","repairs":[]}',
        }
    ]

    audit = kaggle_sft_v6_candidate._prompt_completion_label_mask_audit(
        records,
        _FakeTokenizer(),
        max_seq_length=128,
    )

    assert audit["ok"] is True
    assert audit["completion_only_loss_required"] is True
    assert audit["max_completion_tokens"] > 0


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
    assert "DATAFORGE_SFT_VERSION" in source
    assert "blocked_missing_hf_token_no_gpu" in source
    assert (
        'PROMOTION_REPORT_FILENAME = f"sft_{DEFAULT_SFT_VERSION}_candidate_eval_report.json"'
        in source
    )
    assert "runtime_error" in source
    assert "quality_gate_failed_no_upload" in source
    assert "return 0" in source[source.index("quality_gate_failed_no_upload") :]
    assert "smoke_complete_no_upload" in source or "complete_no_upload" in source
    assert "pass_upload_blocked_missing_hf_token" in source
    assert "pass_uploaded_private_candidate" in source
    assert 'contract_version=str(config["collection"]["prompt_contract_version"])' in source
    assert "KAGGLE_REPORT_SCHEMA" in source
    assert "promotion_report_schema" in source
    assert "private=True" in source
    assert "public_claim_updated" in source
    assert source.index("sft_promotion_gate_failures(") < source.index("api.upload_folder(")
    assert source.index("build_sft_promotion_report(") < source.index("api.upload_folder(")
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


class _FakeCuda:
    def __init__(self, *, available: bool, capability: tuple[int, int], name: str) -> None:
        self._available = available
        self._capability = capability
        self._name = name

    def is_available(self) -> bool:
        return self._available

    def get_device_capability(self, index: int) -> tuple[int, int]:
        assert index == 0
        return self._capability

    def get_device_name(self, index: int) -> str:
        assert index == 0
        return self._name


class _FakeTorch:
    def __init__(self, cuda: _FakeCuda) -> None:
        self.cuda = cuda


def test_sft_runner_accepts_p100_free_tier_gpu() -> None:
    metadata = kaggle_sft_v6_candidate._require_supported_gpu(
        _FakeTorch(
            _FakeCuda(
                available=True,
                capability=(6, 0),
                name="Tesla P100-PCIE-16GB",
            )
        ),
        "SFT-v9",
    )

    assert metadata == {
        "gpu_name": "Tesla P100-PCIE-16GB",
        "capability": "sm_60",
        "precision_mode": "fp16_pascal",
    }


def test_sft_runner_rejects_pre_pascal_gpu() -> None:
    with pytest.raises(RuntimeError, match="Pascal-or-newer"):
        kaggle_sft_v6_candidate._require_supported_gpu(
            _FakeTorch(
                _FakeCuda(
                    available=True,
                    capability=(5, 0),
                    name="Legacy GPU",
                )
            ),
            "SFT-v9",
        )
