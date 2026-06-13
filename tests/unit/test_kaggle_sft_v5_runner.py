"""Contract tests for the gated Kaggle SFT-v5 candidate runner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.remote import kaggle_sft_v5_candidate


def test_sft_v5_runner_validates_curriculum_report(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_sft_v5_repair_curriculum_report_v1",
                "ok": False,
                "metrics": {
                    "deterministic_repair_records": 100,
                    "hard_negative_noop_records": 20,
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="curriculum report blocked"):
        kaggle_sft_v5_candidate._validate_curriculum_report(report)


def test_sft_v5_runner_accepts_hf_secret_env_label(monkeypatch) -> None:
    for label in kaggle_sft_v5_candidate.HF_SECRET_LABELS:
        monkeypatch.delenv(label, raising=False)
    monkeypatch.setenv("HF", "hf_example")

    token, source, error = kaggle_sft_v5_candidate._load_hf_token()

    assert token == "hf_example"
    assert source == "environment"
    assert error == ""


def test_sft_v5_runner_upload_is_private_after_promotion_gate() -> None:
    source = Path(kaggle_sft_v5_candidate.__file__).read_text(encoding="utf-8")

    assert "sft_v5_candidate_eval_report.json" in source
    assert "runtime_error" in source
    assert "quality_gate_failed_no_upload" in source
    assert "return 0" in source[source.index("quality_gate_failed_no_upload") :]
    assert "pass_upload_blocked_missing_hf_token" in source
    assert "pass_uploaded_private_candidate" in source
    assert "private=True" in source
    assert "public_claim_updated" in source
    assert source.index("sft_v5_promotion_gate_failures(") < source.index("api.upload_folder(")
    assert source.index("build_sft_v5_promotion_report(") < source.index("api.upload_folder(")
    assert "SFTConfig" in source
    assert "SFTTrainer" in source
    assert "prepare_model_for_kbit_training" in source
    assert "_cast_trainable_parameters_to_float32(trainer.model)" in source
    assert "max_length" in source
    assert "build_heldout_tasks" in source
    assert "evaluate_causal_lm" in source


def test_sft_v5_runner_casts_trainable_params_to_float32() -> None:
    class FakeTensor:
        def __init__(self, dtype: str) -> None:
            self.dtype = dtype

        def float(self) -> FakeTensor:
            return FakeTensor("torch.float32")

    class FakeParameter:
        def __init__(self, dtype: str, *, requires_grad: bool) -> None:
            self.dtype = dtype
            self.requires_grad = requires_grad
            self.data = FakeTensor(dtype)
            self.grad = None

    class FakeModel:
        def __init__(self) -> None:
            self.trainable = FakeParameter("torch.bfloat16", requires_grad=True)
            self.frozen = FakeParameter("torch.bfloat16", requires_grad=False)

        def named_parameters(self):
            return [("adapter.weight", self.trainable), ("base.weight", self.frozen)]

    model = FakeModel()
    changed = kaggle_sft_v5_candidate._cast_trainable_parameters_to_float32(model)

    assert changed == [
        {
            "name": "adapter.weight",
            "from_dtype": "torch.bfloat16",
            "to_dtype": "torch.float32",
        }
    ]
    assert model.trainable.data.dtype == "torch.float32"
    assert model.frozen.data.dtype == "torch.bfloat16"
