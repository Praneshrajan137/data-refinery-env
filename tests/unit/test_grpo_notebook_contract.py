"""Contract tests for the Week 12 GRPO Kaggle notebook."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "training" / "kaggle" / "grpo_kaggle.ipynb"


def _payload() -> dict[str, object]:
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))


def _source() -> str:
    payload = _payload()
    return "\n".join(
        "".join(cell.get("source", ""))
        for cell in payload["cells"]
        if cell.get("cell_type") == "code"
    )


def test_grpo_notebook_has_six_main_cells_and_no_placeholders() -> None:
    payload = _payload()
    main_cells = [
        cell
        for cell in payload["cells"]
        if cell.get("cell_type") == "code"
        and "week12_grpo_main" in cell.get("metadata", {}).get("tags", [])
    ]
    source = _source()

    assert len(main_cells) == 6
    assert "<you>" not in source
    assert "TBD" not in source
    assert "pending" not in source.lower()


def test_grpo_notebook_enforces_corrected_trl_and_prompt_budget_contract() -> None:
    source = _source()

    assert "PYTHONUTF8" in source
    assert "GRPOConfig" in source
    assert "GRPOTrainer" in source
    assert "inspect.signature(GRPOConfig)" in source
    assert "prompt_token_budget" in source
    assert "_enforce_prompt_budget" in source
    assert "Prompt budget enforced" in source
    assert "max_prompt_length" in source
    assert "trl==0.11" not in source
    assert "dataforge_reward" in source
    assert "repair_contract_v2" in source
    assert "expert_v4.jsonl" in source
    assert "analyze_grpo_readiness" in source
    assert "grpo_readiness_report.json" in source
    assert "report_to" in source and "tensorboard" in source


def test_grpo_notebook_supports_05b_and_15b_gate_before_push() -> None:
    source = _source()

    assert "DataForge-0.5B-GRPO" in source
    assert "DataForge-1.5B-GRPO" in source
    assert "DataForge-1.5B-SFT" in source
    assert "sft_checkpoint_required" in source
    assert "latest_checkpoint" in source
    assert "resume_from_checkpoint=latest_checkpoint" in source
    assert "merge_and_unload" in source
    assert "acceptance_gate_passed" in source
    assert "min_absolute_f1_gain" in source
    assert "DATAFORGE_GRPO_STAGE" in source
    assert '"smoke"' in source
    assert "stage_allows_upload" in source
    assert 'max_steps"] = int(selected_stage["max_steps"])' in source
    assert source.index("if not acceptance_gate_passed") < source.index("api.upload_folder(")
    assert source.index("if not stage_allows_upload") < source.index("api.upload_folder(")
    assert 'raise RuntimeError("GRPO gate failed' in source


def test_grpo_notebook_writes_complete_public_model_card_metadata() -> None:
    source = _source()

    assert "license:" in source
    assert "datasets:" in source
    assert "base_model:" in source
    assert "model-index:" in source
    assert "predecessor" in source
    assert "not production autonomous repair" in source
