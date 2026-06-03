"""No-network tests for the local Hugging Face evaluation adapter."""

from __future__ import annotations

import pytest

from dataforge_evals.agents.hf_local import HfLocalAgent, resolve_default_model_id
from dataforge_evals.agents.provider_base import ProviderError
from dataforge_evals.tasks import load_synthetic_task


class _FakeTokenizer:
    eos_token_id = 0

    def apply_chat_template(
        self,
        messages: list[dict[str, str]],
        *,
        tokenize: bool,
        add_generation_prompt: bool,
    ) -> str:
        return "\n".join(f"{message['role']}: {message['content']}" for message in messages)


class _FakeModel:
    def __init__(self, text: str) -> None:
        self.text = text
        self.prompt: str | None = None

    def complete(self, prompt: str) -> str:
        self.prompt = prompt
        return self.text


def test_resolve_default_model_id_prefers_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAFORGE_EVAL_MODEL", "tester/custom-sft")

    assert resolve_default_model_id() == "tester/custom-sft"


def test_hf_local_agent_parses_list_output_without_network() -> None:
    model = _FakeModel('[{"row":0,"column":"Score","new_value":"4.5","reason":"decimal shift"}]')
    agent = HfLocalAgent(
        model_id="tester/DataForge-0.5B-SFT",
        tokenizer=_FakeTokenizer(),
        model=model,
    )

    result = agent.run(load_synthetic_task())

    assert result.model == "tester/DataForge-0.5B-SFT"
    assert result.usage.calls == 0
    assert result.fixes[0].row == 0
    assert result.fixes[0].column == "Score"
    assert result.fixes[0].new_value == "4.5"
    assert model.prompt is not None
    assert '"action":"submit_repairs"' in model.prompt
    assert '"contract_version":"repair_contract_v2"' in model.prompt
    assert '"valid_rows":[0,1]' in model.prompt
    assert '"target_rows"' in model.prompt
    assert "ground_truth" not in model.prompt


def test_hf_local_agent_accepts_repairs_object_output() -> None:
    agent = HfLocalAgent(
        model_id="tester/DataForge-0.5B-SFT",
        tokenizer=_FakeTokenizer(),
        model=_FakeModel('{"repairs":[{"row":1,"column":"Phone","new_value":"217-555-0101"}]}'),
    )

    result = agent.run(load_synthetic_task())

    assert result.fixes == [result.fixes[0].model_copy(update={"reason": "hf-local proposal"})]
    assert result.fixes[0].reason == "hf-local proposal"
    assert result.fixes[0].new_value == "217-555-0101"


def test_hf_local_agent_extracts_fenced_repairs_object() -> None:
    agent = HfLocalAgent(
        model_id="tester/DataForge-0.5B-SFT",
        tokenizer=_FakeTokenizer(),
        model=_FakeModel(
            '```json\n{"action":"submit_repairs","repairs":'
            '[{"row":0,"column":"Score","new_value":"4.5","reason":"decimal"}]}\n```'
        ),
    )

    result = agent.run(load_synthetic_task())

    assert result.fixes[0].column == "Score"
    assert result.fixes[0].new_value == "4.5"


def test_hf_local_agent_extracts_json_after_leading_text() -> None:
    agent = HfLocalAgent(
        model_id="tester/DataForge-0.5B-SFT",
        tokenizer=_FakeTokenizer(),
        model=_FakeModel(
            'Here is the JSON: {"action":"submit_repairs","repairs":'
            '[{"row":1,"column":"Phone","new_value":"217-555-0101"}]}'
        ),
    )

    result = agent.run(load_synthetic_task())

    assert result.fixes[0].row == 1
    assert result.fixes[0].column == "Phone"


def test_hf_local_agent_rejects_truncated_json() -> None:
    agent = HfLocalAgent(
        model_id="tester/DataForge-0.5B-SFT",
        tokenizer=_FakeTokenizer(),
        model=_FakeModel('{"action":"submit_repairs","repairs":[{"row":0'),
    )

    with pytest.raises(ProviderError, match="truncated JSON"):
        agent.run(load_synthetic_task())


def test_hf_local_agent_raises_on_non_json_output() -> None:
    agent = HfLocalAgent(
        model_id="tester/DataForge-0.5B-SFT",
        tokenizer=_FakeTokenizer(),
        model=_FakeModel("not-json"),
    )

    with pytest.raises(ProviderError, match="non-JSON"):
        agent.run(load_synthetic_task())


def test_hf_local_agent_blocks_unapproved_local_model_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATAFORGE_ALLOW_LOCAL_MODEL_INFERENCE", raising=False)
    agent = HfLocalAgent(model_id="tester/DataForge-0.5B-SFT")

    with pytest.raises(ProviderError, match="disabled"):
        agent._load()
