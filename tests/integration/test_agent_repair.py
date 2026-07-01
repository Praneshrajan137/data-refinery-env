"""Integration tests for the verified autonomous agent controller."""

from __future__ import annotations

from pathlib import Path

from dataforge.agent import AgentRepairRequest, make_policy, run_agent_repair
from dataforge.agent.policy import LLMPolicy, register_policy
from dataforge.cli.common import load_schema
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.transactions.revert import revert_transaction


def _scripted_policy(*responses: str):
    """Build an LLM policy that replays scripted JSON actions in order."""
    state = {"i": 0}

    def _complete(messages, model, temperature):  # noqa: ANN001
        i = state["i"]
        state["i"] = min(i + 1, len(responses) - 1)
        return responses[i]

    return make_policy("hosted", completion_override=_complete)


def _fix_json(row: int, column: str, value: str) -> str:
    return (
        f'{{"action_type":"FIX","row":{row},"column":"{column}",'
        f'"new_value":"{value}","justification":"test fix"}}'
    )


class TestParity:
    """The agent with the deterministic policy must equal the legacy pipeline."""

    def test_deterministic_policy_matches_pipeline(self, tmp_path: Path) -> None:
        csv = tmp_path / "amounts.csv"
        csv.write_text("id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n", encoding="utf-8")
        schema = tmp_path / "schema.yaml"
        schema.write_text(
            "columns:\n  id: str\n  amount: float\n"
            "domain_bounds:\n  amount:\n    min: 0\n    max: 5000\n",
            encoding="utf-8",
        )
        parsed = load_schema(schema)

        legacy = run_repair_pipeline(
            RepairPipelineRequest(source_path=csv, mode="dry_run", schema=parsed)
        )
        agent = run_agent_repair(
            AgentRepairRequest(
                source_path=csv, mode="dry_run", schema=parsed, policy="deterministic"
            )
        )
        assert agent.fixes_count == len(legacy.fixes)
        assert agent.floor_fix_count == len(legacy.fixes)
        assert agent.agent_fix_count == 0
        legacy_cells = {(f.row, f.column, f.new_value) for f in legacy.fixes}
        agent_cells = {(f.row, f.column, f.new_value) for f in agent.fixes}
        assert agent_cells == legacy_cells


class TestSelfCorrection:
    """A rejected FIX must feed back and let the agent converge."""

    def test_agent_self_corrects_after_smt_rejection(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,score\n1,10\n2,20\n3,abc\n", encoding="utf-8")
        schema = tmp_path / "schema.yaml"
        schema.write_text(
            "columns:\n  id: str\n  score: float\n"
            "domain_bounds:\n  score:\n    min: 0\n    max: 100\n",
            encoding="utf-8",
        )
        parsed = load_schema(schema)

        # First proposal violates the bound (rejected), second is valid.
        policy = _scripted_policy(
            _fix_json(2, "score", "9999"),
            _fix_json(2, "score", "30"),
            '{"action_type":"FINALIZE"}',
        )
        result = run_agent_repair(
            AgentRepairRequest(
                source_path=csv,
                mode="dry_run",
                schema=parsed,
                policy="hosted",
                max_steps=6,
                confirm_escalations=True,
            ),
            policy=policy,
        )
        statuses = [(r.action_type, r.accepted) for r in result.trace]
        assert ("FIX", False) in statuses  # the rejection happened
        assert ("FIX", True) in statuses  # then it self-corrected
        assert result.agent_fix_count == 1


class TestReversibility:
    """Apply then revert must restore the source byte-for-byte."""

    def test_apply_then_revert_restores_bytes(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,score\n1,10\n2,20\n3,abc\n", encoding="utf-8")
        original = csv.read_bytes()

        policy = _scripted_policy(_fix_json(2, "score", "30"), '{"action_type":"FINALIZE"}')
        result = run_agent_repair(
            AgentRepairRequest(
                source_path=csv,
                mode="apply",
                policy="hosted",
                max_steps=4,
                confirm_escalations=True,
            ),
            policy=policy,
        )
        assert result.applied is True
        assert result.txn_id is not None
        assert csv.read_bytes() != original

        revert_transaction(result.txn_id, search_root=tmp_path)
        assert csv.read_bytes() == original


class TestSafetyInvariantOverRuns:
    """Across many scripted actions, only verified fixes are ever staged."""

    def test_no_unverified_fix_is_staged(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,score\n1,10\n2,20\n3,abc\n", encoding="utf-8")
        schema = tmp_path / "schema.yaml"
        schema.write_text(
            "columns:\n  id: str\n  score: float\n"
            "domain_bounds:\n  score:\n    min: 0\n    max: 100\n",
            encoding="utf-8",
        )
        parsed = load_schema(schema)

        # A barrage of mostly-invalid proposals; only the in-bounds one is valid.
        policy = _scripted_policy(
            _fix_json(2, "score", "-5"),
            _fix_json(2, "score", "100000"),
            _fix_json(2, "score", "40"),
            '{"action_type":"FINALIZE"}',
        )
        result = run_agent_repair(
            AgentRepairRequest(
                source_path=csv,
                mode="apply",
                schema=parsed,
                policy="hosted",
                max_steps=8,
                confirm_escalations=True,
            ),
            policy=policy,
        )
        # Every staged fix must satisfy the bound.
        for fix in result.fixes:
            if fix.column == "score":
                assert 0 <= float(fix.new_value) <= 100


class TestCustomPolicy:
    """A registered custom policy is still gated by the verified executor."""

    def test_custom_policy_runs_through_the_gate(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,score\n1,10\n2,20\n3,abc\n", encoding="utf-8")

        def _factory(**_kwargs):
            state = {"i": 0}

            def _complete(messages, model, temperature):  # noqa: ANN001
                state["i"] += 1
                if state["i"] == 1:
                    return _fix_json(2, "score", "30")
                return '{"action_type":"FINALIZE"}'

            return LLMPolicy(_complete, name="custom:e2e", provenance="llm_live")

        register_policy("e2e", _factory)
        result = run_agent_repair(
            AgentRepairRequest(
                source_path=csv,
                mode="dry_run",
                policy="custom:e2e",
                max_steps=4,
                confirm_escalations=True,
            )
        )
        assert result.policy_name == "custom:e2e"
        assert result.agent_fix_count == 1

    def test_custom_policy_unsafe_fix_still_denied(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,score\n1,10\n2,20\n3,abc\n", encoding="utf-8")

        def _factory(**_kwargs):
            def _complete(messages, model, temperature):  # noqa: ANN001
                return (
                    '{"action_type":"FIX","row":2,"column":"score","new_value":"x",'
                    '"fix_type":"delete_row","justification":"drop it"}'
                )

            return LLMPolicy(_complete, name="custom:evil", provenance="llm_live")

        register_policy("evil", _factory)
        result = run_agent_repair(
            AgentRepairRequest(
                source_path=csv,
                mode="dry_run",
                policy="custom:evil",
                max_steps=3,
                confirm_escalations=True,
            )
        )
        # The custom policy cannot bypass the constitution: no row delete staged.
        assert result.agent_fix_count == 0
