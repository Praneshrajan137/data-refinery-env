"""Unit tests for the verified-agent policy layer."""

from __future__ import annotations

from dataforge.agent.policy import (
    AgentObservation,
    DeterministicPolicy,
    LLMPolicy,
    PolicyUnavailableError,
    ResidualIssue,
    available_policies,
    make_policy,
    register_policy,
)
from dataforge.agent.tool_actions import Fix, InspectRows


def _observation(residual: bool = True) -> AgentObservation:
    issues = (
        (
            ResidualIssue(
                row=2,
                column="score",
                issue_type="type_mismatch",
                severity="review",
                expected=None,
                actual="abc",
                reason="non-numeric",
            ),
        )
        if residual
        else ()
    )
    return AgentObservation(
        columns=("id", "score"),
        row_count=3,
        residual_issues=issues,
        sample_rows=({"id": "1", "score": "10"},),
        scratchpad_summary="empty",
        last_result="",
        steps_taken=0,
        max_steps=10,
        staged_fix_count=0,
    )


def _scripted(*responses: str):
    """Return a completion fn that yields the given responses in order."""
    state = {"i": 0}

    def _complete(messages, model, temperature):  # noqa: ANN001
        i = state["i"]
        state["i"] = min(i + 1, len(responses) - 1)
        return responses[i]

    return _complete


class TestDeterministicPolicy:
    def test_always_finishes_immediately(self) -> None:
        policy = DeterministicPolicy()
        policy.reset(_observation())
        assert policy.propose_action(_observation()) is None
        assert policy.name == "deterministic"
        assert policy.provenance == "deterministic"


class TestLLMPolicy:
    def test_parses_valid_fix_action(self) -> None:
        policy = LLMPolicy(
            _scripted(
                '{"action_type":"FIX","row":2,"column":"score",'
                '"new_value":"30","justification":"abc is not numeric"}'
            )
        )
        policy.reset(_observation())
        action = policy.propose_action(_observation())
        assert isinstance(action, Fix)
        assert action.row == 2
        assert action.new_value == "30"

    def test_finalize_token_returns_none(self) -> None:
        policy = LLMPolicy(_scripted('{"action_type":"FINALIZE"}'))
        policy.reset(_observation())
        assert policy.propose_action(_observation()) is None

    def test_empty_residual_finishes_even_if_model_rambles(self) -> None:
        policy = LLMPolicy(_scripted("I think the data looks great!"))
        policy.reset(_observation(residual=False))
        assert policy.propose_action(_observation(residual=False)) is None

    def test_malformed_output_falls_back_to_inspect(self) -> None:
        policy = LLMPolicy(_scripted("not json at all"))
        policy.reset(_observation())
        action = policy.propose_action(_observation())
        assert isinstance(action, InspectRows)

    def test_completion_failure_falls_back(self) -> None:
        def _boom(messages, model, temperature):  # noqa: ANN001
            raise RuntimeError("provider down")

        policy = LLMPolicy(_boom)
        policy.reset(_observation())
        action = policy.propose_action(_observation())
        assert isinstance(action, InspectRows)


class TestMakePolicy:
    def test_deterministic_kind(self) -> None:
        assert isinstance(make_policy("deterministic"), DeterministicPolicy)

    def test_override_builds_llm_policy(self) -> None:
        policy = make_policy("hosted", completion_override=_scripted('{"action_type":"FINALIZE"}'))
        assert isinstance(policy, LLMPolicy)

    def test_unknown_kind_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown policy kind"):
            make_policy("banana")


class TestBackendSelection:
    """Selectable backends: hosted default, fail-fast, provider, custom registry."""

    def test_controller_request_defaults_to_hosted(self) -> None:
        from dataforge.agent import AgentRepairRequest

        request = AgentRepairRequest(source_path="x.csv")
        assert request.policy == "hosted"
        assert request.provider is None

    def test_available_policies_lists_builtins(self) -> None:
        kinds = available_policies()
        assert {"hosted", "local", "deterministic"}.issubset(set(kinds))

    def test_hosted_without_key_fails_fast(self, monkeypatch) -> None:  # noqa: ANN001
        import pytest

        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        monkeypatch.delenv("DATAFORGE_LLM_PROVIDER", raising=False)
        with pytest.raises(PolicyUnavailableError, match="No API key"):
            make_policy("hosted")

    def test_hosted_with_key_builds_llm_policy(self, monkeypatch) -> None:  # noqa: ANN001
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        policy = make_policy("hosted", provider="gemini")
        assert isinstance(policy, LLMPolicy)
        assert policy.name == "hosted:gemini"

    def test_hosted_provider_without_its_key_fails(self, monkeypatch) -> None:  # noqa: ANN001
        import pytest

        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        with pytest.raises(PolicyUnavailableError):
            make_policy("hosted", provider="groq")

    def test_local_unavailable_fails_fast(self, monkeypatch) -> None:  # noqa: ANN001
        import pytest

        def _boom(_model):  # noqa: ANN001
            raise RuntimeError("transformers not installed")

        monkeypatch.setattr("dataforge.agent.backends.local.build_local_completion", _boom)
        with pytest.raises(PolicyUnavailableError, match="Local model backend"):
            make_policy("local")

    def test_custom_registry_resolves(self) -> None:
        register_policy("unit_script", lambda **_kwargs: DeterministicPolicy())
        assert "custom:unit_script" in available_policies()
        policy = make_policy("custom:unit_script")
        assert isinstance(policy, DeterministicPolicy)

    def test_unknown_custom_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="not registered"):
            make_policy("custom:does_not_exist")
