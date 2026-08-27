"""The agent-throughput decomposition must keep asserting its finding.

The measurement exists to correct a claim: `docs/STRATEGY.md` attributed a refusal to
"SMT+safety" when the safety constitution returns before the verifier is reached, on a rule that
inspects `provenance` alone. A committed artifact that drifted out of that shape would leave the
correction unsupported while the prose still asserted it -- the exact failure the truth gates
exist to prevent, one level down.

These tests read the committed artifact rather than re-running the harness, so they are cheap
enough to run every time, and they check the two properties that carry the argument: that
something wrote (the finding), and that the control still refused (what makes the finding mean
anything).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = PROJECT_ROOT / "eval" / "results" / "agent_throughput_decomposition.json"


@pytest.fixture(scope="module")
def payload() -> dict:
    return json.loads(ARTIFACT.read_text(encoding="utf-8"))


class TestCommittedArtifact:
    def test_artifact_exists_and_names_its_measurement(self, payload: dict) -> None:
        assert payload["measurement"] == "agent_throughput_decomposition"
        assert payload["corpus"] == "premised_fd_10rows"

    def test_every_arm_is_present(self, payload: dict) -> None:
        names = {arm["arm"] for arm in payload["arms"]}
        assert names == {
            "no_premise_unconfirmed",
            "no_premise_confirmed",
            "premise_unconfirmed",
            "premise_confirmed",
            "premise_confirmed_violating_value",
        }

    def test_every_arm_matched_its_pre_stated_expectation(self, payload: dict) -> None:
        """A recorded divergence means the published prose is describing something else."""
        for arm in payload["arms"]:
            assert arm["observed"] == arm["expectation"], (
                f"{arm['arm']}: expected {arm['expectation']}, artifact records {arm['observed']}"
            )

    def test_every_arm_states_why_it_exists(self, payload: dict) -> None:
        for arm in payload["arms"]:
            assert arm["why"].strip(), f"{arm['arm']} carries no rationale"


class TestTheFindingItself:
    def test_something_wrote(self, payload: dict) -> None:
        """The whole correction. If nothing writes, agent throughput really is zero."""
        assert payload["summary"]["arms_that_wrote"] >= 1

    def test_a_provable_fix_was_still_refused_on_its_origin_label(self, payload: dict) -> None:
        """Shows the escalation and the prove gate are orthogonal, not redundant."""
        assert payload["summary"]["premise_alone_is_insufficient"] is True

    def test_confirmation_alone_did_not_buy_a_write(self, payload: dict) -> None:
        """`no declared premise, no write` must survive confirmation."""
        assert payload["summary"]["confirmation_alone_is_insufficient"] is True

    def test_the_written_arm_had_both_a_premise_and_confirmation(self, payload: dict) -> None:
        written = [arm for arm in payload["arms"] if arm["applied"]]
        assert written, "no arm wrote"
        for arm in written:
            assert arm["premise_declared"] is True, "a write without a declared premise"
            assert arm["confirm_escalations"] is True, "a write without confirmation"


class TestNonVacuity:
    def test_the_control_refused_under_permissive_settings(self, payload: dict) -> None:
        """Without this the measurement shows only that the gate can be opened."""
        assert payload["summary"]["violating_value_refused_under_permissive_settings"] is True

    def test_the_control_was_refused_by_the_prove_gate_specifically(self, payload: dict) -> None:
        """A control satisfied by ANY refusal cannot tell the prove gate from an earlier rule."""
        control = next(
            arm for arm in payload["arms"] if arm["arm"] == "premise_confirmed_violating_value"
        )
        assert control["observed"] == "verifier_rejected"
        assert control["applied"] is False
        assert control["bytes_changed"] is False

    def test_the_control_ran_the_same_settings_as_the_written_arm(self, payload: dict) -> None:
        """The comparison is only valid if the single difference is the proposed value."""
        control = next(
            arm for arm in payload["arms"] if arm["arm"] == "premise_confirmed_violating_value"
        )
        written = next(arm for arm in payload["arms"] if arm["arm"] == "premise_confirmed")
        assert control["premise_declared"] == written["premise_declared"]
        assert control["confirm_escalations"] == written["confirm_escalations"]
        assert control["proposed_value"] != written["proposed_value"]

    def test_no_refused_arm_changed_bytes(self, payload: dict) -> None:
        for arm in payload["arms"]:
            if not arm["applied"]:
                assert arm["bytes_changed"] is False, f"{arm['arm']} mutated a file while refusing"
                assert arm["txn_id"] is None, f"{arm['arm']} recorded a transaction while refusing"
