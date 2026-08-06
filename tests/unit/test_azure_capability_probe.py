"""The committed Azure capability probe is the evidence the design rests on.

Two design decisions in this repo are justified by what the live deployment
accepts, not by intuition:

1. The candidate pool is enforced as a **hard decode-time enum** (Structured
   Outputs) rather than a prompt request plus post-filter.
2. Logprob-based confidence -- the obvious calibration lever -- is **not built**,
   because the deployment rejects the parameter.

If someone later proposes "just use logprobs to fix calibration", the measured
refusal must be discoverable rather than re-litigated. These tests keep the
evidence honest and machine-checked. They read only the committed artifact and
never contact a provider.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

_ARTIFACT = Path(__file__).resolve().parents[2] / "eval" / "results" / "azure_capability_probe.json"


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    """Return the committed capability-probe artifact."""
    assert _ARTIFACT.exists(), (
        f"{_ARTIFACT} is missing; run scripts/bench/probe_azure_capabilities.py"
    )
    payload = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


class TestArtifactShape:
    """The artifact is self-describing and attributable."""

    def test_schema_and_provenance_are_recorded(self, probe: dict[str, Any]) -> None:
        assert probe["schema"] == "dataforge_azure_capability_probe_v1"
        assert probe["provider"] == "azure"
        # Provenance matters: capability is per-deployment, not universal.
        assert probe["model"]
        assert probe["api_version"]

    def test_spend_is_accounted_for(self, probe: dict[str, Any]) -> None:
        # Even a $0.002 probe leaves a receipt; that is the whole doctrine.
        assert probe["calls"] >= 1
        assert probe["estimated_usd"] >= 0.0

    def test_decision_is_one_of_the_two_designed_paths(self, probe: dict[str, Any]) -> None:
        assert probe["decision"] in {"structured_enum", "prompt_json_fallback"}


class TestClosedLevers:
    """Measured refusals that the design depends on."""

    def test_logprobs_are_unsupported(self, probe: dict[str, Any]) -> None:
        # Microsoft Learn lists logprobs/top_logprobs as unsupported on reasoning
        # models; this is the live confirmation for THIS deployment.
        result = probe["probes"]["logprobs"]
        assert result["accepted"] is False, (
            "logprobs are now accepted -- a continuous logprob confidence may be a "
            "better calibration signal than self-consistency agreement. Re-open the "
            "lever deliberately rather than leaving this test failing."
        )
        assert result["matches_documentation"] is True

    def test_temperature_is_unsupported(self, probe: dict[str, Any]) -> None:
        # Consequence: the corrector's temperature=0.4 never took effect on Azure.
        # Samples are drawn at the model's fixed temperature, so k must be
        # measured empirically rather than tuned via temperature.
        result = probe["probes"]["temperature"]
        assert result["accepted"] is False
        assert result["matches_documentation"] is True


class TestStructuredOutputs:
    """The lever the flagship experiment is built on."""

    def test_structured_enum_is_accepted_and_honoured(self, probe: dict[str, Any]) -> None:
        result = probe["probes"]["structured_outputs_enum"]
        if probe["decision"] != "structured_enum":
            pytest.skip("Deployment does not support Structured Outputs; fallback path in use.")
        assert result["accepted"] is True
        # Acceptance alone is not enough -- the returned value must actually come
        # from the enum, otherwise the "hard constraint" is a fiction.
        assert result["enum_honoured"] is True
        assert result["content_is_empty"] is False

    def test_reasoning_effort_none_is_available_as_a_cost_lever(
        self, probe: dict[str, Any]
    ) -> None:
        result = probe["probes"]["reasoning_effort_none"]
        if not result["accepted"]:
            pytest.skip("Deployment does not accept reasoning_effort=none.")
        # 'none' should mean no hidden reasoning tokens, which is the saving.
        assert result["reasoning_tokens"] == 0
