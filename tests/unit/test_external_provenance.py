"""Lock-in tests for the 'external' untrusted provenance (verify_and_apply foundation).

These guard the corruption-critical invariants: an external (agent/tool/human)
value is untrusted for verification-strength (needs a schema to be proven) and
escalates the unconfirmed-write rule, yet is NOT an LLM-calibration provenance (a
schema-proven external fix auto-applies directly, without a calibration map).
"""

from __future__ import annotations

from dataforge.engine.repair import (
    _LLM_PROVENANCE,
    _UNTRUSTED_PROVENANCE,
    verification_strength_for,
)
from dataforge.repairers.base import ProposedFix
from dataforge.safety.constitution import _llm_live_candidate
from dataforge.transactions.txn import CellFix


def _external_fix() -> ProposedFix:
    return ProposedFix(
        fix=CellFix(row=0, column="c", old_value="a", new_value="b", detector_id="external"),
        reason="external proposal",
        confidence=1.0,
        provenance="external",
    )


def test_external_is_untrusted_but_not_llm_calibration() -> None:
    # Untrusted for trust-strength + safety escalation ...
    assert "external" in _UNTRUSTED_PROVENANCE
    # ... but NOT an LLM-calibration provenance: a schema-proven external fix must
    # auto-apply directly, not route through a calibration threshold.
    assert "external" not in _LLM_PROVENANCE


def test_external_needs_a_schema_to_be_proven() -> None:
    assert (
        verification_strength_for("external", authoritative_schema_present=False)
        == "plausibility_only"
    )
    assert verification_strength_for("external", authoritative_schema_present=True) == "proven"


def test_deterministic_strength_unchanged() -> None:
    assert (
        verification_strength_for("deterministic", authoritative_schema_present=False) == "proven"
    )
    assert (
        verification_strength_for("llm_live", authoritative_schema_present=False)
        == "plausibility_only"
    )


def test_external_write_escalates_unconfirmed_write_rule() -> None:
    assert _llm_live_candidate(_external_fix(), None, None) is True


def test_proposed_fix_accepts_external_provenance() -> None:
    assert _external_fix().provenance == "external"
