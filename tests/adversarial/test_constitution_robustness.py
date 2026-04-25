"""Adversarial and benign robustness checks for the Week 3 constitution."""

from __future__ import annotations

from pathlib import Path

import yaml

from dataforge.cli.common import schema_from_mapping
from dataforge.repairers.base import ProposedFix
from dataforge.safety import SafetyContext, SafetyFilter, SafetyVerdict
from dataforge.transactions.txn import CellFix

_ATTACK_DIR = Path(__file__).resolve().parents[2] / "dataforge" / "safety" / "adversarial"
_BENIGN_FIXTURES = Path(__file__).resolve().parent / "benign_samples.yaml"


def _build_fix(payload: dict[str, object]) -> ProposedFix:
    fix = payload["fix"]
    assert isinstance(fix, dict)
    return ProposedFix(
        fix=CellFix(
            row=int(fix["row"]),
            column=str(fix["column"]),
            old_value=str(fix["old_value"]),
            new_value=str(fix["new_value"]),
            detector_id=str(fix["detector_id"]),
            operation=str(fix.get("operation", "update")),
        ),
        reason=str(payload.get("description", "fixture")),
        confidence=0.9,
        provenance="deterministic",
    )


def _build_context(payload: dict[str, object]) -> SafetyContext:
    raw_context = payload.get("context", {})
    assert isinstance(raw_context, dict)
    return SafetyContext(
        allow_pii=bool(raw_context.get("allow_pii", False)),
        confirm_pii=bool(raw_context.get("confirm_pii", False)),
        confirm_escalations=bool(raw_context.get("confirm_escalations", False)),
    )


def test_attack_fixtures_are_all_denied() -> None:
    filter_ = SafetyFilter()
    attack_files = sorted(_ATTACK_DIR.glob("*.yaml"))

    assert len(attack_files) >= 50

    for attack_file in attack_files:
        payload = yaml.safe_load(attack_file.read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
        schema = schema_from_mapping(payload.get("schema", {}))
        verdict = filter_.evaluate(_build_fix(payload), schema, _build_context(payload)).verdict
        assert str(payload["expected_verdict"]).upper() == "DENY"
        assert verdict == SafetyVerdict.DENY


def test_benign_samples_have_low_false_positive_rate() -> None:
    filter_ = SafetyFilter()
    payload = yaml.safe_load(_BENIGN_FIXTURES.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert len(payload) == 50

    deny_count = 0
    for sample in payload:
        assert isinstance(sample, dict)
        schema = schema_from_mapping(sample.get("schema", {}))
        verdict = filter_.evaluate(_build_fix(sample), schema, _build_context(sample)).verdict
        if verdict == SafetyVerdict.DENY:
            deny_count += 1

    assert deny_count / len(payload) < 0.03
