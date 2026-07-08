"""Tests for the selective-repair calibration artifact builder."""

from __future__ import annotations

import json

from dataforge.bench.calibration_artifact import (
    build_calibration_artifact,
    render_methods_note,
)


def _record(
    *,
    model: str,
    samples_by_class: dict[str, list[tuple[float, bool]]],
    ece: float,
    precision_at_auto_apply: float,
    auto_apply_count: int,
) -> dict[str, object]:
    return {
        "provider": "azure",
        "model": model,
        "dataset": "hospital",
        "precision": 0.5,
        "recall": 0.4,
        "f1": 0.44,
        "ece": ece,
        "precision_at_auto_apply": precision_at_auto_apply,
        "auto_apply_count": auto_apply_count,
        "calibration_samples_by_class": {
            k: [list(s) for s in v] for k, v in samples_by_class.items()
        },
    }


class TestBuildCalibrationArtifact:
    def _artifact(self) -> dict[str, object]:
        reliable = _record(
            model="gpt-5-mini-min",
            samples_by_class={"value_format": [(0.99, True)] * 200},
            ece=0.03,
            precision_at_auto_apply=0.99,
            auto_apply_count=200,
        )
        unreliable = _record(
            model="gpt-5-mini-med",
            samples_by_class={"other": [(0.9, i % 2 == 0) for i in range(200)]},
            ece=0.8,
            precision_at_auto_apply=0.1,
            auto_apply_count=50,
        )
        return build_calibration_artifact(
            {"minimal": reliable, "medium": unreliable},
            alphas=(0.01, 0.05, 0.1, 0.2),
            delta=0.05,
            min_support=30,
            splits=40,
        )

    def test_has_both_conditions(self) -> None:
        art = self._artifact()
        assert set(art["conditions"]) == {"minimal", "medium"}

    def test_alpha_sweep_monotone_and_complete(self) -> None:
        art = self._artifact()
        sweep = art["conditions"]["minimal"]["alpha_sweep"]
        assert [row["alpha"] for row in sweep] == [0.01, 0.05, 0.1, 0.2]
        covs = [row["overall_test_coverage"] for row in sweep]
        assert covs == sorted(covs)  # looser alpha => at least as much coverage

    def test_reliable_condition_certifies_unreliable_does_not(self) -> None:
        art = self._artifact()
        assert art["conditions"]["minimal"]["promotion_verdict"]["promoted"] is True
        assert art["conditions"]["medium"]["promotion_verdict"]["promoted"] is False
        # The unreliable condition certifies no coverage at a strict alpha.
        med_primary = art["conditions"]["medium"]["certified_coverage"]["overall_test_coverage"]
        assert med_primary == 0.0

    def test_risk_coverage_and_reliability_present(self) -> None:
        art = self._artifact()
        med = art["conditions"]["medium"]
        assert 0.0 <= med["risk_coverage"]["aurc"] <= 1.0
        assert med["reliability"]["ece"] is not None
        assert med["repeated_split_validity"]["over_alpha_rate"] <= 0.05 + 0.05

    def test_per_class_support_reported(self) -> None:
        art = self._artifact()
        assert art["conditions"]["minimal"]["per_class_support"]["value_format"] == 200

    def test_artifact_is_json_serializable(self) -> None:
        json.dumps(self._artifact())

    def test_methods_note_renders_key_facts(self) -> None:
        note = render_methods_note(self._artifact())
        assert "AURC" in note
        assert "gpt-5-mini-min" in note
        assert "gpt-5-mini-med" in note
        assert "Conclusion:" in note
        assert "conformal" in note.lower()
