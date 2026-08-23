"""Regression tripwire for the corpus tiering and the anchor numbers.

Two gaps this closes.

**The anchor had no test.** `hospital` heuristic correction F1 0.7926 is cited in
`PRODUCT.md`, `README.md`, `CLAUDE.md`, `CHANGELOG.md`, `docs/STRATEGY.md`,
`DECISIONS.md` (fifteen times) and two specs, and is documented as a floor that "must
never regress". Grep found the number only in prose and in the committed artifact --
nothing asserted it. `scripts/ci/benchmark_truth.py` checks that the generated report
*agrees with* the artifact, which does not help if the artifact itself is overwritten by
a bad run.

**Tiering had no enforcement.** A tier that is only documented rots. These tests assert
that the tier assignments hold and that the fields carry their reasons.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.datasets.registry import (
    COLUMN_BENCHMARK_REGISTRY,
    DATASET_REGISTRY,
    headline_corpora,
    non_headline_corpora,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ARTIFACT = PROJECT_ROOT / "eval" / "results" / "agent_comparison.json"

# The documented floor. Asserted exactly, not as an inequality: the claim in the docs is
# that this specific measurement is reproducible, and an inequality would silently accept
# a number that drifted upward for the wrong reason (a relabelled corpus, a changed
# scorer) as readily as one that genuinely improved.
_HOSPITAL_HEURISTIC_F1 = 0.7926
_TOLERANCE = 1e-4


def _record(method: str, dataset: str) -> dict[str, object]:
    """Return one committed benchmark record."""
    payload = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
    for record in payload["records"]:
        if record.get("method") == method and record.get("dataset") == dataset:
            return dict(record)
    raise AssertionError(f"no committed record for {method}/{dataset}")


class TestAnchorNumbers:
    """The most-cited numbers in the project, asserted at last."""

    def test_hospital_heuristic_f1_matches_the_documented_anchor(self) -> None:
        record = _record("heuristic", "hospital")

        # Non-vacuity first. Without this the equality below could be satisfied by an
        # artifact of zeros if the scorer's zero-denominator convention ever changed.
        assert isinstance(record["tp"], int)
        assert record["tp"] > 0, "precondition: the anchor must rest on real true positives"
        assert record["fn"] == 58
        assert record["fp"] == 178

        f1 = record["f1"]
        assert isinstance(f1, float)
        assert abs(f1 - _HOSPITAL_HEURISTIC_F1) < _TOLERANCE, (
            f"hospital heuristic F1 is {f1}, documented anchor is {_HOSPITAL_HEURISTIC_F1}. "
            "This number is cited in PRODUCT.md, README.md, CLAUDE.md and two specs. If the "
            "change is intended, update those together with this test and record why."
        )

    def test_flights_heuristic_zero_is_recorded_as_measured(self) -> None:
        """The zero is real and load-bearing; it must not be quietly repaired away.

        `docs/trust/accuracy-frontier.md` argues this zero is honest abstention under a
        scoring rule that cannot represent abstention. It is evidence, not a bug.
        """
        record = _record("heuristic", "flights")
        assert record["f1"] == 0.0
        assert isinstance(record["fn"], int)
        assert record["fn"] > 0, "precondition: there were errors to miss"


class TestCorpusTiering:
    """Tier assignments, and the reasons that keep them from rotting."""

    def test_every_dataset_declares_provenance_tier_and_a_reason(self) -> None:
        for name, metadata in DATASET_REGISTRY.items():
            assert metadata.error_provenance in {
                "natural",
                "injected",
                "synthetic",
                "contested",
            }, name
            assert metadata.tier in {"headline", "tripwire", "diagnostic"}, name
            assert len(metadata.tier_reason) > 40, (
                f"{name}: a tier without a substantive reason is a tier that will rot"
            )

    def test_hospital_is_a_tripwire_not_a_flagship(self) -> None:
        """Its entire error model is one substituted character."""
        hospital = DATASET_REGISTRY["hospital"]
        assert hospital.error_provenance == "injected"
        assert hospital.tier == "tripwire"

    def test_tax_is_synthetic(self) -> None:
        assert DATASET_REGISTRY["tax"].error_provenance == "synthetic"

    def test_flights_is_contested(self) -> None:
        assert DATASET_REGISTRY["flights"].error_provenance == "contested"

    def test_no_raha_corpus_is_headline_tier(self) -> None:
        """Two of four have non-natural errors, one has contested labels, two are unmeasured."""
        raha_headline = sorted(name for name in DATASET_REGISTRY if name in headline_corpora())
        assert not raha_headline, (
            f"{raha_headline} are headline-tier, but no RAHA corpus currently qualifies"
        )

    def test_the_real_error_corpora_are_headline_tier(self) -> None:
        assert headline_corpora() == {"rt_bench", "st_bench"}

    def test_headline_and_non_headline_partition_every_corpus(self) -> None:
        every = set(DATASET_REGISTRY) | set(COLUMN_BENCHMARK_REGISTRY)
        assert headline_corpora() | non_headline_corpora() == every
        assert not (headline_corpora() & non_headline_corpora())

    @pytest.mark.parametrize("name", sorted(COLUMN_BENCHMARK_REGISTRY))
    def test_column_benchmarks_are_detection_axis_only(self, name: str) -> None:
        """Headline for detection must never be read as headline for correction."""
        metadata = COLUMN_BENCHMARK_REGISTRY[name]
        assert metadata.axis == "detection"
        assert "DETECTION" in metadata.tier_reason or "detection" in metadata.tier_reason


class TestNoImpossibleRemediations:
    """A tier_reason must not promise something the corpus cannot support.

    `flights` carried "Diagnostic until re-scored under specs/SPEC_abstention_scoring.md" for
    a day. That rule requires a `ground_truth_debatable` label class and RAHA ships none, so
    the promise could never be kept -- and it sat in the registry where it reads as a plan.

    A promise embedded in code is worse than one in prose: prose gets re-read sceptically,
    code gets trusted.
    """

    def test_no_raha_corpus_promises_abstention_rescoring(self) -> None:
        """The three-way rule needs a debatable class. Only the column benchmarks have one."""
        for name, metadata in DATASET_REGISTRY.items():
            reason = metadata.tier_reason
            mentions_spec = "SPEC_abstention_scoring" in reason
            if not mentions_spec:
                continue
            # Mentioning the spec is fine; promising to apply it is not. Require that the
            # impossibility is stated wherever the spec is named.
            assert "CANNOT" in reason or "cannot" in reason, (
                f"{name}: tier_reason names SPEC_abstention_scoring without recording that "
                "RAHA ships no ground_truth_debatable class, so the rescoring is impossible"
            )

    def test_only_corpora_with_a_debatable_class_may_claim_abstention_scoring(self) -> None:
        """The scoring spec is claimed only by corpora that can satisfy it."""
        for name, metadata in COLUMN_BENCHMARK_REGISTRY.items():
            assert metadata.scoring_spec == "specs/SPEC_abstention_scoring.md", name
        # Precondition: at least one corpus must genuinely support the rule, or this test
        # passes by there being nothing to check.
        assert COLUMN_BENCHMARK_REGISTRY, "precondition: a three-way corpus must be registered"
