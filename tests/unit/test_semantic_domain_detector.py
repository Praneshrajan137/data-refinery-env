"""Tests for the semantic-domain detector: capability, and its structural inability to write.

The write-path assertions are the reason this file exists. An externally learned constraint
is the most plausible candidate yet for "surely *this* one can be trusted to write", and the
argument fails on a mechanism rather than on taste:
``verification_strength_for("deterministic", ...)`` returns ``proven`` regardless of schema,
so allowlisting a detector with a deterministic repairer would grant proven-strength writes
on statistical evidence.

The safeguard is therefore structural -- no repairer exists -- and structural safeguards
need tests through the real surfaces, because a function can be correct while its wiring is
not.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from dataforge.detectors import default_detectors, run_all_detectors
from dataforge.detectors.semantic_domain import (
    AUTOTEST_SDC_SHA256,
    AUTOTEST_SDC_URL,
    PatternSDC,
    SemanticDomainDetector,
    parse_pattern_sdcs,
)
from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS

_MONTH_SDC = PatternSDC(
    pattern=r"^[a-zA-Z]+$",
    coverage_threshold=0.8,
    confidence=0.97,
    example="january",
)
_NUMERIC_SDC = PatternSDC(
    pattern=r"^[0-9]+$",
    coverage_threshold=0.9,
    confidence=0.88,
    example="1234",
)

_ARTIFACT = (
    b"type\tpre-condition\tpost-condition\tconfidence\tSDC\n"
    b"Pattern\tp\tq\t0.99\t(('pattern', 1, 'january', '^[a-zA-Z]+$', 0.8), ('pattern', 0))\n"
    b"Embedding\tp\tq\t0.91\t(('sbert', 1, 'january', 0.8, 1.2), ('sbert', 1.375))\n"
    b"CTA\tp\tq\t0.85\t(('cta', 1, 'x', 0.8), ('cta', 0))\n"
)


class TestNoWritePath:
    """The invariants that make this detector safe by construction, not by policy."""

    def test_issue_type_is_not_constraint_checkable(self) -> None:
        """The allowlist is the soundness gate. This id must never join it."""
        assert "semantic_domain_violation" not in CONSTRAINT_CHECKABLE_DETECTORS

    def test_no_repairer_is_registered(self) -> None:
        from dataforge.repairers import build_repairers

        registry = build_repairers(cache_dir=None, allow_llm=False)
        assert "semantic_domain_violation" not in registry, (
            "a repairer would create a write path; the safeguard here is the absence of one"
        )

    def test_no_fix_reaches_the_repair_pipeline(self, tmp_path: Path) -> None:
        """Through the real surface, not the helper.

        A mutant that deleted a guard from the calling surface while leaving the guarded
        function intact has survived this project's tests before.
        """
        from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

        path = tmp_path / "domain.csv"
        pd.DataFrame(
            {
                "month": [
                    "january",
                    "february",
                    "march",
                    "april",
                    "may",
                    "june",
                    "july",
                    "1234",
                ]
            }
        ).to_csv(path, index=False)
        before = path.read_bytes()

        result = run_repair_pipeline(RepairPipelineRequest(source_path=path, mode="apply"))

        assert all(fix.detector_id != "semantic_domain_violation" for fix in result.fixes)
        assert path.read_bytes() == before, (
            "bytes on disk must be unchanged; asserting the receipt is not a proof"
        )

    def test_detector_is_absent_from_the_default_ensemble(self) -> None:
        """It needs a fetched artifact; the default ensemble stays offline."""
        assert all(
            type(detector).__name__ != "SemanticDomainDetector" for detector in default_detectors()
        )

    def test_run_all_detectors_never_emits_the_issue_type(self) -> None:
        frame = pd.DataFrame(
            {"month": ["january", "february", "march", "april", "may", "june", "july", "1234"]}
        )
        issues = run_all_detectors(frame)
        assert all(issue.issue_type != "semantic_domain_violation" for issue in issues)


class TestDetection:
    """The capability itself."""

    def _frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": [
                    "january",
                    "february",
                    "march",
                    "april",
                    "may",
                    "june",
                    "july",
                    "august",
                    "1234",
                ]
            }
        )

    def test_flags_the_value_outside_the_learned_domain(self) -> None:
        issues = SemanticDomainDetector((_MONTH_SDC,)).detect(self._frame())
        assert len(issues) == 1
        issue = issues[0]
        assert issue.row == 8
        assert issue.actual == "1234"
        assert issue.issue_type == "semantic_domain_violation"

    def test_confidence_is_the_offline_measured_precision(self) -> None:
        """The property that distinguishes an SDC from a locally inferred constraint."""
        issues = SemanticDomainDetector((_MONTH_SDC,)).detect(self._frame())
        assert issues[0].confidence == pytest.approx(0.97)

    def test_severity_is_always_review(self) -> None:
        """Never SAFE (invites bulk apply), never UNSAFE (overstates a statistical finding)."""
        from dataforge.detectors.base import Severity

        issues = SemanticDomainDetector((_MONTH_SDC,)).detect(self._frame())
        assert all(issue.severity is Severity.REVIEW for issue in issues)

    def test_carries_no_expected_value(self) -> None:
        """It knows the value is wrong, not what it should be. So it suggests nothing."""
        from dataforge.engine.repair import _DETECTION_ONLY_SUGGESTION_TYPES

        issues = SemanticDomainDetector((_MONTH_SDC,)).detect(self._frame())
        assert issues[0].expected is None
        assert "semantic_domain_violation" not in _DETECTION_ONLY_SUGGESTION_TYPES

    def test_reason_states_the_advisory_boundary(self) -> None:
        issues = SemanticDomainDetector((_MONTH_SDC,)).detect(self._frame())
        assert "Advisory" in issues[0].reason
        assert "never a basis for an automatic write" in issues[0].reason

    def test_precondition_below_threshold_means_the_sdc_does_not_apply(self) -> None:
        """Half text and half digits is not a text domain with one bad value."""
        frame = pd.DataFrame(
            {"mixed": ["january", "february", "march", "april", "1", "2", "3", "4"]}
        )
        assert SemanticDomainDetector((_MONTH_SDC,)).detect(frame) == []

    def test_short_columns_are_skipped(self) -> None:
        """'95% of values match' is satisfied by two coincidences."""
        frame = pd.DataFrame({"month": ["january", "february", "1234"]})
        assert SemanticDomainDetector((_MONTH_SDC,)).detect(frame) == []

    def test_boundary_coverage_exactly_at_threshold_applies(self) -> None:
        """>= not >. Off-by-one mutants have survived this suite before."""
        # 8 of 10 non-empty values match => coverage 0.8, exactly the threshold.
        frame = pd.DataFrame(
            {
                "month": [
                    "january",
                    "february",
                    "march",
                    "april",
                    "may",
                    "june",
                    "july",
                    "august",
                    "11",
                    "22",
                ]
            }
        )
        issues = SemanticDomainDetector((_MONTH_SDC,)).detect(frame)
        assert len(issues) == 2, "coverage == threshold must count as applicable"

    def test_highest_confidence_sdc_wins_a_cell(self) -> None:
        frame = pd.DataFrame(
            {"v": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "abc"]},
        )
        issues = SemanticDomainDetector((_NUMERIC_SDC, _MONTH_SDC)).detect(frame)
        assert len(issues) == 1
        assert issues[0].confidence == pytest.approx(0.88)

    def test_a_constraint_firing_on_most_of_a_column_is_suppressed(self) -> None:
        """Then it is describing a column it does not apply to, not finding many errors."""
        sdc = PatternSDC(pattern=r"^a$", coverage_threshold=0.0, confidence=0.9, example="a")
        frame = pd.DataFrame({"v": ["a"] + [f"b{i}" for i in range(9)]})
        assert SemanticDomainDetector((sdc,)).detect(frame) == []

    def test_empty_sdc_set_is_rejected(self) -> None:
        """A detector that never fires is indistinguishable from a precise one."""
        with pytest.raises(ValueError, match="at least one SDC"):
            SemanticDomainDetector(())


class TestArtifactParsing:
    """The artifact is tab-separated despite its .csv extension."""

    def test_only_the_pattern_family_is_loaded(self) -> None:
        result = parse_pattern_sdcs(_ARTIFACT)
        assert len(result.sdcs) == 1
        assert result.sdcs[0].pattern == r"^[a-zA-Z]+$"
        assert result.sdcs[0].coverage_threshold == 0.8

    def test_declined_families_are_counted_not_hidden(self) -> None:
        """Silently skipping 445 of 505 SDCs would misreport coverage."""
        result = parse_pattern_sdcs(_ARTIFACT)
        assert result.total_in_artifact == 3
        assert result.declined_by_family == {"Embedding": 1, "CTA": 1}
        assert result.declined_total == 2

    def test_comma_separated_input_is_rejected_rather_than_misparsed(self) -> None:
        with pytest.raises(ValueError, match="tab-separated"):
            parse_pattern_sdcs(b"type,pre-condition,post-condition,confidence\nPattern,p,q,0.9\n")

    def test_artifact_with_no_pattern_sdcs_raises(self) -> None:
        """Zero constraints is a failure, not an empty ensemble."""
        only_embedding = (
            b"type\tpre-condition\tpost-condition\tconfidence\tSDC\n"
            b"Embedding\tp\tq\t0.91\t(('sbert', 1, 'january', 0.8, 1.2), ('sbert', 1.375))\n"
        )
        with pytest.raises(ValueError, match="zero pattern constraints"):
            parse_pattern_sdcs(only_embedding)

    def test_unparseable_pattern_rows_are_declined_not_crashed_on(self) -> None:
        artifact = (
            b"type\tpre-condition\tpost-condition\tconfidence\tSDC\n"
            b"Pattern\tp\tq\t0.99\t(('pattern', 1, 'january', '^[a-zA-Z]+$', 0.8), ('pattern', 0))\n"
            b"Pattern\tp\tq\t0.98\tnot-a-tuple\n"
            b"Pattern\tp\tq\t0.97\t(('pattern', 1, 'x', '^[unclosed', 0.8), ('pattern', 0))\n"
        )
        result = parse_pattern_sdcs(artifact)
        assert len(result.sdcs) == 1
        assert result.declined_by_family["Pattern (unparseable)"] == 2

    def test_upstream_artifact_is_pinned_to_a_commit_and_a_digest(self) -> None:
        assert "refs/heads/" not in AUTOTEST_SDC_URL
        assert "4acf65cf37a506206bf2888dbd45f17e58dce2e2" in AUTOTEST_SDC_URL
        assert len(AUTOTEST_SDC_SHA256) == 64
