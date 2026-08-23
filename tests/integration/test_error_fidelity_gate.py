"""Executable counterpart to ``specs/SPEC_error_fidelity.md``.

Every condition F1-F4 is asserted here, in both directions where that is meaningful. The
spec's refusal is a claim; this file is what makes it falsifiable.

The condition that matters most is F4. A corpus on which no detector fires correlates
perfectly with any other such corpus, so without F4 the gate would certify the vacuous
corruption oracle this project already shipped once -- one that "generated clean columns so
tightly clustered no correct cell could be flagged".
"""

from __future__ import annotations

import pytest

from dataforge.datasets.column_corpus import BenchmarkColumn, ColumnBenchmark
from dataforge.datasets.inject import (
    FIDELITY_MIN_FIRING_DETECTORS,
    FIDELITY_MIN_RANK_CORRELATION,
    FidelityError,
    FidelityVerdict,
    GeneratedCorpus,
    assess_fidelity,
    generate_character_noise_corpus,
)
from dataforge.datasets.registry import COLUMN_BENCHMARK_REGISTRY


def _column(
    index: int,
    values: tuple[str, ...],
    truth: tuple[str, ...] = (),
    header: str = "col",
) -> BenchmarkColumn:
    """Build one benchmark column."""
    return BenchmarkColumn(
        index=index,
        header=header,
        distinct_values=values,
        ground_truth=frozenset(truth),
        debatable=frozenset(),
        declared_value_count=len(set(values)),
    )


def _corpus(
    *columns: BenchmarkColumn,
    provenance: str = "natural",
    name: str = "rt_bench",
) -> ColumnBenchmark:
    """Build a synthetic corpus with a chosen error provenance."""
    metadata = COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
        update={"name": name, "error_provenance": provenance}
    )
    return ColumnBenchmark(
        metadata=metadata,
        columns=columns,
        quarantined=(),
        sha256="a" * 64,
        padded_rows_discarded=0,
        value_count_mismatches=0,
    )


def _numeric_column(index: int, bad: str) -> BenchmarkColumn:
    """A numeric column with one non-numeric value, which several detectors notice."""
    values = tuple(str(i) for i in range(12)) + (bad,)
    return _column(index, values, truth=(bad,), header="amount")


def _rich_columns() -> tuple[BenchmarkColumn, ...]:
    """A corpus exercising four detectors at two distinct precisions.

    Built from measured detector behaviour rather than guessed: a numeric column with a
    placeholder wakes TypeMismatch and MissingValue, a phone-shaped column wakes
    FormatViolation and MissingValue, and an extreme numeric wakes Outlier.

    Precision must *vary* across detectors or the rank correlation is undefined for want of
    variance, so the extreme value is deliberately left unlabelled -- which is also what
    really happens: Outlier scored 0.0000 precision on both real corpora.

    Values are kept distinct within a column because `dist_val` is a distinct-value list
    upstream, so a repeated value would wake DuplicateRowDetector in a way no real
    benchmark row can.
    """
    return (
        _column(
            0,
            tuple(str(i) for i in range(12)) + ("N/A",),
            truth=("N/A",),
            header="amount",
        ),
        _column(
            1,
            tuple(f"217-555-010{i}" for i in range(9)) + ("not available",),
            truth=("not available",),
            header="phone_number",
        ),
        _column(
            2,
            tuple(str(100 + i) for i in range(12)) + ("999999",),
            truth=(),
            header="reading",
        ),
    )


class TestReferenceIntegrity:
    """Fidelity against injected data would be circular."""

    def test_injected_reference_is_refused_outright(self) -> None:
        reference = _corpus(_numeric_column(0, "N/A"), provenance="injected")
        generated = GeneratedCorpus(
            benchmark=_corpus(_numeric_column(0, "N/A")),
            generator_id="stub",
            seed=1,
            injected_values=1,
        )
        with pytest.raises(FidelityError, match="circular"):
            assess_fidelity(generated, reference)

    @pytest.mark.parametrize("provenance", ("synthetic", "contested"))
    def test_non_natural_references_are_refused(self, provenance: str) -> None:
        reference = _corpus(_numeric_column(0, "N/A"), provenance=provenance)
        generated = GeneratedCorpus(
            benchmark=_corpus(_numeric_column(0, "N/A")),
            generator_id="stub",
            seed=1,
            injected_values=1,
        )
        with pytest.raises(FidelityError):
            assess_fidelity(generated, reference)


class TestF4NonVacuity:
    """The condition without which every other condition can be satisfied vacuously."""

    def test_a_corpus_nothing_fires_on_is_refused(self) -> None:
        """The empty-oracle bug, caught by the gate rather than certified by it."""
        clean = _corpus(_column(0, ("alpha", "beta", "gamma")))
        generated = GeneratedCorpus(
            benchmark=_corpus(_column(0, ("delta", "epsilon", "zeta"))),
            generator_id="empty_oracle",
            seed=1,
            injected_values=0,
        )
        verdict = assess_fidelity(generated, clean)

        assert verdict.status == "REFUSED"
        assert verdict.firing_detectors < FIDELITY_MIN_FIRING_DETECTORS, "precondition"
        assert any(condition.startswith("F4") for condition in verdict.failed_conditions)

    def test_f4_is_reported_even_when_correlation_looks_perfect(self) -> None:
        """A vacuous corpus must not present a clean correlation as evidence."""
        clean = _corpus(_column(0, ("alpha", "beta", "gamma")))
        generated = GeneratedCorpus(
            benchmark=_corpus(_column(0, ("delta", "epsilon", "zeta"))),
            generator_id="empty_oracle",
            seed=1,
            injected_values=0,
        )
        verdict = assess_fidelity(generated, clean)
        assert not verdict.admissible
        f4 = [c for c in verdict.failed_conditions if c.startswith("F4")]
        assert f4, "F4 must be named explicitly, not folded into a generic refusal"


class TestVerdictContract:
    """What a verdict carries, and what it refuses to authorise."""

    def _verdict(self) -> FidelityVerdict:
        reference = _corpus(*_rich_columns())
        generated = GeneratedCorpus(
            benchmark=_corpus(*_rich_columns()),
            generator_id="identity_stub",
            seed=42,
            injected_values=2,
        )
        return assess_fidelity(generated, reference)

    def test_verdict_is_bound_to_generator_seed_and_reference(self) -> None:
        """Varying any of the three is threshold-shopping by another route."""
        verdict = self._verdict()
        assert verdict.generator_id == "identity_stub"
        assert verdict.seed == 42
        assert verdict.reference_sha256 == "a" * 64
        assert any("Bound to" in limit for limit in verdict.limitations)

    def test_verdict_carries_the_detection_only_limit(self) -> None:
        """The gap most likely to be forgotten: detection fidelity is not correction fidelity."""
        verdict = self._verdict()
        assert any("DETECTION only" in limit for limit in verdict.limitations)
        assert any("CORRECTION" in limit for limit in verdict.limitations)

    def test_verdict_refuses_to_authorise_promotion_or_a_write(self) -> None:
        verdict = self._verdict()
        joined = " ".join(verdict.limitations)
        assert "diagnostic tier" in joined
        assert "CONSTRAINT_CHECKABLE_DETECTORS" in joined

    def test_identical_corpora_pass(self) -> None:
        """Sanity floor: a corpus compared against itself must be admissible.

        Without this the suite could pass with a gate that refuses everything, which would
        be safe and useless.
        """
        verdict = self._verdict()
        assert verdict.status == "PASSED", verdict.failed_conditions
        assert verdict.rank_correlation is not None
        assert verdict.rank_correlation >= FIDELITY_MIN_RANK_CORRELATION
        assert verdict.max_precision_gap == 0.0


class TestBaselineGenerator:
    """The naive generator exists so the gate has something to refuse."""

    def test_generator_refuses_to_emit_an_empty_corpus(self) -> None:
        """An empty injection would compare favourably against anything."""
        sparse = _corpus(_column(0, ("only",)))
        with pytest.raises(FidelityError, match="zero errors"):
            generate_character_noise_corpus(sparse, rate=0.0)

    def test_generator_never_labels_a_naturally_labelled_value(self) -> None:
        """An injected error must not be confusable with a natural one."""
        reference = _corpus(
            _column(0, ("alpha", "beta", "gamma", "delta", "natural_bad"), truth=("natural_bad",))
        )
        generated = generate_character_noise_corpus(reference, rate=1.0)
        for column in generated.benchmark.columns:
            assert "natural_bad" not in column.ground_truth
            assert "natural_bad" not in column.distinct_values

    def test_generator_is_reproducible_under_a_seed(self) -> None:
        reference = _corpus(_column(0, tuple(f"value{i}" for i in range(20))))
        first = generate_character_noise_corpus(reference, seed=5, rate=0.5)
        again = generate_character_noise_corpus(reference, seed=5, rate=0.5)
        assert (
            first.benchmark.columns[0].distinct_values == again.benchmark.columns[0].distinct_values
        )

    def test_generated_corpus_is_diagnostic_tier(self) -> None:
        """Fidelity shows a corpus is shaped like real data, not that it is real data."""
        reference = _corpus(_column(0, tuple(f"value{i}" for i in range(20))))
        generated = generate_character_noise_corpus(reference, rate=0.5)
        assert generated.benchmark.metadata.tier == "diagnostic"


class TestPreRegisteredThresholds:
    """Thresholds are constants so they cannot be tuned to a result."""

    def test_thresholds_match_the_pre_registration(self) -> None:
        from dataforge.datasets.inject import (
            FIDELITY_MAX_COVERAGE_MISMATCH,
            FIDELITY_MAX_PRECISION_GAP,
        )

        assert FIDELITY_MIN_RANK_CORRELATION == 0.70
        assert FIDELITY_MAX_PRECISION_GAP == 0.25
        assert FIDELITY_MAX_COVERAGE_MISMATCH == 1
        assert FIDELITY_MIN_FIRING_DETECTORS == 3
