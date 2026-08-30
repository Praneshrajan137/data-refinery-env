"""The entailment witness must model the repairer's write decision exactly.

A witness that disagreed with the repairer would be worse than no witness: it would show a
reviewer a consequence that does not happen, or hide one that does. So these tests pin the
three parts of the decision that are easy to get subtly wrong -- strict majority, canonical
precedence, and the early return -- plus the mechanism the whole design rests on.

The end-to-end validation is `scripts/bench/measure_entailment_witness.py`, which reproduces
hospital's measured 567 writes / 451 repairs / 116 corruptions exactly. These are the unit
fences under it, because a harness that agrees on aggregate can still disagree per cell.
"""

from __future__ import annotations

from dataforge.detectors.base import FunctionalDependency
from dataforge.table import Table
from dataforge.witness import (
    MAX_WITNESS_VALUES,
    GroupDistribution,
    blast_radius,
    fd_label,
    marginal_blast_radius,
    summarise,
    witness_for_cell,
)


def _table(rows: list[dict[str, str]]) -> Table:
    return Table(list(rows[0].keys()), rows)


def _fd(determinant: str, dependent: str) -> FunctionalDependency:
    return FunctionalDependency(determinant=[determinant], dependent=dependent)


class TestStrictMajority:
    """More than half the group, never a plurality.

    Mutant M16 records the stakes: plurality writes on 2 votes of 5 across four distinct
    values with `deterministic` provenance that bypasses calibration, and is worse on every
    measured axis -- clean cells corrupted 731 against 344 on flights.
    """

    def test_a_clear_majority_writes(self) -> None:
        table = _table(
            [
                {"state": "MA", "city": "boston"},
                {"state": "MA", "city": "boston"},
                {"state": "MA", "city": "bostonn"},
            ]
        )

        witness = witness_for_cell(table, 2, "city", (_fd("state", "city"),))

        assert witness is not None
        assert witness.new_value == "boston"
        assert witness.old_value == "bostonn"
        assert witness.support == 2

    def test_a_plurality_that_is_not_a_majority_writes_nothing(self) -> None:
        """Two of five across four values: a plurality, not a majority."""
        table = _table(
            [
                {"state": "MA", "city": "boston"},
                {"state": "MA", "city": "boston"},
                {"state": "MA", "city": "worcester"},
                {"state": "MA", "city": "lowell"},
                {"state": "MA", "city": "bostonn"},
            ]
        )

        assert witness_for_cell(table, 4, "city", (_fd("state", "city"),)) is None

    def test_an_exact_tie_writes_nothing(self) -> None:
        """Half is not more than half."""
        table = _table(
            [
                {"state": "MA", "city": "boston"},
                {"state": "MA", "city": "worcester"},
            ]
        )

        assert witness_for_cell(table, 0, "city", (_fd("state", "city"),)) is None


class TestTheHarmMechanism:
    """A false dependency is inert where its group holds no disagreement.

    This is the finding the whole design rests on -- `PRODUCT.md`:186-190 -- reduced to two
    tests. Measured at scale in `docs/trust/entailment-witness-result.md`: three
    equally-false dependencies sharing the `ZipCode` determinant, and only the one whose
    groups disagree causes any corruption.
    """

    def test_a_unanimous_group_writes_nothing_however_false_the_dependency(self) -> None:
        """`ZipCode -> Address1` in miniature: false, and harmless."""
        table = _table(
            [
                {"zip": "02101", "address": "1 Main St"},
                {"zip": "02101", "address": "1 Main St"},
                {"zip": "02102", "address": "2 Oak Ave"},
            ]
        )

        assert blast_radius(table, (_fd("zip", "address"),)) == []

    def test_a_disagreeing_group_writes_and_the_witness_says_what_it_destroys(self) -> None:
        """`ZipCode -> ProviderNumber` in miniature: equally false, and harmful."""
        table = _table(
            [
                {"zip": "02101", "provider": "A"},
                {"zip": "02101", "provider": "A"},
                {"zip": "02101", "provider": "B"},
            ]
        )

        witnesses = blast_radius(table, (_fd("zip", "provider"),))

        assert len(witnesses) == 1
        assert witnesses[0].new_value == "A"
        assert witnesses[0].old_value == "B"
        assert witnesses[0].destroys == 1, "the count a reviewer needs: values this destroys"

    def test_a_singleton_group_writes_nothing(self) -> None:
        table = _table([{"zip": "02101", "provider": "A"}, {"zip": "02102", "provider": "B"}])

        assert blast_radius(table, (_fd("zip", "provider"),)) == []


class TestCanonicalPrecedenceAndTheEarlyReturn:
    """First applicable dependency in determinant-name order wins, and the search stops.

    `FDViolationRepairer._propose` returns on the first match. So if the first applicable
    dependency's majority already equals the cell's value, NO write happens -- even where a
    later dependency would have written one. Getting this wrong makes the witness over-report,
    which would train a reviewer to dismiss it.
    """

    def test_the_alphabetically_first_determinant_acts(self) -> None:
        table = _table(
            [
                {"aaa": "1", "zzz": "9", "target": "x"},
                {"aaa": "1", "zzz": "8", "target": "x"},
                {"aaa": "1", "zzz": "8", "target": "y"},
            ]
        )

        witness = witness_for_cell(table, 2, "target", (_fd("zzz", "target"), _fd("aaa", "target")))

        assert witness is not None
        assert witness.constraint == "aaa -> target"

    def test_agreement_under_the_first_dependency_stops_the_search(self) -> None:
        """The early return, isolated.

        Under `aaa` the majority is `x` and row 0 already holds `x`, so nothing is written.
        Under `zzz` alone, row 0 would be rewritten to `y`. A witness that fell through to
        `zzz` would report a write the repairer does not make.
        """
        table = _table(
            [
                {"aaa": "1", "zzz": "8", "target": "x"},
                {"aaa": "1", "zzz": "8", "target": "y"},
                {"aaa": "1", "zzz": "8", "target": "y"},
                {"aaa": "1", "zzz": "7", "target": "x"},
                {"aaa": "1", "zzz": "7", "target": "x"},
            ]
        )
        fds = (_fd("aaa", "target"), _fd("zzz", "target"))

        assert witness_for_cell(table, 0, "target", fds) is None
        # Non-vacuity: `zzz` alone really would have written, so the None above is precedence
        # rather than an empty premise.
        alone = witness_for_cell(table, 0, "target", (_fd("zzz", "target"),))
        assert alone is not None and alone.new_value == "y"


class TestMarginalBlastRadius:
    """The reviewer-facing quantity is marginal, because precedence makes it non-additive."""

    def test_a_candidate_masked_by_an_accepted_dependency_adds_nothing(self) -> None:
        table = _table(
            [
                {"aaa": "1", "zzz": "8", "target": "x"},
                {"aaa": "1", "zzz": "8", "target": "y"},
                {"aaa": "1", "zzz": "8", "target": "y"},
                {"aaa": "1", "zzz": "7", "target": "x"},
                {"aaa": "1", "zzz": "7", "target": "x"},
            ]
        )

        marginal = marginal_blast_radius(table, (_fd("aaa", "target"),), _fd("zzz", "target"))

        assert marginal == [], "an earlier-sorted accepted determinant masks the candidate"

    def test_a_candidate_that_pre_empts_an_accepted_dependency_reports_the_change(self) -> None:
        """A candidate sorting earlier can change what an accepted dependency would write.

        Replacing one write with a different one is a consequence of the acceptance, not a
        no-op, so it must appear in the marginal set.
        """
        table = _table(
            [
                {"aaa": "1", "zzz": "8", "target": "x"},
                {"aaa": "1", "zzz": "8", "target": "x"},
                {"aaa": "1", "zzz": "8", "target": "y"},
                {"aaa": "2", "zzz": "8", "target": "z"},
                {"aaa": "2", "zzz": "8", "target": "z"},
            ]
        )

        marginal = marginal_blast_radius(table, (_fd("zzz", "target"),), _fd("aaa", "target"))

        assert marginal, "the candidate sorts first and changes the written value"
        assert all(w.constraint == "aaa -> target" for w in marginal)

    def test_a_candidate_on_an_untouched_column_adds_its_whole_radius(self) -> None:
        """Non-vacuity: marginal must not be empty whenever nothing masks the candidate."""
        table = _table(
            [
                {"k": "1", "a": "x", "b": "p"},
                {"k": "1", "a": "x", "b": "p"},
                {"k": "1", "a": "y", "b": "q"},
            ]
        )

        marginal = marginal_blast_radius(table, (_fd("k", "a"),), _fd("k", "b"))

        assert len(marginal) == 1
        assert marginal[0].column == "b"


class TestWitnessIsBounded:
    """A witness travels inside receipts and attestations, so its size is a budget."""

    def test_group_distribution_caps_values_and_says_so(self) -> None:
        from collections import Counter

        counts = Counter({f"v{index}": index + 1 for index in range(MAX_WITNESS_VALUES + 5)})

        distribution = GroupDistribution.from_counts(counts)

        assert len(distribution.values) == MAX_WITNESS_VALUES
        assert distribution.truncated is True
        assert distribution.group_size == sum(counts.values()), (
            "the dropped shape must still be recoverable from group_size"
        )

    def test_a_small_group_is_not_marked_truncated(self) -> None:
        from collections import Counter

        distribution = GroupDistribution.from_counts(Counter({"a": 2, "b": 1}))

        assert distribution.truncated is False
        assert distribution.group_size == 3


class TestSummarise:
    def test_counts_are_grouped_by_constraint_and_column(self) -> None:
        table = _table(
            [
                {"k": "1", "a": "x", "b": "p"},
                {"k": "1", "a": "x", "b": "p"},
                {"k": "1", "a": "y", "b": "q"},
            ]
        )

        summary = summarise(blast_radius(table, (_fd("k", "a"), _fd("k", "b"))))

        assert summary["cells_written"] == 2
        assert summary["by_column"] == {"a": 1, "b": 1}
        assert summary["by_constraint"] == {"k -> a": 1, "k -> b": 1}
        assert summary["values_destroyed"] == 2

    def test_an_empty_radius_summarises_to_zero(self) -> None:
        assert summarise([]) == {
            "cells_written": 0,
            "values_destroyed": 0,
            "by_constraint": {},
            "by_column": {},
        }


class TestLabels:
    def test_fd_label_matches_the_measurement_harness(self) -> None:
        """Witness output and the bench harness must name the same dependency identically."""
        assert fd_label(_fd("state", "city")) == "state -> city"
        assert fd_label(FunctionalDependency(determinant=["a", "b"], dependent="c")) == "a + b -> c"


class TestTheWitnessCannotDecideAnything:
    """Criterion F3, as a test rather than a promise.

    The witness is evidence ABOUT a write, never an input to whether it happens. When the
    witness was first measured it was imported by nothing, which made "no verdict changed"
    checkable by grep instead of by re-running the hours-long K4 arms.

    It is now imported by `dataforge/cli/constraints.py`, to show a reviewer what accepting a
    dependency would do to their own table. That is display only: the reviewer's decision is
    still theirs, the artifact still records exactly what they chose, and nothing on the
    repair path reads a witness.

    So the tripwire is narrowed rather than removed. The DECISION path -- the modules that
    determine whether a write happens and how it is labelled -- must stay witness-free. If
    one of them ever imports it, this test fails and the K4 fence (FD counts 53/81/85,
    repairs 393/451/451, corruptions 0/86/116, `replication_mismatches` 0) must be re-run
    before the change ships. Deleting the test instead would be the failure mode it exists
    to prevent.
    """

    #: Modules that decide whether a write happens, or what strength it is labelled with.
    #: A witness reaching any of these turns evidence into an input, which F3 forbids.
    DECISION_PATH = (
        "dataforge/safety",
        "dataforge/verifier",
        "dataforge/repairers",
        "dataforge/detectors",
        "dataforge/agent",
        "dataforge/stores",
        "dataforge/engine",
        "dataforge/conformal.py",
        "dataforge/release/corrector_gate.py",
    )

    def test_no_decision_path_module_imports_the_witness(self) -> None:
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        offenders: list[str] = []
        for area in self.DECISION_PATH:
            target = root / area
            candidates = [target] if target.is_file() else sorted(target.rglob("*.py"))
            for path in candidates:
                if "__pycache__" in path.parts:
                    continue
                text = path.read_text(encoding="utf-8", errors="ignore")
                if "dataforge.witness" in text or "from dataforge import witness" in text:
                    offenders.append(str(path.relative_to(root)))

        assert offenders == [], (
            "the witness reached a decision-path module, so it is no longer only evidence. "
            "Criterion F3 now requires re-running the K4 fence before this ships: "
            f"{offenders}"
        )

    def test_the_reviewer_surface_is_allowed_and_is_actually_wired(self) -> None:
        """Non-vacuity in both directions.

        Without this, deleting the consequence preview would leave the test above passing and
        nothing would record that the witness had ever reached a user.
        """
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        cli = (root / "dataforge" / "cli" / "constraints.py").read_text(encoding="utf-8")

        assert "dataforge.witness" in cli, (
            "the reviewer consequence preview is gone; the witness no longer reaches a human"
        )
