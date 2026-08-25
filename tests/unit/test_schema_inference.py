"""Tests for reviewable schema inference artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.schema_inference import (
    ConstraintCandidate,
    ConstraintReviewError,
    build_constraint_review_artifact,
    constraint_candidate_id,
    dump_constraint_review_artifact,
    infer_schema,
    load_constraint_review_artifact,
    update_constraint_review_artifact,
    validate_constraint_review_artifact,
    write_constraint_review_artifact_atomic,
)
from dataforge.table import Table


def test_infer_schema_reports_types_bounds_and_reviewable_fds() -> None:
    """Inference emits candidates without directly mutating repair behavior."""
    table = Table(
        ["city", "state", "amount"],
        [
            {"city": "Boston", "state": "MA", "amount": "10.0"},
            {"city": "Boston", "state": "MA", "amount": "11.0"},
            {"city": "Seattle", "state": "WA", "amount": "12.5"},
            {"city": "Seattle", "state": "WA", "amount": "13.0"},
            {"city": "Austin", "state": "TX", "amount": "9.5"},
        ],
    )

    result = infer_schema(table)
    schema = result.to_schema(include_inferred_constraints=True)

    assert result.columns["amount"] == "float"
    assert any(candidate.kind == "domain_bound" for candidate in result.candidates)
    assert any(
        candidate.kind == "functional_dependency"
        and candidate.columns == ("city",)
        and candidate.dependent == "state"
        for candidate in result.candidates
    )
    assert schema.column_type("amount") == "float"
    assert schema.functional_dependencies


def test_infer_schema_default_schema_excludes_constraints_until_reviewed() -> None:
    """Default conversion preserves inferred types but not review-required constraints."""
    table = Table(
        ["city", "state"],
        [
            {"city": "Boston", "state": "MA"},
            {"city": "Boston", "state": "MA"},
            {"city": "Seattle", "state": "WA"},
            {"city": "Seattle", "state": "WA"},
            {"city": "Austin", "state": "TX"},
        ],
    )

    schema = infer_schema(table).to_schema()

    assert schema.columns == {"city": "str", "state": "str"}
    assert schema.functional_dependencies == ()


def test_constraint_review_artifact_is_pending_stable_and_strict() -> None:
    """Profile inference can be serialized into a deterministic review artifact."""
    table = Table(
        ["code", "name"],
        [
            {"code": "A", "name": "Alpha"},
            {"code": "A", "name": "Alpha"},
            {"code": "B", "name": "Beta"},
            {"code": "B", "name": "Beta"},
            {"code": "C", "name": "Gamma"},
        ],
    )
    inference = infer_schema(table)

    artifact = build_constraint_review_artifact(
        inference,
        source_path=__file__,
        source_sha256="0" * 64,
    )
    repeated = build_constraint_review_artifact(
        inference,
        source_path=__file__,
        source_sha256="0" * 64,
    )

    assert artifact.schema_version == "constraint_review_v1"
    assert {candidate.decision for candidate in artifact.candidates} == {"pending"}
    assert len({candidate.candidate_id for candidate in artifact.candidates}) == len(
        artifact.candidates
    )
    assert dump_constraint_review_artifact(artifact) == dump_constraint_review_artifact(repeated)


def test_constraint_review_updates_decisions_notes_and_keeps_order() -> None:
    """Review updates are explicit and preserve deterministic artifact order."""
    table = Table(
        ["code", "name"],
        [
            {"code": "A", "name": "Alpha"},
            {"code": "A", "name": "Alpha"},
            {"code": "B", "name": "Beta"},
            {"code": "B", "name": "Beta"},
            {"code": "C", "name": "Gamma"},
        ],
    )
    artifact = build_constraint_review_artifact(
        infer_schema(table),
        source_path=__file__,
        source_sha256="0" * 64,
    )
    first_id = artifact.candidates[0].candidate_id
    second_id = artifact.candidates[1].candidate_id

    updated = update_constraint_review_artifact(
        artifact,
        accept_ids=(first_id,),
        reject_ids=(second_id,),
        notes={first_id: "reviewed"},
    )

    assert [candidate.candidate_id for candidate in updated.candidates] == [
        candidate.candidate_id for candidate in artifact.candidates
    ]
    assert updated.candidates[0].decision == "accepted"
    assert updated.candidates[0].review_note == "reviewed"
    assert updated.candidates[1].decision == "rejected"
    assert artifact.candidates[0].decision == "pending"


def test_constraint_review_rejects_unknown_and_conflicting_candidate_ids() -> None:
    """Unknown ids and conflicting decisions fail closed."""
    table = Table(
        ["id", "amount"],
        [{"id": "1", "amount": "10"}, {"id": "2", "amount": "11"}],
    )
    artifact = build_constraint_review_artifact(
        infer_schema(table),
        source_path=__file__,
        source_sha256="0" * 64,
    )
    candidate_id = artifact.candidates[0].candidate_id

    with pytest.raises(ConstraintReviewError, match="Unknown candidate ids"):
        update_constraint_review_artifact(artifact, accept_ids=("cnd-0000000000000000",))

    with pytest.raises(ConstraintReviewError, match="conflicting review decisions"):
        update_constraint_review_artifact(
            artifact,
            accept_ids=(candidate_id,),
            reject_ids=(candidate_id,),
        )


def test_constraint_review_rejects_duplicate_and_tampered_ids() -> None:
    """Artifact integrity checks catch duplicate ids and payload/id drift."""
    table = Table(
        ["id", "amount"],
        [{"id": "1", "amount": "10"}, {"id": "2", "amount": "11"}],
    )
    artifact = build_constraint_review_artifact(
        infer_schema(table),
        source_path=__file__,
        source_sha256="0" * 64,
    )

    duplicate = artifact.model_copy(
        update={"candidates": [artifact.candidates[0], artifact.candidates[0]]}
    )
    with pytest.raises(ConstraintReviewError, match="duplicate candidate ids"):
        validate_constraint_review_artifact(duplicate)

    tampered = artifact.model_copy(
        update={
            "candidates": [
                artifact.candidates[0].model_copy(update={"candidate_id": "cnd-0000000000000000"})
            ]
        }
    )
    with pytest.raises(ConstraintReviewError, match="candidate id payload mismatch"):
        validate_constraint_review_artifact(tampered)


def test_constraint_review_atomic_write_round_trips(tmp_path: Path) -> None:
    """Atomic writes produce deterministic bytes that strict loading accepts."""
    table = Table(
        ["id", "amount"],
        [{"id": "1", "amount": "10"}, {"id": "2", "amount": "11"}],
    )
    artifact = build_constraint_review_artifact(
        infer_schema(table),
        source_path=__file__,
        source_sha256="0" * 64,
    )
    path = tmp_path / "constraints.json"

    written_sha256 = write_constraint_review_artifact_atomic(path, artifact)
    loaded, loaded_sha256 = load_constraint_review_artifact(path)

    assert loaded == artifact
    assert loaded_sha256 == written_sha256
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == "constraint_review_v1"


class TestConstantDependentsAreNotEmitted:
    """A single-valued column is determined by everything, so the dependency is vacuous.

    This is the rule scripts/bench/measure_deductive_coverage.py's oracle already applied,
    with the rationale "a single-valued column is determined by everything", while the miner
    that feeds it did not. On hospital that asymmetry produced 34 vacuous candidates of 119 --
    every one with RAHA's constant 'empty' token as the dependent -- each costing a human a
    review decision for no possible repair.

    It is NOT a precision improvement and must not be described as one: it lowers measured
    FD-set precision from 0.8655 to 0.8118 by removing candidates that are true-but-vacuous.
    See docs/trust/premise-quality-result.md.
    """

    def _table(self, dependent: list[str]) -> Table:
        determinant = ["a", "a", "b", "b", "c", "c"]
        return Table(
            ["det", "dep"],
            [{"det": d, "dep": p} for d, p in zip(determinant, dependent, strict=True)],
        )

    def test_a_constant_dependent_is_rejected(self) -> None:
        table = self._table(["x"] * 6)

        fds = [
            c
            for c in infer_schema(table).candidates
            if c.kind == "functional_dependency" and c.dependent == "dep"
        ]

        assert fds == [], (
            "a constant dependent is determined by everything; emitting it asks a human to adjudicate a tautology"
        )

    def test_a_two_valued_dependent_is_still_emitted(self) -> None:
        """Non-vacuity for the test above: the guard must reject only the degenerate case."""
        table = self._table(["x", "x", "y", "y", "y", "y"])

        fds = [
            c
            for c in infer_schema(table).candidates
            if c.kind == "functional_dependency" and c.dependent == "dep"
        ]

        assert len(fds) == 1
        assert fds[0].confidence == 1.0


class TestSupportStatisticsAreReportedNotGated:
    """The statistics were computed and then discarded into an English sentence.

    tested_confidence separates true from false dependencies perfectly on hospital where
    confidence does not (false at most 0.9554, true at least 0.9599). It is deliberately NOT
    a gate: the separating threshold is fitted to one corpus and no second corpus with false
    dependencies exists to validate it, so shipping a constant chosen that way is the
    overfitting docs/trust/constraint-circularity.md forbids.

    These tests pin that it is carried as a field, and that it is computed on the denominator
    that can actually falsify the dependency.
    """

    def test_tested_confidence_uses_only_rows_that_can_violate(self) -> None:
        """Singleton groups supply no evidence, so they must not dilute the violation rate.

        Four rows in two-row groups with one violation between them, plus eight singleton
        rows. The shipped score divides that one violation by all twelve rows and reports
        0.9167; the tested score divides by the four rows that can actually violate and
        reports 0.75. The gap is the inflation.

        The eight singletons are also what keeps the fixture above the miner's existing 0.9
        floor -- which is itself the point: padding a table with rows that cannot falsify a
        dependency raises its shipped confidence.
        """
        table = Table(
            ["det", "dep"],
            [
                {"det": "a", "dep": "x"},
                {"det": "a", "dep": "y"},  # the only violation
                {"det": "b", "dep": "z"},
                {"det": "b", "dep": "z"},
            ]
            + [{"det": f"s{i}", "dep": f"v{i}"} for i in range(8)],
        )

        fd = next(
            c
            for c in infer_schema(table).candidates
            if c.kind == "functional_dependency" and c.dependent == "dep"
        )

        assert fd.violations == 1
        assert fd.rows_in_multi_row_groups == 4
        assert fd.confidence == round(1 - 1 / 12, 4)
        assert fd.tested_confidence == round(1 - 1 / 4, 4)
        assert fd.tested_confidence < fd.confidence, (
            "singleton groups inflate the shipped confidence; that is the whole point of "
            "the tested denominator"
        )

    def test_majority_share_of_dependent_is_reported(self) -> None:
        """The reviewer needs to know whether the mode would have been right anyway."""
        table = Table(
            ["det", "dep"],
            [
                {"det": "a", "dep": "x"},
                {"det": "a", "dep": "x"},
                {"det": "b", "dep": "x"},
                {"det": "b", "dep": "x"},
                {"det": "c", "dep": "y"},
                {"det": "c", "dep": "y"},
            ],
        )

        fd = next(
            c
            for c in infer_schema(table).candidates
            if c.kind == "functional_dependency" and c.dependent == "dep"
        )

        assert fd.dependent_majority_share == round(4 / 6, 4)

    def test_the_statistics_do_not_gate_emission(self) -> None:
        """A low tested_confidence is reported, not suppressed.

        If this ever starts filtering, the change has silently acquired the fitted threshold
        the pre-registration's K3 forbade.
        """
        table = Table(
            ["det", "dep"],
            [
                {"det": "a", "dep": "x"},
                {"det": "a", "dep": "y"},
                {"det": "b", "dep": "z"},
                {"det": "b", "dep": "z"},
            ]
            + [{"det": f"s{i}", "dep": f"v{i}"} for i in range(8)],
        )

        fds = [
            c
            for c in infer_schema(table).candidates
            if c.kind == "functional_dependency" and c.dependent == "dep"
        ]

        assert fds, "the candidate must still be emitted"
        assert fds[0].tested_confidence is not None
        assert fds[0].tested_confidence < fds[0].confidence

    def test_the_fields_are_optional_so_older_artifacts_still_load(self) -> None:
        """Adding the fields rotates cnd- ids; it must not break reading an old artifact."""
        candidate = ConstraintCandidate(
            kind="functional_dependency",
            columns=("det",),
            dependent="dep",
            confidence=0.95,
            evidence="written before the support statistics existed",
        )

        assert candidate.tested_confidence is None
        assert candidate.rows_in_multi_row_groups is None
        assert constraint_candidate_id(candidate).startswith("cnd-")
