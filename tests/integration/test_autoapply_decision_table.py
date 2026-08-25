"""The auto-apply decision table, executable.

Normative companion to ``specs/SPEC_autoapply_decision.md``. Each ``_CASES`` entry is one
row of that table; each row is asserted against **bytes on disk**, on **both write
surfaces**.

Why bytes rather than ``receipt.applied``: because ``applied`` is a field on a model, and
on 2026-08-22 a test asserting ``result.receipt.applied is True`` passed while the write
was held -- the receipt and the file had drifted apart. The file is the thing the user
loses. Assert the file.

Why both surfaces in one parametrisation rather than two test modules: because the defect
being prevented was precisely that the two surfaces disagreed, and a shared table makes
disagreement a failing row instead of a missing test.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

import dataforge.agent.controller as agent_controller
from dataforge.cli.common import load_schema
from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS
from dataforge.engine.repair import (
    CellFix,
    ProposedFix,
    RepairPipelineRequest,
    UncheckableDetectorWriteError,
    apply_transaction,
    build_repairers,
    corrector_default_policy,
    partition_auto_apply,
    run_repair_pipeline,
)

Surface = Literal["legacy", "agent"]
Disposition = Literal["write", "no_write"]

_FD_ROWS = 9
_FD_BODY = "".join(f"{i},MA,boston\n" for i in range(1, _FD_ROWS + 1))
_FD_SCHEMA = (
    "columns:\n  id: string\n  state: string\n  city: string\n"
    "functional_dependencies:\n  - determinant: [state]\n    dependent: city\n"
)
_FD_SCHEMA_NOT_NULL = (
    "columns:\n  id: string\n  state: string\n  city: string\nnot_null_columns: [city]\n"
)
_FD_SCHEMA_FD_AND_NOT_NULL = (
    "columns:\n  id: string\n  state: string\n  city: string\n"
    "not_null_columns: [city]\n"
    "functional_dependencies:\n  - determinant: [state]\n    dependent: city\n"
)
_TYPE_CSV = "id,age\n1,30\n2,41\n3,N/A\n4,29\n5,35\n"
_SHIFT_CSV = "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n"


@dataclass(frozen=True)
class DecisionRow:
    """One row of the spec's decision table."""

    number: int
    detector: str
    premise: str
    csv_text: str
    schema_text: str | None
    disposition: Disposition
    why: str

    @property
    def test_id(self) -> str:
        return f"{self.number:02d}-{self.detector}-{self.premise}-{self.disposition}"


# Row numbers match the table in specs/SPEC_autoapply_decision.md.
_CASES: tuple[DecisionRow, ...] = (
    DecisionRow(
        1,
        "fd_violation",
        "declared-fd",
        f"id,state,city\n{_FD_BODY}{_FD_ROWS + 1},MA,bostonn\n",
        _FD_SCHEMA,
        "write",
        "value determined by an operator-declared dependency",
    ),
    DecisionRow(
        2,
        "fd_violation",
        "no-premise",
        f"id,state,city\n{_FD_BODY}{_FD_ROWS + 1},MA,bostonn\n",
        None,
        "no_write",
        "repairer abstains rather than inferring a dependency",
    ),
    DecisionRow(
        3,
        "missing_value",
        "declared-fd",
        f"id,state,city\n{_FD_BODY}{_FD_ROWS + 1},MA,\n",
        _FD_SCHEMA_FD_AND_NOT_NULL,
        "write",
        "fill derived from the declared dependency",
    ),
    DecisionRow(
        4,
        "missing_value",
        "not-null-only",
        f"id,state,city\n{_FD_BODY}{_FD_ROWS + 1},MA,\n",
        _FD_SCHEMA_NOT_NULL,
        "no_write",
        "knowing a value is required does not say what it is",
    ),
    DecisionRow(
        5,
        "missing_value",
        "no-premise",
        f"id,state,city\n{_FD_BODY}{_FD_ROWS + 1},MA,\n",
        None,
        "no_write",
        "schema is None, immediate abstain",
    ),
    DecisionRow(
        6,
        "type_mismatch",
        "no-premise",
        _TYPE_CSV,
        None,
        "no_write",
        "type_mismatch left the bypass allowlist 2026-08-25: 156 flags and zero proposals "
        "measured across three corpora, so no committed evidence of a real write exists",
    ),
    DecisionRow(
        7,
        "type_mismatch",
        "declared-integer",
        _TYPE_CSV,
        "columns:\n  id: string\n  age:\n    type: integer\n",
        "no_write",
        "the proposed '' would violate the declared type",
    ),
    DecisionRow(
        8,
        "decimal_shift",
        "no-premise",
        _SHIFT_CSV,
        None,
        "no_write",
        "not constraint-checkable",
    ),
    DecisionRow(
        9,
        "decimal_shift",
        "declared-float",
        _SHIFT_CSV,
        "columns:\n  id: string\n  amount:\n    type: float\n",
        "no_write",
        "not constraint-checkable; a schema does not rescue it",
    ),
)


def _materialise(row: DecisionRow, tmp_path: Path) -> tuple[Path, object | None]:
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(row.csv_text, encoding="utf-8")
    if row.schema_text is None:
        return csv_path, None
    schema_path = tmp_path / "table.schema.yaml"
    schema_path.write_text(row.schema_text, encoding="utf-8")
    return csv_path, load_schema(schema_path)


def _apply_on(surface: Surface, csv_path: Path, schema: object | None) -> None:
    if surface == "legacy":
        run_repair_pipeline(
            RepairPipelineRequest(source_path=csv_path, mode="apply", schema=schema)
        )
        return
    agent_controller.run_agent_repair(
        agent_controller.AgentRepairRequest(
            source_path=csv_path, mode="apply", schema=schema, policy="deterministic"
        )
    )


@pytest.mark.parametrize("surface", ["legacy", "agent"])
@pytest.mark.parametrize("row", _CASES, ids=lambda r: r.test_id)
class TestAutoApplyDecisionTable:
    """Given a detector and a premise, when apply runs, then bytes change or they do not."""

    def test_disposition_matches_the_spec(
        self, row: DecisionRow, surface: Surface, tmp_path: Path
    ) -> None:
        csv_path, schema = _materialise(row, tmp_path)
        before = csv_path.read_text(encoding="utf-8")

        _apply_on(surface, csv_path, schema)

        after = csv_path.read_text(encoding="utf-8")
        changed = after != before
        expected = row.disposition == "write"
        assert changed is expected, (
            f"spec row {row.number} ({row.detector}, {row.premise}) on the {surface} "
            f"surface: expected bytes_changed={expected} because {row.why}, got "
            f"{changed}. Either the product changed or the spec is now wrong -- update "
            f"specs/SPEC_autoapply_decision.md in the same commit."
        )

    def test_a_no_write_row_leaves_the_defect_in_place(
        self, row: DecisionRow, surface: Surface, tmp_path: Path
    ) -> None:
        """A held/abstained row must leave the ORIGINAL value, not a partial edit.

        Distinct from the byte assertion above: a write that mutated one cell and
        reverted another would satisfy "bytes unchanged" only by coincidence. This pins
        that nothing was silently rewritten to an equal-length value.
        """
        if row.disposition == "write":
            pytest.skip("this row is expected to write; covered by the assertion above")
        csv_path, schema = _materialise(row, tmp_path)
        original = csv_path.read_bytes()

        _apply_on(surface, csv_path, schema)

        assert csv_path.read_bytes() == original


class TestInvariantsAcrossTheWholeTable:
    """Properties over the table as a whole, not any single row."""

    def test_no_uncheckable_detector_ever_writes(self, tmp_path: Path) -> None:
        """I1: no non-allowlisted deterministic detector changes bytes, on any surface."""
        offenders: list[str] = []
        for row in _CASES:
            if row.detector in CONSTRAINT_CHECKABLE_DETECTORS:
                continue
            for surface in ("legacy", "agent"):
                case_dir = tmp_path / f"{row.test_id}-{surface}"
                case_dir.mkdir()
                csv_path, schema = _materialise(row, case_dir)
                before = csv_path.read_text(encoding="utf-8")
                _apply_on(surface, csv_path, schema)  # type: ignore[arg-type]
                if csv_path.read_text(encoding="utf-8") != before:
                    offenders.append(f"{row.detector} on {surface} ({row.premise})")
        assert not offenders, (
            f"non-constraint-checkable detectors wrote to disk: {offenders}. "
            "This is the corruption path; see docs/trust/deterministic-is-not-sound.md."
        )

    def test_the_table_actually_exercises_both_dispositions(self) -> None:
        """Non-vacuity: a table of only no-write rows would pass I1 by proving nothing."""
        dispositions = {row.disposition for row in _CASES}
        assert dispositions == {"write", "no_write"}, (
            f"the decision table must contain both dispositions, got {dispositions}. "
            "An all-no_write table satisfies every safety assertion trivially."
        )

    def test_the_table_covers_every_registered_repairer(self) -> None:
        """A new repairer cannot be registered without appearing in the spec.

        Iterates the registry rather than a hardcoded list, so the failure mode this
        guards -- shipping a repairer nobody classified -- cannot slip through by the
        spec simply not mentioning it.
        """
        registered = set(build_repairers(cache_dir=None, allow_llm=False))
        covered = {row.detector for row in _CASES}
        missing = registered - covered
        assert not missing, (
            f"repairers registered but absent from the decision table: {sorted(missing)}. "
            "Add a row to specs/SPEC_autoapply_decision.md and to _CASES, classifying it "
            "as constraint-checkable or not."
        )

    def test_allowlist_membership_is_consistent_with_measured_writes(self) -> None:
        """Every detector that writes somewhere in the table is allowlisted, and vice versa.

        The bidirectional check matters. Forward: a writer that is not allowlisted is the
        corruption bug. Backward: an allowlisted detector that writes nowhere in the table
        means the allowlist grants access nobody demonstrated -- which is how
        ``decimal_shift`` sat in the trusted set for weeks.
        """
        writers = {row.detector for row in _CASES if row.disposition == "write"}
        unallowlisted_writers = writers - set(CONSTRAINT_CHECKABLE_DETECTORS)
        assert not unallowlisted_writers, (
            f"these detectors write but are not allowlisted: {sorted(unallowlisted_writers)}"
        )
        never_demonstrated = set(CONSTRAINT_CHECKABLE_DETECTORS) - writers
        assert not never_demonstrated, (
            f"these detectors are allowlisted but no table row demonstrates a write: "
            f"{sorted(never_demonstrated)}. Either add a row proving the write path is "
            "reachable, or remove them from CONSTRAINT_CHECKABLE_DETECTORS."
        )


class TestPrimitiveBackstop:
    """I3: the mutation primitive refuses, so a surface that forgets to partition raises."""

    @staticmethod
    def _uncheckable_fix() -> ProposedFix:
        return ProposedFix(
            fix=CellFix(
                row=4,
                column="amount",
                old_value="1020",
                new_value="102",
                detector_id="decimal_shift",
                operation="update",
            ),
            provenance="deterministic",
            reason="a decimal shift inferred from this column's own distribution",
            confidence=0.99,
        )

    def test_apply_transaction_refuses_an_uncheckable_fix(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "t.csv"
        csv_path.write_text(_SHIFT_CSV, encoding="utf-8")
        original = csv_path.read_bytes()

        with pytest.raises(UncheckableDetectorWriteError, match="decimal_shift"):
            apply_transaction(
                path=csv_path,
                fixes=[self._uncheckable_fix()],
                source_bytes=original,
                covered_columns=frozenset({"id", "amount"}),
                allow_unproven_autoapply=False,
            )

        assert csv_path.read_bytes() == original, (
            "the primitive raised but had already mutated the file -- the gate must run "
            "before any write, not after"
        )

    def test_the_backstop_names_both_the_detector_and_the_allowlist(self, tmp_path: Path) -> None:
        """A caller hitting this needs to know which gate refused and what would pass."""
        csv_path = tmp_path / "t.csv"
        csv_path.write_text(_SHIFT_CSV, encoding="utf-8")
        with pytest.raises(UncheckableDetectorWriteError) as caught:
            apply_transaction(
                path=csv_path,
                fixes=[self._uncheckable_fix()],
                source_bytes=csv_path.read_bytes(),
                covered_columns=frozenset({"id", "amount"}),
                allow_unproven_autoapply=False,
            )
        message = str(caught.value)
        assert "decimal_shift" in message
        for allowed in CONSTRAINT_CHECKABLE_DETECTORS:
            assert allowed in message, f"{allowed} missing from the refusal message"

    def test_the_opt_in_does_not_unlock_an_uncheckable_write(self, tmp_path: Path) -> None:
        """I4: ``allow_unproven_autoapply`` covers strength, never soundness.

        Mutation-resistant by design: if someone threads the opt-in into the new gate for
        symmetry with the old one, this fails.
        """
        csv_path = tmp_path / "t.csv"
        csv_path.write_text(_SHIFT_CSV, encoding="utf-8")
        original = csv_path.read_bytes()

        with pytest.raises(UncheckableDetectorWriteError):
            apply_transaction(
                path=csv_path,
                fixes=[self._uncheckable_fix()],
                source_bytes=original,
                covered_columns=frozenset({"id", "amount"}),
                allow_unproven_autoapply=True,
            )

        assert csv_path.read_bytes() == original


class TestTheTwoGatesAgree:
    """I5: the partition-point decision and the primitive decision cannot drift apart."""

    @pytest.mark.parametrize("detector", sorted(build_repairers(cache_dir=None, allow_llm=False)))
    def test_partition_and_primitive_agree_for_every_repairer(self, detector: str) -> None:
        fix = ProposedFix(
            fix=CellFix(
                row=1,
                column="c",
                old_value="a",
                new_value="b",
                detector_id=detector,
                operation="update",
            ),
            provenance="deterministic",
            reason="synthetic fix used only to compare the two gates",
            confidence=0.99,
        )
        auto, _calibration_held, _held = partition_auto_apply(
            [fix],
            corrector_default_policy(),
            covered_columns=frozenset({"c"}),
            allow_unproven_autoapply=False,
        )
        partition_would_write = bool(auto)

        primitive_would_write = True
        try:
            from dataforge.engine.repair import enforce_constraint_checkable_only

            enforce_constraint_checkable_only([fix])
        except UncheckableDetectorWriteError:
            primitive_would_write = False

        assert partition_would_write == primitive_would_write, (
            f"gates disagree for {detector!r}: partition_auto_apply would "
            f"{'apply' if partition_would_write else 'hold'} but the mutation primitive "
            f"would {'allow' if primitive_would_write else 'refuse'}. A surface calling "
            "one but not the other would behave differently from its sibling -- exactly "
            "the divergence that let the agent write a held decimal_shift fix."
        )
        assert primitive_would_write == (detector in CONSTRAINT_CHECKABLE_DETECTORS), (
            f"{detector!r} disagrees with its own allowlist membership"
        )
