"""Authority is per-column: narrow evidence must not grant blanket proven status.

Verified as a LIVE defect on 2026-08-09, end-to-end with a real write to disk.

``authoritative_schema_present`` used to be a table-level boolean. Accepting exactly ONE
inferred ``column_type`` candidate on column ``id`` produced an effective schema of
``{"id": "int"}`` with no other constraints, and that flipped an ``external`` garbage fix
on the UNRELATED column ``city`` from held to **applied** -- stamped ``proven`` in the
certificate.

Three properties made it serious:

* It needed no LLM, no agent and no unusual flags -- just ``verify_and_apply``, which ships
  as both a CLI command and an MCP tool, plus one accepted constraint.
* ``external`` is not in ``_LLM_PROVENANCE``, so a fix labelled proven there auto-applies
  immediately without having to clear any calibration threshold.
* The certificate said ``proven``. That is a truthfulness violation, not merely an
  unproven write, and truthfulness is this project's core product claim.

The fix scopes authority to the columns the schema actually constrains
(``authoritative_columns``). These tests pin both directions: the escalation is closed, and
a fix on a genuinely covered column still applies.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from dataforge.detectors.base import DomainBound, FunctionalDependency, Schema
from dataforge.engine.repair import (
    ExternalFix,
    VerifyAndApplyRequest,
    authoritative_columns,
    verification_strength_for,
    verify_and_apply,
)
from dataforge.schema_inference import (
    build_constraint_review_artifact,
    infer_schema,
    update_constraint_review_artifact,
)
from dataforge.table import read_csv
from dataforge.transactions.log import sha256_bytes

_CSV = (
    b"id,amount,city\n"
    b"1,10,Boston\n2,20,Boston\n3,30,Boston\n4,40,Boston\n"
    b"5,50,Boston\n6,60,Boston\n7,70,Boston\n8,80,Boston\n"
)


class TestAuthoritativeColumns:
    """The covered set must include every column the schema speaks about, and no others."""

    def test_declared_types_are_covered(self) -> None:
        schema = Schema(columns={"a": "int", "b": "str"})

        assert authoritative_columns(schema) == frozenset({"a", "b"})

    def test_constraint_only_columns_are_covered(self) -> None:
        schema = Schema(
            columns={},
            domain_bounds=(DomainBound(column="amount", min_value=0.0, max_value=10.0),),
        )

        assert authoritative_columns(schema) == frozenset({"amount"})

    def test_functional_dependency_covers_both_sides(self) -> None:
        # A value in either position is checked by the FD, so both are covered.
        schema = Schema(
            columns={},
            functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
        )

        assert authoritative_columns(schema) == frozenset({"zip", "city"})

    def test_no_schema_covers_nothing(self) -> None:
        assert authoritative_columns(None) == frozenset()

    def test_uncovered_column_is_not_proven(self) -> None:
        covered = authoritative_columns(Schema(columns={"id": "int"}))

        assert verification_strength_for(
            "external", authoritative_schema_present="city" in covered
        ) == ("plausibility_only")
        assert verification_strength_for(
            "external", authoritative_schema_present="id" in covered
        ) == ("proven")


class TestOneConstraintDoesNotGrantBlanketAuthority:
    """The end-to-end reproduction of the defect, and of the legitimate case."""

    @staticmethod
    def _accepted_one_column_type(path: Path) -> object:
        """Accept exactly one ``column_type`` candidate, on column ``id`` only."""
        table = read_csv(path)
        artifact = build_constraint_review_artifact(
            infer_schema(table), source_path=path, source_sha256=sha256_bytes(_CSV)
        )
        candidate_id = next(
            reviewed.candidate_id
            for reviewed in artifact.candidates
            if reviewed.candidate.kind == "column_type" and reviewed.candidate.columns == ("id",)
        )
        return update_constraint_review_artifact(artifact, accept_ids=[candidate_id])

    def test_fix_on_uncovered_column_is_held(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "t.csv"
            source.write_bytes(_CSV)
            accepted = self._accepted_one_column_type(source)

            result = verify_and_apply(
                VerifyAndApplyRequest(
                    source_path=source,
                    fixes=[ExternalFix(row=0, column="city", new_value="ZZZ_GARBAGE")],
                    mode="apply",
                    constraints=accepted,
                    confirm_escalations=True,
                )
            )

            assert result.receipt.applied is False
            assert source.read_bytes() == _CSV, (
                "accepting one constraint on 'id' granted proven status to a fix on 'city'"
            )

    def test_fix_on_the_covered_column_still_applies(self) -> None:
        # The fix must not become a blanket refusal: authority over the column the schema
        # DOES constrain is exactly what an authoritative schema is for.
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "t.csv"
            source.write_bytes(_CSV)
            accepted = self._accepted_one_column_type(source)

            result = verify_and_apply(
                VerifyAndApplyRequest(
                    source_path=source,
                    fixes=[ExternalFix(row=0, column="id", new_value="9")],
                    mode="apply",
                    constraints=accepted,
                    confirm_escalations=True,
                )
            )

            assert result.receipt.applied is True
            assert source.read_bytes() != _CSV
