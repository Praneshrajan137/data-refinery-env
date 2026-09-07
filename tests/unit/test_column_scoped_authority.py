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

from dataforge.detectors.base import (
    DomainBound,
    FunctionalDependency,
    RegexConstraint,
    Schema,
)
from dataforge.domain.vocabulary import (
    NON_DISCRIMINATING_COLUMN_TYPES,
    type_discriminates,
)
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


class TestTypeDiscriminates:
    """The set of non-narrowing type spellings is pinned, because its drift is silent.

    This is a denylist, and the module's own contract warns denylists fail open. Here the
    enumeration is over types meaning "no constraint", so an unrecognised type name confers
    authority. That direction is deliberate -- ``decimal`` and ``timestamp`` genuinely narrow, and
    revoking authority from every schema using an unlisted type would be a regression with no
    evidence behind it -- but it means an added synonym for "anything" would silently grant
    authority. Hence the explicit test.
    """

    def test_string_spellings_do_not_narrow(self) -> None:
        for spelling in ("str", "string", "text", "object", "any", ""):
            assert not type_discriminates(spelling), f"{spelling!r} must not confer authority"

    def test_real_types_narrow(self) -> None:
        for spelling in ("int", "integer", "float", "bool", "date", "datetime", "decimal"):
            assert type_discriminates(spelling), f"{spelling!r} narrows and must confer authority"

    def test_absent_type_does_not_narrow(self) -> None:
        """The absence of information is not authority."""
        assert not type_discriminates(None)

    def test_spelling_is_normalised(self) -> None:
        """A schema written by hand should not gain authority from capitalisation or whitespace."""
        for spelling in ("STR", " str ", "String", "TEXT"):
            assert not type_discriminates(spelling)

    def test_the_denylist_is_exactly_what_is_documented(self) -> None:
        assert (
            frozenset({"", "str", "string", "text", "object", "any"})
            == NON_DISCRIMINATING_COLUMN_TYPES
        )


class TestAuthoritativeColumns:
    """The covered set must include every column the schema speaks about, and no others."""

    def test_a_discriminating_declared_type_is_covered(self) -> None:
        """``int`` can reject a value, so declaring it grants authority over that column."""
        schema = Schema(columns={"a": "int", "b": "str"})

        assert authoritative_columns(schema) == frozenset({"a"})

    def test_a_str_declaration_alone_does_not_grant_authority(self) -> None:
        """Listing a column as ``str`` says nothing that can reject a value.

        This is the next instance of the defect this module already fixed once. Authority was
        narrowed from table-level to per-column after narrow evidence granted blanket authority;
        it is narrowed again here because *mentioning* a column is not *constraining* it.
        Measured on ``eval/results/trust_ledger_adversarial.json``: a premise declaring every
        column ``str`` admitted 10 of 14 constraint-violating writes and stamped every one
        ``proven``, against 0 of 14 under a premise that actually constrained. Every CSV cell is
        already a string and ``read_csv`` is called with ``dtype=str``, so ``str`` is the absence
        of a type rather than a type.
        """
        assert authoritative_columns(Schema(columns={"b": "str", "c": "text"})) == frozenset()

    def test_a_str_column_is_still_covered_by_any_real_constraint(self) -> None:
        """The rule removes free authority, not the ability to declare authority over strings.

        A string column with a regex, an enum, a not-null or an FD is genuinely constrained, and
        the tight adversarial premise depends on exactly this: ``zip`` is ``str`` plus a regex.
        """
        schema = Schema(
            columns={"zip": "str", "city": "str"},
            regex_constraints=(RegexConstraint(column="zip", pattern=r"^\d{5}$"),),
            not_null_columns=frozenset({"city"}),
        )

        assert authoritative_columns(schema) == frozenset({"zip", "city"})

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
        #
        # `mined_constraints_grant_write_authority=True` reproduces the exact 2026-08-09
        # conditions, where an ACCEPTED MINED `column_type` candidate was the authority. C4
        # (2026-09-07) now closes this escalation a second and independent way -- a mined
        # constraint confers no authority at all, so the `city` write is refused even before
        # per-column scoping is consulted. The flag is passed here deliberately so this test
        # keeps measuring per-column scoping rather than silently becoming a test of C4.
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
                    mined_constraints_grant_write_authority=True,
                )
            )

            assert result.receipt.applied is True
            assert source.read_bytes() != _CSV

    def test_c4_refuses_the_covered_column_too_when_the_authority_is_mined(self) -> None:
        """The second, independent closure of the 2026-08-09 escalation.

        The original fix scoped authority to the columns a schema constrains. C4 goes
        further: a constraint MINED from the table and accepted in review confers no write
        authority on any column, so even the covered column is refused. Pinned because a
        defect closed twice should stay closed twice.
        """
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

            assert result.receipt.applied is False
            assert source.read_bytes() == _CSV
