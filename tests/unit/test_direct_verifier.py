"""Unit tests for the independently-written DirectVerifier (N-version twin)."""

from __future__ import annotations

import pandas as pd

from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier import VerificationVerdict
from dataforge.verifier.direct import DirectVerifier
from dataforge.verifier.schema import (
    AcceptedValues,
    DomainBound,
    FunctionalDependency,
    RegexConstraint,
    Schema,
)


def _fix(*, row: int, column: str, new_value: str, old_value: str = "old") -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=row, column=column, old_value=old_value, new_value=new_value, detector_id="test"
        ),
        reason="candidate",
        confidence=1.0,
        provenance="llm_live",
    )


def _verdict(schema: Schema, df: pd.DataFrame, fix: ProposedFix) -> VerificationVerdict:
    return DirectVerifier().verify(df, [fix], schema).verdict


def test_no_schema_is_structural_accept() -> None:
    df = pd.DataFrame({"a": ["1", "2"]})
    assert DirectVerifier().verify(df, [_fix(row=0, column="a", new_value="9")]).verdict == (
        VerificationVerdict.ACCEPT
    )


def test_out_of_bounds_and_missing_column_reject() -> None:
    df = pd.DataFrame({"a": ["1", "2"]})
    schema = Schema(columns={"a": "int"})
    assert (
        _verdict(schema, df, _fix(row=5, column="a", new_value="9")) == VerificationVerdict.REJECT
    )
    assert (
        _verdict(schema, df, _fix(row=0, column="z", new_value="9")) == VerificationVerdict.REJECT
    )


def test_type_ok_and_type_unparseable_is_unknown() -> None:
    df = pd.DataFrame({"a": ["1", "2", "3"]})
    schema = Schema(columns={"a": "int"})
    assert (
        _verdict(schema, df, _fix(row=0, column="a", new_value="7")) == VerificationVerdict.ACCEPT
    )
    # An unparseable value cannot be encoded as the declared type -> UNKNOWN.
    assert (
        _verdict(schema, df, _fix(row=0, column="a", new_value="banana"))
        == VerificationVerdict.UNKNOWN
    )


def test_domain_bounds_inclusive_and_exclusive() -> None:
    df = pd.DataFrame({"a": ["10", "20", "30"]})
    inclusive = Schema(
        columns={"a": "int"},
        domain_bounds=(DomainBound(column="a", min_value=0.0, max_value=100.0),),
    )
    assert _verdict(inclusive, df, _fix(row=0, column="a", new_value="100")) == (
        VerificationVerdict.ACCEPT
    )
    assert _verdict(inclusive, df, _fix(row=0, column="a", new_value="101")) == (
        VerificationVerdict.REJECT
    )
    exclusive = Schema(
        columns={"a": "int"},
        domain_bounds=(
            DomainBound(column="a", min_value=0.0, max_value=100.0, inclusive_max=False),
        ),
    )
    assert _verdict(exclusive, df, _fix(row=0, column="a", new_value="100")) == (
        VerificationVerdict.REJECT
    )


def test_accepted_values_and_regex() -> None:
    df = pd.DataFrame({"status": ["A", "B", "A"]})
    enum_schema = Schema(
        columns={"status": "str"},
        accepted_values=(AcceptedValues(column="status", values=("A", "B", "C")),),
    )
    assert _verdict(enum_schema, df, _fix(row=0, column="status", new_value="C")) == (
        VerificationVerdict.ACCEPT
    )
    assert _verdict(enum_schema, df, _fix(row=0, column="status", new_value="Z")) == (
        VerificationVerdict.REJECT
    )
    regex_schema = Schema(
        columns={"status": "str"},
        regex_constraints=(RegexConstraint(column="status", pattern=r"^[A-Z]$"),),
    )
    assert _verdict(regex_schema, df, _fix(row=0, column="status", new_value="Q")) == (
        VerificationVerdict.ACCEPT
    )
    assert _verdict(regex_schema, df, _fix(row=0, column="status", new_value="qq")) == (
        VerificationVerdict.REJECT
    )


def test_not_null_and_unique() -> None:
    df = pd.DataFrame({"id": ["1", "2", "3"]})
    not_null = Schema(columns={"id": "str"}, not_null_columns=frozenset({"id"}))
    assert _verdict(not_null, df, _fix(row=0, column="id", new_value="")) == (
        VerificationVerdict.REJECT
    )
    unique = Schema(columns={"id": "int"}, unique_columns=frozenset({"id"}))
    assert _verdict(unique, df, _fix(row=0, column="id", new_value="2")) == (
        VerificationVerdict.REJECT
    )  # collides with row 1
    assert _verdict(unique, df, _fix(row=0, column="id", new_value="9")) == (
        VerificationVerdict.ACCEPT
    )


def test_functional_dependency() -> None:
    # zip -> city; rows share zip 02134 -> city must agree.
    df = pd.DataFrame({"zip": ["02134", "02134", "10001"], "city": ["Boston", "Boston", "NYC"]})
    schema = Schema(
        columns={"zip": "str", "city": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
    )
    # Changing row 0 city to a value that conflicts with its zip peer -> REJECT.
    assert _verdict(schema, df, _fix(row=0, column="city", new_value="Atlanta")) == (
        VerificationVerdict.REJECT
    )
    # Consistent value -> ACCEPT.
    assert _verdict(schema, df, _fix(row=0, column="city", new_value="Boston")) == (
        VerificationVerdict.ACCEPT
    )


def test_multi_fix_sequential() -> None:
    df = pd.DataFrame({"a": ["1", "2", "3"]})
    schema = Schema(columns={"a": "int"}, unique_columns=frozenset({"a"}))
    # Two fixes that are individually and jointly unique.
    fixes = [
        _fix(row=0, column="a", new_value="7"),
        _fix(row=1, column="a", new_value="8"),
    ]
    assert DirectVerifier().verify(df, fixes, schema).verdict == VerificationVerdict.ACCEPT
    # Second fix collides with the first fix's applied value -> REJECT.
    colliding = [
        _fix(row=0, column="a", new_value="7"),
        _fix(row=1, column="a", new_value="7"),
    ]
    assert DirectVerifier().verify(df, colliding, schema).verdict == VerificationVerdict.REJECT
