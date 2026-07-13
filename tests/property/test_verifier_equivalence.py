"""Differential equivalence: the two independent verifiers must agree.

This is the crown-jewel N-version validation. It generates random authoritative
schemas, random well-typed tables, and a random candidate fix, then asserts that
the z3-backed ``SMTVerifier`` and the independently-written ``DirectVerifier``
return the SAME verdict. Because the two implementations share none of their
checking logic, a systematic disagreement here is a genuine bug in one of them --
exactly the class of defect that "just trust the verifier" cannot surface.

It also pins the fail-closed safety invariant: ``differential_verify`` never
returns ACCEPT unless BOTH implementations accepted.
"""

from __future__ import annotations

import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st

from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier.differential import differential_verify
from dataforge.verifier.direct import DirectVerifier
from dataforge.verifier.result import VerificationVerdict
from dataforge.verifier.schema import (
    AcceptedValues,
    DomainBound,
    FunctionalDependency,
    RegexConstraint,
    Schema,
)
from dataforge.verifier.smt import SMTVerifier

# Values kept in a small, overlapping alphabet so both accept and reject branches
# of every constraint are exercised across examples.
_STR_POOL = ["A", "B", "C", "AB", "1", ""]
_INT_POOL = ["0", "1", "2", "5", "10", "42"]
_REGEX_POOL = [r"^[A-Z]+$", r"^[A-Z]{1,2}$", r"^\d+$"]


@st.composite
def _schema_table_fix(
    draw: st.DrawFn,
) -> tuple[Schema, pd.DataFrame, ProposedFix]:
    n_cols = draw(st.integers(min_value=1, max_value=3))
    n_rows = draw(st.integers(min_value=2, max_value=4))
    col_types: dict[str, str] = {}
    for i in range(n_cols):
        col_types[f"c{i}"] = draw(st.sampled_from(["int", "str"]))

    def value_for(ctype: str) -> str:
        return draw(st.sampled_from(_INT_POOL if ctype == "int" else _STR_POOL))

    # Build a well-typed table.
    data: dict[str, list[str]] = {name: [] for name in col_types}
    for _ in range(n_rows):
        for name, ctype in col_types.items():
            data[name].append(value_for(ctype))
    df = pd.DataFrame(data)

    columns = list(col_types)
    int_columns = [c for c in columns if col_types[c] == "int"]
    str_columns = [c for c in columns if col_types[c] == "str"]

    domain_bounds: list[DomainBound] = []
    accepted_values: list[AcceptedValues] = []
    regex_constraints: list[RegexConstraint] = []
    functional_dependencies: list[FunctionalDependency] = []
    not_null: set[str] = set()
    unique: set[str] = set()

    # Domain bound on an int column (shared well-defined regime).
    if int_columns and draw(st.booleans()):
        col = draw(st.sampled_from(int_columns))
        lo = draw(st.integers(min_value=0, max_value=5))
        hi = draw(st.integers(min_value=lo, max_value=50))
        domain_bounds.append(
            DomainBound(
                column=col,
                min_value=float(lo),
                max_value=float(hi),
                inclusive_min=draw(st.booleans()),
                inclusive_max=draw(st.booleans()),
            )
        )

    # Accepted values (literals valid for the column type).
    if draw(st.booleans()):
        col = draw(st.sampled_from(columns))
        pool = _INT_POOL if col_types[col] == "int" else _STR_POOL
        chosen = draw(st.lists(st.sampled_from(pool), min_size=1, max_size=3, unique=True))
        accepted_values.append(AcceptedValues(column=col, values=tuple(chosen)))

    # Regex on a str column (valid pattern).
    if str_columns and draw(st.booleans()):
        col = draw(st.sampled_from(str_columns))
        regex_constraints.append(
            RegexConstraint(column=col, pattern=draw(st.sampled_from(_REGEX_POOL)))
        )

    # not_null on a str column (matches primary verifier's string-only handling).
    if str_columns and draw(st.booleans()):
        not_null.add(draw(st.sampled_from(str_columns)))

    if draw(st.booleans()):
        unique.add(draw(st.sampled_from(columns)))

    # FD between two distinct columns.
    if n_cols >= 2 and draw(st.booleans()):
        det, dep = draw(st.lists(st.sampled_from(columns), min_size=2, max_size=2, unique=True))
        functional_dependencies.append(FunctionalDependency(determinant=(det,), dependent=dep))

    schema = Schema(
        columns=col_types,
        domain_bounds=tuple(domain_bounds),
        accepted_values=tuple(accepted_values),
        regex_constraints=tuple(regex_constraints),
        functional_dependencies=tuple(functional_dependencies),
        not_null_columns=frozenset(not_null),
        unique_columns=frozenset(unique),
    )

    fix_row = draw(st.integers(min_value=0, max_value=n_rows - 1))
    fix_col = draw(st.sampled_from(columns))
    new_value = value_for(col_types[fix_col])
    proposed = ProposedFix(
        fix=CellFix(
            row=fix_row,
            column=fix_col,
            old_value=str(df.iat[fix_row, columns.index(fix_col)]),
            new_value=new_value,
            detector_id="prop",
        ),
        reason="candidate",
        confidence=1.0,
        provenance="deterministic",
    )
    return schema, df, proposed


@settings(max_examples=200, deadline=None)
@given(case=_schema_table_fix())
def test_smt_and_direct_agree(case: tuple[Schema, pd.DataFrame, ProposedFix]) -> None:
    schema, df, proposed = case
    smt = SMTVerifier().verify(df, [proposed], schema)
    direct = DirectVerifier().verify(df, [proposed], schema)
    assert smt.verdict == direct.verdict, (
        f"verifiers disagreed: smt={smt.verdict.value} ({smt.reason}) "
        f"direct={direct.verdict.value} ({direct.reason}) schema={schema} fix={proposed.fix}"
    )


@settings(max_examples=200, deadline=None)
@given(case=_schema_table_fix())
def test_differential_never_accepts_without_both(
    case: tuple[Schema, pd.DataFrame, ProposedFix],
) -> None:
    schema, df, proposed = case
    smt = SMTVerifier().verify(df, [proposed], schema)
    direct = DirectVerifier().verify(df, [proposed], schema)
    combined = differential_verify(df, [proposed], schema)
    both_accept = (
        smt.verdict == VerificationVerdict.ACCEPT and direct.verdict == VerificationVerdict.ACCEPT
    )
    if combined.verdict == VerificationVerdict.ACCEPT:
        assert both_accept, "differential accepted without both verifiers accepting"
    else:
        assert combined.verdict == VerificationVerdict.REJECT
