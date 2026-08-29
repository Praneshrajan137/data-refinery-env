"""The two table representations must produce the same verdicts.

`TableLike` admits both `pandas.DataFrame` and `dataforge.table.Table`, and the codebase uses them
asymmetrically:

* the **CLI** reads CSV through `read_csv`, so everything a user runs goes through `Table`;
* every **benchmark and evaluation harness** in `scripts/` builds a `pandas.DataFrame`.

That asymmetry is not academic. `DeterminantGroupIndex` reuses a cached grouping only when the table
can report a write counter through `column_revision`, which `Table` has and `DataFrame` has not. So
the harnesses exercise the uncached scan branch while the product exercises the cached one -- two
different code paths, one of them measured and the other one shipped.

The honest fix would be to make every harness use `Table`. That is a larger change than it looks,
because the harnesses lean on pandas for scoring, not just for holding rows. So this file takes the
other route available: it makes the divergence **non-load-bearing** by proving the two
representations are interchangeable for the thing that matters. If they ever stop agreeing, this
fails, and the pandas-based evidence in `docs/trust/` stops being evidence at that moment rather than
silently becoming wrong.

Note what this does and does not establish. It establishes that verdicts agree. It does NOT establish
that timings agree -- they demonstrably do not, which is precisely why the counted-work instrument in
`scripts/perf/measure_verifier_work.py` runs on `Table` rather than on pandas.
"""

from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dataforge.repairers.base import ProposedFix
from dataforge.table import Table
from dataforge.transactions.txn import CellFix
from dataforge.verifier.differential import differential_verify
from dataforge.verifier.direct import DirectVerifier
from dataforge.verifier.result import VerificationVerdict
from dataforge.verifier.schema import (
    DomainBound,
    FunctionalDependency,
    RegexConstraint,
    Schema,
)
from dataforge.verifier.smt import SMTVerifier

_STR_POOL = ["A", "B", "C", "AB", "1", ""]
_INT_POOL = ["0", "1", "2", "5", "10", "42"]


def _as_table(data: dict[str, list[str]]) -> Table:
    """Build the shipped representation from the same raw columns pandas is handed."""
    rows = [dict(zip(data, values, strict=True)) for values in zip(*data.values(), strict=True)]
    return Table(list(data), rows)


@st.composite
def _case(draw: st.DrawFn) -> tuple[Schema, dict[str, list[str]], ProposedFix]:
    """Draw a schema, column data, and a candidate fix -- representation-agnostic.

    Deliberately returns raw column data rather than a built table, so the SAME values can be handed
    to both representations. Building one and converting would test the converter.
    """
    n_cols = draw(st.integers(min_value=2, max_value=3))
    n_rows = draw(st.integers(min_value=3, max_value=8))
    col_types = {f"c{i}": draw(st.sampled_from(["int", "str"])) for i in range(n_cols)}

    data: dict[str, list[str]] = {name: [] for name in col_types}
    for _ in range(n_rows):
        for name, ctype in col_types.items():
            data[name].append(draw(st.sampled_from(_INT_POOL if ctype == "int" else _STR_POOL)))

    names = list(col_types)
    fds: list[FunctionalDependency] = []
    if draw(st.booleans()) and len(names) >= 2:
        fds.append(FunctionalDependency(determinant=(names[0],), dependent=names[1]))
    bounds: list[DomainBound] = []
    regexes: list[RegexConstraint] = []
    for name, ctype in col_types.items():
        if ctype == "int" and draw(st.booleans()):
            bounds.append(DomainBound(column=name, minimum=0, maximum=20))
        elif ctype == "str" and draw(st.booleans()):
            regexes.append(RegexConstraint(column=name, pattern=r"^[A-Z]*$"))

    schema = Schema(
        column_types=col_types,
        functional_dependencies=tuple(fds),
        domain_bounds=tuple(bounds),
        regex_constraints=tuple(regexes),
        not_null_columns=(),
        unique_columns=(),
        accepted_values=(),
    )

    target_col = draw(st.sampled_from(names))
    target_row = draw(st.integers(min_value=0, max_value=n_rows - 1))
    new_value = draw(st.sampled_from(_INT_POOL if col_types[target_col] == "int" else _STR_POOL))
    fix = ProposedFix(
        fix=CellFix(
            row=target_row,
            column=target_col,
            old_value=data[target_col][target_row],
            new_value=new_value,
            detector_id="fd_violation",
        ),
        reason="representation parity",
        confidence=1.0,
        provenance="deterministic",
    )
    return schema, data, fix


@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_case())
def test_smt_verdict_is_identical_across_representations(
    case: tuple[Schema, dict[str, list[str]], ProposedFix],
) -> None:
    """The z3-backed verifier must not care which container holds the rows."""
    schema, data, fix = case
    frame = pd.DataFrame(data)
    table = _as_table(data)

    from_frame = SMTVerifier().verify(frame, [fix], schema)
    from_table = SMTVerifier().verify(table, [fix], schema)

    assert from_frame.verdict == from_table.verdict, (
        "the SMT verifier returned different verdicts for identical data in different containers, "
        "which would mean every pandas-based measurement in scripts/ describes code the CLI never "
        f"runs: frame={from_frame.verdict} table={from_table.verdict} on {fix.fix}"
    )


@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_case())
def test_direct_verdict_is_identical_across_representations(
    case: tuple[Schema, dict[str, list[str]], ProposedFix],
) -> None:
    """Same claim for the independently-written verifier, which reads cells differently."""
    schema, data, fix = case
    frame = pd.DataFrame(data)
    table = _as_table(data)

    from_frame = DirectVerifier().verify(frame, [fix], schema)
    from_table = DirectVerifier().verify(table, [fix], schema)

    assert from_frame.verdict == from_table.verdict, (
        f"DirectVerifier disagreed with itself across containers: "
        f"frame={from_frame.verdict} table={from_table.verdict} on {fix.fix}"
    )


@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_case())
def test_combined_verdict_is_identical_across_representations(
    case: tuple[Schema, dict[str, list[str]], ProposedFix],
) -> None:
    """The fail-closed combination is what actually gates a write, so pin it directly.

    Testing the two verifiers separately is not quite enough: the combination could in principle
    agree per-representation while the pair disagreed, if the two disagreements cancelled.
    """
    schema, data, fix = case
    frame = pd.DataFrame(data)
    table = _as_table(data)

    from_frame = differential_verify(frame, [fix], schema)
    from_table = differential_verify(table, [fix], schema)

    assert from_frame.verdict == from_table.verdict, (
        "the gate that authorises writes behaved differently on the harness representation than on "
        f"the shipped one: frame={from_frame.verdict} table={from_table.verdict} on {fix.fix}"
    )


# --------------------------------------------------------------------------------------
# Deterministic cases, because agreement is only interesting where a verdict is contested
# --------------------------------------------------------------------------------------
#
# The generated cases above draw ACCEPT about 94% of the time, so on their own they mostly prove that
# two representations agree about uncontroversial writes. Parity that only holds on ACCEPT is close to
# worthless: the whole purpose of the gate is the REJECT path. These cases fix each constraint family
# at a known verdict so that agreement is asserted exactly where disagreement would matter, without
# depending on the draw.

_PARITY_DATA: dict[str, list[str]] = {
    "code": ["A", "A", "A", "B"],
    "name": ["X", "X", "X", "Y"],
    "size": ["1", "2", "3", "4"],
}


def _schema(**overrides: object) -> Schema:
    base: dict[str, object] = {
        "column_types": {"code": "str", "name": "str", "size": "int"},
        "functional_dependencies": (),
        "domain_bounds": (),
        "regex_constraints": (),
        "not_null_columns": (),
        "unique_columns": (),
        "accepted_values": (),
    }
    base.update(overrides)
    return Schema(**base)  # type: ignore[arg-type]


def _fix(column: str, row: int, new_value: str) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=row,
            column=column,
            old_value=_PARITY_DATA[column][row],
            new_value=new_value,
            detector_id="fd_violation",
        ),
        reason="deterministic parity case",
        confidence=1.0,
        provenance="deterministic",
    )


_FD = FunctionalDependency(determinant=("code",), dependent="name")
_BOUND = DomainBound(column="size", minimum=0, maximum=10)
_REGEX = RegexConstraint(column="name", pattern=r"^[A-Z]$")

_CONTESTED_CASES = [
    # An FD-consistent write: row 3 joins group 'A', and 'X' is what that group holds.
    ("fd satisfied", _schema(functional_dependencies=(_FD,)), _fix("name", 3, "X")),
    # An FD-violating write: row 0 stays in group 'A' but disagrees with its peers.
    ("fd violated", _schema(functional_dependencies=(_FD,)), _fix("name", 0, "Q")),
    ("bound satisfied", _schema(domain_bounds=(_BOUND,)), _fix("size", 0, "7")),
    ("bound violated", _schema(domain_bounds=(_BOUND,)), _fix("size", 0, "99")),
    ("regex satisfied", _schema(regex_constraints=(_REGEX,)), _fix("name", 0, "Z")),
    ("regex violated", _schema(regex_constraints=(_REGEX,)), _fix("name", 0, "zz")),
    ("unique satisfied", _schema(unique_columns=("size",)), _fix("size", 0, "9")),
    ("unique violated", _schema(unique_columns=("size",)), _fix("size", 0, "2")),
    ("uncoercible int", _schema(domain_bounds=(_BOUND,)), _fix("size", 0, "not-a-number")),
]


@pytest.mark.parametrize(
    ("label", "schema", "fix"),
    _CONTESTED_CASES,
    ids=[case[0] for case in _CONTESTED_CASES],
)
def test_contested_verdicts_agree_across_representations(
    label: str,
    schema: Schema,
    fix: ProposedFix,
) -> None:
    """Each constraint family, at a contested verdict, must decide the same way on both."""
    frame = pd.DataFrame(_PARITY_DATA)
    table = _as_table(_PARITY_DATA)

    from_frame = differential_verify(frame, [fix], schema)
    from_table = differential_verify(table, [fix], schema)

    assert from_frame.verdict == from_table.verdict, (
        f"representations disagreed on '{label}': frame={from_frame.verdict.value} "
        f"table={from_table.verdict.value}"
    )


def test_the_deterministic_cases_are_not_all_the_same_verdict() -> None:
    """Non-vacuity: if every case above accepted, the parametrisation would prove nothing.

    This is the guard that the case list has not decayed into a list of writes that all sail
    through -- which is how a suite ends up green while defending nothing.
    """
    frame = pd.DataFrame(_PARITY_DATA)
    verdicts = {
        label: differential_verify(frame, [fix], schema).verdict
        for label, schema, fix in _CONTESTED_CASES
    }
    distinct = set(verdicts.values())
    assert len(distinct) >= 2, (
        f"every deterministic parity case reached the same verdict, so none of them is contested: "
        f"{ {label: verdict.value for label, verdict in verdicts.items()} }"
    )
    assert VerificationVerdict.REJECT in distinct, (
        "no deterministic case rejects, so representation parity is only asserted on the ACCEPT "
        f"path -- the path that matters least: { {k: v.value for k, v in verdicts.items()} }"
    )
