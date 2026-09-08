"""Pin the FD-yield harness's structural predictor, which is where it can confirm H4 wrongly.

The predictor in `measure_fd_repair_yield.py` is a partial reimplementation of
`dataforge/verifier/direct.py:245-260`. That is deliberate -- the whole point of P5 is that a
predictor with no pipeline in it reproduces the pipeline's output -- but it is also the most
dangerous thing in the harness: a predictor that has drifted from the verifier would confirm the
hypothesis for the wrong reason and look like a mechanism.

K2 polices that at corpus scale by comparing against the shipped `DirectVerifier` on every
proposal. These tests police it at unit scale, on the three cases where a hand-rolled row scan
usually goes wrong: the candidate row counting itself, the fixed column appearing in a
DETERMINANT rather than a dependent, and dependencies that do not touch the fixed column at all.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dataforge.detectors.base import FunctionalDependency, Schema
from dataforge.table import read_csv

_HARNESS = Path(__file__).resolve().parents[2] / "scripts" / "bench" / "measure_fd_repair_yield.py"


def _load():
    spec = importlib.util.spec_from_file_location("_fd_yield_harness", _HARNESS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _load()


@pytest.fixture
def frame(harness, tmp_path):
    """A zip -> city table with one group holding TWO wrong cells and one holding ONE.

    Rows 0-2 share zip 1: two cells hold 'Reno', one holds 'WRONG'. Fixing the odd one out makes
    the group agree, so it is a SINGLETON violation.
    Rows 3-6 share zip 2: two hold 'Provo', two hold 'BAD'. Fixing either 'BAD' still leaves the
    other disagreeing, so neither is repairable single-cell.
    """
    csv = tmp_path / "t.csv"
    csv.write_text(
        "zip,city,note\n1,Reno,a\n1,Reno,b\n1,WRONG,c\n2,Provo,d\n2,Provo,e\n2,BAD,f\n2,BAD,g\n",
        encoding="utf-8",
    )
    return harness.Frame(read_csv(csv))


@pytest.fixture
def schema():
    return Schema(
        columns={"zip": "str", "city": "str", "note": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
    )


# --------------------------------------------------------------------------------------
# The predictor: "last remaining violation in the group" is the whole hypothesis.
# --------------------------------------------------------------------------------------


def test_the_last_remaining_violation_has_no_disagreements(harness, frame, schema):
    """Row 2 is the only wrong cell in zip group 1, so repairing it satisfies the dependency."""
    remaining, _witnesses = harness.remaining_disagreements(frame, schema, 2, "city", "Reno")
    assert remaining == 0


def test_a_group_with_two_violations_yields_no_single_cell_repair(harness, frame, schema):
    """This is H4. Rows 5 and 6 both hold 'BAD'; fixing one leaves the other disagreeing."""
    for row in (5, 6):
        remaining, witnesses = harness.remaining_disagreements(frame, schema, row, "city", "Provo")
        assert remaining == 1, f"row {row} should have exactly one co-violator"
        assert witnesses, "a disagreement must name a witness row for explainability"


def test_the_candidate_row_is_never_counted_against_itself(harness, frame, schema):
    """A scan that forgets to skip the candidate row reports a phantom disagreement."""
    remaining, _ = harness.remaining_disagreements(frame, schema, 2, "city", "Reno")
    assert remaining == 0, "the candidate row must be excluded from its own group check"


def test_writing_a_value_nobody_else_holds_disagrees_with_the_whole_group(harness, frame, schema):
    """Repairing row 2 to a novel value leaves both other rows in group 1 disagreeing."""
    remaining, _ = harness.remaining_disagreements(frame, schema, 2, "city", "Sparks")
    assert remaining == 2


# --------------------------------------------------------------------------------------
# Scoping: the verifier is NOT global over the schema. Getting this wrong was a published claim.
# --------------------------------------------------------------------------------------


def test_relevant_fds_include_the_column_as_a_determinant(harness):
    """`direct.py:129-133` selects dependencies where the column is dependent OR in the determinant."""
    schema = Schema(
        columns={"a": "str", "b": "str", "c": "str"},
        functional_dependencies=(
            FunctionalDependency(determinant=("a",), dependent="b"),
            FunctionalDependency(determinant=("b",), dependent="c"),
        ),
    )
    labels = {("+".join(fd.determinant), fd.dependent) for fd in harness.relevant_fds(schema, "b")}
    assert labels == {("a", "b"), ("b", "c")}


def test_a_dependency_that_does_not_touch_the_column_is_excluded(harness):
    """The whole schema is NOT checked. `fd_violation.py:99-103` claimed otherwise and was wrong."""
    schema = Schema(
        columns={"a": "str", "b": "str", "x": "str", "y": "str"},
        functional_dependencies=(
            FunctionalDependency(determinant=("a",), dependent="b"),
            FunctionalDependency(determinant=("x",), dependent="y"),
        ),
    )
    assert len(harness.relevant_fds(schema, "b")) == 1


def test_fixing_a_determinant_column_moves_the_candidate_into_another_group(harness, tmp_path):
    """The subtle case: substituting the new value changes which group the candidate belongs to.

    A scan that reads the determinant from the RAW frame instead of through the substitution
    compares the candidate against its old group and gets the wrong answer.
    """
    csv = tmp_path / "d.csv"
    csv.write_text("zip,city\n1,Reno\n2,Provo\n2,Provo\n", encoding="utf-8")
    frame = harness.Frame(read_csv(csv))
    schema = Schema(
        columns={"zip": "str", "city": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
    )
    # Row 0 holds (zip=1, city=Reno). Rewriting its zip to 2 moves it into the Provo group,
    # where its city 'Reno' now disagrees with two rows.
    remaining, _ = harness.remaining_disagreements(frame, schema, 0, "zip", "2")
    assert remaining == 2


# --------------------------------------------------------------------------------------
# P6 headroom: joint repairability, and the caveat that it describes nothing that exists.
# --------------------------------------------------------------------------------------


def _proposal(row: int, column: str, value: str):
    return SimpleNamespace(fix=SimpleNamespace(row=row, column=column, new_value=value))


def test_a_group_repairable_only_jointly_is_counted_as_headroom(harness, frame, schema):
    """Rows 5 and 6 are unrepairable alone and repairable together."""
    proposals = [_proposal(5, "city", "Provo"), _proposal(6, "city", "Provo")]
    single, joint = harness.jointly_repairable(frame, schema, proposals)
    assert single == 0
    assert joint == 2


def test_headroom_is_not_claimed_when_a_co_violator_has_no_proposal(harness, frame, schema):
    """If the other wrong cell is not being repaired, applying one still violates the dependency."""
    proposals = [_proposal(5, "city", "Provo")]
    single, joint = harness.jointly_repairable(frame, schema, proposals)
    assert single == 0
    assert joint == 0


def test_headroom_is_not_claimed_when_co_proposals_disagree(harness, frame, schema):
    """Two proposals writing DIFFERENT values into one group cannot satisfy the dependency."""
    proposals = [_proposal(5, "city", "Provo"), _proposal(6, "city", "Somewhere")]
    _single, joint = harness.jointly_repairable(frame, schema, proposals)
    assert joint < 2


def test_a_singleton_violation_counts_as_both_single_and_joint(harness, frame, schema):
    """Headroom is the DIFFERENCE, so a cell acceptable alone must not inflate it."""
    proposals = [_proposal(2, "city", "Reno")]
    single, joint = harness.jointly_repairable(frame, schema, proposals)
    assert (single, joint) == (1, 1)


# --------------------------------------------------------------------------------------
# Group shape and disposition reporting.
# --------------------------------------------------------------------------------------


def test_violation_group_shape_separates_singletons_from_larger_groups(harness, frame, schema):
    shape = harness.violation_group_shape(
        frame,
        schema,
        [
            _proposal(2, "city", "Reno"),
            _proposal(5, "city", "Provo"),
            _proposal(6, "city", "Provo"),
        ],
    )
    assert shape.get("singleton_violation_groups") == 1
    assert shape.get("groups_with_two_violations") == 2


def test_failure_dispositions_flag_a_functional_dependency_reason(harness):
    """P3 needs to distinguish an FD rejection from a safety denial or an encoding UNKNOWN."""
    result = SimpleNamespace(
        failures=[
            SimpleNamespace(
                status="rejected", reason="Functional dependency zip -> city violated."
            ),
            SimpleNamespace(status="rejected", reason="Value below the minimum."),
            SimpleNamespace(
                status="attempted_not_fixed", reason="No repair proposal was available."
            ),
        ]
    )
    buckets = harness.classify_failures(result)
    assert buckets["rejected"] == 2
    assert buckets["attempted_not_fixed"] == 1
    assert buckets["reason_names_a_functional_dependency"] == 1


# --------------------------------------------------------------------------------------
# K1: the referents are read from the committed artifact, never from constants.
# --------------------------------------------------------------------------------------


def test_committed_write_counts_come_from_the_artifact(harness, tmp_path, monkeypatch):
    """A constant in the harness is what rotted for 54 days; K1 must read the gated artifact."""
    artifact = tmp_path / "declared.json"
    artifact.write_text(
        json.dumps(
            {
                "arms": {
                    "pipeline_declared_premise": {"writes": 7},
                    "pipeline_oracle_premise": {"writes": 11},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(harness, "DECLARED_ARTIFACT", artifact)
    assert harness.committed_writes() == {"declared": 7, "oracle": 11}


def test_the_real_committed_artifact_still_records_the_two_referents(harness):
    """Guards against the artifact being restructured out from under K1."""
    committed = harness.committed_writes()
    assert set(committed) == {"declared", "oracle"}
    assert committed["declared"] == 0
    assert committed["oracle"] == 54


def test_the_predictor_agrees_with_the_shipped_verifier_on_the_fixture(
    harness, frame, schema, tmp_path
):
    """A unit-scale K2: the predictor and `DirectVerifier` must agree on a case with a known answer.

    Cheap insurance that the two have not drifted, run on every suite invocation rather than only
    when someone runs the corpus harness.
    """
    from dataforge.verifier.direct import DirectVerifier
    from dataforge.verifier.result import VerificationVerdict

    csv = tmp_path / "k2.csv"
    csv.write_text(
        "zip,city,note\n1,Reno,a\n1,Reno,b\n1,WRONG,c\n2,Provo,d\n2,Provo,e\n2,BAD,f\n2,BAD,g\n",
        encoding="utf-8",
    )
    table = read_csv(csv)
    verifier = DirectVerifier()
    for row, value, expect_accept in ((2, "Reno", True), (5, "Provo", False)):
        remaining, _ = harness.remaining_disagreements(frame, schema, row, "city", value)
        predictor_accepts = remaining == 0
        proposal = _shipped_proposal(row, "city", value)
        shipped = verifier.verify(table, [proposal], schema)
        shipped_accepts = shipped.verdict == VerificationVerdict.ACCEPT
        assert predictor_accepts == expect_accept
        assert predictor_accepts == shipped_accepts, (
            f"predictor and DirectVerifier disagree on row {row}: "
            f"{predictor_accepts} vs {shipped_accepts} ({shipped.reason})"
        )


def _shipped_proposal(row: int, column: str, value: str):
    """Build a real `ProposedFix` so `DirectVerifier` is exercised through its true contract."""
    from dataforge.repairers.base import ProposedFix
    from dataforge.transactions.txn import CellFix

    return ProposedFix(
        fix=CellFix(
            row=row,
            column=column,
            new_value=value,
            old_value="",
            operation="update",
            detector_id="fd_violation",
        ),
        confidence=1.0,
        provenance="deterministic",
        reason="unit test",
    )
