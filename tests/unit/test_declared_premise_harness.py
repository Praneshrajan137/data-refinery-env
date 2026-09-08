"""Pin the declared-premise harness's REFUSALS, which is where it can mislead.

The finding this harness produced is a **zero**: a premise a user could plausibly declare writes
nothing through the shipped pipeline on hospital. A zero is the most dangerous kind of result to
publish, because at least four different bugs produce it -- an unbound premise, a premise whose
columns are misspelled, a scorer that reports 0/0 as 0.0, and a hash check that silently passes.
Each of those would have produced the same headline for the wrong reason.

So the refusals are pinned independently of the corpus: the harness must be shown to REFUSE a
vacuous premise, to REFUSE an unfrozen premise, and to distinguish "wrote nothing" from "wrote
wrongly". `tests/unit/test_premise_write_exposure_harness.py` does the same job for the P3
harness and is the model for this one.
"""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from dataforge.detectors.base import FunctionalDependency, Schema

_HARNESS = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "bench"
    / "measure_declared_premise_capability.py"
)
_PREMISE = Path(__file__).resolve().parents[2] / "eval" / "premises" / "hospital_declared.yaml"


def _load():
    spec = importlib.util.spec_from_file_location("_declared_premise_harness", _HARNESS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _load()


def _fix(row: int, column: str, value: str, detector: str = "fd_violation") -> SimpleNamespace:
    return SimpleNamespace(row=row, column=column, new_value=value, detector_id=detector)


@pytest.fixture
def frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dirty = pd.DataFrame(
        {
            "zip": ["1", "1", "1", "2", "2", "2"],
            "city": ["Reno", "Reno", "WRONG", "Provo", "Provo", "Provo"],
            "flat": ["x", "x", "x", "x", "x", "x"],
        }
    )
    clean = dirty.copy()
    clean.loc[2, "city"] = "Reno"
    return dirty, clean


# --------------------------------------------------------------------------------------
# K3: the premise must be provably the one that was frozen before ground truth was seen.
# --------------------------------------------------------------------------------------


def test_premise_hash_ignores_line_endings(harness, tmp_path):
    """A CRLF worktree and an LF checkout must hash the same, or CI voids every run.

    This is not hypothetical. `core.autocrlf=true` leaves CRLF in a Windows worktree while git
    stores LF, so a byte hash would make K3 fire in Linux CI for a reason that has nothing to do
    with the premise -- and a kill criterion that fires for the wrong reason teaches people to
    ignore it.
    """
    lf = tmp_path / "lf.yaml"
    crlf = tmp_path / "crlf.yaml"
    lf.write_bytes(b"columns:\n  a: str\n")
    crlf.write_bytes(b"columns:\r\n  a: str\r\n")
    assert harness.premise_sha256(lf) == harness.premise_sha256(crlf)


def test_committed_premise_matches_the_registered_hash(harness):
    """The shipped premise must satisfy its own K3, so the artifact is reproducible."""
    assert harness.premise_sha256(_PREMISE) == harness.registered_premise_sha256()


def test_a_premise_hash_is_sensitive_to_content(harness, tmp_path):
    """Normalising line endings must not make the hash blind to a changed dependency."""
    one = tmp_path / "one.yaml"
    two = tmp_path / "two.yaml"
    one.write_text("columns:\n  a: str\n", encoding="utf-8")
    two.write_text("columns:\n  b: str\n", encoding="utf-8")
    assert harness.premise_sha256(one) != harness.premise_sha256(two)


def test_a_preregistration_without_a_hash_is_refused(harness, tmp_path, monkeypatch):
    """An unfrozen premise must abort, never be measured and quietly reported."""
    empty = tmp_path / "prereg.md"
    empty.write_text("# no hash recorded here\n", encoding="utf-8")
    monkeypatch.setattr(harness, "PREREGISTRATION", empty)
    with pytest.raises(SystemExit) as excinfo:
        harness.registered_premise_sha256()
    assert "K3" in str(excinfo.value)


# --------------------------------------------------------------------------------------
# K2: a premise that never bound anything is not a premise that refused.
# --------------------------------------------------------------------------------------


def test_a_binding_premise_passes_the_vacuity_check(harness, frames):
    dirty, _clean = frames
    schema = Schema(
        columns={"zip": "str", "city": "str", "flat": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
    )
    bound, report = harness.check_vacuity(schema, dirty)
    assert bound is True
    assert report["fd_issues_raised_on_declared_dependents"] > 0
    assert report["columns_absent_from_frame"] == []


def test_a_misspelled_column_is_caught_rather_than_scored_as_a_refusal(harness, frames):
    """A typo in a premise writes nothing. Reporting that as a capability result would be false."""
    dirty, _clean = frames
    schema = Schema(
        columns={"zip": "str", "CITY_TYPO": "str"},
        functional_dependencies=(
            FunctionalDependency(determinant=("zip",), dependent="CITY_TYPO"),
        ),
    )
    bound, report = harness.check_vacuity(schema, dirty)
    assert bound is False
    assert "CITY_TYPO" in report["columns_absent_from_frame"]


def test_a_constant_dependent_is_caught(harness, frames):
    """Nothing can violate a dependency into a single-valued column, so it cannot repair."""
    dirty, _clean = frames
    schema = Schema(
        columns={"zip": "str", "flat": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="flat"),),
    )
    bound, report = harness.check_vacuity(schema, dirty)
    assert bound is False
    assert "flat" in report["declared_dependents_that_are_constant"]


def test_declared_str_types_grant_no_authority(harness, frames):
    """`str` does not discriminate, so authority must come only from the dependencies.

    Load-bearing for the premise's defence: if a 20-column `str` map granted blanket authority,
    the declared arm could be accused of measuring the type map rather than the declaration.
    """
    dirty, _clean = frames
    schema = Schema(
        columns={"zip": "str", "city": "str", "flat": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
    )
    _bound, report = harness.check_vacuity(schema, dirty)
    assert set(report["authoritative_columns"]) == {"zip", "city"}
    assert report["declared_columns"] == 3


# --------------------------------------------------------------------------------------
# Scoring: a refusal is not a wrong answer.
# --------------------------------------------------------------------------------------


def test_a_zero_write_arm_reports_no_write_precision_rather_than_zero(harness, frames):
    """The declared arm writes 0. Reporting precision 0.0 would call a refusal an error."""
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    scored = harness.score_arm([], (), dirty, clean, truth)
    assert scored["writes"] == 0
    assert scored["write_precision"] is None


def test_the_shared_scorer_precision_is_still_reported_for_comparability(harness, frames):
    """Both numbers must be present: `score_repairs` gives 0.0, and that is what the stage
    artifact records, so dropping it would make the arms incomparable."""
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    scored = harness.score_arm([], (), dirty, clean, truth)
    assert scored["precision"] == 0.0
    assert scored["write_precision"] is None


def test_writes_are_attributed_to_the_detector_that_produced_them(harness, frames):
    """Without this, another detector's win inside the covered columns looks like an FD win."""
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    split = harness.by_detector(
        [_fix(2, "city", "Reno"), _fix(0, "city", "Sparks", detector="value_format")],
        dirty,
        clean,
        truth,
    )
    assert split["fd_violation"]["repaired_a_real_error"] == 1
    assert split["value_format"]["corrupted_a_clean_cell"] == 1


def test_the_four_outcome_vocabulary_is_not_reinvented(harness, frames):
    """A fifth name for the same four outcomes would make the instruments incomparable."""
    dirty, clean = frames
    scored = harness.score_arm([], (), dirty, clean, {})
    assert {
        "repaired_a_real_error",
        "wrong_value_on_a_real_error",
        "no_op_on_a_clean_cell",
        "corrupted_a_clean_cell",
    } <= set(scored)


# --------------------------------------------------------------------------------------
# The premise is graded by ground truth, never admitted by it.
# --------------------------------------------------------------------------------------


def test_a_refuted_declared_dependency_is_reported_not_dropped(harness, frames):
    """This is the whole difference between a declared premise and the oracle arm.

    `discover_oracle_fds` uses `fd_holds_on_clean` as an ADMISSION FILTER. Here the same predicate
    only GRADES, so a dependency that ground truth refutes must appear in the report and stay in
    the arm.
    """
    _dirty, clean = frames
    fds = (
        FunctionalDependency(determinant=("zip",), dependent="city"),
        FunctionalDependency(determinant=("city",), dependent="zip"),
    )
    audit = harness.premise_audit(fds, clean, None)
    assert audit["fd_count"] == 2
    assert audit["fd_count_holding_on_clean"] <= 2
    assert isinstance(audit["declared_fds_refuted_by_ground_truth"], list)


def test_the_committed_premise_records_a_warrant_for_every_dependency(harness):
    """An unlabelled warrant would silently collapse the documented/format-evident split."""
    warrants = harness.load_premise_warrants(_PREMISE)
    assert warrants, "the premise records no dependencies"
    assert "unlabelled" not in set(warrants.values())
    assert set(warrants.values()) == {"cms_documented", "format_evident"}


def test_thinning_keeps_column_coverage_and_drops_redundancy(harness):
    """The reverse mechanism probe depends on this being coverage-preserving."""
    fds = (
        FunctionalDependency(determinant=("a",), dependent="c"),
        FunctionalDependency(determinant=("b",), dependent="c"),
        FunctionalDependency(determinant=("a",), dependent="d"),
    )
    thinned = harness.thin_to_one_determinant(fds)
    assert {fd.dependent for fd in thinned} == {"c", "d"}
    assert len(thinned) == 2


# --------------------------------------------------------------------------------------
# K1: the referents are read from gated artifacts, never from constants in the harness.
# --------------------------------------------------------------------------------------


def test_the_anchor_referent_comes_from_the_gated_artifact(harness, tmp_path, monkeypatch):
    """A constant in the harness is exactly what rotted for 54 days."""
    artifact = tmp_path / "agent_comparison.json"
    artifact.write_text(
        '{"records": [{"method": "heuristic", "dataset": "hospital", "f1": 0.4242}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr(harness, "ANCHOR_ARTIFACT", artifact)
    assert harness.anchor_f1() == 0.4242


def test_a_missing_anchor_record_aborts(harness, tmp_path, monkeypatch):
    artifact = tmp_path / "agent_comparison.json"
    artifact.write_text('{"records": []}', encoding="utf-8")
    monkeypatch.setattr(harness, "ANCHOR_ARTIFACT", artifact)
    with pytest.raises(SystemExit) as excinfo:
        harness.anchor_f1()
    assert "K1a" in str(excinfo.value)


def test_the_mined_c4_referent_comes_from_the_committed_stage_artifact(
    harness, tmp_path, monkeypatch
):
    """Two independently gated referents, because one is what drifted."""
    artifact = tmp_path / "stage.json"
    artifact.write_text(
        '{"arms": {"pipeline_c4_declared_authority": {"writes": 7, "tp": 3, "fp": 4}}}',
        encoding="utf-8",
    )
    monkeypatch.setattr(harness, "STAGE_ARTIFACT", artifact)
    assert harness.stage_mined_c4() == {"writes": 7, "tp": 3, "fp": 4}


def test_the_premise_is_loadable_by_the_shipped_user_facing_loader():
    """The declared arm must enter through the same door as `dataforge repair --schema`.

    Imported here rather than through the harness so the assertion is about the product's loader,
    not about the harness agreeing with itself.
    """
    from dataforge.cli.common import load_schema

    schema = load_schema(_PREMISE)
    assert len(schema.functional_dependencies) == 15
    assert len(schema.columns) == 20
    assert all(declared == "str" for declared in schema.columns.values())


def test_the_premise_hash_recorded_in_the_artifact_matches_the_file(harness):
    """Guards the artifact against describing a premise that is no longer on disk."""
    import json

    artifact = Path(__file__).resolve().parents[2] / "eval" / "results"
    payload = json.loads(
        (artifact / "declared_premise_capability.json").read_text(encoding="utf-8")
    )
    assert payload["premise_sha256"] == harness.premise_sha256(_PREMISE)
    expected = hashlib.sha256(
        _PREMISE.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")
    ).hexdigest()
    assert payload["premise_sha256"] == expected
