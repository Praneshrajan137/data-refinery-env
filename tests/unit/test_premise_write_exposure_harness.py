"""Pin the P3 write-exposure harness's scoring, especially where a zero-write arm can mislead.

The finding this harness produced is that a mined premise writes ONE cell through the shipped
pipeline, not the 451 widely quoted from a proposal-level instrument. A scoring bug that
silently zeroed or inflated an arm would have produced the same headline for the wrong reason,
so the scorer is pinned independently of the corpora.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

_HARNESS = Path(__file__).resolve().parents[2] / "scripts" / "bench" / "measure_premise_write_exposure.py"


def _load():
    spec = importlib.util.spec_from_file_location("_p3_harness", _HARNESS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _load()


def _fix(row: int, column: str, value: str) -> SimpleNamespace:
    return SimpleNamespace(row=row, column=column, new_value=value)


@pytest.fixture
def frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dirty = pd.DataFrame({"city": ["Reno", "WRONG", "Provo"], "zip": ["1", "2", "3"]})
    clean = pd.DataFrame({"city": ["Reno", "Tulsa", "Provo"], "zip": ["1", "2", "3"]})
    return dirty, clean


def test_truth_is_the_disagreement_between_dirty_and_clean(harness, frames):
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    assert truth == {(1, "city"): "Tulsa"}


def test_a_correct_write_on_a_real_error_is_a_repair(harness, frames):
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    tally = harness._score([_fix(1, "city", "Tulsa")], dirty, clean, truth)
    assert tally["repaired_a_real_error"] == 1
    assert tally["corrupted_a_clean_cell"] == 0
    assert tally["net_cells_improved"] == 1


def test_a_wrong_write_on_a_real_error_is_not_a_repair(harness, frames):
    """Hitting a real error with the wrong value is damage, not partial credit."""
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    tally = harness._score([_fix(1, "city", "Boise")], dirty, clean, truth)
    assert tally["repaired_a_real_error"] == 0
    assert tally["wrong_value_on_a_real_error"] == 1
    assert tally["net_cells_improved"] == -1


def test_overwriting_a_clean_cell_is_a_corruption(harness, frames):
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    tally = harness._score([_fix(0, "city", "Sparks")], dirty, clean, truth)
    assert tally["corrupted_a_clean_cell"] == 1
    assert tally["net_cells_improved"] == -1


def test_rewriting_a_clean_cell_with_its_own_value_is_a_no_op(harness, frames):
    """A no-op is not a corruption. Conflating them would inflate C4's apparent benefit."""
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    tally = harness._score([_fix(0, "city", "Reno")], dirty, clean, truth)
    assert tally["no_op_on_a_clean_cell"] == 1
    assert tally["corrupted_a_clean_cell"] == 0
    assert tally["net_cells_improved"] == 0


def test_a_zero_write_arm_reports_no_precision_rather_than_zero(harness, frames):
    """The C4 arm writes nothing. Reporting 0.0 precision would read as total failure.

    This is the trap the measured result walks straight into: `c4` writes 0 cells on every
    corpus, and a scorer that computed 0/0 as 0.0 would describe a refusal as a wrong answer.
    """
    dirty, clean = frames
    truth = harness._truth_by_cell(dirty, clean)
    tally = harness._score([], dirty, clean, truth)
    assert tally["writes"] == 0
    assert tally["write_precision"] is None
    assert tally["net_cells_improved"] == 0


def test_scoring_vocabulary_matches_the_k4_fence_exactly(harness, frames):
    """A fourth name for the same four outcomes would make the instruments incomparable.

    Comparing this arm against `measure_deductive_coverage.py` is the whole point of the P3
    finding, so the two must not drift apart in what they call things.
    """
    dirty, clean = frames
    tally = harness._score([], dirty, clean, {})
    assert {
        "repaired_a_real_error",
        "wrong_value_on_a_real_error",
        "no_op_on_a_clean_cell",
        "corrupted_a_clean_cell",
    } <= set(tally)


def test_fixes_outside_the_clean_frame_are_not_scored(harness, frames):
    """A column absent from ground truth cannot be judged, so it must not be counted."""
    dirty, clean = frames
    tally = harness._score([_fix(0, "not_a_column", "x"), _fix(99, "city", "x")], dirty, clean, {})
    assert tally["writes"] == 0


def test_beers_is_refused_by_the_dataset_scope_rule(harness, monkeypatch, capsys):
    """`beers` is excluded from this project; the harness must refuse it, not measure it."""
    monkeypatch.setattr(
        "sys.argv", ["measure_premise_write_exposure.py", "--corpora", "hospital,beers"]
    )
    assert harness.main() == 2
    assert "dataset-scope rule" in capsys.readouterr().err


def test_beers_is_absent_from_the_default_corpora(harness):
    assert "beers" not in harness.DEFAULT_CORPORA
