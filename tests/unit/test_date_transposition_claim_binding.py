"""The date-transposition claims are bound by an index-based JSON pointer.

`docs/quantitative_claims.yaml` binds this detector's measured `tp`, `fp` and `precision`
to `/per_detector/9/score/...` in `eval/results/cell_detection_rayyan.json`. That artifact
stores `per_detector` as a LIST, so the pointer names a position, not a detector.

**The residual gap is narrower than it first looks, and it is worth stating precisely
rather than overselling it.** I checked by planting a swap of indices 8 and 9, and
`docs_truth.py --check` DID fail -- with `cannot descend into NoneType at 'tp'`, because the
neighbouring entry never fired and carries no `score`. So the value gate already catches a
reorder whenever the new occupant's values are absent or differ. What it cannot catch is a
reorder onto a detector whose `tp`, `fp` and `precision` coincide exactly with 722, 0 and
1.0 -- unlikely, not impossible, and silent if it happens.

The second reason this file exists is diagnostic quality. `cannot descend into NoneType at
'tp'` does not tell a reader that three published claims are now describing the wrong
detector; `index 9 is now 'X', not 'DateTranspositionDetector'` does. A gate that fails for
the right reason with the wrong message costs the next person an investigation.

This is the identity half of a binding whose value half is already gated. It is the same
shape as the write-primitive registry deriving its population: a check that resolves a
position rather than a name can only see changes to the value at that position.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_ARTIFACT = _REPO / "eval" / "results" / "cell_detection_rayyan.json"
_LEDGER = _REPO / "docs" / "quantitative_claims.yaml"

#: The index the ledger's pointers name. Kept as a literal deliberately -- the ledger
#: hardcodes it too, and the point of this test is to fail when the two disagree with the
#: artifact, not to derive its way around the disagreement.
_BOUND_INDEX = 9
_BOUND_DETECTOR = "DateTranspositionDetector"


@pytest.fixture(scope="module")
def artifact() -> dict[str, object]:
    if not _ARTIFACT.exists():
        pytest.fail(
            f"{_ARTIFACT} is tracked in git and required by this test; the ledger binds "
            f"three claims into it"
        )
    payload = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


class TestThePointerResolvesToTheIntendedDetector:
    """The identity half of the binding, which docs_truth cannot check."""

    def test_index_nine_is_the_date_transposition_detector(
        self, artifact: dict[str, object]
    ) -> None:
        per_detector = artifact["per_detector"]
        assert isinstance(per_detector, list)
        assert len(per_detector) > _BOUND_INDEX, (
            f"per_detector has {len(per_detector)} entries, so the ledger's "
            f"/per_detector/{_BOUND_INDEX}/ pointers do not resolve at all"
        )
        entry = per_detector[_BOUND_INDEX]
        assert isinstance(entry, dict)
        assert entry.get("detector") == _BOUND_DETECTOR, (
            f"docs/quantitative_claims.yaml binds three claims to "
            f"/per_detector/{_BOUND_INDEX}/score/, but index {_BOUND_INDEX} is now "
            f"{entry.get('detector')!r}, not {_BOUND_DETECTOR!r}. Those claims are now "
            f"describing the wrong detector. Repoint them, or restore the ordering."
        )

    def test_the_detector_appears_exactly_once(self, artifact: dict[str, object]) -> None:
        """Two entries for one detector would make 'the' bound score ambiguous."""
        per_detector = artifact["per_detector"]
        assert isinstance(per_detector, list)
        matches = [
            i
            for i, e in enumerate(per_detector)
            if isinstance(e, dict) and e.get("detector") == _BOUND_DETECTOR
        ]
        assert matches == [_BOUND_INDEX], (
            f"{_BOUND_DETECTOR} appears at indices {matches}; the ledger assumes exactly "
            f"[{_BOUND_INDEX}]"
        )


class TestTheBoundValuesAreTheOnesTheDocstringQuotes:
    """The docstring's corrected numbers must be the artifact's numbers."""

    def test_scores_match_the_corrected_docstring(self, artifact: dict[str, object]) -> None:
        per_detector = artifact["per_detector"]
        assert isinstance(per_detector, list)
        entry = per_detector[_BOUND_INDEX]
        assert isinstance(entry, dict)
        score = entry["score"]
        assert isinstance(score, dict)
        assert score["tp"] == 722
        assert score["fp"] == 0
        assert score["precision"] == 1.0

    def test_the_detector_module_no_longer_quotes_the_rejected_design(self) -> None:
        """0.94 belonged to a different, rejected Phase 1C detector and has no artifact.

        The first version of this test asserted the old phrasing was simply ABSENT. That
        was a badly designed guard: the corrected docstring has to QUOTE the old wording in
        order to explain what was wrong with it, so the guard failed on the very edit it was
        written to protect. The property that actually matters is not absence but framing --
        0.94 may appear, provided it appears as a superseded figure attributed to another
        design, and provided the shipped figure is stated.
        """
        source = (_REPO / "dataforge" / "detectors" / "date_transposition.py").read_text(
            encoding="utf-8"
        )
        assert "1.0000, not 0.94" in source, (
            "the docstring should state the shipped figure and name the distinction, so "
            "the next reader does not re-derive the wrong conclusion from DECISIONS.md"
        )
        # Wherever the old phrasing survives, it must be marked as what the file used to
        # say -- not asserted in the present tense about this detector.
        normalized = " ".join(source.split())
        if "measured best precision ~0.94" in normalized:
            assert "previously read" in normalized, (
                "the docstring states 'measured best precision ~0.94' without marking it "
                "as the superseded wording, so it reads as this detector's measured "
                "precision again"
            )
        assert "rejected detector design" in normalized, (
            "the docstring must attribute 0.94 to the design it actually described, or the "
            "next reader will find it in DECISIONS.md and reinstate the wrong conclusion"
        )


class TestTheLedgerActuallyBindsTheseClaims:
    """Non-vacuity: if the ledger entries were dropped, this file proves nothing."""

    def test_the_three_claim_ids_are_registered(self) -> None:
        text = _LEDGER.read_text(encoding="utf-8")
        for claim_id in (
            "date_transposition_rayyan_tp",
            "date_transposition_rayyan_fp",
            "date_transposition_rayyan_precision",
        ):
            assert f"id: {claim_id}" in text, (
                f"{claim_id} is not registered in docs/quantitative_claims.yaml, so the "
                f"docstring's figures are unbound again and this test guards nothing"
            )

    def test_the_ledger_uses_the_index_this_test_pins(self) -> None:
        text = _LEDGER.read_text(encoding="utf-8")
        assert f"/per_detector/{_BOUND_INDEX}/score/" in text, (
            f"the ledger no longer points at /per_detector/{_BOUND_INDEX}/score/, so "
            f"_BOUND_INDEX in this test is stale and the identity check is guarding "
            f"nothing"
        )
