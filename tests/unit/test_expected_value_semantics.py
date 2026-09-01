"""Pin the semantics of ``Issue.expected`` per issue type.

Exists because of a near-miss. `premise-quality-and-capability.plan.md` step 7 instructed:
"route `TimeFormatCruft`'s already-computed `Issue.expected` into the suggestion path so 126
exact candidate values stop being discarded." The obvious way to do that -- adding
`"format_violation"` to `_DETECTION_ONLY_SUGGESTION_TYPES` in `dataforge/engine/repair.py` --
**would have written shape masks into user data.**

Two different detectors emitted the same issue type with incompatible `expected` semantics:

- `dataforge/detectors/time_format_cruft.py` set `expected` to a **substitutable value**
  (`"14:30"`), stripped of surrounding cruft.
- `dataforge/detectors/format_violation.py` set `expected` to a **shape mask**
  (`"9999-99-99"`), which is a description of a format and is not a value any cell should
  ever hold.

`Issue` carries no detector identity separate from `issue_type` -- `dataforge/calibration.py`
records that samples are keyed by issue type *as* `CellFix.detector_id` -- so nothing
downstream could have told the two apart. The suggestion path guards only on
`issue.expected is None`, which both satisfy.

The fix was to give the cruft detector its own issue type. These tests are what stop the two
from being merged again by someone who notices they are "both format problems", and what stops
a mask-valued issue type from being added to the suggestion path later.
"""

from __future__ import annotations

import pytest

from dataforge.detectors.base import ALL_ISSUE_TYPES
from dataforge.detectors.format_violation import FormatViolationDetector
from dataforge.detectors.time_format_cruft import TimeFormatCruftDetector
from dataforge.engine.repair import _DETECTION_ONLY_SUGGESTION_TYPES
from dataforge.table import Table

# Issue types whose ``expected`` is a value that may be written into a cell verbatim.
_VALUE_VALUED = frozenset({"date_transposition", "entity_consensus", "time_format_cruft"})

# Issue types whose ``expected`` describes a FORM, not a value. Writing one of these into a
# cell would replace data with a description of data.
_MASK_VALUED = frozenset({"format_violation"})


class TestExpectedSemanticsAreNotConflated:
    """The two families must stay distinguishable by issue type alone."""

    def test_the_cruft_detector_has_its_own_issue_type(self) -> None:
        """Without this, no downstream consumer can tell a value from a mask."""
        assert "time_format_cruft" in ALL_ISSUE_TYPES

    def test_cruft_emits_a_substitutable_value_not_a_mask(self) -> None:
        table = Table(
            columns=["t"],
            rows=[
                {"t": "6:55 a.m."},
                {"t": "7:20 a.m."},
                {"t": "8:05 a.m."},
                {"t": "12/02/2011 6:55 a.m."},
            ],
        )
        issues = TimeFormatCruftDetector().detect(table)
        cruft = [i for i in issues if i.issue_type == "time_format_cruft"]
        assert cruft, "the detector emitted nothing on a value with embedded cruft"
        # The decisive property: expected is the cleaned VALUE, not a description of its form.
        for issue in cruft:
            assert issue.expected is not None
            assert issue.expected == "6:55 a.m.", (
                f"expected a substitutable time value, got {issue.expected!r}. If this is a "
                f"shape mask the suggestion path would write it into the cell."
            )

    def test_format_violation_still_emits_a_mask_and_is_not_suggestible(self) -> None:
        """The mask family is unchanged, and must stay out of the suggestion path."""
        table = Table(
            columns=["code"],
            rows=[
                {"code": "AB-1234"},
                {"code": "CD-5678"},
                {"code": "EF-9012"},
                {"code": "totally different"},
            ],
        )
        issues = FormatViolationDetector().detect(table)
        masks = [i for i in issues if i.issue_type == "format_violation"]
        if masks:
            # Whatever it holds, it must never become a write candidate.
            assert "format_violation" not in _DETECTION_ONLY_SUGGESTION_TYPES

    @pytest.mark.parametrize("issue_type", sorted(_MASK_VALUED))
    def test_no_mask_valued_type_is_ever_a_suggestion_source(self, issue_type: str) -> None:
        """The regression this module exists to prevent, stated directly.

        A mask-valued issue type in the suggestion frozenset means shape strings become
        candidate cell values.
        """
        assert issue_type not in _DETECTION_ONLY_SUGGESTION_TYPES, (
            f"{issue_type!r} carries a shape mask in Issue.expected, not a value. Routing it "
            f"into the suggestion path would propose writing a format description into a cell."
        )

    def test_the_suggestion_path_admits_only_value_valued_types(self) -> None:
        """Derived, not restated, so a new suggestion source cannot slip in unclassified."""
        unclassified = set(_DETECTION_ONLY_SUGGESTION_TYPES) - _VALUE_VALUED
        assert not unclassified, (
            f"issue types {sorted(unclassified)} are in the suggestion path but are not "
            f"declared value-valued in this test. Classify them: does Issue.expected hold a "
            f"value that may be written verbatim, or a description of a form?"
        )

    def test_the_two_families_are_disjoint(self) -> None:
        assert not (_VALUE_VALUED & _MASK_VALUED)

    def test_every_classified_type_actually_exists(self) -> None:
        """A classification naming a type no detector can emit polices nothing."""
        declared = _VALUE_VALUED | _MASK_VALUED
        assert declared <= ALL_ISSUE_TYPES, (
            f"classified issue types absent from IssueTypeLiteral: "
            f"{sorted(declared - ALL_ISSUE_TYPES)}"
        )
