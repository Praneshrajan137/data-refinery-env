"""The docs-truth checker must actually bite.

This checker exists because the API phase published five false or mis-scoped numbers in
prose, none of which CI could catch: `readme_truth.py` polices claim *kinds*,
`benchmark_truth.py` regenerates marker blocks, and `openapi_contract.py` diffs snapshots.
A hand-written number in `DECISIONS.md` was unguarded.

A truth checker that cannot fail is worse than none, because it manufactures confidence.
These tests therefore verify the failure modes, not just the happy path.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.ci import docs_truth
from tests.support.docs_truth_sandbox import build_docs_truth_sandbox

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHECKER = PROJECT_ROOT / "scripts" / "ci" / "docs_truth.py"
LEDGER = PROJECT_ROOT / "docs" / "quantitative_claims.yaml"


def _run(root: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Run the checker, against a sandbox root when one is given.

    Every divergence test passes a sandbox. Falsifying a file in the real repository would be
    correct serially and unsafe under ``-n``: two workers can each read ``original`` while the
    other holds the file falsified, and the second ``finally`` writes the falsified bytes back
    for good.
    """
    argv = [sys.executable, str(CHECKER), "--check"]
    if root is not None:
        argv += ["--root", str(root)]
    return subprocess.run(argv, capture_output=True, text=True, cwd=PROJECT_ROOT)


class TestLedgerIsWellFormed:
    def test_ledger_exists_and_parses(self) -> None:
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        assert isinstance(payload["claims"], list)
        assert payload["claims"], "an empty ledger would make the checker vacuous"

    def test_every_claim_is_complete(self) -> None:
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        seen: set[str] = set()
        for claim in payload["claims"]:
            for field in ("id", "doc", "artifact", "pointer", "expect"):
                assert field in claim, f"{claim.get('id')} missing {field}"
            assert claim["id"] not in seen, f"duplicate claim id {claim['id']}"
            seen.add(str(claim["id"]))

    def test_the_disconfirming_flights_claim_is_bound(self) -> None:
        """The number that refuted a published claim must stay under CI."""
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        ids = {str(claim["id"]) for claim in payload["claims"]}
        assert "flights_llm_ranker_at_chance" in ids
        assert "free_ranker_transfer_rayyan" in ids


class TestCheckerPasses:
    def test_committed_state_is_consistent(self) -> None:
        result = _run()
        assert result.returncode == 0, f"docs truth check failing:\n{result.stderr}"
        assert "Verified" in result.stdout

    def test_the_sandbox_reproduces_the_committed_verdict(self, tmp_path: Path) -> None:
        """Non-vacuity for the sandbox itself.

        Every divergence test below asserts that a falsified sandbox *fails*. That is only
        evidence if an unfalsified sandbox *passes* -- otherwise the failures could come from
        the sandbox being incomplete, and the tests would pass while proving nothing.
        """
        root = build_docs_truth_sandbox(tmp_path / "sandbox")

        result = _run(root)

        assert result.returncode == 0, f"the unfalsified sandbox must pass:\n{result.stderr}"
        assert "Verified" in result.stdout


class TestCheckerDetectsDivergence:
    """Both directions of divergence must fail: artifact drift and prose falsification."""

    def test_artifact_drift_is_detected(self, tmp_path: Path) -> None:
        root = build_docs_truth_sandbox(tmp_path / "sandbox")
        artifact = root / "eval" / "results" / "free_vs_llm_ranker.json"
        if not artifact.exists():
            pytest.skip("free ranker artifact not committed")

        payload = json.loads(artifact.read_text(encoding="utf-8"))
        block = payload["regimes"]["default"]["leave_one_dataset_out"]["rayyan"]
        block["free_transfer_roc_auc"] = 0.95
        artifact.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        result = _run(root)

        assert result.returncode == 1, "silent artifact drift was not detected"
        assert "free_ranker_transfer_rayyan" in result.stderr

    def test_prose_falsification_is_detected(self, tmp_path: Path) -> None:
        root = build_docs_truth_sandbox(tmp_path / "sandbox")
        doc = root / "DECISIONS.md"
        original = doc.read_text(encoding="utf-8")
        falsified = original.replace("0.2722", "0.9500")
        assert falsified != original, "the number under test moved out of DECISIONS.md"
        doc.write_text(falsified, encoding="utf-8")

        result = _run(root)

        assert result.returncode == 1, "a falsified prose number was not detected"
        assert "does not state" in result.stderr

    def test_missing_artifact_field_is_detected(self, tmp_path: Path) -> None:
        root = build_docs_truth_sandbox(tmp_path / "sandbox")
        artifact = root / "eval" / "results" / "free_vs_llm_ranker.json"
        if not artifact.exists():
            pytest.skip("free ranker artifact not committed")

        payload = json.loads(artifact.read_text(encoding="utf-8"))
        del payload["regimes"]["default"]["leave_one_dataset_out"]["rayyan"][
            "free_transfer_roc_auc"
        ]
        artifact.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        result = _run(root)

        assert result.returncode == 1, "a removed artifact field was not detected"


class TestShortValuesCannotBeSatisfiedIncidentally:
    """The correction of 2026-08-26, and the case that motivated it.

    Direction B was ``expected not in doc.read_text()`` -- a raw substring over the whole file. So a
    claim whose rendered value was ``"0"`` was satisfied by the document containing ``2026-08-26``.
    Fourteen of forty-six claims had an ``expect`` of two characters or fewer, and **six were
    verifiable in name only**: the prose could be edited to contradict them and CI would still pass.
    One of the six was a control arm that its own document described as the number making the claim
    precise. So ``"Verified 46 quantitative claims"`` was itself an overclaim -- the exact defect
    this checker exists to prevent, committed by the checker.

    These tests are about the TOKEN and CONTEXT rules that replaced it. They are written against
    the real failing inputs rather than invented ones, because the whole point is that the old
    check passed on these.
    """

    def test_a_digit_inside_a_date_does_not_satisfy_a_claim(self) -> None:
        """The literal case. ``0`` occurs in ``2026-08-26`` and must not count."""
        pattern = docs_truth._token_pattern("0")

        assert pattern.search("2026-08-26") is None
        assert pattern.search("An **LF** source has **0** lines re-terminated") is not None

    def test_a_digit_inside_a_filename_does_not_satisfy_a_claim(self) -> None:
        """``1`` occurs in ``premised_fd_10rows.csv``."""
        pattern = docs_truth._token_pattern("1")

        assert pattern.search("dataforge/fixtures/premised_fd_10rows.csv") is None
        assert pattern.search("| Cells changed | - | 1 |") is not None

    def test_a_value_inside_a_longer_number_does_not_satisfy_a_claim(self) -> None:
        """Both boundaries, including the asymmetric right-hand one.

        A rendered number may legitimately end a sentence with a full stop, but must not be
        followed by one that continues a number.
        """
        pattern = docs_truth._token_pattern("0.6189")

        assert pattern.search("10.6189") is None
        assert pattern.search("0.61890") is None
        assert pattern.search("write precision is 0.6189.") is not None
        assert docs_truth._token_pattern("11").search("110 rows") is None

    @pytest.mark.parametrize(
        ("value", "text", "matches"),
        [
            # A unit suffix states the value; it does not make a different token.
            ("25.42", "25.42x the review rows of hospital's 20", True),
            ("12", "12% of writes were harmful", True),
            # An ordinal is a different meaning and must not satisfy a claim.
            ("116", "the 116th row", False),
            ("116", "corrupts 116 clean cells", True),
            # The two cases that motivated the whole correction.
            ("0", "**Status**: open, recorded 2026-08-26", False),
            ("1", "dataforge/fixtures/premised_fd_10rows.csv", False),
        ],
    )
    def test_the_unit_suffix_allowance_is_narrow(
        self, value: str, text: str, matches: bool
    ) -> None:
        """``x`` and ``%`` are permitted after a value; arbitrary letters are not.

        The allowance was added when the gate rejected ``25.42x``, a legitimately-written
        multiplier. Widening it to any letter would let the word ``116th`` satisfy a claim of 116,
        so the two cases are pinned together -- the permission and its limit.
        """
        assert (docs_truth._token_pattern(value).search(text) is not None) is matches

    def test_every_short_claim_declares_context(self) -> None:
        """The class-level gate, so the six instances cannot recur as a seventh."""
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))

        missing = [
            str(claim["id"])
            for claim in payload["claims"]
            if len(str(claim["expect"])) <= docs_truth.CONTEXT_REQUIRED_MAX_LENGTH
            and not claim.get("context")
        ]

        assert missing == [], f"short-value claims without 'context': {missing}"

    def test_a_short_claim_without_context_is_refused(self, tmp_path: Path) -> None:
        """Non-vacuity for the gate above: it must actually fire."""
        errors = docs_truth._prose_errors(
            "probe",
            {"doc": "DECISIONS.md", "artifact": "a.json", "pointer": "/x"},
            "0",
        )

        assert any("must declare 'context'" in error for error in errors)

    def test_context_on_the_wrong_line_is_refused(self, tmp_path: Path) -> None:
        """The number must appear beside the claim it supports, not merely in the same file.

        This is the failure the token rule alone cannot catch: a document can legitimately contain
        a standalone ``0`` somewhere while the sentence that is supposed to state the claim says
        something else entirely.
        """
        doc = tmp_path / "claim.md"
        doc.write_text(
            "The count of things is 0 in general.\nThe LF control arm reported 7 lines.\n",
            encoding="utf-8",
        )

        errors = docs_truth._prose_errors(
            "probe",
            {
                "doc": str(doc),
                "pointer": "/x",
                "artifact": "a.json",
                "context": "LF control arm",
            },
            "0",
        )

        assert any("never on a line containing" in error for error in errors)

    def test_the_previously_vacuous_claims_are_now_falsifiable(self, tmp_path: Path) -> None:
        """The decisive test: contradict a claim that used to pass, and require a failure.

        ``line_ending_lf_control`` is the sharpest of the six. Its value is ``0`` and its document
        contains a date, so under the old substring rule the sentence could say any number at all.
        """
        root = build_docs_truth_sandbox(tmp_path / "sandbox")
        doc = root / "docs" / "trust" / "apply-rewrites-line-endings.md"
        original = doc.read_text(encoding="utf-8")
        falsified = original.replace(
            "An **LF** source has **0** lines re-terminated",
            "An **LF** source has **7** lines re-terminated",
        )
        assert falsified != original, "the sentence under test moved"
        doc.write_text(falsified, encoding="utf-8")

        result = _run(root)

        assert result.returncode == 1, (
            "contradicting the LF control arm did not fail the checker; "
            "Direction B is vacuous again"
        )
        assert "line_ending_lf_control" in result.stderr


class TestCheckerIsWiredIntoCi:
    def test_backend_gate_runs_it(self) -> None:
        text = (PROJECT_ROOT / "scripts" / "ci" / "backend_gate.py").read_text(encoding="utf-8")
        assert "docs_truth.py" in text, "the checker is not run by the aggregate gate"
        assert '"scripts/ci/docs_truth.py"' in text, "the checker is not type-checked in CI"

    def test_makefile_type_target_includes_it(self) -> None:
        text = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
        assert "scripts/ci/docs_truth.py" in text
