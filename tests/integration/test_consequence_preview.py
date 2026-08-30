"""The acceptance keystroke must show the reviewer THEIR table's consequence.

Until 2026-08-29 the only figure shown at this moment was hospital's -- 116 corrupted cells,
0.2046 harmful write rate -- measured on a public research corpus. A published statistic
about somebody else's table, at the moment a human authorises unsupervised writes to their
own. `PRODUCT.md`:193-201 already records that a number a user reads is a published claim
whatever file extension it lives in; this is the same argument about a number that describes
the wrong table.

Why the figure is marginal rather than per-candidate-in-isolation: `constraint-additivity.md`
measures that isolated per-candidate harm does not compose, and
`docs/trust/entailment-witness-result.md` quantifies it -- 2779 cells summed in isolation
against 567 for the set accepted together, while marginal deltas along an acceptance path
sum to 567 exactly.

The tests below pin three things that are each easy to get wrong in a way that would make the
preview worse than nothing: it must be correct, it must refuse rather than mislead when it
cannot be computed, and it must not claim to measure harm.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()


def _flat(text: str) -> str:
    """Collapse whitespace so assertions survive Rich's line wrapping.

    The message content is the contract; where the terminal chose to break it is not.
    """
    return " ".join(text.split())


#: Same fixture `tests/integration/test_zero_config_journey.py` uses, and located the same
#: way, so the preview's prediction can be checked against what that test asserts the repair
#: actually is.
FIXTURES = Path(__file__).resolve().parents[2] / "dataforge" / "fixtures"
_FIXTURE = "mined_fd_25rows.csv"


def _fixture_bytes() -> bytes:
    return (FIXTURES / _FIXTURE).read_bytes()


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    source = tmp_path / "t.csv"
    source.write_bytes(_fixture_bytes())
    result = runner.invoke(
        app, ["profile", str(source), "--constraints-out", str(tmp_path / "c.json")]
    )
    assert result.exit_code == 0, result.output
    return tmp_path


def _fd_candidate_id(artifact_path: Path, determinant: str, dependent: str) -> str:
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    for entry in payload["candidates"]:
        candidate = entry["candidate"]
        if (
            candidate["kind"] == "functional_dependency"
            and candidate.get("columns") == [determinant]
            and candidate.get("dependent") == dependent
        ):
            return str(entry["candidate_id"])
    raise AssertionError(f"{determinant} -> {dependent} was not mined from the fixture")


class TestConsequencePreviewIsCorrect:
    def test_it_predicts_the_cell_the_repair_actually_writes(self, workspace: Path) -> None:
        """The preview and the repair must agree.

        `test_zero_config_journey.py` asserts the repair writes (row 24, city) changing
        `albny` to `albany`. A preview that named a different cell would be worse than no
        preview: it would teach the reviewer to distrust it.
        """
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert result.exit_code == 0, result.output
        assert "state -> city" in result.output
        assert "albny" in result.output
        assert "albany" in result.output
        assert "row 24" in result.output

    def test_an_inert_dependency_reports_no_rewrites(self, workspace: Path) -> None:
        """`city -> state` holds exactly on this fixture, so it rewrites nothing.

        The asymmetry this surfaces is the whole finding: a dependency is inert where its
        determinant groups already agree, whatever its confidence.
        """
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "city", "state")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert result.exit_code == 0, result.output
        assert "No cell in this table would be rewritten" in _flat(result.output)

    def test_no_preview_when_nothing_was_newly_accepted(self, workspace: Path) -> None:
        """Rejecting a candidate authorises no write, so there is no consequence to show."""
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--reject", candidate, "--no-tui"],
        )

        assert result.exit_code == 0, result.output
        assert "would do to THIS table" not in result.output


class TestConsequencePreviewRefusesRatherThanMisleads:
    """An unavailable preview must never read as "this acceptance is harmless"."""

    def test_a_missing_source_says_so_and_says_what_it_does_not_mean(self, workspace: Path) -> None:
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")
        (workspace / "t.csv").unlink()

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert result.exit_code == 0, result.output
        assert "unavailable" in result.output
        assert "not a statement that they would rewrite nothing" in _flat(result.output)

    def test_a_changed_source_is_refused_because_it_is_a_different_table(
        self, workspace: Path
    ) -> None:
        """Silently previewing against edited bytes would describe a table nobody reviewed."""
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")
        source = workspace / "t.csv"
        source.write_bytes(source.read_bytes() + b"99,ZZ,nowhere\n")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert result.exit_code == 0, result.output
        assert "no longer matches the hash" in _flat(result.output)

    def test_an_explicit_source_flag_is_honoured(self, workspace: Path) -> None:
        """A reviewer whose table moved must be able to point at it."""
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")
        moved = workspace / "moved" / "t.csv"
        moved.parent.mkdir()
        moved.write_bytes((workspace / "t.csv").read_bytes())
        (workspace / "t.csv").unlink()

        result = runner.invoke(
            app,
            [
                "constraints",
                "review",
                str(artifact),
                "--accept",
                candidate,
                "--no-tui",
                "--source",
                str(moved),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "state -> city" in result.output
        assert "unavailable" not in result.output

    def test_the_preview_can_be_turned_off(self, workspace: Path) -> None:
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            [
                "constraints",
                "review",
                str(artifact),
                "--accept",
                candidate,
                "--no-tui",
                "--no-consequence",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "would do to THIS table" not in result.output


class TestConsequencePreviewDoesNotBreakMachineOutput:
    """``--json`` must emit only JSON, and must carry the data rather than the prose.

    The first version of this feature printed the human table unconditionally and corrupted
    ``--json``, which `test_cli_constraints.py` caught. Recorded as a test because the failure
    is silent for any consumer that does not parse strictly, and an agent is exactly such a
    consumer.
    """

    def test_json_output_stays_parseable(self, workspace: Path) -> None:
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            [
                "constraints",
                "review",
                str(artifact),
                "--accept",
                candidate,
                "--no-tui",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["acceptance_consequence"]["available"] is True

    def test_json_output_carries_the_marginal_counts(self, workspace: Path) -> None:
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            [
                "constraints",
                "review",
                str(artifact),
                "--accept",
                candidate,
                "--no-tui",
                "--json",
            ],
        )

        block = json.loads(result.output)["acceptance_consequence"]
        assert block["marginal"] is True
        assert block["covers"] == ["functional_dependency"]
        assert block["total_cells_rewritten"] == 1
        assert block["dependencies"][0]["dependency"] == "state -> city"
        assert "albny" in str(block["dependencies"][0]["example"])

    def test_json_output_reports_unavailability_as_data(self, workspace: Path) -> None:
        """A consumer must be able to tell "no consequence" from "could not compute"."""
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")
        (workspace / "t.csv").unlink()

        result = runner.invoke(
            app,
            [
                "constraints",
                "review",
                str(artifact),
                "--accept",
                candidate,
                "--no-tui",
                "--json",
            ],
        )

        block = json.loads(result.output)["acceptance_consequence"]
        assert block["available"] is False
        assert "not a statement that they would rewrite nothing" in block["reason"]

    """Without a clean copy of the table, a replaced value may be exactly the error wanted.

    On this fixture the single replaced value IS the error, so a preview labelled "harm"
    would be wrong in the most misleading possible direction -- it would report the repair
    the user wants as damage.
    """

    def test_the_message_distinguishes_consequence_from_harm(self, workspace: Path) -> None:
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert "CONSEQUENCE, not harm" in _flat(result.output)
        assert "may be exactly the error you want repaired" in _flat(result.output)

    def test_the_message_states_its_coverage(self, workspace: Path) -> None:
        """F1 in the pre-registration: the witness covers functional dependencies only."""
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert "functional dependencies only" in _flat(result.output)

    def test_the_message_says_the_writes_bypass_calibration(self, workspace: Path) -> None:
        """The fact that makes this consequential: nothing downstream holds these back."""
        artifact = workspace / "c.json"
        candidate = _fd_candidate_id(artifact, "state", "city")

        result = runner.invoke(
            app,
            ["constraints", "review", str(artifact), "--accept", candidate, "--no-tui"],
        )

        assert "deterministic" in result.output
        assert "calibration threshold" in _flat(result.output)
