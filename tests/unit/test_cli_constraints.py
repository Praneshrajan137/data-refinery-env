"""CLI and TUI tests for reviewed constraint artifacts."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app
from dataforge.cli.constraints import ConstraintReviewApp
from dataforge.schema_inference import (
    ConstraintReviewArtifact,
    build_constraint_review_artifact,
    dump_constraint_review_artifact,
    infer_schema,
)
from dataforge.table import read_csv

runner = CliRunner()


def _write_fd_repairable_csv(path: Path) -> None:
    """Write a small table with one functional dependency violation."""
    path.write_text(
        "code,name\n"
        "A,Alpha\n"
        "A,Alpha\n"
        "A,Alfa\n"
        "B,Beta\n"
        "B,Beta\n"
        "C,Gamma\n"
        "C,Gamma\n"
        "D,Delta\n"
        "D,Delta\n"
        "E,Echo\n",
        encoding="utf-8",
    )


def _fd_artifact(csv_path: Path) -> ConstraintReviewArtifact:
    return build_constraint_review_artifact(
        infer_schema(read_csv(csv_path)),
        source_path=csv_path,
        source_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
    )


def _fd_candidate_id(artifact: ConstraintReviewArtifact) -> str:
    for reviewed in artifact.candidates:
        candidate = reviewed.candidate
        if (
            candidate.kind == "functional_dependency"
            and candidate.columns == ("code",)
            and candidate.dependent == "name"
        ):
            return reviewed.candidate_id
    raise AssertionError("expected code -> name FD candidate")


def test_constraints_review_json_lists_candidates(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    constraints_path = tmp_path / "constraints.json"
    _write_fd_repairable_csv(csv_path)
    constraints_path.write_text(dump_constraint_review_artifact(_fd_artifact(csv_path)))

    result = runner.invoke(
        app,
        ["constraints", "review", str(constraints_path), "--no-tui", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["schema_version"] == "constraint_review_v1"
    assert payload["candidate_count"] > 0
    assert payload["decision_counts"]["pending"] == payload["candidate_count"]
    assert any(candidate["repair_supported"] for candidate in payload["candidates"])


def test_constraints_review_noninteractive_accept_reject_note(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    constraints_path = tmp_path / "constraints.json"
    _write_fd_repairable_csv(csv_path)
    artifact = _fd_artifact(csv_path)
    fd_id = _fd_candidate_id(artifact)
    rejected_id = next(
        reviewed.candidate_id for reviewed in artifact.candidates if reviewed.candidate_id != fd_id
    )
    constraints_path.write_text(dump_constraint_review_artifact(artifact), encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "constraints",
            "review",
            str(constraints_path),
            "--accept",
            fd_id,
            "--reject",
            rejected_id,
            "--note",
            f"{fd_id}=reviewed by analyst",
            "--no-tui",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(constraints_path.read_text(encoding="utf-8"))
    reviewed_by_id = {candidate["candidate_id"]: candidate for candidate in payload["candidates"]}
    assert reviewed_by_id[fd_id]["decision"] == "accepted"
    assert reviewed_by_id[fd_id]["review_note"] == "reviewed by analyst"
    assert reviewed_by_id[rejected_id]["decision"] == "rejected"


def test_constraints_review_dry_run_leaves_file_unchanged(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    constraints_path = tmp_path / "constraints.json"
    _write_fd_repairable_csv(csv_path)
    artifact = _fd_artifact(csv_path)
    fd_id = _fd_candidate_id(artifact)
    constraints_path.write_text(dump_constraint_review_artifact(artifact), encoding="utf-8")
    before = constraints_path.read_bytes()

    result = runner.invoke(
        app,
        [
            "constraints",
            "review",
            str(constraints_path),
            "--accept",
            fd_id,
            "--dry-run",
            "--no-tui",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert constraints_path.read_bytes() == before
    assert json.loads(result.output)["sha256"] is None


def test_constraints_review_output_writes_separate_artifact(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    constraints_path = tmp_path / "constraints.json"
    output_path = tmp_path / "reviewed.json"
    _write_fd_repairable_csv(csv_path)
    artifact = _fd_artifact(csv_path)
    fd_id = _fd_candidate_id(artifact)
    constraints_path.write_text(dump_constraint_review_artifact(artifact), encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "constraints",
            "review",
            str(constraints_path),
            "--accept",
            fd_id,
            "--output",
            str(output_path),
            "--no-tui",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert '"accepted": 1' in result.output
    assert (
        json.loads(constraints_path.read_text(encoding="utf-8"))["candidates"][0]["decision"]
        == "pending"
    )


def test_constraints_review_invalid_artifact_exits_cleanly(tmp_path: Path) -> None:
    constraints_path = tmp_path / "constraints.json"
    constraints_path.write_text('{"schema_version": "wrong"}', encoding="utf-8")

    result = runner.invoke(app, ["constraints", "review", str(constraints_path), "--no-tui"])

    assert result.exit_code == 2
    assert "Constraint Review Error" in result.output


def test_constraints_review_acceptance_feeds_repair(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    constraints_path = tmp_path / "constraints.json"
    _write_fd_repairable_csv(csv_path)
    artifact = _fd_artifact(csv_path)
    fd_id = _fd_candidate_id(artifact)
    constraints_path.write_text(dump_constraint_review_artifact(artifact), encoding="utf-8")

    review_result = runner.invoke(
        app,
        ["constraints", "review", str(constraints_path), "--accept", fd_id, "--no-tui"],
        catch_exceptions=False,
    )
    repair_result = runner.invoke(
        app,
        [
            "repair",
            str(csv_path),
            "--constraints",
            str(constraints_path),
            "--dry-run",
            "--json",
            # C4 (2026-09-07): a mined constraint no longer confers write authority. This
            # test's subject is that review acceptance FEEDS repair, so the opt-in keeps it
            # measuring the feed rather than the write default.
            "--trust-mined-constraints",
        ],
        catch_exceptions=False,
    )

    assert review_result.exit_code == 0
    assert repair_result.exit_code == 0
    payload = json.loads(repair_result.output)
    assert payload["receipt"]["accepted_constraint_ids"] == [fd_id]
    assert payload["fixes"][0]["detector_id"] == "fd_violation"


def test_constraints_review_textual_accepts_selected_candidate(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    _write_fd_repairable_csv(csv_path)
    artifact = _fd_artifact(csv_path)
    first_id = artifact.candidates[0].candidate_id
    review_app = ConstraintReviewApp(artifact)

    async def exercise() -> None:
        async with review_app.run_test() as pilot:
            await pilot.press("a")
            await pilot.press("s")

    asyncio.run(exercise())

    reviewed = {candidate.candidate_id: candidate for candidate in review_app.artifact.candidates}
    assert review_app.saved is True
    assert reviewed[first_id].decision == "accepted"


class TestTestedConfidenceReachesItsConsumer:
    """The number is REPORTED, so it must actually arrive. Added 2026-08-26.

    `tested_confidence` is the confidence measured only on rows that can falsify a dependency --
    singleton determinant groups are consistent with any value and inflate the shipped `confidence`.
    On hospital it separates true from false mined dependencies perfectly where `confidence`
    overlaps, and it was deliberately NOT shipped as a gate: the separating threshold is fitted to
    one corpus and no other corpus can validate it. The resolution was to report it to the human who
    accepts the constraint instead.

    That resolution was right and its delivery was incomplete. The field reached a human only inside
    the English `evidence` blob: `_candidate_summary` emitted `confidence` alone, so a programmatic
    consumer saw the inflated number and had to parse prose for the honest one, and no column showed
    it. A named consumer that cannot read the field is not a consumer -- which is the rule
    PRODUCT.md section 1.3 states one level up, about hardening a component nothing reads.
    """

    def test_the_json_summary_carries_tested_confidence(self, tmp_path: Path) -> None:
        """The machine-readable path, which had only the inflated number."""
        csv_path = tmp_path / "fd.csv"
        constraints_path = tmp_path / "constraints.json"
        _write_fd_repairable_csv(csv_path)
        constraints_path.write_text(dump_constraint_review_artifact(_fd_artifact(csv_path)))

        result = runner.invoke(
            app,
            ["constraints", "review", str(constraints_path), "--no-tui", "--json"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        candidates = json.loads(result.output)["candidates"]
        fds = [c for c in candidates if c["kind"] == "functional_dependency"]
        assert fds, "fixture must mine a dependency or this test is vacuous"
        assert all("tested_confidence" in candidate for candidate in candidates)
        assert any(candidate["tested_confidence"] is not None for candidate in fds)

    def test_the_review_table_has_its_own_tested_column(self, tmp_path: Path) -> None:
        """The human path. Previously the number existed only inside the Evidence prose."""
        csv_path = tmp_path / "fd.csv"
        constraints_path = tmp_path / "constraints.json"
        _write_fd_repairable_csv(csv_path)
        constraints_path.write_text(dump_constraint_review_artifact(_fd_artifact(csv_path)))

        result = runner.invoke(
            app,
            ["constraints", "review", str(constraints_path), "--no-tui"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        assert "Tested" in result.output

    def test_an_absent_tested_confidence_renders_as_not_applicable(self, tmp_path: Path) -> None:
        """ "n/a", never a blank or a zero.

        Most candidate kinds are not dependencies and have no tested confidence. Rendering that as
        an empty cell or 0.0000 would invite a reviewer to read "unknown" as "low", which is the
        opposite of the field's purpose -- it exists so a reviewer can see when a high confidence is
        unsupported, not to make unsupported and low look alike.
        """
        csv_path = tmp_path / "fd.csv"
        constraints_path = tmp_path / "constraints.json"
        _write_fd_repairable_csv(csv_path)
        constraints_path.write_text(dump_constraint_review_artifact(_fd_artifact(csv_path)))

        result = runner.invoke(
            app,
            ["constraints", "review", str(constraints_path), "--no-tui"],
            catch_exceptions=False,
        )

        assert "n/a" in result.output

    def test_tested_confidence_never_gates_a_decision(self, tmp_path: Path) -> None:
        """The refusal, pinned. Reporting it must not become deciding with it.

        Every candidate stays `pending` regardless of its tested confidence. If a future change
        auto-rejected below a threshold, that threshold would be the one fitted to a single corpus
        which this project declined to ship -- and this test is where that would surface.
        """
        csv_path = tmp_path / "fd.csv"
        constraints_path = tmp_path / "constraints.json"
        _write_fd_repairable_csv(csv_path)
        constraints_path.write_text(dump_constraint_review_artifact(_fd_artifact(csv_path)))

        result = runner.invoke(
            app,
            ["constraints", "review", str(constraints_path), "--no-tui", "--json"],
            catch_exceptions=False,
        )

        payload = json.loads(result.output)
        assert payload["decision_counts"]["pending"] == payload["candidate_count"]
        assert all(candidate["decision"] == "pending" for candidate in payload["candidates"])
