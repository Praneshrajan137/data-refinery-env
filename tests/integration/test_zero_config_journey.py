"""The zero-configuration journey, end to end, through the commands the docs publish.

Why this file exists, dated 2026-08-26. `docs/trust/deductive-coverage-result.md` describes the
three-step journey by which a **mined** dependency reaches the repairer:

    dataforge profile <csv> --constraints-out <artifact.json>
    dataforge constraints review <artifact.json> --accept <cnd-id>
    dataforge repair <csv> --constraints <artifact.json>

and this is the product's only path from a table with **no declared schema** to an unsupervised
write. Nothing tested it end to end. The nearest test,
`tests/unit/test_cli_constraints.py::test_constraints_review_acceptance_feeds_repair`, is
`--dry-run` and starts from an in-process artifact rather than from `profile`. So the journey that
mutates a user's bytes had no test that mutated any.

The distinction matters because it is the *whole* difference between the two write paths the product
has. `premised_fd_10rows.csv` exercises a **declared** dependency supplied by an operator; that
fixture cannot exercise this journey at all, because its `state` column is constant and the miner
rejects a constant determinant. Discovering that is what showed the two paths need two fixtures.

What is asserted here is the outcome, not the sentence: a repair count, the mined provenance chain,
`audit` returning verified, and byte identity after revert.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[2] / "dataforge" / "fixtures"
#: 25 rows, no schema, one provable violation. `state -> city` is MINED at confidence 0.96 rather
#: than declared, which is what makes this the zero-config journey. Row 25 carries `albny` against a
#: 12-row `albany` majority, so exactly one cell is repairable and the repair is derivable.
SOURCE_NAME = "mined_fd_25rows.csv"


def _run(workspace: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Invoke the real CLI in a subprocess, as a user would."""
    return subprocess.run(
        [sys.executable, "-m", "dataforge", *args],
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=120,
    )


def _json(result: subprocess.CompletedProcess[str]) -> dict:
    """Parse stdout, requiring success.

    ``profile`` writes a queue-cost warning to **stderr** before the JSON, so the streams must stay
    separated; a combined capture would make the payload unparseable.
    """
    assert result.returncode == 0, f"command failed:\n{result.stderr[-3000:]}"
    return json.loads(result.stdout)


def _json_any_exit(result: subprocess.CompletedProcess[str]) -> dict:
    """Parse stdout without requiring success.

    ``repair --apply`` exits **non-zero when it abstains**, which is correct -- an unrepaired issue
    is a finding a caller should notice -- but it means the abstention cases cannot use
    :func:`_json`. Discovered by this test failing; the exit code is asserted where it matters
    rather than assumed away.
    """
    return json.loads(result.stdout)


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    shutil.copyfile(FIXTURES / SOURCE_NAME, tmp_path / SOURCE_NAME)
    return tmp_path


class TestTheZeroConfigJourneyWrites:
    """profile -> review --accept -> repair --apply -> audit -> revert, with no schema anywhere."""

    def test_the_published_journey_repairs_reverts_and_verifies(self, workspace: Path) -> None:
        """One test for the whole chain, because the chain is the claim.

        Split across several tests each step would pass while the composition failed, which is how
        a journey documented in three commands came to have no test that ran all three.
        """
        source = workspace / SOURCE_NAME
        original = source.read_bytes()
        original_sha = hashlib.sha256(original).hexdigest()

        # 1. profile, with NO --schema. This is the whole point: the premise is mined, not declared.
        profile = _json(
            _run(
                workspace,
                "profile",
                str(source),
                "--constraints-out",
                str(workspace / "constraints.json"),
                "--json",
            )
        )
        assert profile["schema_inference"]["row_count"] == 25

        # 2. review. The FD must be present and PENDING -- mining something is not accepting it.
        review = _json(
            _run(
                workspace,
                "constraints",
                "review",
                str(workspace / "constraints.json"),
                "--no-tui",
                "--json",
            )
        )
        fds = [c for c in review["candidates"] if c["kind"] == "functional_dependency"]
        target = next(c for c in fds if c["target"] == "state -> city")
        assert target["decision"] == "pending", "a mined dependency must not be accepted by default"
        assert target["tested_confidence"] is not None, "the honest number must reach the reviewer"

        # 3. accept exactly one dependency, then repair with --apply.
        _json(
            _run(
                workspace,
                "constraints",
                "review",
                str(workspace / "constraints.json"),
                "--accept",
                target["candidate_id"],
                "--no-tui",
                "--json",
            )
        )
        applied = _json(
            _run(
                workspace,
                "repair",
                str(source),
                "--constraints",
                str(workspace / "constraints.json"),
                "--apply",
                "--json",
            )
        )
        receipt = applied["receipt"]

        # Assert the NUMBER and the chain, never the sentence.
        assert receipt["applied"] is True
        assert receipt["fixes_count"] == 1, "the journey must actually repair something"
        assert receipt["accepted_constraint_ids"] == [target["candidate_id"]]
        fix = receipt["applied_fixes"][0]
        assert (fix["row"], fix["column"]) == (24, "city")
        assert (fix["old_value"], fix["new_value"]) == ("albny", "albany")
        assert fix["detector_id"] == "fd_violation"
        assert fix["verification_strength"] == "proven"
        assert fix["provenance"] == "deterministic"
        assert source.read_bytes() != original, "apply reported success but changed nothing"

        txn_id = str(receipt["txn_id"])

        # 4. audit must verify the journal for a write authorised by a MINED premise.
        audit = _json(_run(workspace, "audit", txn_id, "--search-root", str(workspace), "--json"))
        assert audit["verdict"] == "verified"

        # 5. revert must restore the user's exact bytes. Reversibility is the floor.
        _json(_run(workspace, "revert", txn_id, "--search-root", str(workspace), "--json"))
        assert hashlib.sha256(source.read_bytes()).hexdigest() == original_sha
        assert source.read_bytes() == original

        # 6. and the journal must still verify after the revert.
        reverted = _json(
            _run(workspace, "audit", txn_id, "--search-root", str(workspace), "--json")
        )
        assert reverted["verdict"] == "verified"

    def test_without_acceptance_the_journey_writes_nothing(self, workspace: Path) -> None:
        """Non-vacuity, and the safety property. Mining is not consent.

        If this passed while the test above also passed, acceptance would be decorative. Asserting
        `fixes_count == 0` rather than only a clean exit is deliberate: a run that abstains and a run
        that writes both exit 0.
        """
        source = workspace / SOURCE_NAME
        original = source.read_bytes()
        _json(
            _run(
                workspace,
                "profile",
                str(source),
                "--constraints-out",
                str(workspace / "constraints.json"),
                "--json",
            )
        )

        applied = _json_any_exit(
            _run(
                workspace,
                "repair",
                str(source),
                "--constraints",
                str(workspace / "constraints.json"),
                "--apply",
                "--json",
            )
        )

        assert applied["receipt"]["fixes_count"] == 0
        assert applied["receipt"]["accepted_constraint_ids"] == []
        assert source.read_bytes() == original

    def test_without_the_artifact_the_journey_writes_nothing(self, workspace: Path) -> None:
        """The default zero-config run. Inference alone never repairs.

        `infer_verification_schema` feeds the SMT guard only, so a bare `repair --apply` on an
        unschema'd table must abstain. Pinned because "zero-config does nothing by default" is a
        load-bearing claim about this product, and it would be easy to break by widening a default.
        """
        source = workspace / SOURCE_NAME
        original = source.read_bytes()

        applied = _json_any_exit(_run(workspace, "repair", str(source), "--apply", "--json"))

        assert applied["receipt"]["fixes_count"] == 0
        assert source.read_bytes() == original
