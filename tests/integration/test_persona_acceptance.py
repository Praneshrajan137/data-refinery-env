"""BDD-style persona acceptance tests.

These encode the three design personas from the project brief as executable
acceptance criteria, so "the outcomes work" is verified, not asserted:

- Priya (data engineer): `dataforge profile` returns readable output fast.
- Marcus (staff engineer): a fresh user can detect+preview repairs in seconds
  with zero setup, from packaged data.
- Shreya (applied-AI PM): the repo states honest coverage and records decisions.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app
from tests.integration.test_quickstart import repair_count

runner = CliRunner()
_REPO_ROOT = Path(__file__).resolve().parents[2]


class TestPriyaProfilesQuickly:
    """Priya runs profile on a problematic file and expects fast, readable output."""

    def test_profile_is_fast_and_readable(self, tmp_path: Path) -> None:
        csv = tmp_path / "model.csv"
        csv.write_text(
            "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n6,99\n7,101\n8,97\n",
            encoding="utf-8",
        )
        started = time.perf_counter()
        result = runner.invoke(app, ["profile", str(csv)])
        elapsed = time.perf_counter() - started
        assert result.exit_code in (0, 1)  # 0 = clean, 1 = issues found
        assert elapsed < 5.0, "profile must return in under 5 seconds"
        assert result.output.strip(), "profile must produce human-readable output"


class TestMarcusGetsValueInSeconds:
    """Marcus wants pip-install-then-run value with zero setup.

    This class was vacuous until 2026-08-25. It asserted that the quickstart printed the
    words "verified, reversible repair" -- a sentence the command emits whether it repaired
    seven cells or none. When ``type_mismatch`` left the calibration-bypass allowlist the
    demo began reporting **zero** repairs and this test still passed, so the persona's
    stated criterion ("gets value") was never actually verified.

    It now checks the outcome Marcus was promised, end to end: a repair is found, applied,
    audited, and reverted to byte identity. Every step is a command the README tells him to
    run.
    """

    def test_quickstart_delivers_a_repair_not_just_a_sentence(self) -> None:
        started = time.perf_counter()
        result = runner.invoke(app, ["quickstart"])
        elapsed = time.perf_counter() - started
        assert result.exit_code == 0
        assert elapsed < 10.0
        assert repair_count(result.output) >= 1, (
            "the quickstart promised a verified reversible repair and delivered none"
        )

    def test_the_promised_apply_audit_revert_journey_completes(self, tmp_path: Path) -> None:
        """The documented walkthrough, run verbatim, including the txn-id handoff.

        `docs/docs/quickstart.md` tells a user to apply, then audit the transaction id, then
        revert it. Until this test existed, nothing checked that an id was ever produced --
        and for a period it was not, because the fixture's only repair came from a detector
        that had lost its write path. A documented step that cannot complete is worse than a
        missing feature, so the handoff is asserted rather than assumed.
        """
        fixtures = _REPO_ROOT / "dataforge" / "fixtures"
        source = tmp_path / "readings.csv"
        source.write_bytes((fixtures / "premised_fd_10rows.csv").read_bytes())
        schema = fixtures / "premised_fd_10rows.schema.yaml"
        original = source.read_bytes()

        applied = runner.invoke(app, ["repair", str(source), "--schema", str(schema), "--apply"])
        assert applied.exit_code == 0, f"apply failed:\n{applied.output}"
        assert source.read_bytes() != original, "apply reported success but wrote nothing"

        match = re.search(r"(txn-[0-9a-zA-Z._-]+)", applied.output)
        assert match is not None, (
            "apply produced no transaction id, so the documented `audit <txn-id>` and "
            f"`revert <txn-id>` steps cannot be run. Output was:\n{applied.output}"
        )
        txn_id = match.group(1)

        # `--search-root` is required because the transaction journal is written beside the
        # data, not under the process's working directory. The published walkthrough in
        # `docs/docs/quickstart.md` applies to a file in /tmp and then runs a bare
        # `audit <txn-id>`, which only works if the user happens to be sitting in /tmp.
        # Discovered by writing this test; the docs are corrected alongside it.
        audited = runner.invoke(app, ["audit", txn_id, "--search-root", str(tmp_path)])
        assert audited.exit_code == 0, f"audit of {txn_id} failed:\n{audited.output}"

        reverted = runner.invoke(app, ["revert", txn_id, "--search-root", str(tmp_path)])
        assert reverted.exit_code == 0, f"revert of {txn_id} failed:\n{reverted.output}"
        assert source.read_bytes() == original, (
            "revert did not restore byte identity, which is the one guarantee this "
            "product makes unconditionally"
        )


class TestShreyaReadsHonestEvidence:
    """Shreya evaluates the repo's honesty and decision rationale."""

    def test_readme_has_honest_coverage_table(self) -> None:
        readme = (_REPO_ROOT / "README.md").read_text(encoding="utf-8")
        assert "Coverage: what DataForge can and cannot safely fix" in readme
        assert "Detection" in readme and "Correction" in readme

    def test_decisions_log_records_repositioning(self) -> None:
        decisions = (_REPO_ROOT / "DECISIONS.md").read_text(encoding="utf-8")
        assert "verified+calibrated repair" in decisions or "honest coverage" in decisions

    def test_coverage_floors_committed(self) -> None:
        floors = _REPO_ROOT / "eval" / "thresholds" / "coverage_floors.json"
        assert floors.is_file()
