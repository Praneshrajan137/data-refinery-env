"""The mutation harness must not be able to report a clean sweep it did not earn.

This harness is the only instrument that proves the auto-apply guards are pinned by tests,
and it had two defects that made its headline number unfalsifiable.

1. It hardcoded ``.venv/Scripts/python.exe``, a Windows-only path. CI creates no venv and runs
   on Linux, so the harness raised ``FileNotFoundError`` on the first mutant and had **never
   once executed there**. It failed closed, so no guard was silently unpinned -- but "18/18
   killed" was a number produced only on one machine.

2. It scored a mutant ``KILLED`` on **any** non-zero pytest exit. A collection error, an
   import error, or a missing dependency therefore read as "a test noticed the mutation".
   This is not hypothetical: an earlier version invoked a bare ``python``, every mutant died
   of ``ModuleNotFoundError: textual``, and the run was briefly recorded as a clean sweep.
   Pinning the venv path fixed that *incidentally*; it left the instrument still unable to
   distinguish "guard unpinned" from "pytest broken".

The tests below pin the distinction itself, and the baseline refusal that makes a kill
verdict mean something. Asserting the file changed on disk (already in ``run``) catches a
mutant that was never applied; only a green baseline catches a suite that was already red.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.ci import mutate_autoapply_guards as harness

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestKillRuleDistinguishesFailureFromBreakage:
    def test_only_a_test_failure_counts_as_a_kill(self) -> None:
        assert harness._verdict_for(1) == "KILLED"

    def test_a_green_subset_means_the_mutant_survived(self) -> None:
        assert harness._verdict_for(0) == "SURVIVED"

    @pytest.mark.parametrize(
        ("returncode", "meaning"),
        [
            (2, "interrupted"),
            (3, "internal error"),
            (4, "usage error"),
            (5, "no tests collected"),
        ],
    )
    def test_harness_breakage_is_never_scored_as_a_kill(
        self, returncode: int, meaning: str
    ) -> None:
        """The regression that let a ModuleNotFoundError read as a dead mutant.

        ``no tests collected`` (5) is the sharpest case: a mutant whose test paths no longer
        exist would otherwise be reported as pinned by the very tests that vanished.
        """
        verdict = harness._verdict_for(returncode)
        assert verdict != "KILLED", f"pytest exit {returncode} ({meaning}) must not read as a kill"
        assert "HARNESS_ERROR" in verdict
        assert str(returncode) in verdict, "the verdict must name the exit code it refused"


class TestInterpreterResolutionIsPortable:
    @staticmethod
    def _venv_with_both_layouts(root: Path) -> None:
        """Create both a POSIX and a Windows venv layout so selection is what is measured."""
        (root / ".venv" / "bin").mkdir(parents=True)
        (root / ".venv" / "bin" / "python").write_text("", encoding="utf-8")
        (root / ".venv" / "Scripts").mkdir(parents=True)
        (root / ".venv" / "Scripts" / "python.exe").write_text("", encoding="utf-8")

    def test_posix_selects_the_posix_layout(self, monkeypatch, tmp_path) -> None:
        """The exact defect: a Windows interpreter path selected on a Linux runner.

        Both layouts exist here, so this measures the choice rather than the fallback.
        Compared as strings because patching ``os.name`` also changes which concrete
        ``Path`` flavour is constructed, making equality fail on identical text.
        """
        self._venv_with_both_layouts(tmp_path)
        monkeypatch.setattr(harness, "REPO", tmp_path)
        monkeypatch.setattr(harness.os, "name", "posix")
        monkeypatch.delenv("DATAFORGE_MUTATE_PYTHON", raising=False)

        resolved = harness._resolve_python().replace("\\", "/")

        assert resolved.endswith(".venv/bin/python")
        assert "Scripts" not in resolved
        assert resolved != sys.executable, "must select the venv, not fall back"

    def test_windows_selects_the_windows_layout(self, monkeypatch, tmp_path) -> None:
        self._venv_with_both_layouts(tmp_path)
        monkeypatch.setattr(harness, "REPO", tmp_path)
        monkeypatch.setattr(harness.os, "name", "nt")
        monkeypatch.delenv("DATAFORGE_MUTATE_PYTHON", raising=False)

        resolved = harness._resolve_python().replace("\\", "/")

        assert resolved.endswith(".venv/Scripts/python.exe")

    def test_a_missing_venv_degrades_to_the_running_interpreter(
        self, monkeypatch, tmp_path
    ) -> None:
        """Absent venv must fall back, not crash. This is what CI hits."""
        monkeypatch.setattr(harness, "REPO", tmp_path)
        monkeypatch.delenv("DATAFORGE_MUTATE_PYTHON", raising=False)
        assert harness._resolve_python() == sys.executable

    def test_an_override_wins(self, monkeypatch) -> None:
        monkeypatch.setenv("DATAFORGE_MUTATE_PYTHON", "/opt/python")
        assert harness._resolve_python() == "/opt/python"

    def test_the_resolved_interpreter_exists(self) -> None:
        """Whatever was chosen on this machine must be runnable."""
        assert Path(harness.PY).exists() or sys.executable == harness.PY


class TestBaselineRefusal:
    def test_a_red_baseline_refuses_to_score_any_mutant(self, monkeypatch, capsys) -> None:
        """A red suite must produce a refusal, not a sweep.

        Without this, every mutant "fails" its subset for the pre-existing reason and the
        harness reports success while proving nothing.
        """
        monkeypatch.setattr(harness, "baseline_verdict", lambda: (False, "pytest exit 3: boom"))

        def _explode(mutant: harness.Mutant) -> str:
            raise AssertionError(f"no mutant may be scored on a red baseline (got {mutant.name})")

        monkeypatch.setattr(harness, "run", _explode)

        assert harness.main() == 1
        assert "REFUSING TO SCORE MUTANTS" in capsys.readouterr().out

    def test_the_baseline_covers_every_mutants_tests(self) -> None:
        """The baseline is only sufficient if it spans the union of subsets."""
        union = {test for mutant in harness.MUTANTS for test in mutant.tests}
        assert union, "the mutant population declares no tests"
        for test in union:
            target = PROJECT_ROOT / test
            assert target.exists(), f"mutant test path does not exist: {test}"


class TestPopulationIsNonVacuous:
    def test_there_are_mutants_to_score(self) -> None:
        assert len(harness.MUTANTS) >= 18

    def test_every_mutant_targets_a_real_file_and_states_why(self) -> None:
        for mutant in harness.MUTANTS:
            assert (PROJECT_ROOT / mutant.target).exists(), f"{mutant.name}: {mutant.target}"
            assert mutant.why.strip(), f"{mutant.name} has no rationale"
            assert mutant.old != mutant.new, f"{mutant.name} is a no-op mutation"


class TestTheHarnessRunsAsAScript:
    def test_module_imports_under_the_running_interpreter(self) -> None:
        """Guards against a syntax or import error reaching CI as a mutation failure."""
        proc = subprocess.run(
            [sys.executable, "-c", "import scripts.ci.mutate_autoapply_guards"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, proc.stderr
