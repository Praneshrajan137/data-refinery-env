"""The working-tree integrity guard must actually bite.

A guard that cannot fire manufactures confidence, which is worse than having none. The session
fixture in ``tests/conftest.py`` cannot test itself -- by the time it runs, the session is over --
so the decision logic lives in ``tests/support/tree_integrity.py`` and is tested here against
real git state in a throwaway repository.

The cases are chosen from the two incidents that motivated the guard: a test that modifies a
tracked file and restores it late (mutation residue), and a test that leaves a new file behind.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.support import tree_integrity


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A throwaway git repository with one committed file."""
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "guard@example.invalid")
    _git(root, "config", "user.name", "guard")
    (root / "tracked.txt").write_text("original\n", encoding="utf-8")
    _git(root, "add", "tracked.txt")
    _git(root, "commit", "-q", "-m", "seed")
    return root


class TestTheGuardDetectsWhatMotivatedIt:
    def test_a_modified_tracked_file_is_reported(self, repo: Path) -> None:
        """The mutation-residue case: source changed and not restored."""
        before = tree_integrity.snapshot(repo)
        assert before == {}, "the seed repository should start clean"

        (repo / "tracked.txt").write_text("mutated\n", encoding="utf-8")
        after = tree_integrity.snapshot(repo)

        problems = tree_integrity.diff(before or {}, after or {})
        assert problems, "modifying a tracked file was not detected"
        assert any("tracked.txt" in problem for problem in problems)

    def test_a_new_file_left_behind_is_reported(self, repo: Path) -> None:
        before = tree_integrity.snapshot(repo)

        (repo / "leftover.json").write_text("{}", encoding="utf-8")
        after = tree_integrity.snapshot(repo)

        problems = tree_integrity.diff(before or {}, after or {})
        assert any("leftover.json" in problem for problem in problems)

    def test_a_restored_file_is_not_reported(self, repo: Path) -> None:
        """The guard measures the end state, so a test that cleans up correctly passes.

        This is the limit of what it can prove: it catches residue, not the race that produces
        residue. The fix for the race is injecting a root, not detecting it afterwards.
        """
        before = tree_integrity.snapshot(repo)

        target = repo / "tracked.txt"
        original = target.read_bytes()
        target.write_text("mutated\n", encoding="utf-8")
        target.write_bytes(original)
        after = tree_integrity.snapshot(repo)

        assert tree_integrity.diff(before or {}, after or {}) == []


class TestTheGuardDoesNotBlameThePreexistingState:
    def test_a_tree_dirty_before_the_run_is_not_reported(self, repo: Path) -> None:
        """Developing with uncommitted edits must not fail every test session."""
        (repo / "tracked.txt").write_text("work in progress\n", encoding="utf-8")
        before = tree_integrity.snapshot(repo)
        assert before, "the fixture should be dirty before the run"

        after = tree_integrity.snapshot(repo)

        assert tree_integrity.diff(before or {}, after or {}) == []

    def test_a_status_change_on_an_already_dirty_file_is_reported(self, repo: Path) -> None:
        """Being dirty already is not a licence to change further."""
        new_file = repo / "extra.txt"
        new_file.write_text("untracked\n", encoding="utf-8")
        before = tree_integrity.snapshot(repo)
        assert before == {"extra.txt": "??"}

        _git(repo, "add", "extra.txt")
        after = tree_integrity.snapshot(repo)

        problems = tree_integrity.diff(before or {}, after or {})
        assert any("extra.txt" in problem and "status changed" in problem for problem in problems)


class TestUnavailableGitIsDistinctFromCleanTree:
    def test_a_non_repository_yields_none_not_empty(self, tmp_path: Path) -> None:
        """``None`` and ``{}`` must not be conflated, or a broken guard looks like a clean tree."""
        assert tree_integrity.snapshot(tmp_path) is None

    def test_the_failure_message_names_the_remedy(self) -> None:
        message = tree_integrity.failure_message(["dataforge/engine/repair.py: status changed"])

        assert "tmp_path" in message
        assert "docs_truth_sandbox" in message
        assert "dataforge/engine/repair.py" in message
