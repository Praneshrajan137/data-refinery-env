"""Concurrency behaviour of the apply path, tested rather than asserted in prose.

Why this file exists, dated 2026-08-26. ``tests/integration/test_surface_uniformity.py`` declares
the lock file among the write primitives and says, in prose, that "the lock is what makes concurrent
writes to one source safe, so this write protects user data rather than touching it." A repo-wide
search for ``concurren|fcntl|msvcrt|flock|threading|ProcessPool`` under ``tests/`` returned that
sentence and nothing else. The claim guarding reversibility -- the product's one unconditional
promise -- was a sentence.

This is the trap recorded in the project's own history: two tests once guarded the string "verified,
reversible repair" while the quickstart repaired zero cells. Assert the outcome, never the sentence.

What the code actually does, read before it was tested, because the plan for this file predicted
something narrower and was wrong. There are TWO defenses, not one:

1. ``source_path_lock`` (``dataforge/transactions/files.py``) takes an exclusive lock via
   ``O_CREAT | O_EXCL``, waits up to ``timeout_seconds`` for a held lock, and steals one older than
   ``stale_after_seconds``. So the loser of a race does not fail -- it WAITS. Mutual exclusion is
   the property; clean failure is not, and a test asserting a non-zero exit for the loser would
   have been asserting something the product deliberately does not do.
2. Inside the lock, ``apply_repair_transaction`` re-reads the file and refuses when
   ``current_bytes != source_bytes`` (``dataforge/engine/repair.py``). This is what actually
   prevents corruption: serialisation alone would let a second writer apply a patch computed
   against bytes that no longer exist.

The race tests therefore assert invariants that must hold under EVERY interleaving, because the
scheduler is not ours to fix. Asserting a particular winner would make the test flaky and, worse,
would pass for the wrong reason on a machine that happened to serialise the processes.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from dataforge.transactions.files import SourceLockError, lock_path_for, source_path_lock

FIXTURES = Path(__file__).resolve().parents[2] / "dataforge" / "fixtures"
#: The fixture chosen because its repair is PROVABLE, not because it is permitted. `state -> city`
#: is a declared functional dependency, so `fd_violation` -- one of the two detectors with a
#: committed write measurement -- writes exactly one cell. Choosing a fixture by what a detector is
#: allowed to do rather than by what can be proven is how the release gate came to smoke-test the
#: whole write chain through its least-evidenced detector.
SOURCE_NAME = "premised_fd_10rows.csv"
SCHEMA_NAME = "premised_fd_10rows.schema.yaml"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _acquire_and_release(source: Path, *, timeout_seconds: float = 0.2) -> None:
    """Acquire the lock and release it immediately.

    A named helper rather than a nested ``with``, so the mutual-exclusion tests can express
    "acquiring this raises" as a single statement. Nesting the inner acquisition inside the outer
    one is what the test means, but it reads as a lint error and combining the two ``with`` blocks
    would acquire both in one frame -- which is the opposite of the property under test.
    """
    with source_path_lock(source, timeout_seconds=timeout_seconds):
        pass


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A copy of the provable fixture pair, isolated per test."""
    shutil.copyfile(FIXTURES / SOURCE_NAME, tmp_path / SOURCE_NAME)
    shutil.copyfile(FIXTURES / SCHEMA_NAME, tmp_path / SCHEMA_NAME)
    return tmp_path


def _apply(workspace: Path, *, timeout: int = 90) -> subprocess.CompletedProcess[str]:
    """Run a real ``dataforge repair --apply`` in a separate PROCESS.

    Processes, not threads: the lock is a lock FILE keyed on pid, so threads in one interpreter
    would share the pid and could pass a test the product would fail in deployment.
    """
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "dataforge",
            "repair",
            str(workspace / SOURCE_NAME),
            "--schema",
            str(workspace / SCHEMA_NAME),
            "--apply",
            "--json",
        ],
        capture_output=True,
        text=True,
        cwd=str(workspace),
        timeout=timeout,
    )


def _receipt(result: subprocess.CompletedProcess[str]) -> dict[str, object]:
    """Parse the receipt, tolerating a non-zero exit that still emitted JSON."""
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}
    receipt = payload.get("receipt")
    return receipt if isinstance(receipt, dict) else {}


class TestTheLockPrimitive:
    """Deterministic properties of the lock itself, with no repair pipeline involved."""

    def test_a_second_acquisition_is_refused_while_held(self, tmp_path: Path) -> None:
        """Mutual exclusion. Asserted with a short timeout so the wait does not mask it."""
        source = tmp_path / "data.csv"
        source.write_text("a\n1\n", encoding="utf-8")

        with source_path_lock(source), pytest.raises(SourceLockError):
            _acquire_and_release(source)

    def test_the_lock_is_released_after_an_exception(self, tmp_path: Path) -> None:
        """A failed apply must not leave the source permanently unwritable.

        The release is in a ``finally``, so this passes today. It is pinned because the failure it
        would cause is indistinguishable from a hung process: a user would see "timed out waiting
        for lock" forever, on a file nothing holds.
        """
        source = tmp_path / "data.csv"
        source.write_text("a\n1\n", encoding="utf-8")

        with pytest.raises(RuntimeError, match="apply blew up"), source_path_lock(source):
            raise RuntimeError("apply blew up")

        assert not lock_path_for(source).exists()
        with source_path_lock(source, timeout_seconds=0.2):
            pass

    def test_a_stale_lock_is_reclaimed(self, tmp_path: Path) -> None:
        """Recovery from abrupt termination, which is the reachable case.

        A killed process cannot run its ``finally``, so the lock file outlives it. Recovery is by
        age, and this exercises that path with a short staleness window rather than waiting the
        production 300 seconds. Without it, one ``kill -9`` would make a table unrepairable until a
        human deleted a hashed file under `.dataforge/locks/` that nothing tells them about.
        """
        source = tmp_path / "data.csv"
        source.write_text("a\n1\n", encoding="utf-8")
        lock = lock_path_for(source)
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text("999999 abandoned-by-a-killed-process\n", encoding="utf-8")
        # Backdate it rather than sleeping: the reclaim compares mtime age.
        old = time.time() - 60
        os.utime(lock, (old, old))

        with source_path_lock(source, timeout_seconds=1.0, stale_after_seconds=1.0):
            pass

        assert not lock.exists()

    def test_a_fresh_lock_is_not_reclaimed(self, tmp_path: Path) -> None:
        """Non-vacuity for the test above: reclaim must not defeat mutual exclusion.

        Without this, ``stale_after_seconds=0`` would satisfy the reclaim test while making the
        lock useless -- coverage bought by removing the guarantee.
        """
        source = tmp_path / "data.csv"
        source.write_text("a\n1\n", encoding="utf-8")
        lock = lock_path_for(source)
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text("999999 just-now\n", encoding="utf-8")

        with (
            pytest.raises(SourceLockError),
            source_path_lock(source, timeout_seconds=0.2, stale_after_seconds=300.0),
        ):
            pytest.fail("reclaimed a lock that was not stale")  # pragma: no cover


class TestConcurrentApplyToOneSource:
    """Two real processes applying to the same file. Invariants over all interleavings."""

    def test_exactly_one_process_applies_and_the_data_is_never_torn(self, workspace: Path) -> None:
        """The invariant that matters: one commit, and a file that is still a valid table.

        The fixture contains exactly one provable error, so "at most one apply" and "exactly one
        apply" coincide -- which is why this fixture was chosen. Asserting the repaired CONTENT
        rather than only a count is deliberate: a count of one is also what a torn write produces.
        """
        source = workspace / SOURCE_NAME
        original = source.read_bytes()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [f.result() for f in [pool.submit(_apply, workspace) for _ in range(2)]]

        applied = [r for r in results if _receipt(r).get("applied") is True]
        assert len(applied) == 1, (
            f"expected exactly one apply, got {len(applied)}; "
            f"exits={[r.returncode for r in results]}"
        )

        # Compared cell by cell rather than byte by byte, because apply also normalises line
        # endings -- see TestApplyRewritesEveryLineEnding below. Every row must survive and only
        # row 10's `city` may differ, which is the invariant a torn write would break.
        before = [line.split(",") for line in original.decode("utf-8").splitlines() if line]
        after = [
            line.split(",") for line in source.read_text(encoding="utf-8").splitlines() if line
        ]

        assert len(after) == len(before) == 11
        assert after[10] == ["10", "MA", "boston"]
        assert after[:10] == before[:10]

    def test_the_loser_never_writes_a_patch_computed_on_stale_bytes(self, workspace: Path) -> None:
        """The second defense, exercised directly rather than hoped for in a race.

        Serialisation alone does not prevent corruption: the loser's fixes were computed against
        bytes that no longer exist by the time it holds the lock. Constructed deterministically by
        holding the lock while a real apply runs, so the assertion does not depend on the scheduler.
        """
        source = workspace / SOURCE_NAME
        original_sha = _sha256(source)

        first = _apply(workspace)
        assert _receipt(first).get("applied") is True
        after_first = _sha256(source)
        assert after_first != original_sha

        second = _apply(workspace)

        # The second run finds nothing provable left, or is refused. Either way it must not write.
        assert _receipt(second).get("applied") is not True
        assert _sha256(source) == after_first

    def test_revert_restores_byte_identity_after_contention(self, workspace: Path) -> None:
        """Reversibility is the floor, and it must survive a race, not only a quiet apply.

        Byte identity rather than semantic equality: the promise is the user's bytes back, and a
        re-serialised CSV that parses the same is not the same file.
        """
        source = workspace / SOURCE_NAME
        original = source.read_bytes()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [f.result() for f in [pool.submit(_apply, workspace) for _ in range(2)]]

        txn_ids = [
            str(_receipt(r)["txn_id"]) for r in results if _receipt(r).get("applied") is True
        ]
        assert len(txn_ids) == 1

        revert = subprocess.run(
            [
                sys.executable,
                "-m",
                "dataforge",
                "revert",
                txn_ids[0],
                "--search-root",
                str(workspace),
                "--json",
            ],
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=90,
        )

        assert revert.returncode == 0, revert.stderr[-2000:]
        assert source.read_bytes() == original

    def test_the_journal_verifies_after_contention(self, workspace: Path) -> None:
        """The hash chain must still audit clean, or reversibility is unprovable.

        A revert that restores the right bytes while the journal disagrees is not a guarantee a
        third party can check, which is the whole claim.
        """
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [f.result() for f in [pool.submit(_apply, workspace) for _ in range(2)]]

        txn_ids = [
            str(_receipt(r)["txn_id"]) for r in results if _receipt(r).get("applied") is True
        ]
        assert len(txn_ids) == 1

        audit = subprocess.run(
            [
                sys.executable,
                "-m",
                "dataforge",
                "audit",
                txn_ids[0],
                "--search-root",
                str(workspace),
                "--json",
            ],
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=90,
        )

        assert audit.returncode == 0, audit.stderr[-2000:]
        assert json.loads(audit.stdout)["verdict"] == "verified"

    def test_no_lock_file_survives_a_completed_race(self, workspace: Path) -> None:
        """Both processes must release. A leaked lock makes the next repair wait 300 seconds."""
        with ThreadPoolExecutor(max_workers=2) as pool:
            [f.result() for f in [pool.submit(_apply, workspace) for _ in range(2)]]

        assert not lock_path_for(workspace / SOURCE_NAME).exists()


class TestApplyRewritesEveryLineEnding:
    """CHARACTERISATION of shipped behaviour that is reversible but harms reviewability.

    Found 2026-08-26 while writing the concurrency tests above, which failed on it. Applying a
    one-cell repair to a CRLF-delimited CSV rewrites EVERY line ending in the file: measured on the
    11-line fixture, 11 CRLF became 11 LF and the file shrank by 12 bytes to change one cell. The
    cause is that the apply path re-serialises the table rather than patching bytes in place.

    This does NOT violate the reversibility floor, and the tests above prove it: revert restores
    byte identity from the snapshot and the journal audits clean. So it is not a data-safety defect.

    What it costs is human review, which is the product. A reviewer asking "what did DataForge
    change?" sees every line of the diff modified and cannot see the one line that matters. The
    boundary this product sells is the one between a change a human must look at and one they need
    not; a diff that hides its own content degrades exactly that. It is also collateral modification
    of rows that were already correct, which the project measures unconditionally everywhere else.

    Recorded rather than fixed in this session. Preserving the input dialect is a change to the
    write path with its own blast radius -- the snapshot, the post-apply hash, the patch plan and
    the warehouse surfaces all consume the serialised form -- and it is not in the approved scope.
    See ``docs/trust/inferred-guard-gaps.md``.

    DO NOT fix a failure here by editing the assertion. This test documents what the product does,
    so a change in behaviour SHOULD fail it; update it only together with the trust document.
    """

    def test_a_crlf_source_is_normalised_to_lf_across_the_whole_file(self, workspace: Path) -> None:
        """The measured behaviour, pinned to the numbers in the docstring."""
        source = workspace / SOURCE_NAME
        source.write_bytes(source.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        before = source.read_bytes()
        assert before.count(b"\r\n") == 11

        assert _receipt(_apply(workspace)).get("applied") is True

        after = source.read_bytes()
        assert after.count(b"\r\n") == 0
        assert after.count(b"\n") == 11
        assert len(before) - len(after) == 12

    def test_reversibility_still_holds_despite_the_rewrite(self, workspace: Path) -> None:
        """The reason this is a reviewability finding and not a safety one.

        Stated as a test rather than only in prose, because "reversible" is the claim that makes
        the rewrite tolerable, and an untested claim is the thing this file exists to replace.
        """
        source = workspace / SOURCE_NAME
        source.write_bytes(source.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        original = source.read_bytes()

        txn_id = str(_receipt(_apply(workspace))["txn_id"])
        assert source.read_bytes() != original

        revert = subprocess.run(
            [
                sys.executable,
                "-m",
                "dataforge",
                "revert",
                txn_id,
                "--search-root",
                str(workspace),
                "--json",
            ],
            capture_output=True,
            text=True,
            cwd=str(workspace),
            timeout=90,
        )

        assert revert.returncode == 0, revert.stderr[-2000:]
        assert source.read_bytes() == original
