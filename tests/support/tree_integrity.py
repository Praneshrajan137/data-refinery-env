"""Detect any test that leaves the working tree changed, and name it.

This exists because of two incidents with one shape.

The first: four tests in ``test_docs_truth.py`` falsified committed files and restored them in a
``finally``. Correct serially, unsafe in parallel, and invisible either way -- nothing asserted
the tree was clean afterwards.

The second, and the reason this is a guard rather than a comment: a mutation run was killed
before its ``finally`` could restore ``dataforge/engine/repair.py``, leaving the write-safety
allowlist **inverted** in the working tree (``not in`` had become ``in``). It was found only
because someone happened to read ``git status`` before staging. Committing blindly would have
shipped a gate that refuses trusted detectors and permits uncheckable ones.

So the guard is deliberately about the *class* -- "the tree changed during the run" -- rather
than about either instance. It cannot know which mechanism did it, and does not need to.

Implementation note: ``git status --porcelain`` is used rather than hashing 1,976 tracked files.
Git already maintains the stat cache that makes this cheap, and some tracked files in this repo
are tens of megabytes, so hashing them twice per session would cost more than the defect it
protects against. The comparison is *start versus end*, so a working tree that was already dirty
before the run -- the normal state during development -- is not reported as a test's fault.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def snapshot(root: Path | None = None) -> dict[str, str] | None:
    """Return ``{path: status code}`` for every file git considers changed.

    Returns:
        A mapping of repo-relative path to its two-character porcelain status, or ``None`` if
        git is unavailable. ``None`` is distinct from ``{}``: an empty mapping means a clean
        tree, while ``None`` means the guard could not run and must say so rather than pass.
    """
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=root or PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None

    entries: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if len(line) < 4:
            continue
        code, path = line[:2], line[3:].strip().strip('"')
        # A rename is reported as "old -> new"; the destination is what now exists.
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        entries[path.replace("\\", "/")] = code
    return entries


def diff(before: dict[str, str], after: dict[str, str]) -> list[str]:
    """Report every path whose git status appeared or changed between the two snapshots.

    Paths already dirty before the run are ignored unless their status *changed*, so an
    in-progress edit does not masquerade as a test writing the repository.
    """
    problems: list[str] = []
    for path in sorted(set(after) - set(before)):
        problems.append(f"{path}: appeared during the run ({after[path].strip()})")
    for path in sorted(set(after) & set(before)):
        if after[path] != before[path]:
            problems.append(
                f"{path}: status changed {before[path].strip()!r} -> {after[path].strip()!r}"
            )
    return problems


def failure_message(problems: list[str]) -> str:
    """Render a report that says what to do, not merely what happened."""
    listed = "\n".join(f"  - {problem}" for problem in problems)
    return (
        "The test session changed the working tree:\n"
        f"{listed}\n\n"
        "A test must not write into the repository. Two reasons, both of which have already "
        "cost this project real defects:\n"
        "  1. under parallel execution, two workers can each read the original bytes while the "
        "other holds the file modified, so the second restore writes the modified bytes back "
        "permanently;\n"
        "  2. a restore in a 'finally' does not survive a hard kill -- that is how an inverted "
        "write-safety allowlist once survived a mutation run.\n"
        "Write to 'tmp_path' instead, and inject the root the code under test reads. "
        "'tests/support/docs_truth_sandbox.py' is the worked example."
    )
