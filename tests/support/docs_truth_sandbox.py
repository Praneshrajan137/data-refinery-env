"""Build an isolated copy of everything ``docs_truth`` reads, so its tests never write the repo.

``docs_truth.py --check`` validates *every* claim on every run. That is the property that makes
it useful and also the reason its own tests are awkward: to prove a falsified number fails, some
file has to be falsified, and a partial copy of the inputs would fail for the wrong reason
(missing docs) rather than the right one (a contradicted claim).

So the sandbox is derived from the ledger via :func:`docs_truth.claim_paths` rather than listed
by hand. If a claim is added tomorrow that reads a new document, the sandbox picks it up with no
edit here. A hand-maintained list would be a second source of truth that can agree with itself
while disagreeing with the checker.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _docs_truth() -> object:
    """Import the checker, adding the project root to ``sys.path`` first if needed.

    Imported inside the function rather than at module scope so the ``sys.path`` adjustment does
    not have to precede a top-level import, which ruff correctly flags (E402). Suppressing that
    would trade a real lint signal for one line of convenience.
    """
    if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover - import-path plumbing
        sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.ci import docs_truth

    return docs_truth


def build_docs_truth_sandbox(destination: Path) -> Path:
    """Copy every doc and artifact the ledger references into ``destination``.

    Args:
        destination: An empty directory, normally derived from ``tmp_path``.

    Returns:
        ``destination``, ready to pass as ``--root``.

    Raises:
        FileNotFoundError: If the ledger references a path that does not exist. Raised rather
            than skipped, because a silently thinner sandbox would let a test pass while
            exercising less than the real checker does.
    """
    docs_truth = _docs_truth()
    destination.mkdir(parents=True, exist_ok=True)
    for relative in docs_truth.claim_paths():  # type: ignore[attr-defined]
        source = PROJECT_ROOT / relative
        if not source.exists():
            raise FileNotFoundError(
                f"the ledger references {relative}, which does not exist. The sandbox must "
                "contain everything the checker reads, or a test passes against a thinner "
                "population than CI checks."
            )
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return destination
