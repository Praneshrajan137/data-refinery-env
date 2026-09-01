"""CI wrapper for the full original DataForge vision external gate.

**Relocated out of ``scripts/ci/`` on 2026-09-01, and the move is the point.** This file
lived in ``scripts/ci/`` and appeared in exactly two places repo-wide -- the ``mypy``
argument lists in ``Makefile`` and ``scripts/ci/backend_gate.py`` -- so it was
type-checked and never executed. That is the fourth instance of the orphaned-gate defect
recorded at ``scripts/ci/backend_gate.py``: *"they appeared only in the Makefile's mypy
argument list, so they were type-checked and never executed."*

The previous three were fixed by wiring them into the gate. This one must NOT be, and the
reason matters more than the file does. The gate it wraps checks live external state --
PyPI publication, a Workers playground, an HF Space backend, an HF model family -- and
requires a ``design_partners/manifest.json`` that does not exist, so by its own committed
evidence it CANNOT pass. Running it per pull request would make ``main`` permanently red
for a correct reason, and ``docs/quantitative_claims.yaml`` already records what that
costs: a gate that fails for no defect *"is how a gate earns being ignored"*.

So it is not a continuous-integration gate. It is a release-readiness reporter whose
NOT MET output is the honest current answer, and it now lives under ``scripts/release/``
where that is legible. The user-facing entry point is unchanged: ``dataforge release
full-vision``.
"""

from __future__ import annotations

import sys

from dataforge.release.full_vision import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
