"""Repairer package for turning detected issues into proposed fixes."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RepairAttempt, Repairer, RetryContext
from dataforge.repairers.decimal_shift import DecimalShiftRepairer
from dataforge.repairers.fd_violation import FDViolationRepairer
from dataforge.repairers.type_mismatch import TypeMismatchRepairer

__all__ = [
    "DecimalShiftRepairer",
    "FDViolationRepairer",
    "ProposedFix",
    "RepairAttempt",
    "Repairer",
    "RetryContext",
    "TypeMismatchRepairer",
    "build_repairers",
    "propose_fixes",
]


def build_repairers(
    *,
    cache_dir: Path | None,
    allow_llm: bool,
    model: str,
) -> dict[str, Repairer]:
    """Construct the default repairer registry."""
    return {
        "type_mismatch": TypeMismatchRepairer(),
        "decimal_shift": DecimalShiftRepairer(),
        "fd_violation": FDViolationRepairer(
            cache_dir=cache_dir,
            allow_llm=allow_llm,
            model=model,
        ),
    }


def propose_fixes(
    issues: list[Issue],
    df: pd.DataFrame,
    schema: Schema | None,
    *,
    cache_dir: Path | None,
    allow_llm: bool = False,
    model: str = "gemini-2.0-flash",
) -> list[ProposedFix]:
    """Run all Week 2 repairers and return proposed fixes.

    Args:
        issues: Detected issues from the detector layer.
        df: The input DataFrame being repaired.
        schema: Optional declared schema.
        cache_dir: Cache directory for any LLM-backed repair decisions.
        allow_llm: Whether fd-violation repair may call the LLM provider.
        model: The provider model name for fd-violation fallback.

    Returns:
        A deduplicated list of proposed fixes.
    """
    registry = build_repairers(
        cache_dir=cache_dir,
        allow_llm=allow_llm,
        model=model,
    )
    proposed: list[ProposedFix] = []
    seen_cells: set[tuple[int, str]] = set()

    for issue in issues:
        repairer = registry.get(issue.issue_type)
        if repairer is None:
            continue
        fix = repairer.propose(issue, df, schema, retry_context=None)
        if fix is None:
            continue
        key = (fix.fix.row, fix.fix.column)
        if key in seen_cells:
            continue
        seen_cells.add(key)
        proposed.append(fix)

    return proposed
