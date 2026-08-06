"""Repairer package for turning detected issues into proposed fixes."""

from __future__ import annotations

from pathlib import Path

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RepairAttempt, Repairer, RetryContext
from dataforge.repairers.categorical_normalization import CategoricalNormalizationRepairer
from dataforge.repairers.decimal_shift import DecimalShiftRepairer
from dataforge.repairers.entity_consensus import EntityConsensusRepairer
from dataforge.repairers.fallback import FallbackRepairer
from dataforge.repairers.fd_violation import FDViolationRepairer
from dataforge.repairers.format_violation import FormatViolationRepairer
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer
from dataforge.repairers.missing_value import MissingValueRepairer
from dataforge.repairers.type_mismatch import TypeMismatchRepairer
from dataforge.table import TableLike

__all__ = [
    "CategoricalNormalizationRepairer",
    "DecimalShiftRepairer",
    "EntityConsensusRepairer",
    "FDViolationRepairer",
    "FallbackRepairer",
    "FormatViolationRepairer",
    "LLMCorrectorRepairer",
    "MissingValueRepairer",
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
    model: str | None = None,
    allow_entity_consensus: bool = False,
    corrector_pool_constrained: bool = False,
    corrector_structured: bool = False,
) -> dict[str, Repairer]:
    """Construct the default repairer registry.

    Deterministic repairers are always registered. When ``allow_llm`` is True,
    the grounded, contract-bound :class:`LLMCorrectorRepairer` is added for the
    correction bottleneck classes that have no deterministic exact-value
    derivation (format_violation, categorical_normalization, outlier) and is
    chained *behind* the deterministic missing_value repairer as a fallback, so
    the precise functional-dependency fill always wins when it applies.

    Corrector proposals carry ``llm_*`` provenance and are still subject to the
    safety constitution, the SMT verifier, and the inferred-constraint guard.
    They are auto-applied only when a calibrated per-class threshold is cleared;
    by default (propose-not-apply) they surface as reviewable suggestions.

    When ``allow_llm`` is False the registry is exactly the four deterministic
    repairers, keeping deterministic runs byte-identical. ``format_violation``
    and ``categorical_normalization`` deterministic repairers remain withheld
    from auto-apply (they regressed benchmark precision); the corrector replaces
    them as the calibrated, gated correction path for those classes.
    """
    registry: dict[str, Repairer] = {
        "type_mismatch": TypeMismatchRepairer(),
        "decimal_shift": DecimalShiftRepairer(),
        "fd_violation": FDViolationRepairer(
            cache_dir=cache_dir,
            allow_llm=allow_llm,
            model=model,
        ),
        "missing_value": MissingValueRepairer(),
    }
    if allow_entity_consensus:
        # Cross-row entity consensus: proposes the sibling-row consensus value.
        # Classified plausibility_only by the engine (evidence-strong, not proof),
        # so held for review by default and auto-applied only under the
        # allow_unproven_autoapply opt-in (or when a declared schema proves it).
        # OFF by default so the deterministic baseline stays byte-identical (a
        # noisy consensus proposal would otherwise regress the locked hospital
        # anchor by adding a false positive).
        registry["entity_consensus"] = EntityConsensusRepairer()
    if allow_llm:
        corrector = LLMCorrectorRepairer(
            cache_dir=cache_dir,
            allow_llm=True,
            model=model,
            pool_constrained=corrector_pool_constrained,
            structured=corrector_structured,
        )
        registry["missing_value"] = FallbackRepairer(MissingValueRepairer(), corrector)
        registry["format_violation"] = corrector
        registry["categorical_normalization"] = corrector
        registry["outlier"] = corrector
    return registry


def propose_fixes(
    issues: list[Issue],
    df: TableLike,
    schema: Schema | None,
    *,
    cache_dir: Path | None,
    allow_llm: bool = False,
    model: str | None = None,
    allow_entity_consensus: bool = False,
    corrector_pool_constrained: bool = False,
    corrector_structured: bool = False,
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
        allow_entity_consensus=allow_entity_consensus,
        corrector_pool_constrained=corrector_pool_constrained,
        corrector_structured=corrector_structured,
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
