"""Build the DataForge Selective-Repair Calibration Benchmark artifact.

This turns raw LLM-corrector benchmark runs into a single, reproducible
calibration artifact. The expensive step (collecting real ``(confidence,
was_correct)`` samples) happens in the benchmark; everything here is free
post-processing on those persisted samples:

- Distribution-free certified auto-apply coverage (conformal risk control) at a
  primary alpha and across an alpha sweep.
- The selective-classification risk-coverage curve and its AURC summary
  (Geifman & El-Yaniv, 2017).
- Real-data K-split validity of the guarantee.
- Reliability-diagram data and ECE (Guo et al., 2017).

Conditions (e.g. reasoning-effort levels or models) are kept separate: samples
are pooled only *within* a condition, never across, so the comparison is honest.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from dataforge.bench.error_classes import expected_calibration_error
from dataforge.conformal import (
    LabeledSample,
    area_under_risk_coverage,
    certified_coverage_report,
    reliability_curve,
    repeated_split_certification,
    risk_coverage_curve,
)

_ARTIFACT_SCHEMA = "dataforge_selective_repair_calibration_v1"
_PURPOSE = (
    "Distribution-free certified auto-apply coverage for the LLM corrector, framed "
    "as selective classification. Thresholds are certified on a calibration split "
    "(conformal risk control) and measured on a disjoint test split - never an "
    "in-sample number - and summarized by a risk-coverage curve (AURC), an alpha "
    "sweep, real-data split validity, and a reliability diagram."
)


def _samples_by_class(record: Mapping[str, object]) -> dict[str, list[LabeledSample]]:
    """Extract per-class (confidence, was_correct) pairs from a bench record."""
    raw = record.get("calibration_samples_by_class") or {}
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, list[LabeledSample]] = {}
    for error_class, pairs in raw.items():
        out[str(error_class)] = [(float(conf), bool(correct)) for conf, correct in pairs]
    return out


def _promotion_verdict(
    *, precision_at_auto_apply: float | None, ece: float | None, auto_apply_count: int | None
) -> dict[str, object]:
    """Inline corrector promotion verdict (>= 0.95 precision, <= 0.1 ECE, >= 1 applied)."""
    reasons: list[str] = []
    if precision_at_auto_apply is None or auto_apply_count is None:
        return {"promoted": False, "reasons": ["no auto-apply measurement"]}
    if auto_apply_count < 1:
        reasons.append("no high-agreement proposals")
    if precision_at_auto_apply < 0.95:
        reasons.append(f"precision_at_auto_apply {precision_at_auto_apply} < 0.95")
    if ece is not None and ece > 0.1:
        reasons.append(f"ECE {ece} > 0.1 (confidence poorly calibrated)")
    return {"promoted": not reasons, "reasons": reasons}


def build_calibration_artifact(
    records_by_condition: Mapping[str, Mapping[str, object]],
    *,
    alphas: Sequence[float] = (0.01, 0.02, 0.05, 0.1, 0.2),
    delta: float = 0.05,
    min_support: int = 30,
    splits: int = 200,
    primary_alpha: float = 0.05,
) -> dict[str, object]:
    """Assemble the calibration artifact from one or more corrector bench records.

    Args:
        records_by_condition: ``{condition_label: bench_record_dict}``; each record
            carries ``calibration_samples_by_class`` and aggregate metrics.
        alphas: Error budgets for the coverage sweep.
        delta: Guarantee failure probability.
        min_support: Minimum accepted count to certify a class.
        splits: Random splits for the real-data validity check.
        primary_alpha: The headline error budget.

    Returns:
        A JSON-serializable artifact with a per-condition breakdown and an overall
        conclusion.
    """
    conditions: dict[str, object] = {}
    any_certified = False
    for label, record in records_by_condition.items():
        by_class = _samples_by_class(record)
        pooled: list[LabeledSample] = [s for pairs in by_class.values() for s in pairs]

        primary_report = certified_coverage_report(
            by_class, alpha=primary_alpha, delta=delta, min_support=min_support
        )
        primary_cov = primary_report["overall_test_coverage"]
        if isinstance(primary_cov, (int, float)) and primary_cov > 0.0:
            any_certified = True

        sweep = [
            {
                "alpha": alpha,
                "overall_test_coverage": certified_coverage_report(
                    by_class, alpha=alpha, delta=delta, min_support=min_support
                )["overall_test_coverage"],
                "overall_test_error": certified_coverage_report(
                    by_class, alpha=alpha, delta=delta, min_support=min_support
                )["overall_test_error"],
            }
            for alpha in alphas
        ]
        curve = risk_coverage_curve(pooled)
        conditions[label] = {
            "provider": record.get("provider"),
            "model": record.get("model"),
            "dataset": record.get("dataset"),
            "aggregate": {
                "precision": record.get("precision"),
                "recall": record.get("recall"),
                "f1": record.get("f1"),
                "ece": record.get("ece"),
                "precision_at_auto_apply": record.get("precision_at_auto_apply"),
                "auto_apply_count": record.get("auto_apply_count"),
            },
            "per_class_support": {cls: len(pairs) for cls, pairs in by_class.items()},
            "pooled_samples": len(pooled),
            "certified_coverage": primary_report,
            "alpha_sweep": sweep,
            "risk_coverage": {"aurc": area_under_risk_coverage(curve), "curve": curve},
            "repeated_split_validity": repeated_split_certification(
                by_class, alpha=primary_alpha, delta=delta, min_support=min_support, splits=splits
            ),
            "reliability": {
                "ece": expected_calibration_error(pooled),
                "curve": reliability_curve(pooled),
            },
            "promotion_verdict": _promotion_verdict(
                precision_at_auto_apply=record.get("precision_at_auto_apply"),  # type: ignore[arg-type]
                ece=record.get("ece"),  # type: ignore[arg-type]
                auto_apply_count=record.get("auto_apply_count"),  # type: ignore[arg-type]
            ),
        }

    conclusion = (
        "At least one condition earned distribution-free certified auto-apply coverage."
        if any_certified
        else (
            "No condition earned any distribution-free certified auto-apply coverage at "
            f"the tested alphas (primary alpha={primary_alpha}). Propose-not-apply is the "
            "provably correct policy; calibration - not model capability or effort - is "
            "the binding constraint."
        )
    )
    return {
        "artifact": _ARTIFACT_SCHEMA,
        "purpose": _PURPOSE,
        "primary_alpha": primary_alpha,
        "delta": delta,
        "min_support": min_support,
        "splits": splits,
        "conditions": conditions,
        "conclusion": conclusion,
    }


def render_methods_note(artifact: Mapping[str, object]) -> str:
    """Render a short human-readable methods note (markdown) for the artifact."""
    lines: list[str] = []
    lines.append("# DataForge Selective-Repair Calibration Benchmark")
    lines.append("")
    lines.append(str(artifact["purpose"]))
    lines.append("")
    lines.append(
        "Method: the auto-apply gate is a selective classifier (Geifman & El-Yaniv, "
        "2017). Per-class thresholds are certified with conformal risk control "
        "(Angelopoulos et al., 2022; RCPS, Bates et al., 2021) on a calibration split "
        "and measured on a disjoint test split. We report the risk-coverage curve and "
        "its AURC, an alpha sweep of certified coverage, a K random-split validity "
        "check, and a reliability diagram with ECE (Guo et al., 2017)."
    )
    lines.append("")
    lines.append(
        f"Settings: primary alpha={artifact['primary_alpha']}, delta={artifact['delta']}, "
        f"min_support={artifact['min_support']}, splits={artifact['splits']}."
    )
    lines.append("")
    lines.append(
        "| Condition | Model | Dataset | Pooled n | ECE | prec@auto | "
        "AURC | Certified coverage | Promoted |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    conditions = artifact["conditions"]
    assert isinstance(conditions, Mapping)
    for label, cond in conditions.items():
        agg = cond["aggregate"]
        lines.append(
            f"| {label} | {cond['model']} | {cond['dataset']} | {cond['pooled_samples']} | "
            f"{agg['ece']} | {agg['precision_at_auto_apply']} | "
            f"{cond['risk_coverage']['aurc']} | "
            f"{cond['certified_coverage']['overall_test_coverage']} | "
            f"{cond['promotion_verdict']['promoted']} |"
        )
    lines.append("")
    lines.append(f"Conclusion: {artifact['conclusion']}")
    lines.append("")
    lines.append(
        "Scope and limits: the conformal guarantee holds for data exchangeable with "
        "the calibration sample (a distribution-shift monitor downgrades auto-apply "
        "otherwise); error classes are assigned by the versioned heuristic labeler "
        "(LABELER_VERSION v1); classes below min_support are reported as insufficient "
        "support rather than certified. The SMT verifier and safety constitution remain "
        "the hard floor beneath the calibration layer."
    )
    return "\n".join(lines) + "\n"
