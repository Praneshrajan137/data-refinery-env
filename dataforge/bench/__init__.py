"""Shared benchmark helpers for real-world DataForge evaluation."""

from dataforge.bench.agent_gate import (
    AgentGateReport,
    FixtureParity,
    PromotionVerdict,
    agent_promotion_verdict,
    compare_agent_vs_deterministic,
    default_gate_fixtures,
)
from dataforge.bench.core import (
    AggregateBenchmarkResult,
    BenchmarkRepair,
    BenchmarkRunOutput,
    ClassScore,
    SeedBenchmarkResult,
    chunk_row_indices,
    estimate_llm_calls,
    normalize_repairs,
    quota_units,
    score_repairs,
    validate_estimated_calls,
)
from dataforge.bench.error_classes import (
    BENCH_ERROR_CLASSES,
    LABELER_VERSION,
    check_coverage_regression,
    class_coverage_matrix,
    classify_error_cell,
    expected_calibration_error,
    precision_at_auto_apply,
    score_repairs_by_class,
)
from dataforge.bench.report import write_benchmark_outputs
from dataforge.bench.runner import run_agent_comparison

__all__ = [
    "AgentGateReport",
    "AggregateBenchmarkResult",
    "BENCH_ERROR_CLASSES",
    "BenchmarkRepair",
    "BenchmarkRunOutput",
    "ClassScore",
    "FixtureParity",
    "LABELER_VERSION",
    "PromotionVerdict",
    "SeedBenchmarkResult",
    "agent_promotion_verdict",
    "check_coverage_regression",
    "chunk_row_indices",
    "class_coverage_matrix",
    "classify_error_cell",
    "compare_agent_vs_deterministic",
    "default_gate_fixtures",
    "estimate_llm_calls",
    "expected_calibration_error",
    "normalize_repairs",
    "precision_at_auto_apply",
    "quota_units",
    "run_agent_comparison",
    "score_repairs",
    "score_repairs_by_class",
    "validate_estimated_calls",
    "write_benchmark_outputs",
]
