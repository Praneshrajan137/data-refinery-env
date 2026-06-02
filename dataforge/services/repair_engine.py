"""Repair orchestration engine."""

from dataclasses import dataclass, field
from typing import Optional

from dataforge.config import DataForgeConfig
from dataforge.core import RepairerRegistry, Verifier
from dataforge.models import Issue, ProposedFix, RepairResult, Schema, TransactionOperation
from dataforge.types import TableData


@dataclass
class RepairPlan:
    """Plan for repairs to apply."""

    issues: list[Issue]
    """Issues to repair."""

    proposed_fixes: list[ProposedFix]
    """Proposed fixes for issues."""

    auto_apply: bool = False
    """Whether to apply fixes automatically."""

    verified_fixes: list[ProposedFix] = field(default_factory=list)
    """Fixes that have been verified."""

    def get_total_confidence(self) -> float:
        """Get average confidence of proposed fixes."""
        if not self.proposed_fixes:
            return 0.0
        return sum(f.confidence for f in self.proposed_fixes) / len(
            self.proposed_fixes
        )

    def should_auto_apply(self, threshold: float) -> bool:
        """Check if fixes meet auto-apply threshold."""
        return self.auto_apply and self.get_total_confidence() >= threshold


@dataclass
class RepairOutcome:
    """Outcome of repair operation."""

    success: bool
    """Whether repair succeeded."""

    results: list[RepairResult] = field(default_factory=list)
    """Individual repair results."""

    applied_count: int = 0
    """Number of repairs applied."""

    skipped_count: int = 0
    """Number of repairs skipped."""

    failed_count: int = 0
    """Number of repairs that failed."""

    repaired_data: Optional[TableData] = None
    """Data with repairs applied."""

    def __post_init__(self):
        """Calculate statistics from results."""
        if self.results:
            self.applied_count = sum(1 for r in self.results if r.success)
            self.skipped_count = sum(1 for r in self.results if not r.success)
            self.failed_count = sum(
                1 for r in self.results if not r.success and r.error
            )


class RepairEngine:
    """Engine for proposing and applying repairs."""

    def __init__(self, config: Optional[DataForgeConfig] = None):
        """
        Initialize repair engine.

        Args:
            config: DataForge configuration
        """
        self.config = config or DataForgeConfig()
        self.verifier = Verifier(timeout_seconds=self.config.verifier.timeout_seconds)

    def plan_repairs(
        self, data: TableData, issues: list[Issue], schema: Optional[Schema] = None
    ) -> RepairPlan:
        """
        Create a repair plan for detected issues.

        Args:
            data: Original data
            issues: Issues to repair
            schema: Optional schema

        Returns:
            RepairPlan with proposed fixes
        """
        # Get proposed fixes from all repairers
        proposed_fixes_by_repairer = RepairerRegistry.repair_all(
            data, issues, schema, enabled=self.config.repairers.enabled_repairers
        )

        # Flatten and filter by confidence
        all_fixes = []
        for fixes in proposed_fixes_by_repairer.values():
            all_fixes.extend(fixes)

        filtered_fixes = [
            f
            for f in all_fixes
            if f.confidence >= self.config.repairers.repair_confidence_threshold
        ]

        # Verify fixes if enabled
        verified_fixes = []
        if self.config.verifier.enabled:
            for fix in filtered_fixes:
                result = self.verifier.verify(data, fix, schema)
                if result.valid:
                    verified_fixes.append(fix)

        return RepairPlan(
            issues=issues,
            proposed_fixes=filtered_fixes,
            verified_fixes=verified_fixes,
        )

    def apply_repairs(
        self,
        data: TableData,
        plan: RepairPlan,
        auto_apply: bool = False,
    ) -> RepairOutcome:
        """
        Apply repairs from a repair plan.

        Args:
            data: Original data
            plan: Repair plan
            auto_apply: Whether to apply all fixes automatically

        Returns:
            RepairOutcome with results
        """
        # Select fixes to apply
        fixes_to_apply = plan.verified_fixes if plan.verified_fixes else plan.proposed_fixes

        # Make a copy of data for repairs
        repaired_data = [row.copy() for row in data]
        results = []

        for fix in fixes_to_apply:
            try:
                # Apply fix to data
                if fix.row < len(repaired_data):
                    repaired_data[fix.row][fix.column] = fix.proposed_value
                    result = RepairResult(
                        success=True,
                        row=fix.row,
                        column=fix.column,
                        original_value=fix.original_value,
                        new_value=fix.proposed_value,
                        verified=fix in plan.verified_fixes,
                    )
                else:
                    result = RepairResult(
                        success=False,
                        row=fix.row,
                        column=fix.column,
                        original_value=fix.original_value,
                        new_value=fix.proposed_value,
                        error=f"Row {fix.row} out of range",
                    )
            except Exception as e:
                result = RepairResult(
                    success=False,
                    row=fix.row,
                    column=fix.column,
                    original_value=fix.original_value,
                    new_value=fix.proposed_value,
                    error=str(e),
                )

            results.append(result)

        return RepairOutcome(
            success=all(r.success for r in results),
            results=results,
            repaired_data=repaired_data,
        )

    def get_repair_summary(self, outcome: RepairOutcome) -> str:
        """Get human-readable summary of repair outcome."""
        return (
            f"Repairs: {outcome.applied_count} applied, "
            f"{outcome.skipped_count} skipped, {outcome.failed_count} failed"
        )
