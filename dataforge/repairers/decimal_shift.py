"""Deterministic repairer for decimal-shift issues."""

from __future__ import annotations

import pandas as pd

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RetryContext
from dataforge.transactions.txn import CellFix


class DecimalShiftRepairer:
    """Repair decimal-shift issues using the detector's expected value."""

    def propose(
        self,
        issue: Issue,
        df: pd.DataFrame,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Return a deterministic fix for a decimal-shift issue."""
        del schema, retry_context
        if issue.issue_type != "decimal_shift" or issue.expected is None:
            return None
        if issue.row >= len(df.index) or issue.column not in df.columns:
            return None

        old_value = str(df.at[issue.row, issue.column])
        if old_value == issue.expected:
            return None

        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value=issue.expected,
                detector_id="decimal_shift",
            ),
            reason=issue.reason,
            confidence=issue.confidence,
            provenance="deterministic",
        )
