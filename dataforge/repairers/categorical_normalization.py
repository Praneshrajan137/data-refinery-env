"""Repairer for categorical-normalization variants.

Maps a minority spelling/format variant to the dominant exact form of its
cluster (the value carried in ``issue.expected`` by the detector). This is a
majority-vote canonicalization analogous to the FD repairer; it abstains when
no expected canonical is present. Every proposal still passes the SMT verifier
and the safety constitution.
"""

from __future__ import annotations

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RetryContext
from dataforge.table import TableLike, cell_value
from dataforge.transactions.txn import CellFix


class CategoricalNormalizationRepairer:
    """Canonicalizes a minority categorical variant to its cluster's dominant form."""

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Propose mapping the variant to the dominant canonical form."""
        del retry_context, schema
        if issue.issue_type != "categorical_normalization" or not issue.expected:
            return None
        old_value = cell_value(df, issue.row, issue.column)
        if issue.expected == old_value:
            return None
        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value=issue.expected,
                detector_id="categorical_normalization",
                operation="update",
            ),
            reason=(f"Normalized '{old_value}' to dominant cluster form '{issue.expected}'."),
            confidence=issue.confidence,
            provenance="deterministic",
        )
