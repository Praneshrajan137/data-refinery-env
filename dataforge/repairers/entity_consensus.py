"""Repairer for cross-row entity-consensus issues.

The :class:`~dataforge.detectors.entity_consensus.EntityConsensusDetector` already
computes the exact fix -- the strong consensus value shared by the sibling rows of
the same entity -- and carries it in ``Issue.expected``. This repairer simply wraps
that value into a proposed fix with ``entity_consensus`` provenance.

Trust boundary: the value is evidence-strong (it already exists in sibling rows),
but a majority can be wrong, so it is NOT proof. It is classified
``plausibility_only`` by the engine and is therefore held for review by default,
auto-applying only under the explicit ``allow_unproven_autoapply`` opt-in (or when
an authoritative schema independently proves it). The repairer never mutates data;
it only proposes.
"""

from __future__ import annotations

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RetryContext
from dataforge.table import TableLike, cell_value, column_names, row_count
from dataforge.transactions.txn import CellFix


class EntityConsensusRepairer:
    """Propose the detector-computed entity-consensus value for a flagged cell."""

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Return the consensus value from ``issue.expected`` as a proposed fix."""
        del schema, retry_context
        if issue.issue_type != "entity_consensus" or issue.expected is None:
            return None
        if issue.row < 0 or issue.row >= row_count(df) or issue.column not in column_names(df):
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
                detector_id="entity_consensus",
            ),
            reason=issue.reason,
            confidence=issue.confidence,
            provenance="entity_consensus",
        )
