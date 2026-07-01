"""A repairer that defers to a fallback when its primary abstains.

Used to give issue classes with a precise deterministic repairer (e.g.
missing_value's functional-dependency fill) a second chance via the LLM
corrector *only when* the deterministic repairer returns ``None``. The
deterministic proposal always wins when it exists, so the precise, byte-stable
path is never displaced; the corrector only fills the gaps the deterministic
layer cannot.
"""

from __future__ import annotations

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, Repairer, RetryContext
from dataforge.table import TableLike


class FallbackRepairer:
    """Try ``primary`` first, then ``fallback`` when it abstains."""

    def __init__(self, primary: Repairer, fallback: Repairer) -> None:
        self._primary = primary
        self._fallback = fallback

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Return the primary proposal, or the fallback's when primary abstains."""
        primary_fix = self._primary.propose(issue, df, schema, retry_context=retry_context)
        if primary_fix is not None:
            return primary_fix
        return self._fallback.propose(issue, df, schema, retry_context=retry_context)
