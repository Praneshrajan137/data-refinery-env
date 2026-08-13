"""Outcome metrics: what a run did for the user, not what a detector scored.

Kept separate from ``dataforge.bench`` because these quantities describe an END-TO-END
outcome (net cells improved, corruption exposure, review effort) rather than a detector's
agreement with a label set.
"""

from __future__ import annotations

from dataforge.metrics.trust_ledger import TrustLedger, clopper_pearson_upper

__all__ = ["TrustLedger", "clopper_pearson_upper"]
