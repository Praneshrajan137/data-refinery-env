"""Unit tests for the inferred-value guard helpers (soundness of one-sided bounds).

The guard is defense-in-depth for the opt-in LLM-auto-apply path. A one-sided
inferred domain bound must still be enforced on its known side rather than
skipped, so an arbitrarily extreme value cannot pass silently.
"""

from __future__ import annotations

from dataforge.verifier.inferred import domain_violation
from dataforge.verifier.schema import DomainBound


def test_two_sided_bound_still_accepts_within_padded_range() -> None:
    bound = DomainBound(column="x", min_value=100.0, max_value=110.0)
    # span 10, pad 5 -> [95, 115] accepted.
    assert domain_violation("112", bound) is None
    assert domain_violation("140", bound) is not None


def test_one_sided_lower_bound_is_enforced() -> None:
    bound = DomainBound(column="x", min_value=100.0, max_value=None)
    # Previously this skipped the check entirely; now the known side is enforced.
    assert domain_violation("1", bound) is not None
    assert domain_violation("120", bound) is None  # above min, no upper limit


def test_one_sided_upper_bound_is_enforced() -> None:
    bound = DomainBound(column="x", min_value=None, max_value=100.0)
    assert domain_violation("100000", bound) is not None
    assert domain_violation("10", bound) is None  # below max, no lower limit


def test_no_bounds_and_non_numeric_pass() -> None:
    assert domain_violation("50", DomainBound(column="x", min_value=None, max_value=None)) is None
    assert domain_violation("abc", DomainBound(column="x", min_value=0.0, max_value=10.0)) is None
