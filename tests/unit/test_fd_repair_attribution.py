"""FD repair must not depend on schema declaration order, nor re-propose a rejected value.

Written FIRST (TDD red phase), and deliberately scoped by what measurement showed rather
than by what was suspected.

## What was suspected, and refuted

The hypothesis was that `FDViolationRepairer._propose` picking the FIRST functional
dependency whose `dependent` matches the flagged column
(`dataforge/repairers/fd_violation.py`) writes an order-dependent value to disk, because
the `Issue` model carries no FD identity and `run_all_detectors` deduplicates per
`(row, column)`, discarding which FD flagged the cell.

The first half is true: on the fixture below the proposal is `Delta` when
`region -> city` is declared first and `Alpha` when `zip -> city` is. The second half is
**false**. The differential verifier is fail-closed and checks a fix against the WHOLE
schema, so it rejects both -- "Row 2 would violate FD (functional dependency)
zip -> city." No value can satisfy two FDs that disagree about one cell, so every
candidate is rejected and the APPLIED state is identical under either order. The safety
invariant held exactly as designed, and there is no corruption to fix here.

## What survives, and is what these tests pin

Two things the verifier cannot clean up:

1. **The proposal is order-dependent.** Reordering a LIST that is semantically a SET
   changes what the product proposes, what the receipt records, and which rejection
   reason the user is shown. `test_proposal_is_independent_of_fd_declaration_order`.

2. **The repairer discards `retry_context`.** `_propose` opens with
   `del retry_context`, so the engine's three-attempt loop
   (`dataforge/engine/repair.py`, `for attempt_number in range(1, 4)`) re-proposes the
   value it was just told was rejected. The engine faithfully rebuilds the context with
   `rejected_values` after every REJECT; the repairer throws it away. That costs three
   full verifier round-trips per conflicted flag -- each one a deep table copy plus a
   fresh z3 encoding, the most expensive path in the product -- and the cell is never
   repaired, even though the other FD's answer is sitting in the same loop untried.
   `test_repairer_does_not_repropose_a_rejected_value`.

Fixing (2) by consulting every applicable FD and abstaining on disagreement subsumes (1),
removes the wasted round-trips instead of making them cheaper, and moves the fail-closed
decision to proposal time where it costs nothing.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.detectors.base import FunctionalDependency, Issue, Schema
from dataforge.detectors.fd_violation import FDViolationDetector
from dataforge.repairers.base import RetryContext
from dataforge.repairers.fd_violation import FDViolationRepairer

# Row 2 is flagged by BOTH dependencies, which disagree about it:
#   zip Z1    -> {Alpha, Alpha, Beta}  => strict majority Alpha
#   region R2 -> {Beta, Delta, Delta, Delta} => strict majority Delta
# Neither value satisfies both, so the fail-closed verifier rejects either one. The point
# is not which value wins; it is that a value wins at all, chosen by declaration order.
_CONFLICTED_ROW = 2

_FD_ZIP = FunctionalDependency(determinant=("zip",), dependent="city")
_FD_REGION = FunctionalDependency(determinant=("region",), dependent="city")


def _frame() -> pd.DataFrame:
    """Return the two-FD conflict fixture."""
    return pd.DataFrame(
        {
            "region": ["R1", "R1", "R2", "R2", "R2", "R2"],
            "zip": ["Z1", "Z1", "Z1", "Z7", "Z8", "Z9"],
            "city": ["Alpha", "Alpha", "Beta", "Delta", "Delta", "Delta"],
        }
    )


def _flagged_issue(df: pd.DataFrame, schema: Schema) -> Issue:
    """Return the deduplicated issue the ensemble reports for the conflicted cell."""
    issues = [i for i in FDViolationDetector().detect(df, schema) if i.row == _CONFLICTED_ROW]
    assert issues, "fixture must flag the conflicted row, or these tests are vacuous"
    return issues[0]


@pytest.fixture()
def repairer() -> FDViolationRepairer:
    """Deterministic repairer: no LLM fallback, no cache."""
    return FDViolationRepairer(cache_dir=None, allow_llm=False)


def test_proposal_is_independent_of_fd_declaration_order(
    repairer: FDViolationRepairer,
) -> None:
    """Permuting a set-like FD list must not change what is proposed."""
    df = _frame()
    proposals: dict[str, str | None] = {}
    for label, fds in (
        ("region_first", (_FD_REGION, _FD_ZIP)),
        ("zip_first", (_FD_ZIP, _FD_REGION)),
    ):
        schema = Schema(functional_dependencies=fds)
        fix = repairer.propose(_flagged_issue(df, schema), df, schema)
        proposals[label] = None if fix is None else fix.fix.new_value

    assert proposals["region_first"] == proposals["zip_first"], (
        "the proposed value depends on the ORDER the functional dependencies were "
        f"declared in, not on the data: {proposals}. `functional_dependencies` is "
        "semantically a set, so reordering it must be a no-op."
    )


def test_repairer_does_not_repropose_a_rejected_value(
    repairer: FDViolationRepairer,
) -> None:
    """A value the verifier already rejected must not be proposed again.

    Deliberately uses a SINGLE dependency. The retry defect is independent of the
    multi-dependency conflict above: with one dependency there is a real, verifiable
    proposal, and the question is only whether being told it was rejected changes
    anything. Under the conflict fixture this test would be vacuous, because the
    corrected repairer abstains on the first call and there is no first proposal to
    reject.
    """
    df = _frame()
    schema = Schema(functional_dependencies=(_FD_ZIP,))
    issue = _flagged_issue(df, schema)

    first = repairer.propose(issue, df, schema)
    assert first is not None, "fixture must yield a first proposal, or this test is vacuous"
    assert first.fix.new_value == "Alpha", "single-dependency behaviour must be unchanged"

    # Exactly what dataforge/engine/repair.py builds after a REJECT verdict.
    retry = RetryContext(
        issue=issue,
        rejected_values=frozenset({first.fix.new_value}),
        hints=(f"Row {_CONFLICTED_ROW} would violate FD (functional dependency) zip -> city.",),
    )
    second = repairer.propose(issue, df, schema, retry)

    if second is not None:
        assert second.fix.new_value not in retry.rejected_values, (
            f"re-proposed {second.fix.new_value!r} after being told it was rejected. The "
            "engine retries three times, so this burns three deep table copies and three "
            "z3 encodings to reach a verdict it already had."
        )


def test_retries_offer_distinct_candidates_instead_of_repeating_one(
    repairer: FDViolationRepairer,
) -> None:
    """Across the engine's three attempts the repairer must not repeat a refused value.

    This replaces an earlier assertion that every proposal must satisfy every dependency
    on the column. That rule was implemented and measured, then withdrawn: it made the
    proposer duplicate the verifier's job, and on hospital's shipped_accept_all arm it
    gave up 23 real repairs to avoid 3 corruptions (write precision 0.7954 -> 0.7911).
    The verifier is the authority on whether a candidate is provable; the proposer's
    obligation is to keep offering NEW candidates until it runs out.

    With two dependencies disagreeing there are exactly two candidates, so the sequence
    must be candidate, other candidate, then abstain -- never the same value twice.
    """
    df = _frame()
    schema = Schema(functional_dependencies=(_FD_REGION, _FD_ZIP))
    issue = _flagged_issue(df, schema)

    offered: list[str] = []
    rejected: set[str] = set()
    for _ in range(3):
        retry = RetryContext(issue=issue, rejected_values=frozenset(rejected))
        fix = repairer.propose(issue, df, schema, retry)
        if fix is None:
            break
        assert fix.fix.new_value not in rejected, (
            f"offered {fix.fix.new_value!r} again after it was rejected; the engine "
            "retries three times, so a repeat burns a deep table copy and a z3 encoding "
            "for a verdict it already had"
        )
        offered.append(fix.fix.new_value)
        rejected.add(fix.fix.new_value)

    assert len(offered) == len(set(offered)), f"repeated a candidate: {offered}"
    assert offered == ["Delta", "Alpha"], (
        "both dependencies' answers should be reachable across retries, strongest "
        f"evidence first (region group of 4 before zip group of 3); got {offered}"
    )
