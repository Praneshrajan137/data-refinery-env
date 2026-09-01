"""Premise quality: does a mined dependency hold at all, measured against a null.

`eval/preregistration/premise_quality.md` split the premise question in two. **Q1** -- given
a dependency that holds on the true data, are these violations errors to fix or legitimate
variation to keep? -- is undecidable in-table and conceded in full. **Q2** -- does the
dependency hold on the true data at all? -- is partly decidable, and this module measures it.

The measure is `mu+` (Piatetsky-Shapiro and Matheus, 1993), which Parciak et al. recommend
for practical use in *Measuring Approximate Functional Dependencies: a Comparative Study*,
ICDE 2024 (arXiv:2312.06296), after evaluating every published alternative against error
rate, LHS-uniqueness and RHS-skew.

**Why this rather than a threshold on `tested_confidence`.** `tested_confidence` (shipped as
C2) separates true from false mined dependencies perfectly on hospital, but gating it needs a
cut point -- 0.9599 -- chosen after seeing which side of it the false dependencies fell on,
from one corpus. `premise_quality.md`'s K3 forbids exactly that. `mu+` needs no cut point:
its correction term is computed from the observed marginals of the specific table and column
pair, and its decision point is **0**, which comes from the permutation null rather than from
a corpus. That is the same "no constant appears" standard C1 and C2 met.

**What the correction corrects.** The literature proves the defect `tested_confidence` was
reaching for. Parciak et al. section IV-B, on the standard `g3` measure:

    "For any non-empty R we can always obtain a subrelation R' of size |dom_X(R)| by
    arbitrarily fixing one y-value for each x-value. As such, g3 is bounded from below by
    |dom_X(R)|/|R| > 0."

Every distinct determinant value contributes one free, unfalsifiable row. As determinant
groups become singletons that floor rises to 1 and the statistic stops carrying information
about whether the dependency is real. That is `ZipCode -> HospitalName` surviving the miner's
guards, stated as a theorem rather than as an observation about one column. Mandros, Boley
and Vreeken reach the same place information-theoretically in KDD 2017: with a one-row group,
conditional entropy is "trivially equal to 0 independent of the true distribution".

See `specs/SPEC_premise_quality.md` for the closed form, the boundary table, and why
`afd-measures` is deliberately not a dependency.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence

__all__ = ["g3_prime", "mu_plus"]


def _pdep_within_groups(groups: Mapping[str, Sequence[str]], total_rows: int) -> float:
    """Probability two rows from the same determinant group agree on the dependent.

    ``pdep(X->Y) = (1/N) * SUM_x [ (1/|g_x|) * SUM_y c_xy^2 ]``
    """
    accumulated = 0.0
    for group_values in groups.values():
        size = len(group_values)
        if size == 0:
            continue
        within = sum(count * count for count in Counter(group_values).values())
        accumulated += within / size
    return accumulated / total_rows


def _pdep_marginal(dependent_values: Sequence[str], total_rows: int) -> float:
    """Probability two rows agree on the dependent, ignoring the determinant.

    ``pdep(Y) = (1/N^2) * SUM_y c_y^2``
    """
    counts = Counter(dependent_values)
    return sum(count * count for count in counts.values()) / (total_rows * total_rows)


def mu_plus(groups: Mapping[str, Sequence[str]], dependent_values: Sequence[str]) -> float:
    """Return ``mu+`` for one candidate dependency, in ``[0.0, 1.0]``.

    Args:
        groups: Determinant value -> the dependent values of the rows sharing it. This is
            the mapping ``_fd_candidates`` already builds, so computing this costs no extra
            pass over the table. A tuple key works unchanged for a multi-column determinant.
        dependent_values: The dependent column's values over the same rows.

    Returns:
        ``max(mu, 0.0)``. Zero means the determinant supplies no evidence beyond what the
        dependent's own distribution already gives -- which is a verdict, not a failure to
        compute. One means an exact dependency on a non-constant dependent.

    Note:
        Every degenerate case returns ``0.0`` rather than raising, and each is a genuine
        limit rather than a defensive guard. The reference implementation in
        ``afd-measures`` raises ``ZeroDivisionError`` on the all-singletons case, which is
        precisely the case a premise gate must reject and therefore not rare.
    """
    total_rows = len(dependent_values)
    # Two rows are needed before "agreement" is defined at all.
    if total_rows < 2 or not groups:
        return 0.0

    determinant_distinct = len(groups)
    # |dom_X| == N: every group is a singleton, nothing could have falsified the dependency,
    # so there is no evidence either way. The formula's correction term diverges here; 0.0
    # is its limit. This is the branch the whole measure exists for.
    if determinant_distinct >= total_rows:
        return 0.0

    marginal = _pdep_marginal(dependent_values, total_rows)
    # pdep(Y) == 1 means the dependent is constant, so it is determined by everything and
    # the dependency is vacuous. The miner rejects these upstream; this must not rely on it.
    if marginal >= 1.0:
        return 0.0

    within = _pdep_within_groups(groups, total_rows)
    proportional_error = (1.0 - within) / (1.0 - marginal)
    singleton_correction = (total_rows - 1) / (total_rows - determinant_distinct)
    mu = 1.0 - proportional_error * singleton_correction
    # Negative mu means the determinant carries LESS information than the permutation null,
    # i.e. weak evidence for a dependency. Parciak et al. clamp rather than report a
    # magnitude, because the ordering below zero is not meaningful.
    return max(mu, 0.0)


def g3_prime(groups: Mapping[str, Sequence[str]]) -> float:
    """Return the normalized ``g3'`` (Giannella and Robertson), in ``[0.0, 1.0]``.

    ``g3'(X->Y) = ( SUM_x max_y c_xy - |dom_X| ) / ( N - |dom_X| )``

    The combinatorial sibling of ``mu+``: it subtracts the same ``|dom_X|`` floor that
    inflates ``g3`` and rescales the remainder. Reported alongside ``mu+`` so the choice
    between them stays auditable, and **never gated on** -- Parciak et al. measure ``g3'``
    as sensitive to RHS-skew where ``mu+`` is not.

    Args:
        groups: Determinant value -> the dependent values of the rows sharing it.

    Returns:
        Zero when every group is a singleton, which is the floor being subtracted.
    """
    if not groups:
        return 0.0
    total_rows = sum(len(group_values) for group_values in groups.values())
    determinant_distinct = len(groups)
    if total_rows < 2 or determinant_distinct >= total_rows:
        return 0.0
    largest_consistent = sum(
        Counter(group_values).most_common(1)[0][1]
        for group_values in groups.values()
        if group_values
    )
    return (largest_consistent - determinant_distinct) / (total_rows - determinant_distinct)
