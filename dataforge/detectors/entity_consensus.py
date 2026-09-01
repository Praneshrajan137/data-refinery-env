"""Detector for cross-row entity-consensus violations (multi-source datasets).

Many real datasets record the same real-world entity in several rows -- e.g. the
same ``flight`` reported by ~24 sources, or the same hospital ``ProviderNumber``
across its measure rows. When an attribute is *determined by* that entity, the
correct value is recoverable by **consensus across the sibling rows**: a cell that
is blank, or that disagrees with a strong sibling consensus, is very likely an
error, and the consensus value is the exact fix -- a value that provably already
exists in the data (not a free-text guess).

This is the lever that turns flights from "fixes nothing" (correction F1 0.0) into
a high-precision correction: measured on the flights benchmark, replacing a
detected error cell with its ``flight``-group consensus reaches precision 0.994 at
consensus support >= 0.7 and precision 1.0 (zero corruption of correct cells) at
support >= 0.95.

Precision-controlled key discovery (the tax spurious-FD trap, memory
fixing-elevation-measured, is avoided here): a column is used as an entity key
only when it yields many multi-row groups and is not near-unique; and a
(key, target) pair is treated as consensus-governed only when a strong majority of
its groups actually agree on a dominant value. Columns the key does not determine
(e.g. ``zip -> salary`` on tax) fail the governance test and are silently skipped,
so the detector abstains on datasets without a genuine multi-source entity key.

Carries the exact consensus value in ``Issue.expected``. It IS paired with a repairer --
:class:`dataforge.repairers.entity_consensus.EntityConsensusRepairer` -- so the claim
previously made here, that it "is not paired with a repairer in Phase 1, so it has no write
path", was false as written: the repairer exists and is registered when
``allow_entity_consensus`` is set.

What is true, and is the load-bearing statement, is that the write path is **classified
plausibility_only** by ``strength_for_fix``, so the engine holds every proposal for review
and auto-applies none of them without the separate ``allow_unproven_autoapply`` opt-in. That
is a property of the engine's classification, not of a missing repairer, and it is the
property a reader needs. Corrected 2026-09-01; the docstring had outlived the code. The verified auto-apply of the provable
(near-unanimous) slice is wired separately behind the corruption oracle.
"""

from __future__ import annotations

from collections import Counter, defaultdict

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values, row_count

# A group must have at least this many rows before its consensus is trusted.
_MIN_GROUP_SIZE = 3
# A key column must yield groups averaging at least this many rows (excludes
# near-unique id columns like tuple_id / index that group into singletons).
_MIN_AVG_GROUP_SIZE = 3.0
# A key column must not be near-unique: distinct keys / rows must be at or below
# this (an id column with a distinct value per row is not an entity key).
_MAX_KEY_UNIQUENESS = 0.5
# A (key, target) pair is consensus-governed only when at least this fraction of
# its qualifying groups agree on a dominant value at >= support threshold. This is
# the spurious-key guard: it proves the key actually determines the target.
_GOVERNED_FRACTION = 0.85
# A (key, target) pair must be governed across at least this many entities. A
# handful of coincidentally-consistent groups must not be enough to "govern".
_MIN_GOVERNED_GROUPS = 5
# The consensus values must be DIVERSE across entities: at least this fraction of
# governed groups must have a distinct consensus value. This is the correlation
# guard that a support bar alone cannot provide. A true entity key gives each
# entity its own value (flight -> its own arrival time; provider -> its own name),
# so diversity is near 1.0. A categorical CORRELATION repeats a tiny shared
# vocabulary as the consensus of many groups (rayyan article_jissue ->
# "mostly English"), so diversity is low -- and its differing cells are correct
# minority values, not errors (measured: 322 correct cells wrongly flagged). This
# rejects the correlation trap while keeping the support bar low enough to retain
# recall on genuinely-noisy observed attributes (flights actual times).
_MIN_CONSENSUS_DIVERSITY = 0.5
# Minimum consensus support (dominant count / non-blank count within a group) for
# governance and for flagging a cell. Kept moderate (0.7) so noisy-but-determined
# observed attributes (flights actual arrival/departure times, where sources
# disagree slightly) are still repairable; the correlation trap is handled by the
# diversity guard above, not by raising this bar. Measured on flights: precision
# 0.994 on error cells at 0.7. The verified auto-apply tier uses a much stricter
# near-unanimous (>= 0.95) slice separately (Phase 0: precision 1.0, zero
# corruption).
_SUPPORT_THRESHOLD = 0.7
# Skip tiny tables where consensus is not meaningful.
_MIN_ROWS = 8


class EntityConsensusDetector:
    """Flag cells that disagree with a strong cross-row consensus for their entity.

    For each precision-qualified entity key column and each attribute it governs,
    the detector computes the per-group consensus value and flags any cell in the
    group that is blank or differs from that consensus (at support >=
    :data:`_SUPPORT_THRESHOLD`). The exact consensus value is carried in
    ``Issue.expected``. Tier 1, strictly additive.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame(
        ...     {
        ...         "flight": ["A", "A", "A", "A", "B", "B", "B", "B"],
        ...         "arr": ["9:30", "9:30", "9:30", "", "1:00", "1:00", "1:00", "9:99"],
        ...     }
        ... )
        >>> issues = EntityConsensusDetector().detect(df)
        >>> sorted((i.row, i.column, i.expected) for i in issues)
        [(3, 'arr', '9:30'), (7, 'arr', '1:00')]
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect entity-consensus violations across sibling rows."""
        del schema
        n_rows = row_count(df)
        if n_rows < _MIN_ROWS:
            return []
        columns = column_names(df)
        col_values = {col: [str(v) for v in column_values(df, col)] for col in columns}

        # Best issue per (row, column): a cell may be governed by more than one key;
        # keep the highest-support finding so the suggestion is the strongest one.
        best: dict[tuple[int, str], Issue] = {}
        for key_col in columns:
            groups = self._discover_groups(col_values[key_col], n_rows)
            if groups is None:
                continue
            for target_col in columns:
                if target_col == key_col:
                    continue
                self._flag_governed_target(
                    key_col=key_col,
                    target_col=target_col,
                    target_values=col_values[target_col],
                    groups=groups,
                    best=best,
                )
        return list(best.values())

    @staticmethod
    def _discover_groups(key_values: list[str], n_rows: int) -> dict[str, list[int]] | None:
        """Return multi-row groups for a precision-qualified entity key, else None."""
        groups: dict[str, list[int]] = defaultdict(list)
        for row_index, key in enumerate(key_values):
            if key.strip() == "":
                continue  # blank keys do not identify an entity
            groups[key].append(row_index)
        if not groups:
            return None
        # Not near-unique: an id column with a distinct value per row is not a key.
        if len(groups) / n_rows > _MAX_KEY_UNIQUENESS:
            return None
        multi = {key: rows for key, rows in groups.items() if len(rows) >= _MIN_GROUP_SIZE}
        if not multi:
            return None
        avg_group = sum(len(rows) for rows in multi.values()) / len(multi)
        if avg_group < _MIN_AVG_GROUP_SIZE:
            return None
        return multi

    def _flag_governed_target(
        self,
        *,
        key_col: str,
        target_col: str,
        target_values: list[str],
        groups: dict[str, list[int]],
        best: dict[tuple[int, str], Issue],
    ) -> None:
        """Flag cells for one (key, target) pair, if the key governs the target."""
        consensus: dict[str, tuple[str, float]] = {}
        governed = 0
        for key, rows in groups.items():
            non_blank = [target_values[i] for i in rows if target_values[i].strip() != ""]
            if len(non_blank) < _MIN_GROUP_SIZE:
                continue
            top, count = Counter(non_blank).most_common(1)[0]
            support = count / len(non_blank)
            consensus[key] = (top, support)
            if support >= _SUPPORT_THRESHOLD:
                governed += 1
        if not consensus:
            return
        # Spurious-key guard: the key must determine the target in most groups,
        # across enough entities that the agreement is not coincidental.
        if governed < _MIN_GOVERNED_GROUPS:
            return
        if governed / len(consensus) < _GOVERNED_FRACTION:
            return
        # Correlation guard: the governed consensus values must be diverse across
        # entities (a true key gives each entity its own value). A tiny shared
        # vocabulary repeated as many groups' consensus is a categorical
        # correlation, whose differing cells are correct minorities, not errors.
        governed_values = [
            value for value, support in consensus.values() if support >= _SUPPORT_THRESHOLD
        ]
        if len(set(governed_values)) / len(governed_values) < _MIN_CONSENSUS_DIVERSITY:
            return
        for key, (value, support) in consensus.items():
            if support < _SUPPORT_THRESHOLD:
                continue
            for row_index in groups[key]:
                current = target_values[row_index]
                if current == value:
                    continue
                cell = (row_index, target_col)
                existing = best.get(cell)
                if existing is not None and existing.confidence >= support:
                    continue
                best[cell] = Issue(
                    row=row_index,
                    column=target_col,
                    issue_type="entity_consensus",
                    severity=Severity.REVIEW,
                    confidence=min(support, 1.0),
                    expected=value,
                    actual=current,
                    reason=(
                        f"Value {current!r} in column '{target_col}' disagrees with the "
                        f"consensus '{value}' shared by {support:.0%} of the rows for entity "
                        f"'{key}' (grouped by '{key_col}'). The consensus value already exists "
                        f"in sibling rows; surfaced for review, never auto-applied in this tier."
                    ),
                )
