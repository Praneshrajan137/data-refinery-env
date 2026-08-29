"""Repairer for functional-dependency violations."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from dataforge.detectors.base import FunctionalDependency, Issue, Schema
from dataforge.fd_index import DeterminantGroupIndex
from dataforge.repairers.base import ProposedFix, ProvenanceLiteral, RetryContext
from dataforge.table import TableLike, cell_value, column_names, row_count
from dataforge.transactions.log import sha256_bytes
from dataforge.transactions.txn import CellFix

if TYPE_CHECKING:
    from dataforge.agent.providers import Message


async def complete(messages: list[Message], *, model: str, temperature: float) -> str:
    """Lazy provider wrapper kept patchable for tests."""
    try:
        from dataforge.agent.providers import complete as provider_complete
    except ImportError as exc:
        raise RuntimeError(
            "LLM-backed FD repair requires the provider extra: pip install 'dataforge[providers]'."
        ) from exc
    return await provider_complete(messages, model=model, temperature=temperature)


def _resolve_model() -> str:
    """Resolve the effective model from the active provider's env/default."""
    try:
        from dataforge.agent.providers import resolve_model
    except ImportError:  # pragma: no cover - exercised only without extra
        return "gemini-2.0-flash"
    resolved = resolve_model()
    return resolved or "gemini-2.0-flash"


def _normalize_cell(value: object) -> str:
    """Normalize a DataFrame cell into a comparable string."""
    return str(value)


class _Choice(TypedDict):
    """Chosen value plus its provenance."""

    value: str
    provenance: ProvenanceLiteral


class FDViolationRepairer:
    """Repair FD violations with majority rules and cached LLM fallback."""

    def __init__(
        self,
        *,
        cache_dir: Path | None,
        allow_llm: bool = False,
        model: str | None = None,
    ) -> None:
        self._cache_dir = cache_dir
        self._allow_llm = allow_llm
        self._model = model or _resolve_model()
        # One index per repairer instance, i.e. per repair pass. Nothing is shared across passes,
        # so a cached grouping cannot outlive the table it describes.
        self._group_index = DeterminantGroupIndex()

    def _propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None,
    ) -> ProposedFix | None:
        """Return a repair proposal for an FD-violation issue.

        When several dependencies name the flagged column, the one with the STRONGEST
        EVIDENCE is used, and a value the verifier already rejected is never proposed again.
        Until 2026-08-29 this loop returned on the FIRST dependency whose ``dependent``
        matched, which had two consequences:

        * **The proposal depended on declaration order.** ``functional_dependencies`` is
          semantically a set, and permuting it changed the proposed value -- measured on
          ``tests/unit/test_fd_repair_attribution.py``'s fixture as ``Delta`` against
          ``Alpha``. No data changed; only the order the dependencies were written in. This
          is not hypothetical on real corpora: on hospital's oracle premise, all 13 dependent
          columns have at least two dependencies and ``State`` has eight.
        * **It re-proposed values it had been told were rejected.** ``retry_context`` was
          discarded on the first line, so the engine's three-attempt loop
          (``dataforge/engine/repair.py``) received the identical candidate three times and
          spent three deep table copies and three z3 encodings -- the most expensive path in
          the product -- reaching a verdict it already had. The other dependency's answer was
          in the same loop, untried. Skipping rejected values is what lets a retry reach it.

        What did NOT happen is corruption. The differential verifier is fail-closed and
        checks a candidate against the WHOLE schema, so it refused every order-dependent
        value. All 21 cells whose proposal this change alters were checked individually on
        hospital: SMT returned UNKNOWN on all 21 and Direct rejected 20 of them, so not one
        would have been applied. The applied set is unchanged; only the wasted work is gone.

        Ranking is by evidence rather than position -- largest voting group, then widest
        majority margin, then determinant name for a total order. Every key is a property of
        the data, so permuting the list cannot change the outcome.

        The stricter rule of abstaining whenever the dependencies disagree was implemented
        and measured first, then rejected: on hospital's shipped_accept_all arm it gave up 23
        real repairs to avoid 3 corruptions, moving write precision 0.7954 -> 0.7911. Both
        rules remove the order-dependence; only one of them also costs coverage.

        Single-dependency behaviour is unchanged, which is every published flights and rayyan
        FD measurement: the chosen value, the provenance, and all ``None`` cases are identical.
        """
        if issue.issue_type != "fd_violation" or schema is None:
            return None
        if issue.row >= row_count(df) or issue.column not in column_names(df):
            return None

        rejected = frozenset[str]() if retry_context is None else retry_context.rejected_values

        applicable: list[tuple[FunctionalDependency, list[dict[str, str]], Counter[str]]] = []
        for fd in schema.functional_dependencies:
            if fd.dependent != issue.column:
                continue
            group_df = self._matching_group(df, issue.row, fd)
            if group_df is None:
                continue
            counts = Counter(row[fd.dependent] for row in group_df)
            if len(counts) <= 1:
                continue
            applicable.append((fd, group_df, counts))

        if not applicable:
            return None

        old_value = cell_value(df, issue.row, issue.column)

        # Canonicalise by determinant NAME, which is the same total order
        # ``measure_deductive_coverage._sorted_fds`` and the schema builders already emit.
        # That makes the choice a property of the dependency set rather than of the order it
        # was written in, and it is numerically identical on every corpus whose premise was
        # already sorted -- which is all of them. Two richer keys were implemented and
        # measured first, and both were withdrawn:
        #
        # * abstain when the dependencies disagree: hospital shipped_accept_all gave up 23
        #   real repairs to avoid 3 corruptions, write precision 0.7954 -> 0.7911;
        # * prefer the largest voting group: better on oracle and mined, but far worse on
        #   shipped_accept_all -- corruptions 116 -> 161, precision 0.7954 -> 0.735 -- because
        #   group SIZE is not evidence QUALITY. On an 85-dependency premise where only 69 hold,
        #   the widest group is usually the most spurious dependency, so ranking by size
        #   amplifies a bad premise.
        #
        # Order-independence was the defect to fix. Changing which dependency wins was not,
        # and every rule that reweighted them traded corruption for coverage in the dark.
        applicable.sort(key=lambda entry: tuple(entry[0].determinant))

        for fd, group_df, counts in applicable:
            chosen_majority = self._deterministic_choice(counts)
            if chosen_majority is None:
                llm_choice = self._choose_with_cache(fd, group_df, old_value)
                if llm_choice is None:
                    return None
                candidate_value = llm_choice["value"]
                provenance: ProvenanceLiteral = llm_choice["provenance"]
            else:
                candidate_value = chosen_majority
                provenance = "deterministic"
            # An abstention and a refusal are different answers and must stay different.
            # If this dependency's own vote agrees with what the cell already holds, the cell
            # may simply be correct, and consulting further dependencies to find one that
            # disagrees is how a clean cell gets overwritten -- measured at +45 corruptions on
            # hospital's shipped_accept_all arm (116 -> 161) when this returned instead of
            # falling through. So agreement with the current value ends the search.
            if candidate_value == old_value:
                return None
            # A value the VERIFIER refused is a different matter: the search should continue
            # to the next dependency's answer. Without this the engine's three-attempt loop
            # received the identical proposal three times and paid three deep table copies and
            # three z3 encodings for a verdict it already had, and the alternative sitting in
            # this same loop was never tried.
            if candidate_value in rejected:
                continue
            return self._build_fix(issue, old_value, candidate_value, provenance)

        return None

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Return a repair proposal for an FD-violation issue."""
        return self._propose(issue, df, schema, retry_context=retry_context)

    def _matching_group(
        self,
        df: TableLike,
        row_index: int,
        fd: FunctionalDependency,
    ) -> list[dict[str, str]] | None:
        """Return the determinant group containing the issue row.

        This is the hot loop of FD repair. An FD violation flags EVERY row of the violating
        group, so the number of flags grows with the number of rows, and each flag ran a full
        O(rows) scan here -- which is where the measured quadratic behaviour came from. Two
        constant-factor defects were fixed on 2026-08-28 (``column_names`` hoisted out of a
        generator expression, and per-cell ``cell_value`` calls replaced by list indexing) and the
        docstring was explicit that only the constants had moved.

        The asymptotics changed on 2026-08-29. The grouping now comes from
        ``DeterminantGroupIndex``, which builds every group in one pass and is reused by every
        flag in that group, so this is O(group) rather than O(rows) per flag. The index is stamped
        with each determinant column's write revision and rebuilt when one changes, which is the
        invalidation contract ``docs/trust/fd-repair-scalability.md`` correctly required before
        this was allowed: FD repairs write the *dependent* column, which cannot move a row between
        determinant groups, while a chained FD that writes a determinant does invalidate and
        rebuild.

        The returned rows, their order, and the None cases are unchanged; only the cost is. Values
        are still read live from the table -- only the row grouping is cached -- so a repair
        applied by an earlier flag is visible to the next one.
        """
        required_columns = [*fd.determinant, fd.dependent]
        available = set(column_names(df))
        if any(column not in available for column in required_columns):
            return None

        rows = self._group_index.rows_for_row(df, tuple(fd.determinant), row_index)
        if not rows:
            return None
        return [
            {column: cell_value(df, row, column) for column in required_columns} for row in rows
        ]

    @staticmethod
    def _deterministic_choice(counts: Counter[str]) -> str | None:
        """Return a strict majority value, if one exists.

        "Strict majority" means more than half the determinant group, and until 2026-08-25 this
        function did not implement that. It returned ``ranked[0] > ranked[1]``, a **plurality**, so
        2 votes out of 5 across four distinct values was enough to write. The docstring had claimed
        majority throughout.

        The gap was assumed cosmetic. It is not, and the difference is measured in
        ``eval/results/deductive_coverage_flights.json``: plurality and majority diverge on 1732
        cells there, and majority is better on every axis -- write precision 0.6602 against 0.5618,
        wrong values 270 against 702, clean cells corrupted 344 against 731, and net cells improved
        +579 against +404 despite lower coverage. On hospital the two rules are bit-identical
        (``plurality_only_not_majority`` is 0 in both premise arms), so this costs nothing there.

        Why it matters more than an ordinary precision trade: this value carries ``deterministic``
        provenance, and ``partition_auto_apply`` lets deterministic fixes on allowlisted detectors
        bypass calibration entirely. There is no threshold downstream to catch a bad vote.

        Counting the target cell's **own** value is deliberate and load-bearing. Excluding it --
        requiring every *other* row to agree -- was measured before being implemented and is worse:
        on hospital it halves coverage and introduces 3 corruptions where this rule causes none,
        because when a group is split the cell's own vote is what prevents a confident overwrite.
        See ``eval/preregistration/entailment_strength.md``.
        """
        ranked = counts.most_common()
        if not ranked:
            return None
        top_value, top_count = ranked[0]
        # Subsumes the single-distinct-value case: there top_count == group_size.
        if top_count * 2 > sum(counts.values()):
            return top_value
        return None

    def _choose_with_cache(
        self,
        fd: FunctionalDependency,
        group_df: list[dict[str, str]],
        old_value: str,
    ) -> _Choice | None:
        """Choose a repaired value via cache-backed LLM fallback."""
        if not self._allow_llm or self._cache_dir is None:
            return None

        prompt_payload = {
            "determinant": fd.determinant,
            "dependent": fd.dependent,
            "rows": group_df,
            "current_value": old_value,
        }
        prompt_text = json.dumps(prompt_payload, sort_keys=True)
        prompt_hash = sha256_bytes(prompt_text.encode("utf-8"))
        cache_path = self._cache_dir / f"{prompt_hash}_{self._model.replace('/', '_')}.json"

        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            chosen_value = str(cached["chosen_value"])
            return {"value": chosen_value, "provenance": "llm_cache"}

        messages: list[Message] = [
            {
                "role": "system",
                "content": (
                    "You resolve tabular functional-dependency conflicts. "
                    'Reply with JSON: {"chosen_value": "..."}.'
                ),
            },
            {
                "role": "user",
                "content": (
                    "Choose the most plausible canonical dependent value for this conflicting "
                    f"group. Payload: {prompt_text}"
                ),
            },
        ]
        raw_response = asyncio.run(complete(messages, model=self._model, temperature=0.0))
        try:
            parsed = json.loads(raw_response)
            chosen_value = str(parsed["chosen_value"])
        except (KeyError, TypeError, json.JSONDecodeError):
            return None

        self._cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "prompt_hash": prompt_hash,
                    "model": self._model,
                    "chosen_value": chosen_value,
                    "raw_response": raw_response,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return {"value": chosen_value, "provenance": "llm_live"}

    @staticmethod
    def _build_fix(
        issue: Issue,
        old_value: str,
        new_value: str,
        provenance: ProvenanceLiteral,
    ) -> ProposedFix:
        """Build a proposed fix object."""
        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value=new_value,
                detector_id="fd_violation",
            ),
            reason=issue.reason,
            confidence=issue.confidence,
            provenance=provenance,
        )
