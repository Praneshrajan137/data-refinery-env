"""Repairer for functional-dependency violations."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from dataforge.detectors.base import FunctionalDependency, Issue, Schema
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
        model: str = "gemini-2.0-flash",
    ) -> None:
        self._cache_dir = cache_dir
        self._allow_llm = allow_llm
        self._model = model

    def _propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None,
    ) -> ProposedFix | None:
        """Return a repair proposal for an FD-violation issue."""
        del retry_context
        if issue.issue_type != "fd_violation" or schema is None:
            return None
        if issue.row >= row_count(df) or issue.column not in column_names(df):
            return None

        for fd in schema.functional_dependencies:
            if fd.dependent != issue.column:
                continue
            group_df = self._matching_group(df, issue.row, fd)
            if group_df is None:
                continue

            counts = Counter(row[fd.dependent] for row in group_df)
            if len(counts) <= 1:
                continue

            old_value = cell_value(df, issue.row, issue.column)
            chosen_majority = self._deterministic_choice(counts)
            if chosen_majority is not None:
                if chosen_majority == old_value:
                    return None
                return self._build_fix(issue, old_value, chosen_majority, "deterministic")

            llm_choice = self._choose_with_cache(fd, group_df, old_value)
            if llm_choice is None or llm_choice["value"] == old_value:
                return None
            return self._build_fix(issue, old_value, llm_choice["value"], llm_choice["provenance"])

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
        """Return the determinant group containing the issue row."""
        required_columns = [*fd.determinant, fd.dependent]
        if any(column not in column_names(df) for column in required_columns):
            return None

        determinant_values = {
            column: cell_value(df, row_index, column) for column in fd.determinant
        }
        group_rows: list[dict[str, str]] = []
        for row in range(row_count(df)):
            if all(
                cell_value(df, row, column) == value for column, value in determinant_values.items()
            ):
                group_rows.append(
                    {column: cell_value(df, row, column) for column in required_columns}
                )
        if not group_rows:
            return None
        return group_rows

    @staticmethod
    def _deterministic_choice(counts: Counter[str]) -> str | None:
        """Return a strict majority value, if one exists."""
        ranked = counts.most_common()
        if len(ranked) < 2:
            return ranked[0][0] if ranked else None
        if ranked[0][1] > ranked[1][1]:
            return ranked[0][0]
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
