"""Standalone repair-contract helpers for the evaluation harness.

The harness must remain installable without the optional ``dataforge`` package.
These helpers intentionally mirror the public DataForge repair contract shape
used for model grading: strict JSON actions, absolute row ids, exact column
names, last-write-wins duplicate handling, and exact-match diagnostics.
"""

from __future__ import annotations

import json
import re
from collections import Counter, OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field, ValidationError, model_validator

CONTRACT_VERSION = "repair_contract_v2"
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?\s*```", re.DOTALL)


class RepairLike(Protocol):
    """Minimal repair shape shared by DataForge and dataforge-evals."""

    @property
    def row(self) -> int: ...

    @property
    def column(self) -> str: ...

    @property
    def new_value(self) -> str: ...

    @property
    def reason(self) -> str: ...


class TruthLike(Protocol):
    """Minimal ground-truth shape shared by DataForge and dataforge-evals."""

    @property
    def row(self) -> int: ...

    @property
    def column(self) -> str: ...

    @property
    def clean_value(self) -> str: ...


class RepairFix(BaseModel):
    """One exact cell replacement."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    new_value: str
    reason: str = Field(default="repair proposal", min_length=1)

    model_config = {"frozen": True}


class RepairAction(BaseModel):
    """Strict JSON repair action."""

    action: Literal["submit_repairs", "finish"]
    repairs: list[RepairFix] = Field(default_factory=list)

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def _finish_must_be_empty(self) -> RepairAction:
        if self.action == "finish" and self.repairs:
            raise ValueError("finish actions must not include repairs")
        return self


class RepairParseResult(BaseModel):
    """Parsed repair action plus release-gate diagnostics."""

    ok: bool
    action: RepairAction | None = None
    error_kind: (
        Literal[
            "parse_failure",
            "truncated_json",
            "schema_error",
            "invalid_column",
            "invalid_row",
        ]
        | None
    ) = None
    error_message: str | None = None
    diagnostics: dict[str, int | str | bool] = Field(default_factory=dict)

    model_config = {"frozen": True}


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    match = _JSON_FENCE_RE.search(stripped)
    return match.group(1).strip() if match else stripped


def extract_json_payload(text: str) -> object:
    """Extract the first complete JSON object or array from text."""
    clean_text = _strip_fence(text)
    decoder = json.JSONDecoder()
    saw_start = False
    for offset, char in enumerate(clean_text):
        if char not in "[{":
            continue
        saw_start = True
        try:
            payload, _end = decoder.raw_decode(clean_text[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict | list):
            return payload
    if saw_start:
        raise ValueError("truncated_json")
    raise ValueError("parse_failure")


def normalize_fixes(fixes: Iterable[RepairLike]) -> list[RepairFix]:
    """Collapse duplicate cell repairs with last-write-wins semantics."""
    by_cell: OrderedDict[tuple[int, str], RepairFix] = OrderedDict()
    for fix in fixes:
        normalized = RepairFix(
            row=fix.row,
            column=fix.column,
            new_value=fix.new_value,
            reason=fix.reason,
        )
        key = (normalized.row, normalized.column)
        if key in by_cell:
            del by_cell[key]
        by_cell[key] = normalized
    return list(by_cell.values())


def _schema_case_error(column: str, allowed_columns: set[str]) -> bool:
    return column.lower() in {allowed.lower() for allowed in allowed_columns}


def parse_repair_action(
    text: str,
    *,
    allowed_columns: Iterable[str] | None = None,
    valid_rows: Iterable[int] | None = None,
    require_explicit_action: bool = False,
) -> RepairParseResult:
    """Parse model text into a canonical repair action without raising."""
    try:
        payload = extract_json_payload(text)
    except ValueError as exc:
        kind: Literal["parse_failure", "truncated_json"] = (
            "truncated_json" if str(exc) == "truncated_json" else "parse_failure"
        )
        return RepairParseResult(ok=False, error_kind=kind, error_message=str(exc))

    diagnostics: dict[str, int | str | bool] = {}
    if isinstance(payload, list):
        if require_explicit_action:
            return RepairParseResult(
                ok=False,
                error_kind="schema_error",
                error_message="repair payload must include an explicit action",
            )
        payload = {"action": "submit_repairs", "repairs": payload}
    if not isinstance(payload, dict):
        return RepairParseResult(
            ok=False,
            error_kind="schema_error",
            error_message="repair payload must be a JSON object or array",
        )
    if "repairs" in payload and "action" not in payload:
        if require_explicit_action:
            return RepairParseResult(
                ok=False,
                error_kind="schema_error",
                error_message="repair payload must include an explicit action",
            )
        payload = {**payload, "action": "submit_repairs"}
    try:
        action = RepairAction.model_validate(payload)
    except ValidationError as exc:
        return RepairParseResult(ok=False, error_kind="schema_error", error_message=str(exc))

    normalized = normalize_fixes(action.repairs)
    duplicate_count = len(action.repairs) - len(normalized)
    if duplicate_count:
        diagnostics["duplicate_cell_count"] = duplicate_count
        action = RepairAction(action=action.action, repairs=normalized)
    if allowed_columns is not None:
        allowed = set(allowed_columns)
        for repair in action.repairs:
            if repair.column not in allowed:
                diagnostics["invalid_column"] = repair.column
                diagnostics["schema_case_error"] = _schema_case_error(repair.column, allowed)
                return RepairParseResult(
                    ok=False,
                    error_kind="invalid_column",
                    error_message=f"column {repair.column!r} is not in allowed_columns",
                    diagnostics=diagnostics,
                )
    if valid_rows is not None:
        rows = {int(row) for row in valid_rows}
        for repair in action.repairs:
            if repair.row not in rows:
                diagnostics["invalid_row"] = repair.row
                return RepairParseResult(
                    ok=False,
                    error_kind="invalid_row",
                    error_message=f"row {repair.row} is not in valid_rows",
                    diagnostics=diagnostics,
                )
    return RepairParseResult(ok=True, action=action, diagnostics=diagnostics)


def _as_jsonable_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    rendered: list[dict[str, str]] = []
    for row in rows:
        rendered.append({str(key): str(value) for key, value in row.items()})
    return rendered


def _valid_rows_from_target_rows(target_rows: Sequence[Mapping[str, Any]]) -> list[int]:
    return [int(str(row.get("_row", index))) for index, row in enumerate(target_rows)]


def render_repair_messages(
    *,
    schema_summary: Mapping[str, Any],
    target_rows: Sequence[Mapping[str, Any]],
    allowed_columns: Sequence[str],
    valid_rows: Sequence[int] | None = None,
    context_rows: Sequence[Mapping[str, Any]] = (),
    label_source: str | None = None,
    dataset_note: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    repairs: Sequence[RepairLike] | None = None,
) -> list[dict[str, str]]:
    """Render canonical chat messages for repair evaluation."""
    payload: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "schema_summary": dict(schema_summary),
        "allowed_columns": list(allowed_columns),
        "valid_rows": list(valid_rows)
        if valid_rows is not None
        else _valid_rows_from_target_rows(target_rows),
        "target_rows": _as_jsonable_rows(target_rows),
        "context_rows": _as_jsonable_rows(context_rows),
    }
    if label_source is not None:
        payload["label_source"] = label_source
    if dataset_note is not None:
        payload["dataset_note"] = dataset_note
    if metadata is not None:
        payload["metadata"] = dict(metadata)
    messages = [
        {
            "role": "system",
            "content": (
                "You repair tabular data by proposing exact cell replacements. "
                "Rows must be absolute row ids from valid_rows and columns must exactly "
                "match allowed_columns. Return strict JSON only in this object shape: "
                '{"action":"submit_repairs","repairs":[{"row":0,"column":"Column",'
                '"new_value":"value","reason":"why"}]}. '
                'Use {"action":"finish","repairs":[]} when no cells should be changed.'
            ),
        },
        {
            "role": "user",
            "content": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        },
    ]
    if repairs is not None:
        fixes = normalize_fixes(repairs)
        action = RepairAction(action="submit_repairs" if fixes else "finish", repairs=fixes)
        messages.append(
            {
                "role": "assistant",
                "content": json.dumps(
                    action.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
        )
    return messages


def repair_failure_taxonomy(
    *,
    ground_truth: Iterable[TruthLike],
    fixes: Iterable[RepairLike],
    allowed_columns: Iterable[str],
    valid_rows: Iterable[int],
) -> dict[str, int]:
    """Classify exact-match failures without changing official scoring."""
    columns = set(allowed_columns)
    lowercase_columns = {column.lower(): column for column in columns}
    rows = {int(row) for row in valid_rows}
    truth_map = {(cell.row, cell.column): str(cell.clean_value) for cell in ground_truth}
    raw_fixes = list(fixes)
    normalized = normalize_fixes(raw_fixes)
    predictions = {(fix.row, fix.column): fix.new_value for fix in normalized}
    counts: Counter[str] = Counter()
    duplicate_count = len(raw_fixes) - len(normalized)
    if duplicate_count:
        counts["duplicate_cell"] += duplicate_count
    for fix in normalized:
        key = (fix.row, fix.column)
        if fix.column not in columns:
            if fix.column.lower() in lowercase_columns:
                counts["schema_case_error"] += 1
            else:
                counts["wrong_cell"] += 1
            continue
        if fix.row not in rows:
            counts["wrong_cell"] += 1
            continue
        if key not in truth_map:
            counts["overrepair"] += 1
            continue
        if truth_map[key] != fix.new_value:
            counts["wrong_value"] += 1
    for key in truth_map:
        if key not in predictions:
            counts["missed_repair"] += 1
    return {kind: count for kind, count in sorted(counts.items()) if count}
