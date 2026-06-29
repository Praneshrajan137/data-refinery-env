"""Canonical prompt, parsing, and scoring contract for DataForge repairs."""

from __future__ import annotations

import json
import re
from collections import Counter, OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field, ValidationError, model_validator

CONTRACT_VERSION_V1 = "repair_contract_v1"
CONTRACT_VERSION_V2 = "repair_contract_v2"
CONTRACT_VERSION_V3 = "repair_contract_v3"
CONTRACT_VERSION = CONTRACT_VERSION_V2

SYSTEM_PROMPT_V2 = (
    "You repair tabular data by proposing exact cell replacements. "
    "Rows must be absolute row ids from valid_rows and columns must exactly match one of "
    "the allowed_columns values. "
    "Use only the provided dirty target rows and optional context rows. "
    "Return strict JSON only in this object shape: "
    '{"action":"submit_repairs","repairs":[{"row":0,"column":"Column",'
    '"new_value":"value","reason":"why"}]}. '
    'Use {"action":"finish","repairs":[]} when no cells should be changed. '
    "Do not wrap the JSON in markdown code fences."
)
SYSTEM_PROMPT_V3 = (
    "You repair tabular data by proposing exact cell replacements. "
    "Output exactly one compact JSON object and nothing else. "
    "The response must start with { and end with }. "
    'Use {"action":"finish","repairs":[]} when no cells should be changed. '
    'Use {"action":"submit_repairs","repairs":[{"row":0,"column":"Column","new_value":"value"}]} '
    "only when a cell should be changed. Never put repairs in a finish action. "
    "Each repair object must have exactly row, "
    "column, and new_value keys. The column value must exactly match one string from "
    "allowed_columns, and row must be an integer from valid_rows. No prose, comments, "
    "wrappers, or extra keys."
)
SYSTEM_PROMPT = SYSTEM_PROMPT_V2

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?\s*```", re.DOTALL)


class RepairLike(Protocol):
    """Minimal shape shared by repair objects across DataForge packages."""

    @property
    def row(self) -> int: ...

    @property
    def column(self) -> str: ...

    @property
    def new_value(self) -> str: ...

    @property
    def reason(self) -> str: ...


class TruthLike(Protocol):
    """Minimal shape shared by ground-truth cell objects across packages."""

    @property
    def row(self) -> int: ...

    @property
    def column(self) -> str: ...

    @property
    def clean_value(self) -> str: ...


class RepairFix(BaseModel):
    """One exact cell replacement proposed by a repair agent."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    new_value: str
    reason: str = Field(default="repair proposal", min_length=1)

    model_config = {"frozen": True}


class RepairAction(BaseModel):
    """The only JSON action shape accepted by the repair contract."""

    action: Literal["submit_repairs", "finish"]
    repairs: list[RepairFix] = Field(default_factory=list)

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def _finish_must_be_empty(self) -> RepairAction:
        if self.action == "finish" and self.repairs:
            raise ValueError("finish actions must not include repairs")
        return self


class RepairParseResult(BaseModel):
    """Parsed repair action plus diagnostics suitable for release gates."""

    ok: bool
    action: RepairAction | None = None
    error_kind: (
        Literal[
            "parse_failure",
            "truncated_json",
            "schema_error",
            "finish_with_repairs",
            "invalid_column",
            "invalid_row",
        ]
        | None
    ) = None
    error_message: str | None = None
    diagnostics: dict[str, int | str | bool] = Field(default_factory=dict)

    model_config = {"frozen": True}


class RepairScore(BaseModel):
    """Exact-match cell repair metrics."""

    tp: int = Field(ge=0)
    fp: int = Field(ge=0)
    fn: int = Field(ge=0)
    precision: float = Field(ge=0.0, le=1.0)
    recall: float = Field(ge=0.0, le=1.0)
    f1: float = Field(ge=0.0, le=1.0)

    model_config = {"frozen": True}


def _as_jsonable_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Return rows as stable string-valued mappings while preserving ``_row``."""
    rendered: list[dict[str, str]] = []
    for row in rows:
        rendered_row: dict[str, str] = {}
        for key, value in row.items():
            rendered_row[str(key)] = str(value)
        rendered.append(rendered_row)
    return rendered


def _valid_rows_from_target_rows(target_rows: Sequence[Mapping[str, Any]]) -> list[int]:
    """Return absolute row ids from target rows, falling back to local ids for legacy rows."""
    valid_rows: list[int] = []
    for fallback_row, row in enumerate(target_rows):
        raw_row = row.get("_row", fallback_row)
        valid_rows.append(int(str(raw_row)))
    return valid_rows


def build_repair_user_payload(
    *,
    schema_summary: Mapping[str, Any],
    target_rows: Sequence[Mapping[str, Any]],
    context_rows: Sequence[Mapping[str, Any]] = (),
    allowed_columns: Sequence[str],
    valid_rows: Sequence[int] | None = None,
    label_source: str | None = None,
    dataset_note: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    contract_version: str = CONTRACT_VERSION,
) -> dict[str, Any]:
    """Build the canonical user payload for repair SFT and evaluation."""
    payload: dict[str, Any] = {
        "contract_version": contract_version,
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
    return payload


def system_prompt_for_contract(contract_version: str) -> str:
    """Return the system prompt for a versioned repair contract."""
    if contract_version == CONTRACT_VERSION_V3:
        return SYSTEM_PROMPT_V3
    return SYSTEM_PROMPT_V2


def repair_action_json_schema(
    *,
    allowed_columns: Sequence[str] | None = None,
    valid_rows: Sequence[int] | None = None,
) -> dict[str, Any]:
    """Return the strict repair-action JSON Schema used by constrained decoding."""
    row_schema: dict[str, Any] = {"type": "integer", "minimum": 0}
    if valid_rows is not None:
        row_schema = {"type": "integer", "enum": [int(row) for row in valid_rows]}
    column_schema: dict[str, Any] = {"type": "string", "minLength": 1}
    if allowed_columns is not None:
        column_schema = {"type": "string", "enum": [str(column) for column in allowed_columns]}
    repair_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["row", "column", "new_value"],
        "properties": {
            "row": row_schema,
            "column": column_schema,
            "new_value": {"type": "string"},
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "DataForge repair_contract_v3 action",
        "type": "object",
        "additionalProperties": False,
        "required": ["action", "repairs"],
        "oneOf": [
            {
                "properties": {
                    "action": {"const": "finish"},
                    "repairs": {"type": "array", "maxItems": 0},
                }
            },
            {
                "properties": {
                    "action": {"const": "submit_repairs"},
                    "repairs": {"type": "array", "minItems": 1, "items": repair_schema},
                }
            },
        ],
    }


def validate_repair_action_json_schema_payload(
    payload: object,
    *,
    allowed_columns: Sequence[str] | None = None,
    valid_rows: Sequence[int] | None = None,
) -> RepairParseResult:
    """Validate one payload against the strict v3 action-envelope schema.

    ``parse_repair_action`` remains backward-compatible with legacy repair
    artifacts. Product constrained decoding needs the narrower v3 envelope:
    exactly action/repairs at the top level and exactly row/column/new_value in
    each repair.
    """
    if not isinstance(payload, dict):
        return RepairParseResult(
            ok=False,
            error_kind="schema_error",
            error_message="repair action payload must be a JSON object",
        )
    if set(payload) != {"action", "repairs"}:
        return RepairParseResult(
            ok=False,
            error_kind="schema_error",
            error_message="repair action must contain exactly action and repairs",
        )
    action = payload.get("action")
    repairs = payload.get("repairs")
    if action not in {"finish", "submit_repairs"}:
        return RepairParseResult(
            ok=False,
            error_kind="schema_error",
            error_message="action must be finish or submit_repairs",
        )
    if not isinstance(repairs, list):
        return RepairParseResult(
            ok=False,
            error_kind="schema_error",
            error_message="repairs must be a JSON array",
        )
    if action == "finish" and repairs:
        return RepairParseResult(
            ok=False,
            error_kind="finish_with_repairs",
            error_message="finish actions must not include repairs",
            diagnostics={"repair_count": len(repairs)},
        )
    if action == "submit_repairs" and not repairs:
        return RepairParseResult(
            ok=False,
            error_kind="schema_error",
            error_message="submit_repairs actions must include at least one repair",
        )
    for repair in repairs:
        if not isinstance(repair, dict):
            return RepairParseResult(
                ok=False,
                error_kind="schema_error",
                error_message="each repair must be a JSON object",
            )
        if set(repair) != {"row", "column", "new_value"}:
            return RepairParseResult(
                ok=False,
                error_kind="schema_error",
                error_message="each repair must contain exactly row, column, and new_value",
            )
        if not isinstance(repair["row"], int) or repair["row"] < 0:
            return RepairParseResult(
                ok=False,
                error_kind="invalid_row",
                error_message="repair row must be a non-negative integer",
            )
        if not isinstance(repair["column"], str) or not repair["column"]:
            return RepairParseResult(
                ok=False,
                error_kind="invalid_column",
                error_message="repair column must be a non-empty string",
            )
        if not isinstance(repair["new_value"], str):
            return RepairParseResult(
                ok=False,
                error_kind="schema_error",
                error_message="repair new_value must be a string",
            )
    return parse_repair_action(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        allowed_columns=allowed_columns,
        valid_rows=valid_rows,
        require_explicit_action=True,
    )


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
    contract_version: str = CONTRACT_VERSION,
) -> list[dict[str, str]]:
    """Render canonical chat messages for a repair task.

    When ``repairs`` is ``None``, only system and user messages are returned.
    When repairs are provided, an assistant message is appended for SFT.
    """
    messages = [
        {"role": "system", "content": system_prompt_for_contract(contract_version)},
        {
            "role": "user",
            "content": json.dumps(
                build_repair_user_payload(
                    schema_summary=schema_summary,
                    target_rows=target_rows,
                    context_rows=context_rows,
                    allowed_columns=allowed_columns,
                    valid_rows=valid_rows,
                    label_source=label_source,
                    dataset_note=dataset_note,
                    metadata=metadata,
                    contract_version=contract_version,
                ),
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    ]
    if repairs is not None:
        repair_fixes = [
            RepairFix(
                row=repair.row,
                column=repair.column,
                new_value=repair.new_value,
                reason=repair.reason,
            )
            for repair in repairs
        ]
        if contract_version == CONTRACT_VERSION_V3:
            repair_payloads: list[dict[str, Any]] = [
                {"row": fix.row, "column": fix.column, "new_value": fix.new_value}
                for fix in repair_fixes
            ]
            assistant_payload: dict[str, Any] = {
                "action": "submit_repairs" if repair_payloads else "finish",
                "repairs": repair_payloads,
            }
        else:
            assistant_payload = RepairAction(
                action="submit_repairs" if repair_fixes else "finish",
                repairs=repair_fixes,
            ).model_dump(mode="json")
        messages.append(
            {
                "role": "assistant",
                "content": json.dumps(
                    assistant_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
        )
    return messages


def _strip_fence(text: str) -> str:
    """Strip a single markdown JSON fence if the model returned one."""
    stripped = text.strip()
    match = _JSON_FENCE_RE.search(stripped)
    return match.group(1).strip() if match else stripped


def extract_json_payload(text: str) -> object:
    """Extract the first complete JSON object or array from model text."""
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


def _schema_case_error(column: str, allowed_columns: set[str]) -> bool:
    """Return whether ``column`` only differs from an allowed column by case."""
    return column.lower() in {allowed.lower() for allowed in allowed_columns}


def parse_repair_action(
    text: str,
    *,
    allowed_columns: Iterable[str] | None = None,
    valid_rows: Iterable[int] | None = None,
    require_explicit_action: bool = False,
) -> RepairParseResult:
    """Parse model text into a canonical repair action without raising.

    By default this remains permissive enough to read legacy v1 artifacts. Pass
    ``allowed_columns``, ``valid_rows``, and ``require_explicit_action=True`` for
    the v2 release-gate contract.
    """
    try:
        payload = extract_json_payload(text)
    except ValueError as exc:
        if str(exc) == "truncated_json":
            return RepairParseResult(
                ok=False,
                error_kind="truncated_json",
                error_message=str(exc),
            )
        return RepairParseResult(ok=False, error_kind="parse_failure", error_message=str(exc))

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
    if (
        payload.get("action") == "finish"
        and isinstance(payload.get("repairs"), list)
        and payload["repairs"]
    ):
        diagnostics["repair_count"] = len(payload["repairs"])
        diagnostic_action: RepairAction | None = None
        try:
            diagnostic_action = RepairAction.model_validate({**payload, "action": "submit_repairs"})
        except ValidationError:
            diagnostic_action = None
        return RepairParseResult(
            ok=False,
            action=diagnostic_action,
            error_kind="finish_with_repairs",
            error_message="finish actions must not include repairs",
            diagnostics=diagnostics,
        )
    try:
        action = RepairAction.model_validate(payload)
    except ValidationError as exc:
        return RepairParseResult(ok=False, error_kind="schema_error", error_message=str(exc))

    normalized_repairs = normalize_fixes(action.repairs)
    duplicate_count = len(action.repairs) - len(normalized_repairs)
    if duplicate_count:
        diagnostics["duplicate_cell_count"] = duplicate_count
        action = RepairAction(action=action.action, repairs=normalized_repairs)

    if allowed_columns is not None:
        allowed = set(allowed_columns)
        for repair in action.repairs:
            if repair.column in allowed:
                continue
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
            if repair.row in rows:
                continue
            diagnostics["invalid_row"] = repair.row
            return RepairParseResult(
                ok=False,
                error_kind="invalid_row",
                error_message=f"row {repair.row} is not in valid_rows",
                diagnostics=diagnostics,
            )

    return RepairParseResult(ok=True, action=action, diagnostics=diagnostics)


def normalize_fixes(fixes: Iterable[RepairLike]) -> list[RepairFix]:
    """Collapse repairs to one final prediction per cell using last-write-wins."""
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


def canonicalize_cell_value(value: str) -> str:
    """Return a diagnostics-only canonical value for fuzzy F1 reporting."""
    return " ".join(str(value).strip().casefold().split())


def _strict_cell_value(value: str) -> str:
    """Return the official exact-match value normalization."""
    return str(value).rstrip()


def score_repair_fixes(
    ground_truth: Iterable[TruthLike],
    fixes: Iterable[RepairLike],
    *,
    canonicalize_values: bool = False,
) -> RepairScore:
    """Score repairs by exact row, column, and string value match."""
    normalized = normalize_fixes(fixes)
    value_fn = canonicalize_cell_value if canonicalize_values else _strict_cell_value
    expected = {(cell.row, cell.column): value_fn(str(cell.clean_value)) for cell in ground_truth}
    matched: set[tuple[int, str]] = set()
    tp = 0
    fp = 0
    for fix in normalized:
        key = (fix.row, fix.column)
        expected_value = expected.get(key)
        if expected_value is not None and value_fn(fix.new_value) == expected_value:
            tp += 1
            matched.add(key)
        else:
            fp += 1
    fn = len(expected) - len(matched)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return RepairScore(
        tp=tp,
        fp=fp,
        fn=fn,
        precision=round(precision, 4),
        recall=round(recall, 4),
        f1=round(f1, 4),
    )


def score_repair_fixes_canonicalized(
    ground_truth: Iterable[TruthLike],
    fixes: Iterable[RepairLike],
) -> RepairScore:
    """Diagnostics-only F1 after conservative value canonicalization."""
    return score_repair_fixes(ground_truth, fixes, canonicalize_values=True)


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
    rows = set(valid_rows)
    truth_map = {(cell.row, cell.column): str(cell.clean_value) for cell in ground_truth}
    raw_fixes = list(fixes)
    normalized_fixes = normalize_fixes(raw_fixes)
    predictions = {(fix.row, fix.column): fix.new_value for fix in normalized_fixes}
    counts: Counter[str] = Counter()
    duplicate_count = len(raw_fixes) - len(normalized_fixes)
    if duplicate_count:
        counts["duplicate_cell"] += duplicate_count

    for fix in normalized_fixes:
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
