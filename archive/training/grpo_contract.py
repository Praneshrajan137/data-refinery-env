"""Shared GRPO reward/eval contract helpers for DataForge repair actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from dataforge.calibration_targets import strict_cell_value
from dataforge.repair_contract import (
    RepairFix,
    RepairParseResult,
    parse_cell_confidences,
    parse_repair_action,
    repair_failure_taxonomy,
    score_repair_fixes,
    score_repair_fixes_canonicalized,
)


@dataclass(frozen=True, slots=True)
class TruthCell:
    """Minimal exact-repair ground-truth cell used by GRPO gates."""

    row: int
    column: str
    clean_value: str


@dataclass(frozen=True, slots=True)
class CalibrationWeights:
    """Weights for the optional, flag-gated selective-prediction reward term.

    Kept small so exact F1 still dominates the reward: calibration nudges the
    model toward honest confidence and abstention without overriding accuracy.
    """

    brier: float = 0.15
    confident_wrong: float = 0.20
    correct_abstention: float = 0.10
    confident_threshold: float = 0.5


DEFAULT_CALIBRATION_WEIGHTS = CalibrationWeights()


def calibration_reward_adjustment(
    *,
    repairs: list[RepairFix],
    truth: list[TruthCell],
    confidences: dict[tuple[int, str], float],
    abstention_slice: bool,
    weights: CalibrationWeights = DEFAULT_CALIBRATION_WEIGHTS,
) -> tuple[float, dict[str, float]]:
    """Return the selective-prediction reward delta and its components.

    The delta rewards well-calibrated confidence (low Brier score), penalizes
    confident-wrong repairs, and rewards correct abstention on the non-inferable
    slices. It is purely a function of ground-truth correctness and model-emitted
    confidence -- never the model's self-reported certainty alone.
    """
    truth_map = {(cell.row, cell.column): strict_cell_value(cell.clean_value) for cell in truth}
    graded: list[tuple[float, int]] = []
    for repair in repairs:
        confidence = confidences.get((repair.row, repair.column))
        if confidence is None:
            continue
        expected = truth_map.get((repair.row, repair.column))
        correct = (
            1 if expected is not None and strict_cell_value(repair.new_value) == expected else 0
        )
        graded.append((confidence, correct))

    brier = sum((conf - correct) ** 2 for conf, correct in graded) / len(graded) if graded else 0.0
    confident_wrong = (
        sum(1 for conf, correct in graded if correct == 0 and conf >= weights.confident_threshold)
        / len(graded)
        if graded
        else 0.0
    )
    correct_abstention = 1.0 if (abstention_slice and not repairs) else 0.0

    delta = (
        (weights.correct_abstention * correct_abstention)
        - (weights.brier * brier)
        - (weights.confident_wrong * confident_wrong)
    )
    components = {
        "calibration_brier": round(brier, 6),
        "calibration_confident_wrong_frac": round(confident_wrong, 6),
        "calibration_correct_abstention": correct_abstention,
        "calibration_graded_count": float(len(graded)),
        "calibration_delta": round(delta, 6),
    }
    return delta, components


def completion_text(completion: object) -> str:
    """Return text from TRL string or chat-message completion shapes."""
    if isinstance(completion, str):
        return completion
    if isinstance(completion, list):
        for item in reversed(completion):
            if isinstance(item, dict) and isinstance(item.get("content"), str):
                return str(item["content"])
    return str(completion)


def batch_item(value: object, index: int, default: object) -> object:
    """Return a per-example item from a scalar-or-batch kwarg."""
    if value is None:
        return default
    if isinstance(value, list) and index < len(value):
        return value[index]
    return value


def truth_cells(raw_truth: object) -> list[TruthCell]:
    """Normalize supported ground-truth shapes into ``TruthCell`` rows."""
    if raw_truth is None:
        return []
    if not isinstance(raw_truth, list):
        raise ValueError("ground_truth entries must be lists.")
    cells: list[TruthCell] = []
    for raw_cell in raw_truth:
        if isinstance(raw_cell, dict):
            clean_value = raw_cell.get(
                "clean_value", raw_cell.get("expected", raw_cell.get("new_value"))
            )
            cells.append(
                TruthCell(
                    row=int(raw_cell["row"]),
                    column=str(raw_cell["column"]),
                    clean_value=str(clean_value),
                )
            )
            continue
        cell_obj = cast(Any, raw_cell)
        row = cell_obj.row
        column = cell_obj.column
        clean_value = (
            cell_obj.clean_value if hasattr(cell_obj, "clean_value") else cell_obj.expected
        )
        cells.append(TruthCell(row=int(row), column=str(column), clean_value=str(clean_value)))
    return cells


def _coerce_int_list(raw_rows: object) -> list[int]:
    """Return integer rows from JSON-friendly row inputs."""
    if raw_rows is None:
        return []
    if isinstance(raw_rows, list):
        return [int(row) for row in raw_rows]
    return [int(raw_rows)]


def _coerce_string_list(raw_columns: object) -> list[str]:
    """Return string columns from JSON-friendly column inputs."""
    if raw_columns is None:
        return []
    if isinstance(raw_columns, list):
        return [str(column) for column in raw_columns]
    return [str(raw_columns)]


def _repairs(parse_result: RepairParseResult) -> list[RepairFix]:
    """Return parsed repairs, or an empty list for invalid/finish completions."""
    if not parse_result.ok or parse_result.action is None:
        return []
    return parse_result.action.repairs


def _valid_rows(raw_rows: object, truth: list[TruthCell], repairs: list[RepairFix]) -> list[int]:
    """Return valid row ids for scoring and diagnostics."""
    rows = set(_coerce_int_list(raw_rows))
    if rows:
        return sorted(rows)
    rows.update(cell.row for cell in truth)
    rows.update(repair.row for repair in repairs)
    return sorted(rows)


def _allowed_columns(
    raw_columns: object, truth: list[TruthCell], repairs: list[RepairFix]
) -> list[str]:
    """Return allowed columns for scoring and diagnostics."""
    columns = set(_coerce_string_list(raw_columns))
    if columns:
        return sorted(columns)
    columns.update(cell.column for cell in truth)
    columns.update(repair.column for repair in repairs)
    return sorted(columns)


def _taxonomy_for_invalid_parse(
    text: str,
    *,
    truth: list[TruthCell],
    raw_allowed_columns: object,
    raw_valid_rows: object,
    parse_result: RepairParseResult,
) -> dict[str, int]:
    """Best-effort taxonomy for contract failures without rewarding them."""
    repairs: list[RepairFix] = []
    relaxed = parse_repair_action(text, require_explicit_action=False)
    if relaxed.ok and relaxed.action is not None:
        repairs = relaxed.action.repairs
    taxonomy = repair_failure_taxonomy(
        ground_truth=truth,
        fixes=repairs,
        allowed_columns=_allowed_columns(raw_allowed_columns, truth, repairs),
        valid_rows=_valid_rows(raw_valid_rows, truth, repairs),
    )
    if parse_result.error_kind == "invalid_column" and parse_result.diagnostics.get(
        "schema_case_error"
    ):
        taxonomy["schema_case_error"] = max(1, taxonomy.get("schema_case_error", 0))
    return taxonomy


def score_grpo_completion(
    completion: object,
    *,
    raw_truth: object,
    raw_allowed_columns: object,
    raw_valid_rows: object,
    raw_inferability: object = None,
    enable_calibration: bool = False,
    calibration_weights: CalibrationWeights = DEFAULT_CALIBRATION_WEIGHTS,
) -> tuple[float, dict[str, Any]]:
    """Score one completion with the strict ``repair_contract_v2`` GRPO reward.

    The public release gate still uses strict evaluator metrics. This shaped
    reward only guides GRPO rollouts: exact F1 dominates, canonicalized value
    matches and precision provide small learning signal, and contract errors
    always receive zero reward.

    When ``enable_calibration`` is True an additive selective-prediction term is
    layered on top of the F1 reward (rewarding calibrated confidence and correct
    abstention, penalizing confident-wrong). The flag defaults to False so the
    reproducible F1-only reward path is byte-identical.
    """
    text = completion_text(completion)
    truth = truth_cells(raw_truth)
    inferability = "" if raw_inferability is None else str(raw_inferability)
    allowed_columns = _coerce_string_list(raw_allowed_columns)
    valid_rows = _coerce_int_list(raw_valid_rows)
    parse_result = parse_repair_action(
        text,
        allowed_columns=allowed_columns,
        valid_rows=valid_rows,
        require_explicit_action=True,
    )
    if not parse_result.ok:
        return 0.0, {
            "parse_ok": False,
            "error_kind": parse_result.error_kind,
            "error_message": parse_result.error_message,
            "inferability": inferability,
            "parser_diagnostics": parse_result.diagnostics,
            "failure_taxonomy": _taxonomy_for_invalid_parse(
                text,
                truth=truth,
                raw_allowed_columns=raw_allowed_columns,
                raw_valid_rows=raw_valid_rows,
                parse_result=parse_result,
            ),
            "reward_components": {"contract_valid": 0.0},
        }

    repairs = _repairs(parse_result)
    truth_cell_count = len(truth)
    predicted_repair_count = len(repairs)
    empty_repair_on_truth_positive = truth_cell_count > 0 and predicted_repair_count == 0
    if not truth and not repairs:
        score = {"tp": 0, "fp": 0, "fn": 0, "precision": 1.0, "recall": 1.0, "f1": 1.0}
        return 1.0, {
            "parse_ok": True,
            "action": parse_result.action.action if parse_result.action else None,
            "inferability": inferability,
            "truth_cell_count": truth_cell_count,
            "predicted_repair_count": predicted_repair_count,
            "empty_repair_on_truth_positive": False,
            "score": score,
            "canonicalized_score": score,
            "failure_taxonomy": {},
            "reward_components": {"strict_f1": 1.0, "clean_finish": 1.0},
        }

    abstention_slice = inferability in {
        "external_reference_required",
        "not_inferable_from_prompt",
    }
    if abstention_slice and not truth and repairs:
        taxonomy = repair_failure_taxonomy(
            ground_truth=truth,
            fixes=repairs,
            allowed_columns=_allowed_columns(raw_allowed_columns, truth, repairs),
            valid_rows=_valid_rows(raw_valid_rows, truth, repairs),
        )
        return 0.0, {
            "parse_ok": True,
            "action": parse_result.action.action if parse_result.action else None,
            "inferability": inferability,
            "truth_cell_count": truth_cell_count,
            "predicted_repair_count": predicted_repair_count,
            "empty_repair_on_truth_positive": False,
            "score": score_repair_fixes(truth, repairs).model_dump(mode="json"),
            "canonicalized_score": score_repair_fixes_canonicalized(truth, repairs).model_dump(
                mode="json"
            ),
            "failure_taxonomy": taxonomy,
            "reward_components": {
                "contract_valid": 1.0,
                "abstention_overrepair": 1.0,
            },
        }

    full_allowed_columns = _allowed_columns(raw_allowed_columns, truth, repairs)
    full_valid_rows = _valid_rows(raw_valid_rows, truth, repairs)
    score = score_repair_fixes(truth, repairs)
    canonicalized_score = score_repair_fixes_canonicalized(truth, repairs)
    taxonomy = repair_failure_taxonomy(
        ground_truth=truth,
        fixes=repairs,
        allowed_columns=full_allowed_columns,
        valid_rows=full_valid_rows,
    )
    penalty = min(
        0.40,
        (0.10 * taxonomy.get("schema_case_error", 0))
        + (0.08 * taxonomy.get("wrong_cell", 0))
        + (0.06 * taxonomy.get("wrong_value", 0))
        + (0.04 * taxonomy.get("overrepair", 0)),
    )
    empty_truth_positive_penalty = 0.05 if empty_repair_on_truth_positive else 0.0
    shaped = (
        (0.65 * score.f1)
        + (0.10 * canonicalized_score.f1)
        + (0.10 * score.precision)
        + (0.15 * score.recall)
        - penalty
        - empty_truth_positive_penalty
    )
    reward_components: dict[str, float] = {
        "strict_f1": score.f1,
        "canonicalized_f1": canonicalized_score.f1,
        "precision": score.precision,
        "recall": score.recall,
        "recall_gap": round(1.0 - score.recall, 6) if truth_cell_count else 0.0,
        "penalty": round(penalty, 6),
        "empty_truth_positive_penalty": empty_truth_positive_penalty,
    }
    if enable_calibration:
        calibration_delta, calibration_components = calibration_reward_adjustment(
            repairs=repairs,
            truth=truth,
            confidences=parse_cell_confidences(text),
            abstention_slice=abstention_slice,
            weights=calibration_weights,
        )
        shaped += calibration_delta
        reward_components.update(calibration_components)
    reward = max(0.0, min(1.0, round(float(shaped), 6)))
    diagnostics = {
        "parse_ok": True,
        "action": parse_result.action.action if parse_result.action else None,
        "inferability": inferability,
        "truth_cell_count": truth_cell_count,
        "predicted_repair_count": predicted_repair_count,
        "empty_repair_on_truth_positive": empty_repair_on_truth_positive,
        "score": score.model_dump(mode="json"),
        "canonicalized_score": canonicalized_score.model_dump(mode="json"),
        "failure_taxonomy": taxonomy,
        "reward_components": reward_components,
    }
    return reward, diagnostics
