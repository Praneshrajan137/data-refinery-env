"""Unsat-core explanation helpers for the Week 3 verifier."""

from __future__ import annotations

from dataforge.verifier.schema import Schema


def explain_unsat_core(unsat_core: tuple[str, ...], schema: Schema) -> str:
    """Convert tracked unsat-core labels into user-facing text."""
    if not unsat_core:
        return "The verifier rejected the fix, but did not expose a tracked explanation."

    parts: list[str] = []
    for label in unsat_core:
        tokens = label.split("::")
        if len(tokens) >= 5 and tokens[0] == "domain":
            _, column, bound_kind, _, row = tokens[:5]
            adjective = "minimum" if bound_kind == "min" else "maximum"
            parts.append(f"Row {row} would violate the {adjective} bound for column '{column}'.")
            continue
        if len(tokens) >= 5 and tokens[0] == "fd":
            _, determinant, dependent, _, row = tokens[:5]
            determinant_text = determinant.replace("+", ", ")
            parts.append(
                f"Row {row} would violate FD (functional dependency) "
                f"{determinant_text} -> {dependent}."
            )
            continue
        if len(tokens) >= 4 and tokens[0] in {"not_null", "primary_key_not_null"}:
            _, column, _, row = tokens[:4]
            parts.append(f"Row {row} would violate the not-null constraint for '{column}'.")
            continue
        if len(tokens) >= 4 and tokens[0] in {"unique", "primary_key_unique"}:
            _, column, _, row = tokens[:4]
            parts.append(f"Row {row} would violate the unique constraint for '{column}'.")
            continue
        if len(tokens) >= 4 and tokens[0] == "accepted_values":
            _, column, _, row = tokens[:4]
            parts.append(f"Row {row} would violate the accepted values constraint for '{column}'.")
            continue
        if len(tokens) >= 4 and tokens[0] == "regex":
            _, column, _, row = tokens[:4]
            parts.append(f"Row {row} would violate the regex constraint for '{column}'.")
            continue
        parts.append(f"Tracked verifier rule '{label}' rejected the fix.")

    return " ".join(parts)
