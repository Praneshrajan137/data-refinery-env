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
        parts.append(f"Tracked verifier rule '{label}' rejected the fix.")

    return " ".join(parts)
