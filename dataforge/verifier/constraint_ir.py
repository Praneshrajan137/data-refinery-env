"""ConstraintIR models bridging schemas, dbt tests, SMT, and SQL proof queries."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from dataforge.verifier.schema import Schema

ConstraintIRKind = Literal[
    "column_type",
    "not_null",
    "domain_bound",
    "regex",
    "unique",
    "accepted_values",
    "functional_dependency",
    "referential",
    "dbt_generic_test",
]


class ConstraintIR(BaseModel):
    """Backend-neutral constraint representation used by patch plans."""

    constraint_id: str = Field(min_length=1)
    kind: ConstraintIRKind
    columns: tuple[str, ...] = Field(default_factory=tuple)
    expression: str | None = None
    verifier: Literal["smt", "sql", "dbt"] = "smt"
    repair_supported: bool = False

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


def constraint_ir_from_schema(schema: Schema | None) -> tuple[ConstraintIR, ...]:
    """Map DataForge's current schema model into the v1 ConstraintIR."""
    if schema is None:
        return ()
    constraints: list[ConstraintIR] = []
    for column, column_type in sorted(schema.columns.items()):
        constraints.append(
            ConstraintIR(
                constraint_id=f"column_type::{column}",
                kind="column_type",
                columns=(column,),
                expression=column_type,
                verifier="smt",
                repair_supported=True,
            )
        )
    for bound in schema.domain_bounds:
        parts: list[str] = []
        if bound.min_value is not None:
            operator = ">=" if bound.inclusive_min else ">"
            parts.append(f"{bound.column} {operator} {bound.min_value}")
        if bound.max_value is not None:
            operator = "<=" if bound.inclusive_max else "<"
            parts.append(f"{bound.column} {operator} {bound.max_value}")
        constraints.append(
            ConstraintIR(
                constraint_id=f"domain_bound::{bound.column}",
                kind="domain_bound",
                columns=(bound.column,),
                expression=" AND ".join(parts) if parts else None,
                verifier="smt",
                repair_supported=True,
            )
        )
    for column in sorted(schema.not_null_columns):
        constraints.append(
            ConstraintIR(
                constraint_id=f"not_null::{column}",
                kind="not_null",
                columns=(column,),
                expression=f"{column} IS NOT NULL",
                verifier="smt",
                repair_supported=True,
            )
        )
    for column in sorted(schema.unique_columns):
        constraints.append(
            ConstraintIR(
                constraint_id=f"unique::{column}",
                kind="unique",
                columns=(column,),
                expression=f"{column} must be unique",
                verifier="smt",
                repair_supported=True,
            )
        )
    for column in sorted(schema.primary_key_columns):
        constraints.append(
            ConstraintIR(
                constraint_id=f"primary_key::{column}",
                kind="unique",
                columns=(column,),
                expression=f"{column} must be not null and unique",
                verifier="smt",
                repair_supported=False,
            )
        )
    for accepted_rule in schema.accepted_values:
        constraints.append(
            ConstraintIR(
                constraint_id=f"accepted_values::{accepted_rule.column}",
                kind="accepted_values",
                columns=(accepted_rule.column,),
                expression=", ".join(accepted_rule.values),
                verifier="smt",
                repair_supported=True,
            )
        )
    for regex_rule in schema.regex_constraints:
        constraints.append(
            ConstraintIR(
                constraint_id=f"regex::{regex_rule.column}",
                kind="regex",
                columns=(regex_rule.column,),
                expression=regex_rule.pattern,
                verifier="smt",
                repair_supported=True,
            )
        )
    for relationship_rule in schema.relationships:
        constraints.append(
            ConstraintIR(
                constraint_id=(
                    f"relationship::{relationship_rule.column}->"
                    f"{relationship_rule.reference}.{relationship_rule.reference_column}"
                ),
                kind="referential",
                columns=(relationship_rule.column,),
                expression=(
                    f"{relationship_rule.column} references "
                    f"{relationship_rule.reference}({relationship_rule.reference_column})"
                ),
                verifier="sql",
                repair_supported=False,
            )
        )
    for fd in schema.functional_dependencies:
        determinant = "+".join(fd.determinant)
        constraints.append(
            ConstraintIR(
                constraint_id=f"fd::{determinant}->{fd.dependent}",
                kind="functional_dependency",
                columns=(*fd.determinant, fd.dependent),
                expression=f"{determinant} -> {fd.dependent}",
                verifier="smt",
                repair_supported=True,
            )
        )
    return tuple(constraints)
