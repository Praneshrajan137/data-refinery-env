"""Per-issue correction contracts.

A :class:`CorrectionContract` binds a detected :class:`~dataforge.detectors.base.Issue`
to the constraints a valid correction for it must satisfy. It is the bridge
between detection and (LLM) correction:

* ``describe()`` turns the issue + inferred/declared constraints into a precise,
  evidence-light target specification a corrector can be prompted with.
* ``validate(value)`` cheaply checks a candidate value against that target
  before the SMT verifier and constitution gates are consulted, and gives the
  corrector an actionable rejection reason for a retry.

The contract is intentionally value-local: it reuses
:mod:`dataforge.verifier.inferred` for type / domain / regex checks so it can
never disagree with the schema-less verification guard. Table-relative
constraints (functional dependencies, uniqueness) remain the verifier's job;
the contract never claims a candidate is fully valid, only that it is well
formed enough to be worth verifying.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from dataforge.detectors.base import Issue, IssueTypeLiteral
from dataforge.verifier.inferred import value_local_violation
from dataforge.verifier.schema import Schema

# Issue classes whose correct value is necessarily numeric.
_NUMERIC_ISSUE_TYPES: frozenset[IssueTypeLiteral] = frozenset(
    {"decimal_shift", "outlier"}
)

# Row-level issue classes that are not single-cell value corrections.
_NON_CELL_ISSUE_TYPES: frozenset[IssueTypeLiteral] = frozenset({"duplicate_row"})


class ContractResult(BaseModel):
    """Outcome of validating a candidate correction against a contract."""

    ok: bool
    reason: str = Field(min_length=1)

    model_config = {"frozen": True}


class CorrectionContract(BaseModel):
    """What a valid correction for a specific issue must look like."""

    issue: Issue
    constraints: Schema

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    @property
    def column(self) -> str:
        """Column the correction targets."""
        return self.issue.column

    @property
    def is_cell_correction(self) -> bool:
        """Whether this issue is a single-cell value correction at all."""
        return self.issue.issue_type not in _NON_CELL_ISSUE_TYPES

    def _requires_numeric(self) -> bool:
        if self.issue.issue_type in _NUMERIC_ISSUE_TYPES:
            return True
        column_type = (self.constraints.column_type(self.column) or "str").strip().lower()
        return column_type in {"int", "integer", "float", "decimal", "real"}

    def check(self, value: str) -> ContractResult:
        """Validate a candidate correction value against the contract.

        Rejects definitively wrong candidates (a no-op equal to the dirty value,
        an empty value, a value violating inferred type/domain/regex, or a value
        for a non-cell issue). A passing result means "well formed enough to send
        to the verifier", not "guaranteed correct".
        """
        if not self.is_cell_correction:
            return ContractResult(
                ok=False,
                reason=(
                    f"Issue type '{self.issue.issue_type}' is a row-level operation, "
                    "not a single-cell value correction."
                ),
            )

        candidate = str(value).strip()
        if not candidate:
            return ContractResult(
                ok=False,
                reason="A correction must be a non-empty value.",
            )

        if candidate == str(self.issue.actual).strip():
            return ContractResult(
                ok=False,
                reason="A correction must differ from the current (erroneous) value.",
            )

        if self._requires_numeric():
            numeric_reason = value_local_violation(
                candidate, self.column, _numeric_only_schema(self.constraints, self.column)
            )
            if numeric_reason is not None:
                return ContractResult(ok=False, reason=numeric_reason)

        violation = value_local_violation(candidate, self.column, self.constraints)
        if violation is not None:
            return ContractResult(ok=False, reason=violation)

        return ContractResult(
            ok=True,
            reason="Candidate satisfies the value-local correction contract.",
        )

    def describe(self) -> str:
        """Return a precise, corrector-facing specification of a valid value."""
        requirements: list[str] = [
            "be different from the current value",
            "be a single non-empty value",
        ]

        column_type = (self.constraints.column_type(self.column) or "str").strip().lower()
        if self._requires_numeric() or column_type in {
            "int",
            "integer",
            "float",
            "decimal",
            "real",
        }:
            if column_type in {"int", "integer"}:
                requirements.append("be a valid integer")
            else:
                requirements.append("be a valid number")

        for bound in self.constraints.domain_bounds_for(self.column):
            if bound.min_value is not None and bound.max_value is not None:
                requirements.append(
                    f"lie near the observed numeric range [{bound.min_value}, {bound.max_value}]"
                )

        for rule in self.constraints.regex_constraints_for(self.column):
            requirements.append(f"match the pattern {rule.pattern}")

        requirement_lines = "\n".join(f"  - {item}" for item in requirements)
        expected_hint = ""
        if self.issue.expected is not None:
            expected_hint = (
                f"\nA deterministic analysis suggests the value may be "
                f"{self.issue.expected!r}; use it only if it is correct."
            )

        return (
            f"Correct the value at row {self.issue.row}, column '{self.column}'.\n"
            f"Current value: {self.issue.actual!r}\n"
            f"Detected issue: {self.issue.issue_type} - {self.issue.reason}\n"
            f"A valid correction must:\n{requirement_lines}{expected_hint}\n"
            f"Respond with only the corrected value and nothing else."
        )


def _numeric_only_schema(schema: Schema, column: str) -> Schema:
    """Return a schema asserting ``column`` is numeric, for the numeric pre-check.

    Used when the issue class implies a numeric value (e.g. outlier) even if the
    inferred column type was left as ``str`` because of dirty cells.
    """
    if (schema.column_type(column) or "str").strip().lower() in {
        "int",
        "integer",
        "float",
        "decimal",
        "real",
    }:
        return schema
    columns = dict(schema.columns)
    columns[column] = "float"
    return Schema(
        columns=columns,
        functional_dependencies=schema.functional_dependencies,
        regex_constraints=schema.regex_constraints,
        domain_bounds=schema.domain_bounds,
    )


def build_correction_contract(issue: Issue, constraints: Schema) -> CorrectionContract:
    """Build a correction contract for an issue against inferred/declared rules."""
    return CorrectionContract(issue=issue, constraints=constraints)
