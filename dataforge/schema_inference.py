"""Reviewable schema and constraint inference for DataForge profiles."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from collections import Counter, defaultdict
from contextlib import suppress
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from dataforge.table import TableLike, column_names, column_values, row_count
from dataforge.transactions.log import sha256_bytes
from dataforge.verifier.schema import DomainBound, FunctionalDependency, Schema

ConstraintKind = Literal[
    "column_type",
    "domain_bound",
    "regex",
    "unique",
    "functional_dependency",
]
ConstraintDecision = Literal["pending", "accepted", "rejected"]
CONSTRAINT_REVIEW_SCHEMA_VERSION: Literal["constraint_review_v1"] = "constraint_review_v1"
REPAIR_SUPPORTED_CONSTRAINT_KINDS = frozenset(
    {"column_type", "domain_bound", "functional_dependency"}
)

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DIGITS_RE = re.compile(r"^\d+$")
_UPPER_CODE_RE = re.compile(r"^[A-Z0-9_-]+$")


class ConstraintCandidate(BaseModel):
    """One inferred constraint candidate that must be reviewed before adoption."""

    kind: ConstraintKind
    columns: tuple[str, ...] = Field(min_length=1)
    dependent: str | None = None
    inferred_type: str | None = None
    pattern: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: str = Field(min_length=1)
    provenance: str = "profile_inference_v1"

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class SchemaInferenceResult(BaseModel):
    """Reviewable schema inference result emitted by profile and benchmarks."""

    columns: dict[str, str] = Field(default_factory=dict)
    candidates: list[ConstraintCandidate] = Field(default_factory=list)
    row_count: int = Field(ge=0)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    def to_schema(self, *, include_inferred_constraints: bool = False) -> Schema:
        """Convert reviewed inference output into a verifier Schema."""
        if not include_inferred_constraints:
            return Schema(columns=dict(self.columns))

        fds: list[FunctionalDependency] = []
        bounds: list[DomainBound] = []
        for candidate in self.candidates:
            if (
                candidate.kind == "functional_dependency"
                and candidate.dependent is not None
                and candidate.confidence >= 0.9
            ):
                fds.append(
                    FunctionalDependency(
                        determinant=candidate.columns,
                        dependent=candidate.dependent,
                    )
                )
            elif candidate.kind == "domain_bound" and candidate.confidence >= 0.95:
                bounds.append(
                    DomainBound(
                        column=candidate.columns[0],
                        min_value=candidate.min_value,
                        max_value=candidate.max_value,
                    )
                )
        return Schema(
            columns=dict(self.columns),
            functional_dependencies=tuple(fds),
            domain_bounds=tuple(bounds),
        )


class ReviewedConstraintCandidate(BaseModel):
    """A profile-inferred candidate plus its explicit review decision."""

    candidate_id: str = Field(pattern=r"^cnd-[0-9a-f]{16}$")
    decision: ConstraintDecision = "pending"
    candidate: ConstraintCandidate
    review_note: str | None = None

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class ConstraintReviewArtifact(BaseModel):
    """Strict JSON artifact that records review decisions for inferred constraints."""

    schema_version: Literal["constraint_review_v1"] = CONSTRAINT_REVIEW_SCHEMA_VERSION
    source_path: str = Field(min_length=1)
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(ge=0)
    candidates: list[ReviewedConstraintCandidate] = Field(default_factory=list)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    def accepted_candidates(self) -> list[ReviewedConstraintCandidate]:
        """Return candidates explicitly accepted by review."""
        return [candidate for candidate in self.candidates if candidate.decision == "accepted"]

    def to_schema(self) -> Schema:
        """Convert accepted repair-supported candidates into a verifier Schema."""
        columns: dict[str, str] = {}
        fds: list[FunctionalDependency] = []
        bounds: list[DomainBound] = []

        for reviewed in self.accepted_candidates():
            candidate = reviewed.candidate
            if candidate.kind == "column_type" and candidate.inferred_type is not None:
                columns[candidate.columns[0]] = candidate.inferred_type
            elif candidate.kind == "domain_bound":
                bounds.append(
                    DomainBound(
                        column=candidate.columns[0],
                        min_value=candidate.min_value,
                        max_value=candidate.max_value,
                    )
                )
            elif candidate.kind == "functional_dependency" and candidate.dependent is not None:
                fds.append(
                    FunctionalDependency(
                        determinant=candidate.columns,
                        dependent=candidate.dependent,
                    )
                )

        return Schema(
            columns=columns,
            functional_dependencies=tuple(fds),
            domain_bounds=tuple(bounds),
        )

    def accepted_candidate_ids(self) -> list[str]:
        """Return accepted candidate ids that affect repair in v1."""
        return [
            reviewed.candidate_id
            for reviewed in self.accepted_candidates()
            if reviewed.candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS
        ]


class ConstraintReviewError(ValueError):
    """Raised when a reviewed constraint artifact cannot be used safely."""


def _canonical_candidate_payload(candidate: ConstraintCandidate) -> str:
    """Return stable JSON used for candidate ids and deterministic artifacts."""
    return json.dumps(
        candidate.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )


def constraint_candidate_id(candidate: ConstraintCandidate) -> str:
    """Return the stable id for one inferred constraint candidate."""
    digest = hashlib.sha256(_canonical_candidate_payload(candidate).encode("utf-8")).hexdigest()
    return f"cnd-{digest[:16]}"


def build_constraint_review_artifact(
    inference: SchemaInferenceResult,
    *,
    source_path: Path,
    source_sha256: str,
) -> ConstraintReviewArtifact:
    """Create a pending review artifact from a schema inference result."""
    reviewed: list[ReviewedConstraintCandidate] = []
    seen_ids: set[str] = set()
    for candidate in inference.candidates:
        candidate_id = constraint_candidate_id(candidate)
        if candidate_id in seen_ids:
            raise ConstraintReviewError(f"Duplicate inferred constraint id: {candidate_id}.")
        seen_ids.add(candidate_id)
        reviewed.append(
            ReviewedConstraintCandidate(
                candidate_id=candidate_id,
                candidate=candidate,
            )
        )
    return ConstraintReviewArtifact(
        source_path=str(source_path),
        source_sha256=source_sha256,
        row_count=inference.row_count,
        candidates=reviewed,
    )


def validate_constraint_review_artifact(artifact: ConstraintReviewArtifact) -> None:
    """Validate review-artifact integrity beyond the strict JSON schema."""
    seen_ids: set[str] = set()
    duplicate_ids: list[str] = []
    mismatched_ids: list[str] = []
    for reviewed in artifact.candidates:
        if reviewed.candidate_id in seen_ids:
            duplicate_ids.append(reviewed.candidate_id)
        seen_ids.add(reviewed.candidate_id)

        expected_id = constraint_candidate_id(reviewed.candidate)
        if reviewed.candidate_id != expected_id:
            mismatched_ids.append(f"{reviewed.candidate_id} should be {expected_id}")

    errors: list[str] = []
    if duplicate_ids:
        errors.append("duplicate candidate ids: " + ", ".join(sorted(set(duplicate_ids))))
    if mismatched_ids:
        errors.append("candidate id payload mismatch: " + "; ".join(mismatched_ids))
    if errors:
        raise ConstraintReviewError("Invalid constraints artifact: " + "; ".join(errors))


def load_constraint_review_artifact(path: Path) -> tuple[ConstraintReviewArtifact, str]:
    """Load a strict constraint review artifact and return it with its SHA-256."""
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ConstraintReviewError(f"Could not read constraints file '{path}': {exc}") from exc
    try:
        artifact = ConstraintReviewArtifact.model_validate_json(payload)
    except ValueError as exc:
        raise ConstraintReviewError(f"Invalid constraints file '{path}': {exc}") from exc
    validate_constraint_review_artifact(artifact)
    return artifact, sha256_bytes(payload)


def dump_constraint_review_artifact(artifact: ConstraintReviewArtifact) -> str:
    """Return deterministic, human-reviewable JSON for a constraint artifact."""
    validate_constraint_review_artifact(artifact)
    return json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def update_constraint_review_artifact(
    artifact: ConstraintReviewArtifact,
    *,
    accept_ids: list[str] | tuple[str, ...] = (),
    reject_ids: list[str] | tuple[str, ...] = (),
    pending_ids: list[str] | tuple[str, ...] = (),
    notes: dict[str, str | None] | None = None,
) -> ConstraintReviewArtifact:
    """Return a reviewed artifact with explicit decision and note edits applied."""
    validate_constraint_review_artifact(artifact)
    notes = notes or {}
    decisions: dict[str, ConstraintDecision] = {}
    conflicts: set[str] = set()
    for candidate_id in accept_ids:
        if candidate_id in decisions:
            conflicts.add(candidate_id)
        decisions[candidate_id] = "accepted"
    for candidate_id in reject_ids:
        if candidate_id in decisions:
            conflicts.add(candidate_id)
        decisions[candidate_id] = "rejected"
    for candidate_id in pending_ids:
        if candidate_id in decisions:
            conflicts.add(candidate_id)
        decisions[candidate_id] = "pending"
    if conflicts:
        raise ConstraintReviewError(
            "Candidate ids received conflicting review decisions: " + ", ".join(sorted(conflicts))
        )

    known_ids = {reviewed.candidate_id for reviewed in artifact.candidates}
    unknown_ids = sorted((set(decisions) | set(notes)) - known_ids)
    if unknown_ids:
        raise ConstraintReviewError("Unknown candidate ids: " + ", ".join(unknown_ids))

    updated_candidates: list[ReviewedConstraintCandidate] = []
    for reviewed in artifact.candidates:
        update: dict[str, object] = {}
        if reviewed.candidate_id in decisions:
            update["decision"] = decisions[reviewed.candidate_id]
        if reviewed.candidate_id in notes:
            note = notes[reviewed.candidate_id]
            update["review_note"] = note if note else None
        updated_candidates.append(reviewed.model_copy(update=update))

    updated = artifact.model_copy(update={"candidates": updated_candidates})
    validate_constraint_review_artifact(updated)
    return updated


def write_constraint_review_artifact_atomic(path: Path, artifact: ConstraintReviewArtifact) -> str:
    """Atomically rewrite a constraints artifact and return the written SHA-256."""
    payload = dump_constraint_review_artifact(artifact).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        with suppress(OSError):
            temp_path.unlink()
        raise
    return sha256_bytes(payload)


def merge_schema_with_reviewed_constraints(
    base_schema: Schema | None,
    artifact: ConstraintReviewArtifact | None,
    *,
    source_sha256: str,
) -> tuple[Schema | None, list[str]]:
    """Merge a declared schema with accepted reviewed constraints.

    Pending and rejected candidates are ignored. Accepted regex and uniqueness
    candidates stay recorded in the artifact but do not affect repair in v1.
    """
    if artifact is None:
        return base_schema, []
    if artifact.source_sha256 != source_sha256:
        raise ConstraintReviewError(
            "Constraint review artifact source_sha256 does not match the CSV being repaired."
        )

    accepted_schema = artifact.to_schema()
    accepted_ids = artifact.accepted_candidate_ids()
    if not accepted_ids:
        return base_schema, []
    if base_schema is None:
        return accepted_schema, accepted_ids

    conflicts: list[str] = []
    merged_columns = dict(base_schema.columns)
    accepted_by_id = {
        reviewed.candidate_id: reviewed
        for reviewed in artifact.accepted_candidates()
        if reviewed.candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS
    }

    for candidate_id, reviewed in accepted_by_id.items():
        candidate = reviewed.candidate
        if candidate.kind != "column_type" or candidate.inferred_type is None:
            continue
        column = candidate.columns[0]
        declared_type = merged_columns.get(column)
        if declared_type is not None and declared_type != candidate.inferred_type:
            conflicts.append(
                f"{candidate_id}: column '{column}' declared as {declared_type!r} "
                f"but accepted candidate infers {candidate.inferred_type!r}"
            )
            continue
        merged_columns[column] = candidate.inferred_type

    merged_fds = list(base_schema.functional_dependencies)
    fd_keys = {(fd.determinant, fd.dependent) for fd in merged_fds}
    for fd in accepted_schema.functional_dependencies:
        fd_key = (fd.determinant, fd.dependent)
        if fd_key not in fd_keys:
            merged_fds.append(fd)
            fd_keys.add(fd_key)

    merged_bounds = list(base_schema.domain_bounds)
    bound_keys = {
        (
            bound.column,
            bound.min_value,
            bound.max_value,
            bound.inclusive_min,
            bound.inclusive_max,
        )
        for bound in merged_bounds
    }
    bounds_by_column = {bound.column: bound for bound in base_schema.domain_bounds}
    for candidate_id, reviewed in accepted_by_id.items():
        candidate = reviewed.candidate
        if candidate.kind != "domain_bound":
            continue
        accepted_bound = DomainBound(
            column=candidate.columns[0],
            min_value=candidate.min_value,
            max_value=candidate.max_value,
        )
        declared_bound = bounds_by_column.get(accepted_bound.column)
        if declared_bound is not None and declared_bound != accepted_bound:
            conflicts.append(
                f"{candidate_id}: domain bound for '{accepted_bound.column}' conflicts "
                "with declared schema"
            )
            continue
        bound_key = (
            accepted_bound.column,
            accepted_bound.min_value,
            accepted_bound.max_value,
            accepted_bound.inclusive_min,
            accepted_bound.inclusive_max,
        )
        if bound_key not in bound_keys:
            merged_bounds.append(accepted_bound)
            bound_keys.add(bound_key)

    if conflicts:
        raise ConstraintReviewError(
            "Accepted constraints conflict with the declared schema: " + "; ".join(conflicts)
        )

    return (
        Schema(
            columns=merged_columns,
            functional_dependencies=tuple(merged_fds),
            pii_columns=base_schema.pii_columns,
            primary_key_columns=base_schema.primary_key_columns,
            not_null_columns=base_schema.not_null_columns,
            unique_columns=base_schema.unique_columns,
            accepted_values=base_schema.accepted_values,
            regex_constraints=base_schema.regex_constraints,
            relationships=base_schema.relationships,
            domain_bounds=tuple(merged_bounds),
            aggregate_dependencies=base_schema.aggregate_dependencies,
        ),
        accepted_ids,
    )


def _non_empty(values: list[object]) -> list[str]:
    """Return non-empty string values."""
    return [str(value).strip() for value in values if str(value).strip()]


def _try_float(value: str) -> float | None:
    """Parse a finite float or return None."""
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _infer_column_type(values: list[str]) -> tuple[str, float, str]:
    """Infer a conservative verifier-compatible column type."""
    if not values:
        return "str", 0.0, "No non-empty values were available."

    int_count = sum(1 for value in values if _INT_RE.fullmatch(value))
    float_count = sum(1 for value in values if _FLOAT_RE.fullmatch(value))
    date_count = sum(1 for value in values if _ISO_DATE_RE.fullmatch(value))
    total = len(values)

    if int_count / total >= 0.95:
        return "int", round(int_count / total, 4), f"{int_count}/{total} values parse as integers."
    if float_count / total >= 0.9:
        return (
            "float",
            round(float_count / total, 4),
            f"{float_count}/{total} values parse as floats.",
        )
    if date_count / total >= 0.9:
        return (
            "str",
            round(date_count / total, 4),
            f"{date_count}/{total} values look like ISO dates.",
        )
    return "str", 1.0, "Column is treated as string unless reviewed otherwise."


def _regex_candidate(column: str, values: list[str]) -> ConstraintCandidate | None:
    """Infer a simple regex candidate for consistent identifier-like columns."""
    if not values:
        return None
    if all(_DIGITS_RE.fullmatch(value) for value in values):
        lengths = sorted({len(value) for value in values})
        pattern = rf"^\d{{{lengths[0]}}}$" if len(lengths) == 1 else r"^\d+$"
    elif all(_UPPER_CODE_RE.fullmatch(value) for value in values):
        pattern = r"^[A-Z0-9_-]+$"
    else:
        return None
    return ConstraintCandidate(
        kind="regex",
        columns=(column,),
        pattern=pattern,
        confidence=1.0,
        evidence=f"{len(values)} non-empty values matched {pattern}.",
    )


def _domain_candidate(column: str, values: list[str]) -> ConstraintCandidate | None:
    """Infer a numeric min/max domain candidate."""
    numeric = [_try_float(value) for value in values]
    parsed = [value for value in numeric if value is not None]
    if len(parsed) < 3 or len(parsed) != len(values):
        return None
    return ConstraintCandidate(
        kind="domain_bound",
        columns=(column,),
        min_value=min(parsed),
        max_value=max(parsed),
        confidence=1.0,
        evidence=f"{len(parsed)} values define the observed numeric range.",
    )


def _unique_candidate(column: str, values: list[str]) -> ConstraintCandidate | None:
    """Infer a uniqueness candidate when every non-empty value is distinct."""
    if len(values) < 3 or len(set(values)) != len(values):
        return None
    return ConstraintCandidate(
        kind="unique",
        columns=(column,),
        confidence=1.0,
        evidence=f"{len(values)} non-empty values are distinct.",
    )


def _fd_candidates(table: TableLike, columns: list[str]) -> list[ConstraintCandidate]:
    """Infer single-column functional dependencies with violation tolerance."""
    total_rows = row_count(table)
    if total_rows < 5:
        return []

    values_by_column = {column: _non_empty(column_values(table, column)) for column in columns}
    candidates: list[ConstraintCandidate] = []
    for determinant in columns:
        determinant_values = values_by_column[determinant]
        if len(determinant_values) != total_rows:
            continue
        determinant_unique = len(set(determinant_values))
        if determinant_unique < 2 or determinant_unique == total_rows:
            continue

        for dependent in columns:
            if dependent == determinant:
                continue
            dependent_values = values_by_column[dependent]
            if len(dependent_values) != total_rows:
                continue
            groups: dict[str, list[str]] = defaultdict(list)
            for det_value, dep_value in zip(determinant_values, dependent_values, strict=True):
                groups[det_value].append(dep_value)

            violations = 0
            for group_values in groups.values():
                most_common = Counter(group_values).most_common(1)[0][1]
                violations += len(group_values) - most_common
            confidence = round(1.0 - (violations / total_rows), 4)
            if confidence < 0.9:
                continue
            candidates.append(
                ConstraintCandidate(
                    kind="functional_dependency",
                    columns=(determinant,),
                    dependent=dependent,
                    confidence=confidence,
                    evidence=(
                        f"{determinant} determined {dependent} in "
                        f"{total_rows - violations}/{total_rows} rows."
                    ),
                )
            )
    return candidates


def infer_schema(table: TableLike) -> SchemaInferenceResult:
    """Infer reviewable schema candidates from a table-like object."""
    columns = column_names(table)
    inferred_columns: dict[str, str] = {}
    candidates: list[ConstraintCandidate] = []

    for column in columns:
        values = _non_empty(column_values(table, column))
        inferred_type, confidence, evidence = _infer_column_type(values)
        inferred_columns[column] = inferred_type
        candidates.append(
            ConstraintCandidate(
                kind="column_type",
                columns=(column,),
                inferred_type=inferred_type,
                confidence=confidence,
                evidence=evidence,
            )
        )
        regex_candidate = _regex_candidate(column, values)
        if regex_candidate is not None:
            candidates.append(regex_candidate)
        domain_candidate = _domain_candidate(column, values)
        if domain_candidate is not None:
            candidates.append(domain_candidate)
        unique_candidate = _unique_candidate(column, values)
        if unique_candidate is not None:
            candidates.append(unique_candidate)

    candidates.extend(_fd_candidates(table, columns))
    return SchemaInferenceResult(
        columns=inferred_columns,
        candidates=candidates,
        row_count=row_count(table),
    )
