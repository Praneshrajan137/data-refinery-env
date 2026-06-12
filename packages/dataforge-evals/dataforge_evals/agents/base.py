"""Public data models and agent protocol for dataforge-evals.

This module defines the stable public contract that every agent adapter,
the grader, the harness, and external consumers depend on. Changes to
these types require a spec update and version bump.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

import pandas as pd
from pydantic import BaseModel, Field

InferabilityLabel = Literal[
    "deterministic_normalization",
    "context_derivable",
    "external_reference_required",
    "not_inferable_from_prompt",
]


class Fix(BaseModel):
    """One proposed cell repair emitted by an agent.

    A fix identifies a single cell by ``(row, column)`` and proposes
    ``new_value`` as the corrected content. The ``reason`` field is for
    human audit and observability â€” it is never used for scoring.

    Attributes:
        row: Zero-based row index in the dirty DataFrame.
        column: Column name in the canonical column set.
        new_value: Proposed corrected cell value as a string.
        reason: Human-readable rationale for the repair proposal.
    """

    row: int = Field(ge=0, description="Zero-based row index in the dirty DataFrame.")
    column: str = Field(min_length=1, description="Column name in the canonical column set.")
    new_value: str = Field(description="Proposed corrected cell value as a string.")
    reason: str = Field(
        default="agent proposal",
        min_length=1,
        description="Human-readable rationale for the repair proposal.",
    )

    model_config = {"frozen": True}

    def __repr__(self) -> str:
        return f"Fix(row={self.row}, column={self.column!r}, new_value={self.new_value!r})"


class GroundTruthCell(BaseModel):
    """One canonical dirty-to-clean cell correction used for grading.

    Ground-truth cells are computed from aligned dirty/clean DataFrames
    and represent the authoritative answer the grader scores against.

    Attributes:
        row: Zero-based row index in the aligned DataFrames.
        column: Column name from the canonical (clean) column set.
        dirty_value: The original incorrect cell content.
        clean_value: The authoritative corrected cell content.
    """

    row: int = Field(ge=0, description="Zero-based row index.")
    column: str = Field(min_length=1, description="Canonical column name.")
    dirty_value: str = Field(description="Original incorrect cell content.")
    clean_value: str = Field(description="Authoritative corrected cell content.")

    model_config = {"frozen": True}

    def __repr__(self) -> str:
        return (
            f"GroundTruthCell(row={self.row}, column={self.column!r}, "
            f"dirty={self.dirty_value!r} -> clean={self.clean_value!r})"
        )


@dataclass(frozen=True, kw_only=True)
class AgentTask:
    """A label-hidden data-quality repair task passed to normal agents.

    This is the public runtime view of an evaluation task. It deliberately
    omits ground-truth labels; the harness keeps labels separately for grading.

    Attributes:
        name: Human-readable task identifier (e.g. ``"hospital"``).
        dirty_df: The DataFrame containing data-quality issues.
        canonical_columns: Ordered column names from the clean reference.
        metadata: Provenance and descriptive metadata for reporting.
    """

    name: str
    dirty_df: pd.DataFrame
    canonical_columns: tuple[str, ...]
    metadata: dict[str, str | int | float | tuple[str, ...]]
    inferability: InferabilityLabel = "deterministic_normalization"


@dataclass(frozen=True, kw_only=True)
class Task(AgentTask):
    """Full grading task retained inside the harness and oracle tests only.

    Normal agents receive ``AgentTask``. Only adapters explicitly marked with
    ``uses_ground_truth = True`` receive this full task.
    """

    ground_truth: tuple[GroundTruthCell, ...]


class Usage(BaseModel):
    """Provider usage accounting for one agent run.

    Tracks raw API call counts, token consumption, and a provider-normalized
    free-tier quota fraction. The ``quota_units`` field represents a
    fraction of the provider's free-tier allocation consumed, enabling
    cross-provider cost comparison on a common scale.

    Attributes:
        calls: Number of HTTP requests made to the provider.
        prompt_tokens: Total prompt/input tokens consumed.
        completion_tokens: Total completion/output tokens consumed.
        quota_units: Provider-normalized free-tier fraction consumed.
    """

    calls: int = Field(default=0, ge=0, description="Number of HTTP requests made.")
    prompt_tokens: int = Field(default=0, ge=0, description="Total prompt/input tokens.")
    completion_tokens: int = Field(default=0, ge=0, description="Total completion/output tokens.")
    quota_units: float = Field(
        default=0.0, ge=0.0, description="Provider-normalized quota fraction."
    )

    model_config = {"frozen": True}

    def __repr__(self) -> str:
        return (
            f"Usage(calls={self.calls}, prompt={self.prompt_tokens}, "
            f"completion={self.completion_tokens}, quota={self.quota_units:.4f})"
        )

    def __add__(self, other: Usage) -> Usage:
        """Accumulate usage across multiple API calls within a single run.

        Args:
            other: Another Usage instance to merge.

        Returns:
            A new Usage with summed fields.
        """
        return Usage(
            calls=self.calls + other.calls,
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            quota_units=round(self.quota_units + other.quota_units, 4),
        )


class AgentRunResult(BaseModel):
    """Normalized result returned by built-in adapters.

    Wraps the agent's proposed fixes alongside usage accounting,
    step count, and optional model identification for reproducibility.

    Attributes:
        fixes: Ordered list of proposed cell repairs.
        usage: Provider usage accounting for this run.
        steps: Number of reasoning steps the agent performed.
        model: Provider model identifier for reproducibility.
        warnings: Non-fatal diagnostic messages from the adapter.
    """

    fixes: list[Fix]
    usage: Usage = Field(default_factory=Usage)
    steps: int = Field(default=1, ge=0, description="Reasoning steps performed.")
    model: str | None = Field(default=None, description="Provider model identifier.")
    warnings: list[str] = Field(default_factory=list)


@runtime_checkable
class Agent(Protocol):
    """Protocol implemented by every data-quality repair agent adapter.

    Any object with a ``name`` attribute and a ``run`` method matching
    this signature can be used as an agent in the evaluation harness.
    The agent receives a ``Task`` and returns proposed ``Fix`` objects.
    Agents must never set their own metrics â€” the grader is the sole
    source of truth.

    Example:
        >>> class MyAgent:
        ...     name = "my-agent"
        ...     def run(self, task: Task) -> list[Fix]:
        ...         return [Fix(row=0, column="Score", new_value="4.5")]
    """

    name: str

    def run(self, task: AgentTask) -> list[Fix] | AgentRunResult:
        """Run the agent on a task and return proposed fixes.

        Args:
            task: The data-quality repair task to evaluate.

        Returns:
            A list of Fix objects or an AgentRunResult with usage accounting.
        """
        ...
