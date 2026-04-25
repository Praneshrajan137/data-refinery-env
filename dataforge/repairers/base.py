"""Core models and protocol for DataForge repairers."""

from __future__ import annotations

from typing import Literal, Protocol

import pandas as pd
from pydantic import BaseModel, Field

from dataforge.detectors.base import Issue, Schema
from dataforge.transactions.txn import CellFix

ProvenanceLiteral = Literal["deterministic", "llm_cache", "llm_live"]
AttemptStatusLiteral = Literal[
    "accepted",
    "denied",
    "escalated",
    "rejected",
    "unknown",
    "attempted_not_fixed",
]


class ProposedFix(BaseModel):
    """A repair proposal emitted by a repairer.

    Args:
        fix: The cell mutation to apply.
        reason: Human-readable explanation of why this repair is proposed.
        confidence: Repair confidence in the range [0.0, 1.0].
        provenance: Where the proposed value came from.
    """

    fix: CellFix
    reason: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    provenance: ProvenanceLiteral

    model_config = {"frozen": True}


class RepairAttempt(BaseModel):
    """Recorded outcome for one issue-repair attempt."""

    issue: Issue
    attempt_number: int = Field(ge=1)
    fix: ProposedFix | None = None
    status: AttemptStatusLiteral
    reason: str = Field(min_length=1)
    unsat_core: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


class RetryContext(BaseModel):
    """Hints passed back to a repairer after a failed attempt."""

    issue: Issue
    previous_attempts: tuple[RepairAttempt, ...] = Field(default_factory=tuple)
    rejected_values: frozenset[str] = Field(default_factory=frozenset)
    hints: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


class Repairer(Protocol):
    """Structural protocol implemented by every repairer."""

    def propose(
        self,
        issue: Issue,
        df: pd.DataFrame,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Return a repair proposal for an issue, or ``None`` if unavailable."""
        ...  # pragma: no cover
