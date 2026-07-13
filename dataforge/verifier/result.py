"""Verifier result contract, shared across independent verifier implementations.

These types are the *output specification* of a constraint check. They live in
their own dependency-free module so that a verifier implementation can import
them WITHOUT pulling in z3 (or any particular checking mechanism). This is what
lets the independently-written ``DirectVerifier`` share the result contract with
the z3-backed ``SMTVerifier`` while sharing none of their checking logic.
"""

from __future__ import annotations

import enum

from pydantic import BaseModel, Field


class VerificationVerdict(enum.Enum):
    """Possible outcomes of the verifier gate."""

    ACCEPT = "accept"
    REJECT = "reject"
    UNKNOWN = "unknown"


class VerificationResult(BaseModel):
    """Typed result for the verifier gate."""

    verdict: VerificationVerdict
    reason: str = Field(min_length=1)
    unsat_core: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}
