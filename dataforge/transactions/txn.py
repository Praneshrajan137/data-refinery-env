"""Transaction models for reversible DataForge repairs."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

TxnId = Annotated[str, Field(pattern=r"^txn-\d{4}-\d{2}-\d{2}-[0-9a-f]{6}$")]
Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


def _require_utc(value: datetime, field_name: str) -> datetime:
    """Validate that a datetime is timezone-aware UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field_name} must be UTC")
    return value


def generate_txn_id(now: datetime | None = None) -> str:
    """Generate a transaction identifier in the canonical Week 2 format.

    Args:
        now: Optional timestamp override. If omitted, current UTC time is used.

    Returns:
        A transaction identifier like ``txn-2026-04-20-a1b2c3``.
    """
    current = now or datetime.now(UTC)
    current_utc = current.astimezone(UTC)
    return f"txn-{current_utc:%Y-%m-%d}-{secrets.token_hex(3)}"


class CellFix(BaseModel):
    """A single cell mutation proposed or applied by DataForge.

    Args:
        row: Zero-indexed row number in the CSV body.
        column: Column name to update.
        old_value: The value observed before repair.
        new_value: The value to write during repair.
        detector_id: The detector / repairer family that produced the fix.
    """

    row: int = Field(ge=0, description="Zero-indexed row number")
    column: str = Field(min_length=1, description="Column name")
    old_value: str = Field(description="Original value before repair")
    new_value: str = Field(description="Replacement value after repair")
    detector_id: str = Field(min_length=1, description="Detector / repairer identifier")
    operation: Literal["update", "delete_row"] = Field(
        default="update",
        description="Repair operation kind",
    )

    model_config = {"frozen": True}


class RepairTransaction(BaseModel):
    """Audit record for a reversible repair transaction.

    Args:
        txn_id: Canonical transaction identifier.
        created_at: UTC timestamp when the transaction was recorded.
        source_path: Absolute path to the repaired source file.
        source_sha256: SHA-256 of the original source bytes.
        post_sha256: SHA-256 of the applied file bytes, once written.
        source_snapshot_path: Absolute path to the immutable source snapshot.
        fixes: Ordered list of cell fixes recorded for auditability.
        applied: Whether the journal records that the repair was applied.
        reverted_at: UTC timestamp when the transaction was reverted, if any.
    """

    txn_id: TxnId
    created_at: datetime
    source_path: str = Field(min_length=1)
    source_sha256: Sha256Hex
    post_sha256: Sha256Hex | None = None
    source_snapshot_path: str = Field(min_length=1)
    fixes: list[CellFix] = Field(default_factory=list)
    applied: bool
    reverted_at: datetime | None = None

    @field_validator("created_at")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        """Require ``created_at`` to be UTC."""
        return _require_utc(value, "created_at")

    @field_validator("reverted_at")
    @classmethod
    def _validate_reverted_at(cls, value: datetime | None) -> datetime | None:
        """Require ``reverted_at`` to be UTC when present."""
        if value is None:
            return None
        return _require_utc(value, "reverted_at")

    model_config = {"frozen": True}
