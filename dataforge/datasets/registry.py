"""Canonical metadata for real-world benchmark datasets."""

from __future__ import annotations

from pydantic import BaseModel, Field


class HeaderMismatch(BaseModel):
    """Pair of dirty/clean header names that align by column position."""

    dirty_name: str = Field(min_length=1)
    clean_name: str = Field(min_length=1)

    model_config = {"frozen": True}


class DatasetMetadata(BaseModel):
    """Metadata describing a canonical benchmark dataset."""

    name: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    n_rows: int = Field(ge=0)
    n_columns: int = Field(ge=1)
    error_types: tuple[str, ...] = Field(default_factory=tuple)
    source_urls: tuple[str, str]
    source_revision: str = Field(min_length=7)
    dirty_sha256: str = Field(min_length=64, max_length=64)
    clean_sha256: str = Field(min_length=64, max_length=64)
    citation: str = Field(min_length=1)
    header_mismatches: tuple[HeaderMismatch, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


RAHA_GIT_REVISION = "7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c"
_BASE_URL = f"https://raw.githubusercontent.com/BigDaMa/raha/{RAHA_GIT_REVISION}/datasets"

DATASET_REGISTRY: dict[str, DatasetMetadata] = {
    "hospital": DatasetMetadata(
        name="hospital",
        domain="healthcare",
        n_rows=1000,
        n_columns=20,
        error_types=("typo", "missing_value", "formatting"),
        source_urls=(
            f"{_BASE_URL}/hospital/dirty.csv",
            f"{_BASE_URL}/hospital/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="dbc5575b915fe8b5e0ac6dc6172f38ba91e611fdb76d09a8f4a81cb7ea9925ac",
        clean_sha256="ea3ee44998455c0b491750c348509de176c758a3bbf58e4530c0a136bb248b4b",
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Hospital) via the BigDaMa/raha repository."
        ),
    ),
    "flights": DatasetMetadata(
        name="flights",
        domain="aviation",
        n_rows=2376,
        n_columns=7,
        error_types=("missing_value", "formatting", "datetime"),
        source_urls=(
            f"{_BASE_URL}/flights/dirty.csv",
            f"{_BASE_URL}/flights/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="1b5c1afa10aa0e7c20fd7e14d05c56772715b2771aa0f5fa67ed1709e1eecd46",
        clean_sha256="0acfcfd8985b06fdd363965c9e8d9522c43e7589a93d79ae7dc311e1c37fdf3b",
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Flights) via the BigDaMa/raha repository."
        ),
    ),
    "rayyan": DatasetMetadata(
        name="rayyan",
        domain="bibliographic",
        n_rows=1000,
        n_columns=11,
        error_types=("typo", "missing_value", "formatting"),
        source_urls=(
            f"{_BASE_URL}/rayyan/dirty.csv",
            f"{_BASE_URL}/rayyan/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="7e25e6db262b0c72ca2d9735d5959599cf5a582e1c705459507c7b45d0d1d174",
        clean_sha256="23159f43c0706782388ed8957ad0c74eb7b88bc98f34d65bd49296e186d4673f",
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Rayyan) via the BigDaMa/raha repository."
        ),
    ),
    "tax": DatasetMetadata(
        name="tax",
        domain="finance",
        n_rows=200000,
        n_columns=15,
        error_types=("typo", "formatting", "rule_violation"),
        source_urls=(
            f"{_BASE_URL}/tax/dirty.csv",
            f"{_BASE_URL}/tax/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="8dd3429ec4791b2ed1a688c308a57a9f3d1a94f77d1f4e98294a67273270b973",
        clean_sha256="201290927ae92e65b3940d776b3df5b4d953c5dfd9abb231715a2e65ecca87b0",
        citation=("Mahdavi et al. Raha benchmark dataset (Tax) via the BigDaMa/raha repository."),
    ),
}


def get_dataset_metadata(name: str) -> DatasetMetadata:
    """Return canonical metadata for a named benchmark dataset.

    Args:
        name: Canonical dataset name.

    Returns:
        The immutable metadata entry for the dataset.

    Raises:
        KeyError: If the dataset is not registered.
    """
    return DATASET_REGISTRY[name]
