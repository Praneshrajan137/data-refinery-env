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
    citation: str = Field(min_length=1)
    header_mismatches: tuple[HeaderMismatch, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


_BASE_URL = "https://raw.githubusercontent.com/BigDaMa/raha/refs/heads/master/datasets"

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
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Flights) via the BigDaMa/raha repository."
        ),
    ),
    "beers": DatasetMetadata(
        name="beers",
        domain="consumer",
        n_rows=2410,
        n_columns=11,
        error_types=("formatting", "missing_value", "normalization"),
        source_urls=(
            f"{_BASE_URL}/beers/dirty.csv",
            f"{_BASE_URL}/beers/clean.csv",
        ),
        citation=("Mahdavi et al. Raha benchmark dataset (Beers) via the BigDaMa/raha repository."),
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
