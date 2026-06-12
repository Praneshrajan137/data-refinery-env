"""Shared test fixtures for the dataforge-evals test suite."""

from __future__ import annotations

import pytest

from dataforge_evals.agents.base import Fix, GroundTruthCell, Task
from dataforge_evals.agents.mock import MockAgent
from dataforge_evals.tasks import load_synthetic_task


@pytest.fixture()
def synthetic_task() -> Task:
    """Return the built-in synthetic smoke-test task."""
    return load_synthetic_task()


@pytest.fixture()
def mock_agent() -> MockAgent:
    """Return the deterministic oracle mock agent."""
    return MockAgent()


@pytest.fixture()
def sample_ground_truth() -> tuple[GroundTruthCell, ...]:
    """Return a small ground-truth set for grader tests."""
    return (
        GroundTruthCell(
            row=0, column="HospitalName", dirty_value="Mercy Hosp", clean_value="Mercy Hospital"
        ),
        GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),
        GroundTruthCell(
            row=1, column="Phone", dirty_value="not available", clean_value="217-555-0101"
        ),
    )


@pytest.fixture()
def perfect_fixes(sample_ground_truth: tuple[GroundTruthCell, ...]) -> list[Fix]:
    """Return fixes that perfectly match the sample ground truth."""
    return [
        Fix(row=cell.row, column=cell.column, new_value=cell.clean_value, reason="perfect")
        for cell in sample_ground_truth
    ]
