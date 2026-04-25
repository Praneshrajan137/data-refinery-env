"""Benchmark gate for the Week 3 SMT verifier."""

from __future__ import annotations

import math

import pandas as pd

from dataforge.detectors.base import DomainBound, FunctionalDependency, Schema
from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier import SchemaToSMT, VerificationVerdict


def _p95(samples: list[float]) -> float:
    sorted_samples = sorted(samples)
    index = max(0, math.ceil(0.95 * len(sorted_samples)) - 1)
    return sorted_samples[index]


def test_smt_verifier_p95_under_two_hundred_milliseconds(benchmark: object) -> None:
    rows = 1000
    df = pd.DataFrame(
        {
            "code": ["A"] * rows,
            "name": ["Alpha"] * rows,
            "state": ["IL"] * rows,
            "amount": ["100"] * rows,
        }
    )
    schema = Schema(
        columns={"code": "str", "name": "str", "state": "str", "amount": "float"},
        functional_dependencies=[
            FunctionalDependency(determinant=["code"], dependent="name"),
            FunctionalDependency(determinant=["name"], dependent="state"),
        ],
        domain_bounds=[DomainBound(column="amount", min_value=0.0, max_value=5000.0)],
    )
    verifier = SchemaToSMT(schema, df)
    fix = ProposedFix(
        fix=CellFix(
            row=rows - 1,
            column="amount",
            old_value="100",
            new_value="99",
            detector_id="decimal_shift",
        ),
        reason="candidate",
        confidence=0.9,
        provenance="deterministic",
    )

    result = benchmark.pedantic(lambda: verifier.verify_fix(fix), rounds=40, iterations=1)

    assert result.verdict == VerificationVerdict.ACCEPT
    stats = benchmark.stats.stats
    assert _p95(list(stats.data)) < 0.2
