"""A triage score must never be able to influence a mutation.

The review ranker is safe "by construction": it is passed to ``run_repair_pipeline`` as a
separate keyword argument rather than as a field of ``RepairPipelineRequest``, and
``partition_auto_apply`` never receives it. But construction-based safety was never
*tested* under varying scores. The existing unit tests in
``tests/unit/test_review_triage_surface.py`` stub the completion function with a constant
(``lambda _m: "yes"``), so every cell receives the same score -- which means those tests
cannot detect score-dependent behaviour even in principle. A ranker that quietly reordered
or filtered fixes according to its scores would pass them.

That matters right now for a specific reason: the free ranker measured in
``eval/results/free_vs_llm_ranker.json`` is a candidate replacement for the paid one, and
swapping a scorer is only provably safe if the score cannot reach the write path. This file
supplies that proof over randomised score vectors.

The invariant, stated precisely: for a fixed table and request, the set of applied fixes and
the bytes of the source file are **identical** for every possible assignment of triage
scores, including adversarial ones (all-zero, all-one, reversed, random).
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from io import StringIO
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dataforge.review import CellScore
from dataforge.table import TableLike
from dataforge.verifier.schema import FunctionalDependency, Schema

# Chosen empirically to produce BOTH detected issues and an accepted deterministic fix.
# A fixture that produces neither makes every assertion below vacuously true, which is
# exactly how the pre-existing unit tests came to prove nothing; `_assert_non_vacuous`
# enforces this at runtime so the fixture cannot silently rot.
#
# UPDATED 2026-08-22, and the reason is worth keeping. The accepted fix used to come from
# `decimal_shift`, which auto-applied unconditionally because `partition_auto_apply` read
# `if deterministic or ...`. That bypass was removed: `decimal_shift` infers a repair from
# the column's own distribution and measured precision 0.0000 on hospital, flights and
# rayyan, so it is no longer in `CONSTRAINT_CHECKABLE_DETECTORS`. `_assert_non_vacuous`
# then fired correctly -- the fixture produced no accepted fixes at all.
#
# It is repaired here by supplying a DECLARED functional dependency instead of leaning on
# a schema-free heuristic. `state -> city` holds on every row but one, so `fd_violation`
# produces a deterministic fix that is checkable against a stated premise rather than
# guessed from a distribution. That is a better fixture than the original: it exercises
# the write path that the product actually stands behind.
_ROWS = (
    [
        {
            "id": str(i + 1),
            "state": "MA",
            "city": "boston",
            "age": str(30 + i % 5),
            "amount": "100.00",
        }
        for i in range(14)
    ]
    + [{"id": "15", "state": "MA", "city": "bostonn", "age": "30", "amount": "100.00"}]
    + [{"id": "16", "state": "MA", "city": "boston", "age": "", "amount": "100.00"}]
    + [{"id": "17", "state": "MA", "city": "boston", "age": "99999", "amount": "abc"}]
)

#: Declared premise for the fixture. Without it nothing auto-applies, which is the
#: intended behaviour of the engine and would make this test vacuous.
_SCHEMA = Schema(
    columns={
        "id": "string",
        "state": "string",
        "city": "string",
        "age": "string",
        "amount": "string",
    },
    functional_dependencies=(FunctionalDependency(determinant=("state",), dependent="city"),),
)


def _write_csv(path: Path) -> bytes:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(_ROWS[0]))
    writer.writeheader()
    writer.writerows(_ROWS)
    payload = buffer.getvalue().encode("utf-8")
    path.write_bytes(payload)
    return payload


class _ScriptedRanker:
    """A ranker that returns caller-supplied scores, to probe score sensitivity.

    Deliberately not a subclass of ``ReviewRanker``: the pipeline must depend only on the
    ``rank`` protocol, so a substitute scorer (including the free one) is interchangeable.
    """

    def __init__(self, scores: Sequence[float]) -> None:
        self._scores = list(scores)

    def rank(self, cells: Sequence[tuple[int, str]], df: TableLike) -> list[CellScore]:
        scored = [
            CellScore(row, column, self._scores[index % len(self._scores)], "heuristic")
            for index, (row, column) in enumerate(cells)
        ]
        return sorted(scored, key=lambda cell: cell.score, reverse=True)


_VOLATILE_RECEIPT_KEYS = (
    "review_ranking",
    "duration_ms",
    "elapsed_ms",
    "timestamp",
    "utc",
    "created_at",
)


def _run(tmp_path: Path, scores: Sequence[float] | None) -> tuple[dict[str, object], bytes]:
    """Run the pipeline with the given triage scores.

    Returns the **entire result** as a canonical dict with the ranking and any wall-clock
    fields removed, plus the source bytes. Comparing the whole payload rather than just
    ``fixes`` is deliberate: on a table where no fix is accepted, a fixes-only assertion is
    vacuously true, which is precisely the trap the pre-existing unit tests fell into.
    Everything the pipeline reports -- issues, failures, receipt counts, provenance,
    abstentions -- must be invariant to the scores; only the ranking may differ.

    All runs must share one directory, because receipt fields derive from the source path;
    comparing runs in different temp directories would fail for reasons unrelated to triage.

    ``mode="dry_run"`` still exercises ``partition_auto_apply``, so the invariant is tested
    where the auto-apply decision is made, not merely where bytes land.
    """
    from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

    source = tmp_path / "table.csv"
    _write_csv(source)
    request = RepairPipelineRequest(source_path=source, mode="dry_run", repair_schema=_SCHEMA)
    ranker = _ScriptedRanker(scores) if scores else None
    result = run_repair_pipeline(request, review_ranker=ranker)
    payload = result.model_dump(mode="json")
    receipt = payload.get("receipt")
    if isinstance(receipt, dict):
        for key in _VOLATILE_RECEIPT_KEYS:
            receipt.pop(key, None)
    return payload, source.read_bytes()


def _assert_non_vacuous(payload: dict[str, object]) -> None:
    """Guard against the test silently proving nothing.

    If the fixture stopped producing detected issues or accepted fixes, every comparison
    below would be trivially equal and the invariant would go unchecked. An earlier version
    of this file did exactly that -- it compared an empty fix set against an empty fix set
    and passed. Fail loudly instead.
    """
    receipt = payload.get("receipt")
    assert isinstance(receipt, dict), "result carries no receipt to compare"
    assert int(receipt.get("issues_count") or 0) >= 1, (
        "fixture produced no detected issues, so this property test is vacuous; "
        "restore a table that the detectors flag"
    )
    fixes = payload.get("fixes")
    assert isinstance(fixes, list) and fixes, (
        "fixture produced no accepted fixes, so the mutation path is never exercised; "
        "restore a table that yields at least one deterministic fix"
    )


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    scores=st.lists(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=12,
    )
)
def test_triage_scores_never_change_the_fixes(scores: list[float]) -> None:
    """No score vector may alter anything the pipeline reports except the ranking."""
    import tempfile

    with tempfile.TemporaryDirectory() as raw:
        shared = Path(raw)
        baseline, baseline_bytes = _run(shared, None)
        _assert_non_vacuous(baseline)
        scored, scored_bytes = _run(shared, scores)

    assert scored == baseline, (
        "triage scores changed the pipeline result; the ranker is no longer "
        "presentation-only and there is now a path from a score to a decision"
    )
    assert scored_bytes == baseline_bytes, "ranking mutated the source file bytes"


def test_adversarial_score_vectors_are_inert() -> None:
    """Degenerate and inverted orderings must also be inert, not merely random ones."""
    import tempfile

    adversarial: list[list[float]] = [
        [0.0],
        [1.0],
        [0.0, 1.0],
        [1.0, 0.0],
        [0.5, 0.5, 0.5],
        [1.0, 0.75, 0.5, 0.25, 0.0],
    ]
    with tempfile.TemporaryDirectory() as raw:
        shared = Path(raw)
        baseline, baseline_bytes = _run(shared, None)
        _assert_non_vacuous(baseline)
        for scores in adversarial:
            payload, source_bytes = _run(shared, scores)
            assert payload == baseline, f"score vector {scores} changed the result"
            assert source_bytes == baseline_bytes, f"score vector {scores} mutated the file"
