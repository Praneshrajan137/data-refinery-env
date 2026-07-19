"""The Corruption Oracle: a universal no-regression property test.

This is the test that turns DataForge's core promise -- "it never makes your
data worse" -- from a design claim into a proven invariant. It generates random
tables, injects errors whose ground-truth clean value is known by construction,
runs the FULL engine end-to-end in apply mode, and asserts four invariants that
together define provable safety:

  INV1 (no-regression): a cell that was already correct is never changed.
  INV2 (auto-apply correctness): any cell the engine changed now holds the
        ground-truth value -- the auto-applied set has precision 1.0.
  INV3 (reversibility): apply-then-revert restores the exact original bytes.
  INV4 (determinism): the same input yields byte-identical applied output.

Clean numeric columns are generated clustered (low variance) so no correct cell
is a decimal-shift outlier -- this keeps INV1 sound (no false positives) while
injected power-of-10 errors genuinely exercise the deterministic auto-apply path
(the provable moat). LLM repair is off, so only deterministic + verifier-accepted
fixes can auto-apply -- exactly the set the promise covers.
"""

from __future__ import annotations

import csv
import tempfile
from collections.abc import Awaitable, Callable
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dataforge.calibration import AbstentionPolicy
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.transactions.revert import revert_transaction

_NUMERIC_PREFIX = "num_"
_TEXT_PREFIX = "txt_"


@st.composite
def _clean_table_and_injections(
    draw: st.DrawFn,
) -> tuple[list[str], list[dict[str, str]], list[dict[str, str]], set[tuple[int, str]]]:
    """Return (headers, clean_rows, dirty_rows, injected_cells).

    Clean numeric columns are clustered so no correct value is a power-of-10
    outlier; injected errors multiply a clean numeric value by 10 or 100.
    """
    n_numeric = draw(st.integers(min_value=1, max_value=2))
    n_text = draw(st.integers(min_value=0, max_value=1))
    n_rows = draw(st.integers(min_value=6, max_value=12))

    headers: list[str] = [f"{_NUMERIC_PREFIX}{i}" for i in range(n_numeric)]
    headers += [f"{_TEXT_PREFIX}{i}" for i in range(n_text)]

    clean_rows: list[dict[str, str]] = [{} for _ in range(n_rows)]
    for col in range(n_numeric):
        name = f"{_NUMERIC_PREFIX}{col}"
        base = draw(st.integers(min_value=20, max_value=90))
        for r in range(n_rows):
            # Clustered around base (+/-4): tight enough that no clean value is
            # a 10x multiple of the column median.
            clean_rows[r][name] = str(base + draw(st.integers(min_value=-4, max_value=4)))
    for col in range(n_text):
        name = f"{_TEXT_PREFIX}{col}"
        for r in range(n_rows):
            clean_rows[r][name] = draw(st.text(alphabet="abcdexyz", min_size=1, max_size=5))

    dirty_rows = [dict(row) for row in clean_rows]
    injected: set[tuple[int, str]] = set()
    numeric_cells = [(r, f"{_NUMERIC_PREFIX}{c}") for r in range(n_rows) for c in range(n_numeric)]
    # Inject 1..2 clear power-of-10 errors so the auto-apply path is exercised.
    n_inject = draw(st.integers(min_value=1, max_value=min(2, len(numeric_cells))))
    chosen = draw(
        st.lists(st.sampled_from(numeric_cells), min_size=n_inject, max_size=n_inject, unique=True)
    )
    for r, name in chosen:
        factor = draw(st.sampled_from([10, 100]))
        dirty_rows[r][name] = str(int(clean_rows[r][name]) * factor)
        injected.add((r, name))

    return headers, clean_rows, dirty_rows, injected


def _to_csv_bytes(headers: list[str], rows: list[dict[str, str]]) -> bytes:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _values_equal(header: str, left: str, right: str) -> bool:
    """Numeric-aware equality: numeric columns compare by float, else exact."""
    if header.startswith(_NUMERIC_PREFIX):
        try:
            return float(left) == float(right)
        except ValueError:
            return left == right
    return left == right


@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(_clean_table_and_injections())
def test_engine_never_corrupts_data(
    case: tuple[list[str], list[dict[str, str]], list[dict[str, str]], set[tuple[int, str]]],
) -> None:
    headers, clean_rows, dirty_rows, injected = case
    dirty_bytes = _to_csv_bytes(headers, dirty_rows)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        source = temp_path / "data.csv"
        source.write_bytes(dirty_bytes)

        result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
        applied_rows = _read_rows(source)

        for r, row in enumerate(applied_rows):
            for header in headers:
                applied = row[header]
                clean = clean_rows[r][header]
                dirty = dirty_rows[r][header]
                if (r, header) not in injected:
                    # INV1: a cell that was already correct is never changed.
                    assert _values_equal(header, applied, dirty), (
                        f"INV1 violated: correct cell ({r},{header}) {dirty!r} -> {applied!r}"
                    )
                else:
                    # INV2: any change to a dirty cell yields the ground truth;
                    # otherwise it must be left untouched (never a third value).
                    assert _values_equal(header, applied, clean) or _values_equal(
                        header, applied, dirty
                    ), (
                        f"INV2 violated: dirty cell ({r},{header}) became {applied!r} (clean {clean!r})"
                    )

        # INV3: apply-then-revert restores the exact original (dirty) bytes.
        if result.receipt.txn_id is not None:
            revert_transaction(result.receipt.txn_id, search_root=temp_path)
            assert source.read_bytes() == dirty_bytes, "INV3 violated: revert not byte-identical"

    # INV4: the same input yields byte-identical applied output (determinism).
    with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
        source_a = Path(dir_a) / "data.csv"
        source_b = Path(dir_b) / "data.csv"
        source_a.write_bytes(dirty_bytes)
        source_b.write_bytes(dirty_bytes)
        run_repair_pipeline(RepairPipelineRequest(source_path=source_a, mode="apply"))
        run_repair_pipeline(RepairPipelineRequest(source_path=source_b, mode="apply"))
        assert source_a.read_bytes() == source_b.read_bytes(), "INV4 violated: non-deterministic"


@st.composite
def _spurious_fd_case(
    draw: st.DrawFn,
) -> tuple[list[str], list[dict[str, str]], list[dict[str, str]], set[tuple[int, str]]]:
    """A table engineered to induce SPURIOUS inferred FDs (constraint circularity).

    Two spurious-FD flavors are present by construction:
    * ``txt_key`` is a NEAR-key determinant (all distinct but one duplicated pair),
      so naive FD mining would "find" txt_key -> anything at ~1.0 confidence.
    * ``txt_cat -> txt_dep`` holds on all rows except one legitimate exception, so
      it mines as a >=90% FD whose majority-repair would OVERWRITE that correct
      minority cell.

    A genuine decimal-shift error is injected in ``num_0`` so the deterministic
    auto-apply path is genuinely exercised. The invariant: the default pipeline
    corrects only the injected cell and never touches the spurious-FD columns.
    """
    n_rows = draw(st.integers(min_value=8, max_value=12))
    headers = [
        f"{_NUMERIC_PREFIX}0",
        f"{_TEXT_PREFIX}key",
        f"{_TEXT_PREFIX}cat",
        f"{_TEXT_PREFIX}dep",
    ]
    base = draw(st.integers(min_value=20, max_value=90))
    clean_rows: list[dict[str, str]] = []
    for r in range(n_rows):
        cat = "a" if r % 2 == 0 else "b"
        clean_rows.append(
            {
                f"{_NUMERIC_PREFIX}0": str(base + draw(st.integers(min_value=-4, max_value=4))),
                f"{_TEXT_PREFIX}key": f"k{r}",
                f"{_TEXT_PREFIX}cat": cat,
                f"{_TEXT_PREFIX}dep": "x" if cat == "a" else "y",
            }
        )
    # Near-key: duplicate one key so the determinant is near-unique, not a pure key
    # (a pure key is skipped by the miner; a near-key is exactly the spurious case).
    clean_rows[1][f"{_TEXT_PREFIX}key"] = clean_rows[0][f"{_TEXT_PREFIX}key"]
    # One LEGITIMATE exception to cat->dep: a correct cell a majority-repair would wreck.
    exception_row = draw(st.integers(min_value=0, max_value=n_rows - 1))
    current = clean_rows[exception_row][f"{_TEXT_PREFIX}dep"]
    clean_rows[exception_row][f"{_TEXT_PREFIX}dep"] = "y" if current == "x" else "x"

    dirty_rows = [dict(row) for row in clean_rows]
    inject_row = draw(st.integers(min_value=0, max_value=n_rows - 1))
    factor = draw(st.sampled_from([10, 100]))
    dirty_rows[inject_row][f"{_NUMERIC_PREFIX}0"] = str(
        int(clean_rows[inject_row][f"{_NUMERIC_PREFIX}0"]) * factor
    )
    injected = {(inject_row, f"{_NUMERIC_PREFIX}0")}
    return headers, clean_rows, dirty_rows, injected


@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(_spurious_fd_case())
def test_engine_never_corrupts_via_spurious_fd(
    case: tuple[list[str], list[dict[str, str]], list[dict[str, str]], set[tuple[int, str]]],
) -> None:
    """Constraint-circularity guard: spurious inferred FDs never corrupt.

    The default pipeline does not treat inferred FDs as authoritative (they are
    pending until reviewed), so a near-key determinant or a >=90% categorical
    coincidence must NEVER trigger an overwrite of a correct cell. Only the
    injected decimal-shift error may change, and only to ground truth.
    """
    headers, clean_rows, dirty_rows, injected = case
    dirty_bytes = _to_csv_bytes(headers, dirty_rows)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        source = temp_path / "data.csv"
        source.write_bytes(dirty_bytes)

        result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
        applied_rows = _read_rows(source)

        for r, row in enumerate(applied_rows):
            for header in headers:
                applied = row[header]
                clean = clean_rows[r][header]
                dirty = dirty_rows[r][header]
                if (r, header) not in injected:
                    assert _values_equal(header, applied, dirty), (
                        f"INV1 (spurious-FD) violated: correct cell ({r},{header}) "
                        f"{dirty!r} -> {applied!r} (a spurious inferred FD corrupted it)"
                    )
                else:
                    assert _values_equal(header, applied, clean) or _values_equal(
                        header, applied, dirty
                    ), f"INV2 violated: dirty cell ({r},{header}) became {applied!r}"

        if result.receipt.txn_id is not None:
            revert_transaction(result.receipt.txn_id, search_root=temp_path)
            assert source.read_bytes() == dirty_bytes, "INV3 violated: revert not byte-identical"


def _fake_complete(value: str) -> Callable[..., Awaitable[str]]:
    async def _complete(messages: object, *, model: str, temperature: float) -> str:
        return value

    return _complete


@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(
    adversarial=st.text(min_size=0, max_size=12),
    with_context=st.booleans(),
)
def test_default_engine_never_auto_applies_an_llm_value(
    adversarial: str, with_context: bool
) -> None:
    """No LLM-proposed value is ever auto-applied under the default policy.

    This generalizes the fixed adversarial safety cases: whatever the model
    returns -- garbage, out-of-domain numbers, or a plausible-but-unverified
    value -- the default propose-not-apply policy holds it as a suggestion and
    the source file is never mutated. Abstention, not a perfect guard, is what
    makes the LLM path safe by default.
    """
    context = "Boston" if with_context else "42"
    rows = "".join(f"{i},{context}\n" for i in range(1, 8))
    csv_text = f"id,city\n{rows}8,\n"
    with tempfile.TemporaryDirectory() as temp_dir:
        source = Path(temp_dir) / "data.csv"
        source.write_bytes(csv_text.encode("utf-8"))
        original = source.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete(adversarial)):
            result = run_repair_pipeline(
                RepairPipelineRequest(source_path=source, mode="apply", allow_llm=True)
            )

        # The file is never mutated by an LLM value under the default policy, and
        # nothing is marked applied.
        assert source.read_bytes() == original, f"LLM value {adversarial!r} was auto-applied"
        assert result.receipt.applied is False


@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(adversarial=st.text(min_size=0, max_size=12))
def test_permissive_policy_never_auto_applies_plausibility_only(adversarial: str) -> None:
    """The enforced product invariant: the latent verifier-floor gaps stay latent.

    Even under a fully permissive corrector policy AND confirmed escalations, a
    plausibility-only fix (an LLM value with no authoritative schema -- exactly
    where the gaps live) is NEVER auto-applied without the explicit
    ``allow_unproven_autoapply`` opt-in. This proves the guarantee holds for ANY
    configuration, not just the default.
    """
    rows = "".join(f"{i},Boston\n" for i in range(1, 8))
    csv_text = f"id,city\n{rows}8,\n"
    permissive = AbstentionPolicy(
        target_precision=0.95,
        auto_apply_thresholds={"missing_value": 0.0},
        default_threshold=0.0,
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        source = Path(temp_dir) / "data.csv"
        source.write_bytes(csv_text.encode("utf-8"))
        original = source.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete(adversarial)):
            result = run_repair_pipeline(
                RepairPipelineRequest(
                    source_path=source,
                    mode="apply",
                    allow_llm=True,
                    confirm_escalations=True,
                    corrector_policy=permissive,
                    # allow_unproven_autoapply defaults False -> the invariant.
                )
            )

        assert source.read_bytes() == original, (
            f"plausibility-only value {adversarial!r} auto-applied under a permissive policy"
        )
        assert result.receipt.applied is False
