"""The Corruption Oracle: a no-regression property test over CLUSTERED numeric columns.

RETRACTION (2026-08-22): this file previously called itself "a universal no-regression
property test" and claimed to turn "it never makes your data worse" into a proven
invariant. **"Universal" was false, and the falsehood was load-bearing.** The clean
numeric columns below are generated clustered (low variance) *specifically so that no
correct cell is a decimal-shift outlier* -- see the note at the end of this docstring,
which said so plainly. The precondition that makes `decimal_shift` sound was therefore a
property of this FIXTURE rather than a check in the CODE, and INV1 was proven only over
data constructed to satisfy it.

Real warehouse columns do not satisfy it. Measured log-space IQR: `orders.o_totalprice`
0.44 dex, `lineitem.l_extendedprice` 0.47, `customer.c_acctbal` 0.62,
`QUERY_HISTORY.total_elapsed_time` 0.48. At that spread a 10x offset is ordinary, and the
ungated detector would have rewritten 263,428 correct monetary values on error-free
TPC-H. A live `dataforge repair --apply` turned `1131.20` into `113120`. See
`docs/trust/deterministic-is-not-sound.md`.

**Also note what this file can no longer prove.** INV1 ("a correct cell is never changed")
and INV2 ("a changed cell holds ground truth") are BOTH satisfied by changing nothing. The
schema-free auto-apply path is now empty by design, so on that path these invariants hold
trivially. They are not the guard for it. `tests/property/test_clean_data_is_not_flagged.py`
carries the invariant that can actually fail there, plus a non-vacuity anchor proving the
write path is still alive under a declared premise.

What this file still earns, stated honestly: over clustered numeric columns -- where a
power-of-10 offset genuinely is anomalous -- the engine corrects only injected cells,
never invents a third value, reverts byte-for-byte, and is deterministic.

  INV1 (no-regression): a cell that was already correct is never changed.
  INV2 (auto-apply correctness): any cell the engine changed now holds the
        ground-truth value -- the auto-applied set has precision 1.0.
  INV3 (reversibility): apply-then-revert restores the exact original bytes.
  INV4 (determinism): the same input yields byte-identical applied output.

Clean numeric columns are generated clustered (low variance) so no correct cell is a
decimal-shift outlier. That is now the *enforced* precondition of the detector rather than
an assumption of this fixture: `dataforge/detectors/decimal_shift.py` abstains when a
power-of-10 offset falls inside 3 log-IQRs of the median. LLM repair is off, so only
deterministic + verifier-accepted fixes can auto-apply -- exactly the set the promise
covers.
"""

from __future__ import annotations

import csv
import tempfile
from collections.abc import Awaitable, Callable
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dataforge.agent import AgentRepairRequest, make_policy, run_agent_repair
from dataforge.calibration import AbstentionPolicy
from dataforge.engine.repair import (
    ExternalFix,
    RepairPipelineRequest,
    UnprovenWriteError,
    VerifyAndApplyRequest,
    apply_transaction,
    run_repair_pipeline,
    verify_and_apply,
)
from dataforge.repairers.base import ProposedFix
from dataforge.stores.base import TableStoreError
from dataforge.stores.patch_plan import (
    PatchOperation,
    PatchPlan,
    RowIdentity,
    enforce_plan_proven_only,
)
from dataforge.transactions.revert import revert_transaction
from dataforge.transactions.txn import CellFix

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
    ``allow_unproven_autoapply`` opt-in.

    Scope, stated precisely: this varies the CORRECTOR POLICY on ONE surface
    (``run_repair_pipeline``). It previously claimed to prove the guarantee "for ANY
    configuration", which was false in a way that mattered -- the guarantee did not
    hold on the agent or table-store surfaces, and this test could not have detected
    that because it never called them. Surface coverage is
    ``test_every_write_surface_enforces_proven_only`` below.
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


# ── Surface coverage for the proven-only invariant ──────────────────────────────
#
# DECISIONS.md 2026-07-11 declared proven-only auto-apply an invariant holding "under
# any policy". The gate was plumbed into engine/repair.py only, and its two callers
# (run_repair_pipeline, verify_and_apply) were the only paths that honoured it. The
# agent controller and run_table_store_repair each reached a mutation primitive
# directly, so an LLM value could be written having cleared no value-level check.
#
# The invariant is now enforced INSIDE the mutation primitives, and these tests
# parametrize over the surfaces rather than over one surface's policy. A new surface
# inherits the gate by construction; if someone adds one that does not, the primitive
# raises rather than writing.


def _unproven_fix() -> ProposedFix:
    """An LLM-derived fix: untrusted provenance, so unproven without a schema."""
    return ProposedFix(
        fix=CellFix(
            row=1, column="score", old_value="abc", new_value="30", detector_id="type_mismatch"
        ),
        reason="llm proposal",
        confidence=0.99,
        provenance="llm_live",
    )


@settings(max_examples=25, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(new_value=st.text(min_size=1, max_size=8).filter(lambda s: s.strip() != ""))
def test_write_primitive_refuses_any_unproven_value(new_value: str) -> None:
    """``apply_transaction`` itself refuses, so no caller can bypass the gate.

    This is the structural property. The previous design enforced proven-only at the
    calling surface, which meant safety depended on each caller remembering to
    partition first -- and two of them did not.
    """
    fix = _unproven_fix().model_copy(
        update={"fix": _unproven_fix().fix.model_copy(update={"new_value": new_value})}
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        source = Path(temp_dir) / "data.csv"
        source.write_bytes(b"id,score\n1,10\n2,abc\n")
        original = source.read_bytes()

        try:
            apply_transaction(source, [fix], original)
        except UnprovenWriteError:
            pass
        else:  # pragma: no cover - this is the failure we are asserting against
            raise AssertionError(f"unproven value {new_value!r} was written to disk")

        assert source.read_bytes() == original


class TestEveryWriteSurfaceEnforcesProvenOnly:
    """Each surface that can mutate user data must hold an unproven LLM value."""

    def test_pipeline_surface_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "data.csv"
            rows = "".join(f"{i},Boston\n" for i in range(1, 8))
            source.write_bytes(f"id,city\n{rows}8,\n".encode())
            original = source.read_bytes()

            with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("Atlantis")):
                result = run_repair_pipeline(
                    RepairPipelineRequest(
                        source_path=source,
                        mode="apply",
                        allow_llm=True,
                        confirm_escalations=True,
                    )
                )

            assert result.receipt.applied is False
            assert source.read_bytes() == original

    def test_verify_and_apply_surface_holds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "data.csv"
            source.write_bytes(b"id,score\n1,10\n2,abc\n")
            original = source.read_bytes()

            result = verify_and_apply(
                VerifyAndApplyRequest(
                    source_path=source,
                    fixes=[ExternalFix(row=1, column="score", new_value="30")],
                    mode="apply",
                    confirm_escalations=True,
                )
            )

            assert result.receipt.applied is False
            assert source.read_bytes() == original

    def test_agent_surface_holds(self) -> None:
        responses = [
            '{"action_type":"FIX","row":2,"column":"score",'
            '"new_value":"30","justification":"guess"}',
            '{"action_type":"FINALIZE"}',
        ]
        state = {"i": 0}

        def _complete(messages, model, temperature):  # noqa: ANN001, ANN202
            index = state["i"]
            state["i"] = min(index + 1, len(responses) - 1)
            return responses[index]

        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "data.csv"
            source.write_bytes(b"id,score\n1,10\n2,20\n3,abc\n")
            original = source.read_bytes()

            result = run_agent_repair(
                AgentRepairRequest(
                    source_path=source,
                    mode="apply",
                    policy="hosted",
                    max_steps=4,
                    confirm_escalations=True,
                ),
                policy=make_policy("hosted", completion_override=_complete),
            )

            assert result.applied is False
            assert source.read_bytes() == original

    def test_watch_surface_has_no_llm_path_at_all(self) -> None:
        """``dataforge watch --apply`` is safe by construction, not by the gate.

        Watch re-applies on EVERY mtime change, in a loop, so it is the
        highest-frequency write surface in the product, and Round 1 never enumerated it.
        Investigating it produced a better answer than adding it to the gate parametrization:
        ``_repair_once`` builds a ``RepairPipelineRequest`` with no ``allow_llm`` and no
        agent option, and the field defaults to ``False``, so this surface **cannot
        produce an untrusted fix in the first place**. Its exposure is structurally zero.

        A first version of this test ran ``_repair_once`` with a patched corrector and
        asserted the bytes were unchanged. That assertion held for a trivial reason -- the
        corrector was never called -- and it SURVIVED having the gate removed, which is
        the definition of a worthless test. Pinning the real property instead: if someone
        adds an LLM or agent option to watch, this test fails and they must add watch to
        the gate parametrization above.
        """
        import importlib

        # Resolve the MODULE explicitly. ``dataforge.cli.__init__`` binds the name
        # ``watch`` to a function, which shadows the submodule -- so both
        # ``from dataforge.cli import watch`` and ``import dataforge.cli.watch as w``
        # silently yield the function, and a source scan then reads only its body.
        watch_module = importlib.import_module("dataforge.cli.watch")
        source = Path(str(watch_module.__file__)).read_text(encoding="utf-8")
        assert "allow_llm" not in source, (
            "watch now has an LLM path. It can therefore produce plausibility_only "
            "fixes, so add it to the surface parametrization above and re-verify by "
            "mutation that the new test actually detects an ungated write."
        )
        assert "run_agent_repair" not in source, (
            "watch now has an agent path -- same requirement as above."
        )
        # And confirm the only write it can reach is the gated pipeline.
        assert "run_repair_pipeline" in source

    def test_dbt_surface_reaches_the_gated_store_path(self) -> None:
        """The dbt package's ``mode="apply"`` path must go through the gated store API.

        ``packages/dataforge-dbt`` calls ``run_table_store_repair(mode="apply")``, which
        reaches DuckDB's raw SQL primitive. It never sets ``allow_llm``, so today every
        fix it produces is ``deterministic`` and therefore proven -- its exposure is
        zero for the same structural reason as watch. This test pins that, so enabling
        LLM repairs there becomes a deliberate, visible act rather than a silent one.

        Checked by source scan because exercising it needs a dbt project and a DuckDB
        profile; the runtime gate itself is covered by
        ``tests/unit/test_table_store_proven_gate.py``.
        """
        repo_root = Path(__file__).resolve().parents[2]
        dispatch = repo_root / "packages" / "dataforge-dbt" / "dataforge_dbt" / "dispatch.py"
        if not dispatch.exists():  # pragma: no cover - side package may be absent
            pytest.skip("dataforge-dbt package not present")
        source = dispatch.read_text(encoding="utf-8")

        assert "run_table_store_repair" in source
        assert "allow_llm" not in source, (
            "dataforge-dbt now enables LLM repairs, so its writes can be "
            "plausibility_only. Thread allow_unproven_autoapply through and add a "
            "runtime test that a held fix never becomes SQL."
        )

    def test_table_store_surface_holds(self) -> None:
        # The warehouse primitive is raw SQL, not apply_transaction, so it carries its
        # own copy of the gate driven by the plan's recorded schema status.
        unproven_plan = PatchPlan.new(
            backend="duckdb",
            target="warehouse://duckdb/t",
            relation="t",
            row_identity_columns=("id",),
            operations=(
                PatchOperation.from_cell_fix(
                    _unproven_fix().fix,
                    relation="t",
                    row_identity=RowIdentity(
                        kind="column_values",
                        columns=("id",),
                        values={"id": "2"},
                        stable=True,
                        reason="test",
                    ),
                    reason="llm proposal",
                    confidence=0.99,
                    provenance="llm_live",
                ),
            ),
            safety_verdict="allow",
            rows_scanned=2,
            reason="test plan",
            authoritative_schema_present=False,
        )

        with pytest.raises(TableStoreError, match="unproven"):
            enforce_plan_proven_only(unproven_plan, allow_unproven_autoapply=False)
