"""What the engine must NOT do on data that is already correct.

Why this file exists
--------------------
The corruption oracle (``tests/property/test_no_corruption_invariant.py``) asserts that a
correct cell is never changed. It could not catch the worst bug this project has shipped,
because it **generates its clean numeric columns "clustered (low variance) so no correct
cell is a decimal-shift outlier"**. The precondition that makes the detector sound became
a property of the *fixture* rather than a check in the *code*, so the invariant was true
of the test data and false of the world.

Real warehouse columns are not clustered. Measured log-space inter-quartile ranges:
``orders.o_totalprice`` 0.44 dex, ``lineitem.l_extendedprice`` 0.47,
``customer.c_acctbal`` 0.62, ``QUERY_HISTORY.total_elapsed_time`` 0.48. At that spread a
10x offset from the median sits only 1.6-2.3 IQR units out -- entirely ordinary. On
error-free TPC-H the ungated rule would have rewritten **263,428** monetary values, and a
live ``dataforge repair --apply`` turned a legitimate ``1131.20`` into ``113120``.

So this module tests the missing case: **wide-dispersion, internally-consistent data**.
Every flag on such a table is a false positive by construction, which makes the assertion
sharp rather than a matter of judgement.

Three families here, and each exists because a specific thing went wrong:

* ``TestCleanWideTablesAreNotFlagged`` -- the case the oracle's fixture excluded.
* ``TestSchemaFreeApplyWritesNothing`` -- INV0. Without a declared premise nothing is
  proven, so nothing may be written. This is the invariant that actually protects the
  schema-free path, and it is stated here because INV1/INV2 in the oracle are now
  **trivially satisfied** on that path: if no cell changes, "no correct cell changed"
  holds for free. A silently-vacuous guard is worse than a missing one.
* ``TestDeclaredPremiseStillAutoApplies`` -- the non-vacuity anchor. Removing
  ``decimal_shift`` from the calibration bypass emptied the *schema-free* deterministic
  write path entirely (it was the only deterministic repairer that proposed anything
  without a schema). If that were the whole story, every no-write assertion above would
  pass for the boring reason that the engine writes nothing ever. This test proves the
  write path is alive under a declared premise, so the no-write assertions mean something.
"""

from __future__ import annotations

import math
import random
import tempfile
from pathlib import Path

import pytest

from dataforge.detectors import run_all_detectors
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.table import Table
from dataforge.verifier.schema import FunctionalDependency, Schema

# Measured log-IQR of real warehouse money and telemetry columns is 0.44-0.62 dex.
# Generating at or above that range is what makes these tables representative; the
# oracle's clustered fixture sits far below it.
_REALISTIC_LOG_SPREADS = (0.5, 1.0, 2.0, 3.0)


def _log_uniform_column(rng: random.Random, *, n: int, low: float, spread_dex: float) -> list[str]:
    """A monetary-shaped column: log-uniform over ``spread_dex`` orders of magnitude."""
    return [f"{low * (10 ** rng.uniform(0.0, spread_dex)):.2f}" for _ in range(n)]


def _clean_wide_table(rng: random.Random, *, n_rows: int, spread_dex: float) -> Table:
    """An internally-consistent table with realistically wide numeric columns.

    Nothing here is an error: amounts are drawn from one distribution, ``region`` is
    functionally determined by ``customer`` and that dependency holds exactly, ids are
    unique and sequential, and no cell is blank. A detector that flags any of it is
    reporting a false positive.
    """
    amounts = _log_uniform_column(rng, n=n_rows, low=10.0, spread_dex=spread_dex)
    durations = _log_uniform_column(rng, n=n_rows, low=1.0, spread_dex=spread_dex)
    regions = {"c0": "emea", "c1": "apac", "c2": "amer"}
    rows = []
    for i in range(n_rows):
        customer = f"c{i % 3}"
        rows.append(
            {
                "invoice_id": str(100000 + i),
                "customer": customer,
                "region": regions[customer],
                "amount": amounts[i],
                "duration_ms": durations[i],
            }
        )
    return Table(["invoice_id", "customer", "region", "amount", "duration_ms"], rows)


class TestCleanWideTablesAreNotFlagged:
    """The class of test whose absence allowed a 100x monetary rewrite to ship."""

    @pytest.mark.parametrize("spread_dex", _REALISTIC_LOG_SPREADS)
    @pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
    def test_no_decimal_shift_on_a_correct_wide_column(self, spread_dex: float, seed: int) -> None:
        table = _clean_wide_table(random.Random(seed), n_rows=60, spread_dex=spread_dex)
        flags = [i for i in run_all_detectors(table, None) if i.issue_type == "decimal_shift"]
        assert flags == [], (
            f"decimal_shift flagged {len(flags)} correct cells in a column spanning "
            f"{spread_dex} dex. Every flag here is a false positive by construction: "
            "the column was drawn from a single distribution and contains no errors. "
            f"First: row {flags[0].row} {flags[0].column} value {flags[0].actual!r}"
            if flags
            else ""
        )

    @pytest.mark.parametrize("spread_dex", _REALISTIC_LOG_SPREADS)
    def test_the_generated_column_really_is_wide(self, spread_dex: float) -> None:
        """Guard the guard: if the generator collapsed, the test above proves nothing.

        A fixture that silently became narrow would make every assertion in this class
        pass for the same reason the oracle's did. So the spread is asserted, not assumed.
        """
        table = _clean_wide_table(random.Random(11), n_rows=60, spread_dex=spread_dex)
        logs = sorted(math.log10(float(table.cell(r, "amount"))) for r in table.index)
        log_iqr = logs[(3 * len(logs)) // 4] - logs[len(logs) // 4]
        assert log_iqr > 0.2, (
            f"generator produced log-IQR {log_iqr:.3f} dex at spread {spread_dex}; "
            "real warehouse columns measure 0.44-0.62 dex and this fixture must be at "
            "least that wide or it repeats the oracle's mistake"
        )

    def test_a_genuine_decimal_shift_is_still_caught(self) -> None:
        """The dispersion gate must not have simply disabled the detector.

        On a genuinely clustered column -- the case where a 10x offset really is
        anomalous -- an injected shift must still be found. This is what stops the fix
        above from being 'delete the feature'.
        """
        rng = random.Random(7)
        values = [f"{rng.uniform(95.0, 105.0):.2f}" for _ in range(40)]
        clean = float(values[10])
        values[10] = f"{clean * 100:.2f}"
        table = Table(["qty"], [{"qty": v} for v in values])
        flags = [i for i in run_all_detectors(table, None) if i.issue_type == "decimal_shift"]
        assert [i.row for i in flags] == [10], (
            "an injected 100x error on a tightly-clustered column must still be detected; "
            f"got flags at rows {[i.row for i in flags]}"
        )


class TestSchemaFreeApplyWritesNothing:
    """INV0: with no declared premise, nothing is proven, so nothing may be written.

    This is the invariant that protects the default path. It is separated from the
    corruption oracle deliberately: the oracle's INV1 ("a correct cell is never changed")
    and INV2 ("a changed cell holds ground truth") are both satisfied by changing nothing,
    so on this path they can no longer fail. This test can.
    """

    def _apply(self, table: Table, tmp: Path) -> tuple[bytes, bytes]:
        source = tmp / "data.csv"
        header = ",".join(table.columns)
        lines = [header] + [",".join(table.cell(r, c) for c in table.columns) for r in table.index]
        original = ("\n".join(lines) + "\n").encode("utf-8")
        source.write_bytes(original)
        run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
        return original, source.read_bytes()

    @pytest.mark.parametrize("spread_dex", _REALISTIC_LOG_SPREADS)
    def test_wide_clean_table_is_byte_identical_after_apply(self, spread_dex: float) -> None:
        table = _clean_wide_table(random.Random(23), n_rows=60, spread_dex=spread_dex)
        with tempfile.TemporaryDirectory() as temp_dir:
            before, after = self._apply(table, Path(temp_dir))
        assert before == after, "apply modified a table that contains no errors"

    def test_even_a_clustered_column_with_a_real_shift_is_not_written(self) -> None:
        """The detector may flag; the engine may not write. That split is the whole fix.

        Here the flag is *correct* -- a genuine 100x injection on a clustered column --
        and the engine must still refuse to auto-apply it, because ``decimal_shift`` is a
        distributional inference and carries no calibrated threshold. It goes to review
        instead. Confidence in a heuristic is not authority to write.
        """
        rng = random.Random(31)
        values = [f"{rng.uniform(95.0, 105.0):.2f}" for _ in range(40)]
        values[5] = f"{float(values[5]) * 100:.2f}"
        table = Table(["qty"], [{"qty": v} for v in values])
        assert any(i.issue_type == "decimal_shift" for i in run_all_detectors(table, None)), (
            "fixture must produce a decimal_shift flag or this proves nothing"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            before, after = self._apply(table, Path(temp_dir))
        assert before == after, (
            "decimal_shift was auto-applied. It is a distributional inference with "
            "precision 0.0000 measured on hospital, flights and rayyan; it must clear a "
            "calibrated threshold like any other fallible source, and the shipped policy "
            "certifies none."
        )


class TestDeclaredPremiseStillAutoApplies:
    """The non-vacuity anchor for every no-write assertion above."""

    def test_a_declared_fd_violation_is_repaired(self) -> None:
        """A deterministic repair checkable against a *declared* reference still writes.

        Without this, the tests above would pass trivially on an engine that never writes
        anything, which is exactly the failure mode this module was created to expose.
        ``fd_violation`` under a declared dependency is checkable rather than
        distributional, so it is in ``CONSTRAINT_CHECKABLE_DETECTORS`` and auto-applies.
        """
        from dataforge.repairers import build_repairers

        schema = Schema(
            columns={"cat": "string", "dep": "string"},
            functional_dependencies=(FunctionalDependency(determinant=("cat",), dependent="dep"),),
        )
        rows = [
            {"cat": "a" if i % 2 == 0 else "b", "dep": "x" if i % 2 == 0 else "y"}
            for i in range(10)
        ]
        rows[4]["dep"] = "WRONG"
        table = Table(["cat", "dep"], rows)
        repairers = build_repairers(cache_dir=None, allow_llm=False)
        proposed = [
            repairers[i.issue_type].propose(i, table, schema)
            for i in run_all_detectors(table, schema)
            if i.issue_type == "fd_violation"
        ]
        writes = [f for f in proposed if f is not None]
        assert writes, "the declared-FD repair path produced nothing; the write path is dead"
        assert writes[0].fix.new_value == "x"
        assert writes[0].provenance == "deterministic"

    def test_the_allowlist_is_an_allowlist(self) -> None:
        """An unclassified detector must be calibration-bound, not exempt.

        A denylist of known-bad detectors fails open: anything nobody thought of would
        bypass calibration. This pins the direction, because the direction is the point.
        """
        from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS

        assert "decimal_shift" not in CONSTRAINT_CHECKABLE_DETECTORS
        assert "outlier" not in CONSTRAINT_CHECKABLE_DETECTORS
        assert "some_detector_invented_next_year" not in CONSTRAINT_CHECKABLE_DETECTORS
        assert "fd_violation" in CONSTRAINT_CHECKABLE_DETECTORS
