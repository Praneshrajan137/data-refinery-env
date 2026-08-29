"""Integration tests for the Week 3 repair pipeline."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app
from tests.support.tables import build_premised_repairable_table

runner = CliRunner()


def _write_csv(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


class TestRepairPipeline:
    """End-to-end Week 3 gating behavior."""

    def test_pipeline_blocks_pii_overwrite_attempt(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "patients.csv"
        schema_path = tmp_path / "schema.yaml"
        _write_csv(
            csv_path,
            "patient_id,phone_number\n1,2175550101\n2,3125550202\n3,not available\n4,6305551010\n",
        )
        schema_path.write_text(
            "columns:\n  patient_id: str\n  phone_number: str\npii_columns:\n  - phone_number\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )

        assert result.exit_code == 1
        assert "attempted but not fixed" in result.output.lower()
        assert "NO_PII_OVERWRITE" in result.output

    def test_pipeline_blocks_fd_violating_fix_after_retries(self, tmp_path: Path) -> None:
        """A repair that satisfies its own FD but breaks a chained one is never applied.

        ``code -> name`` and ``name -> state`` are CHAINED, not conflicting: repairing
        row 2's name from Beta to Alpha satisfies ``code -> name`` and then makes
        ``name -> state`` map Alpha to both IL and NY. The verifier rejects it, and the
        cell must be left alone.

        Retry accounting changed on 2026-08-29 and the assertion is now explicit rather
        than incidental. This test previously asserted only that ``"3"`` appeared
        somewhere in the output, which it does for unrelated reasons. The repairer used to
        discard ``retry_context`` and re-propose the identical rejected value three times,
        costing three deep table copies and three z3 encodings to reach a verdict it
        already had on the first. It now abstains once the candidate is known-rejected, so
        there are TWO attempts: one real rejection, then the abstention. What must not
        change is the outcome -- nothing applied, exit 1 -- and that the user is still told
        WHICH constraint blocked it.
        """
        csv_path = tmp_path / "codes.csv"
        schema_path = tmp_path / "schema.yaml"
        original = "code,name,state\nA,Alpha,IL\nA,Alpha,IL\nA,Beta,NY\n"
        _write_csv(csv_path, original)
        schema_path.write_text(
            "columns:\n"
            "  code: str\n"
            "  name: str\n"
            "  state: str\n"
            "functional_dependencies:\n"
            "  - determinant: [code]\n"
            "    dependent: name\n"
            "  - determinant: [name]\n"
            "    dependent: state\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )

        assert result.exit_code == 1
        # Rich hard-wraps panel text, so a literal substring assertion silently depends on
        # the wrap column: "functional dependency" arrives split across a newline. Collapse
        # whitespace before matching, or the test passes and fails for reasons unrelated to
        # the behaviour it names.
        flattened = " ".join(result.output.split()).lower()
        assert "attempted but not fixed" in flattened
        assert "functional dependency) name -> state" in flattened, (
            "the user must still be told which constraint blocked the repair; abstaining "
            "must not degrade the diagnostic to 'no proposal was available'"
        )
        assert "after 1 attempt(s)" in flattened, (
            "one real rejection then an abstention. Three identical rejected proposals "
            "would read 'after 3 attempt(s)'."
        )
        assert csv_path.read_text(encoding="utf-8") == original, (
            "no fix is provable here, so the file must be byte-identical"
        )

    def test_pipeline_refuses_to_apply_a_decimal_shift_fix(self, tmp_path: Path) -> None:
        """A declared schema does NOT make a decimal-shift write acceptable.

        This test was ``test_pipeline_accepts_valid_decimal_shift_fix`` and asserted
        ``Applied 1 fix`` plus ``"1020" not in ...``. That expectation is retracted, not
        migrated: ``decimal_shift`` infers its value from the shape of the column's own
        distribution, which flagged 263,428 money cells across three TPC-H tables with
        zero true errors. It is held on every surface regardless of schema
        (``specs/SPEC_autoapply_decision.md`` rows 8-9).

        The ``domain_bounds`` schema is kept deliberately: its presence is what made the
        original expectation look reasonable, and pinning that it does not rescue the
        write is the point.
        """
        csv_path = tmp_path / "amounts.csv"
        schema_path = tmp_path / "schema.yaml"
        _write_csv(
            csv_path,
            "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        )
        schema_path.write_text(
            "columns:\n"
            "  id: str\n"
            "  amount: float\n"
            "domain_bounds:\n"
            "  amount:\n"
            "    min: 0\n"
            "    max: 5000\n",
            encoding="utf-8",
        )
        before = csv_path.read_text(encoding="utf-8")

        result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )

        assert result.exit_code == 1, "no fix was applied, so the command must not succeed"
        assert "1020" in csv_path.read_text(encoding="utf-8"), "the cell must be untouched"
        assert csv_path.read_text(encoding="utf-8") == before

    def test_pipeline_applies_a_constraint_checkable_fix(self, tmp_path: Path) -> None:
        """The accept path still works, on a repair the product stands behind.

        Companion to the test above: together they pin that the refusal is specific to
        non-constraint-checkable detectors rather than a blanket failure of ``--apply``.
        Without this, a bug that broke every write would satisfy the refusal test.
        """
        table = build_premised_repairable_table(tmp_path / "premised.csv")

        result = runner.invoke(
            app,
            ["repair", str(table.csv_path), "--apply", "--schema", str(table.schema_path)],
        )

        assert result.exit_code == 0, result.output
        assert "Applied 1 fix" in result.output
        assert "bostonn" not in table.read()
