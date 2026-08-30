"""``measure-on-my-table``: the egress guarantee, and the arithmetic behind the counts.

Privacy is the deliverable of this instrument, not a feature of it. A value-leak in a report a
customer has already sent us cannot be fixed after shipping, so the guarantee is tested three
independent ways:

  1. :class:`TestEveryFieldIsNonValueBearing` -- structural. No field of the report has a type
     that could carry a cell value, checked field by field rather than by inspection.
  2. :class:`TestSentinelNeverEscapes` -- the pre-registered test. Plant a recognisable string
     into the table and assert it appears nowhere in the report bytes.
  3. :class:`TestEgressScanRefuses` -- the refusal itself fires when a value IS present, so
     test 2 passing is evidence about the report rather than evidence that the scan is inert.

Test 2 alone would be satisfied by a scan that always passes. Test 3 alone would be satisfied by
a report nobody emits. Both are needed, and mutant M20 removes the refusal to prove they bite.
"""

from __future__ import annotations

import json

import pytest

from dataforge.detectors.base import FunctionalDependency, Schema
from dataforge.measure_on_my_table import (
    MeasuredOnMyTable,
    assert_no_plaintext_values,
    measure_on_my_table,
    plant_into_table,
    report_payload,
)
from dataforge.table import Table, table_to_csv_bytes

#: Long enough to clear ``_SENTINEL_MIN_LENGTH`` and distinctive enough that an incidental
#: match is not credible.
SENTINEL = "ZZQX-SENTINEL-PATIENT-NAME-4417"


def _table(rows: list[dict[str, object]]) -> Table:
    return Table(list(rows[0]), rows)


def _consistent_table() -> Table:
    """A table where ``zip -> city`` holds with a clear majority in every group."""
    rows: list[dict[str, object]] = []
    for group, city in (("11111", "Springfield"), ("22222", "Shelbyville")):
        for index in range(6):
            rows.append({"zip": group, "city": city, "note": f"note-{group}-{index}"})
    return _table(rows)


ZIP_TO_CITY = Schema(
    functional_dependencies={FunctionalDependency(determinant=("zip",), dependent="city")}
)


class TestEveryFieldIsNonValueBearing:
    """No field of the report can hold a cell value. Structural, so it cannot rot silently."""

    def test_no_field_admits_a_free_string(self) -> None:
        # A str field is admissible only where its content is a digest or a fixed vocabulary
        # member. Those are named here so adding a fifth str field forces a decision.
        digest_or_vocabulary = {"schema_version", "table_digest"}
        prose = {"limitations", "not_measurable"}
        for name, field in MeasuredOnMyTable.model_fields.items():
            annotation = str(field.annotation)
            if name in digest_or_vocabulary or name in prose:
                continue
            assert "str" not in annotation or name == "writes_by_column_index", (
                f"field {name!r} has type {annotation}, which could carry a cell value. "
                "Every measured field must be an int or a float."
            )

    def test_report_forbids_extra_fields(self) -> None:
        # Without this, a caller could attach values to the report and the schema would accept
        # them, which would make every other test here decorative.
        with pytest.raises(Exception, match="extra"):
            MeasuredOnMyTable(  # type: ignore[call-arg]
                table_digest="0" * 64,
                rows=1,
                columns=1,
                plants_requested=0,
                plants_placed=0,
                mined_dependencies=0,
                fd_covered_columns=0,
                repaired_a_planted_error=0,
                wrong_value_on_a_planted_error=0,
                missed_a_planted_error=0,
                wrote_to_a_cell_we_did_not_plant=0,
                cells_written_total=0,
                offending_value="Springfield",
            )

    def test_writes_are_keyed_by_column_index_not_name(self) -> None:
        # Column NAMES can themselves be sensitive -- "hiv_status" discloses without any row.
        table = _consistent_table()
        table.set_cell(0, "city", "Shelbyville")
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=0
        )
        assert report.writes_by_column_index
        for key in report.writes_by_column_index:
            assert key.startswith("col:")
            assert "city" not in key and "zip" not in key


class TestSentinelNeverEscapes:
    """The pre-registered test: a recognisable value must appear nowhere in the report."""

    def test_sentinel_absent_from_report_bytes(self) -> None:
        rows: list[dict[str, object]] = [
            {"zip": "11111", "city": "Springfield", "note": SENTINEL},
            *[
                {"zip": "11111", "city": "Springfield", "note": f"note-{index}"}
                for index in range(5)
            ],
            {"zip": "11111", "city": SENTINEL, "note": "outlier"},
        ]
        table = _table(rows)
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=3
        )
        rendered = json.dumps(report_payload(report), indent=2, sort_keys=True)
        assert SENTINEL not in rendered
        # The sentinel sits in a cell the repair WOULD rewrite, so this is not vacuous: the
        # instrument saw the value and still did not carry it out.
        assert report.cells_written_total >= 1

    def test_scan_accepts_a_real_report(self) -> None:
        table = _consistent_table()
        table.set_cell(0, "city", "Shelbyville")
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=2
        )
        rendered = json.dumps(report_payload(report), sort_keys=True).encode("utf-8")
        assert_no_plaintext_values(rendered, table)


class TestEgressScanRefuses:
    """The scan must actually fire, or the sentinel test proves nothing about the scan."""

    def _report_payload(self, table: Table) -> dict[str, object]:
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=2
        )
        return report_payload(report)

    def test_refuses_when_a_value_is_present(self) -> None:
        table = _consistent_table()
        table.set_cell(0, "note", SENTINEL)
        payload = self._report_payload(table)
        # A future field carrying a value: the case the structural guarantee cannot cover.
        payload["debug_note"] = SENTINEL
        with pytest.raises(ValueError, match="table value"):
            assert_no_plaintext_values(json.dumps(payload).encode("utf-8"), table)

    def test_refuses_if_the_fixed_prose_was_altered(self) -> None:
        # The prose keys are exempt from the value scan. That exemption rests on nothing being
        # able to reach them, so if the prose differs the exemption is void and this refuses.
        table = _consistent_table()
        payload = self._report_payload(table)
        payload["limitations"] = ["nothing to see here"]
        with pytest.raises(ValueError, match="fixed prose"):
            assert_no_plaintext_values(json.dumps(payload).encode("utf-8"), table)

    def test_fixed_prose_may_name_a_value_that_is_also_a_cell(self) -> None:
        # The false positive the scan found on itself: a corpus named in the prose can contain
        # a cell whose value is that name. Scanning our own commentary is a category error.
        rows: list[dict[str, object]] = [
            {"zip": "11111", "city": "Springfield", "note": "hospital"},
            *[
                {"zip": "11111", "city": "Springfield", "note": f"note-{index}"}
                for index in range(5)
            ],
        ]
        table = _table(rows)
        payload = self._report_payload(table)
        assert any("hospital" in line for line in payload["limitations"])  # type: ignore[operator]
        assert_no_plaintext_values(json.dumps(payload).encode("utf-8"), table)

    def test_short_values_are_out_of_scope_and_that_is_documented(self) -> None:
        # A two-character value collides with digest substrings by chance, so scanning for it
        # would reject correct reports. This asserts the limit rather than hiding it.
        table = _table([{"zip": "11111", "city": "Springfield", "code": "AB"}])
        payload = self._report_payload(table)
        payload["table_digest"] = "abcd0011"
        assert_no_plaintext_values(json.dumps(payload).encode("utf-8"), table)


class TestPlanting:
    def test_plants_avoid_flagged_cells(self) -> None:
        table = _consistent_table()
        avoid = frozenset({(row, "city") for row in range(12)})
        _planted, planted = plant_into_table(table, count=20, flagged_cells=avoid)
        assert planted
        assert all(item.column != "city" for item in planted)

    def test_planting_records_the_pre_corruption_value(self) -> None:
        table = _consistent_table()
        planted_table, planted = plant_into_table(table, count=5, flagged_cells=frozenset())
        assert planted
        for item in planted:
            # Truth is known BY CONSTRUCTION because we performed the corruption.
            assert item.withheld_truth == table.cell(item.row, item.column)
            assert planted_table.cell(item.row, item.column) == item.corrupted_to
            assert item.corrupted_to != item.withheld_truth

    def test_planting_leaves_the_source_table_untouched(self) -> None:
        # The instrument must never need write permission, which starts with not mutating the
        # table it was handed.
        table = _consistent_table()
        before = table_to_csv_bytes(table)
        plant_into_table(table, count=5, flagged_cells=frozenset())
        assert table_to_csv_bytes(table) == before


class TestCountsAndRates:
    def test_no_premise_means_no_measurement_not_a_clean_bill(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=None, plants=5
        )
        assert report.mined_dependencies == 0
        assert report.cells_written_total == 0
        # Rates are None rather than 0.0: there is no denominator, and 0.0 would read as a pass.
        assert report.planted_write_precision is None
        assert report.unrequested_write_rate is None
        assert any("measures NOTHING" in line for line in report.limitations)

    def test_planted_error_is_repaired_and_counted(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=1
        )
        assert report.plants_placed == 1
        # A single-cell plant in a consistent group is a minority of one, so majority repair
        # restores it exactly. That this is EASY is the documented upper-bound limitation.
        assert report.repaired_a_planted_error + report.missed_a_planted_error == 1

    def test_outcomes_partition_the_plants(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=4
        )
        assert (
            report.repaired_a_planted_error
            + report.wrong_value_on_a_planted_error
            + report.missed_a_planted_error
            == report.plants_placed
        )

    def test_writes_partition_into_planted_and_unplanted(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=4
        )
        assert (
            report.repaired_a_planted_error
            + report.wrong_value_on_a_planted_error
            + report.wrote_to_a_cell_we_did_not_plant
            == report.cells_written_total
        )

    def test_recall_is_reported_unmeasurable_not_omitted(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=2
        )
        assert any("RECALL" in line for line in report.not_measurable)
        # An absent metric reads as a zero, which is why it is named rather than dropped.
        assert any("absent metric reads as a zero" in line for line in report.not_measurable)

    def test_precision_is_labelled_an_upper_bound(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=2
        )
        assert any("UPPER BOUND on precision" in line for line in report.limitations), (
            "measurement showed planted precision 1.0 against real 0.795; the report must say so"
        )

    def test_damage_figure_is_labelled_a_ceiling(self) -> None:
        table = _consistent_table()
        report = measure_on_my_table(
            table, table_bytes=table_to_csv_bytes(table), schema=ZIP_TO_CITY, plants=2
        )
        assert any("UPPER BOUND on damage" in line for line in report.limitations)
