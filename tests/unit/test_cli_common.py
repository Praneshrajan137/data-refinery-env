"""Unit tests for shared CLI helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

from dataforge.cli.common import load_schema, schema_from_mapping


class TestLoadSchema:
    """Schema-loading edge cases."""

    def test_empty_yaml_returns_empty_schema(self, tmp_path: Path) -> None:
        schema_path = tmp_path / "empty.yaml"
        schema_path.write_text("", encoding="utf-8")

        schema = load_schema(schema_path)

        assert schema.columns == {}
        assert schema.functional_dependencies == ()

    def test_non_mapping_yaml_is_rejected(self, tmp_path: Path) -> None:
        schema_path = tmp_path / "bad.yaml"
        schema_path.write_text("- item\n", encoding="utf-8")

        with pytest.raises(typer.BadParameter, match="must be a YAML mapping"):
            load_schema(schema_path)

    def test_missing_schema_file_is_rejected(self, tmp_path: Path) -> None:
        missing_path = tmp_path / "missing.yaml"

        with pytest.raises(typer.BadParameter, match="Could not read schema file"):
            load_schema(missing_path)

    def test_non_mapping_fd_entries_are_skipped(self, tmp_path: Path) -> None:
        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text(
            "columns:\n"
            "  code: str\n"
            "functional_dependencies:\n"
            "  - determinant: [code]\n"
            "    dependent: name\n"
            "  - bad-entry\n",
            encoding="utf-8",
        )

        schema = load_schema(schema_path)

        assert len(schema.functional_dependencies) == 1
        assert schema.functional_dependencies[0].dependent == "name"

    def test_week3_schema_metadata_is_parsed(self, tmp_path: Path) -> None:
        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text(
            "columns:\n"
            "  amount: float\n"
            "  phone_number: str\n"
            "pii_columns:\n"
            "  - phone_number\n"
            "domain_bounds:\n"
            "  amount:\n"
            "    min: 0\n"
            "    max: 5000\n"
            "aggregate_dependencies:\n"
            "  - source_column: amount\n"
            "    aggregate: sum\n"
            "    target_column: total_amount\n"
            "    group_by: [account_id]\n",
            encoding="utf-8",
        )

        schema = load_schema(schema_path)

        assert schema.pii_columns == frozenset({"phone_number"})
        assert len(schema.domain_bounds) == 1
        assert schema.domain_bounds[0].min_value == 0.0
        assert len(schema.aggregate_dependencies) == 1
        assert schema.aggregate_dependencies[0].target_column == "total_amount"

    def test_schema_from_mapping_ignores_non_iterable_pii_and_invalid_aggregate(self) -> None:
        schema = schema_from_mapping(
            {
                "columns": {"amount": "float"},
                "pii_columns": 123,
                "aggregate_dependencies": [
                    {
                        "source_column": "amount",
                        "aggregate": "median",
                        "target_column": "total_amount",
                        "group_by": "account_id",
                    }
                ],
            }
        )

        assert schema.pii_columns == frozenset()
        assert schema.aggregate_dependencies == ()
