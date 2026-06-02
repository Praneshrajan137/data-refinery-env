"""Schema management and inference service."""

from typing import Optional

from dataforge.io import CSVReader, SchemaStore
from dataforge.models import Schema
from dataforge.types import CSVPath, SchemePath, TableData


class SchemaManager:
    """Service for managing and inferring schemas."""

    def __init__(self):
        """Initialize schema manager."""
        self.csv_reader = CSVReader()

    def infer_from_file(self, csv_path: CSVPath) -> Schema:
        """
        Infer schema from a CSV file.

        Args:
            csv_path: Path to CSV file

        Returns:
            Inferred Schema
        """
        # Read sample of data
        data = self.csv_reader.read_rows(csv_path, limit=1000)
        return SchemaStore.infer_from_data(data)

    def infer_from_data(self, data: TableData) -> Schema:
        """
        Infer schema from in-memory data.

        Args:
            data: List of row dictionaries

        Returns:
            Inferred Schema
        """
        return SchemaStore.infer_from_data(data)

    def save_schema(self, schema: Schema, path: SchemePath) -> None:
        """
        Save schema to file.

        Args:
            schema: Schema to save
            path: Path to schema file
        """
        SchemaStore.save(schema, path)

    def load_schema(self, path: SchemePath) -> Schema:
        """
        Load schema from file.

        Args:
            path: Path to schema file

        Returns:
            Loaded Schema
        """
        return SchemaStore.load(path)

    def merge_schemas(self, schema1: Schema, schema2: Schema) -> Schema:
        """
        Merge two schemas (union of columns and constraints).

        Args:
            schema1: First schema
            schema2: Second schema

        Returns:
            Merged schema
        """
        # Combine columns
        merged_columns = {**schema1.columns, **schema2.columns}

        # Combine constraints (avoiding duplicates by name)
        constraint_dict = {c.name: c for c in schema1.constraints}
        constraint_dict.update({c.name: c for c in schema2.constraints})
        merged_constraints = tuple(constraint_dict.values())

        # Use average confidence
        avg_confidence = (schema1.confidence + schema2.confidence) / 2

        return Schema(
            columns=merged_columns,
            constraints=merged_constraints,
            inferred=True,
            confidence=avg_confidence,
        )

    def validate_schema(self, schema: Schema, data: TableData) -> list[str]:
        """
        Validate schema against data.

        Args:
            schema: Schema to validate
            data: Data to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        if not data:
            return errors

        # Check that all data columns are in schema
        first_row = data[0]
        for column in first_row.keys():
            if not schema.has_column(column):
                errors.append(f"Column '{column}' not in schema")

        # Check that all schema columns exist in data
        for column in schema.columns.keys():
            if column not in first_row:
                errors.append(f"Schema column '{column}' not in data")

        return errors

    def get_schema_summary(self, schema: Schema) -> str:
        """Get human-readable summary of schema."""
        lines = [
            f"Schema ({len(schema.columns)} columns, {len(schema.constraints)} constraints)",
            f"Inferred: {schema.inferred}, Confidence: {schema.confidence:.0%}",
            "Columns:",
        ]

        for col in schema.columns.values():
            lines.append(f"  - {col.name}: {col.type.value} {'NULL' if col.nullable else 'NOT NULL'}")

        if schema.constraints:
            lines.append("Constraints:")
            for constraint in schema.constraints:
                lines.append(f"  - {constraint}")

        return "\n".join(lines)
