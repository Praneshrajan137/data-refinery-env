"""Schema persistence layer."""

import json
from pathlib import Path
from typing import Optional

from dataforge.exceptions import IOError as DataForgeIOError, SchemaError
from dataforge.models import Column, ColumnType, Constraint, Schema
from dataforge.types import SchemePath


class SchemaStore:
    """Store and retrieve schema definitions."""

    @staticmethod
    def save(schema: Schema, path: SchemePath) -> None:
        """
        Save schema to JSON file.

        Args:
            schema: Schema to save
            path: Path to schema file

        Raises:
            IOError: If save fails
        """
        path = Path(path)

        try:
            schema_dict = {
                "columns": {
                    name: {
                        "name": col.name,
                        "type": col.type.value,
                        "nullable": col.nullable,
                        "constraints": [
                            {
                                "name": c.name,
                                "type": c.type,
                                "columns": c.columns,
                                "expression": c.expression,
                            }
                            for c in col.constraints
                        ],
                        "cardinality": col.cardinality,
                    }
                    for name, col in schema.columns.items()
                },
                "constraints": [
                    {
                        "name": c.name,
                        "type": c.type,
                        "columns": c.columns,
                        "expression": c.expression,
                    }
                    for c in schema.constraints
                ],
                "inferred": schema.inferred,
                "confidence": schema.confidence,
            }

            with open(path, "w") as f:
                json.dump(schema_dict, f, indent=2)

        except Exception as e:
            raise DataForgeIOError(
                f"Failed to save schema: {e}",
                context={"path": str(path)},
            )

    @staticmethod
    def load(path: SchemePath) -> Schema:
        """
        Load schema from JSON file.

        Args:
            path: Path to schema file

        Returns:
            Loaded Schema

        Raises:
            IOError: If load fails
        """
        path = Path(path)

        if not path.exists():
            raise DataForgeIOError(
                f"Schema file not found: {path}",
                context={"path": str(path)},
            )

        try:
            with open(path, "r") as f:
                data = json.load(f)

            # Reconstruct columns
            columns = {}
            for name, col_data in data.get("columns", {}).items():
                constraints = tuple(
                    Constraint(
                        name=c["name"],
                        type=c["type"],
                        columns=tuple(c["columns"]),
                        expression=c.get("expression"),
                    )
                    for c in col_data.get("constraints", [])
                )
                columns[name] = Column(
                    name=col_data["name"],
                    type=ColumnType(col_data["type"]),
                    nullable=col_data.get("nullable", True),
                    constraints=constraints,
                    cardinality=col_data.get("cardinality", 0),
                )

            # Reconstruct table-level constraints
            constraints = tuple(
                Constraint(
                    name=c["name"],
                    type=c["type"],
                    columns=tuple(c["columns"]),
                    expression=c.get("expression"),
                )
                for c in data.get("constraints", [])
            )

            return Schema(
                columns=columns,
                constraints=constraints,
                inferred=data.get("inferred", True),
                confidence=data.get("confidence", 0.0),
            )

        except json.JSONDecodeError as e:
            raise SchemaError(
                f"Invalid JSON in schema file: {e}",
                context={"path": str(path)},
            )
        except Exception as e:
            raise DataForgeIOError(
                f"Failed to load schema: {e}",
                context={"path": str(path)},
            )

    @staticmethod
    def infer_from_data(data: list[dict]) -> Schema:
        """
        Infer schema from sample data.

        Args:
            data: Sample rows of data

        Returns:
            Inferred Schema
        """
        if not data:
            return Schema(columns={})

        # Infer column types from first row
        columns = {}
        first_row = data[0]

        for key in first_row.keys():
            # Simple type inference: all strings for now
            # In production, analyze multiple rows
            columns[key] = Column(
                name=key,
                type=ColumnType.STRING,
                nullable=True,
                cardinality=len(set(row.get(key) for row in data)),
            )

        return Schema(
            columns=columns,
            inferred=True,
            confidence=0.5,  # Low confidence for auto-inferred schemas
        )
