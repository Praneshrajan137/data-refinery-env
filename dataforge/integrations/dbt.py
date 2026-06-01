"""dbt artifact helpers for DataForge's local repair contract.

The external ``dataforge15-dbt`` package can call these helpers without
duplicating dbt manifest parsing rules.  Only dbt generic tests that DataForge
can represent as local constraints are mapped; everything else is ignored
conservatively.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dataforge.verifier.schema import AcceptedValues, RelationshipConstraint, Schema


def schema_from_dbt_artifacts(
    manifest_path: Path,
    *,
    model_name: str | None = None,
    model_unique_id: str | None = None,
) -> Schema:
    """Load a dbt manifest and map supported generic tests into a ``Schema``."""
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("dbt manifest must be a JSON object.")
    return schema_from_dbt_manifest(
        payload,
        model_name=model_name,
        model_unique_id=model_unique_id,
    )


def schema_from_dbt_manifest(
    manifest: Mapping[str, Any],
    *,
    model_name: str | None = None,
    model_unique_id: str | None = None,
) -> Schema:
    """Map supported dbt generic tests from a manifest into DataForge constraints.

    Supported dbt tests:
    - ``not_null`` -> ``Schema.not_null_columns``
    - ``unique`` -> ``Schema.unique_columns``
    - ``accepted_values`` -> ``Schema.accepted_values``
    - ``relationships`` -> ``Schema.relationships``
    """
    target_model = _resolve_target_model(manifest, model_name, model_unique_id)
    nodes = _mapping(manifest.get("nodes"))
    columns = _model_columns(target_model)
    not_null_columns: set[str] = set()
    unique_columns: set[str] = set()
    accepted_values: list[AcceptedValues] = []
    relationships: list[RelationshipConstraint] = []

    for raw_node in nodes.values():
        node = _mapping(raw_node)
        if node.get("resource_type") != "test":
            continue
        if target_model is not None and not _test_depends_on_model(node, target_model):
            continue
        metadata = _mapping(node.get("test_metadata"))
        test_name = str(metadata.get("name", node.get("name", ""))).strip()
        kwargs = _mapping(metadata.get("kwargs"))
        column = _column_name(kwargs, node)
        if not column:
            continue

        if test_name == "not_null":
            not_null_columns.add(column)
        elif test_name == "unique":
            unique_columns.add(column)
        elif test_name == "accepted_values":
            values = _accepted_values(kwargs)
            if values:
                accepted_values.append(AcceptedValues(column=column, values=values))
        elif test_name == "relationships":
            reference = str(kwargs.get("to", "")).strip()
            reference_column = str(kwargs.get("field", "")).strip()
            if reference and reference_column:
                relationships.append(
                    RelationshipConstraint(
                        column=column,
                        reference=reference,
                        reference_column=reference_column,
                    )
                )

    return Schema(
        columns=columns,
        not_null_columns=frozenset(not_null_columns),
        unique_columns=frozenset(unique_columns),
        accepted_values=tuple(accepted_values),
        relationships=tuple(relationships),
    )


def _mapping(value: object) -> Mapping[str, Any]:
    """Return a mapping view for JSON objects, or an empty mapping."""
    return value if isinstance(value, Mapping) else {}


def _resolve_target_model(
    manifest: Mapping[str, Any],
    model_name: str | None,
    model_unique_id: str | None,
) -> Mapping[str, Any] | None:
    """Resolve the optional target model node from a dbt manifest."""
    if model_name is None and model_unique_id is None:
        return None
    nodes = _mapping(manifest.get("nodes"))
    for unique_id, raw_node in nodes.items():
        node = _mapping(raw_node)
        if node.get("resource_type") != "model":
            continue
        if model_unique_id is not None and str(unique_id) == model_unique_id:
            return {**node, "unique_id": str(unique_id)}
        if model_name is not None and str(node.get("name", "")) == model_name:
            return {**node, "unique_id": str(unique_id)}
    raise ValueError("Requested dbt model was not found in manifest.")


def _model_columns(model_node: Mapping[str, Any] | None) -> dict[str, str]:
    """Extract declared column types from a dbt model node when available."""
    if model_node is None:
        return {}
    columns: dict[str, str] = {}
    for column_name, raw_column in _mapping(model_node.get("columns")).items():
        column = _mapping(raw_column)
        raw_type = column.get("data_type") or column.get("type")
        if raw_type:
            columns[str(column_name)] = str(raw_type)
    return columns


def _test_depends_on_model(
    test_node: Mapping[str, Any],
    model_node: Mapping[str, Any],
) -> bool:
    """Return whether a dbt test node depends on the target model."""
    target_unique_id = str(model_node.get("unique_id", ""))
    if not target_unique_id:
        target_unique_id = str(model_node.get("name", ""))
    depends_on = _mapping(test_node.get("depends_on"))
    raw_nodes = depends_on.get("nodes", [])
    if not isinstance(raw_nodes, list):
        return False
    return target_unique_id in {str(node) for node in raw_nodes}


def _column_name(kwargs: Mapping[str, Any], test_node: Mapping[str, Any]) -> str:
    """Extract the tested column name from dbt metadata variants."""
    raw_column = kwargs.get("column_name") or kwargs.get("field") or test_node.get("column_name")
    return str(raw_column or "").strip()


def _accepted_values(kwargs: Mapping[str, Any]) -> tuple[str, ...]:
    """Extract dbt accepted_values values as a stable tuple of strings."""
    raw_values = kwargs.get("values", [])
    if not isinstance(raw_values, list):
        return ()
    return tuple(str(value) for value in raw_values)
