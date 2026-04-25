"""Unit tests for constitution parsing and registry behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.safety.constitution import (
    ConstitutionError,
    default_constitution_path,
    load_constitution,
)


class TestConstitutionParsing:
    """Constitution-loading behavior."""

    def test_default_constitution_contains_required_rules(self) -> None:
        constitution = load_constitution(default_constitution_path())
        rule_ids = {rule.rule_id for rule in constitution.single_rules}
        rule_ids.update(rule.rule_id for rule in constitution.batch_rules)
        rule_ids.update(rule.rule_id for rule in constitution.preference_rules)
        assert {
            "NO_PII_OVERWRITE",
            "NO_ROW_DELETE",
            "NO_AGGREGATE_BREAK",
            "MINIMAL_EDIT",
            "NO_CONFLICTING_CELL_WRITES",
        }.issubset(rule_ids)

    def test_unknown_predicate_is_rejected(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "constitution.yaml"
        yaml_path.write_text(
            "hard_never:\n"
            "  - id: BAD_RULE\n"
            "    description: broken\n"
            "    predicate: does_not_exist\n",
            encoding="utf-8",
        )

        with pytest.raises(ConstitutionError, match="does_not_exist"):
            load_constitution(yaml_path)

    def test_soft_prefer_requires_scorer(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "constitution.yaml"
        yaml_path.write_text(
            "soft_prefer:\n"
            "  - id: MINIMAL_EDIT\n"
            "    description: broken\n"
            "    predicate: minimal_edit_distance\n",
            encoding="utf-8",
        )

        with pytest.raises(ConstitutionError, match="scorer"):
            load_constitution(yaml_path)

    def test_non_mapping_root_is_rejected(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "constitution.yaml"
        yaml_path.write_text("- bad-root\n", encoding="utf-8")

        with pytest.raises(ConstitutionError, match="YAML mapping"):
            load_constitution(yaml_path)

    def test_non_list_tier_is_rejected(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "constitution.yaml"
        yaml_path.write_text("hard_never: nope\n", encoding="utf-8")

        with pytest.raises(ConstitutionError, match="must be a YAML list"):
            load_constitution(yaml_path)

    def test_batch_rule_with_unknown_predicate_is_rejected(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "constitution.yaml"
        yaml_path.write_text(
            "hard_never:\n"
            "  - id: BAD_BATCH\n"
            "    description: broken batch rule\n"
            "    scope: batch\n"
            "    predicate: not_registered\n",
            encoding="utf-8",
        )

        with pytest.raises(ConstitutionError, match="not_registered"):
            load_constitution(yaml_path)

    def test_soft_prefer_unknown_scorer_is_rejected(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "constitution.yaml"
        yaml_path.write_text(
            "soft_prefer:\n"
            "  - id: BAD_SCORER\n"
            "    description: broken scorer\n"
            "    scorer: not_registered\n",
            encoding="utf-8",
        )

        with pytest.raises(ConstitutionError, match="not_registered"):
            load_constitution(yaml_path)
