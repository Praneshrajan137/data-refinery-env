"""The test-map coverage ratchet must actually bite.

The value of this gate is narrow and worth stating precisely, because overstating it would be
the same defect it guards against. An unmapped module falls back to the **full** suite
(``scripts/test_mapped.py:94-95``), so a gap in the map costs *speed*, never correctness. What
the gate prevents is the gap growing silently, and what it deliberately does not do is force
mappings to be invented in bulk -- a wrong mapping runs the wrong tests and reports green, so 76
hastily-written entries would be 76 chances to be wrong.

These tests therefore check the three failure modes, not the happy path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.ci import test_map_coverage

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def payload() -> dict[str, Any]:
    """The committed test map, parsed."""
    return json.loads((PROJECT_ROOT / "test_map.json").read_text(encoding="utf-8"))


class TestTheCommittedStateIsCovered:
    def test_every_module_has_a_decision(self) -> None:
        assert test_map_coverage.errors() == []

    def test_the_declaration_list_is_not_empty(self, payload: dict[str, Any]) -> None:
        """Non-vacuity: an empty list would make the ratchet trivially satisfiable.

        If every module were mapped this would legitimately be empty, but that is not the
        current state, and a silently-emptied list is how the gate would stop meaning anything.
        """
        declared = payload[test_map_coverage.UNMAPPED_KEY]
        assert declared, "either every module is mapped, or this list records the ones that are not"


class TestTheGateDetectsEachFailureMode:
    """Each case perturbs an in-memory copy; the committed file is never written."""

    def _errors_for(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, payload: dict[str, Any]
    ) -> list[str]:
        target = tmp_path / "test_map.json"
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        monkeypatch.setattr(test_map_coverage, "TEST_MAP", target)
        return test_map_coverage.errors()

    def test_an_undecided_module_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, payload: dict[str, Any]
    ) -> None:
        """The case a new file creates: neither mapped nor declared."""
        dropped = payload[test_map_coverage.UNMAPPED_KEY].pop()

        problems = self._errors_for(monkeypatch, tmp_path, payload)

        assert any("neither mapped nor declared" in problem for problem in problems)
        assert any(dropped in problem for problem in problems)

    def test_a_module_both_mapped_and_declared_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, payload: dict[str, Any]
    ) -> None:
        """A contradiction the reader cannot resolve must not pass."""
        already_mapped = next(key for key in payload if key.startswith("dataforge/"))
        payload[test_map_coverage.UNMAPPED_KEY].append(already_mapped)

        problems = self._errors_for(monkeypatch, tmp_path, payload)

        assert any("BOTH mapped and declared" in problem for problem in problems)

    def test_a_stale_declaration_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, payload: dict[str, Any]
    ) -> None:
        """A path that no longer exists keeps a closed gap looking open."""
        payload[test_map_coverage.UNMAPPED_KEY].append("dataforge/deleted_module.py")

        problems = self._errors_for(monkeypatch, tmp_path, payload)

        assert any("do not exist" in problem for problem in problems)

    def test_a_malformed_declaration_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, payload: dict[str, Any]
    ) -> None:
        payload[test_map_coverage.UNMAPPED_KEY] = {"not": "a list"}

        problems = self._errors_for(monkeypatch, tmp_path, payload)

        assert any("must be a list" in problem for problem in problems)


class TestTheGateIsWiredIn:
    def test_the_makefile_runs_it(self) -> None:
        text = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
        assert "test_map_coverage.py" in text, "an unrun gate polices nothing"

    def test_the_backend_gate_runs_it(self) -> None:
        text = (PROJECT_ROOT / "scripts" / "ci" / "backend_gate.py").read_text(encoding="utf-8")
        assert "test_map_coverage.py" in text
