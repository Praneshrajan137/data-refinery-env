"""Unit tests for DataForge MCP tool functions."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from dataforge_mcp.server import create_server
from dataforge_mcp.tools import (
    configure_mcp_security,
    dataforge_apply_repairs,
    dataforge_detect_errors,
    dataforge_profile,
    dataforge_revert,
    dataforge_verify_fix,
)


def _write_repairable_csv(path: Path) -> None:
    """Write a small CSV with a deterministic decimal-shift repair."""
    path.write_text(
        "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        encoding="utf-8",
    )


def _fix_spec(path: Path, *, old_value: str = "1020", new_value: str = "102") -> dict[str, object]:
    """Build a verifier payload for the decimal-shift fixture."""
    return {
        "path": str(path),
        "fix": {
            "row": 3,
            "column": "amount",
            "old_value": old_value,
            "new_value": new_value,
            "detector_id": "decimal_shift",
        },
        "reason": "candidate decimal-shift repair",
        "confidence": 0.9,
        "provenance": "deterministic",
    }


@pytest.fixture(autouse=True)
def _mcp_security(tmp_path: Path) -> None:
    """Allow each test's temporary files and enable explicit apply coverage."""
    configure_mcp_security(enable_apply=True, allowed_roots=[tmp_path])


class TestDataForgeMcpTools:
    """Direct coverage for MCP tool behavior."""

    def test_server_registers_expected_tools(self) -> None:
        server = create_server()

        tools = server._tool_manager.list_tools()
        names = {tool.name for tool in tools}

        assert names == {
            "dataforge_profile",
            "dataforge_detect_errors",
            "dataforge_verify_fix",
            "dataforge_apply_repairs",
            "dataforge_revert",
        }

    def test_registered_tools_have_output_schemas(self) -> None:
        server = create_server()

        for tool in server._tool_manager.list_tools():
            schema = tool.output_schema
            assert schema["type"] == "object"
            assert "properties" in schema

    def test_profile_and_detect_errors_return_decimal_shift_issue(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "amounts.csv"
        _write_repairable_csv(csv_path)

        profile = dataforge_profile(str(csv_path))
        issues = dataforge_detect_errors(str(csv_path))

        assert profile.rows == 5
        assert profile.columns == 2
        assert profile.total_issues >= 1
        assert any(issue.issue_type == "decimal_shift" for issue in issues)

    def test_verify_fix_accepts_valid_candidate(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "amounts.csv"
        _write_repairable_csv(csv_path)

        result = dataforge_verify_fix(_fix_spec(csv_path))

        assert result.accept is True
        assert result.safety_verdict == "allow"
        assert result.verifier_verdict == "accept"

    def test_verify_fix_rejects_stale_candidate(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "amounts.csv"
        _write_repairable_csv(csv_path)

        result = dataforge_verify_fix(_fix_spec(csv_path, old_value="999"))

        assert result.accept is False
        assert "stale fix" in result.reason.lower()

    def test_dry_run_does_not_mutate_source(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "amounts.csv"
        _write_repairable_csv(csv_path)
        original = csv_path.read_bytes()

        receipt = dataforge_apply_repairs(str(csv_path), "dry_run")

        assert receipt.receipt_version == "repair_receipt_v1"
        assert receipt.applied is False
        assert receipt.txn_id is None
        assert receipt.fixes_count >= 1
        assert receipt.root_causes
        assert receipt.candidate_repairs
        assert receipt.proof_obligations
        assert receipt.patch_plan_sha256 is not None
        assert receipt.limitations
        assert csv_path.read_bytes() == original

    def test_apply_requires_explicit_enablement(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "amounts.csv"
        _write_repairable_csv(csv_path)
        configure_mcp_security(enable_apply=False, allowed_roots=[tmp_path])

        with pytest.raises(ValueError, match="apply mode is disabled"):
            dataforge_apply_repairs(str(csv_path), "apply")

    def test_apply_then_revert_restores_source_bytes(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "amounts.csv"
        _write_repairable_csv(csv_path)
        original = csv_path.read_bytes()

        receipt = dataforge_apply_repairs(str(csv_path), "apply")

        assert receipt.applied is True
        assert receipt.txn_id is not None
        assert re.fullmatch(r"txn-\d{4}-\d{2}-\d{2}-[0-9a-f]{6}", receipt.txn_id)
        assert csv_path.read_bytes() != original

        revert = dataforge_revert(receipt.txn_id)

        assert revert.restored is True
        assert csv_path.read_bytes() == original
