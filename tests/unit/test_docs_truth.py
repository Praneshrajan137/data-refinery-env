"""The docs-truth checker must actually bite.

This checker exists because the API phase published five false or mis-scoped numbers in
prose, none of which CI could catch: `readme_truth.py` polices claim *kinds*,
`benchmark_truth.py` regenerates marker blocks, and `openapi_contract.py` diffs snapshots.
A hand-written number in `DECISIONS.md` was unguarded.

A truth checker that cannot fail is worse than none, because it manufactures confidence.
These tests therefore verify the failure modes, not just the happy path.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHECKER = PROJECT_ROOT / "scripts" / "ci" / "docs_truth.py"
LEDGER = PROJECT_ROOT / "docs" / "quantitative_claims.yaml"


def _run() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), "--check"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )


class TestLedgerIsWellFormed:
    def test_ledger_exists_and_parses(self) -> None:
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        assert isinstance(payload["claims"], list)
        assert payload["claims"], "an empty ledger would make the checker vacuous"

    def test_every_claim_is_complete(self) -> None:
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        seen: set[str] = set()
        for claim in payload["claims"]:
            for field in ("id", "doc", "artifact", "pointer", "expect"):
                assert field in claim, f"{claim.get('id')} missing {field}"
            assert claim["id"] not in seen, f"duplicate claim id {claim['id']}"
            seen.add(str(claim["id"]))

    def test_the_disconfirming_flights_claim_is_bound(self) -> None:
        """The number that refuted a published claim must stay under CI."""
        yaml = pytest.importorskip("yaml")
        payload = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        ids = {str(claim["id"]) for claim in payload["claims"]}
        assert "flights_llm_ranker_at_chance" in ids
        assert "free_ranker_transfer_rayyan" in ids


class TestCheckerPasses:
    def test_committed_state_is_consistent(self) -> None:
        result = _run()
        assert result.returncode == 0, f"docs truth check failing:\n{result.stderr}"
        assert "Verified" in result.stdout


class TestCheckerDetectsDivergence:
    """Both directions of divergence must fail: artifact drift and prose falsification."""

    def test_artifact_drift_is_detected(self) -> None:
        artifact = PROJECT_ROOT / "eval" / "results" / "free_vs_llm_ranker.json"
        if not artifact.exists():
            pytest.skip("free ranker artifact not committed")
        original = artifact.read_bytes()
        try:
            payload = json.loads(original.decode("utf-8"))
            block = payload["regimes"]["default"]["leave_one_dataset_out"]["rayyan"]
            block["free_transfer_roc_auc"] = 0.95
            artifact.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = _run()
            assert result.returncode == 1, "silent artifact drift was not detected"
            assert "free_ranker_transfer_rayyan" in result.stderr
        finally:
            artifact.write_bytes(original)
        assert _run().returncode == 0, "checker did not recover after restore"

    def test_prose_falsification_is_detected(self) -> None:
        doc = PROJECT_ROOT / "DECISIONS.md"
        original = doc.read_bytes()
        try:
            doc.write_text(original.decode("utf-8").replace("0.2722", "0.9500"), encoding="utf-8")
            result = _run()
            assert result.returncode == 1, "a falsified prose number was not detected"
            assert "does not contain" in result.stderr
        finally:
            doc.write_bytes(original)
        assert _run().returncode == 0, "checker did not recover after restore"

    def test_missing_artifact_field_is_detected(self) -> None:
        artifact = PROJECT_ROOT / "eval" / "results" / "free_vs_llm_ranker.json"
        if not artifact.exists():
            pytest.skip("free ranker artifact not committed")
        original = artifact.read_bytes()
        try:
            payload = json.loads(original.decode("utf-8"))
            del payload["regimes"]["default"]["leave_one_dataset_out"]["rayyan"][
                "free_transfer_roc_auc"
            ]
            artifact.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = _run()
            assert result.returncode == 1, "a removed artifact field was not detected"
        finally:
            artifact.write_bytes(original)


class TestCheckerIsWiredIntoCi:
    def test_backend_gate_runs_it(self) -> None:
        text = (PROJECT_ROOT / "scripts" / "ci" / "backend_gate.py").read_text(encoding="utf-8")
        assert "docs_truth.py" in text, "the checker is not run by the aggregate gate"
        assert '"scripts/ci/docs_truth.py"' in text, "the checker is not type-checked in CI"

    def test_makefile_type_target_includes_it(self) -> None:
        text = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
        assert "scripts/ci/docs_truth.py" in text
