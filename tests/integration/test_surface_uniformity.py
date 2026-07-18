"""Cross-surface uniformity contract.

Enforces the product invariant that every surface (CLI, MCP, verified agent,
playground) shares ONE write primitive and produces ONE self-verifying trust
certificate. A new surface that introduces a parallel write path, or an agent
result that cannot be verified as a ``repair_receipt_v1``, fails here.
"""

from __future__ import annotations

from pathlib import Path

from dataforge.agent import AgentRepairRequest, run_agent_repair
from dataforge.certificate import verify_certificate
from dataforge.cli.common import load_schema
from dataforge.engine.repair import RepairPipelineRequest, RepairReceipt, run_repair_pipeline

DATAFORGE_PKG = Path(__file__).resolve().parents[2] / "dataforge"

# The single journaled-write primitive is ``engine.repair.apply_transaction``.
# These are the only modules permitted to call it: the pipeline (defines it),
# the agent controller, the CLI delegating wrapper, and the CSV store boundary.
# A new caller means a new surface is writing outside the blessed path -> review.
_WRITE_CALLER_ALLOWLIST = {
    "engine/repair.py",
    "agent/controller.py",
    "cli/repair.py",
    "stores/csv.py",
}


def _decimal_shift_case(tmp_path: Path) -> tuple[Path, object]:
    """A CSV + schema whose deterministic floor auto-applies one proven fix."""
    csv = tmp_path / "amounts.csv"
    csv.write_text(
        "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n6,101\n7,99\n8,102\n",
        encoding="utf-8",
    )
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(
        "columns:\n  id: str\n  amount: float\n"
        "domain_bounds:\n  amount:\n    min: 0\n    max: 5000\n",
        encoding="utf-8",
    )
    return csv, load_schema(schema_path)


def test_single_write_primitive_is_defined_once() -> None:
    """There is exactly one journaled-write primitive, in engine/repair.py."""
    definitions = [
        path.relative_to(DATAFORGE_PKG).as_posix()
        for path in DATAFORGE_PKG.rglob("*.py")
        if "def apply_transaction(" in path.read_text(encoding="utf-8")
    ]
    assert definitions == ["engine/repair.py"], (
        f"apply_transaction must be defined exactly once in engine/repair.py; found {definitions}"
    )


def test_no_surface_introduces_a_parallel_write_path() -> None:
    """Every caller of the write primitive is on the reviewed allowlist."""
    callers: set[str] = set()
    for path in DATAFORGE_PKG.rglob("*.py"):
        rel = path.relative_to(DATAFORGE_PKG).as_posix()
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if "apply_transaction(" not in stripped:
                continue
            if stripped.startswith(("def ", "from ", "import ")):
                continue
            callers.add(rel)
    unexpected = callers - _WRITE_CALLER_ALLOWLIST
    assert not unexpected, (
        "New surface(s) call the write primitive outside the reviewed allowlist "
        f"(possible parallel write path): {sorted(unexpected)}. If intentional, add "
        "to _WRITE_CALLER_ALLOWLIST and confirm it routes through apply_transaction."
    )


def test_pipeline_receipt_is_a_verifiable_certificate(tmp_path: Path) -> None:
    """The deterministic pipeline emits a certificate that verifies against data."""
    csv, schema = _decimal_shift_case(tmp_path)
    result = run_repair_pipeline(
        RepairPipelineRequest(source_path=csv, mode="apply", schema=schema)
    )
    assert result.receipt.applied is True
    verification = verify_certificate(
        result.receipt.model_dump(mode="json"), data_bytes=csv.read_bytes()
    )
    assert verification.ok, [c for c in verification.checks if not c.ok]


def test_agent_receipt_is_a_verifiable_certificate(tmp_path: Path) -> None:
    """The agent surface emits the SAME certificate, and it verifies."""
    csv, schema = _decimal_shift_case(tmp_path)
    result = run_agent_repair(
        AgentRepairRequest(source_path=csv, mode="apply", schema=schema, policy="deterministic")
    )
    assert result.applied is True
    receipt = result.to_receipt()
    assert isinstance(receipt, RepairReceipt)
    verification = verify_certificate(receipt.model_dump(mode="json"), data_bytes=csv.read_bytes())
    assert verification.ok, [c for c in verification.checks if not c.ok]


def test_agent_and_pipeline_certificates_share_one_schema(tmp_path: Path) -> None:
    """Both surfaces produce the identical certificate schema and field set."""
    csv, schema = _decimal_shift_case(tmp_path)
    pipeline = run_repair_pipeline(
        RepairPipelineRequest(source_path=csv, mode="apply", schema=schema)
    ).receipt
    agent = run_agent_repair(
        AgentRepairRequest(source_path=csv, mode="apply", schema=schema, policy="deterministic")
    ).to_receipt()
    assert pipeline.schema_version == agent.schema_version == "repair_receipt_v1"
    assert set(pipeline.model_dump().keys()) == set(agent.model_dump().keys())
