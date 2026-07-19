"""Cross-surface uniformity contract.

Enforces the product invariant that every surface (CLI, MCP, verified agent,
playground) shares ONE write primitive and produces ONE self-verifying trust
certificate.

Scope, stated honestly (this is a defense-in-depth pair, not one silver bullet):

* ``test_single_write_primitive_is_defined_once`` + ``test_no_new_apply_transaction_caller``
  are STATIC guards: they prove the journaled-write primitive is defined once and
  is only *called* from a reviewed allowlist. They do NOT, by themselves, prove a
  surface cannot mutate data by some OTHER mechanism (e.g. a raw ``to_csv``); a
  determined new write path via a different call would slip past a string scan.
* The RUNTIME no-corruption / reversibility guarantee is enforced elsewhere and
  is the real safety net: ``tests/property/test_no_corruption_invariant.py`` (a
  correct cell is never changed; nothing unverified is auto-applied) and
  ``tests/property/test_revert_is_bytes_identical.py`` (every applied change is
  byte-for-byte reversible). ``test_pipeline_writes_are_journaled_and_reversible``
  below ties that to this file: an applied repair is journaled and reverts exactly.

Together the static allowlist (catches the easy regression: a new
``apply_transaction`` caller) and the runtime invariants (catch the hard one:
corruption/irreversibility regardless of mechanism) give the guarantee; neither
alone is claimed to be complete.
"""

from __future__ import annotations

from pathlib import Path

from dataforge.agent import AgentRepairRequest, run_agent_repair
from dataforge.certificate import reverify_certificate, verify_certificate
from dataforge.cli.common import load_schema
from dataforge.engine.repair import RepairPipelineRequest, RepairReceipt, run_repair_pipeline
from dataforge.transactions.revert import revert_transaction

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


def test_no_new_apply_transaction_caller() -> None:
    """STATIC guard: every caller of the journaled-write primitive is allowlisted.

    This catches the common regression -- a new surface calling ``apply_transaction``
    directly. It is a string scan, so it does NOT prove a surface cannot write by
    another mechanism; the runtime no-corruption/reversibility invariants (see the
    module docstring) are what guarantee safety regardless of mechanism.
    """
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


def test_pipeline_writes_are_journaled_and_reversible(tmp_path: Path) -> None:
    """RUNTIME guard: an applied repair is journaled and reverts byte-for-byte.

    Proves the sanctioned write path actually goes through the transaction journal
    (a txn id is issued) and is exactly reversible -- the real safety property, not
    just that a specific function name was called.
    """
    csv, schema = _decimal_shift_case(tmp_path)
    before = csv.read_bytes()
    result = run_repair_pipeline(
        RepairPipelineRequest(source_path=csv, mode="apply", schema=schema)
    )
    assert result.receipt.applied is True
    assert result.receipt.txn_id, "an applied write must carry a journal transaction id"
    assert csv.read_bytes() != before, "the apply must actually mutate the file"
    revert_transaction(result.receipt.txn_id, search_root=tmp_path)
    assert csv.read_bytes() == before, "the journaled write must revert byte-for-byte"


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
    """The agent surface emits the SAME certificate, and it verifies (shallow + deep)."""
    csv, schema = _decimal_shift_case(tmp_path)
    result = run_agent_repair(
        AgentRepairRequest(source_path=csv, mode="apply", schema=schema, policy="deterministic")
    )
    assert result.applied is True
    receipt = result.to_receipt()
    assert isinstance(receipt, RepairReceipt)
    post_bytes = csv.read_bytes()

    shallow = verify_certificate(receipt.model_dump(mode="json"), data_bytes=post_bytes)
    assert shallow.ok, [c for c in shallow.checks if not c.ok]

    # Deep re-verification: re-runs the real verifier per applied cell against the
    # certified bytes AND checks the recorded verification_strength labels are
    # truthful. This is the claim the prior session left unproven.
    deep = reverify_certificate(
        receipt.model_dump(mode="json"), data_bytes=post_bytes, schema=schema
    )
    assert deep.ok, [c for c in deep.checks if not c.ok]
    assert all(f.verification_strength == "proven" for f in receipt.applied_fixes)


def test_agent_receipt_reverify_catches_tampering(tmp_path: Path) -> None:
    """Deep re-verification of the agent certificate detects tampered bytes."""
    csv, schema = _decimal_shift_case(tmp_path)
    result = run_agent_repair(
        AgentRepairRequest(source_path=csv, mode="apply", schema=schema, policy="deterministic")
    )
    receipt = result.to_receipt().model_dump(mode="json")
    tampered = b"id,amount\n1,999999\n"  # not the certified post-state
    deep = reverify_certificate(receipt, data_bytes=tampered, schema=schema)
    assert not deep.ok, "reverify must reject bytes that do not match the certificate"


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
