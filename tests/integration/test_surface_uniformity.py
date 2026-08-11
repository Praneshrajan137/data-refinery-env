"""Cross-surface uniformity contract.

Enforces the product invariant that every surface (CLI, MCP, verified agent,
playground) shares ONE write primitive and produces ONE self-verifying trust
certificate.

Scope, stated honestly (this is defense-in-depth, not one silver bullet):

* ``test_every_write_primitive_is_registered`` + ``test_registry_has_no_stale_entries``
  + ``test_every_user_data_write_names_its_gate`` are STATIC guards over the
  ``_WRITE_PRIMITIVE_REGISTRY`` below. They prove that every write site in every package
  in this repo has been classified, and that each one able to touch user data names the
  gate protecting it. They are a regex scan, so a write reached through an alias,
  ``getattr``, or an entry point will not appear.
* ``test_raw_byte_writer_is_not_public`` pins the one write that CANNOT be gated:
  ``_apply_fixes_to_csv`` takes ``CellFix``, which has no provenance, and performs no
  journalling or locking. Privacy, not a gate, is what keeps it off other surfaces.
* The RUNTIME no-corruption / reversibility guarantee is the real safety net:
  ``tests/property/test_no_corruption_invariant.py`` (a correct cell is never changed;
  nothing unproven is auto-applied on ANY write surface) and
  ``tests/property/test_revert_is_bytes_identical.py`` (every applied change is
  byte-for-byte reversible). ``test_pipeline_writes_are_journaled_and_reversible``
  below ties that to this file.

Neither layer alone is claimed to be complete.

Two lessons are encoded here, both learned from real failures on 2026-08-09:

1. **Registries are keyed by primitive, not by caller.** This file used to allowlist the
   *callers* of ``apply_transaction``. That could not see ``DuckDBStore``'s raw SQL,
   ``revert_transaction``'s direct ``atomic_write_bytes``, or the constraints-artifact
   rewrite -- three genuine user-data writes. Callers are unbounded; primitives are ~30
   and change rarely. The allowlist had also rotted, carrying ``cli/repair.py`` on the
   strength of a wrapper with no callers at all, which is why
   ``test_registry_has_no_stale_entries`` now exists.
2. **A guard that delegates must name what the other guard covers.** This file used to
   delegate "nothing unverified is auto-applied" to
   ``test_no_corruption_invariant.py``, while that file only ever exercised
   ``run_repair_pipeline``. Each guard assumed the other covered the agent and
   table-store surfaces, and neither did -- so an unproven LLM value could be written
   for four weeks with a green suite. The delegation above is sound only because that
   file now parametrizes the invariant over every write surface. If you add a surface,
   add it there too; being in this registry is classification, not coverage.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import dataforge.engine as engine_pkg
from dataforge.agent import AgentRepairRequest, run_agent_repair
from dataforge.certificate import reverify_certificate, verify_certificate
from dataforge.cli.common import load_schema
from dataforge.engine.repair import (
    ExternalFix,
    RepairPipelineRequest,
    RepairReceipt,
    VerifyAndApplyRequest,
    run_repair_pipeline,
    verify_and_apply,
)
from dataforge.transactions.revert import revert_transaction

REPO_ROOT = Path(__file__).resolve().parents[2]
DATAFORGE_PKG = REPO_ROOT / "dataforge"

# ── Write-primitive registry ────────────────────────────────────────────────────
#
# WHY THIS IS KEYED BY PRIMITIVE AND NOT BY CALLER.
#
# Until 2026-08-09 this file allowlisted the *callers* of ``apply_transaction``. That
# design failed twice over:
#   * It saw one primitive. ``DuckDBStore.apply_patch_plan`` issues raw SQL, and
#     ``revert_transaction`` calls ``atomic_write_bytes`` directly -- both invisible to a
#     scan for ``apply_transaction(``.
#   * Callers are unbounded and grow with every surface; the list rotted (it carried
#     ``cli/repair.py`` on the strength of a wrapper that had no callers at all).
#
# Primitives are few (29 entries, and they change rarely) and every write must go
# through one. So the registry enumerates PRIMITIVES and requires each to be classified.
# An unregistered write fails the scan; a registry entry whose write has been deleted
# ALSO fails, so the registry cannot rot the way the allowlist did.
#
# STATED LIMITS (do not upgrade these to guarantees):
#   * This is a regex scan of source text. A write reached through an alias, ``getattr``,
#     an entry point, or a C extension will not appear. Widening the patterns already
#     caught one real miss: ``schema_inference.py:333`` uses ``os.fdopen``, which an
#     earlier ``.open("wb")``-only pattern did not match.
#   * Being registered is not proof of safety. It is proof that somebody classified it.
#     The runtime guarantee is ``tests/property/test_no_corruption_invariant.py``.

_WritePrimitiveKind = Literal["user_data", "metadata", "scratch", "read_only"]

_WRITE_PATTERNS: dict[str, re.Pattern[str]] = {
    "open_write": re.compile(r"(?:\.|\b)(?:open|fdopen)\(\s*[^)]*?[\"'][wxa]b?\+?[\"']"),
    "os_write": re.compile(r"os\.write\("),
    "write_bytes": re.compile(r"\.write_bytes\("),
    "write_text": re.compile(r"\.write_text\("),
    "writelines": re.compile(r"\.writelines\("),
    "os_replace": re.compile(r"os\.replace\("),
    "sql_mutate": re.compile(r"execute\(\s*[\"']?\s*(UPDATE|INSERT|DELETE|MERGE|COMMIT)", re.I),
    "sql_exec_var": re.compile(r"\.execute\(sql\)"),
    "unlink": re.compile(r"\.unlink\("),
    "shutil_mutate": re.compile(r"shutil\.(move|copy|copy2|copyfile|rmtree)\("),
}

# Every distribution in the repo, so a side package cannot write outside the registry.
_SCAN_ROOTS: dict[str, Path] = {
    "dataforge": DATAFORGE_PKG,
    "mcp": REPO_ROOT / "dataforge-mcp" / "dataforge_mcp",
    "dbt": REPO_ROOT / "packages" / "dataforge-dbt" / "dataforge_dbt",
    "evals": REPO_ROOT / "packages" / "dataforge-evals" / "dataforge_evals",
    "patterns": REPO_ROOT / "packages" / "dataforge-agent-patterns" / "src",
}


@dataclass(frozen=True)
class WritePrimitive:
    """One classified write site. ``gate`` is required for ``user_data``."""

    location: str
    token: str
    kind: _WritePrimitiveKind
    note: str
    gate: str = ""


_WRITE_PRIMITIVE_REGISTRY: tuple[WritePrimitive, ...] = (
    # ---- user data: the writes that can damage what the user gave us ----
    WritePrimitive(
        "dataforge:transactions/files.py",
        "open_write",
        "user_data",
        "atomic_write_bytes: the single leaf that can rewrite a user CSV.",
        gate="Callers only: apply_transaction (enforce_proven_only) and "
        "revert_transaction (audit verdict + post-state hash match).",
    ),
    WritePrimitive(
        "dataforge:transactions/files.py",
        "os_replace",
        "user_data",
        "The atomic rename that publishes the payload written above.",
        gate="Same as the open_write entry: it is the second half of one operation.",
    ),
    WritePrimitive(
        "dataforge:stores/duckdb.py",
        "sql_mutate",
        "user_data",
        "COMMIT of forward/rollback SQL against the user's relation.",
        gate="enforce_plan_proven_only at the top of apply_patch_plan.",
    ),
    WritePrimitive(
        "dataforge:stores/duckdb.py",
        "sql_exec_var",
        "user_data",
        "Executes plan.forward_sql / rollback_sql (UPDATE). Line 214 of the same "
        "module is a scalar READ that this pattern over-matches; kept in one entry "
        "because the scan is per-module, not per-line.",
        gate="enforce_plan_proven_only at the top of apply_patch_plan.",
    ),
    WritePrimitive(
        "dataforge:schema_inference.py",
        "open_write",
        "user_data",
        "Rewrites the user's CONSTRAINTS artifact, not their table. Mutating it "
        "changes which constraints count as authoritative, i.e. it moves the premise "
        "of 'proven' rather than a value. See docs/trust/authority-is-mutable.md.",
        gate="validate_constraint_review_artifact + human review via "
        "'dataforge constraints review'. Deliberately NOT proven-only: there is no "
        "fix whose strength could be judged.",
    ),
    WritePrimitive(
        "dataforge:schema_inference.py",
        "os_replace",
        "user_data",
        "Atomic publish of the constraints artifact written above.",
        gate="Same as the schema_inference open_write entry.",
    ),
    WritePrimitive(
        "dataforge:schema_inference.py",
        "unlink",
        "user_data",
        "Cleanup of the constraints temp file on failure.",
        gate="Failure path only; removes the temp file, never the user's artifact.",
    ),
    # ---- metadata: journals, snapshots, caches, reports ----
    WritePrimitive(
        "dataforge:engine/repair.py",
        "open_write",
        "metadata",
        "_write_snapshot_once: the immutable pre-apply snapshot that makes revert "
        "possible. Writing this is what MAKES a write reversible.",
    ),
    WritePrimitive(
        "dataforge:engine/repair.py", "unlink", "metadata", "Snapshot cleanup on failure."
    ),
    WritePrimitive("dataforge:transactions/files.py", "unlink", "metadata", "Lock-file release."),
    WritePrimitive(
        "dataforge:transactions/files.py",
        "os_write",
        "metadata",
        "Writes pid+timestamp into the exclusive lock file. The lock is what makes "
        "concurrent writes to one source safe, so this write protects user data "
        "rather than touching it.",
    ),
    WritePrimitive(
        "dataforge:stores/duckdb.py", "open_write", "metadata", "Table-store snapshot write."
    ),
    WritePrimitive(
        "dataforge:stores/duckdb.py", "unlink", "metadata", "Table-store snapshot cleanup."
    ),
    WritePrimitive("dataforge:spend.py", "write_text", "metadata", "Spend ledger receipts."),
    WritePrimitive("dataforge:bench/core.py", "write_text", "metadata", "Benchmark results."),
    WritePrimitive(
        "dataforge:bench/report.py", "write_text", "metadata", "Benchmark report/README refresh."
    ),
    WritePrimitive(
        "dataforge:cli/calibrate.py", "write_text", "metadata", "Calibration session artifact."
    ),
    WritePrimitive(
        "dataforge:cli/profile.py", "write_text", "metadata", "Inferred-constraints output."
    ),
    WritePrimitive(
        "dataforge:repairers/fd_violation.py", "write_text", "metadata", "LLM response cache."
    ),
    WritePrimitive(
        "dataforge:repairers/llm_corrector.py", "write_text", "metadata", "LLM response cache."
    ),
    WritePrimitive("dataforge:review/ranker.py", "write_text", "metadata", "LLM triage cache."),
    WritePrimitive(
        "dataforge:datasets/real_world.py",
        "write_bytes",
        "metadata",
        "RAHA benchmark dataset cache under ~/.dataforge/cache.",
    ),
    WritePrimitive(
        "dataforge:release/doctor.py", "unlink", "metadata", "Release-doctor temp cleanup."
    ),
    WritePrimitive(
        "dataforge:release/gate.py",
        "shutil_mutate",
        "scratch",
        "Release-gate scratch trees (packaging smoke tests).",
    ),
    WritePrimitive("dbt:dispatch.py", "write_text", "metadata", "dbt journal artifact."),
    WritePrimitive("dbt:dispatch.py", "unlink", "metadata", "dbt temp cleanup."),
    WritePrimitive("evals:report.py", "write_text", "metadata", "Evaluation report artifacts."),
    # ---- scratch: temp dirs and packaged fixtures, never user data ----
    WritePrimitive(
        "dataforge:certificate.py",
        "write_bytes",
        "scratch",
        "Re-verification writes the post-state into a TemporaryDirectory to re-read it.",
    ),
    WritePrimitive(
        "dataforge:cli/quickstart.py",
        "write_bytes",
        "scratch",
        "Copies a packaged fixture into a TemporaryDirectory; takes no user path.",
    ),
)


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


def _scan_write_primitives() -> dict[tuple[str, str], list[int]]:
    """Return {(location, token): line numbers} for every write site in every package."""
    found: dict[tuple[str, str], list[int]] = {}
    for label, root in _SCAN_ROOTS.items():
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            location = f"{label}:{path.relative_to(root).as_posix()}"
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                for token, pattern in _WRITE_PATTERNS.items():
                    if pattern.search(stripped):
                        found.setdefault((location, token), []).append(lineno)
    return found


def test_every_write_primitive_is_registered() -> None:
    """No code in any package may write without being classified in the registry.

    This replaced ``test_no_new_apply_transaction_caller`` on 2026-08-09. That test
    allowlisted CALLERS of one primitive and therefore could not see DuckDB's raw SQL,
    revert's direct ``atomic_write_bytes``, or the constraints-artifact rewrite -- three
    real user-data writes.
    """
    found = _scan_write_primitives()
    registered = {(entry.location, entry.token) for entry in _WRITE_PRIMITIVE_REGISTRY}

    unregistered = sorted(found.keys() - registered)
    assert not unregistered, (
        "Unregistered write primitive(s) found: "
        + "; ".join(f"{loc} [{tok}] at lines {found[(loc, tok)]}" for loc, tok in unregistered)
        + ". Every write must be classified in _WRITE_PRIMITIVE_REGISTRY as user_data, "
        "metadata, scratch or read_only. If it is user_data it must also name its gate."
    )


def test_registry_has_no_stale_entries() -> None:
    """A registry entry whose write no longer exists must be deleted.

    The predecessor allowlist rotted exactly this way: it carried ``cli/repair.py``
    because of ``_apply_transaction``, a wrapper with no callers anywhere. A stale entry
    is worse than a missing one -- it makes the registry look reviewed when it is not.
    """
    found = _scan_write_primitives()
    registered = {(entry.location, entry.token) for entry in _WRITE_PRIMITIVE_REGISTRY}

    stale = sorted(registered - found.keys())
    assert not stale, (
        f"Registry entries no longer match any write in the source: {stale}. "
        "Delete them; do not leave the registry describing code that is gone."
    )


def test_every_user_data_write_names_its_gate() -> None:
    """A write that can touch user data must say what protects it."""
    ungated = [
        (entry.location, entry.token)
        for entry in _WRITE_PRIMITIVE_REGISTRY
        if entry.kind == "user_data" and not entry.gate.strip()
    ]
    assert not ungated, f"user_data write(s) with no declared gate: {ungated}"


def test_registry_entries_are_unique() -> None:
    """Duplicate keys would let one entry silently mask another's classification."""
    keys = [(entry.location, entry.token) for entry in _WRITE_PRIMITIVE_REGISTRY]
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    assert not duplicates, f"Duplicate registry keys: {duplicates}"


def test_raw_byte_writer_is_not_public() -> None:
    """``_apply_fixes_to_csv`` must never be re-exported.

    It writes a user CSV with no journal, no snapshot and no lock, and it takes
    ``CellFix`` (no provenance) so the proven-only gate is undecidable there. It WAS in
    ``engine.__all__`` until 2026-08-09, which made an irreversible user-data write part
    of the supported API -- a reversibility hole, which is a stronger invariant in
    PRODUCT.md than the proven-only one.
    """
    assert "apply_fixes_to_csv" not in engine_pkg.__all__
    assert not hasattr(engine_pkg, "apply_fixes_to_csv")

    callers = {
        path.relative_to(DATAFORGE_PKG).as_posix()
        for path in DATAFORGE_PKG.rglob("*.py")
        if "_apply_fixes_to_csv(" in path.read_text(encoding="utf-8")
    }
    assert callers == {"engine/repair.py"}, (
        f"The raw byte-writer must only be used inside engine/repair.py; found {callers}"
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


def test_verify_and_apply_shares_one_certificate(tmp_path: Path) -> None:
    """The external-fix entry emits the SAME repair_receipt_v1, and it verifies.

    verify_and_apply routes writes through the allowlisted engine apply path (so
    the no-parallel-write-path guard already covers it) and must produce the same
    certificate any other surface does.
    """
    csv, schema = _decimal_shift_case(tmp_path)
    pipeline = run_repair_pipeline(
        RepairPipelineRequest(source_path=csv, mode="dry_run", schema=schema)
    ).receipt
    external = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=csv,
            fixes=[ExternalFix(row=3, column="amount", new_value="102", expected_old_value="1020")],
            mode="apply",
            schema=schema,
            confirm_escalations=True,
        )
    ).receipt
    assert external.schema_version == pipeline.schema_version == "repair_receipt_v1"
    assert set(external.model_dump().keys()) == set(pipeline.model_dump().keys())
    assert external.applied is True
    verification = verify_certificate(external.model_dump(mode="json"), data_bytes=csv.read_bytes())
    assert verification.ok, [c for c in verification.checks if not c.ok]
