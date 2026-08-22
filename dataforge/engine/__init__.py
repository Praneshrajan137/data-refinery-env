"""Public backend engine APIs for DataForge."""

from dataforge.engine.repair import (
    CandidateFix,
    CandidateRepair,
    ProofObligation,
    RepairFailure,
    RepairMode,
    RepairPipelineRequest,
    RepairPipelineResult,
    RepairReceipt,
    RootCause,
    UncheckableDetectorWriteError,
    UnprovenWriteError,
    VerifiedFix,
    apply_transaction,
    create_repair_transaction,
    enforce_proven_only,
    propose_repairs,
    run_repair_pipeline,
    source_path_lock,
)

__all__ = [
    "CandidateFix",
    "CandidateRepair",
    "ProofObligation",
    "RepairFailure",
    "RepairMode",
    "RepairPipelineRequest",
    "RepairPipelineResult",
    "RepairReceipt",
    "RootCause",
    "UncheckableDetectorWriteError",
    "UnprovenWriteError",
    "VerifiedFix",
    # NOTE: ``apply_fixes_to_csv`` is deliberately absent. It is the raw byte-writer
    # beneath ``apply_transaction`` -- no journal, no snapshot, no lock, and it takes
    # ``CellFix`` which has no provenance, so the proven-only gate is undecidable there.
    # It was public until 2026-08-09, which made an irreversible user-data write part of
    # the supported API. Pinned by ``test_raw_byte_writer_is_not_public``.
    "apply_transaction",
    "create_repair_transaction",
    "enforce_proven_only",
    "propose_repairs",
    "run_repair_pipeline",
    "source_path_lock",
]
