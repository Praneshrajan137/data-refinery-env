"""Public API and integration import-boundary tests."""

from __future__ import annotations

from pathlib import Path

import dataforge

EXPECTED_PUBLIC_EXPORTS = {
    "CONTRACT_VERSION",
    "CandidateFix",
    "CandidateRepair",
    "CellScore",
    "ReviewRanker",
    "CellFix",
    "ExternalFix",
    "ConstraintCandidate",
    "ConstraintReviewArtifact",
    "ConstraintReviewError",
    "ConstraintIR",
    "DuckDBStore",
    "Issue",
    "PatchPlan",
    "ProposedFix",
    "ProofObligation",
    "RepairFailure",
    "RepairPipelineRequest",
    "RepairPipelineResult",
    "RepairReceipt",
    "RepairTransaction",
    "RootCause",
    "ReviewedConstraintCandidate",
    "SMTVerifier",
    "SafetyContext",
    "SafetyFilter",
    "SafetyResult",
    "SafetyVerdict",
    "Schema",
    "SchemaInferenceResult",
    "Severity",
    "TransactionAuditReport",
    "TransactionAuditVerdict",
    "TransactionLogError",
    "TransactionRevertError",
    "TableStoreError",
    "TableStoreRepairResult",
    "VerificationResult",
    "VerificationVerdict",
    "VerifiedFix",
    "VerifyAndApplyRequest",
    "verify_and_apply",
    "build_constraint_review_artifact",
    "constraint_ir_from_schema",
    "dump_constraint_review_artifact",
    "load_schema",
    "load_constraint_review_artifact",
    "read_csv",
    "revert_transaction",
    "run_all_detectors",
    "run_repair_pipeline",
    "schema_from_mapping",
    "schema_from_dbt_artifacts",
    "schema_from_dbt_manifest",
    "infer_schema",
    "is_table_store_uri",
    "run_table_store_repair",
    "store_from_uri",
    "verify_transaction_log",
}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_ENTRYPOINTS = [
    PROJECT_ROOT / "dataforge-mcp" / "dataforge_mcp" / "tools.py",
    PROJECT_ROOT / "playground" / "api" / "app.py",
    PROJECT_ROOT / "packages" / "dataforge-dbt" / "dataforge_dbt" / "dispatch.py",
]
BANNED_INTEGRATION_IMPORTS = (
    "from dataforge.detectors",
    "import dataforge.detectors",
    "from dataforge.repairers",
    "import dataforge.repairers",
    "from dataforge.engine",
    "import dataforge.engine",
    "from dataforge.safety",
    "import dataforge.safety",
    "from dataforge.verifier",
    "import dataforge.verifier",
    "from dataforge.transactions",
    "import dataforge.transactions",
)


def test_root_facade_exports_integration_surface() -> None:
    """The root package exposes the supported integration API."""
    assert set(dataforge.__all__) == EXPECTED_PUBLIC_EXPORTS | {"__version__"}

    namespace: dict[str, object] = {}
    exec(
        "from dataforge import " + ", ".join(sorted(EXPECTED_PUBLIC_EXPORTS)),
        namespace,
    )

    for name in EXPECTED_PUBLIC_EXPORTS:
        assert namespace[name] is getattr(dataforge, name)


def test_integrations_import_from_root_public_facade() -> None:
    """External integration entrypoints should not import core internals."""
    available = [path for path in INTEGRATION_ENTRYPOINTS if path.exists()]
    assert PROJECT_ROOT / "dataforge-mcp" / "dataforge_mcp" / "tools.py" in available
    assert PROJECT_ROOT / "playground" / "api" / "app.py" in available
    assert (
        PROJECT_ROOT / "packages" / "dataforge-dbt" / "dataforge_dbt" / "dispatch.py"
    ) in available

    for path in available:
        text = path.read_text(encoding="utf-8")
        for banned_import in BANNED_INTEGRATION_IMPORTS:
            assert banned_import not in text, f"{path} imports internal API {banned_import!r}"
