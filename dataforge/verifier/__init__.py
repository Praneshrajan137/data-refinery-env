"""Verifier exports for DataForge."""

from __future__ import annotations

from typing import Any

__all__ = [
    "AggregateDependency",
    "DomainBound",
    "FunctionalDependency",
    "SMTVerifier",
    "Schema",
    "SchemaToSMT",
    "VerificationResult",
    "VerificationVerdict",
    "explain_unsat_core",
]


def __getattr__(name: str) -> Any:
    """Lazily expose verifier symbols without import-time cycles."""
    if name in {"AggregateDependency", "DomainBound", "FunctionalDependency", "Schema"}:
        from dataforge.verifier import schema as schema_module

        return getattr(schema_module, name)
    if name in {"SchemaToSMT", "VerificationResult", "VerificationVerdict"}:
        from dataforge.verifier import smt as smt_module

        return getattr(smt_module, name)
    if name == "SMTVerifier":
        from dataforge.verifier import gate as gate_module

        return gate_module.SMTVerifier
    if name == "explain_unsat_core":
        from dataforge.verifier import explain as explain_module

        return explain_module.explain_unsat_core
    raise AttributeError(name)
