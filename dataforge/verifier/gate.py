"""Compatibility exports for the Week 3 verifier."""

from dataforge.verifier.result import VerificationResult, VerificationVerdict
from dataforge.verifier.smt import SMTVerifier

__all__ = ["SMTVerifier", "VerificationResult", "VerificationVerdict"]
