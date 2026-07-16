"""Tests for backend release-gate policy helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from scripts.ci import backend_gate


def test_pip_audit_exception_is_structured_and_not_expired() -> None:
    """The Torch audit exception must remain scoped and time-bounded."""
    errors = backend_gate.pip_audit_exception_errors(today=date(2026, 7, 16))

    assert errors == []
    assert backend_gate.PIP_AUDIT_EXCEPTIONS[0].package == "torch"
    assert backend_gate.PIP_AUDIT_EXCEPTIONS[0].expires_on == date(2026, 10, 14)
    assert backend_gate.pip_audit_ignore_args() == ["--ignore-vuln", "CVE-2025-3000"]


def test_pip_audit_exception_expires_deterministically() -> None:
    """Expired audit exceptions become release blockers."""
    errors = backend_gate.pip_audit_exception_errors(today=date(2026, 10, 15))

    assert errors
    assert "expired on 2026-10-14" in errors[0]


def test_coverage_policy_rejects_makefile_fail_under_drift(tmp_path: Path) -> None:
    """The Makefile must not duplicate coverage threshold policy."""
    makefile = tmp_path / "Makefile"
    pyproject = tmp_path / "pyproject.toml"
    makefile.write_text(
        "coverage:\n\tpython -m pytest --cov=dataforge --cov-fail-under=90\n",
        encoding="utf-8",
    )
    pyproject.write_text("[tool.coverage.report]\nfail_under = 82\n", encoding="utf-8")

    errors = backend_gate.coverage_policy_errors(
        makefile_path=makefile,
        pyproject_path=pyproject,
    )

    assert any("must not duplicate" in error for error in errors)


def test_coverage_policy_accepts_single_pyproject_threshold(tmp_path: Path) -> None:
    """A single pyproject coverage threshold is the accepted release policy."""
    makefile = tmp_path / "Makefile"
    pyproject = tmp_path / "pyproject.toml"
    makefile.write_text(
        "coverage:\n\tpython -m pytest tests/ --cov=dataforge --cov-report=term-missing\n",
        encoding="utf-8",
    )
    pyproject.write_text("[tool.coverage.report]\nfail_under = 82\n", encoding="utf-8")

    assert (
        backend_gate.coverage_policy_errors(
            makefile_path=makefile,
            pyproject_path=pyproject,
        )
        == []
    )


def test_canonical_gate_provisions_every_required_optional_surface() -> None:
    """Required optional checks must have their dependencies installed first."""
    workflow = (backend_gate.PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    canonical_job = workflow.split("  canonical-backend-gate:", 1)[1].split(
        "  test-map-validate:", 1
    )[0]

    assert 'pip install -e "./dataforge-mcp[dev]"' in canonical_job
    assert "pip install -r docs/requirements.txt" in canonical_job
    assert "pip install -r playground/api/requirements.txt" in canonical_job
    assert "backend_gate.py --require-optional" in canonical_job
