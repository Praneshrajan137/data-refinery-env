"""Tests for backend release-gate policy helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from scripts.ci import backend_gate

#: Every vulnerability the backend gate is allowed to ignore, listed explicitly so a new
#: exception cannot be added without a reviewer seeing it here. Adding an entry to
#: PIP_AUDIT_EXCEPTIONS without updating this list fails the suite, which is the point.
EXPECTED_AUDIT_EXCEPTIONS = [
    ("PYSEC-2026-3716", "datasets", date(2026, 11, 26)),
]


def test_pip_audit_exceptions_are_exactly_the_reviewed_set() -> None:
    """No vulnerability may be ignored without appearing in the reviewed list above."""
    actual = [(e.vuln_id, e.package, e.expires_on) for e in backend_gate.PIP_AUDIT_EXCEPTIONS]

    assert actual == EXPECTED_AUDIT_EXCEPTIONS


def test_pip_audit_exception_is_structured_and_not_expired() -> None:
    """Every audit exception must remain scoped and time-bounded."""
    errors = backend_gate.pip_audit_exception_errors(today=date(2026, 8, 28))

    assert errors == []
    expected_args = [
        arg for vuln_id, _, _ in EXPECTED_AUDIT_EXCEPTIONS for arg in ("--ignore-vuln", vuln_id)
    ]
    assert backend_gate.pip_audit_ignore_args() == expected_args


def test_every_audit_exception_carries_its_justification() -> None:
    """An exception without scope, reason and an upstream reference is not a triage.

    These fields are what separate a documented, time-boxed exception from a silenced
    check, so emptiness is a policy violation rather than a cosmetic gap.
    """
    for exception in backend_gate.PIP_AUDIT_EXCEPTIONS:
        assert exception.scope.strip(), f"{exception.vuln_id} has no scope"
        assert exception.reason.strip(), f"{exception.vuln_id} has no reason"
        assert exception.upstream_reference.startswith("https://"), (
            f"{exception.vuln_id} has no upstream reference"
        )


def test_pip_audit_exception_expires_deterministically() -> None:
    """Expired audit exceptions become release blockers.

    The only expiry in the reviewed set is 2026-11-26, so a clock one day past it must produce
    a blocking error. This is the forcing function that stops an exception becoming a permanent
    silence: nobody has to remember to revisit it, the gate goes red on its own.

    Worth stating because the list is now a single entry, which is exactly where a validator can
    quietly go vacuous -- if this ever stops asserting, an empty or all-passing list would look
    identical to a healthy one.
    """
    errors = backend_gate.pip_audit_exception_errors(today=date(2026, 11, 27))

    assert errors
    assert any("expired on 2026-11-26" in error for error in errors)


#: Every npm advisory the gate is allowed to ignore, listed for the same reason as the pip
#: set above: an exception must be visible to a reviewer, not buried in a helper.
EXPECTED_NPM_EXCEPTIONS = [
    ("GHSA-r28c-9q8g-f849", "postcss"),
    ("GHSA-fxqj-rqcc-2cmp", "postcss"),
    ("GHSA-28wg-ghj8-5hjv", "nanoid"),
    ("GHSA-2v37-7h3g-55p8", "nanoid"),
]


def _payload(advisory_url: str, *, severity: str = "high", name: str = "somepkg") -> dict:
    return {"vulnerabilities": {name: {"severity": severity, "via": [{"url": advisory_url}]}}}


def test_npm_audit_exceptions_are_exactly_the_reviewed_set() -> None:
    actual = [(e.advisory_id, e.package) for e in backend_gate.NPM_AUDIT_EXCEPTIONS]

    assert actual == EXPECTED_NPM_EXCEPTIONS


def test_npm_audit_exceptions_validate_and_are_unexpired() -> None:
    assert backend_gate.npm_audit_exception_errors(today=date(2026, 8, 8)) == []


def test_npm_audit_exceptions_expire() -> None:
    """An exception that never expires is a permanently silenced check."""
    errors = backend_gate.npm_audit_exception_errors(today=date(2026, 11, 9))

    assert errors
    assert any("expired on 2026-11-08" in error for error in errors)


def test_a_triaged_advisory_does_not_block() -> None:
    payload = _payload("https://github.com/advisories/GHSA-r28c-9q8g-f849", name="postcss")

    assert backend_gate.npm_audit_blocking_advisories(payload) == []


def test_an_untriaged_advisory_blocks() -> None:
    """The guard must actually stop something, or it is decoration."""
    payload = _payload("https://github.com/advisories/GHSA-not-triaged-0000")

    blocking = backend_gate.npm_audit_blocking_advisories(payload)

    assert blocking
    assert "GHSA-not-triaged-0000" in blocking[0]


def test_severities_below_the_floor_are_ignored() -> None:
    payload = _payload("https://github.com/advisories/GHSA-low-0000", severity="low")

    assert backend_gate.npm_audit_blocking_advisories(payload) == []


def test_high_severity_is_above_the_floor() -> None:
    payload = _payload("https://github.com/advisories/GHSA-high-0000", severity="high")

    assert backend_gate.npm_audit_blocking_advisories(payload) != []


def test_an_advisory_with_no_identifier_fails_closed() -> None:
    """An advisory that cannot be identified must not be silently dropped."""
    payload = {"vulnerabilities": {"x": {"severity": "high", "via": [{"url": ""}]}}}

    assert backend_gate.npm_audit_blocking_advisories(payload) != []


def test_an_unparseable_entry_fails_closed() -> None:
    payload = {"vulnerabilities": {"x": "not-a-dict"}}

    assert backend_gate.npm_audit_blocking_advisories(payload) != []


def test_a_clean_audit_blocks_nothing() -> None:
    assert backend_gate.npm_audit_blocking_advisories({"vulnerabilities": {}}) == []


def test_string_via_entries_are_not_treated_as_advisories() -> None:
    """A string 'via' means 'vulnerable through another package', carrying no id."""
    payload = {"vulnerabilities": {"x": {"severity": "high", "via": ["postcss"]}}}

    assert backend_gate.npm_audit_blocking_advisories(payload) == []


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
