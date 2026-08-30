"""Tests for backend release-gate policy helpers."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from scripts.ci import backend_gate

#: Every vulnerability the backend gate is allowed to ignore, listed explicitly so a new
#: exception cannot be added without a reviewer seeing it here. Adding an entry to
#: PIP_AUDIT_EXCEPTIONS without updating this list fails the suite, which is the point.
#:
#: EMPTY as of 2026-08-30: the last entry (`datasets`/PYSEC-2026-3716) was retired by taking the
#: datasets 5.0.1 fix. Because an empty list cannot exercise the validators, the tests below that
#: previously relied on the live entry now run against SYNTHETIC exceptions -- see
#: `test_pip_audit_exception_expires_deterministically`.
EXPECTED_AUDIT_EXCEPTIONS: list[tuple[str, str, date]] = []

#: A stand-in exception, well-formed in every respect, for tests that must exercise the
#: validators now that the live list is empty. It is deliberately NOT added to
#: PIP_AUDIT_EXCEPTIONS: policing a real suppression and proving the policing works are
#: different jobs, and conflating them is how a validator goes quietly vacuous.
SYNTHETIC_EXCEPTION = backend_gate.PipAuditException(
    vuln_id="PYSEC-9999-0001",
    package="a-package-that-is-not-installed",
    scope="synthetic fixture; never installed, never suppressed in a real run",
    expires_on=date(2099, 1, 1),
    reason="Exists only so the exception validators have a population to act on.",
    upstream_reference="https://example.invalid/synthetic",
)


def test_pip_audit_exceptions_are_exactly_the_reviewed_set() -> None:
    """No vulnerability may be ignored without appearing in the reviewed list above."""
    actual = [(e.vuln_id, e.package, e.expires_on) for e in backend_gate.PIP_AUDIT_EXCEPTIONS]

    assert actual == EXPECTED_AUDIT_EXCEPTIONS


def test_nothing_is_currently_suppressed() -> None:
    """The empty list must reach pip-audit as literally no `--ignore-vuln` argument.

    Stated as its own assertion because "the list is empty" and "the audit is unsuppressed" are
    different claims, and only the second one is the property that matters.
    """
    assert backend_gate.PIP_AUDIT_EXCEPTIONS == []
    assert backend_gate.pip_audit_ignore_args() == []


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

    This is the forcing function that stops an exception becoming a permanent silence: nobody has
    to remember to revisit it, the gate goes red on its own.

    Rewritten 2026-08-30 to assert against a SYNTHETIC exception rather than the live list. The
    previous version asserted the real `datasets` entry expired on 2026-11-26, and its own
    docstring warned why that mattered: "if this ever stops asserting, an empty or all-passing
    list would look identical to a healthy one." Retiring that entry emptied the list and would
    have made exactly that happen. The node id is unchanged so the gate population does not move.
    """
    expiring = replace(SYNTHETIC_EXCEPTION, expires_on=date(2026, 11, 26))

    errors = backend_gate.pip_audit_exception_errors([expiring], today=date(2026, 11, 27))

    assert errors
    assert any("expired on 2026-11-26" in error for error in errors)


def test_pip_audit_exception_validator_rejects_an_unjustified_entry() -> None:
    """Non-vacuity for the shape validators, which the empty live list cannot exercise."""
    unjustified = replace(
        SYNTHETIC_EXCEPTION, scope="", reason="", upstream_reference="http://insecure.invalid"
    )

    errors = backend_gate.pip_audit_exception_errors([unjustified], today=date(2026, 8, 30))

    assert any("is missing scope" in error for error in errors)
    assert any("is missing reason" in error for error in errors)
    assert any("HTTPS upstream_reference" in error for error in errors)


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


class TestDependencyAuditScopeIsExplicit:
    """A green pip-audit must state what it looked at.

    `pip-audit --local` audits the installed environment, so its population is whatever
    happens to be installed. That made the gate scope vary silently by machine: on one
    commit it failed locally on 11 upstream advisories in the training and dbt stacks and
    passed in CI, whose install set (`pip install -e ".[dev]"`) does not contain them.
    Both results printed as "pip-audit" and neither said what it had covered.

    The response is scope reporting, not another exception. An exception silences a known
    advisory deliberately and expires; this fails when the audit could not have seen a
    surface the product actually ships.
    """

    def test_the_scope_report_names_installed_count_and_surfaces(self) -> None:
        report = backend_gate.pip_audit_scope_report()

        assert "installed distribution(s)" in report
        assert "required surfaces covered" in report
        assert "optional stacks present" in report

    def test_required_surfaces_are_the_three_a_user_installs(self) -> None:
        """Optional extras are deliberately excluded: requiring them makes the gate
        unrunnable rather than honest."""
        assert set(backend_gate._AUDITED_SURFACES) == {"core", "playground", "mcp"}

    def test_no_scope_errors_in_a_correctly_provisioned_environment(self) -> None:
        """Non-vacuity. The suite itself imports these, so absence means a broken env."""
        assert backend_gate.pip_audit_scope_errors() == []

    def test_a_missing_surface_is_reported_as_uncovered(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """The case a passing audit must not be allowed to resemble."""
        monkeypatch.setitem(
            backend_gate._AUDITED_SURFACES,
            "core",
            ("a_package_that_is_not_installed",),
        )

        errors = backend_gate.pip_audit_scope_errors()

        assert len(errors) == 1
        assert "did not cover the core surface" in errors[0]
        assert "says nothing about it" in errors[0]

    def test_the_scope_check_actually_fails_the_gate(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """The wiring, not the helper. This is the test whose absence hid a dead gate.

        From the commit that introduced `pip_audit_scope_errors` until 2026-08-30, `main()`
        computed it, printed it, and never appended the result to `checks`. The helper was
        covered by four tests and the wiring by none, so a check that could not fail read as
        covered -- the fourth such gate found in this repository. Asserting on the returned
        verdict is what makes that regression detectable.
        """
        monkeypatch.setitem(
            backend_gate._AUDITED_SURFACES,
            "core",
            ("a_package_that_is_not_installed",),
        )

        assert backend_gate._pip_audit_scope_check(optional=False) is False
        # ...and is a warning, not a failure, when the audit is not mandatory: an
        # under-provisioned developer machine must not be turned into a red gate.
        assert backend_gate._pip_audit_scope_check(optional=True) is True

    def test_the_scope_check_passes_in_this_environment(self) -> None:
        """Non-vacuity for the enforced path: the same call must pass when nothing is missing."""
        assert backend_gate._pip_audit_scope_check(optional=False) is True


class TestExceptionLivenessIsObservable:
    """An exception that no longer suppresses anything must be detectable by machine.

    The shape and expiry validators cannot see this case, and it has now been caught by hand
    three times: the torch entry (resolved by torch 2.13.0, retired 2026-08-28) and both
    pymdown-extensions entries (unblocked by mkdocs-material 9.7.0, retired the same day). Each
    carried an expiry that would have failed `canonical-backend-gate` for a vulnerability the
    environment no longer had.
    """

    def test_an_observable_exception_is_reported_with_its_installed_version(self) -> None:
        """`pytest` is certainly installed, so this exercises the observable branch."""
        observable = replace(SYNTHETIC_EXCEPTION, package="pytest")

        liveness = backend_gate.pip_audit_exception_liveness([observable])
        report = backend_gate.pip_audit_liveness_report([observable])

        assert liveness[0][1] is not None
        assert "observable@" in report

    def test_an_observable_exception_is_never_a_liveness_failure(self) -> None:
        """Even at one day from expiry: it suppresses something real, so it is a live triage."""
        observable = replace(SYNTHETIC_EXCEPTION, package="pytest", expires_on=date(2026, 8, 31))

        errors = backend_gate.pip_audit_exception_liveness_errors(
            [observable], today=date(2026, 8, 30)
        )

        assert errors == []

    def test_a_dead_exception_near_expiry_fails(self) -> None:
        """The torch and pymdown case, made mechanical."""
        dead = replace(SYNTHETIC_EXCEPTION, expires_on=date(2026, 9, 5))

        errors = backend_gate.pip_audit_exception_liveness_errors([dead], today=date(2026, 8, 30))

        assert len(errors) == 1
        assert "suppresses nothing in this environment" in errors[0]
        assert "expires in 6 day(s)" in errors[0]

    def test_a_dead_exception_far_from_expiry_is_reported_but_not_failed(self) -> None:
        """Deliberate. Requiring the optional extras would make the gate unrunnable rather than
        honest, which is the same reason `pip_audit_scope_errors` does not demand train/dbt."""
        dead = replace(SYNTHETIC_EXCEPTION, expires_on=date(2027, 6, 1))

        errors = backend_gate.pip_audit_exception_liveness_errors([dead], today=date(2026, 8, 30))
        report = backend_gate.pip_audit_liveness_report([dead])

        assert errors == []
        assert "NOT INSTALLED" in report

    def test_the_empty_list_reports_that_nothing_is_suppressed(self) -> None:
        errors = backend_gate.pip_audit_exception_liveness_errors([], today=date(2026, 8, 30))

        assert errors == []
        assert backend_gate.pip_audit_liveness_report([]) == (
            "pip-audit exceptions: none (nothing is suppressed)"
        )


class TestSecurityPolicyMatchesTheExceptionList:
    """`SECURITY.md` must advertise exactly the exceptions that exist.

    The liveness gap had a documentation half, and it was the worse one. Until 2026-08-30
    `SECURITY.md` advertised the torch exception as the "Current scoped exception" -- deleted two
    days earlier -- with a passed expiry of 2026-07-13 and the claim "pip-audit reports no fixed
    version", which torch 2.13.0 falsifies. Nothing in the repository read the file. This check
    was red on the tree that introduced it, which is the only convincing evidence that it can be.
    """

    def test_the_repository_document_agrees_with_the_exception_list(self) -> None:
        assert backend_gate.security_policy_exception_errors() == []

    def test_a_retired_exception_left_advertised_is_a_failure(self, tmp_path: Path) -> None:
        """Reproduces the exact defect found on 2026-08-30, on a fixture."""
        document = tmp_path / "SECURITY.md"
        document.write_text(
            "## Dependency Audit Policy\n\n"
            "Current scoped exception:\n\n"
            "- `CVE-2025-3000` / `GHSA-rrmf-rvhw-rf47` in `torch`: optional only.\n\n"
            "## Out Of Scope\n",
            encoding="utf-8",
        )

        errors = backend_gate.security_policy_exception_errors([], path=document)

        assert len(errors) == 2
        assert any("CVE-2025-3000" in error for error in errors)
        assert all("must not be documented as current" in error for error in errors)

    def test_an_undisclosed_live_exception_is_a_failure(self, tmp_path: Path) -> None:
        """The other direction: a suppression the policy does not mention."""
        document = tmp_path / "SECURITY.md"
        document.write_text(
            "Current scoped exception:\n\n- **None.**\n\n## Out Of Scope\n", encoding="utf-8"
        )

        errors = backend_gate.security_policy_exception_errors([SYNTHETIC_EXCEPTION], path=document)

        assert len(errors) == 1
        assert "PYSEC-9999-0001" in errors[0]
        assert "undocumented one" in errors[0]

    def test_prose_discussing_a_retired_advisory_is_not_an_advertisement(
        self, tmp_path: Path
    ) -> None:
        """Found by this check failing on its own fix.

        Recording a retirement requires naming the retired advisory, and scanning the whole
        section made that indistinguishable from advertising it as current -- the same
        false-positive class as a secret scanner matching its own pattern list. Only bullets
        count. The accepted cost is that a stale id hidden in prose goes undetected.
        """
        document = tmp_path / "SECURITY.md"
        document.write_text(
            "Current scoped exception:\n\n"
            "- **None.**\n\n"
            "Until 2026-08-30 this section advertised CVE-2025-3000 / GHSA-rrmf-rvhw-rf47 in\n"
            "torch, which torch 2.13.0 resolved.\n\n"
            "## Out Of Scope\n",
            encoding="utf-8",
        )

        assert backend_gate.security_policy_exception_errors([], path=document) == []

    def test_a_missing_section_is_a_failure_not_a_pass(self, tmp_path: Path) -> None:
        """Fail closed: a document that cannot be compared is not evidence of agreement."""
        document = tmp_path / "SECURITY.md"
        document.write_text("## Reporting\n\nEmail us.\n", encoding="utf-8")

        errors = backend_gate.security_policy_exception_errors([], path=document)

        assert len(errors) == 1
        assert "has no 'Current scoped exception' section" in errors[0]

    def test_a_missing_document_is_a_failure(self, tmp_path: Path) -> None:
        errors = backend_gate.security_policy_exception_errors([], path=tmp_path / "absent.md")

        assert len(errors) == 1
        assert "is missing" in errors[0]


class TestTheNewChecksAreActuallyWiredIntoMain:
    """The checks added on 2026-08-30 must reach `checks`, and stay there.

    `scripts/ci/gate_population.py` pins the gate's step list by AST-parsing `_run` and
    `GateCommand` literals, so it cannot see the print-style checks invoked directly from
    `main()` -- `_coverage_policy_check`, `_pip_audit_exception_check`,
    `_corrector_promotion_gate`, `_secret_scan`, and the two added here. Those steps could be
    deleted from `main()` without the anti-erosion gate noticing.

    That is exactly the hole that let `pip_audit_scope_errors` sit inert while looking covered,
    so leaving the fix exposed to it would be the same mistake one level up. This asserts on the
    parsed call graph of `main()` rather than on a substring, so reformatting cannot break it and
    deleting the call cannot pass it.
    """

    @staticmethod
    def _appended_call_names() -> set[str]:
        """Return every function name whose result contributes to `main()`'s `checks` list.

        Covers both shapes `main()` uses: the initial `checks: list[bool] = [...]` literal and the
        later `checks.append(...)` / `checks.extend(...)` calls. Both were needed -- the
        non-vacuity test below failed on a first version that only looked at `append` and so
        missed the three checks in the literal, which is precisely the assertion it exists to make.
        """
        import ast

        source = Path(backend_gate.__file__).read_text(encoding="utf-8")
        main_function = next(
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.FunctionDef) and node.name == "main"
        )
        contributing: set[str] = set()

        def record(candidate: ast.expr) -> None:
            if isinstance(candidate, ast.Call) and isinstance(candidate.func, ast.Name):
                contributing.add(candidate.func.id)

        for node in ast.walk(main_function):
            if isinstance(node, ast.AnnAssign | ast.Assign):
                targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
                is_checks = any(
                    isinstance(target, ast.Name) and target.id == "checks" for target in targets
                )
                if is_checks and isinstance(node.value, ast.List):
                    for element in node.value.elts:
                        record(element)
                continue
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {"append", "extend"}:
                continue
            if not (isinstance(node.func.value, ast.Name) and node.func.value.id == "checks"):
                continue
            for argument in node.args:
                record(argument)
        return contributing

    def test_the_scope_and_liveness_checks_are_appended_to_the_gate_result(self) -> None:
        appended = self._appended_call_names()

        assert "_pip_audit_scope_check" in appended, (
            "the dependency audit scope check is computed but its result is not part of the "
            "gate verdict -- the exact defect fixed on 2026-08-30"
        )
        assert "_pip_audit_liveness_check" in appended

    def test_the_pre_existing_print_style_checks_are_still_appended(self) -> None:
        """Non-vacuity: the same parse must find the checks that were already wired."""
        appended = self._appended_call_names()

        assert {
            "_coverage_policy_check",
            "_pip_audit_exception_check",
            "_corrector_promotion_gate",
            "_secret_scan",
        } <= appended
