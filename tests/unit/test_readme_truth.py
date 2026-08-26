"""Tests for release-truth checks."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from scripts.ci import readme_truth


def _publish_artifact(package_name: str, index_name: str, artifact_name: str) -> dict[str, object]:
    """Build complete publish evidence for one test artifact."""
    workflow = readme_truth.EXPECTED_PUBLISH_WORKFLOWS[package_name][index_name]
    sha256 = "a" * 64
    host = "pypi.org" if index_name == "pypi" else "test.pypi.org"
    return {
        "download_url": f"https://files.pythonhosted.org/{package_name}/{artifact_name}",
        "filename": f"{package_name}-0.1.0.{artifact_name}",
        "integrity_predicate_type": readme_truth.PUBLISH_ATTESTATION_PREDICATE,
        "integrity_subject_sha256": sha256,
        "package_type": "bdist_wheel" if artifact_name == "wheel" else "sdist",
        "provenance_url": f"https://{host}/integrity/{package_name}/0.1.0/{artifact_name}",
        "sha256": sha256,
        "trusted_publisher": {
            "identity": (
                f"https://github.com/Aegis15/dataforge/.github/workflows/{workflow}@refs/heads/main"
            ),
            "oidc_issuer": "https://token.actions.githubusercontent.com",
            "ref": "refs/heads/main",
            "repository": "Aegis15/dataforge",
            "workflow": workflow,
        },
        "upload_time_iso_8601": "2026-06-13T03:44:38.733869Z",
    }


def _publish_index(package_name: str, index_name: str) -> dict[str, object]:
    """Build complete PyPI/TestPyPI index evidence for one test package."""
    project_host = "pypi.org" if index_name == "pypi" else "test.pypi.org"
    return {
        "index": index_name,
        "project_url": f"https://{project_host}/project/{package_name.replace('_', '-')}/",
        "wheel": _publish_artifact(package_name, index_name, "wheel"),
        "sdist": _publish_artifact(package_name, index_name, "sdist"),
    }


def _publish_package(name: str, tmp_path: Path) -> dict[str, object]:
    """Build complete publish-report package evidence."""
    _ = tmp_path
    smoke_dir = readme_truth.PROJECT_ROOT / "docs" / "evidence" / "pypi"
    smoke_dir.mkdir(parents=True, exist_ok=True)
    pypi_log = smoke_dir / f"{name}-pypi-smoke.json"
    testpypi_log = smoke_dir / f"{name}-testpypi-smoke.json"
    pypi_log.write_text('{"ok": true}\n', encoding="utf-8")
    testpypi_log.write_text('{"ok": true}\n', encoding="utf-8")
    return {
        "name": name,
        "version": "0.1.0",
        "pypi": _publish_index(name, "pypi"),
        "testpypi": _publish_index(name, "testpypi"),
        "attestations": True,
        "pypi_fresh_install": True,
        "testpypi_fresh_install": True,
        "trusted_publishing": True,
        "pypi_smoke_log_path": f"pypi/{pypi_log.name}",
        "testpypi_smoke_log_path": f"pypi/{testpypi_log.name}",
        "workflow_run_url": "https://github.com/Aegis15/dataforge/actions/runs/123",
    }


def _write_publish_report(path: Path, packages: list[dict[str, object]]) -> None:
    """Write a publish report fixture."""
    path.write_text(
        json.dumps(
            {
                "schema_version": "dataforge_pypi_publish_report_v2",
                "packages": packages,
            }
        ),
        encoding="utf-8",
    )


def test_design_partner_gate_is_explicitly_not_met() -> None:
    """The current release should not imply design-partner evidence exists."""
    assert readme_truth.design_partner_gate_not_met() is True
    assert readme_truth.check_design_partner_claims(readme_truth.DESIGN_PARTNER_TRUTH_DOCS) == []


def test_release_subcommand_claims_are_checked() -> None:
    """Nested release commands in README prose must map to registered commands."""
    text = "Run dataforge15 release gate --json and dataforge release doctor --core."

    claimed = readme_truth.extract_release_subcommands_from_readme(text)
    registered = readme_truth.get_registered_release_commands()

    assert claimed == {"gate", "doctor"}
    assert claimed <= registered


def test_claim_ledger_exists_and_records_cloud_apply_as_roadmap() -> None:
    """Public claims should have an explicit shipped/beta/experimental/roadmap ledger."""
    entries = readme_truth.load_claim_ledger()

    assert entries
    assert all(entry["status"] in readme_truth.CLAIM_LEDGER_STATUSES for entry in entries)
    assert any(
        entry["claim"] == "credentialed_cloud_warehouse_apply" and entry["status"] == "roadmap"
        for entry in entries
    )


def test_evidence_ledger_is_part_of_truth_gate() -> None:
    """The public truth checker should validate the canonical evidence ledger."""
    assert readme_truth.check_evidence_ledger() == []


def test_claim_ledger_rejects_unknown_status(tmp_path: Path) -> None:
    """Claim status values are intentionally closed vocabulary."""
    ledger = tmp_path / "claims.yaml"
    ledger.write_text(
        "claims:\n  - claim: impossible\n    status: almost\n    evidence: none\n",
        encoding="utf-8",
    )

    errors = readme_truth.check_claim_ledger(ledger)

    assert errors
    assert "unknown status" in errors[0]


def test_pypi_publish_report_requires_all_public_packages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publication truth gate must prove every public package family member."""
    monkeypatch.setattr(readme_truth, "PROJECT_ROOT", tmp_path)
    report = tmp_path / "publish_report.json"
    _write_publish_report(
        report,
        [_publish_package(name, tmp_path) for name in readme_truth.PUBLISHED_DISTS],
    )

    assert readme_truth.check_pypi_publish_report(report) == []


def test_pypi_publish_report_rejects_wrong_local_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publication evidence must match the version in each local pyproject."""
    monkeypatch.setattr(readme_truth, "PROJECT_ROOT", tmp_path)
    report = tmp_path / "publish_report.json"
    packages = [_publish_package(name, tmp_path) for name in readme_truth.PUBLISHED_DISTS]
    packages[0]["version"] = "9.9.9"
    _write_publish_report(report, packages)

    errors = readme_truth.check_pypi_publish_report(report)

    assert any("version does not match local pyproject" in error for error in errors)


def test_pypi_publish_report_rejects_missing_smoke_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Referenced fresh-install smoke logs are part of release evidence."""
    monkeypatch.setattr(readme_truth, "PROJECT_ROOT", tmp_path)
    report = tmp_path / "publish_report.json"
    packages = [_publish_package(name, tmp_path) for name in readme_truth.PUBLISHED_DISTS]
    packages[0]["pypi_smoke_log_path"] = "pypi/missing-smoke.json"
    _write_publish_report(report, packages)

    errors = readme_truth.check_pypi_publish_report(report)

    assert any("references missing" in error for error in errors)


def test_pypi_publish_report_rejects_missing_attestation_predicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each wheel and sdist must carry the PyPI publish attestation predicate."""
    monkeypatch.setattr(readme_truth, "PROJECT_ROOT", tmp_path)
    report = tmp_path / "publish_report.json"
    packages = [_publish_package(name, tmp_path) for name in readme_truth.PUBLISHED_DISTS]
    packages[0]["pypi"]["wheel"]["integrity_predicate_type"] = "wrong"  # type: ignore[index]
    _write_publish_report(report, packages)

    errors = readme_truth.check_pypi_publish_report(report)

    assert any("publish attestation predicate" in error for error in errors)


def test_stale_publication_wording_fails_after_publish(tmp_path: Path) -> None:
    """Published package docs must not drift back to pre-publication language."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text("The PyPI package is not published yet.\n", encoding="utf-8")

    errors = readme_truth.check_stale_publication_claims([claim_path])

    assert errors
    assert "published DataForge package" in errors[0]


def test_unqualified_design_partner_claim_fails_when_gate_not_met(tmp_path: Path) -> None:
    """Customer validation prose must be qualified while the evidence gate is unmet."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text("DataForge has design partners and pilot users.\n", encoding="utf-8")

    errors = readme_truth.check_design_partner_claims([claim_path])

    assert errors
    assert "unqualified" in errors[0]


def test_explicitly_unmet_design_partner_claim_is_allowed(tmp_path: Path) -> None:
    """Honest not-met wording should not fail the truth checker."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text(
        "DataForge does not claim design-partner or customer validation evidence yet.\n",
        encoding="utf-8",
    )

    assert readme_truth.check_design_partner_claims([claim_path]) == []


def test_unqualified_benchmark_claim_outside_generated_block_fails(tmp_path: Path) -> None:
    """Public metric claims must live in generated benchmark evidence blocks."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text("DataForge reaches F1 0.99 on Hospital.\n", encoding="utf-8")

    errors = readme_truth.check_public_claim_boundaries([claim_path])

    assert errors
    assert "outside a generated evidence block" in errors[0]


def test_generated_benchmark_claim_block_is_allowed(tmp_path: Path) -> None:
    """Metric values inside BENCH markers are governed by benchmark_truth."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text(
        "<!-- BENCH:START -->\nF1 0.99 is generated evidence.\n<!-- BENCH:END -->\n",
        encoding="utf-8",
    )

    assert readme_truth.check_public_claim_boundaries([claim_path]) == []


def test_workers_dev_playground_url_is_checked() -> None:
    """The lightweight URL checker includes the canonical Workers playground."""
    text = (
        "Try https://dataforge.praneshrajan15.workers.dev/playground now.\n"
        "Backend: https://Praneshrajan15-dataforge-playground.hf.space.\n"
    )

    urls = readme_truth.extract_playground_urls(text)

    assert "https://dataforge.praneshrajan15.workers.dev/playground" in urls
    assert "https://Praneshrajan15-dataforge-playground.hf.space" in urls


def test_removed_domain_claim_fails(tmp_path: Path) -> None:
    """The removed domain must not appear in release docs."""
    removed = "dataforge" + ".dev"
    claim_path = tmp_path / "claim.md"
    claim_path.write_text(f"Live playground: https://{removed}/playground\n", encoding="utf-8")

    errors = readme_truth.check_custom_domain_claims([claim_path])

    assert errors
    assert "removed domain" in errors[0]


def test_optional_removed_domain_claim_fails(tmp_path: Path) -> None:
    """Optional removed-domain wording is forbidden too."""
    removed = "dataforge" + ".dev"
    claim_path = tmp_path / "claim.md"
    claim_path.write_text(
        f"Future optional removed domain, not a release target: https://{removed}/playground\n",
        encoding="utf-8",
    )

    errors = readme_truth.check_custom_domain_claims([claim_path])

    assert errors
    assert "removed domain" in errors[0]


def test_mandatory_removed_domain_gate_fails(tmp_path: Path) -> None:
    """Mandatory blocked-domain wording is no longer honest."""
    removed = "dataforge" + ".dev"
    claim_path = tmp_path / "claim.md"
    claim_path.write_text(
        f"Mandatory hard gate, not yet live: https://{removed}/playground\n",
        encoding="utf-8",
    )

    errors = readme_truth.check_custom_domain_claims([claim_path])

    assert errors
    assert "removed domain" in errors[0]


def test_unqualified_unshipped_integration_claim_fails(tmp_path: Path) -> None:
    """Airbyte and Databricks must stay roadmap-only until packages exist."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text("Ships dataforge-airbyte and dataforge-databricks.\n", encoding="utf-8")

    errors = readme_truth.check_unshipped_integration_claims([claim_path])

    assert errors
    assert "unqualified" in errors[0]


def test_unqualified_model_family_claim_fails(tmp_path: Path) -> None:
    """The 0.5B-to-7B model family claim needs release evidence first."""
    claim_path = tmp_path / "claim.md"
    claim_path.write_text(
        "DataForge ships a 0.5B to 7B SFT GRPO GiGPO model family.\n",
        encoding="utf-8",
    )

    errors = readme_truth.check_public_claim_boundaries([claim_path])

    assert errors
    assert "outside a generated evidence block" in errors[0]


class TestAutoApplyMembershipClaims:
    """The gate that did not exist when ten claims went stale in one commit.

    `type_mismatch` left CONSTRAINT_CHECKABLE_DETECTORS and README.md went on describing it
    as auto-correcting while withholding that label from `missing_value`, the only repairer
    measured at unconditional write precision 1.0000 -- wrong in both directions at once.
    Nothing caught it, because this module checked that CLI commands exist and nothing
    checked what the product claims to DO.

    These tests exist because a gate nobody has seen fail is not a gate.
    """

    def _doc(self, tmp_path: Path, text: str) -> Path:
        path = tmp_path / "claim.md"
        path.write_text(text, encoding="utf-8")
        return path

    def test_a_stale_auto_correcting_claim_fails(self, tmp_path: Path) -> None:
        """The exact sentence README.md carried for a day."""
        doc = self._doc(
            tmp_path,
            "Eight detector families: `type_mismatch`, `decimal_shift`, "
            "`fd_violation` (auto-correcting, tier 0).\n",
        )

        errors = readme_truth.check_autoapply_membership_claims([doc])

        assert errors
        assert "type_mismatch" in errors[0]
        assert "decimal_shift" in errors[0]
        assert "CONSTRAINT_CHECKABLE_DETECTORS" in errors[0]

    def test_a_truthful_claim_passes(self, tmp_path: Path) -> None:
        """Non-vacuity: the check must accept the corrected wording, or it blocks the fix."""
        doc = self._doc(
            tmp_path,
            "Two detectors may auto-apply: `fd_violation` and `missing_value`, and only "
            "from a declared functional dependency.\n",
        )

        assert readme_truth.check_autoapply_membership_claims([doc]) == []

    def test_denying_the_claim_is_allowed(self, tmp_path: Path) -> None:
        """Correcting a doc must not trip the check that demanded the correction.

        A table row reading `type_mismatch | ... | no | removed on measurement` names a
        non-writer beside an authority phrase, and is exactly what an honest doc looks like.
        """
        doc = self._doc(
            tmp_path,
            "| `type_mismatch` | Values that mismatch | no | removed from the allowlist |\n",
        )

        assert readme_truth.check_autoapply_membership_claims([doc]) == []

    def test_an_undocumented_allowlist_member_fails(self, tmp_path: Path) -> None:
        """The other direction: write authority must not be gained silently.

        A doc that names only one of the two members leaves the other undocumented, which is
        how `missing_value` came to hold the strongest measured write precision in the
        project while no public page said it could write at all.
        """
        doc = self._doc(
            tmp_path,
            "Only `fd_violation` may auto-apply, from a declared dependency.\n",
        )

        errors = readme_truth.check_autoapply_members_are_documented([doc])

        assert errors
        assert "missing_value" in errors[-1]
        assert "no public doc says so" in errors[-1]

    def test_documenting_both_members_satisfies_the_coverage_check(self, tmp_path: Path) -> None:
        """Non-vacuity for the test above."""
        doc = self._doc(
            tmp_path,
            "`fd_violation` and `missing_value` may auto-apply from a declared dependency.\n",
        )

        assert readme_truth.check_autoapply_members_are_documented([doc]) == []

    def test_naming_a_detector_without_claiming_authority_is_not_a_claim(
        self, tmp_path: Path
    ) -> None:
        """Detectors must stay freely discussable; only write claims are policed.

        The two sentences are on separate lines because the check is line-scoped: a detector
        name sharing a line with an authority phrase does read as a claim about it, which is
        the behaviour that catches the stale README table row.
        """
        doc = self._doc(
            tmp_path,
            "`decimal_shift` finds numeric values off by powers of ten.\n"
            "`fd_violation` and `missing_value` may auto-apply.\n",
        )

        assert readme_truth.check_autoapply_membership_claims([doc]) == []

    def test_the_shipped_docs_satisfy_the_check(self) -> None:
        """The live documents, not a fixture. This is the assertion that rots if docs drift."""
        assert (
            readme_truth.check_autoapply_membership_claims(readme_truth.AUTOAPPLY_TRUTH_DOCS) == []
        )
        assert (
            readme_truth.check_autoapply_members_are_documented(readme_truth.AUTOAPPLY_TRUTH_DOCS)
            == []
        )


class TestDetectorFamilyCountClaims:
    """The published count of detector families, gated against the closed vocabulary.

    Why this class exists, dated 2026-08-26. FOUR documents said "Eight detector families" while
    ``IssueTypeLiteral`` defined eleven, and the count was checkable by nothing. It went stale for
    the same reason the write-authority check was blind: the population was frozen.

    Two of these tests exist because running the check against the live docs refuted it before it
    was trusted. It fired on "first-party GPT-5 family" (a claim about a model, not a detector) and
    on the sub-count sentences written to correct the stale total. Both are pinned below, because a
    gate that fires on unrelated prose or blocks its own remedy gets ignored rather than fixed.
    """

    def _doc(self, tmp_path: Path, text: str) -> Path:
        path = tmp_path / "claim.md"
        path.write_text(text, encoding="utf-8")
        return path

    def _all_named(self) -> str:
        """A line naming every issue type, so the coverage half of the check is satisfied."""
        from dataforge.detectors.base import ALL_ISSUE_TYPES

        return " ".join(sorted(ALL_ISSUE_TYPES)) + "\n"

    def test_a_stale_total_fails(self, tmp_path: Path) -> None:
        """The exact sentence four documents carried."""
        doc = self._doc(tmp_path, "Eight detector families across an additive ensemble.\n")

        errors = readme_truth.check_detector_family_count_claims([doc])

        assert any("claims 8 detector families" in error for error in errors)
        assert any("IssueTypeLiteral defines 11" in error for error in errors)

    def test_the_correct_total_passes(self) -> None:
        """Non-vacuity, against the live documents rather than a fixture."""
        assert (
            readme_truth.check_detector_family_count_claims(readme_truth.AUTOAPPLY_TRUTH_DOCS) == []
        )

    def test_a_digit_spelling_is_policed_too(self, tmp_path: Path) -> None:
        """ "8 issue families" must fail identically to "Eight detector families"."""
        doc = self._doc(tmp_path, "8 issue families ship.\n" + self._all_named())

        assert readme_truth.check_detector_family_count_claims([doc])

    def test_a_model_name_is_not_a_family_claim(self, tmp_path: Path) -> None:
        """The false positive found on README.md before this check was trusted.

        "first-party GPT-5 family" is a claim about an Azure OpenAI model. It matched because the
        number was glued to a preceding token and the noun was singular and unqualified.
        """
        doc = self._doc(
            tmp_path,
            "For Azure OpenAI (first-party GPT-5 family, works on trial credit).\n"
            + self._all_named(),
        )

        assert readme_truth.check_detector_family_count_claims([doc]) == []

    @pytest.mark.parametrize(
        "sentence",
        [
            "Ten families come from the eleven detectors in the default ensemble.",
            "The three families below `duplicate_row` carry no write measurement.",
            "Two of them may auto-apply.",
            "Eight families are detection-only.",
        ],
    )
    def test_a_sub_count_is_not_a_total_claim(self, tmp_path: Path, sentence: str) -> None:
        """A partition is not a contradiction, and the check must not block its own remedy."""
        doc = self._doc(tmp_path, sentence + "\n" + self._all_named())

        assert readme_truth.check_detector_family_count_claims([doc]) == []

    def test_an_undocumented_issue_type_fails(self, tmp_path: Path) -> None:
        """The coverage half. A count alone cannot show a family was documented.

        This is the check that matters more than the count: three shipped families
        (`date_transposition`, `entity_consensus`, `semantic_domain_violation`) were absent from
        the detectors table entirely, so the published review surface understated what a user
        would actually be shown.
        """
        doc = self._doc(tmp_path, "Eleven issue families ship: `fd_violation`.\n")

        errors = readme_truth.check_detector_family_count_claims([doc])

        assert any("no public doc names them" in error for error in errors)
        assert any("semantic_domain_violation" in error for error in errors)


class TestTheAutoapplyGateSeesTheWholeUniverse:
    """The gate polices a population. These tests are about the population, not the policy.

    Why this class exists, dated 2026-08-26. ``check_autoapply_membership_claims`` imported the
    allowlist from source of truth and then subtracted it from an eight-name set literal, while
    ``IssueTypeLiteral`` had grown to eleven. Three issue types were therefore invisible to it in
    both directions, and ``README.md`` said "Eight detector families" for the same reason -- the
    doc and the gate agreed with each other and both disagreed with the code.

    Nothing had exploited the blindness: with the population widened, the shipped docs still
    pass. That is worth stating precisely, because it means the old gate's green was uninformative
    rather than wrong, and "no stale claim was found" is not evidence a gate could have found one.

    So these tests plant claims about the previously-invisible types. They fail against a frozen
    population and pass against a derived one, which is the only way to demonstrate that the fix
    is load-bearing rather than cosmetic.
    """

    def _doc(self, tmp_path: Path, text: str) -> Path:
        path = tmp_path / "claim.md"
        path.write_text(text, encoding="utf-8")
        return path

    @pytest.mark.parametrize(
        "issue_type",
        ["date_transposition", "entity_consensus", "semantic_domain_violation"],
    )
    def test_a_claim_about_a_previously_invisible_type_now_fails(
        self, tmp_path: Path, issue_type: str
    ) -> None:
        """Each of the three types the frozen population omitted.

        ``semantic_domain_violation`` is the sharpest of the three: it is structurally
        detection-only with no repairer at all, so a doc claiming it auto-applies asserts a write
        path that does not exist anywhere in the product.
        """
        doc = self._doc(tmp_path, f"`{issue_type}` auto-applies once calibrated.\n")

        errors = readme_truth.check_autoapply_membership_claims([doc])

        assert errors, f"{issue_type} is invisible to the write-authority gate"
        assert issue_type in errors[0]

    def test_the_policed_population_is_derived_from_the_closed_vocabulary(self) -> None:
        """The regression guard for the class of failure, not for the three instances.

        Asserting the three names would go stale the moment a twelfth issue type ships -- the
        exact mistake being corrected. This asserts the *relationship* instead: every member of
        the closed vocabulary is either an allowlist member or policed as a non-writer, with no
        third category. A future issue type is covered on the day it is added to the Literal.
        """
        from dataforge.detectors.base import ALL_ISSUE_TYPES
        from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS

        policed_non_writers = {
            issue_type
            for issue_type in ALL_ISSUE_TYPES
            if readme_truth.check_autoapply_membership_claims([self._write_probe(issue_type)])
        }

        assert policed_non_writers == ALL_ISSUE_TYPES - set(CONSTRAINT_CHECKABLE_DETECTORS)

    def _write_probe(self, issue_type: str) -> Path:
        """Write a one-line authority claim for ``issue_type`` to a temp file."""
        probe = Path(tempfile.mkdtemp()) / "probe.md"
        probe.write_text(f"`{issue_type}` auto-applies.\n", encoding="utf-8")
        return probe

    def test_an_allowlist_member_absent_from_the_vocabulary_fails_at_import(self) -> None:
        """The structural invariant in ``dataforge/detectors/base.py``.

        An allowlist entry naming an issue type no detector can emit is a write permission for
        nothing -- or a typo that silently disables one somebody measured and earned. Asserted at
        import so it cannot be reached at runtime.
        """
        from dataforge.detectors.base import ALL_ISSUE_TYPES
        from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS

        assert set(CONSTRAINT_CHECKABLE_DETECTORS) <= ALL_ISSUE_TYPES
        assert set(CONSTRAINT_CHECKABLE_DETECTORS)
