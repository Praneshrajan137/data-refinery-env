"""Tests for release-truth checks."""

from __future__ import annotations

from pathlib import Path

from scripts.ci import readme_truth


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
