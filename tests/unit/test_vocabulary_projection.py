"""Tests for the TypeScript vocabulary projection's POPULATION, not its values.

Why this file exists, dated 2026-08-26. ``scripts/ci/generate_domain_vocabulary.py`` derives
every projected *value* from ``dataforge/domain/vocabulary.py``, and a byte-for-byte ``--check``
proves the two sides agree about what was projected. Neither of those asks whether everything
that should have been projected was.

The population it projects was a frozen sequence of hand-written ``parts.append(...)`` calls. The
embedded source fingerprint made adding a constant fail CI -- and then resolve by omission: run
``--write``, the hash updates, CI goes green, and the new vocabulary is silently never projected.
A frozen population that fails loudly and then disappears is still a frozen population.

These tests are about that accounting. They are the same defect class corrected in
``scripts/ci/readme_truth.py`` on the same day; see ``dataforge/detectors/base.py`` for the rule.
"""

from __future__ import annotations

import pytest

from dataforge.domain import vocabulary
from scripts.ci import generate_domain_vocabulary as gen


class TestProjectionCoverage:
    """Every exported data constant is projected or explicitly withheld, with no third category."""

    def test_the_shipped_projection_accounts_for_every_constant(self) -> None:
        """The live generator against the live vocabulary. This is the assertion that rots."""
        assert gen._projection_coverage_errors(gen.render()) == []

    def test_the_accounting_is_exhaustive_and_disjoint(self) -> None:
        """Projected and withheld partition the exported data constants exactly.

        Asserted as a partition rather than as two membership checks, because the failure being
        guarded is a constant falling into neither set.
        """
        rendered = gen.render()
        constants = set(gen._data_constants())
        projected = {name for name in constants if f"export const {name}" in rendered}
        withheld = set(gen._UNPROJECTED)

        assert projected | withheld == constants
        assert projected & withheld == set()

    def test_an_unprojected_constant_is_refused(self) -> None:
        """Non-vacuity: the check must fail on the case it exists to catch.

        Simulated by removing a real constant's projection from the rendered text rather than by
        adding a fake constant to the vocabulary, so the test exercises the same code path a new
        unprojected vocabulary would.
        """
        rendered = gen.render().replace("export const RUNG_ORDER", "export const RENAMED_AWAY")

        errors = gen._projection_coverage_errors(rendered)

        assert errors
        assert "RUNG_ORDER" in errors[0]
        assert "_UNPROJECTED" in errors[0]

    def test_a_stale_exemption_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An exemption for something that no longer exists hides the next real omission."""
        monkeypatch.setitem(gen._UNPROJECTED, "CONSTANT_THAT_WAS_DELETED", "reason")

        errors = gen._projection_coverage_errors(gen.render())

        assert any("CONSTANT_THAT_WAS_DELETED" in error for error in errors)
        assert any("stale bookkeeping" in error for error in errors)

    def test_an_exemption_that_contradicts_a_projection_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Listing a projected constant as withheld means the two records disagree."""
        monkeypatch.setitem(gen._UNPROJECTED, "RUNG_ORDER", "reason")

        errors = gen._projection_coverage_errors(gen.render())

        assert any("RUNG_ORDER" in error and "disagree" in error for error in errors)

    def test_every_exemption_carries_a_reason(self) -> None:
        """An exemption without a reason is an omission with extra steps."""
        for name, reason in gen._UNPROJECTED.items():
            assert len(reason) > 40, f"{name} is withheld without a substantive reason"

    def test_the_write_authority_allowlist_stays_out_of_the_browser(self) -> None:
        """The one exemption that is a safety property rather than a redundancy argument.

        ``ALL_PROVENANCE`` and ``SEVERITIES`` are withheld because they are derivable. The
        allowlist is withheld because a client-side copy of a write permission can be stale while
        looking authoritative, and the playground must display what the engine decided rather than
        re-decide it. Pinned as a test so the exemption cannot be relaxed as a convenience.
        """
        assert "CONSTRAINT_CHECKABLE_DETECTORS" in gen._UNPROJECTED
        assert "CONSTRAINT_CHECKABLE_DETECTORS" not in gen.render()
        assert vocabulary.CONSTRAINT_CHECKABLE_DETECTORS
