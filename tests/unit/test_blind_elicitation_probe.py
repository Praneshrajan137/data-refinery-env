"""Tests for the blind-elicitation protocol probe.

Offline. No API calls. These pin the two things that decide whether the measurement means
anything: that the arms differ **only** in proposal-revelation, and that an item which is not
actually a control cannot enter the sample.

Pre-registration: `eval/preregistration/blind_elicitation.md`.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

_PROBE = Path(__file__).resolve().parents[2] / "scripts" / "bench" / "probe_label_protocol.py"


def _load() -> object:
    spec = importlib.util.spec_from_file_location("probe_label_protocol", _PROBE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so dataclasses can resolve the module by name; without this,
    # @dataclass raises AttributeError on module_from_spec output.
    sys.modules["probe_label_protocol"] = module
    spec.loader.exec_module(module)
    return module


probe = _load()
_CONTEXT = ["1998", "2001", "2004"]


def _item(proposal: str = "1999", truth: str = "2000") -> object:
    return probe.ControlItem(  # type: ignore[attr-defined]
        row=3, column="article_jcreated_at", flagged="19999", proposal=proposal, truth=truth
    )


class TestTheArmsDifferOnlyInRevelation:
    """The independent variable, asserted rather than eyeballed."""

    def test_prompts_are_identical_outside_the_reveal_block(self) -> None:
        assert probe.prompts_differ_only_in_reveal(_item(), _CONTEXT) is True  # type: ignore[attr-defined]

    def test_the_check_detects_arm_dependent_text_outside_the_reveal(self) -> None:
        """Non-vacuity, and it required being honest about what the check can see.

        `prompts_differ_only_in_reveal` strips exactly the reveal strings it inserted, so it
        cannot detect extra text placed *inside* a reveal constant -- that would strip cleanly and
        still compare equal. What it genuinely catches is the shared template being rendered
        differently per arm: arm-dependent context, column formatting, or flagged-value handling.

        This test injects exactly that fault by making the shared template's rendering depend on
        the arm, and asserts the check fails. Without it, a `True` return proves nothing.
        """
        original = probe.build_prompt  # type: ignore[attr-defined]

        def arm_dependent(item: object, *, arm: str, context: list[str]) -> str:
            rendered = original(item, arm=arm, context=context)
            # A realistic fault: the ratify arm gains a hint outside its reveal block.
            return rendered + ("\nReview carefully." if arm == "ratify" else "")

        try:
            probe.build_prompt = arm_dependent  # type: ignore[attr-defined]
            assert probe.prompts_differ_only_in_reveal(_item(), _CONTEXT) is False  # type: ignore[attr-defined]
        finally:
            probe.build_prompt = original  # type: ignore[attr-defined]

    def test_reveal_text_lands_only_in_its_own_arm(self) -> None:
        """Each arm's reveal block must not leak into the other arm's prompt."""
        original = probe._RATIFY_REVEAL  # type: ignore[attr-defined]
        try:
            # Text that the strip step will not remove, so it survives into the comparison.
            probe._RATIFY_REVEAL = original + "\nEXTRA UNSHARED SENTENCE"  # type: ignore[attr-defined]
            item = _item()
            ratify = probe.build_prompt(item, arm="ratify", context=_CONTEXT)  # type: ignore[attr-defined]
            elicit = probe.build_prompt(item, arm="elicit", context=_CONTEXT)  # type: ignore[attr-defined]
            assert "EXTRA UNSHARED SENTENCE" in ratify
            assert "EXTRA UNSHARED SENTENCE" not in elicit
        finally:
            probe._RATIFY_REVEAL = original  # type: ignore[attr-defined]

    def test_the_elicit_arm_never_contains_the_proposal(self) -> None:
        """The whole point of the arm. A leak here would silently make it a ratify arm."""
        item = _item(proposal="UNIQUE-PROPOSAL-TOKEN")
        elicit = probe.build_prompt(item, arm="elicit", context=_CONTEXT)  # type: ignore[attr-defined]
        assert "UNIQUE-PROPOSAL-TOKEN" not in elicit
        ratify = probe.build_prompt(item, arm="ratify", context=_CONTEXT)  # type: ignore[attr-defined]
        assert "UNIQUE-PROPOSAL-TOKEN" in ratify

    def test_an_unknown_arm_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown arm"):
            probe.build_prompt(_item(), arm="both", context=_CONTEXT)  # type: ignore[attr-defined]


class TestOnlyRealControlsEnterTheSample:
    """A correct proposal cannot measure a false-accept rate."""

    def test_a_proposal_equal_to_truth_raises(self) -> None:
        with pytest.raises(ValueError, match="not a control"):
            _item(proposal="2000", truth="2000")

    def test_equality_is_checked_after_normalisation(self) -> None:
        """Otherwise ' 2000 ' would pad the denominator and deflate beta."""
        with pytest.raises(ValueError, match="not a control"):
            _item(proposal="  2000  ", truth="2000")

    def test_a_genuinely_wrong_proposal_is_accepted(self) -> None:
        item = _item(proposal="1999", truth="2000")
        assert item.proposal == "1999"


class TestNormalisation:
    """Fixed in the pre-registration, so it cannot be tuned after seeing results."""

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            ("Smith, J.", "smith, j."),
            ("a  b", "a b"),
            (" x ", "x"),
            ("A\tB", "a b"),
        ],
    )
    def test_equivalent_strings_normalise_together(self, left: str, right: str) -> None:
        assert probe._normalise(left) == probe._normalise(right)  # type: ignore[attr-defined]

    def test_genuinely_different_strings_stay_different(self) -> None:
        assert probe._normalise("1999") != probe._normalise("2000")  # type: ignore[attr-defined]


class TestImportingTheProbeHasNoSideEffects:
    """A module import must not read secrets.

    This class exists because of a real failure. The probe originally called `load_dotenv` at
    module level, and this test file imports the probe, so importing it read the real Azure API key
    into `os.environ` for the entire pytest session. That broke
    `tests/integration/test_agent_cli.py::test_hosted_without_key_fails_clearly`, whose whole
    point is that no key is present -- a failure that appeared only in the full run and passed in
    isolation, which is the signature of exactly this kind of leak.
    """

    def test_importing_the_probe_does_not_populate_azure_credentials(self) -> None:
        """Measured in a clean subprocess, because session state is not this test's to assert.

        An in-process check on `os.environ` was tried first: it failed in the full run while
        passing in isolation, because other modules in the session also load dotenv. That made it
        a test of a global property rather than of this probe. A subprocess importing only the
        probe measures exactly the thing in question.
        """
        env = {k: v for k, v in os.environ.items() if not k.startswith("AZURE_")}
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import importlib.util, os, sys;"
                    f"spec = importlib.util.spec_from_file_location('p', r'{_PROBE}');"
                    "m = importlib.util.module_from_spec(spec);"
                    "sys.modules['p'] = m;"
                    "spec.loader.exec_module(m);"
                    "print('LEAKED' if os.environ.get('AZURE_API_KEY') else 'CLEAN')"
                ),
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
        )
        assert result.returncode == 0, result.stderr[-600:]
        assert "CLEAN" in result.stdout, (
            "importing the probe read the Azure key into the environment. Credentials must load "
            f"inside main(), not at import time. stdout={result.stdout!r}"
        )

    def test_the_probe_defines_no_module_level_dotenv_call(self) -> None:
        """Pins the fix structurally, so a re-added top-level load_dotenv fails here.

        The environment assertion above can pass by accident when no .env exists on the machine
        running CI, so it is not sufficient on its own.
        """
        source = _PROBE.read_text(encoding="utf-8")
        body = source.split("def main()", 1)[0]
        assert "load_dotenv(" not in body, (
            "load_dotenv appears before main(); importing the probe would read secrets"
        )
