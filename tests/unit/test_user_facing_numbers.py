"""Every measured number a USER SEES must be bound to an artifact, or exempted with a reason.

Why this file exists, dated 2026-08-26
--------------------------------------
`dataforge/cli/constraints.py` prints a warning at what its own docstring calls "the moment of
choice": the point where a human accepts mined functional dependencies and thereby authorises
unsupervised writes to their own data. That warning said **86 cells overwritten, 0.1601 harmful write
rate**. Those were the figures for a 0.95-floor premise. The premise that keystroke actually creates
is measured at **116 and 0.2046**.

So the single sentence a user reads while deciding whether to let software rewrite their data
understated the harm of that decision, in the direction that encourages it, and it went stale within
hours of the correct number being published.

**Nothing could have caught it.** `readme_truth.py` polices documents. `docs_truth.py` bound document
prose to artifacts. A number living in a Python string was bound by nothing -- which made the
least-guarded claim in the product the one with the most consequence attached.

The fix needed no new gate: `docs_truth.py` reads its `doc` field as text and does not care about the
file extension, so a claim can name a `.py` file. This test gates the **class** rather than the two
instances: a number a user reads is a published claim, so it must be bound or exempted.

Scope, and why it is this narrow
--------------------------------
Only string literals passed to a printing call are policed. An earlier draft scanned every string
constant and produced **68** hits, almost all docstrings, arXiv identifiers and dates -- the same
over-firing that the detector-count gate produced on "GPT-5 family" earlier the same day. Docstrings
are developer-facing and are covered by review; policing them would train readers to ignore this test,
which is the failure mode that matters most for a gate.

Narrowed to printed strings, the live tree yields **two** hits, and both were real.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Final

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = PROJECT_ROOT / "dataforge"
LEDGER = PROJECT_ROOT / "docs" / "quantitative_claims.yaml"

#: Method names that put text in front of a user. `rich`'s console, Typer's echo, and logging.
_PRINTERS: Final[frozenset[str]] = frozenset({"print", "echo", "secho", "log", "warn"})

#: Words that mark a string as asserting a MEASURED quantity rather than describing a parameter.
#: Deliberately short: a string that merely contains a number is not a claim about evidence.
_CLAIM_WORDS: Final[tuple[str, ...]] = (
    "measured",
    "harmful write rate",
    "overwrit",
    "corrupt",
)

#: A standalone numeric token, matching `docs_truth._token_pattern`'s notion of a value so this test
#: cannot flag something the ledger has no way to express. The optional `x`/`%` suffix means a
#: multiplier like `19x` IS policed -- which is how the unbindable rounded ratio in the constraints
#: warning was found and replaced with the two counts behind it.
_NUMBER: Final[re.Pattern[str]] = re.compile(
    r"(?<![\w.])(\d+(?:,\d{3})*(?:\.\d+)?)(?:x|%)?(?![\w])"
)

#: Printed numbers that are NOT measurements, each with the reason it cannot be bound to an artifact.
#: An exemption is a claim about the number's nature, so it carries an argument rather than a name.
_EXEMPT: Final[dict[tuple[str, str], str]] = {
    (
        "dataforge/cli/calibrate.py",
        "certification will refuse without a measured bound",
    ): (
        "alpha=0.05, the 0.10 it becomes, beta=0.5 and a suggested --plant-controls 30 are a worked "
        "illustration plus a flag default. 0.10 is arithmetic on the other two, not a value any "
        "artifact holds, so there is nothing to bind it to. The sentence asserts a RELATIONSHIP "
        "between parameters, not a measurement of this product's behaviour."
    ),
}


def _printed_strings(tree: ast.Module) -> list[tuple[int, str]]:
    """Return ``(lineno, text)`` for every string handed to a printing call.

    Concatenated literals and f-string fragments are joined, because a claim is routinely split
    across several source lines and each fragment alone would carry neither the claim word nor the
    number.
    """
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name not in _PRINTERS:
            continue
        parts = [
            inner.value
            for inner in ast.walk(node)
            if isinstance(inner, ast.Constant) and isinstance(inner.value, str)
        ]
        if parts:
            found.append((node.lineno, " ".join(parts)))
    return found


def _bound_values_by_doc() -> dict[str, set[str]]:
    """Map each ledger ``doc`` to the set of rendered values bound within it."""
    yaml = pytest.importorskip("yaml")
    claims = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))["claims"]
    bound: dict[str, set[str]] = {}
    for claim in claims:
        bound.setdefault(str(claim["doc"]), set()).add(str(claim["expect"]))
    return bound


def _exemption_for(relative: str, text: str) -> str | None:
    """Return the exemption reason covering this string, if any."""
    for (path, marker), reason in _EXEMPT.items():
        if path == relative and marker in text:
            return reason
    return None


class TestUserFacingNumbersAreBound:
    """A number a user reads is a published claim, whatever file it lives in."""

    def test_every_printed_measured_number_is_bound_or_exempt(self) -> None:
        """The class gate. Its first run found the stale warning; keep it able to find the next."""
        bound = _bound_values_by_doc()
        failures: list[str] = []

        for path in sorted(PACKAGE.rglob("*.py")):
            relative = path.relative_to(PROJECT_ROOT).as_posix()
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for lineno, text in _printed_strings(tree):
                if not any(word in text.lower() for word in _CLAIM_WORDS):
                    continue
                numbers = {match.group(1) for match in _NUMBER.finditer(text)}
                if not numbers:
                    continue
                if _exemption_for(relative, text) is not None:
                    continue
                unbound = sorted(numbers - bound.get(relative, set()))
                if unbound:
                    failures.append(
                        f"{relative}:{lineno} prints measured numbers {unbound} that no ledger "
                        f"entry binds. Add a claim to docs/quantitative_claims.yaml with "
                        f"doc: {relative}, or add an exemption to _EXEMPT with the reason the "
                        "number is not a measurement."
                    )

        assert failures == [], "unbound user-facing numbers:\n" + "\n".join(failures)

    def test_the_write_authorisation_warning_is_covered(self) -> None:
        """Non-vacuity, aimed at the specific sentence that motivated this file.

        Without this, narrowing the scan for a false positive could silently drop the one string
        whose staleness had the most consequence, and the class gate above would still pass.
        """
        bound = _bound_values_by_doc()

        assert "dataforge/cli/constraints.py" in bound
        # The shipped figures, not the proxy ones.
        assert {"116", "0.2046"} <= bound["dataforge/cli/constraints.py"]

    def test_the_scan_actually_finds_printed_claims(self) -> None:
        """Guard against the gate becoming vacuous by finding nothing at all.

        A checker whose scan silently matches zero strings passes forever. This asserts the scan
        reaches the constraints warning, independently of whether its numbers are bound.
        """
        tree = ast.parse((PACKAGE / "cli" / "constraints.py").read_text(encoding="utf-8"))

        claims = [
            text
            for _lineno, text in _printed_strings(tree)
            if any(word in text.lower() for word in _CLAIM_WORDS)
        ]

        assert claims, "the printed-string scan no longer reaches the write-authorisation warning"
        assert any("AUTHORIZE WRITES" in text for text in claims)

    def test_every_exemption_still_matches_something(self) -> None:
        """A stale exemption hides the next real omission behind dead bookkeeping."""
        unmatched: list[str] = []
        for path_marker in _EXEMPT:
            relative, marker = path_marker
            source = PROJECT_ROOT / relative
            if not source.exists() or marker not in source.read_text(encoding="utf-8"):
                unmatched.append(f"{relative}: {marker!r}")

        assert unmatched == [], f"exemptions matching nothing: {unmatched}"

    def test_every_exemption_carries_a_substantive_reason(self) -> None:
        """An exemption without an argument is an omission with extra steps."""
        for path_marker, reason in _EXEMPT.items():
            assert len(reason) > 80, f"{path_marker} is exempted without a substantive reason"
