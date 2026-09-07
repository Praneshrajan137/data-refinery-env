"""Guard: favorable SOTA comparisons in trust docs must stay protocol-honest.

The hospital 0.7926 (DataForge harness) vs Raha+Baran 0.73 (BClean protocol)
figures are NOT measured under an identical protocol. Any doc that states a
favorable comparison must carry a comparability qualifier nearby, so the honesty
doctrine (PRODUCT.md) cannot silently regress into an unqualified "beats SOTA".
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]

# `DECISIONS.md` is deliberately NOT policed line-by-line. It is a newest-first
# historical log, and three of its pre-2026-09-01 entries state the retracted
# "beats cited SOTA" / "the one measured SOTA win" claims as what was believed at
# the time. PRODUCT.md 5 forbids rewriting frozen historical evidence to look
# better, so those lines must stand. A fourth apparent hit is the 2026-09-01
# entry *quoting* the bad phrasing in order to record its retraction -- the same
# self-referential false positive that the SECURITY.md exception scan hit.
# The standing retraction notice at the top of DECISIONS.md is what carries the
# correction forward; this guard polices the surfaces that state current claims.
_EXCLUDED_HISTORICAL = frozenset({"DECISIONS.md"})


def _policed_docs() -> list[Path]:
    """Derive the policed population; never restate it.

    This list was originally two hand-named files (`PRODUCT.md` and
    `accuracy-frontier.md`) while `docs/trust/` held 43 documents. A favorable
    comparison written into any of the other 41 was invisible to this guard --
    the same "population narrower than it appears" defect PRODUCT.md 1.3 warns
    about, occurring in the very gate that exists to police that class of claim.

    Deriving it means a new trust document is covered on the day it is added,
    with no edit here.
    """
    named = [_ROOT / "PRODUCT.md", _ROOT / "README.md"]
    trust = sorted((_ROOT / "docs" / "trust").glob("*.md"))
    return [
        path for path in [*named, *trust] if path.exists() and path.name not in _EXCLUDED_HISTORICAL
    ]


_TRUST_DOCS = _policed_docs()

# A line makes a comparison if it names a baseline...
_BASELINE = re.compile(r"raha|baran|0\.73\b|\bsota\b|state[- ]of[- ]the[- ]art", re.IGNORECASE)
# ...and asserts superiority.
_SUPERIORITY = re.compile(
    r"\bbeats?\b|\boutperforms?\b|\bexceeds?\b|better than|\bwin\b", re.IGNORECASE
)
# It is honest only if a comparability qualifier appears in the ±2 line window.
_QUALIFIER = re.compile(
    r"protocol|comparab|under our scoring|own harness|own scoring|not .*head-to-head|"
    r"competitive|in the range of|not rerun|citation",
    re.IGNORECASE,
)


def _unqualified_comparisons(lines: list[str], label: str) -> list[str]:
    """Return every line asserting superiority over a baseline without a qualifier."""
    violations: list[str] = []
    for i, line in enumerate(lines):
        if not (_BASELINE.search(line) and _SUPERIORITY.search(line)):
            continue
        window = "\n".join(lines[max(0, i - 2) : i + 3])
        if not _QUALIFIER.search(window):
            violations.append(f"{label}:{i + 1}: {line.strip()}")
    return violations


def test_no_unqualified_favorable_sota_comparison() -> None:
    violations: list[str] = []
    for doc in _TRUST_DOCS:
        rel = doc.relative_to(_ROOT).as_posix()
        violations.extend(
            _unqualified_comparisons(doc.read_text(encoding="utf-8").splitlines(), rel)
        )
    assert not violations, (
        "Unqualified favorable SOTA comparison(s) without a protocol-comparability "
        f"qualifier: {violations}"
    )


def test_the_policed_population_is_derived_not_restated() -> None:
    """The guard must cover the whole trust corpus, not a hand-named pair.

    It shipped covering 2 documents while 43 existed. A floor here means shrinking
    the population back to a hand-named subset fails instead of passing quietly.
    """
    names = {path.name for path in _TRUST_DOCS}
    trust_dir = sorted((_ROOT / "docs" / "trust").glob("*.md"))

    assert len(trust_dir) >= 40, (
        f"Expected the trust corpus to hold at least 40 documents, found {len(trust_dir)}. "
        "If documents were legitimately removed, lower this floor deliberately."
    )
    missing = {path.name for path in trust_dir} - names
    assert not missing, f"Trust documents excluded from the comparability guard: {sorted(missing)}"
    assert "PRODUCT.md" in names
    assert "README.md" in names
    # The single exclusion is explicit and justified in `_EXCLUDED_HISTORICAL`.
    assert "DECISIONS.md" not in names


def test_the_guard_can_actually_fail() -> None:
    """A gate that cannot fail is indistinguishable from no gate.

    Four gates in this repository were found unable to fail because their tests
    covered a check's logic and never its effect. This pins the effect.
    """
    planted = [
        "Some preceding context line.",
        "DataForge beats Raha+Baran on hospital.",
        "Some following context line.",
    ]
    assert _unqualified_comparisons(planted, "planted.md"), (
        "The guard failed to flag an unqualified favorable comparison."
    )

    # ...and it accepts the same claim once qualified, so it is not merely
    # rejecting every line that names a baseline.
    qualified = [
        "Some preceding context line.",
        "DataForge beats Raha+Baran on hospital.",
        "These are not protocol-comparable; the baseline is citation-only.",
    ]
    assert not _unqualified_comparisons(qualified, "planted.md"), (
        "The guard rejected a properly qualified comparison."
    )
