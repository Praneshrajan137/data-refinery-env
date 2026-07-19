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
_TRUST_DOCS = [
    _ROOT / "PRODUCT.md",
    _ROOT / "docs" / "trust" / "accuracy-frontier.md",
]

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


def test_no_unqualified_favorable_sota_comparison() -> None:
    violations: list[str] = []
    for doc in _TRUST_DOCS:
        if not doc.exists():
            continue
        lines = doc.read_text(encoding="utf-8").splitlines()
        for i, line in enumerate(lines):
            if not (_BASELINE.search(line) and _SUPERIORITY.search(line)):
                continue
            window = "\n".join(lines[max(0, i - 2) : i + 3])
            if not _QUALIFIER.search(window):
                violations.append(f"{doc.name}:{i + 1}: {line.strip()}")
    assert not violations, (
        "Unqualified favorable SOTA comparison(s) without a protocol-comparability "
        f"qualifier: {violations}"
    )
