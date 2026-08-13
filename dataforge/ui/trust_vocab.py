"""Shared trust vocabulary -- the non-visual twin of the perceptual language.

This module is the single source of truth for how DataForge's trust vocabulary
reads as *text*, so the CLI, the MCP tools, and (where useful) the playground API
all speak the same language a screen reader or a ``NO_COLOR`` terminal can follow.

The TOKENS and their phrasing come from ``dataforge.domain.vocabulary``; this module
owns only presentation (glyphs, labels, disposition wording). It used to re-list all 13
review reasons and their sentences by hand, which is how one reason went missing and
rendered to a user as a raw machine token.

Rendering only -- no business logic. Pure Python (no ``rich``) so every surface,
including the rich-free MCP package, can import it.
"""

from __future__ import annotations

import os
import sys
from typing import IO

from dataforge.domain.vocabulary import REVIEW_REASON_HUMAN

__all__ = [
    "REVIEW_REASON_HUMAN",
    "SEVERITY_GLYPH",
    "disposition_label",
    "humanize_review_reason",
    "independent_verification_label",
    "severity_glyph",
    "verification_strength_label",
]


def humanize_review_reason(reason: str | None) -> str:
    """Render a machine ``review_reason`` token as an honest sentence."""
    if not reason:
        return "Held for review -- not proven safe to auto-apply."
    known = REVIEW_REASON_HUMAN.get(reason)
    if known is not None:
        return known
    return reason.replace("_", " ").strip().capitalize() + "."


def verification_strength_label(strength: str | None) -> str:
    """Text label for ``verification_strength``.

    ``proven`` reads as trustworthy; ``plausibility_only`` always carries the
    "not written" frame, because an unproven value is never silently applied.
    """
    if strength == "proven":
        return "proven"
    if strength == "plausibility_only":
        return "plausibility-only -- not written"
    return "unverified -- not written"


def independent_verification_label(value: str | None) -> str:
    """Text label for ``independent_verification``."""
    if value == "agreed":
        return "independently verified (two verifiers agreed)"
    return "single verifier"


def disposition_label(*, applied: bool, held: bool = False, rejected: bool = False) -> str:
    """One disposition vocabulary everywhere: applied / held / rejected."""
    if applied:
        return "applied"
    if rejected:
        return "rejected"
    if held:
        return "held"
    return "held"


# --- Non-visual redundancy: meaning must survive color stripping -------------
# Text glyphs so severity/strength are legible even when Rich emits no color
# (NO_COLOR, piped output, screen readers).
SEVERITY_GLYPH: dict[str, str] = {
    "safe": "[ok]",
    "review": "[review]",
    "unsafe": "[!!]",
}


def severity_glyph(severity: str) -> str:
    """A color-independent glyph for a severity level."""
    return SEVERITY_GLYPH.get(severity.lower(), "[*]")


def strength_glyph(strength: str | None) -> str:
    """A color-independent glyph for a verification strength."""
    if strength == "proven":
        return "[proven]"
    return "[plausible]"


def should_use_color(stream: IO[str] | None = None) -> bool:
    """Whether ANSI color is appropriate for ``stream``.

    Honors the ``NO_COLOR`` convention and ``FORCE_COLOR``, then falls back to
    TTY detection. Rich also honors ``NO_COLOR`` at the library level; this helper
    lets non-Rich surfaces (and explicit checks) behave identically.
    """
    if os.environ.get("NO_COLOR") is not None:
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    target = stream if stream is not None else sys.stdout
    try:
        return bool(target.isatty())
    except Exception:
        return False
