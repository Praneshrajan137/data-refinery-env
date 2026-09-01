"""CLAUDE.md is injected into every session, so its size and scope are behaviour.

On 2026-08-30 through 2026-09-01, 163 lines of cloud-sandbox operating detail were appended
to CLAUDE.md across three sessions, taking it from 104 lines to 259. None of it applied to
normal local work, and all of it was being fed to every editor session as instructions. It
was moved to `docs/automation/README.md`, which nothing auto-loads.

Nothing prevented that, and nothing prevented it recurring. The file's own final heading was
`## Append-Only From Here Onward`, which is an explicit invitation to grow an auto-injected
instruction file without bound.

This is not a style gate. An auto-injected file is closer to production configuration than
to documentation: every line competes for the reader's attention with the project's actual
conventions, and volatile operational detail in it goes stale in the one place a session is
most likely to trust it. The two failure modes are therefore SIZE and SCOPE, and both are
checked here.

Deliberately NOT checked: whether the content is correct. That is what
`tests/unit/test_corpus_tiering.py` does for the 0.7926 claim and what `docs_truth.py` does
for registered numbers. This file only polices what KIND of thing may live here.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_CLAUDE = _REPO / "CLAUDE.md"

#: Budget. CLAUDE.md sits at 116 lines / ~7.1 KB after the 2026-09-01 restoration; the peak
#: during the inflation was 259 lines. 170 leaves roughly 45% headroom for genuine
#: convention growth while refusing anything close to another 163-line dump. Raise it
#: deliberately, in a commit that says what earned the space -- that review is the point of
#: the number, so a bump with no rationale is the failure this guards against.
_MAX_LINES = 170
_MAX_BYTES = 12_000

#: Markers for content that is scoped to one environment, one session, or one deployment.
#: Each maps to why it does not belong in an always-injected instruction file. These are the
#: actual strings from the content that was removed, not invented examples.
_OUT_OF_SCOPE_MARKERS: dict[str, str] = {
    "USER$": "a Snowflake account-scoped schema name",
    "snow://": "a Snowflake stage URI",
    "/workspace": "a path that only exists inside the automation sandbox",
    "cortex automation": "automation CLI operation, not a repository convention",
    "AGENT TASK": "a Snowflake object type specific to the scheduled review",
    "GH_TOKEN": "a credential name from the sandbox environment",
    "EgressSecrets": "a task-payload internal",
    "cortex ws ": "workspace CLI operation, not a repository convention",
    "Snowsight": "a specific web console",
    "daily-review": "the automation's output artifact",
}

#: Headings that invite unbounded growth. The file may still be appended to -- the project's
#: convention of dated notes is genuinely useful -- but a heading that says so without a
#: budget is how 163 lines arrived one paragraph at a time.
_FORBIDDEN_HEADING_PATTERN = re.compile(r"^##\s+Append-Only\s+From\s+Here\s+Onward\s*$", re.M)


def _text() -> str:
    return _CLAUDE.read_text(encoding="utf-8")


class TestTheFileExists:
    """Non-vacuity: every check below reads this file."""

    def test_claude_md_is_present(self) -> None:
        assert _CLAUDE.exists(), (
            "CLAUDE.md is auto-injected into every session and is required by this gate"
        )

    def test_claude_md_is_not_empty(self) -> None:
        assert _text().strip(), "CLAUDE.md is empty, which would make every check vacuous"


class TestSizeBudget:
    """The failure mode that actually occurred."""

    def test_line_count_is_within_budget(self) -> None:
        lines = len(_text().splitlines())
        assert lines <= _MAX_LINES, (
            f"CLAUDE.md is {lines} lines, over the {_MAX_LINES}-line budget. It is injected "
            f"into every session, so growth here is a change to every session's "
            f"instructions. Either move the new material to a file nothing auto-loads -- "
            f"docs/automation/README.md is the precedent -- or raise _MAX_LINES in a commit "
            f"that says what earned the space."
        )

    def test_byte_count_is_within_budget(self) -> None:
        size = len(_text().encode("utf-8"))
        assert size <= _MAX_BYTES, (
            f"CLAUDE.md is {size} bytes, over the {_MAX_BYTES}-byte budget. The line check "
            f"alone would pass a file of very long lines, which costs the same attention."
        )


class TestScopeBudget:
    """Content that belongs to one environment must not instruct every session."""

    def test_no_environment_scoped_markers(self) -> None:
        text = _text()
        found = {
            marker: reason for marker, reason in _OUT_OF_SCOPE_MARKERS.items() if marker in text
        }
        assert not found, (
            f"CLAUDE.md contains environment- or session-scoped content: {found}. This is "
            f"the 2026-08-30 defect -- sandbox operating detail was instructing local work "
            f"for three sessions. Put it in docs/automation/README.md, which nothing "
            f"auto-loads and which sits beside the directory the automation writes into."
        )

    def test_the_unbounded_append_heading_is_gone(self) -> None:
        assert not _FORBIDDEN_HEADING_PATTERN.search(_text()), (
            "CLAUDE.md still carries the 'Append-Only From Here Onward' heading. Dated "
            "notes are welcome, but a heading that invites unbounded appending to an "
            "auto-injected file is how 163 lines arrived one paragraph at a time. Name the "
            "budget in the heading instead."
        )


class TestTheGuardCannotRotSilently:
    """A marker list is a population; a stale one reads as coverage."""

    def test_every_marker_has_a_reason(self) -> None:
        blank = [m for m, r in _OUT_OF_SCOPE_MARKERS.items() if not r.strip()]
        assert not blank, f"these markers carry no reason: {blank}"

    def test_the_marker_list_is_not_empty(self) -> None:
        assert _OUT_OF_SCOPE_MARKERS, "an empty marker list makes the scope check vacuously pass"

    def test_the_relocation_target_exists(self) -> None:
        """The error messages above tell a reader where to put things instead."""
        target = _REPO / "docs" / "automation" / "README.md"
        assert target.exists(), (
            "docs/automation/README.md is named as the relocation target by this gate's "
            "failure messages; if it moved, update them or they send the next person nowhere"
        )
