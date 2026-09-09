"""Regression tests for the automation pipeline's prompt files."""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROMPTS = _REPO_ROOT / "scripts" / "automation" / "prompts"

# The four stage prompt filenames and their expected stage numbers.
_STAGE_FILES = [
    ("stage1-explore.md", 1),
    ("stage2-plan.md", 2),
    ("stage3-code.md", 3),
    ("stage4-verify-commit.md", 4),
]


@pytest.mark.parametrize(
    ("filename", "stage_number"),
    _STAGE_FILES,
    ids=[f"stage{n}" for _, n in _STAGE_FILES],
)
def test_stage_prompt_declares_status_lines(filename: str, stage_number: int) -> None:
    """Each stage prompt must exist, be non-empty, and declare both status lines.

    The local pickup script reads ``STAGE<N>_OK`` and ``STAGE<N>_FAILED`` from
    each stage's transcript to decide whether the stage succeeded.  These tokens
    are specified only inside the prompt files themselves.  If a prompt is edited
    and its status line is renamed or dropped, the pipeline silently produces
    nothing — the fire runs, does its work, and the token the tooling looks for
    never appears.  This test makes that class of breakage visible.
    """
    path = _PROMPTS / filename
    assert path.exists(), f"{filename} is missing from scripts/automation/prompts/"
    content = path.read_text(encoding="utf-8")
    assert len(content.strip()) > 0, f"{filename} is empty"
    ok_token = f"STAGE{stage_number}_OK"
    failed_token = f"STAGE{stage_number}_FAILED"
    assert ok_token in content, f"{filename} does not contain {ok_token!r}"
    assert failed_token in content, f"{filename} does not contain {failed_token!r}"


def test_preamble_exists_and_has_no_dollar_dollar() -> None:
    """The shared preamble must exist, be non-empty, and contain no ``$$`` sequence.

    ``create_stages.ps1`` embeds each prompt inside a ``CREATE AGENT TASK``
    statement using ``$$`` as the SQL body delimiter.  If the preamble itself
    contains ``$$``, the generated SQL is unparseable.  The PowerShell script
    rejects this at build time (line 119-123); this test enforces the same
    invariant from the Python test suite so it runs on every commit, not only
    when someone remembers to run the build script.
    """
    path = _PROMPTS / "_preamble.md"
    assert path.exists(), "_preamble.md is missing from scripts/automation/prompts/"
    content = path.read_text(encoding="utf-8")
    assert len(content.strip()) > 0, "_preamble.md is empty"
    assert "$$" not in content, (
        "_preamble.md contains '$$', which breaks the CREATE AGENT TASK SQL delimiter"
    )
