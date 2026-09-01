"""The sandbox measurement floor must be derived from source, not restated in prose.

`docs/automation/README.md` tells an unattended run which commands it can execute with no
`pip install`. That document is read by a fire that has no way to check it and a budget too
small to recover from being wrong, so a false entry there costs the whole run -- which is
exactly what happened: the note claimed ten stdlib-only scripts and offered five commands
as needing no installs, when the true count was six and three of the five
(`docs_truth.py`, `gate_population.py`, `openapi_contract.py`) import outside the standard
library.

The failure was not carelessness. The note recorded that those commands had been "verified
to exit 0", and they had been -- in a full virtualenv, **an environment in which a
zero-install claim cannot fail**. Verification that cannot falsify the claim is not
verification, and the same shape has bitten this repository before as "a green local gate
is not evidence about CI".

`docs_truth.py` is the wrong instrument here: it binds a number to a JSON pointer in a
measured artifact, and this is a *structural* property of the import graph. So the claim is
derived instead, by the same rule the write-primitive registry follows -- derive the
population, never restate it. Both directions are checked, so a script that gains a
dependency and one that loses one are equally visible.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_CI_DIR = _REPO / "scripts" / "ci"
_DOC = _REPO / "docs" / "automation" / "README.md"

#: Scripts that are stdlib-only but must NOT be offered to an unattended run, with the
#: reason. Derived membership answers "does this need installs"; it cannot answer "is this
#: safe to run unattended", which is a judgement about side effects.
_UNSAFE_UNATTENDED = {
    "mutate_adversarial_corpus.py": "rewrites the adversarial corpus",
    "mutate_autoapply_guards.py": "mutates product source in place",
    "mutate_domain_vocabulary.py": "rewrites the generated vocabulary",
    "installed_cli_smoke.py": "requires an installed console script, not just the source",
}


def _toplevel_imports(path: Path) -> set[str]:
    """Return every root module name imported anywhere in a script.

    Walks the whole tree rather than only module-level statements: a dependency imported
    inside a function still has to be installed before that code path runs, and the
    zero-install claim is about the command succeeding, not about import time.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.add(node.module.split(".")[0])
    return modules


def _stdlib_only_scripts() -> dict[str, set[str]]:
    """Map each stdlib-only script in scripts/ci/ to its (empty) external import set."""
    stdlib = set(sys.stdlib_module_names)
    result: dict[str, set[str]] = {}
    for path in sorted(_CI_DIR.glob("*.py")):
        external = {m for m in _toplevel_imports(path) if m not in stdlib}
        if not external:
            result[path.name] = external
    return result


def _documented_zero_install_commands() -> list[str]:
    """Return the script names in the document's zero-install table."""
    text = _DOC.read_text(encoding="utf-8")
    marker = "| Zero-install command | Reports |"
    start = text.find(marker)
    if start == -1:
        pytest.fail(
            "docs/automation/README.md no longer contains the zero-install table header. "
            "If the table was renamed, update this test rather than deleting the check -- "
            "an unattended run still needs to know what it can execute."
        )
    table = text[start:].split("\n\n", 1)[0]
    return re.findall(r"scripts/ci/([\w_]+\.py)", table)


class TestTheDocumentIsNotVacuous:
    """Guards that would let every assertion below pass trivially."""

    def test_the_document_exists(self) -> None:
        assert _DOC.exists(), f"{_DOC} is required by this test"

    def test_the_zero_install_table_is_not_empty(self) -> None:
        documented = _documented_zero_install_commands()
        assert documented, (
            "the zero-install table lists no commands, so every claim about it is "
            "vacuously true and an unattended run is told nothing"
        )

    def test_some_script_is_stdlib_only(self) -> None:
        assert _stdlib_only_scripts(), (
            "no stdlib-only script was derived, which would make the comparison below "
            "vacuous -- more likely the AST walk or the stdlib set is broken"
        )


class TestEveryDocumentedCommandIsActuallyZeroInstall:
    """The claim that cost a run: a listed command that needs an install."""

    def test_no_documented_command_imports_outside_the_stdlib(self) -> None:
        stdlib = set(sys.stdlib_module_names)
        offenders: dict[str, list[str]] = {}
        for name in _documented_zero_install_commands():
            path = _CI_DIR / name
            assert path.exists(), f"{name} is listed as zero-install but does not exist"
            external = sorted(m for m in _toplevel_imports(path) if m not in stdlib)
            if external:
                offenders[name] = external
        assert not offenders, (
            f"these are documented as needing no installs but import outside the standard "
            f"library: {offenders}. A fire following that guidance dies on a dependency "
            f"wall, which is the failure the document exists to prevent."
        )

    def test_no_documented_command_is_unsafe_unattended(self) -> None:
        listed = set(_documented_zero_install_commands())
        unsafe = {n: r for n, r in _UNSAFE_UNATTENDED.items() if n in listed}
        assert not unsafe, (
            f"these are offered to an unattended run but have side effects: {unsafe}. "
            f"Being stdlib-only is not the same as being safe to run."
        )


class TestTheStatedCountMatchesTheDerivedSet:
    """A count in prose rots silently; this is what makes it fail loudly instead."""

    def test_the_document_states_the_derived_stdlib_only_count(self) -> None:
        derived = _stdlib_only_scripts()
        text = _DOC.read_text(encoding="utf-8")
        match = re.search(
            r"\*\*(\w+)\*\* scripts under `scripts/ci/` import only the standard library",
            text,
        )
        if match is None:
            pytest.fail(
                "docs/automation/README.md no longer states the stdlib-only script count "
                "in the expected form. Restore it or update this test -- the count is the "
                "part that rots."
            )
        words = {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5,
            "six": 6,
            "seven": 7,
            "eight": 8,
            "nine": 9,
            "ten": 10,
            "eleven": 11,
            "twelve": 12,
        }
        stated_word = match.group(1).lower()
        stated = words.get(stated_word)
        assert stated is not None, (
            f"could not read {match.group(1)!r} as a number; extend the mapping in this test"
        )
        assert stated == len(derived), (
            f"the document says {stated} stdlib-only scripts under scripts/ci/, but "
            f"{len(derived)} were derived: {sorted(derived)}. Update the document."
        )

    def test_every_safe_stdlib_only_script_is_either_listed_or_declared_unsafe(self) -> None:
        """Both directions, so a newly-safe script cannot go unoffered in silence."""
        derived = set(_stdlib_only_scripts())
        listed = set(_documented_zero_install_commands())
        unaccounted = derived - listed - set(_UNSAFE_UNATTENDED)
        assert not unaccounted, (
            f"these scripts are stdlib-only and neither offered to an unattended run nor "
            f"declared unsafe: {sorted(unaccounted)}. Add them to the table, or record why "
            f"they must not run unattended in _UNSAFE_UNATTENDED."
        )

    def test_the_unsafe_list_has_no_stale_entries(self) -> None:
        """A renamed or deleted script must not linger here looking like coverage."""
        stale = [n for n in _UNSAFE_UNATTENDED if not (_CI_DIR / n).exists()]
        assert not stale, f"_UNSAFE_UNATTENDED names scripts that no longer exist: {stale}"
