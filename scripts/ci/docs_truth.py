"""Verify that hand-written numbers in prose docs match the artifacts they came from.

The existing truth checkers each cover a different surface and none covers this one:

* ``readme_truth.py`` polices whether a *kind* of claim is allowed to appear at all;
* ``benchmark_truth.py`` regenerates marker-delimited blocks and byte-compares them;
* ``openapi_contract.py`` diffs canonical schema snapshots.

So a number typed by hand into ``DECISIONS.md`` or a pre-registration was never checked
against its evidence. The API phase published five false or mis-scoped numbers that way,
and every one was caught by manual re-investigation rather than by CI. This closes that gap.

Design notes, all deliberate:

* **No float tolerance.** Each claim states the exact rendered string, mirroring how the
  rest of the repo handles numbers in markdown (``dataforge/bench/report.py`` freezes
  floats to fixed precision at generation time, so string equality *is* the numeric
  comparison). Tolerances invite drift; an exact expectation forces a deliberate edit.
* **Bidirectional.** A claim fails if the artifact no longer produces the expected value
  *or* if the value is missing from the prose. Silent divergence in either direction is the
  failure mode being prevented.
* **The prose side matches a TOKEN, not a substring, and short values must declare context.**
  Added 2026-08-26 after auditing this checker against itself. The prose check was
  ``expected not in doc.read_text()`` -- a raw substring over the whole file -- so a claim whose
  expected value was ``"0"`` passed because the document contained ``2026-08-26``. Fourteen of
  forty-six claims had an ``expect`` of two characters or fewer, and **six were verifiable in
  name only**: the prose could be edited to contradict them and CI would still pass. One of the
  six was a control arm described in its own document as the number that made the claim precise.
  So the summary line "Verified 46 quantitative claims" was itself an overclaim, which is the
  exact defect this file exists to prevent, committed by the file that prevents it.

  Two corrections, neither of which is a tunable constant:

  1. the value must appear as a standalone token -- not inside a longer number, word, path or
     date -- so a digit occurring incidentally cannot satisfy a claim;
  2. a claim whose rendered value is two characters or fewer must declare ``context``: a
     substring that has to appear on the *same line* as the token. Short integers are
     unavoidably common in prose, so for them token-matching alone is still too weak, and the
     honest fix is to make the claim say where it lives rather than to loosen the check.

  ``context`` is optional for longer values and always permitted, because pinning a number to
  its sentence is strictly more informative than pinning it to a file.
* **Retraction-aware.** Withdrawn figures must stay visible in these documents (a
  requirement enforced by ``tests/unit/test_flagship_artifact_honesty.py``), so this checker
  only requires that the *current* value is present. It never requires that superseded
  values are absent.

Usage::

    python scripts/ci/docs_truth.py --check
    python scripts/ci/docs_truth.py --write   # refresh expectations from artifacts
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Final

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LEDGER = PROJECT_ROOT / "docs" / "quantitative_claims.yaml"

#: Rendered values at or below this length must declare ``context``. Two characters is not a
#: tuning knob chosen for a pass rate -- it is the length at which a value stops being
#: self-identifying in English prose. "0.8519" occurs in a document because someone wrote that
#: number; "0" and "11" occur in dates, row counts, file names and section numbers.
CONTEXT_REQUIRED_MAX_LENGTH: Final[int] = 2


def _resolve_pointer(payload: Any, pointer: str) -> Any:
    """Resolve a slash-delimited JSON pointer, raising a readable error on a miss."""
    current = payload
    for raw in pointer.strip("/").split("/"):
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            try:
                current = current[int(token)]
            except (ValueError, IndexError) as exc:
                raise KeyError(f"index {token!r} not usable in {pointer!r}") from exc
        elif isinstance(current, dict):
            if token not in current:
                raise KeyError(f"key {token!r} missing in {pointer!r}")
            current = current[token]
        else:
            raise KeyError(f"cannot descend into {type(current).__name__} at {token!r}")
    return current


def _load_claims() -> list[dict[str, Any]]:
    """Read the claim ledger."""
    import yaml

    raw = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("claims"), list):
        raise ValueError(f"{LEDGER.name} must be a mapping containing a 'claims' list.")
    return [claim for claim in raw["claims"] if isinstance(claim, dict)]


def _rendered(claim: dict[str, Any]) -> tuple[str, str | None]:
    """Return ``(rendered_value, error)`` for one claim's artifact value."""
    artifact = PROJECT_ROOT / str(claim["artifact"])
    if not artifact.exists():
        return "", f"artifact {claim['artifact']} does not exist"
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return "", f"artifact {claim['artifact']} is not valid JSON: {exc}"
    try:
        value = _resolve_pointer(payload, str(claim["pointer"]))
    except KeyError as exc:
        return "", f"{claim['artifact']}: {exc}"
    if value is None:
        return "", f"{claim['artifact']}{claim['pointer']} resolved to null"
    try:
        return str(claim.get("format", "{}")).format(value), None
    except (ValueError, TypeError) as exc:
        return "", f"cannot format {value!r} with {claim.get('format')!r}: {exc}"


def _token_pattern(value: str) -> re.Pattern[str]:
    """Compile a pattern matching ``value`` only as a standalone token.

    The boundaries are asymmetric on purpose, because a rendered number can legitimately be
    followed by a full stop that ends a sentence but must not be followed by one that continues a
    number:

    * left ``(?<![\\w.])`` rejects ``10.6189`` when looking for ``0.6189``, and ``2026`` when
      looking for ``0``;
    * right ``(?!\\w|\\.\\d)`` rejects ``0.61890`` and ``11`` inside ``110``, while still allowing
      ``precision is 0.6189.`` at the end of a sentence.

    Verified against the case that motivated this: ``0`` does not match anywhere in
    ``2026-08-26``, and ``1`` does not match inside ``premised_fd_10rows.csv``.
    """
    return re.compile(r"(?<![\w.])" + re.escape(value) + r"(?!\w|\.\d)")


def _prose_errors(claim_id: str, claim: dict[str, Any], expected: str) -> list[str]:
    """Check the prose side of one claim: token presence, and context when required.

    Returns:
        One error string per distinct failure, so a claim that is both context-less and absent
        reports both rather than only the first.
    """
    errors: list[str] = []
    doc_path = PROJECT_ROOT / str(claim["doc"])
    if not doc_path.exists():
        return [f"{claim_id}: doc {claim['doc']} does not exist"]

    context = claim.get("context")
    if context is not None and not isinstance(context, str):
        return [f"{claim_id}: 'context' must be a string when present"]
    if len(expected) <= CONTEXT_REQUIRED_MAX_LENGTH and not context:
        errors.append(
            f"{claim_id}: rendered value {expected!r} is {len(expected)} character(s), so the "
            "claim must declare 'context' -- a substring that appears on the same line as the "
            "number. Short integers occur incidentally in dates, row counts and file names, so "
            "presence alone does not show the prose states this claim."
        )

    text = doc_path.read_text(encoding="utf-8")
    matches = [
        line for line in text.splitlines() if _token_pattern(expected).search(line) is not None
    ]
    if not matches:
        errors.append(
            f"{claim_id}: {claim['doc']} does not state {expected!r} as a standalone value, the "
            f"current value of {claim['artifact']}{claim['pointer']}. The prose and the evidence "
            "have diverged."
        )
        return errors

    if context and not any(context.lower() in line.lower() for line in matches):
        errors.append(
            f"{claim_id}: {claim['doc']} states {expected!r} but never on a line containing "
            f"{context!r}. The number appears without the claim it is supposed to support, which "
            "is how a value satisfies a check while the sentence around it says something else."
        )
    return errors


def check() -> int:
    """Verify every claim; return a process exit code."""
    errors: list[str] = []
    checked = 0
    with_context = 0
    for claim in _load_claims():
        claim_id = claim.get("id", "<unnamed>")
        for field in ("doc", "artifact", "pointer", "expect"):
            if field not in claim:
                errors.append(f"{claim_id}: ledger entry is missing '{field}'")
        if any(field not in claim for field in ("doc", "artifact", "pointer", "expect")):
            continue

        rendered, error = _rendered(claim)
        if error:
            errors.append(f"{claim_id}: {error}")
            continue

        expected = str(claim["expect"])
        if rendered != expected:
            errors.append(
                f"{claim_id}: artifact now yields {rendered!r} but the ledger expects "
                f"{expected!r} ({claim['artifact']}{claim['pointer']}). If the new value is "
                "correct, run scripts/ci/docs_truth.py --write and update the prose."
            )
            continue

        doc_errors = _prose_errors(claim_id, claim, expected)
        if doc_errors:
            errors.extend(doc_errors)
            continue
        checked += 1
        if claim.get("context"):
            with_context += 1

    if errors:
        print("Docs truth check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    # Reported separately so the summary cannot overclaim again. A claim verified by token alone is
    # pinned to a document; one verified with context is pinned to a sentence.
    print(
        f"Docs truth check passed. Verified {checked} quantitative claims against artifacts "
        f"({with_context} pinned to a specific sentence via 'context', "
        f"{checked - with_context} to a standalone value in the document)."
    )
    return 0


def write() -> int:
    """Refresh each claim's ``expect`` from its artifact, reporting what changed."""
    text = LEDGER.read_text(encoding="utf-8")
    changed: list[str] = []
    for claim in _load_claims():
        rendered, error = _rendered(claim)
        if error:
            print(f"skipping {claim.get('id')}: {error}", file=sys.stderr)
            continue
        expected = str(claim["expect"])
        if rendered == expected:
            continue
        # Replace only within this claim's own block, so identical numbers elsewhere in
        # the ledger are untouched.
        marker = f"id: {claim['id']}"
        start = text.find(marker)
        if start < 0:
            continue
        segment_end = text.find("\n  - id:", start)
        segment_end = len(text) if segment_end < 0 else segment_end
        segment = text[start:segment_end]
        updated = segment.replace(f'expect: "{expected}"', f'expect: "{rendered}"', 1)
        if updated != segment:
            text = text[:start] + updated + text[segment_end:]
            changed.append(f"{claim['id']}: {expected} -> {rendered}")

    if changed:
        LEDGER.write_text(text, encoding="utf-8")
        print("Updated expectations:")
        for line in changed:
            print(f"  - {line}")
        print("\nNow update the prose in the affected documents to match.")
    else:
        print("All expectations already match their artifacts.")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Verify prose against artifacts.")
    mode.add_argument("--write", action="store_true", help="Refresh expectations from artifacts.")
    args = parser.parse_args(argv)
    return write() if args.write else check()


if __name__ == "__main__":
    raise SystemExit(main())
