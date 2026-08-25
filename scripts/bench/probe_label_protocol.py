"""Measure whether blind elicitation reduces the false-accept rate.

Pre-registered in `eval/preregistration/blind_elicitation.md`. Read that first: it fixes the
sample, the arms, the capability control, the kill criterion and the VOID condition before any
call is made.

The question
------------
`docs/trust/stratified-label-noise-result.md` killed human-labelled per-table certification at
`alpha = 0.05`: the binding control class gives `beta_upper = 0.8712`. The one surviving route is a
protocol that elicits the correct value **before** revealing the machine's proposal, so acquiescence
bias cannot operate. This probe measures the mechanism that route depends on.

Two arms over identical items, independent calls, no shared conversation:

* **RATIFY** -- shown the proposal, asked whether it is correct.
* **ELICIT** -- shown only the flagged value and column context, asked for the correct value. The
  proposal is never shown. Accept is scored afterwards, by comparing the elicited value to the
  proposal.

What keeps this honest
----------------------
1. **The labeller is a model, and its verdicts cannot certify.** `label_source="llm_probe"` makes
   `certify_session` refuse. A model labelling a model's proposals has correlated errors with the
   corrector, and correlated errors understate `beta`.
2. **The capability control decides whether the result means anything.** If the ELICIT arm rarely
   reproduces *any* specific string, a low `beta_elicit` is low agreement rather than reduced
   acquiescence. `elicited_matches_truth` below 0.10 is a **VOID** condition, pre-registered.
3. **The two prompts differ only in the proposal-revealing sentence**, asserted programmatically
   rather than by eye, because that difference is the entire independent variable.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.bench.contamination import paired_signflip_p_value  # noqa: E402
from dataforge.bench.groq_client import AzureBenchClient  # noqa: E402
from dataforge.bench.stratified import wilson_interval  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402

#: Fixed in the pre-registration.
SEED = 0
PER_COLUMN_CAP = 12
RESAMPLES = 20_000
ALPHA = 0.01
CAPABILITY_FLOOR = 0.10

#: The one sentence that differs between arms. Everything else in the prompt is shared, so the
#: contrast isolates proposal-revelation rather than wording, length or task framing.
_RATIFY_REVEAL = (
    "A corrector has proposed replacing it with: {proposal}\n"
    "Is that proposed replacement the correct value? Answer YES or NO on the first line."
)
_ELICIT_REVEAL = (
    "No replacement has been proposed.\n"
    "What is the correct value? Answer with the value alone on the first line."
)

_SHARED_PROMPT = """You are reviewing one cell of a bibliographic database table.

Column: {column}
Other values from this same column, for context:
{context}

The value currently in the cell is flagged as an error: {flagged}

{reveal}"""


def _normalise(value: str) -> str:
    """Case-fold and whitespace-normalise for comparison. Fixed in the pre-registration."""
    return re.sub(r"\s+", " ", value.strip()).casefold()


@dataclass(frozen=True, slots=True)
class ControlItem:
    """A real error with retained truth and a wrong corrector proposal."""

    row: int
    column: str
    flagged: str
    proposal: str
    truth: str

    def __post_init__(self) -> None:
        if _normalise(self.proposal) == _normalise(self.truth):
            raise ValueError(
                f"proposal equals truth for row {self.row} column {self.column!r}; this is not a "
                "control. A correct proposal cannot measure a false-accept rate, and admitting "
                "one would deflate beta by padding the denominator."
            )


def build_prompt(item: ControlItem, *, arm: str, context: list[str]) -> str:
    """Build the prompt for one arm.

    Args:
        item: The control being labelled.
        arm: ``"ratify"`` or ``"elicit"``.
        context: Sample values from the same column, for capability parity across arms.

    Returns:
        The prompt.

    Raises:
        ValueError: On an unknown arm. Defaulting would silently mix the arms.
    """
    if arm == "ratify":
        reveal = _RATIFY_REVEAL.format(proposal=item.proposal)
    elif arm == "elicit":
        reveal = _ELICIT_REVEAL
    else:
        raise ValueError(f"unknown arm {arm!r}")
    return _SHARED_PROMPT.format(
        column=item.column,
        context="\n".join(f"  - {value}" for value in context),
        flagged=item.flagged,
        reveal=reveal,
    )


def prompts_differ_only_in_reveal(item: ControlItem, context: list[str]) -> bool:
    """Whether the two arms' prompts are identical outside the reveal block.

    The independent variable is proposal-revelation. If the prompts differ anywhere else the
    contrast measures wording too, so this is asserted rather than trusted.
    """
    ratify = build_prompt(item, arm="ratify", context=context)
    elicit = build_prompt(item, arm="elicit", context=context)
    strip = (
        _RATIFY_REVEAL.format(proposal=item.proposal),
        _ELICIT_REVEAL,
    )
    return ratify.replace(strip[0], "") == elicit.replace(strip[1], "")


def _sample_controls(dataset: object, *, cap: int, seed: int) -> list[tuple[int, str, str, str]]:
    """Stratified sample of real errors, capped per column.

    76% of rayyan's errors sit in one date column. Sampling proportionally would make this a
    measurement about date corrections; the cap spreads it across error types. Pairing controls
    for composition regardless, since both arms see identical items.
    """
    by_column: dict[str, list[tuple[int, str, str, str]]] = {}
    for cell in dataset.ground_truth:  # type: ignore[attr-defined]
        by_column.setdefault(cell.column, []).append(
            (cell.row, cell.column, cell.dirty_value, cell.clean_value)
        )
    rng = random.Random(seed)
    sampled: list[tuple[int, str, str, str]] = []
    for column in sorted(by_column):
        pool = sorted(by_column[column])
        rng.shuffle(pool)
        sampled.extend(pool[:cap])
    sampled.sort()
    return sampled


def _column_context(dataset: object, column: str, *, exclude: str, limit: int = 8) -> list[str]:
    """Clean values from the same column, so both arms have identical capability support."""
    seen: list[str] = []
    frame = dataset.clean_df  # type: ignore[attr-defined]
    if column not in frame.columns:
        return seen
    for value in frame[column].astype(str).tolist():
        if value and value != exclude and value not in seen:
            seen.append(value)
        if len(seen) >= limit:
            break
    return seen


def _first_line(text: str) -> str:
    for line in text.splitlines():
        if line.strip():
            return line.strip()
    return ""


def main() -> int:
    # Credentials are loaded HERE, not at import. A module-level ``load_dotenv`` reads the real
    # Azure key into ``os.environ`` for the whole process, and this module is imported by
    # ``tests/unit/test_blind_elicitation_probe.py`` -- which leaked the key across the pytest
    # session and broke ``test_hosted_without_key_fails_clearly``, a test whose entire point is
    # that no key is present. Importing a module must not read secrets.
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="rayyan")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--max-usd", type=float, default=4.0)
    parser.add_argument("--cap", type=int, default=PER_COLUMN_CAP)
    parser.add_argument("--calibrate", action="store_true", help="build items, make no calls")
    args = parser.parse_args()

    dataset = load_real_world_dataset(args.corpus)
    sampled = _sample_controls(dataset, cap=args.cap, seed=SEED)
    print(
        f"sampled {len(sampled)} real errors across "
        f"{len({c for _, c, _, _ in sampled})} columns (cap {args.cap}, seed {SEED})"
    )
    for column in sorted({c for _, c, _, _ in sampled}):
        print(f"  {column:<34} {sum(1 for _, c, _, _ in sampled if c == column)}")

    if args.calibrate:
        print("\ncalibrate: no calls made. Corrector proposals are harvested in the live run.")
        return 0

    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
    api_key = os.environ.get("AZURE_API_KEY")
    deployment = os.environ.get("DATAFORGE_AZURE_MODEL", "gpt-5.6-sol")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview")
    if not endpoint or not api_key:
        raise SystemExit("AZURE_OPENAI_ENDPOINT and AZURE_API_KEY must be set")

    client = AzureBenchClient(
        api_key=api_key,
        model=deployment,
        endpoint=endpoint,
        api_version=api_version,
        max_usd=args.max_usd,
        reasoning_effort="none",
        max_tokens=512,
        usd_per_1k_input=0.00125,
        usd_per_1k_output=0.01,
    )

    def ask(prompt: str) -> str:
        """One completion, first non-empty line.

        A failed call returns empty and is recorded as no reply. It is never scored as a verdict,
        because a transport error is not evidence about the labeller.
        """
        try:
            return _first_line(client.complete([{"role": "user", "content": prompt}]).text)
        except Exception as exc:  # noqa: BLE001 - a failed call must not become a verdict
            print(f"  call failed ({type(exc).__name__}); recorded as no reply")
            return ""

    # Phase 1: harvest wrong corrector proposals. The corrector is asked the same question the
    # ELICIT arm will be asked, at the same effort, so the proposals are distributionally the
    # corrector's own mistakes rather than synthesised plants.
    items: list[ControlItem] = []
    correct_proposals = 0
    abstentions = 0
    for row, column, flagged, truth in sampled:
        context = _column_context(dataset, column, exclude=flagged)
        prompt = build_prompt(
            ControlItem(row=row, column=column, flagged=flagged, proposal="?", truth=truth),
            arm="elicit",
            context=context,
        )
        reply = ask(prompt)
        if not reply:
            abstentions += 1
            continue
        if _normalise(reply) == _normalise(truth):
            correct_proposals += 1
            continue
        items.append(
            ControlItem(row=row, column=column, flagged=flagged, proposal=reply, truth=truth)
        )
        if client.meter.cumulative_usd > args.max_usd:
            raise SystemExit(f"budget ceiling ${args.max_usd} reached during harvest")

    print(
        f"\nharvest: {len(items)} wrong proposals, {correct_proposals} correct, "
        f"{abstentions} empty (spend ${client.meter.cumulative_usd:.4f})"
    )
    if not items:
        raise SystemExit("no wrong proposals harvested; nothing to label")

    # Phase 2: the two arms over identical items.
    ratify_accepts: list[int] = []
    elicit_accepts: list[int] = []
    elicit_matches_truth = 0
    elicit_matches_neither = 0
    for item in items:
        context = _column_context(dataset, item.column, exclude=item.flagged)
        if not prompts_differ_only_in_reveal(item, context):
            raise SystemExit(
                "arm prompts differ outside the reveal block; the contrast would not isolate "
                "proposal-revelation"
            )
        ratify_reply = ask(build_prompt(item, arm="ratify", context=context))
        ratify_accepts.append(1 if ratify_reply.upper().startswith("YES") else 0)

        elicit_reply = ask(build_prompt(item, arm="elicit", context=context))
        if _normalise(elicit_reply) == _normalise(item.proposal):
            elicit_accepts.append(1)
        else:
            elicit_accepts.append(0)
            if _normalise(elicit_reply) == _normalise(item.truth):
                elicit_matches_truth += 1
            else:
                elicit_matches_neither += 1
        if client.meter.cumulative_usd > args.max_usd:
            raise SystemExit(f"budget ceiling ${args.max_usd} reached during labelling")

    n = len(items)
    beta_ratify = sum(ratify_accepts) / n
    beta_elicit = sum(elicit_accepts) / n
    capability = elicit_matches_truth / n
    deltas = [e - r for e, r in zip(elicit_accepts, ratify_accepts, strict=True)]
    p_value = (
        paired_signflip_p_value(deltas, resamples=RESAMPLES, seed=SEED) if any(deltas) else None
    )
    void = capability < CAPABILITY_FLOOR

    payload = {
        "corpus": args.corpus,
        "labeller": "gpt-5.6-sol",
        "label_source": "llm_probe",
        "may_certify": False,
        "n_controls": n,
        "harvest": {
            "sampled": len(sampled),
            "wrong_proposals": n,
            "correct_proposals": correct_proposals,
            "empty_replies": abstentions,
        },
        "beta_ratify": round(beta_ratify, 4),
        "beta_ratify_wilson": [round(x, 4) for x in wilson_interval(sum(ratify_accepts), n)],
        "beta_elicit": round(beta_elicit, 4),
        "beta_elicit_wilson": [round(x, 4) for x in wilson_interval(sum(elicit_accepts), n)],
        "elicited_matches_truth": round(capability, 4),
        "elicited_matches_neither": round(elicit_matches_neither / n, 4),
        "paired_p_value": p_value,
        "alpha": ALPHA,
        "capability_floor": CAPABILITY_FLOOR,
        "void": void,
        "p1_blind_elicitation_reduces_beta": (not void) and beta_elicit < beta_ratify,
        "kill_criterion_fires": (not void) and beta_elicit >= beta_ratify,
        "usd": round(client.meter.cumulative_usd, 4),
        "calls": client.meter.calls,
    }
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(f"\nn controls            {n}")
    print(f"beta_ratify           {beta_ratify:.4f}")
    print(f"beta_elicit           {beta_elicit:.4f}")
    print(f"elicited==truth       {capability:.4f}  (floor {CAPABILITY_FLOOR}, VOID below)")
    print(f"elicited==neither     {elicit_matches_neither / n:.4f}")
    print(f"paired p              {p_value}")
    print(f"VOID                  {void}")
    print(f"P1 (elicit < ratify)  {payload['p1_blind_elicitation_reduces_beta']}")
    print(f"kill criterion fires  {payload['kill_criterion_fires']}")
    print(
        f"spend                 ${client.meter.cumulative_usd:.4f} over {client.meter.calls} calls"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
