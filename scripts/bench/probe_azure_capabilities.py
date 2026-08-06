"""Probe which Azure OpenAI request features the live deployment actually accepts.

Microsoft's own documentation is internally inconsistent about GPT-5-family
support: the reasoning-models page marks Structured Outputs as available for the
GPT-5 series, while the structured-outputs page's supported-model list is older
and does not mention every deployment. Two docs disagreeing means the only
honest way to know is to ask the deployment.

This is deliberately the cheapest possible experiment: one tiny call per feature
(a handful of tokens each), a hard USD cap, and a committed verdict artifact so
no later step has to guess or re-spend.

What each probe decides:

* ``structured_outputs_enum`` -- THE decision gate. If accepted, the candidate
  pool can become a hard decode-time constraint (an ``enum``) instead of a prompt
  request plus post-filter, and the corrector's dormant model-confidence field
  becomes real. If rejected, fall back to prompt-instructed JSON with a strict
  local validator.
* ``reasoning_effort_none`` -- a cost and latency lever, documented for gpt-5.6.
* ``temperature`` and ``logprobs`` -- expected *rejections*. They confirm two
  documented constraints that shape the design: the corrector's ``temperature``
  is a no-op on reasoning deployments, and logprob-based confidence (the obvious
  calibration lever) is unavailable, which is why the enum path is the one worth
  building.

Usage (foreground and bounded -- detached runs die at session boundaries)::

    python scripts/bench/probe_azure_capabilities.py --max-usd 1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from dataforge.spend import (  # noqa: E402
    ModelPrice,
    SpendMeter,
    append_receipt,
    price_for,
)

_ARTIFACT = ROOT / "eval" / "results" / "azure_capability_probe.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_azure_capability_probe_v1"

# Tiny prompt: we are testing parameter acceptance, not model quality.
_MESSAGES = [{"role": "user", "content": "Reply with the single word: ok"}]
_MAX_COMPLETION_TOKENS = 2048


def _enum_schema() -> dict[str, Any]:
    """Return a strict schema mirroring the constrained corrector's real shape.

    Honours every documented Structured Outputs constraint: ``strict: true``,
    ``additionalProperties: false``, all properties required, and no
    unsupported type-specific keywords (no ``minimum``/``maximum`` on the
    number -- confidence is clamped locally instead).
    """
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "cell_correction",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "value": {"type": "string", "enum": ["ATLANTA", "BIRMINGHAM", "NONE"]},
                    "confidence": {"type": "number"},
                },
                "required": ["value", "confidence"],
                "additionalProperties": False,
            },
        },
    }


def _probes() -> list[tuple[str, dict[str, Any], str]]:
    """Return (name, extra_payload, expectation) for each feature probe."""
    return [
        ("baseline", {}, "accept"),
        ("json_mode", {"response_format": {"type": "json_object"}}, "accept"),
        ("structured_outputs_enum", {"response_format": _enum_schema()}, "accept"),
        ("reasoning_effort_none", {"reasoning_effort": "none"}, "accept"),
        ("reasoning_effort_minimal", {"reasoning_effort": "minimal"}, "accept"),
        ("verbosity_low", {"verbosity": "low"}, "accept"),
        # Documented as unsupported on reasoning models. A rejection here is the
        # evidence that omitting temperature by default is correct, and that
        # the corrector's temperature=0.4 never took effect on Azure.
        ("temperature", {"temperature": 0.2}, "reject"),
        # Documented as unsupported on reasoning models. A rejection closes the
        # logprob-calibration lever for good.
        ("logprobs", {"logprobs": True, "top_logprobs": 3}, "reject"),
    ]


def _run_probe(
    client: httpx.Client,
    *,
    url: str,
    api_version: str,
    extra: dict[str, Any],
    meter: SpendMeter,
) -> dict[str, Any]:
    """Issue one probe call and return a structured verdict."""
    payload: dict[str, Any] = {
        "messages": _MESSAGES,
        "max_completion_tokens": _MAX_COMPLETION_TOKENS,
        **extra,
    }
    try:
        response = client.post(url, json=payload, params={"api-version": api_version})
    except httpx.TimeoutException as exc:
        return {"accepted": False, "error_kind": "timeout", "error": str(exc)}

    if response.status_code >= 400:
        body = response.text[:600].replace("\n", " ")
        return {
            "accepted": False,
            "error_kind": f"http_{response.status_code}",
            "error": body,
        }

    data = response.json()
    usage = data.get("usage") or {}
    prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    details = usage.get("completion_tokens_details") or {}
    reasoning_tokens = int(details.get("reasoning_tokens", 0) or 0)
    meter.record(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        reasoning_tokens=reasoning_tokens,
    )
    try:
        content = str(data["choices"][0]["message"]["content"])
    except (KeyError, IndexError, TypeError):
        content = ""
    return {
        "accepted": True,
        "content": content[:200],
        "content_is_empty": content.strip() == "",
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "reasoning_tokens": reasoning_tokens,
    }


def _enum_was_honoured(result: dict[str, Any]) -> bool | None:
    """Return whether a structured-output probe actually respected the enum."""
    if not result.get("accepted"):
        return None
    try:
        parsed = json.loads(str(result.get("content", "")))
    except json.JSONDecodeError:
        return False
    if not isinstance(parsed, dict):
        return False
    return parsed.get("value") in {"ATLANTA", "BIRMINGHAM", "NONE"}


def main() -> int:
    """Run the probe suite and commit the verdict artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-usd",
        type=float,
        default=1.0,
        help="Hard spend cap for this probe (default: 1.0).",
    )
    parser.add_argument(
        "--timeout-s", type=float, default=180.0, help="Per-request timeout in seconds."
    )
    args = parser.parse_args()

    load_dotenv()  # repo-root .env (Azure creds)

    api_key = os.environ.get("AZURE_API_KEY", "").strip()
    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "").strip().rstrip("/")
    deployment = os.environ.get("DATAFORGE_AZURE_MODEL", "").strip()
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview").strip()
    missing = [
        name
        for name, value in (
            ("AZURE_API_KEY", api_key),
            ("AZURE_OPENAI_ENDPOINT", endpoint),
            ("DATAFORGE_AZURE_MODEL", deployment),
        )
        if not value
    ]
    if missing:
        print(f"Cannot probe: {', '.join(missing)} not set. See .env.example.", file=sys.stderr)
        return 2

    price = price_for("azure") or ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015)
    meter = SpendMeter(provider="azure", model=deployment, price=price, max_usd=args.max_usd)
    url = f"{endpoint}/openai/deployments/{deployment}/chat/completions"

    results: dict[str, Any] = {}
    with httpx.Client(
        timeout=args.timeout_s,
        headers={"api-key": api_key, "Content-Type": "application/json"},
    ) as client:
        for name, extra, expectation in _probes():
            outcome = _run_probe(client, url=url, api_version=api_version, extra=extra, meter=meter)
            outcome["expectation"] = expectation
            outcome["matches_documentation"] = (
                outcome["accepted"] if expectation == "accept" else not outcome["accepted"]
            )
            if name == "structured_outputs_enum":
                outcome["enum_honoured"] = _enum_was_honoured(outcome)
            results[name] = outcome
            verdict = "ACCEPTED" if outcome["accepted"] else "REJECTED"
            flag = "" if outcome["matches_documentation"] else "  <-- DIVERGES FROM DOCS"
            print(f"{name:26s} {verdict:9s} (expected {expectation}){flag}")

    structured = results.get("structured_outputs_enum", {})
    decision = (
        "structured_enum"
        if structured.get("accepted") and structured.get("enum_honoured")
        else "prompt_json_fallback"
    )

    payload = {
        "schema": _SCHEMA,
        "provider": "azure",
        "model": deployment,
        "api_version": api_version,
        "probes": results,
        "decision": decision,
        "decision_note": (
            "structured_enum: the candidate pool becomes a hard decode-time enum and the "
            "model-confidence field becomes real. prompt_json_fallback: instruct JSON in the "
            "prompt and validate locally; pool membership stays a post-filter."
        ),
        "estimated_usd": round(meter.cumulative_usd, 6),
        "calls": meter.calls,
    }
    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    append_receipt(
        _LEDGER,
        meter.receipt(
            run_id=f"azure-capability-probe-{uuid.uuid4().hex[:8]}",
            method="capability_probe",
            notes=(f"decision={decision}",),
        ),
    )

    print(f"\nDecision: {decision}")
    print(f"Calls: {meter.calls}   Estimated spend: ${meter.cumulative_usd:.4f}")
    print(f"Artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
