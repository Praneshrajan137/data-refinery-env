"""Repoint the local Azure config at a chosen resource + deployment, safely.

Rewrites only the keys it owns and never prints the file, so the API key is not echoed into
a transcript.

**Why this file is now parameterised rather than hardcoded.** It previously pinned
``MODEL = "gpt-5-mini"`` together with gpt-5-mini's token prices, and it was the only code
that wrote either value into ``.env``. That coupling was invisible and financially dangerous:
because ``price_for`` was provider-keyed, pointing ``DATAFORGE_AZURE_MODEL`` at a frontier
deployment while these mini prices stayed behind would have metered a 46x-more-expensive
model at the cheap rate, so a ``DATAFORGE_AZURE_MAX_USD=15`` cap would have authorised
hundreds of dollars of real spend before tripping.

The fix is twofold: :func:`dataforge.spend.price_for` is now model-aware, and this script
refuses to write a model without a price that matches it. Model, resource and price are
chosen together or not at all.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENV = ROOT / ".env"

RESOURCE_GROUP = "rg-praneshrajan15-3033"
API_VERSION = "2025-04-01-preview"


def _fetch_key(resource: str, resource_group: str) -> str:
    """Read key1 from the Azure CLI. Never printed."""
    result = subprocess.run(
        [
            "az",
            "cognitiveservices",
            "account",
            "keys",
            "list",
            "--name",
            resource,
            "--resource-group",
            resource_group,
            "--query",
            "key1",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        shell=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"az failed: {result.stderr.strip()[:200]}")
    key = result.stdout.strip()
    if len(key) < 20:
        raise RuntimeError("az returned an implausibly short key")
    return key


def _verify_deployment(resource: str, resource_group: str, model: str) -> None:
    """Refuse to write a deployment that does not exist on the target resource.

    Without this the failure mode is a 404 at the first paid call, after the config has
    already been changed. The endpoint and the deployment must agree, and only Azure can
    say whether they do.
    """
    result = subprocess.run(
        [
            "az",
            "cognitiveservices",
            "account",
            "deployment",
            "list",
            "--name",
            resource,
            "--resource-group",
            resource_group,
            "--query",
            "[].name",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        shell=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"az deployment list failed: {result.stderr.strip()[:200]}")
    names = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    if model not in names:
        raise RuntimeError(
            f"deployment {model!r} does not exist on {resource!r} "
            f"(found: {sorted(names) or 'none'}); refusing to write a config that would 404"
        )


def main(argv: list[str] | None = None) -> int:
    """Rewrite the owned keys in .env in place."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource", required=True, help="Azure Cognitive Services account name.")
    parser.add_argument("--model", required=True, help="Deployment name, e.g. gpt-5.6-sol.")
    parser.add_argument("--resource-group", default=RESOURCE_GROUP)
    parser.add_argument("--api-version", default=API_VERSION)
    parser.add_argument(
        "--max-usd",
        default=None,
        help="Write DATAFORGE_AZURE_MAX_USD. Set this deliberately for a paid run.",
    )
    parser.add_argument(
        "--skip-deployment-check",
        action="store_true",
        help="Skip verifying the deployment exists (not recommended).",
    )
    args = parser.parse_args(argv)

    # Fail closed on price BEFORE touching anything. A model we cannot price is a model we
    # cannot cap, and an uncapped frontier deployment is the whole hazard this guards.
    from dataforge.spend import require_price_for

    price = require_price_for("azure", args.model)

    if not args.skip_deployment_check:
        _verify_deployment(args.resource, args.resource_group, args.model)

    endpoint = f"https://{args.resource}.cognitiveservices.azure.com"
    updates = {
        "AZURE_API_KEY": _fetch_key(args.resource, args.resource_group),
        "AZURE_OPENAI_ENDPOINT": endpoint,
        "AZURE_OPENAI_API_VERSION": args.api_version,
        "DATAFORGE_AZURE_MODEL": args.model,
        # Written from the per-model table, so the prices in .env can never contradict the
        # model in .env. This is the specific inversion that made the old script unsafe.
        "DATAFORGE_AZURE_USD_PER_1K_INPUT": f"{price.usd_per_1k_input:g}",
        "DATAFORGE_AZURE_USD_PER_1K_OUTPUT": f"{price.usd_per_1k_output:g}",
    }
    if args.max_usd is not None:
        updates["DATAFORGE_AZURE_MAX_USD"] = str(args.max_usd)

    text = ENV.read_text(encoding="utf-8") if ENV.exists() else ""
    for name, value in updates.items():
        line = f"{name}={value}"
        pattern = re.compile(rf"^{re.escape(name)}=.*$", re.MULTILINE)
        if pattern.search(text):
            text = pattern.sub(line, text)
        else:
            text = text.rstrip("\n") + "\n" + line + "\n"
    ENV.write_text(text, encoding="utf-8")

    print(f"Repointed {ENV.name} at {args.resource}:")
    for name, value in updates.items():
        shown = f"<set, {len(value)} chars>" if "KEY" in name else value
        print(f"  {name} = {shown}")
    print(
        f"\nPrices written from dataforge.spend.MODEL_PRICES for {args.model!r}, so the cap "
        "and the ledger agree with the deployment. Measurements are NOT comparable across "
        "deployments."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
