"""Diagnose why a given Azure OpenAI model cannot be deployed, and what can be.

Written after losing `gpt-5.6-sol` mid-campaign and spending an investigation rediscovering
why. The failure was **not** a bad model name, a wrong region, or a stale key: the model is
genuinely offered by the resource, but the subscription's quota for it is zero. Distinguishing
those four causes takes several CLI calls in a specific order, so this encodes the order.

What it checks, cheapest and most decisive first:

1. the subscription tier -- a ``FreeTrial_*`` ``quotaId`` gets **zero quota for every premium
   model tier**, which no amount of region-hopping or CLI work can fix;
2. whether the model is offered by the resource at all (vs a wrong name);
3. whether an existing deployment already serves it (a fresh resource has none, which is the
   most common "it stopped working" cause and *is* fixable);
4. quota for that model across SKUs in the resource's region;
5. which chat models **do** have usable quota, so there is an actionable alternative.

Read-only: it creates nothing and spends nothing.

Usage::

    python scripts/bench/diagnose_azure_model.py                      # defaults below
    python scripts/bench/diagnose_azure_model.py --model gpt-5.6-sol
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any

_DEFAULT_RESOURCE = "praneshrajan15-9819-resource"
_DEFAULT_GROUP = "rg-praneshrajan15-3033"
_DEFAULT_MODEL = "gpt-5.6-sol"


def _az(args: list[str]) -> Any:
    """Run an az command and parse its JSON, returning None on failure."""
    result = subprocess.run(
        ["az", *args, "--output", "json"], capture_output=True, text=True, shell=True
    )
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def main() -> int:
    """Print an ordered diagnosis for one model on one resource."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource", default=_DEFAULT_RESOURCE)
    parser.add_argument("--resource-group", default=_DEFAULT_GROUP)
    parser.add_argument("--model", default=_DEFAULT_MODEL)
    args = parser.parse_args()

    account = _az(["account", "show"])
    if account is None:
        print("Not logged in. Run: az login", file=sys.stderr)
        return 2
    subscription = account["id"]
    print(f"subscription : {account['name']} ({account.get('user', {}).get('name')})")

    # 1. Tier first: it explains a zero-quota result better than anything downstream.
    policy = _az(
        [
            "rest",
            "--method",
            "get",
            "--url",
            f"https://management.azure.com/subscriptions/{subscription}?api-version=2020-01-01",
        ]
    )
    quota_id = (policy or {}).get("subscriptionPolicies", {}).get("quotaId", "?")
    print(f"quotaId      : {quota_id}")
    free_trial = quota_id.lower().startswith("freetrial")
    if free_trial:
        print(
            "  -> FREE TRIAL. Premium model tiers get ZERO quota on this tier; only "
            "mini/nano do. No CLI action changes that -- it needs a billing conversion."
        )

    show = _az(
        [
            "cognitiveservices",
            "account",
            "show",
            "--name",
            args.resource,
            "--resource-group",
            args.resource_group,
        ]
    )
    region = (show or {}).get("location", "?")
    print(f"resource     : {args.resource} ({region})")

    # 2. Is the model even offered here? Distinguishes "bad name" from "no quota".
    models = (
        _az(
            [
                "cognitiveservices",
                "account",
                "list-models",
                "--name",
                args.resource,
                "--resource-group",
                args.resource_group,
            ]
        )
        or []
    )
    offered = [m for m in models if (m.get("name") or "") == args.model]
    if not offered:
        near = sorted(
            {m.get("name", "") for m in models if args.model[:7] in (m.get("name") or "")}
        )
        print(f"\n{args.model}: NOT OFFERED by this resource. Closest names: {near or 'none'}")
        return 1
    versions = sorted({m.get("version", "?") for m in offered})
    skus = sorted({s.get("name", "?") for m in offered for s in (m.get("skus") or [])})
    print(f"\n{args.model}: OFFERED (versions {versions}, skus {skus})")

    # 3. Already deployed? A fresh resource has no deployments -- the fixable case.
    deployments = (
        _az(
            [
                "cognitiveservices",
                "account",
                "deployment",
                "list",
                "--name",
                args.resource,
                "--resource-group",
                args.resource_group,
            ]
        )
        or []
    )
    serving = [
        d["name"]
        for d in deployments
        if ((d.get("properties") or {}).get("model") or {}).get("name") == args.model
    ]
    print(f"deployments serving it: {serving or 'NONE'}")
    if not serving:
        print("  -> if quota allows, this is fixable: create a deployment.")

    # 4. Quota in this region, across SKUs.
    usage = _az(["cognitiveservices", "usage", "list", "--location", region]) or []
    entries = [u for u in usage if args.model in (u.get("name", {}).get("value", "") or "")]
    print(f"\nquota for {args.model} in {region}:")
    if not entries:
        print("  no quota entries found")
    for entry in entries:
        limit = float(entry.get("limit") or 0)
        marker = "  <-- BLOCKER" if limit == 0 else ""
        print(f"  {entry['name']['value']:<52} {entry.get('currentValue')}/{limit}{marker}")
    blocked = entries and all(float(e.get("limit") or 0) == 0 for e in entries)

    # 5. Always give an actionable alternative.
    usable = sorted(
        (
            (float(u.get("limit") or 0), u["name"]["value"])
            for u in usage
            if float(u.get("limit") or 0) > 0
            and "embedding" not in u["name"]["value"].lower()
            and "finetune" not in u["name"]["value"].lower()
            and "AccountCount" not in u["name"]["value"]
        ),
        reverse=True,
    )
    print("\nmodels WITH usable quota here (best first):")
    for limit, name in usable[:8]:
        print(f"  {limit:8.0f}  {name}")

    if blocked:
        print(
            f"\nVERDICT: {args.model} is offered but has ZERO quota"
            + (" and this is a Free Trial subscription." if free_trial else ".")
            + "\nUnlocks: (a) convert the subscription off Free Trial and request quota, or"
            "\n         (b) use a subscription that already holds the quota."
            "\nQuota is per-region, but a Free Trial is zero in every region."
        )
        return 1
    print(f"\nVERDICT: {args.model} has quota. Create a deployment to use it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
