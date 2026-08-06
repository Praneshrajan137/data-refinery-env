"""One-off: repoint the local Azure config at the currently reachable resource.

Rewrites only the keys it owns and never prints the file, so the API key is not echoed into
a transcript. Run after `az login` when the Azure resource behind DATAFORGE has changed.

Why this exists as a script: the previous resource (`praneshrajan15-8087-resource`,
`*.openai.azure.com`, deployment `gpt-5.6-sol`) started returning HTTP 401 mid-session. The
reachable resource is now `praneshrajan15-9819-resource` (`*.cognitiveservices.azure.com`)
whose only chat deployment is `gpt-5-mini` -- a **different model**, so numbers produced
after this change are not comparable to earlier gpt-5.6-sol measurements.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENV = ROOT / ".env"

RESOURCE = "praneshrajan15-9819-resource"
RESOURCE_GROUP = "rg-praneshrajan15-3033"
ENDPOINT = f"https://{RESOURCE}.cognitiveservices.azure.com"
MODEL = "gpt-5-mini"
API_VERSION = "2025-04-01-preview"

# Published Azure OpenAI Global Standard rates for gpt-5-mini, in USD per 1K tokens.
# PROVENANCE, stated because the ledger depends on it: the Azure retail prices API returned
# no gpt-5-mini meters under serviceName 'Cognitive Services', so these are Microsoft's
# published list rates rather than an API-verified lookup. They are ~20x cheaper than the
# gpt-5.6-sol rates previously configured (0.005 / 0.015), so leaving the old values in place
# would have overstated spend by roughly that factor.
USD_PER_1K_INPUT = "0.00025"
USD_PER_1K_OUTPUT = "0.002"


def _fetch_key() -> str:
    """Read key1 from the Azure CLI. Never printed."""
    result = subprocess.run(
        [
            "az",
            "cognitiveservices",
            "account",
            "keys",
            "list",
            "--name",
            RESOURCE,
            "--resource-group",
            RESOURCE_GROUP,
            "--query",
            "key1",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        shell=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"az failed: {result.stderr.strip()[:200]}")
    key = result.stdout.strip()
    if len(key) < 20:
        raise RuntimeError("az returned an implausibly short key")
    return key


def main() -> int:
    """Rewrite the owned keys in .env in place."""
    updates = {
        "AZURE_API_KEY": _fetch_key(),
        "AZURE_OPENAI_ENDPOINT": ENDPOINT,
        "AZURE_OPENAI_API_VERSION": API_VERSION,
        "DATAFORGE_AZURE_MODEL": MODEL,
        "DATAFORGE_AZURE_USD_PER_1K_INPUT": USD_PER_1K_INPUT,
        "DATAFORGE_AZURE_USD_PER_1K_OUTPUT": USD_PER_1K_OUTPUT,
    }
    text = ENV.read_text(encoding="utf-8") if ENV.exists() else ""
    for name, value in updates.items():
        line = f"{name}={value}"
        pattern = re.compile(rf"^{re.escape(name)}=.*$", re.MULTILINE)
        if pattern.search(text):
            text = pattern.sub(line, text)
        else:
            text = text.rstrip("\n") + "\n" + line + "\n"
    ENV.write_text(text, encoding="utf-8")

    print(f"Repointed {ENV.name} at {RESOURCE}:")
    for name, value in updates.items():
        shown = f"<set, {len(value)} chars>" if "KEY" in name else value
        print(f"  {name} = {shown}")
    print(
        "\nNOTE: deployment is gpt-5-mini, NOT gpt-5.6-sol. Measurements taken after this "
        "change are not comparable to earlier gpt-5.6-sol numbers."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
