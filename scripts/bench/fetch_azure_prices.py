"""One-off: fetch authoritative gpt-5-mini token prices from the Azure retail API.

Written as a file rather than an inline command because the query needs a literal ``$``
in the OData filter, which PowerShell here-strings mangle. Read-only, no credentials.
"""

from __future__ import annotations

import httpx

URL = "https://prices.azure.com/api/retail/prices"
FILTER = "serviceName eq 'Cognitive Services' and contains(meterName, 'gpt 5 mini')"


def main() -> int:
    """Print distinct gpt-5-mini meters and their retail prices."""
    response = httpx.get(
        URL,
        params={"api-version": "2023-01-01-preview", "$filter": FILTER},
        timeout=60,
    )
    items = response.json().get("Items", [])
    seen: set[tuple[str, str, float]] = set()
    for item in items:
        key = (item["meterName"], item["unitOfMeasure"], item["retailPrice"])
        if key in seen:
            continue
        seen.add(key)
        print(
            f"{item['meterName']:<52} {item['unitOfMeasure']:<16} "
            f"{item['retailPrice']} {item['currencyCode']}"
        )
    print(f"\nvariants returned: {len(items)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
