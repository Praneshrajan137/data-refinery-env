> **Note:** This is reference skeleton material for the future public README.
> The current shipped README is [README.md](README.md). Do not promote
> aspirational claims from this file into README.md until the corresponding
> features ship.

# DataForge

> The AI agent that keeps your data honest.
>
> Detect, diagnose, and safely repair data-quality bugs in CSVs, dbt models,
> and warehouse tables — with SMT-verified fixes and reversible transactions.

<!-- demo.gif will be added once the CLI is functional -->


```bash
pip install dataforge
dataforge profile path/to/your.csv
```

[Docs](TBD) · [Playground](TBD) · [GitHub](TBD) · [Discord](TBD)

## What it finds

- **Type mismatches** — numeric columns storing strings, dates as numbers.
- **Decimal shifts** — `0.75` discount written as `75` (10× too large).
- **Broken functional dependencies** — zip `94103` mapped to `Boston` instead of `San Francisco`.
- **Cascading errors** — a wrong `discount_pct` producing wrong `order_total` and wrong `tax`.
- **PII leakage** — phone numbers in the `notes` column, emails in `customer_id`.
- **14 other real-world bug classes** — see [docs/detectors.md](docs/detectors.md).

## How it repairs safely

Every proposed fix flows through three gates:

```
Agent proposes fix
│
▼
┌──────────────────┐       DENY      ┌──────────────────────┐
│  Safety Filter   │ ──────────────► │  Refuse + log        │
└──────────────────┘                 └──────────────────────┘
│ ALLOW
▼
┌──────────────────┐     REJECT      ┌──────────────────────┐
│   SMT Verifier   │ ──────────────► │  Return unsat core   │
└──────────────────┘                 │  as feedback to agent │
│ ACCEPT                      └──────────────────────┘
▼
┌──────────────────┐
│ Transaction log  │ ─── applied ───► Data modified on disk
│  (reversible)    │
└──────────────────┘
```

One command rolls back any repair: `dataforge revert <txn-id>`.

## Benchmark results

Scores are F1 on cell-level diff, averaged over 3 seeds. Reproduce with
`dataforge bench --methods all --datasets hospital,flights,beers`.

| Method                          | Hospital | Flights | Beers  | Avg    |
| ------------------------------- | -------- | ------- | ------ | ------ |
| Random                          | TBD      | TBD     | TBD    | TBD    |
| Heuristic (regex + FD)          | TBD      | TBD     | TBD    | TBD    |
| HoloClean (reference)           | TBD      | TBD     | TBD    | TBD    |
| Raha + Baran (reference)        | TBD      | TBD     | TBD    | TBD    |
| Cocoon (Claude 3.5 Sonnet, 2024)| TBD      | TBD     | TBD    | TBD    |
| DataForge (Claude 4.7, ReAct)   | TBD      | TBD     | TBD    | TBD    |
| DataForge-0.5B (ours, GRPO)     | TBD      | TBD     | TBD    | TBD    |
| DataForge-1.5B (ours, GRPO+GiGPO) | TBD    | TBD     | TBD    | TBD    |

_TBD cells are filled as the benchmark scripts land. Numbers are never hand-typed._

## Quickstart

```bash
# install
pip install dataforge

# profile a CSV and see detected issues
dataforge profile data.csv

# dry-run repairs (shows a diff, changes nothing)
dataforge repair data.csv --dry-run

# apply repairs, logging a reversible transaction
dataforge repair data.csv --apply

# roll back if needed
dataforge revert txn-2026-04-20-a8f2
```

## When DataForge is the wrong tool

Do not use DataForge if your data is:

- **(a) Streaming / unbounded** — DataForge is batch-oriented.
- **(b) > 100 million rows** — the SMT verifier has linear-in-schema cost but not sublinear-in-data cost.
- **(c) In a regulated environment where every fix must be human-authored** (healthcare billing, SOX, EU AI Act high-risk) — DataForge proposes fixes an agent generated, which is precisely what your compliance officer needs to review and likely reject.
- **(d) Under a strict SLA where a 30-second profile is already too slow.**
- **(e) Already well-served by Great Expectations suites your team has maintained for years** — adding DataForge is a solution in search of a problem.

Naming these cases up front earns trust for everything else.

## What DataForge is NOT

- Not a data catalog. Use DataHub, Amundsen, OpenMetadata.
- Not lineage. Use OpenLineage, Marquez, dbt's DAG.
- Not a full observability platform. Use Monte Carlo, Elementary, or Soda.
- Not a warehouse. DuckDB is the query engine; you bring the warehouse.
- Not a replacement for your CI — DataForge runs in your CI, against your dbt models.

## Integrations

- **dbt** (planned) — `pip install dataforge-dbt`, then add to your `dbt_project.yml`.
- **Airbyte** (planned) — DataForge source connector on the Airbyte Cloud marketplace.
- **Databricks** (planned) — marketplace notebook + native function.
- **MCP** (planned) — `pip install dataforge-mcp && dataforge-mcp serve` exposes
  DataForge as an MCP server usable from Claude, Cursor, Windsurf.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system diagram,
and [DECISIONS.md](DECISIONS.md) for the technical-decision log
(alternatives considered, reasoning, reversal criteria).

## Research

DataForge is also an OpenEnv-compatible RL environment. Training recipes,
model cards, and reproduction scripts live in [training/](training/).

## License

Apache 2.0. See [LICENSE](LICENSE).
