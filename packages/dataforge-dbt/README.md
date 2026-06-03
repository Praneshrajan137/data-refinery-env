# dataforge-dbt

Run DataForge checks from a dbt model post-hook and keep the audit artifacts in your dbt `target/` directory.

```bash
python -m pip install -e ".[dev]"
```

## Quick start

### 1. Install the Python dependency

The PyPI package is not published yet. Install from this source checkout:

```bash
python -m pip install -e ".[dev]"
```

After PyPI publication, `pip install dataforge_07_dbt dbt-duckdb` will install
the integration and the free local integration-test warehouse adapter.

If you install only the runtime package from source, install `dbt-duckdb`
separately before running the DuckDB example.

### 2. Add the dbt package

In `packages.yml`:

```yaml
packages:
  - package: dataforge/dataforge
    version: 0.1.0
```

During local development from a checkout, use:

```yaml
packages:
  - local: ../dataforge-dbt
```

Then run:

```bash
dbt deps
```

### 3. Add a model post-hook

```sql
{{ config(post_hook="{{ dataforge.dataforge_repair('column_x', mode='dry_run') }}") }}

select *
from {{ ref('my_source_model') }}
```

Supported modes:

- **`dry_run`**: logs proposed DataForge findings as warnings and writes no transaction artifact.
- **`apply`**: writes a JSONL audit artifact under `target/dataforge_txns/`.
- **`refuse`**: fails the hook if DataForge detects any `UNSAFE` issue.

### 4. Run dbt through the dispatcher

```bash
dataforge-dbt \
  --relation main.my_model \
  --column column_x \
  --mode dry_run \
  --target-path target \
  --project-dir . \
  --run-dbt
```

The macro emits a stable log line showing the dispatcher command for the
relation and column. The Python dispatcher is the execution boundary: it can
run dbt, export the relation through the adapter, run DataForge detectors, and
write audit artifacts under `target/`. The macro itself remains SQL-only
because dbt post-hooks do not provide a portable, safe shell execution surface.

### 5. Verify transaction artifacts

For `mode='apply'`, verify that the target directory contains a DataForge transaction artifact:

```bash
ls target/dataforge_txns/
```

Each file is JSONL and includes the dbt relation, inspected column, UTC timestamp, and serialized DataForge issues.

## Python dispatch entrypoint

The package also ships a Python command used by tests and automation:

```bash
dataforge-dbt \
  --relation main.example_with_dirty_data \
  --column column_x \
  --mode dry_run \
  --input-csv integration_tests/dbt_project/seeds/dirty_decimal_shift.csv \
  --target-path target
```

This command reads the CSV with `dtype=str`, runs DataForge detectors, logs any findings with the prefix `DATAFORGE_DBT`, and enforces the selected mode.

## Configuration block

You may add a conservative integration block to `profiles.yml`:

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./dev.duckdb
  dataforge:
    mode: dry_run
    target_path: target
```

Macro arguments remain the source of truth for the common case. The profile
block is useful for central defaults in CI; an explicit hook or dispatcher
`--mode` always overrides `dataforge.mode` from `profiles.yml`.

## Free-tier test setup

The integration tests use DuckDB only. No Snowflake, BigQuery, or paid warehouse account is required.

```bash
python -m pip install -e ".[dev]"
python -m pytest
```

The test project includes a seed CSV with a known decimal-shift anomaly and asserts that DataForge logs the issue.

## Publishing

The package is not published yet. The intended release path is PyPI Trusted
Publishing after repository ownership is configured; do not add PyPI API tokens
to GitHub Secrets.

## When this is the wrong tool

Use dbt tests or warehouse constraints first for deterministic rules such as `not_null`, `unique`, accepted values, and referential integrity. Use `dataforge-dbt` when you want DataForge's anomaly detectors and repair audit trail around messy values that are hard to express as static SQL tests.
