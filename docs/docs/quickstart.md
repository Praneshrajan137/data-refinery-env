# Quickstart

This walkthrough takes about five minutes from a fresh checkout.

## 1. Install

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

The PyPI package is not published yet. After PyPI publication, install the DataForge distribution with `python -m pip install dataforge_07`; the installed import namespace and CLI command remain `dataforge`.

## 2. Profile the hospital fixture

```bash
dataforge profile fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml
```

The command prints a Rich table of detected issues, including issue type,
severity, confidence, and reason.

For machine-readable CI or agent calls:

```bash
dataforge profile fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --json
dataforge profile fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --fail-on unsafe
dataforge profile fixtures/hospital_10rows.csv --constraints-out constraints.json
dataforge constraints review constraints.json
```

## 3. Preview repairs

```bash
dataforge repair fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --dry-run
```

Dry-run mode exercises detection, repair proposal, safety, and verification
without writing to disk.

## 4. Watch once for CI

```bash
dataforge watch fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --once --json
```

Without `--once`, watch polls the path and reruns `profile` or dry-run repair
when the file changes. It does not mutate files unless `--action repair --apply`
is passed explicitly.

## 5. Apply and revert on a copy

```bash
cp fixtures/hospital_10rows.csv /tmp/hospital_10rows.csv
dataforge repair /tmp/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --apply
dataforge audit <txn-id>
dataforge revert <txn-id>
```

Applied repairs write a transaction journal and source snapshot before the CSV
is mutated. Audit verifies the local hash chain for newly written logs. Revert
restores the original bytes when the current file still matches the recorded
post-state hash.

## 6. Regenerate benchmark docs

```bash
python scripts/bench/refresh_benchmark_truth.py --seed-list 0,1,2
```

The README benchmark block, docs homepage block, and `BENCHMARK_REPORT.md` are
generated from committed JSON evidence. Public benchmark numbers should not be
edited by hand.
