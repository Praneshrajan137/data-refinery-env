# Quickstart

This walkthrough takes about five minutes.

## 1. Install

For the released CLI/library package:

```bash
python -m pip install dataforge_07
```

For development from this source checkout:

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

The installed import namespace and CLI command remain `dataforge`.

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

A repair has to be earned by a premise that constrains the column being written. Hospital's
schema declares mostly `str` columns, and a `str` declaration constrains nothing, so it
proves nothing and this command reports zero fixes:

```bash
dataforge repair fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --dry-run
```

Use the premised fixture to see a repair actually proven. `state -> city` is a declared
functional dependency, so `fd_violation` can derive the fix:

```bash
dataforge repair fixtures/premised_fd_10rows.csv \
  --schema fixtures/premised_fd_10rows.schema.yaml --dry-run
```

Dry-run mode exercises detection, repair proposal, safety, and verification without writing
to disk. Comparing the two commands is the fastest way to see the product's central rule:
no discriminating premise, no write.

## 4. Watch once for CI

```bash
dataforge watch fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --once --json
```

Without `--once`, watch polls the path and reruns `profile` or dry-run repair
when the file changes. It does not mutate files unless `--action repair --apply`
is passed explicitly.

## 5. Apply and revert on a copy

This step uses the premised fixture, because a file with nothing provable produces no
transaction and the `audit` and `revert` steps below would have no id to consume.

```bash
cp fixtures/premised_fd_10rows.csv /tmp/readings.csv
dataforge repair /tmp/readings.csv \
  --schema fixtures/premised_fd_10rows.schema.yaml --apply
# The apply output prints the transaction id. The journal is written beside the DATA,
# so audit and revert need --search-root when you are not in that directory.
dataforge audit <txn-id> --search-root /tmp
dataforge revert <txn-id> --search-root /tmp
```

Applied repairs write a transaction journal and source snapshot before the CSV is mutated.
Audit verifies the local hash chain for newly written logs. Revert restores the original
bytes when the current file still matches the recorded post-state hash.

This whole sequence is executed as a test, including the id handoff and a byte-identity
assertion, in `tests/integration/test_persona_acceptance.py`. It is run rather than trusted
because an earlier version of this page documented an `apply` that produced no transaction
id, making the next two commands impossible.

## 6. Regenerate benchmark docs

```bash
python scripts/bench/refresh_benchmark_truth.py --seed-list 0,1,2
```

The README benchmark block, docs homepage block, and `BENCHMARK_REPORT.md` are
generated from committed JSON evidence. Public benchmark numbers should not be
edited by hand.
