# Branch Reconciliation Ledger

Purpose: converge the repository's diverged branches into one coherent trunk
(`main`) with a documented, verified path, per the `dataforge-definitive-standard`
plan (Task 2). This ledger is the record of what lands, what is already merged, and
what is intentionally archived.

Generated 2026-07-18. Regenerate the topology facts with:

```
git merge-base main codex/go-live-agent-config
git log --oneline main..codex/go-live-agent-config
git log --oneline codex/go-live-agent-config..main
git merge-tree --write-tree --name-only main codex/go-live-agent-config
```

## Topology (verified)

Merge-base of `main` and `codex/go-live-agent-config`: `236df75`
("Remove beers dataset; adopt capability-based dataset selection rule").

| Branch | Tip | Ahead of main | Behind main | Disposition |
| --- | --- | --- | --- | --- |
| `main` | `c1f1cdc` | - | - | Target trunk |
| `codex/go-live-agent-config` | `29f36ba` | 5 | 4 | **Definitive superset - merge into main** |
| `codex/bedrock-corrector-benchmark` | `67a1e0e` | 0 | 4 | Already in main - archive |
| `codex/playground-production-elevation` | `9b72e24` | 0 | 4 | Already in main - archive |
| `codex/dependabot-vuln-remediation` | `b166a28` | 0 | 4 | Already in main - archive |
| `origin/v0/product-redesign-6c3bd624` | (remote) | 4 | 58 | Dead June-02 scaffolding spike - archive, do not merge |

Key facts:

- The three `codex/*` feature branches other than `go-live-agent-config` are
  **fully contained in `main`** (`git branch --contains <tip> main` is true; ahead
  count 0). They carry no unmerged product work; they only lag main by the four
  later merge commits. Nothing to converge - they are archivable.
- `main`'s four commits that `go-live-agent-config` lacks are all **PR merge
  commits (#23-#26) of earlier `go-live-agent-config` states**. They introduce no
  file content that did not originate on `go-live-agent-config`.
- `codex/go-live-agent-config` is therefore a **content superset** of `main`: its
  five unmerged commits are the only real divergence.
- `origin/v0/product-redesign-6c3bd624` is an abandoned early redesign spike (last
  commit 2026-06-02, 58 commits behind main). It is not part of the definitive
  product and must not be merged.

## The five unmerged commits (the definitive trust-core work)

These are on `codex/go-live-agent-config` and not yet on `main`:

1. `7c80b0c` Calibrated, drift-guarded auto-apply moat + honest-frontier fixing work
2. `c2be12b` Re-triage expired torch CVE-2025-3000 pip-audit exception
3. `a215fb2` Fix pip-audit gate failure: eliminate vulnerable click 8.2.1
4. `db0ae83` Truth-in-numbers: requalify calibration claims, self-document the disabled auto-apply gate
5. `29f36ba` Make calibration artifacts fully reproducible from committed inputs

They add (verified additive, no deletions of product logic): `dataforge/
calibration_map.py`, `dataforge/conformal.py` extensions, `dataforge/detectors/
date_transposition.py`, corrector auto-apply wiring in `dataforge/engine/repair.py`
+ `dataforge/cli/repair.py` + `dataforge/calibration.py`, the reproducible
`eval/results/corrector_cache/*` response cache, calibration artifacts, and their
tests (`test_calibration_map*.py`, `test_corrector_autoapply_wiring.py`,
`test_date_transposition.py`), plus doc/DECISIONS updates.

## Merge safety (verified)

`git merge-tree --write-tree --name-only main codex/go-live-agent-config` returns a
single resulting tree OID with **no conflict output** -> the merge of
`go-live-agent-config` into `main` is **conflict-free**. The diff is 145 files,
almost entirely additive (`+2679 / -685`, where most deletions are the
`agent_comparison.json` benchmark artifact being regenerated).

## Convergence action

1. Commit the in-flight Task 1 constitution work (`PRODUCT.md` + cross-links +
   DECISIONS entry) to `codex/go-live-agent-config`.
2. Merge `codex/go-live-agent-config` into `main` (clean, `--no-ff` to preserve the
   trust-core provenance).
3. Re-green the gates on `main` (plan Task 3).
4. Archive the three merged `codex/*` branches and the dead `v0/product-redesign`
   spike (delete local, leave remote deletion to the maintainer).
5. Do not push until the maintainer approves (git-safety: no unrequested pushes).

After convergence, `main` is the single coherent trunk; `PRODUCT.md` is its
constitution; all subsequent plan tasks proceed on `main`.
