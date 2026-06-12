# Reference

## CLI

```bash
dataforge profile <csv> [--schema schema.yaml]
dataforge profile <csv> --constraints-out constraints.json
dataforge constraints review constraints.json
dataforge constraints review constraints.json --accept cnd-... --no-tui --json
dataforge repair <csv> [--schema schema.yaml] --dry-run
dataforge repair <csv> --constraints constraints.json --dry-run
dataforge repair <csv> [--schema schema.yaml] --apply
dataforge watch <csv> [--schema schema.yaml] --once --json
dataforge audit <txn-id>
dataforge revert <txn-id>
dataforge bench --methods heuristic --datasets hospital --seeds 1
```

## Public modules

| Module | Purpose |
| --- | --- |
| `dataforge.detectors` | Detector registry and detector implementations |
| `dataforge.repairers` | Deterministic repair proposal generation |
| `dataforge.safety` | Constitution-backed repair policy |
| `dataforge.verifier` | SMT-backed repair verification |
| `dataforge.transactions` | Hash-chained journals, snapshots, audit, and revert |
| `dataforge.env` | OpenEnv-compatible environment |
| `dataforge.causal` | Causal DAG and root-cause utilities |
| `dataforge.bench` | Benchmark runners, metrics, and reports |

## Version support

DataForge 0.1.0 supports Python 3.11 and 3.12. The core package is
intentionally separate from playground, training, and model-demo dependencies.
