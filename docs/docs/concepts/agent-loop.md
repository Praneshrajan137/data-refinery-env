# Agent Loop

DataForge separates environment actions from repair execution. The agent loop
can inspect rows, query the in-memory dataset, run statistics, record
hypotheses, diagnose issues, propose fixes, and ask for root-cause analysis.

```mermaid
stateDiagram-v2
    [*] --> Observe
    Observe --> Inspect: INSPECT_ROWS or SQL_QUERY
    Inspect --> Diagnose: DIAGNOSE
    Diagnose --> Fix: FIX
    Fix --> Verify: safety + SMT
    Verify --> Observe: accepted or rejected
    Observe --> RootCause: ROOT_CAUSE
    RootCause --> Observe
    Verify --> [*]: step budget exhausted
```

The loop is intentionally local and auditable. It does not require an LLM, and
the OpenEnv surface can be exercised entirely with deterministic actions.

## Design rules

- Tool actions are typed before dispatch.
- SQL actions are read-only and capped.
- Fix actions must pass the same safety and verification checks used by the CLI.
- Terminal reward is based on detection, fix quality, false positives, and step
  discipline.
