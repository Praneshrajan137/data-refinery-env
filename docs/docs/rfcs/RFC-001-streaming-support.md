# RFC-001: Streaming Support

## Problem

DataForge 0.1.0 works on local CSV snapshots. Some users will want to inspect
append-only event streams or warehouse extracts without waiting for a complete
file. Streaming support must preserve the same safety and reversibility
standards as batch repair.

## Alternatives

- Add streaming directly to the existing CLI commands.
- Add a separate `dataforge stream` command with windowed profiling and no
  mutation in v1.
- Defer streaming until a warehouse adapter exists.

## Decision

Draft a separate streaming interface before adding runtime code. The first
implementation should be read-only and windowed. It should emit detector
findings and benchmarkable summaries, not apply repairs to live streams.

## Rollout Plan

1. Define a `StreamWindow` data contract with source, offset, schema, rows, and
   watermark metadata.
2. Add detector-only streaming tests with deterministic fixtures.
3. Add CLI preview support after the contract is stable.
4. Revisit mutation only after transaction semantics are designed for streams.

## Open Questions

- What replay guarantees are required for Kafka, files, and warehouse streams?
- How should transaction journals map to unbounded sources?
- What latency target is realistic without weakening verification?
