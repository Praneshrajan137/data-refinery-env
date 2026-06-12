# Transactions

Applied repairs are reversible because DataForge writes transaction evidence
before it mutates the source CSV.

## Journal contents

- Transaction identifier.
- Source path and immutable pre-mutation snapshot.
- Planned cell fixes.
- Post-state hash guard.
- Applied and reverted events.
- A local hash chain for newly written v2 events: each event records its index,
  the previous event hash, and its own SHA-256 over canonical JSON.

## Evidence schemas

New JSONL events use `schema_name: transaction_journal_v2` and
`schema_version: 2`. Legacy logs use `schema_version: 1` and are treated as
`transaction_journal_v1` during audit reporting.

The v2 canonical hash material is the event object serialized as JSON with
sorted keys and compact separators, excluding only `event_sha256`. Every v2
event contains:

- `schema_name`
- `schema_version`
- `event_index`
- `event_type`
- `occurred_at`
- `previous_event_sha256`
- `event_sha256`

The `created` event additionally embeds the immutable transaction payload:
`txn_id`, `created_at`, `source_path`, `source_sha256`,
`source_snapshot_path`, ordered `fixes`, `applied`, optional `post_sha256`,
and optional `reverted_at`. The `applied` event records `post_sha256`; the
`reverted` event records the same `txn_id` and closes the local revert path.

Repair pipeline output uses `schema_version: repair_receipt_v1` and
`receipt_version: repair_receipt_v1`. Release-gate output uses
`schema_version: release_gate_report_v1`.

## Audit

Run `dataforge audit <txn-id>` to verify a transaction log before relying on
it as audit evidence. A verified v2 log proves local event order, payload
integrity, and replayability. Legacy v1 logs can still be replayed and reverted,
but audit reports mark them `legacy_unverified` because they do not contain
event hashes.

The hash chain is tamper-evident inside the local workspace. It is not an
external non-repudiation system unless the head hash is anchored in a separate
trusted system.

Audit verdicts are intentionally strict:

- `verified`: v2 hash chain, event order, replay, and revert prerequisites pass.
- `legacy_unverified`: v1 replay succeeds but no event hashes exist.
- `tampered`: v2 payload hash, event order, previous hash, event type, or txn id
  consistency fails.
- `malformed`: JSONL, schema version, or replay structure is invalid.
- `unrevertible`: the v2 log is intact but source/snapshot prerequisites for
  revert are missing or changed.
- `missing`: no unique transaction log can be found.

## Revert invariant

Revert is byte-for-byte only when the current file still matches the recorded
post-state hash. If another process changed the file after the DataForge apply,
the revert is refused to avoid losing unrelated work. For v2 logs, revert also
refuses when audit verification fails.
