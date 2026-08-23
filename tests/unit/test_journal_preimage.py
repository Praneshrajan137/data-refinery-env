"""The journal's on-disk bytes must be the bytes that were hashed.

Until 2026-08-23 they were not. ``_write_jsonl_line`` used ``json.dumps``'s **default**
separators and ``ensure_ascii=True`` while ``_canonical_event_bytes`` used compact
separators and ``ensure_ascii=False``.

Verification always succeeded, because it re-parses each line and recomputes from the
parsed object. That is exactly why the defect survived: nothing was red. The cost was that
a third party could not hash a line and compare -- they had to know to re-parse it with a
JSON library configured identically to this one. In a system whose central claim is
byte-level verifiability, that puts canonicalisation inside the trust boundary, which is
the failure ``dataforge/attestation/__init__.py`` deliberately avoids by signing a DSSE PAE
rather than JSON text, and which cited this log as its counterexample.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.transactions.log import (
    TransactionAuditVerdict,
    _canonical_event_bytes,
    _write_jsonl_line,
    verify_transaction_log,
)


def _line_bytes(path: Path) -> list[bytes]:
    """Return each non-empty line of a JSONL file as raw bytes."""
    return [line for line in path.read_bytes().split(b"\n") if line.strip()]


class TestOnDiskBytesAreThePreimage:
    """The property the fix establishes."""

    def test_line_minus_event_hash_is_byte_identical_to_the_preimage(self, tmp_path: Path) -> None:
        """A third party can now derive the preimage by deleting one specified key."""
        path = tmp_path / "log.jsonl"
        record = {
            "schema_version": "v2",
            "event": "created",
            "txn_id": "txn-abc",
            "nested": {"b": 2, "a": 1},
            "event_sha256": "0" * 64,
        }
        _write_jsonl_line(path, record, create=True)

        line = _line_bytes(path)[0]
        parsed = json.loads(line)
        del parsed["event_sha256"]
        rebuilt = json.dumps(
            parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

        assert rebuilt == _canonical_event_bytes(record), (
            "re-serializing the parsed line with the documented canonical options must "
            "reproduce the exact preimage"
        )

    def test_written_line_uses_compact_separators(self, tmp_path: Path) -> None:
        """The regression this file exists to prevent.

        Asserted on bytes rather than on a round-trip, because a round-trip through
        json.loads is insensitive to precisely the difference that was wrong.
        """
        path = tmp_path / "log.jsonl"
        _write_jsonl_line(path, {"a": 1, "b": {"c": 2}}, create=True)
        line = _line_bytes(path)[0]

        assert b", " not in line, "default separators reintroduced; the preimage uses ','"
        assert b'": ' not in line, "default separators reintroduced; the preimage uses ':'"
        assert line == b'{"a":1,"b":{"c":2}}'

    def test_non_ascii_is_written_raw_not_escaped(self, tmp_path: Path) -> None:
        """``ensure_ascii=True`` on disk against ``False`` in the preimage was the second half.

        A value containing a non-ASCII character -- a city name, an accented surname -- was
        enough on its own to make the line differ from what was hashed.
        """
        path = tmp_path / "log.jsonl"
        _write_jsonl_line(path, {"city": "Malmö"}, create=True)
        line = _line_bytes(path)[0]

        assert "Malmö".encode() in line
        assert b"\\u00f6" not in line, "non-ASCII escaped on disk but raw in the preimage"

    @pytest.mark.parametrize(
        "record",
        (
            {"plain": "value"},
            {"unicode": "café", "n": 1},
            {"nested": {"z": [1, 2, {"y": "ü"}]}},
            {"empty": {}, "list": []},
        ),
    )
    def test_property_holds_across_shapes(self, tmp_path: Path, record: dict) -> None:
        path = tmp_path / "log.jsonl"
        signed = dict(record)
        _write_jsonl_line(path, signed, create=True)
        line = _line_bytes(path)[0]
        assert line == _canonical_event_bytes(signed), (
            "with no event_sha256 present, the line IS the preimage"
        )


class TestBackwardCompatibility:
    """Older logs were never dependent on the on-disk layout, and must still verify."""

    def test_a_legacy_formatted_log_still_verifies(self, tmp_path: Path) -> None:
        """A real log, re-serialized in the OLD layout, must still verify.

        Verification re-parses, so the layout change is not a breaking one. Asserting this
        matters because "fix the writer" would be unsafe if it silently invalidated every
        journal a user already holds.

        Built through the real append path rather than hand-crafted, so the record shape is
        whatever the product actually writes rather than whatever a test author guessed.
        """
        from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
        from dataforge.transactions.log import find_transaction_log
        from tests.support.tables import build_premised_repairable_table

        table = build_premised_repairable_table(tmp_path / "premised.csv")
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=table.csv_path, mode="apply", repair_schema=table.schema
            )
        )
        assert result.fixes, "precondition: a real transaction must have been recorded"
        log_path = find_transaction_log(result.receipt.txn_id, search_root=tmp_path)

        fresh = verify_transaction_log(log_path=log_path)
        assert fresh.verdict is TransactionAuditVerdict.VERIFIED, fresh.errors

        # Rewrite every line in the OLD serialization: default separators, default
        # ensure_ascii. The hashes are untouched.
        records = [
            json.loads(line)
            for line in log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        log_path.write_text(
            "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
            encoding="utf-8",
        )

        legacy = verify_transaction_log(log_path=log_path)
        assert legacy.verdict is TransactionAuditVerdict.VERIFIED, legacy.errors
        assert legacy.event_count == fresh.event_count
        assert legacy.head_sha256 == fresh.head_sha256, (
            "the chain head must be independent of the on-disk byte layout"
        )
