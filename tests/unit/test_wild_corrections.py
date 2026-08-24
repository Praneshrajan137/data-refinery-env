"""Tests for the wild-column determinability labels.

Offline. The label file is committed, so these run without fetching anything.

The load-bearing test is `TestNoCorpusBytesAreVendored`: RT-bench and ST-bench carry no licence,
so the whole point of hashing the keys is that this file can be committed at all. If a value ever
leaks into it, that test is what catches it.
"""

from __future__ import annotations

import hashlib
import json
import re

import pytest

from dataforge.datasets.registry import COLUMN_BENCHMARK_REGISTRY
from dataforge.datasets.wild_corrections import (
    LABELS_PATH,
    WildCorrectionError,
    determinability_counts,
    label_key,
    load_wild_correction_labels,
    lookup_label,
)


class TestNoCorpusBytesAreVendored:
    """The licence constraint, enforced rather than trusted.

    Upstream publishes no licence, so `registry.py` records `license_spdx=None` and the bytes may
    live only in a fetched, hash-verified cache. A label file quoting the erroneous values would
    be redistribution.
    """

    def test_the_file_asserts_it_holds_no_corpus_values(self) -> None:
        payload = json.loads(LABELS_PATH.read_text(encoding="utf-8"))
        assert payload["contains_corpus_values"] is False

    def test_the_loader_refuses_a_file_that_does_not_assert_it(self, tmp_path, monkeypatch) -> None:
        """Fail closed: an unasserted file might be carrying values."""
        bad = tmp_path / "labels.json"
        bad.write_text(json.dumps({"labels": {"rt_bench:1:abc": {"label": "ambiguous"}}}), "utf-8")
        monkeypatch.setattr("dataforge.datasets.wild_corrections.LABELS_PATH", bad)
        load_wild_correction_labels.cache_clear()
        with pytest.raises(WildCorrectionError, match="contains_corpus_values"):
            load_wild_correction_labels()
        load_wild_correction_labels.cache_clear()

    def test_every_key_is_a_hash_not_a_value(self) -> None:
        for key, entry in load_wild_correction_labels().items():
            corpus, index, digest = key.split(":")
            assert corpus in COLUMN_BENCHMARK_REGISTRY
            assert index.isdigit()
            assert len(digest) == 16
            assert all(character in "0123456789abcdef" for character in digest)
            assert entry.value_sha256_prefix == digest

    def test_notes_do_not_quote_values(self) -> None:
        """A note describes a value abstractly rather than pasting it in.

        Possessive apostrophes are stripped first, because "the column's range" is not a quoted
        value and an earlier version of this test failed on exactly that. What remains is
        checked for any quote character at all.

        Not a proof -- a note could paraphrase a value exactly -- but it catches the obvious
        failure of quoting a value in for clarity, which is the one a hurried annotator commits.
        """
        for key, entry in load_wild_correction_labels().items():
            prose = re.sub(r"'(?=s\b)", "", entry.note)
            for quote in ("'", '"'):
                assert quote not in prose, (
                    f"{key}: note contains a quote character, which may be a corpus value"
                )


class TestTheCensus:
    """88 values, every one labelled."""

    def test_all_88_labelled_errors_are_covered(self) -> None:
        assert len(load_wild_correction_labels()) == 88

    def test_the_counts_are_the_published_ones(self) -> None:
        """These are the numbers docs/trust/wild-correction-determinability.md quotes."""
        counts = determinability_counts()
        assert counts == {"correctable": 52, "not_determinable": 35, "ambiguous": 1}
        assert sum(counts.values()) == 88

    def test_both_corpora_are_represented(self) -> None:
        by_corpus: dict[str, int] = {}
        for entry in load_wild_correction_labels().values():
            by_corpus[entry.corpus] = by_corpus.get(entry.corpus, 0) + 1
        assert by_corpus == {"rt_bench": 41, "st_bench": 47}

    def test_the_correctable_share_is_reported_to_four_places(self) -> None:
        counts = determinability_counts()
        share = counts["correctable"] / sum(counts.values())
        assert round(share, 4) == 0.5909


class TestRulesAndLabelsAgree:
    """A label is a rule application, so the two may never disagree."""

    def test_every_correctable_carries_a_correctable_rule(self) -> None:
        for key, entry in load_wild_correction_labels().items():
            if entry.label == "correctable":
                assert entry.rule in {"R1", "R2", "R3", "R4"}, key

    def test_every_not_determinable_carries_a_not_determinable_rule(self) -> None:
        for key, entry in load_wild_correction_labels().items():
            if entry.label == "not_determinable":
                assert entry.rule in {"N1", "N2", "N3"}, key

    def test_the_ambiguous_entry_cites_a_rule_conflict(self) -> None:
        """The taxonomy admits ambiguous only when both a correctable and a not rule apply."""
        ambiguous = [e for e in load_wild_correction_labels().values() if e.label == "ambiguous"]
        assert len(ambiguous) == 1
        assert "/" in ambiguous[0].rule, "an ambiguous label must name the conflicting rules"

    def test_a_mismatched_label_and_rule_is_refused(self, tmp_path, monkeypatch) -> None:
        bad = tmp_path / "labels.json"
        bad.write_text(
            json.dumps(
                {
                    "contains_corpus_values": False,
                    "labels": {
                        "rt_bench:1:0123456789abcdef": {"label": "correctable", "rule": "N1"}
                    },
                }
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr("dataforge.datasets.wild_corrections.LABELS_PATH", bad)
        load_wild_correction_labels.cache_clear()
        with pytest.raises(WildCorrectionError, match="non-correctable rule"):
            load_wild_correction_labels()
        load_wild_correction_labels.cache_clear()

    def test_every_note_is_substantive(self) -> None:
        """A label with no reasoning is not auditable.

        The floor is deliberately low. It exists to catch an empty or placeholder note, not to
        mandate verbosity: "misspelt month name" is a complete justification for an R3 label.
        """
        for key, entry in load_wild_correction_labels().items():
            assert len(entry.note) >= 15, f"{key}: note too short to justify a judgement"


class TestLookup:
    """Joining a label to a value requires the value, and yields no correction."""

    def test_a_key_is_derived_from_the_value_hash(self) -> None:
        expected = hashlib.sha256(b"anything").hexdigest()[:16]
        assert label_key("rt_bench", 7, "anything") == f"rt_bench:7:{expected}"

    def test_an_unlabelled_value_returns_none(self) -> None:
        assert lookup_label("rt_bench", 0, "a value that is not a labelled error") is None

    def test_the_same_string_can_be_labelled_in_several_columns(self) -> None:
        """Why the key includes the column index. Sentinels recur across columns."""
        by_digest: dict[str, set[str]] = {}
        for entry in load_wild_correction_labels().values():
            by_digest.setdefault(entry.value_sha256_prefix, set()).add(
                f"{entry.corpus}:{entry.column_index}"
            )
        shared = {digest: places for digest, places in by_digest.items() if len(places) > 1}
        assert shared, (
            "precondition: at least one value must recur across columns, or keying on the "
            "column index is unnecessary complexity"
        )

    def test_no_label_carries_a_correction(self) -> None:
        """This is a determinability census, not a repair benchmark."""
        from dataforge.datasets.wild_corrections import WildCorrectionLabel

        fields = set(WildCorrectionLabel.__dataclass_fields__)
        for forbidden in ("clean_value", "correction", "correct_value", "replacement"):
            assert forbidden not in fields
