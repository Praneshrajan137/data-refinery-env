"""Guard: the rwd corpus fetcher must refuse anything that is not the published artifact.

This exists because the failure it prevents already happened. The first premise-quality
measurement was computed against a local `included_candidates.csv` holding 1,170 candidates
in 68,995 bytes, while the Zenodo record it cites publishes 1,262 in 79,075. The candidate
universe defines the negative label set, so an unverified copy silently changed every
separation figure in a published trust document.

A truncated download that still parses is the worst available failure here: it produces
numbers rather than an error. So the checksum is not a nicety, and these tests pin the
refusal rather than the happy path.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scripts.bench.fetch_rwd_corpus import (
    FILES,
    LICENSE,
    NOT_A_RELATION,
    ZENODO_RECORD,
    _md5,
    _verified,
)


def test_the_manifest_covers_ten_relations_plus_three_metadata_files() -> None:
    """The corpus is ten tables; a manifest that drifts from that is not the corpus."""
    relations = sorted(set(FILES) - NOT_A_RELATION)
    assert len(relations) == 10, f"expected 10 relations, manifest has {len(relations)}"
    assert len(FILES) == 13, "10 relations + ground_truth + included/excluded candidates"
    for key in ("ground_truth.csv", "included_candidates.csv", "excluded_candidates.csv"):
        assert key in NOT_A_RELATION


def test_every_manifest_entry_carries_a_real_md5_and_size() -> None:
    for key, (digest, size) in FILES.items():
        assert len(digest) == 32, f"{key} checksum is not an MD5"
        assert int(digest, 16) >= 0, f"{key} checksum is not hexadecimal"
        assert size > 0, f"{key} has no expected size"


def test_the_licence_is_recorded_as_cc_by_not_mit() -> None:
    """Three places in this repository said MIT. The Zenodo record says CC-BY-4.0.

    Pinned because it is a licensing claim about a third party's data, which is the class of
    error that damages someone other than this project.
    """
    assert LICENSE == "CC-BY-4.0"
    assert ZENODO_RECORD == "8098909"


def test_verification_refuses_a_truncated_file(tmp_path: Path) -> None:
    """The exact failure Zenodo produced in practice: a short body that still parses."""
    payload = b"table,lhs,rhs\na,b,c\n"
    target = tmp_path / "included_candidates.csv"
    target.write_bytes(payload)
    digest = hashlib.md5(payload).hexdigest()  # noqa: S324 - matching a published checksum

    assert _verified(target, digest, len(payload)) is True

    # One byte short: the size check alone must reject it, before any parse can succeed.
    target.write_bytes(payload[:-1])
    assert _verified(target, digest, len(payload)) is False


def test_verification_refuses_a_same_length_different_file(tmp_path: Path) -> None:
    """Size is not sufficient. A same-length substitution must fail on the digest."""
    payload = b"table,lhs,rhs\na,b,c\n"
    target = tmp_path / "t.csv"
    target.write_bytes(payload)
    digest = hashlib.md5(payload).hexdigest()  # noqa: S324

    swapped = bytearray(payload)
    swapped[-2] = ord("d")
    target.write_bytes(bytes(swapped))

    assert len(bytes(swapped)) == len(payload)
    assert _verified(target, digest, len(payload)) is False


def test_verification_refuses_an_absent_file(tmp_path: Path) -> None:
    assert _verified(tmp_path / "nope.csv", "0" * 32, 1) is False


def test_md5_is_chunked_and_matches_hashlib(tmp_path: Path) -> None:
    """A 73 MB table must not be read into memory; the chunked digest must still be right."""
    payload = b"x" * (1024 * 1024 + 7)  # crosses the chunk boundary
    target = tmp_path / "big.bin"
    target.write_bytes(payload)
    assert _md5(target) == hashlib.md5(payload).hexdigest()  # noqa: S324


@pytest.mark.parametrize(
    "key",
    [
        "hospital.csv",
        "dblp10k.csv",
        "adult.csv",
        "claims.csv",
        "tax.csv",
        "t_biocase_gathering_agent_r72738_c18.csv",
        "t_biocase_gathering_namedareas_r137711_c11.csv",
        "t_biocase_gathering_r90992_c35.csv",
        "t_biocase_identification_highertaxon_r562959_c3.csv",
        "t_biocase_identification_r91800_c38.csv",
    ],
)
def test_each_named_relation_is_in_the_manifest(key: str) -> None:
    """Named explicitly so silently dropping a table from the manifest fails here.

    The pre-registration declares a 3-table arm and a 10-table arm, and which arm ran
    determines the scope of every conclusion. A manifest that quietly lost a table would
    narrow that scope without narrowing any claim.
    """
    assert key in FILES
