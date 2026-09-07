"""Fetch the rwd corpus into `.benchmarks/rwd/`, verifying every file against its checksum.

The corpus is the annotated real-world AFD benchmark from Parciak, Weytjens, Neven, Hens,
Peeters and Vansummeren, *Measuring Approximate Functional Dependencies: a Comparative
Study*, ICDE 2024 (arXiv:2312.06296), published on Zenodo as record 8098909 under
**CC-BY-4.0**.

Two reasons this exists as a script rather than as instructions in a document:

1. **Checksums.** Zenodo publishes an MD5 per file. A partially-downloaded 73 MB table that
   still parses is the worst failure mode available here -- it would silently change every
   measure computed from it. Every file is verified before it counts as present.
2. **The corpus is a premise, and its provenance is part of the result.** The manifest below
   records the exact bytes each measurement was taken from.

Idempotent: a file already present with the right checksum is left alone. Nothing outside
`.benchmarks/rwd/` is written, and `.benchmarks/` is not tracked by git.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CORPUS_DIR = REPO / ".benchmarks" / "rwd"

ZENODO_RECORD = "8098909"
LICENSE = "CC-BY-4.0"
_BASE = f"https://zenodo.org/api/records/{ZENODO_RECORD}/files"
_MAX_ATTEMPTS = 3

# key -> (md5, size_bytes). Taken from the Zenodo record's own file listing, so a mismatch
# means either a truncated download or that the record changed under us. Either way, stop.
FILES: dict[str, tuple[str, int]] = {
    # Annotations and the candidate universe. Small, and load-bearing for every measure.
    "ground_truth.csv": ("0a44bb6e5aa445f8c37125f52aab4ff2", 5962),
    "included_candidates.csv": ("b53cfbd72cc3c927ec166de5bc93ad23", 79075),
    "excluded_candidates.csv": ("afc8eec78c8ca755a066a9a06ab954b5", 270004),
    # The ten relations.
    "adult.csv": ("06a12d8b45ce8d320f62f30ef2339e4c", 3974476),
    "claims.csv": ("0e933659996d859144ae45e9c6fdd7be", 17370317),
    "dblp10k.csv": ("4d9e63e1ace2166a11bcc81103d93328", 4992341),
    "hospital.csv": ("bea9c52f8dd6d2187276e8f52edaa285", 30629835),
    "tax.csv": ("ccaec8c35740f0f640fc59a675b6097d", 73013202),
    "t_biocase_gathering_agent_r72738_c18.csv": (
        "6e2b4906d63949c18cf1aedb1fb63386",
        14146109,
    ),
    "t_biocase_gathering_namedareas_r137711_c11.csv": (
        "1117a398aa162add9b1ddd5605923508",
        21211815,
    ),
    "t_biocase_gathering_r90992_c35.csv": ("7f871fffe3fb8c7c82969578a77b4327", 24869368),
    "t_biocase_identification_highertaxon_r562959_c3.csv": (
        "b02f7a46c5961addbec7c20a6fcf53d1",
        30534167,
    ),
    "t_biocase_identification_r91800_c38.csv": ("5ebcbef341a17c4b1320c3dc887845f5", 29815247),
}

# `excluded_candidates.csv` is fetched but is NOT part of the annotated universe: the authors
# excluded a candidate when no tuple had both attributes present, or when `g3_prime` was too
# small. It is downloaded so that the exclusion criterion can be inspected rather than assumed.
NOT_A_RELATION = {"ground_truth.csv", "included_candidates.csv", "excluded_candidates.csv"}


def _md5(path: Path) -> str:
    """Return the MD5 of a file, read in chunks so a 73 MB table does not land in memory."""
    digest = hashlib.md5()  # noqa: S324 - matching Zenodo's published checksum, not security
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verified(path: Path, expected_md5: str, expected_size: int) -> bool:
    """True when the file on disk is exactly the published artifact."""
    if not path.is_file():
        return False
    if path.stat().st_size != expected_size:
        return False
    return _md5(path) == expected_md5


def main() -> int:
    """Download any missing or corrupt corpus file, then report what is present."""
    parser = argparse.ArgumentParser(description="Fetch the rwd AFD benchmark corpus.")
    parser.add_argument(
        "--only",
        action="append",
        default=None,
        help="Fetch only this file key; repeatable. Default: all.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Check what is present and correct, download nothing.",
    )
    args = parser.parse_args()

    wanted = args.only if args.only else sorted(FILES)
    unknown = [key for key in wanted if key not in FILES]
    if unknown:
        print(f"FAIL unknown file key(s): {unknown}", file=sys.stderr)
        return 2

    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    fetched = 0
    already = 0

    for key in wanted:
        expected_md5, expected_size = FILES[key]
        target = CORPUS_DIR / key
        if _verified(target, expected_md5, expected_size):
            already += 1
            continue
        if args.verify_only:
            state = "MISSING" if not target.is_file() else "CHECKSUM MISMATCH"
            print(f"  {state:<18} {key}")
            failures.append(key)
            continue

        url = f"{_BASE}/{key}/content"
        print(f"  fetching {key} ({expected_size / 1_048_576:.1f} MB) ...", flush=True)
        # Download to a sibling temp path so an interrupted transfer cannot leave a
        # truncated file that later reads as "present". Zenodo returned a short body on
        # the first real run of this script ("got only 262744 out of 270004 bytes"), which
        # is precisely why the staging path and the checksum are not optional.
        staging = target.with_suffix(target.suffix + ".partial")
        ok = False
        last_error = "unknown"
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                urllib.request.urlretrieve(url, staging)  # noqa: S310 - fixed https host
            except Exception as error:  # noqa: BLE001 - retry, then report
                last_error = str(error)
                staging.unlink(missing_ok=True)
                if attempt < _MAX_ATTEMPTS:
                    print(f"    attempt {attempt} failed ({error}); retrying", flush=True)
                    time.sleep(2 * attempt)
                continue
            if _verified(staging, expected_md5, expected_size):
                ok = True
                break
            got = _md5(staging) if staging.is_file() else "no file"
            last_error = f"checksum mismatch (expected {expected_md5}, got {got})"
            staging.unlink(missing_ok=True)
            if attempt < _MAX_ATTEMPTS:
                print(f"    attempt {attempt} corrupt; retrying", flush=True)
                time.sleep(2 * attempt)

        if not ok:
            print(f"    FAILED {key}: {last_error}", file=sys.stderr)
            failures.append(key)
            continue

        staging.replace(target)
        fetched += 1

    relations = [k for k in FILES if k not in NOT_A_RELATION]
    present_relations = [
        k for k in relations if _verified(CORPUS_DIR / k, FILES[k][0], FILES[k][1])
    ]

    print(
        f"\nrwd corpus at {CORPUS_DIR}\n"
        f"  license      : {LICENSE} (Zenodo record {ZENODO_RECORD})\n"
        f"  fetched now   : {fetched}\n"
        f"  already ok    : {already}\n"
        f"  relations     : {len(present_relations)}/{len(relations)} verified\n"
        f"  failures      : {len(failures)}"
    )
    if failures:
        print(f"  failed keys  : {failures}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
