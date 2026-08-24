"""Every corpus fetch in the repository must be pinned, enumerated by search not by hand.

The pre-existing guard asserted that *one* registry entry avoided a mutable ref:

    assert "refs/heads/master" not in DATASET_REGISTRY["hospital"].source_urls[0]

That is the partial closure this project has already been burned by. "Gate G is closed" is
a claim about **every** surface, and a registry-only check could not see that
`scripts/remote/hf_full_eval_job.py` and nine training configs fetched
`refs/heads/master` with no checksum at all -- so a held-out release evaluation could
change silently whenever upstream moved.

This file enumerates fetch sites by scanning the tree, so a new unpinned fetch fails here
whether or not anyone remembered to add it to a list.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dataforge.datasets.registry import (
    AUTOTEST_GIT_REVISION,
    COLUMN_BENCHMARK_REGISTRY,
    DATASET_REGISTRY,
    RAHA_GIT_REVISION,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Directories that are generated, vendored, or frozen history rather than source.
_EXCLUDED_PARTS = {
    ".venv",
    ".venv_bench",
    "node_modules",
    "site",
    "htmlcov",
    "build",
    "dist",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    ".hf-space-repo",
    ".hf-space-stage",
    ".hf-space-stage-plan",
    "kaggle_dataset_v3",
    "logs",
}

# A mutable git ref in a raw-content URL: the digest of what you download is whatever
# upstream last pushed.
_MUTABLE_REF = re.compile(r"raw\.githubusercontent\.com/[^\s\"']+/refs/heads/")

# Executable code, where an unpinned fetch is a live risk: this is what actually runs.
_EXECUTABLE_ROOTS = ("dataforge", "scripts", "tests", "packages", "server", "eval/*.py")

# Snapshots of completed runs. `PRODUCT.md` forbids rewriting frozen historical evidence,
# and a run's recorded source URLs are part of what that run did.
_FROZEN_SNAPSHOT_MARKERS = ("dataforge-src", "_handoff")

# Training configs still carrying mutable refs. Each one both configures a future run and
# documents a completed one, so rewriting them would misrepresent the run's provenance
# while leaving them leaves a future run unpinned.
#
# Rather than pick one wrong answer silently, the debt is counted. The ceiling cannot be
# raised without editing this constant, which makes any growth a reviewed decision. The
# live release-evaluation path -- `scripts/remote/hf_full_eval_job.py`, the one that could
# change a held-out number -- is pinned and verified, and is covered by the hard guard.
_TRAINING_CONFIG_DEBT_CEILING = 90


def _source_files(*, executable_only: bool) -> list[Path]:
    """Return candidate source files, optionally restricted to executable code."""
    files: list[Path] = []
    for pattern in ("**/*.py", "**/*.yaml", "**/*.yml"):
        for path in PROJECT_ROOT.glob(pattern):
            if _EXCLUDED_PARTS & set(path.parts):
                continue
            relative = path.relative_to(PROJECT_ROOT)
            if any(marker in str(relative) for marker in _FROZEN_SNAPSHOT_MARKERS):
                continue
            if executable_only and relative.parts[0] not in {
                "dataforge",
                "scripts",
                "tests",
                "packages",
                "server",
            }:
                continue
            files.append(path)
    return files


def _mutable_ref_sites(paths: list[Path]) -> list[str]:
    """Return `path:line` for every mutable-ref fetch in the given files."""
    offenders: list[str] = []
    for path in paths:
        if path.name == Path(__file__).name:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if _MUTABLE_REF.search(line):
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}:{number}")
    return offenders


def test_no_executable_code_fetches_a_corpus_from_a_mutable_ref() -> None:
    """No `refs/heads/...` raw URL in any code that runs.

    This test is the point of the file: it enumerates by scanning, so it cannot be
    satisfied by fixing only the site someone happened to think of. The pre-existing guard
    checked one registry entry and therefore could not see that the HF Jobs release
    evaluation fetched a moving branch with no checksum at all.
    """
    offenders = _mutable_ref_sites(_source_files(executable_only=True))
    assert not offenders, (
        "corpus fetched from a mutable git ref in executable code at: "
        + ", ".join(offenders)
        + ". Pin to a commit SHA and verify a SHA-256, or a held-out evaluation can change "
        "silently when upstream moves."
    )


def test_training_config_pinning_debt_does_not_grow() -> None:
    """The remaining unpinned sites are counted, so they cannot quietly multiply.

    Counted rather than fixed because each config is simultaneously a future run's input
    and a past run's record. Counted rather than ignored because an uncounted exception
    becomes a permanent one.
    """
    sites = _mutable_ref_sites(_source_files(executable_only=False))
    non_training = [site for site in sites if not site.startswith("training")]
    assert not non_training, (
        f"unpinned fetch outside training/: {non_training}. Only training configs carry "
        "accepted pinning debt."
    )
    assert len(sites) <= _TRAINING_CONFIG_DEBT_CEILING, (
        f"training-config pinning debt grew from {_TRAINING_CONFIG_DEBT_CEILING} to "
        f"{len(sites)} sites. A new config must pin a commit SHA; do not raise the ceiling "
        "to accommodate one."
    )
    # Non-vacuity: if the scan silently stopped finding anything, the ceiling assertion
    # above would pass while proving nothing.
    assert sites, (
        "precondition: the known debt must still be visible. If it is genuinely zero, "
        "delete this test and the ceiling constant rather than leaving a vacuous pass."
    )


# Which upstream each registered corpus is pinned to. Keyed by corpus name so a corpus
# from a third upstream can be registered without weakening the assertion.
#
# This was `assert metadata.source_revision == RAHA_GIT_REVISION` over the whole registry
# until 2026-08-24 -- correct while every corpus shared one upstream, and a hard blocker
# the moment one does not. The failure mode it invited is the dangerous direction: the
# cheapest way to make a third corpus pass is to delete the assertion, which would remove
# commit pinning from every corpus at once.
#
# A corpus absent from this map fails rather than being skipped, so adding one is a
# deliberate act recorded in review.
_EXPECTED_DATASET_REVISION: dict[str, str] = dict.fromkeys(
    ("hospital", "flights", "rayyan", "tax"), RAHA_GIT_REVISION
)
_EXPECTED_COLUMN_BENCHMARK_REVISION: dict[str, str] = dict.fromkeys(
    ("rt_bench", "st_bench"), AUTOTEST_GIT_REVISION
)


@pytest.mark.parametrize("name", sorted(DATASET_REGISTRY))
def test_registry_entries_pin_a_commit_sha(name: str) -> None:
    metadata = DATASET_REGISTRY[name]
    expected = _EXPECTED_DATASET_REVISION.get(name)
    assert expected is not None, (
        f"{name} has no expected upstream revision. Add it to "
        "_EXPECTED_DATASET_REVISION rather than relaxing this test; a corpus whose "
        "pinning nobody declared is a corpus whose pinning nobody checked."
    )
    assert metadata.source_revision == expected
    assert len(metadata.source_revision) == 40, "a short ref is ambiguous across forks"
    for url in metadata.source_urls:
        assert metadata.source_revision in url
        assert "refs/heads/" not in url


@pytest.mark.parametrize("name", sorted(COLUMN_BENCHMARK_REGISTRY))
def test_column_benchmarks_pin_a_commit_sha(name: str) -> None:
    metadata = COLUMN_BENCHMARK_REGISTRY[name]
    expected = _EXPECTED_COLUMN_BENCHMARK_REVISION.get(name)
    assert expected is not None, (
        f"{name} has no expected upstream revision. Add it to "
        "_EXPECTED_COLUMN_BENCHMARK_REVISION rather than relaxing this test."
    )
    assert metadata.source_revision == expected
    assert len(metadata.source_revision) == 40
    assert metadata.source_revision in metadata.source_url
    assert "refs/heads/" not in metadata.source_url


def test_every_registered_corpus_has_a_declared_upstream() -> None:
    """The maps above must cover the registries, or a corpus escapes the check.

    Asserted separately from the parametrized tests because a parametrized test can only
    fail for corpora that exist; this fails if the map and the registry drift apart in
    either direction.
    """
    assert set(_EXPECTED_DATASET_REVISION) == set(DATASET_REGISTRY)
    assert set(_EXPECTED_COLUMN_BENCHMARK_REVISION) == set(COLUMN_BENCHMARK_REGISTRY)


class TestStandaloneJobDuplication:
    """The HF Jobs script cannot import the registry, so its copy must be verified."""

    def _job_source(self) -> str:
        return (PROJECT_ROOT / "scripts" / "remote" / "hf_full_eval_job.py").read_text(
            encoding="utf-8"
        )

    def test_job_pins_the_same_revision_as_the_registry(self) -> None:
        assert f'RAHA_GIT_REVISION = "{RAHA_GIT_REVISION}"' in self._job_source()

    @pytest.mark.parametrize("name", ("hospital", "flights"))
    def test_job_digests_match_the_registry(self, name: str) -> None:
        """Duplication is acceptable only while it is checked."""
        source = self._job_source()
        metadata = DATASET_REGISTRY[name]
        assert metadata.dirty_sha256 in source, f"{name} dirty digest drifted from the registry"
        assert metadata.clean_sha256 in source, f"{name} clean digest drifted from the registry"

    def test_job_verifies_digests_rather_than_merely_recording_them(self) -> None:
        """A recorded digest that is never compared is decoration."""
        source = self._job_source()
        assert "_fetch_verified" in source
        assert "hashlib.sha256" in source
        assert "does not match pinned revision" in source

    def test_job_no_longer_fetches_the_deregistered_corpus(self) -> None:
        source = self._job_source()
        assert '"beers"' not in source
        assert "datasets/beers/" not in source


def test_deregistered_corpus_is_not_a_hardcoded_default_anywhere() -> None:
    """A hardcoded default cannot be de-registered; it becomes a crash instead.

    `scripts/data/audit_real_world_sources.py` kept `("hospital", "flights", "beers")` as
    its default and therefore raised KeyError on its default invocation.
    """
    audit = (PROJECT_ROOT / "scripts" / "data" / "audit_real_world_sources.py").read_text(
        encoding="utf-8"
    )
    assert "DEFAULT_DATASETS = tuple(sorted(DATASET_REGISTRY))" in audit
    # A mention in an explanatory comment is fine and desirable; a fetchable reference is
    # not. Checking the URL path rather than the bare name keeps the comment legal.
    assert "datasets/beers/" not in audit
    assert set(DATASET_REGISTRY) >= {"hospital", "flights"}, "precondition"
