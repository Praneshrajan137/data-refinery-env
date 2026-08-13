"""Run the adversarial corpus through the write gate and measure corruption.

The claim under test is the product's central one, stated precisely: an untrusted proposer
cannot write a value that is **not proven against the stated constraints**, and everything
written reverses byte-for-byte. It is NOT the claim that every written value is true -- no
verifier can establish that without ground truth it does not have.

The corpus therefore splits along exactly that line:

* **Discriminable attacks** violate a declared type, bound, pattern, category, or the
  table's structure. The gate must block every one. A write here is a real defect.
* **Undecidable attacks** satisfy every declared constraint and are merely false. The gate
  writes them, labels them proven-against-the-schema, and keeps them reversible. Counting
  these as gate failures would be asserting a guarantee the product explicitly refuses to
  make, which is the error this project has corrected before.

The first run of this suite asserted zero corruptions across the whole corpus and failed on
three undecidable attacks. The assertion was wrong, not the gate -- recorded here because
the distinction is the substance of the result.

Two premises are measured, both covering every column, so the gate labels writes ``proven``
under both. The difference between them is what the label is worth when the premise is
weak.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.detectors.base import Schema
from dataforge.engine.repair import ExternalFix, VerifyAndApplyRequest, verify_and_apply
from dataforge.transactions.revert import revert_transaction
from tests.adversarial.corpus import (
    PERMISSIVE_SCHEMA,
    TIGHT_SCHEMA,
    Attack,
    CorpusOutcome,
    build_corpus,
    clopper_pearson_upper,
    truth,
    write_table,
)


def _expected_truth(row: int, column: str) -> str | None:
    try:
        return truth(row, column)
    except (IndexError, ValueError):
        return None


def run_corpus(source: Path, schema: Schema) -> CorpusOutcome:
    """Submit every attack individually and score it against ground truth.

    One request per attack, not one batch: a batch can be rejected wholesale for an
    unrelated reason, which would make the corpus look effective while proving nothing about
    the individual attacks.
    """
    corpus = build_corpus()
    corruptions: list[str] = []
    correct: list[str] = []
    wrongly_blocked: list[str] = []
    reasons: dict[str, str] = {}
    applied_total = 0
    held_total = 0

    for attack in corpus:
        write_table(source)
        if attack.pre_corrupt is not None:
            row, column, wrong = attack.pre_corrupt
            _poke(source, row, column, wrong)

        result = verify_and_apply(
            VerifyAndApplyRequest(
                source_path=source,
                fixes=[attack.fix],
                mode="apply",
                schema=schema,
                confirm_escalations=True,
                proposer="adversarial-corpus",
            )
        )
        receipt = result.receipt

        if receipt.applied_fixes:
            applied_total += len(receipt.applied_fixes)
            for applied_fix in receipt.applied_fixes:
                expected = _expected_truth(applied_fix.row, applied_fix.column)
                if expected is None or applied_fix.new_value != expected:
                    corruptions.append(attack.name)
                else:
                    correct.append(attack.name)
        else:
            held_total += 1
            reason = next(
                (s.review_reason for s in receipt.suggested_fixes if s.review_reason),
                None,
            )
            reasons[attack.name] = reason or "held_without_reason"
            if attack.is_correct_proposal:
                wrongly_blocked.append(attack.name)

    return CorpusOutcome(
        proposed=len(corpus),
        applied=applied_total,
        held=held_total,
        corruptions=tuple(corruptions),
        correct_applications=tuple(correct),
        wrongly_blocked=tuple(wrongly_blocked),
        block_reasons=reasons,
    )


def _poke(source: Path, row: int, column: str, value: str) -> None:
    """Write one wrong cell directly, bypassing the engine, to create a real error."""
    lines = source.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    index = header.index(column)
    cells = lines[row + 1].split(",")
    cells[index] = value
    lines[row + 1] = ",".join(cells)
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def source(tmp_path: Path) -> Path:
    return write_table(tmp_path / "readings.csv")


def test_corpus_is_not_vacuous() -> None:
    """A corpus that shrank to a couple of easy cases would prove nothing."""
    corpus = build_corpus()
    assert len(corpus) >= 15
    classes = {attack.attack_class for attack in corpus}
    for required in (
        "plausible_but_wrong",
        "rare_correct_overwrite",
        "stale_precondition",
        "homoglyph",
        "zero_width",
        "boundary",
        "invalid_target",
        "injection",
        "legitimate",
    ):
        assert required in classes, f"corpus no longer covers {required}"
    assert any(attack.is_correct_proposal for attack in corpus), (
        "a corpus with no legitimate proposal cannot detect a gate that blocks everything"
    )
    # Both halves must be populated, or the split is decorative.
    assert sum(1 for a in corpus if a.discriminable) >= 10
    assert sum(1 for a in corpus if not a.discriminable) >= 3


def test_no_discriminable_attack_is_ever_written(source: Path) -> None:
    """The product's actual guarantee, measured.

    Every attack the stated constraints CAN reject must be rejected. This is the assertion
    that would catch a real gate regression.
    """
    outcome = run_corpus(source, TIGHT_SCHEMA)
    corpus = {attack.name: attack for attack in build_corpus()}

    discriminable_corruptions = [name for name in outcome.corruptions if corpus[name].discriminable]
    discriminable_total = sum(1 for attack in corpus.values() if attack.discriminable)
    bound = clopper_pearson_upper(len(discriminable_corruptions), discriminable_total)

    print(
        f"\nADVERSARIAL tight schema, discriminable attacks: n={discriminable_total} "
        f"corruptions={len(discriminable_corruptions)} rate_upper_95={bound:.4f}"
    )
    assert discriminable_corruptions == [], (
        f"the gate wrote a value that violates the stated constraints: {discriminable_corruptions}"
    )
    # And it did not achieve that by refusing everything.
    assert outcome.wrongly_blocked == (), (
        f"a legitimate proposal was blocked: {list(outcome.wrongly_blocked)}"
    )
    assert outcome.correct_applications, "no correct proposal was applied at all"


def test_undecidable_attacks_are_written_but_reversible(source: Path) -> None:
    """The honest limit, stated as a test rather than as a caveat.

    A constraint-satisfying but false value IS written. The guarantee that applies to it is
    not correctness -- it is that the write is recorded, attributed, and reverses to the
    exact prior bytes. This test pins that guarantee, because it is the one being relied on.
    """
    corpus = [attack for attack in build_corpus() if not attack.discriminable]
    assert corpus, "no undecidable attacks left to measure"

    for attack in corpus:
        write_table(source)
        before = source.read_bytes()
        result = verify_and_apply(
            VerifyAndApplyRequest(
                source_path=source,
                fixes=[attack.fix],
                mode="apply",
                schema=TIGHT_SCHEMA,
                confirm_escalations=True,
                proposer="adversarial-corpus",
            )
        )
        receipt = result.receipt
        assert receipt.applied_fixes, (
            f"{attack.name} was blocked; it satisfies every declared constraint, so a "
            "block would mean the schema is doing something other than what it declares"
        )
        # Labelled honestly: proven against the schema, and the schema is named.
        assert all(f.verification_strength == "proven" for f in receipt.applied_fixes)
        assert receipt.authoritative_columns, (
            "a proven untrusted write must name the authority that proved it"
        )
        assert source.read_bytes() != before

        # The guarantee that actually holds for this class.
        assert receipt.txn_id is not None
        revert_transaction(receipt.txn_id, search_root=source.parent)
        assert source.read_bytes() == before, (
            f"{attack.name} did not revert to the exact prior bytes"
        )


def test_without_authority_nothing_untrusted_is_written(source: Path) -> None:
    """With no schema, every external proposal must be HELD -- including the correct one.

    This is the run that exercises the PROVEN-ONLY gate rather than the SMT verifier. The
    first version of this corpus always supplied a schema, so every attack was blocked by
    constraint checking and `enforce_proven_only` was never reached. Mutation testing proved
    it: neutering the proven-only gate entirely, and separately making every provenance
    trusted, left the whole corpus green. It was measuring the verifier and claiming to
    measure the guarantee.

    Without an authoritative schema an `external` value is `plausibility_only` by
    construction, so a correct proposal is held too. That is the intended behaviour: the
    gate refuses to write what it cannot prove, and being right is not the same as being
    provable.
    """
    corpus = build_corpus()
    written: list[str] = []

    for attack in corpus:
        write_table(source)
        if attack.pre_corrupt is not None:
            row, column, wrong = attack.pre_corrupt
            _poke(source, row, column, wrong)
        before = source.read_bytes()

        result = verify_and_apply(
            VerifyAndApplyRequest(
                source_path=source,
                fixes=[attack.fix],
                mode="apply",
                schema=None,
                confirm_escalations=True,
                proposer="adversarial-corpus",
            )
        )
        if result.receipt.applied_fixes or source.read_bytes() != before:
            written.append(attack.name)

    assert written == [], (
        f"an untrusted value was written with no authoritative schema to prove it: {written}"
    )


def test_authority_does_not_leak_across_columns(source: Path) -> None:
    """A schema that speaks about one column must not authorise writes to another.

    The 2026-08-09 defect: one accepted `column_type` constraint on `id` granted blanket
    authority, so a garbage `external` value on the unrelated column `city` was applied and
    stamped `proven`. Mutation testing showed the earlier corpus could not detect it,
    because the tight schema already covered every column.
    """
    partial = Schema(columns={"score": "float"})

    write_table(source)
    before = source.read_bytes()
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[
                # A value that satisfies nothing in particular, on a column the schema
                # never mentions.
                ExternalFix(row=0, column="city", new_value="ZZZ_GARBAGE"),
            ],
            mode="apply",
            schema=partial,
            confirm_escalations=True,
            proposer="adversarial-corpus",
        )
    )
    assert result.receipt.applied_fixes == [], (
        "authority over 'score' authorised a write to 'city'; authority is leaking across columns"
    )
    assert source.read_bytes() == before
    assert "city" not in result.receipt.authoritative_columns


def test_a_permissive_premise_measurably_weakens_the_guarantee(source: Path) -> None:
    """How much of the guarantee comes from the premise rather than the gate.

    Both schemas cover every column, so the gate reports ``proven`` under both. The gap is
    the measured cost of a premise that constrains nothing -- which is what schema inference
    produces from dirty data, and what a hurried operator accepts.
    """
    corpus = {attack.name: attack for attack in build_corpus()}
    discriminable_total = sum(1 for attack in corpus.values() if attack.discriminable)

    tight = run_corpus(source, TIGHT_SCHEMA)
    permissive = run_corpus(source, PERMISSIVE_SCHEMA)

    tight_bad = [name for name in tight.corruptions if corpus[name].discriminable]
    permissive_bad = [name for name in permissive.corruptions if corpus[name].discriminable]

    print(
        f"\nADVERSARIAL premise comparison over {discriminable_total} discriminable attacks:"
        f"\n  tight schema (typed, bounded, patterned, enumerated): "
        f"{len(tight_bad)} written, upper95="
        f"{clopper_pearson_upper(len(tight_bad), discriminable_total):.4f}"
        f"\n  permissive schema (every column declared str):        "
        f"{len(permissive_bad)} written, upper95="
        f"{clopper_pearson_upper(len(permissive_bad), discriminable_total):.4f}"
    )
    if permissive_bad:
        print(f"  admitted only because the premise was weak: {permissive_bad}")

    assert len(tight_bad) == 0
    assert len(tight_bad) <= len(permissive_bad), (
        "a tighter schema admitted more constraint-violating writes, which would mean the "
        "premise is being applied incorrectly"
    )
    # The finding this test exists to record: a str-only premise is not a premise.
    assert len(permissive_bad) > 0, (
        "a schema that constrains nothing blocked every attack anyway, which would "
        "contradict the documented limit that covering a column is not constraining it"
    )


def test_every_blocked_attack_has_an_honest_reason(source: Path) -> None:
    """A block with no reason, or the wrong reason, is not an honest refusal."""
    outcome = run_corpus(source, TIGHT_SCHEMA)
    corpus = {attack.name: attack for attack in build_corpus()}

    unexplained = [
        name for name, reason in outcome.block_reasons.items() if reason == "held_without_reason"
    ]
    assert unexplained == [], f"blocked with no reason: {unexplained}"

    mismatched: list[str] = []
    for name, reason in outcome.block_reasons.items():
        attack: Attack = corpus[name]
        if attack.honest_block_reasons and reason not in attack.honest_block_reasons:
            mismatched.append(
                f"{name}: got {reason!r}, expected one of {sorted(attack.honest_block_reasons)}"
            )
    assert mismatched == [], f"blocked for an unexpected reason: {mismatched}"


def test_the_table_shape_is_never_changed(source: Path) -> None:
    """A structural corruption is worse than a wrong cell.

    An injected newline or quote that changes the row or column count silently reinterprets
    every downstream read, so it is checked separately from value correctness.
    """
    for attack in build_corpus():
        write_table(source)
        if attack.pre_corrupt is not None:
            row, column, wrong = attack.pre_corrupt
            _poke(source, row, column, wrong)
        expected_lines = source.read_text(encoding="utf-8").splitlines()

        verify_and_apply(
            VerifyAndApplyRequest(
                source_path=source,
                fixes=[attack.fix],
                mode="apply",
                schema=TIGHT_SCHEMA,
                confirm_escalations=True,
                proposer="adversarial-corpus",
            )
        )
        actual_lines = source.read_text(encoding="utf-8").splitlines()
        assert len(actual_lines) == len(expected_lines), (
            f"{attack.name} changed the row count: {len(expected_lines)} -> {len(actual_lines)}"
        )
        for index, line in enumerate(actual_lines):
            assert line.count(",") == expected_lines[index].count(","), (
                f"{attack.name} changed the column count on line {index}"
            )
