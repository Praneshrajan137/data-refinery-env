# Apply rewrites every line ending: reversible, but not reviewable

**Status**: open, recorded 2026-08-26. Found while replacing a concurrency claim that had been
prose for weeks with tests that assert outcomes. The first race test failed on this, not on the
race.

## What was measured

Applying a **one-cell** repair to a CRLF-delimited CSV rewrites **every line ending in the file**.

On `dataforge/fixtures/premised_fd_10rows.csv` — 11 lines, one provable `fd_violation` on row 10:

| Quantity | Before apply | After apply |
| --- | --- | --- |
| CRLF terminators | 11 | 0 |
| LF terminators | 0 | 11 |
| File size (bytes) | 147 | 135 |
| Bytes removed | — | 12 |
| Lines re-terminated | — | 11 |
| Cells changed | — | 1 |

So changing one cell modified 11 lines and 12 bytes. The cause is that the apply path
**re-serialises the table** rather than patching bytes in place, and the serialiser writes `\n`
regardless of the input dialect. Nothing in the product records the input's line-ending dialect, so
nothing could restore it.

## The control arm, which narrows the claim

An **LF** source has **0** lines re-terminated by the same repair.

That number is what makes the finding precise. Without it, the CRLF result is equally consistent
with a much larger claim — that apply rewrites every line unconditionally — which would be a
different defect with a different fix. It does not. The defect is dialect **conversion**, and it is
reachable only by users whose CSVs are CRLF-delimited, which on Windows is the default.

Both dialects change exactly **1** cell, and both revert to byte identity.

Reproduce with:

```
python scripts/bench/measure_line_ending_rewrite.py --artifact eval/results/line_ending_rewrite.json
```

The script normalises the fixture to each dialect before measuring, so the developer's git
`autocrlf` setting cannot decide the result. A measurement that depends on a local config is not a
measurement.

Reproduced by `tests/integration/test_concurrent_apply.py::TestApplyRewritesEveryLineEnding`, which
pins each number above.

## What this does NOT say

**It is not a data-safety defect, and the tests prove that rather than assuming it.**

- Revert restores **byte identity**, including the original CRLF terminators, because the snapshot
  is the pre-apply bytes and not a re-serialisation of them
  (`test_reversibility_still_holds_despite_the_rewrite`).
- The journal hash chain audits `verified` afterwards.
- No cell value other than the repaired one changes.

The reversibility floor holds. This page is not a retraction of it.

## What it costs, and why that matters here

It costs **human review**, which is the product.

The boundary DataForge sells is the line between a change that may be applied unsupervised and one
a human must look at. When a human does look, the artifact they look at is a diff. A reviewer asking
"what did DataForge change?" on a CRLF file sees *every line modified* and cannot see the one line
that matters. The signal is not lost — it is buried at a ratio of 11 to 1 on an 11-line file, and
the ratio grows with the table.

It is also **collateral modification of rows that were already correct**. This project measures that
unconditionally everywhere else: `PRODUCT.md` §1.3 records that conditional precision cannot show a
write path to be safe, because the failure that costs a user data is not in its denominator. Eleven
correct rows being rewritten to fix one cell is the same shape of quantity, on a different axis.

## What this authorises

Nothing. No behaviour changes on the strength of this page.

## What it does not authorise

- Reading "reversible" as "safe to apply unsupervised on a CRLF source". Reversibility is what makes
  the rewrite *tolerable*, not what makes the resulting diff *reviewable*.
- Quoting the 11-line, 12-byte figures as a general magnitude. They are one fixture. The count of
  rewritten lines is the row count of whatever table is repaired, which is the point: the collateral
  scales with the table while the repair does not.

## Why it was not fixed in the session that found it

Preserving the input dialect is a change to the **write path**, and the write path has four
surfaces. The serialised form is consumed by the pre-apply snapshot, the post-apply SHA-256 in the
receipt, the patch plan, and the warehouse dry-run contract. A fix that preserved CRLF would change
the bytes every one of those hashes, so it needs its own measurement and its own regression guard,
not an opportunistic edit inside a concurrency commit.

`docs/trust/write-surface-uniformity.md` records what happened the last time a write-path invariant
was assumed uniform across those four surfaces: it held on two of them for four weeks while a
schema-less LLM value was written to a user's file and reported as SMT-verified. That is the
precedent for declining to touch this here.

## What would close it

Either of these, measured rather than asserted:

1. Record the source dialect at detection time and re-emit it, with a test that a CRLF source is
   byte-identical outside the repaired cell — the same shape of assertion as the revert test, but
   applied to the *forward* direction.
2. Patch the changed cell in place instead of re-serialising, which removes the whole class rather
   than this instance of it. Larger, and it must not weaken the atomic-replace guarantee in
   `dataforge/transactions/files.py::atomic_write_bytes`.

Until one of those ships, the honest statement to a user reviewing an applied diff on a CRLF file is
that the line-ending change is expected, is reversible, and is not a repair.
