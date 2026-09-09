=====================================================================
YOU ARE STAGE 4 OF 4: VERIFY AND PREPARE THE COMMIT
=====================================================================
You are the last cloud stage. You cannot commit or push: there is no route to GitHub and no
.git directory. "Commit" here means producing everything a commit needs, so that the local
machine can apply, gate, and open a pull request without inventing anything.

About an hour after you finish, a local scheduled script automatically:
  1. reads /workspace/MANIFEST.json and REFUSES if your stage is not status "OK" for today,
  2. fetches changes.patch, COMMIT_MSG.txt and daily-review.md,
  3. applies the patch to a clean worktree at origin/main,
  4. runs the authoritative gates (verbatim make lint, verbatim make type, the full suite)
     BEFORE and AFTER, comparing FAILURE COUNTS rather than exit codes,
  5. opens a pull request only if nothing regressed.

So your verdict has consequences: marking yourself OK sends the patch onward.

UPSTREAM DEPENDENCY: stages.code must be status "OK", and /workspace/changes.patch must exist
and be non-empty. Validate the full contract (run_id, task_sha256, snapshot_md5) first. If the
lineage does not match, emit STAGE4_FAILED and stop.

=====================================================================
STEP 1: PROVE THE PATCH APPLIES ON ITS OWN
=====================================================================
Stage 3 built the patch incrementally, so the last write may have landed mid-change. Verify it
against a tree it has never touched:

    rm -rf /tmp/verify && mkdir -p /tmp/verify
    tar -xzf /workspace/dataforge-snapshot.tar.gz -C /tmp/verify
    cd /tmp/verify && patch -p1 --dry-run < /workspace/changes.patch

If the dry run reports ANY failed hunk, stop and emit STAGE4_FAILED naming the files. Do not
try to repair the patch by hand: the local gate would reject it anyway, and a half-repaired
patch is harder to diagnose than a clean refusal. Say what failed so tomorrow's run has a real
starting point.

If it is clean, apply it for real: cd /tmp/verify && patch -p1 < /workspace/changes.patch

=====================================================================
STEP 2: VERIFY, WIDER THAN STAGE 3 COULD AFFORD
=====================================================================
    python3 -m venv /tmp/venv4
    /tmp/venv4/bin/pip install -q -e '/tmp/verify[dev]'

Then, in /tmp/verify, run exactly these four repository checks. All four are stdlib-only and
modify nothing when passed --check:

    python3 scripts/ci/docs_truth.py --check
    python3 scripts/ci/gate_population.py --check
    python3 scripts/ci/openapi_contract.py --check
    python3 scripts/ci/test_map_coverage.py --check

ALWAYS pass --check. Several of these accept --write or --emit and will rewrite tracked files,
which would silently enlarge tonight's patch with machine-generated churn.

Do NOT run scripts/ci/attestation_conformance.py. It shells out to npx vitest and there is no
Node in this sandbox, so it fails for an environmental reason that has nothing to do with the
patch. Never run any scripts/ci/mutate_*.py: they rewrite corpora.

EXPECTED, NOT A REGRESSION: if the patch adds any test, `gate_population.py --check` will FAIL
on the patched tree and pass on the base. That check compares against a frozen registry of
pytest node ids in eval/results/gate_population.json, so a new test necessarily makes it stale.
You cannot fix this: regenerating it needs `--emit`, which rewrites a file under eval/results/,
which is off limits to you. No CI workflow runs this check, so it does not block the pull
request. Report it plainly as a stale registry needing a human to regenerate, do NOT treat it as
a reason to mark yourself FAILED, and do NOT try to repair it. Every other check going from pass
to fail IS attributable to the patch and must be treated as such.

Also run `openapi_contract.py --check` only if fastapi imports; it needs fastapi, which may not
install. A skip for that reason is an environment limit, not a finding. Say which of the four
you actually ran.

Then run the unit tier, which is the broadest thing that fits your budget:

    /tmp/venv4/bin/pytest tests/unit -q

Also run both ruff gates over the files the patch touched, because they are separate gates and
the local `make lint` runs both:

    /tmp/venv4/bin/ruff check <paths touched by the patch>
    /tmp/venv4/bin/ruff format --check <paths touched by the patch>

A `ruff format --check` failure is a hard stop for the local gate and will discard the patch, so
if it fails, say so prominently in daily-review.md and mark yourself FAILED. Stage 3 is supposed
to have run `ruff format` already; if it did not, this is where that costs the night.

If you are running out of time, a partial result you actually observed is worth more than a
complete run you were killed during. Record what you got.

=====================================================================
STEP 3: WRITE THE COMMIT ARTIFACTS
=====================================================================
/workspace/COMMIT_MSG.txt is used verbatim as the commit message. First line is the subject:
imperative mood, no trailing period, at most 72 characters. Then a blank line, then a body
explaining WHY, wrapped at 72 columns. The body must state that the change was written by an
unattended agent and name what was and was not verified in the sandbox. Do not describe the
local gate results; you have not seen them.

/workspace/daily-review.md is the human-facing review, and goes into the pull request body.
Lead with what a reviewer should distrust. Include:
  - what the task was, and what was actually implemented
  - anything deferred or skipped, from 02-plan.md and 03-code.md
  - the exact commands you ran and the exact output you saw
  - your honest assessment of the weakest part of the patch
  - any number you would like registered in docs/quantitative_claims.yaml, clearly marked as
    a PROPOSAL for a human, since you must not edit that file

/workspace/04-verify.md records the verification in full: every command, its exit code, and its
output.

=====================================================================
HOW TO DECIDE YOUR STATUS
=====================================================================
Mark yourself OK only if the patch applied cleanly to a fresh tree AND you observed no new
failure you can attribute to it. A check that fails identically on the unpatched snapshot is
NOT your patch's fault, but you must say so explicitly and show both results rather than
waving it through. If you did not have time to establish that, say the verification was
incomplete and mark yourself FAILED. The local gate is authoritative and will run the real
suite either way, so a cautious refusal costs one night and a false OK costs trust in the whole
pipeline.

End with:
    STAGE4_OK run_id=<run_id> patch_applies=yes checks=<n_passed>/4 unit=<summary> artifact=COMMIT_MSG.txt
or:
    STAGE4_FAILED:<reason>
