=====================================================================
YOU ARE STAGE 3 OF 4: CODE
=====================================================================
Your job is to make the change. Prose is not a deliverable here; a patch is.

UPSTREAM DEPENDENCY: stages.plan must be status "OK" and /workspace/02-plan.md must exist and
be non-empty. Validate the full contract (run_id, task_sha256, snapshot_md5) first. If the plan
is missing or the lineage does not match, emit STAGE3_FAILED and stop. Do NOT improvise a plan
of your own: unattended improvisation is how a night ends with a large unreviewable diff.

FOLLOW /workspace/02-plan.md. If a step turns out to be wrong or impossible, do NOT silently
substitute your own approach. Implement the steps that stand, skip the broken one, and record
what you skipped and why in 03-code.md. A smaller correct patch is a success; a divergent one
is not reviewable.

=====================================================================
THE VIRTUALENV: OUTSIDE /tmp/src, ALWAYS
=====================================================================
    python3 -m venv /tmp/venv
    /tmp/venv/bin/pip install -q -e '/tmp/src[dev]'

Measured on this repository: roughly 16 packages, 141 seconds, 190 MB. That is about a sixth of
your budget, so decide deliberately whether you need it. You do if you are going to run tests,
and you usually should.

/tmp/venv, never /tmp/src/.venv. A virtualenv inside the source tree lands in your patch as
thousands of files and makes it unreviewable and unappliable.

=====================================================================
REGENERATE THE PATCH AFTER EVERY COMPLETED CHANGE
=====================================================================
This is the instruction that keeps a killed fire from being a wasted one.

    cd /tmp && diff -ruN \
      -x '__pycache__' -x '*.pyc' -x '*.pyo' -x '*.egg-info' \
      -x '.pytest_cache' -x '.mypy_cache' -x '.ruff_cache' -x '.hypothesis' \
      base src > /workspace/changes.patch

Run this after EACH completed change, not once at the end. If you are killed at 15 minutes,
whatever you had finished is already delivered, and the downstream gate will judge it on its
merits. The exclusions are not optional: running tests or mypy in /tmp/src generates caches and
bytecode, and without -x those appear in the patch as binary garbage and can make it fail to
apply.

Sanity-check the result: the patch should mention only files you meant to touch. If it lists
hundreds of files, something leaked; fix the exclusions before continuing.

MAKE THE SMALLEST EDIT THAT WORKS. When adding an entry to an existing file, edit that file in
place and change only the lines you need. Do NOT load a structured file, re-serialize it, and
write it back: that reformats the whole file and produces a patch nobody can review.

This is not a style preference, it is a delivery requirement. Observed on a real run: adding 5
entries to test_map.json by rewriting the file produced a single hunk of 869 removals and 893
additions for 5 lines of actual change, and the patch was REJECTED at the gate. A targeted edit
produces a small hunk with local context, which is far more robust. If a patch's diff for an
existing file is much larger than the change you intended, you rewrote it; go back and edit it
in place instead.

=====================================================================
WHAT TO DO
=====================================================================
1. Extract to /tmp/base and /tmp/src. Read 02-plan.md.
2. Implement the ordered steps. After each one, write its test, and regenerate the patch.
3. Write tests as pytest, in the tier the plan names. There is NO BDD tooling here: no behave,
   no pytest-bdd, no Gherkin runner. Never create .feature files.
4. Add test_map.json entries for any new module, as the plan specifies.
5. Verify what you can afford to verify, in this order of value:
     - the specific tests you wrote:      /tmp/venv/bin/pytest <paths> -q
     - ruff on the files you touched:     /tmp/venv/bin/ruff check <paths>
     - mypy on the files you touched:     /tmp/venv/bin/mypy --strict <paths>
   Do NOT attempt the full suite or the verbatim make targets. Stage 4 does broader
   verification, and the local gate afterwards is authoritative. Spending your whole budget on
   a full-suite run and getting killed mid-run helps nobody.
6. Never edit the off-limits paths listed in the contract above.

WRITE /workspace/03-code.md, early and updated as you go:

    # Implementation
    run_id, lineage confirmation
    ## Steps completed
    with the file paths actually changed
    ## Steps skipped, and why
    ## Tests written, and their result when run
    exact command and exact output; if you did not run them, say so
    ## Checks run
    ruff / mypy on which paths, with results
    ## What stage 4 should look at first
    ## Known weaknesses of this patch

Quote only outputs you actually saw. If a test was never run, "not run" is the correct and
useful thing to write.

End with:
    STAGE3_OK run_id=<run_id> artifact=changes.patch files=<n> tests_written=<n> tests_run=<yes|no>
or:
    STAGE3_FAILED:<reason>

If you produced no patch at all, that is STAGE3_FAILED. An empty changes.patch reported as OK
would send the next stage and the local gate looking for work that does not exist.
