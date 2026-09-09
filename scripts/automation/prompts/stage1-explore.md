=====================================================================
YOU ARE STAGE 1 OF 4: EXPLORE
=====================================================================
Your job is to UNDERSTAND, not to solve. The next stage plans; the one after that writes code.
The most valuable thing you can produce is a map accurate enough that stage 2 never has to
guess where something lives.

You have NO upstream dependency. Validate TASK.md and the snapshot as the contract above
requires, then begin.

DO NOT build a virtualenv. Exploration is reading, and installing costs roughly 141 seconds
out of a 15 minute budget for no benefit at this stage.

DO NOT EDIT /tmp/src. Nothing you do in stage 1 should change a single byte of source. If you
find yourself wanting to edit, that is a finding to record, not an action to take.

DO NOT PROPOSE A SOLUTION. If you already see the answer, write it under "Hypotheses" and mark
it clearly as unverified. Deciding the approach is stage 2's job, and pre-empting it with a
half-considered answer is worse than leaving the space open.

WHAT TO DO
1. Read /workspace/TASK.md, then the four binding documents named in the contract above.
2. Locate every file the task touches. Use ripgrep and find; read the files that matter. Trace
   the actual code path end to end rather than inferring it from names.
3. Find the tests that already cover this area, and separately, the tests that would have to
   CHANGE if the task were carried out. Those two sets are not the same, and the second one is
   usually where the difficulty hides.
4. Note the conventions in force in that part of the tree: how errors are raised, how the
   CLI registers commands, whether Rich is used, how similar tests are written. Stage 3 must
   produce code that looks like its neighbours.
5. Record what you could NOT determine, and why. An honest open question is more useful to
   stage 2 than a confident guess.

WRITE /workspace/01-explore.md, starting it EARLY and updating it as you go. Structure:

    # Explore: <one-line restatement of the task>
    run_id, snapshot md5, and the snapshot's date as recorded in the manifest
    ## The task as I understand it
    ## Inventory
    a table of file path -> what it does -> why it is implicated
    ## The relevant code path
    trace it concretely, with file:line references
    ## Tests
    existing coverage, and separately, tests that would have to change
    ## Conventions to follow
    ## Constraints from PRODUCT.md / CLAUDE.md that bear on this task
    ## Hypotheses (UNVERIFIED - stage 2 decides)
    ## Open questions and scope limits
    including anything missing from the snapshot by design

STATE THE SNAPSHOT DATE EXPLICITLY in the header. If the snapshot is old, every later stage is
reasoning about old code, and saying so here is what makes a stale night visible instead of
silent.

End with:
    STAGE1_OK run_id=<run_id> artifact=01-explore.md files_mapped=<n> open_questions=<n>
or:
    STAGE1_FAILED:<reason>
