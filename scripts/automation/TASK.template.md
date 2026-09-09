# Task for the nightly pipeline

Replace this file's contents with tonight's task, then publish it with:

    powershell -NoProfile -File scripts\automation\publish_run_inputs.ps1 -TaskFile <path>

Everything below is read by stage 1 as the sole definition of the work. The four stage prompts
never change; this file is the only input that does.

## How to write a good task here

The pipeline gets roughly 15 minutes per stage. That is the binding constraint, and it should
shape what you ask for.

- Scope it to something one focused change can accomplish. "Add a --json flag to
  `dataforge repair`" works. "Improve the repair engine" does not, and will produce four
  stages of hedging.
- Say what DONE looks like, concretely enough that stage 4 can check it.
- Name the files you already know are involved. Stage 1 will still explore, but a starting
  point saves budget that is better spent on the parts you did not anticipate.
- State anything out of scope. An unattended agent that notices an adjacent problem will be
  tempted to fix it, and the resulting diff is harder to review.
- If you want a specific approach, say so and say why. Stage 2 will otherwise choose for
  itself, and it is choosing without you in the room.

## Template

### Task
<one sentence: what should be true after this change that is not true now>

### Why
<the motivation, so stage 2 can weigh alternatives against the actual goal>

### Definition of done
- <observable condition>
- <the test that proves it>

### Known starting points
- <path/to/file.py> - <why you think it is involved>

### Out of scope
- <the adjacent thing you do NOT want touched tonight>

### Constraints or preferred approach
<optional; leave empty to let stage 2 decide>
