# Cloud automation: sandbox operating notes

Operational notes for the scheduled Cortex Code automation that reviews this
repository. **This file is about the execution environment, not about DataForge.**
It is deliberately kept out of `CLAUDE.md`: that file is auto-injected as
instructions into every editor session, so environment-specific detail for one
non-local execution surface does not belong there.

Nothing here is product evidence. Nothing here has the standing of anything under
[`docs/trust/`](../trust/).

> **Volatile figures below carry the date they were measured.** `scripts/ci/docs_truth.py`
> is an *allowlist* over `docs/quantitative_claims.yaml`, so it cannot police a number in a
> file it does not know about. An undated number here would rot silently, which is the exact
> failure mode that checker exists to prevent. Re-measure before relying on any of them.

## What this is

A Snowflake `AGENT TASK` runs Cortex Code unattended, on a schedule, in a
Snowflake-managed sandbox. Each fire runs as the creating user. There is no human
available, no clarifying turn, and permission gating is off, so destructive tools
run unprompted.

The fire plans, implements code changes, and writes two files into the user workspace:
`changes.patch` and `daily-review.md`. It cannot commit, push, or open a pull request. A
local script gates the patch and opens the PR. See "The two-stage pipeline" below.

## The two-stage pipeline

The fire cannot reach GitHub, so implementation and delivery are split. Stage two is
[`scripts/automation/apply_and_pr.ps1`](../../scripts/automation/apply_and_pr.ps1).

```
FIRE (sandbox)                          LOCAL (this machine)
  extract snapshot twice                  GET changes.patch + daily-review.md
  /tmp/base pristine, /tmp/src edited     git worktree at origin/main  (LF!)
  plan, implement, add tests              gates BEFORE  -> baseline
  run the 4 permitted checks              git apply --3way -p1
  diff -ruN base src > changes.patch      gates AFTER
  write daily-review.md                   no regression? push branch + gh pr create
```

Four properties of stage two that are deliberate, not incidental:

1. **It works in a throwaway `git worktree` at `origin/main`, never in the developer
   checkout.** The checkout routinely carries unrelated in-flight work, so applying a patch
   there would make gate failures unattributable and could corrupt someone else's changes.
2. **The PR condition is no regression against a baseline, not an absolute pass.** `main`
   may already be red, and a shared virtualenv can distort results; measuring before and
   after in the same worktree cancels both, since whatever distorts one run distorts the
   other identically. A gate already failing at baseline does not block.
3. **Protected paths are enforced in the script, not just requested in the prompt.** A patch
   touching `PRODUCT.md`, `DECISIONS.md`, `CLAUDE.md`, `docs/quantitative_claims.yaml`,
   `docs/trust/**` or `eval/results/**` is a hard stop, not a regression to be weighed.
4. **It never merges, never pushes to `main`, and never tags.** A pull request cannot land
   by itself, so human review stays the last gate. This matters more than usual because the
   fire **cannot run the test suite**, so its code arrives unverified by its author.

### Line endings will silently break this if you change the worktree setup

`core.autocrlf = true` on this machine and there is no `.gitattributes`, so a normal
checkout produces **CRLF** files. The snapshot comes from `git archive`, which emits the
**LF** blob content, so a patch generated in the Linux sandbox carries LF context lines.
Applying an LF patch to a CRLF worktree fails every hunk with `patch does not apply`, which
reads like a corrupt patch and sends you looking in the wrong place.

The worktree is therefore created with `git -c core.autocrlf=false worktree add`, and the
script asserts the result really is LF before going further.

### Two git behaviours the script depends on

- **`git apply --3way` stages what it applies.** `git diff --name-only` shows only unstaged
   changes and so reports nothing, making a successful patch look like a no-op. Use
   `git diff HEAD --name-only`.
- **`--3way` prints `repository lacks the necessary blob to perform 3-way merge` for a
   `diff -ruN` patch**, which carries no index lines, then falls back to direct application
   and succeeds. That message is noise, not failure. Judge by the exit code.

### Running it

It is scheduled, so normally you run nothing. To invoke it by hand:

```powershell
powershell -NoProfile -File scripts/automation/apply_and_pr.ps1
powershell -NoProfile -File scripts/automation/apply_and_pr.ps1 -DryRun -SkipTests -PatchFile C:\tmp\x.patch
```

`pwsh` (PowerShell 7) is **not** installed here; use `powershell`.

Exit codes are meaningful and distinct, which matters because two of them look alike from a
distance:

| Exit | Meaning |
| --- | --- |
| 0 | A PR was opened, **or** retrieval worked and there was genuinely nothing to pick up |
| 1 | Blocked: patch did not apply, touched a protected path, or regressed a gate. No PR |
| 2 | **Retrieval failed.** The workspace could not be reached, so we know *nothing* |

Exit 2 must never be read as "the fire implemented nothing". An earlier version of this
script conflated those: `cortex sql` does not exist, so the retrieval silently did nothing and
the script exited **0** reporting that the fire had implemented nothing. That is a vacuous
success, the exact failure this pipeline exists to prevent, and it survived three tests
because all of them passed `-PatchFile` and never exercised retrieval. A second, subtler
version of the same trap: `GET` on a file that does not exist also exits 1, so a missing
patch and a broken connection are indistinguishable by `GET` alone. The script therefore
**lists the stage first** and only fetches when the file is actually present.

### Headless setup (why a 3 AM run works at all)

The scheduled run happens while nobody is logged in and attending, so every part of it must
work without a browser. Three pieces make that true:

1. **Snowflake CLI in its own venv** at
   `%LOCALAPPDATA%\dataforge-automation\venv` (Python 3.12, `snowflake-cli` 3.26.0).
   Deliberately **not** the repo `.venv`: a dependency added there would diverge from
   `pyproject.toml` and `uv.lock` and could perturb the long, exact file lists in
   `make lint` and `make type`.
2. **RSA key-pair auth.** The interactive profile uses
   `authenticator = "oauth_authorization_code"`, which needs a browser once its cached token
   expires — fatal at 03:00. A separate `[dataforge_automation]` profile in
   `~/.snowflake/connections.toml` uses `SNOWFLAKE_JWT` with an unencrypted PKCS#8 key at
   `%USERPROFILE%\.snowflake\keys\dataforge_automation_rsa.p8`, ACL-restricted to the
   current user. `[AEGIS15]` is left untouched for interactive use.
3. **`snow sql` accepts a `snow://workspace/...` URI** in both `LS` and `GET`. This was the
   one genuine unknown in the design; verified 2026-09-02 by round-tripping an 8,580,592-byte
   file byte-identically (md5 `7AF9711DFEEC3D3936C4E96EF30AF173`) with no browser.

**The security tradeoff, stated plainly.** The user holding this key has
`default_role = ACCOUNTADMIN` and `has_mfa = true`. Key-pair auth presents no MFA challenge,
and a headless key cannot have a passphrase, so an unencrypted file on disk grants
account-admin access and bypasses MFA. The obvious mitigation — a dedicated low-privilege
service user — **is not possible here**: the patch is delivered into
`USER$PRANESH07.PUBLIC.DEFAULT$`, a *personal* database that cannot be granted to another
role, and the fire can only write to its own `/workspace` mount. Retrieval must therefore run
as that user. The blast radius is inherent to the delivery channel. If that is unacceptable,
the honest options are to drop the schedule and pick patches up interactively, or to add a
network policy restricting where the key may be used from.

### The scheduled task

`DataForge automation pickup`, daily at 03:00 local (IST), 2.5 hours after the fire.

| Setting | Value | Why |
| --- | --- | --- |
| Action | `powershell.exe -File %LOCALAPPDATA%\dataforge-automation\run_pickup.ps1` | A wrapper that owns the log file |
| `StartWhenAvailable` | true | A laptop that was off or asleep runs on next wake instead of skipping the day |
| `WakeToRun` | true | Best effort only: **Windows ignores wake timers on battery**, so on battery it runs late rather than at 03:00 |
| `RunLevel` | Limited | Nothing here needs elevation |
| `ExecutionTimeLimit` | 2 hours | `make test` is two full pytest passes |
| `MultipleInstances` | IgnoreNew | A long run must not overlap the next day's |

**The laptop must be powered on** (or able to wake). Task Scheduler cannot run on a
powered-off machine; with `StartWhenAvailable` the run is deferred, not lost. Cortex Code
itself does **not** need to be open.

Logs land in `%LOCALAPPDATA%\dataforge-automation\logs\pickup-YYYY-MM-DD.log`, pruned after
30 days, each ending with a line that spells out what the exit code meant. Log naming lives
in the PowerShell wrapper because two `.cmd` attempts produced `pickup-20269-02.log` and then
`pickup-+.log`: `%DATE%` substring parsing is locale-dependent and `for /f` quoting does not
survive being generated programmatically.

Inspect or re-run it with:

```powershell
Get-ScheduledTaskInfo -TaskName 'DataForge automation pickup'   # LastTaskResult, NextRunTime
Start-ScheduledTask   -TaskName 'DataForge automation pickup'   # run now
```


## Inventory, as of 2026-09-01

Everything lives in account **`YCJETJE-SK82196`** (user `PRANESH07`). It is invisible
from any other account.

| Object | Notes |
| --- | --- |
| `USER$PRANESH07.PUBLIC.COCO_ROUTINE_PROJECT` | The `AGENT TASK`. State `started`, `USING CRON 30 0 * * * Asia/Calcutta` (00:30 IST). |
| `USER$PRANESH07.PUBLIC.DEFAULT$` | Workspace mounted at `/workspace` in the fire. Carries the source snapshot in, and the review out. |
| `INTEGRATIONS.PUBLIC.GITHUB_PAT` | `TYPE = PASSWORD`, `USERNAME = 'git'`. Injected into the fire as `$GH_TOKEN`. Currently unusable by the fire, since GitHub is unreachable. |
| `INTEGRATIONS.PUBLIC.DATAFORGE_REPO` | Git mirror of this repo. Readable from a normal session, **not** readable from a fire. |

`SHOW TASKS` does **not** list agent tasks and will return nothing. Use:

```sql
SHOW AGENT TASKS IN ACCOUNT;
SELECT RIGHT("definition", 900) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

The second query exposes the sandbox configuration block, which is where the egress
and mount facts below were established.

## Hard environment constraints

These were each established by a failed run, not by inference. Changing the prompt to
contradict any of them will reproduce a known failure.

### `/workspace` is a stage mount, not a filesystem

`git clone` or `git init` under `/workspace` dies with
`could not write config file ... Input/output error`, leaving a `.git/info/` skeleton
behind. Work under `/tmp/src`.

Avoid `/tmp/dataforge` specifically: a read-only mount has been observed at that path.

**This failure precedes any network syscall.** A clone failing here is *not* evidence
that egress is blocked. Conflating the two sent one investigation to entirely the wrong
conclusion.

### GitHub is unreachable, and this is not a permissions problem

Tested twice, the second time with a correct tokenized clone from `/tmp`: the proxy
returns `empty reply from server` for `github.com`, while `pypi.org` returns 200 through
the same proxy. Only `github.com` is even nominally allowlisted, so `api.github.com`,
`raw.githubusercontent.com`, `codeload.github.com` and the `gh` CLI are all unavailable
and probing them proves nothing.

Note the platform contradiction: the task payload contains
`"AllowEgressToGithubAndDbt": true` while the proxy blocks GitHub. That is worth raising
with Snowflake support; it is not a misconfiguration on this side.

Consequences:

- **A fire cannot push.** `/workspace` is the only channel out.
- Network egress is enforced by the sandbox proxy. **No Snowflake role, grant, or
  Restricted Session Scope affects it.** Granting write access to every database in the
  account would not change this.

### Snowflake access from a fire is `RUNTIME_MANAGED`, and cannot be widened here

`RUNTIME_MANAGED` does not include `READ` on `INTEGRATIONS.PUBLIC.DATAFORGE_REPO`, which
is why the git mirror is useless to a fire even though `LS` works and its content is
already fetched. This is **not** a missing grant, so `GRANT READ` will not fix it.

It also cannot be widened from this setup: `SHOW RESTRICTED SESSION SCOPES` is empty,
`RUNTIME_MANAGED` is applied by the platform rather than from the task payload, the
Snowsight Automations dialog exposes no control for it, and the
`--with-restricted-session-scope` / `--without-read-only` flags belong to the
`cortex automation` CLI, which fails locally with *"Could not confirm whether automations
are enabled for this account (the Cortex Agent endpoint was unreachable)"* — including
after automations demonstrably ran, so it is a local endpoint fault rather than an account
capability. A review needs no DML, so this is moot in practice.

## Moving files in and out of the workspace

**`cortex ws cp` is broken on Windows in both directions.** Upload fails with
`undefined is not a directory` for every source form; download fails with a mangled
doubled drive letter, `stat 'C:\C:\Users\...'`. `cortex ws ls` and `cortex ws rm` work
because they never touch a local path.

Use SQL instead:

```sql
-- upload
PUT 'file://C:/path/to/dataforge-snapshot.tar.gz'
    'snow://workspace/USER$PRANESH07.PUBLIC.DEFAULT$/versions/live/'
    AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- download
GET 'snow://workspace/USER$PRANESH07.PUBLIC.DEFAULT$/versions/live/daily-review.md'
    'file://C:/dev/dataforge/docs/automation/';
```

Path mapping: `/workspace/X` inside a fire is `/versions/live/X` in `cortex ws ls` output.

**`cortex ws ls` misreports size and md5.** A 2,588,508-byte upload was listed as
2,588,512 with an unrelated md5, yet a `PUT` then `GET` round trip returned a
byte-identical file that `tar -tzf` read cleanly. Verify integrity by round trip, never
by comparing against the listed md5.

## The source snapshot

The fire has no checkout and cannot clone, so the source is staged as a tarball in the
workspace at `/workspace/dataforge-snapshot.tar.gz`. It has no top-level wrapper
directory, so extract with `-C /tmp/src`.

Build it from `git archive HEAD`, not from the working tree, so that in-flight
uncommitted work is never shipped into a review.

Included: `dataforge`, `docs`, `tests`, `scripts`, `specs`, `packages`, `dataforge-mcp`,
`playground`, `training`, `constitutions`, `requirements`, `fixtures`,
`benchmark_results`, `eval/thresholds`, `eval/preregistration`, **`eval/results`**,
`.github` (so CI config is reviewable), the root markdown, `pyproject.toml`, `Makefile`,
`uv.lock`, **`test_map.json`**.

Excluded deliberately, none of it source: `data/` (309.8 MB of datasets as of 2026-09-01),
`node_modules`, caches, `*.pyc`, and `training/kaggle_dataset_v3/expert_v3.jsonl` (11.9 MB
frozen curriculum).

### The gates need their input artifacts, and omitting them manufactures false findings

This is the most expensive mistake made so far, so it is worth stating at length.

An earlier snapshot excluded `eval/results` on the belief that it was 313.6 MB of archived
run output. **That figure was the working-tree total including untracked files. Tracked is
30.2 MB** across 862 JSON files, as measured 2026-09-01. The exclusion decision rested on a
wrong number.

The consequence was not a missing directory but a **fabricated finding**.
`docs/quantitative_claims.yaml` holds **106 artifact references, all pointing into
`eval/results`**, resolving to 24 distinct files totalling 0.75 MB. With them absent,
`docs_truth.py` reported almost every claim as "artifact does not exist", and the fire
correctly observed that real disagreement had become indistinguishable from unbuilt
evidence — then reasonably raised it as a defect, and asked whether `eval/results` was
version-controlled at all. It is: **908 tracked files.** The defect was in the snapshot, not
the repository.

So: a check the fire cannot pass for environmental reasons does not merely fail to inform,
it actively produces false conclusions that cost a human cycle to refute. Either give a
check everything it needs, or remove it from the permitted list and say why.

The inputs the four permitted checks read, all now present:

| Check | Reads |
| --- | --- |
| `docs_truth.py` | `docs/quantitative_claims.yaml` plus its 24 artifacts under `eval/results` |
| `gate_population.py` | `eval/results/gate_population.json` |
| `openapi_contract.py` | `specs/openapi/*.openapi.json` |
| `test_map_coverage.py` | root `test_map.json` |

Verify after any snapshot change by extracting the tarball to a scratch directory and
running all four against it. As of 2026-09-01 all four exit 0 that way, with `docs_truth`
verifying 106 claims and `gate_population` reporting 2724 pytest node ids.

`playground/` is only **139 source files / 3.7 MB** once `node_modules` is excluded, as
measured 2026-09-01. Its 10,629-file raw total caused an earlier snapshot to skip it
entirely, which left the fire unable to import 10 modules. Judge directories by source
content, not by file count.

**The snapshot contains no `.git`**, so a fire using it cannot read history. It is a
point-in-time copy and **nothing refreshes it automatically** — rebuild and re-upload it
when the repository has moved on, or reviews will describe stale code and patches will stop
applying to `origin/main`.

## The work budget, and why it exists

One run exhausted its entire budget on `pip install` plus the test suite, and produced no
review.

That was caused by a prompt defect, not by the fire misbehaving: the prompt required that
no number be stated unless the fire measured it in that run, which left no option but to
make the tree executable — and the snapshot of the day was missing `training/` and
`playground/`, so it could not import 10 modules after paying the install cost.

**If a prompt forbids installs, it must also state what remains measurable.** Otherwise the
constraint is incoherent and consumes the run.

### Installing is affordable, and the ban that followed was wrong

The ban was justified by naming `pyarrow`, `networkx`, `causal-learn` and `hyppo` as part of
the dependency tree. **They are optional extras** (`bench`, `causal`, `pandas`), as are
`torch`/`transformers`/`trl` (`train`). Core is **11 packages**. Measured 2026-09-02 in a
clean 3.12 venv:

| Step | Cost |
| --- | --- |
| 16 packages (11 core + pytest, pytest-xdist, hypothesis, jsonschema, psutil) | 141 s, 190 MB |
| `pytest tests/unit` (2373 tests) | 111 s |
| full `pytest tests/ -n logical` (2724 collected) | 32 s |

So a fully TDD-capable environment costs about four and a half minutes. A fire is now told to
build one, because red-green TDD is not possible without it and a fix with no executed test is
not an implemented fix.

Two things a fire must get right when it does:

- **Repoint the package after installing deps**: `pip install -e /tmp/src --no-deps`. Without
  it `import dataforge` can resolve elsewhere and the tests exercise the wrong tree. Verify
  from a directory *other* than `/tmp/src`, since Python resolves an uninstalled package from
  the current directory and the check would pass regardless.
- **Never combine `-x` with `-n`.** On this suite that produces spurious collection errors and
  aborts the session, so everything after the abort goes untested. See the gate note below.


**Six** scripts under `scripts/ci/` import only the standard library, derived by AST on
2026-09-01 and enforced by `tests/unit/test_sandbox_measurement_floor.py` so the set cannot
rot unnoticed. Of those, exactly **two** are read-only reporters worth running unattended:

| Zero-install command | Reports |
| --- | --- |
| `python scripts/ci/test_map_coverage.py --check` | modules carrying a mapping decision |

**`attestation_conformance.py` is NOT in the permitted list**, even though it is stdlib-only.
It shells out to `npx vitest`, and the sandbox has neither Node nor `node_modules` (the
latter is excluded from the snapshot deliberately). It therefore always fails there for
purely environmental reasons. Verified 2026-09-01 against the extracted snapshot: the other
four exit 0, this one exits 1 with `CONFORMANCE FAILED: ['typescript']`. Handing it to a fire
would manufacture another false finding, exactly as the `eval/results` omission did.

The other four stdlib-only scripts are `installed_cli_smoke.py` and the three
`mutate_*.py`, which rewrite corpora or need an installed console script. Do not run them
unattended.

> **This list previously named five commands and claimed ten were stdlib-only. Both figures
> were wrong, and the error is instructive.** `docs_truth.py` imports `yaml` (PyYAML),
> `gate_population.py` imports `scripts.ci.readme_truth` and `scripts.ci.backend_gate` and
> so transitively needs `dataforge`, `httpx` and `yaml`, and `openapi_contract.py` imports
> `dataforge.env.server` and `playground.api.app`. Three of the five commands offered as
> needing no installs need the package installed.
>
> The note said they were "verified on 2026-09-01 to exit 0", and they were — **in a full
> virtualenv, which is an environment where a zero-install claim cannot fail.** That is the
> same error as treating a green local gate as evidence about CI. A claim about a
> constrained environment has to be checked under that constraint, or derived from source;
> this one is now derived.

If you need the claim-count or contract gates, they are still the right tools — but they
belong after an install step, and the budget has to pay for it.

**Always pass `--check`.** `docs_truth.py` and `openapi_contract.py` accept `--write`, and
`gate_population.py` accepts `--emit`; all three rewrite tracked files. Never run
`scripts/ci/mutate_*.py` unattended — they rewrite corpora.

### Two defects that made the gate weaker than its own PR body claimed

Both found 2026-09-02, both mine, and the second had already let a real regression through in
testing.

**1. The gates were an approximation.** The script ran `ruff check dataforge tests scripts/ci`
and `mypy --strict dataforge` while the PR body said "make lint / make type / make test". Real
`make lint` is six commands over a longer path list; real `make type` covers 187 files. GNU
Make 4.4.1 is installed, so the approximation bought nothing. It now runs `make lint` and
`make type` verbatim, with `PYTHON=` overridden because the Makefile prefers
`.venv/Scripts/python.exe` and the throwaway worktree has none.

**2. The tests were exercising the wrong source tree.**
`.venv/Lib/site-packages/__editable__.dataforge_07-0.1.0.pth` installs a finder that
**hardcodes `C:\dev\dataforge`**. Using the repo venv, `import dataforge` resolves to the main
checkout no matter the cwd, so pytest would collect the worktree's test files while running the
main checkout's code. A patch breaking runtime behaviour would be invisible; only ruff and
mypy, being file-based, saw the patch at all. Baseline-vs-after does **not** rescue this —
both runs would exercise identical unpatched source.

Fixed with a persistent gate venv at `%LOCALAPPDATA%\dataforge-automation\gatevenv` carrying
the repo's `[dev,playground]` extras, repointed each run via
`pip install -e <worktree>[dev,playground]`, plus an assertion that `dataforge.__file__` really
is under the worktree. The assertion is the load-bearing part; without it the gate silently
tests whatever tree the finder happens to reference.

### The test gate compares failure COUNTS, not exit codes

This is not fussiness. The suite is **flaky in a worktree under `-n logical`**: an unmodified
`origin/main` produced `2715 passed, 12 errors` on one run and `2715 passed, 0 errors` on the
next. Because "a gate already failing at baseline does not block", a flaky-red baseline made
new breakage invisible.

Demonstrated, not theorised. A patch inverting one branch of `SafetyFilter.confirms` — type
correct, lint clean — produced **2 genuine test failures** and was **waved through** by
exit-code comparison, because baseline and after were both exit 1. Comparing failure counts
(`0 -> 2 failed`) blocks it correctly.

For the same reason the gate does not run `make test` verbatim: that target uses `pytest -x`,
and `-x` under `-n` both triggers the spurious errors and aborts the session, leaving
everything after the abort unrun. Running the whole suite without `-x` is more stable and more
informative. This is a deliberate, documented deviation from the Makefile target.

One accepted caveat: the gate venv resolves tool versions within the repo's declared pins
(e.g. `ruff>=0.16.2,<0.17`) and may differ by a patch version from CI's resolution. Since
baseline and after share the venv, that cannot manufacture a false regression, and the PR still
faces real CI.


## Prompt invariants

Beyond the environment facts above, an unattended prompt must:

1. State that it runs unattended and must not ask clarifying questions.
2. Pre-resolve every name to an identifier, so no fire stalls on "which one did you mean".
3. Forbid inventing numbers, and say explicitly that an unregistered figure passes CI
   silently because `docs_truth` is an allowlist.
4. Forbid modifying `PRODUCT.md`, `DECISIONS.md`, `CLAUDE.md`, anything under
   `docs/trust/`, and `docs/quantitative_claims.yaml`.
5. Instruct that if the source could not be obtained, **no review is written**. A review
   assembled from filenames and byte counts is fabrication, and an honest failure is the
   correct outcome.
6. End with a single machine-parseable status line, so a vacuous success is
   distinguishable from a real one.
7. Name the repo's own discipline explicitly, because a generic instruction to "write tests"
   will not find it:
   - **Spec first.** Behaviour lives in `specs/SPEC_*.md`; a behaviour change updates its spec
     in the same patch.
   - **Real red-green TDD**, now that a fire can build an environment: run the new test, see
     it fail, implement, run it again, and report both results.
   - **The right tier**: `tests/unit`, `tests/property` (hypothesis), `tests/adversarial`,
     `tests/regression`, `tests/integration`.
   - **`test_map.json`**: every `dataforge` module needs a decision (131 currently). A new
     module means a new entry, mapped or a deliberately declared fallback.
   - **There is no BDD framework.** No `behave`, no `pytest-bdd`, no Gherkin runner is
     installed, so `.feature` files would be unexecutable ceremony. Say so, or a fire asked
     for "BDD" will invent it. The five tiers are this repo's equivalent and are stricter.
8. Forbid self-registering measurements. `eval/results/` and `docs/quantitative_claims.yaml`
   are off-limits, so a new measurement is *proposed* in the review with the command that
   produced it, for a human to register. Otherwise the fire writes its own evidence.

## Debugging a fire

```bash
cortex automation doctor <name>              # state, error, query_id, thread_id
cortex conversations transcript <thread_id>  # exactly what ran
```

Prefer the transcript. The dangerous case is state `SUCCEEDED` while the side effect never
happened, which the state alone cannot distinguish. Both commands depend on the
`cortex automation` CLI reaching its endpoint, which is currently failing locally.

## Retrieving a review

```sql
GET 'snow://workspace/USER$PRANESH07.PUBLIC.DEFAULT$/versions/live/daily-review.md'
    'file://C:/dev/dataforge/docs/automation/';
```

Then read it and decide whether to commit it. Treat it as a draft by an author who could
not run the tests: check that every number cites a source, and that anything unmeasurable
was left as an open question rather than filled in.
