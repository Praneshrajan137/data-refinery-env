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

The fire writes one file, `daily-review.md`, into the user workspace. A human
retrieves it and commits it. The fire cannot commit or push (see below).

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
`benchmark_results`, `eval/thresholds`, `eval/preregistration`, `.github` (so CI config
is reviewable), the root markdown, `pyproject.toml`, `Makefile`, `uv.lock`.

Excluded deliberately, none of it source: `data/` (309.8 MB of datasets as of
2026-09-01), `eval/results/` (313.6 MB of archived run snapshots), `node_modules`,
caches, `*.pyc`, and `training/kaggle_dataset_v3/expert_v3.jsonl` (11.9 MB frozen
curriculum).

`playground/` is only **139 source files / 3.7 MB** once `node_modules` is excluded, as
measured 2026-09-01. Its 10,629-file raw total caused an earlier snapshot to skip it
entirely, which left the fire unable to import 10 modules. Judge directories by source
content, not by file count.

**The snapshot contains no `.git`**, so a fire using it cannot read history and cannot
push. It is a point-in-time copy and **nothing refreshes it automatically** — rebuild and
re-upload it when the repository has moved on, or reviews will silently describe stale
code.

## The work budget, and why it exists

One run exhausted its entire budget on `pip install` plus the test suite, and produced no
review.

That was caused by a prompt defect, not by the fire misbehaving: the prompt required that
no number be stated unless the fire measured it in that run, which left no option but to
make the tree executable. The dependency tree it walked into includes `z3-solver`,
`pandas`, `numpy`, `pyarrow`, `networkx`, `causal-learn` and `hyppo`.

**If a prompt forbids installs, it must also state what remains measurable.** Otherwise the
constraint is incoherent and consumes the run.

**Six** scripts under `scripts/ci/` import only the standard library, derived by AST on
2026-09-01 and enforced by `tests/unit/test_sandbox_measurement_floor.py` so the set cannot
rot unnoticed. Of those, exactly **two** are read-only reporters worth running unattended:

| Zero-install command | Reports |
| --- | --- |
| `python scripts/ci/attestation_conformance.py --check` | whether both attestation implementations agree on every vector |
| `python scripts/ci/test_map_coverage.py --check` | modules carrying a mapping decision |

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

Shell tools (`grep`, `wc`, `find`, `sort`, `uniq`, and `python` for one-off stdlib
analysis) are free and give real citable structure: file counts, line counts, test counts,
import graphs, persistent TODOs.

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
