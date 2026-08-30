# Dependency advisory triage

**Date:** 2026-08-07
**Verdict: 0 of 14 open advisories are reachable from a production code path.**

Fourteen open Dependabot advisories, five rated high, looked like a five-alarm problem. They
are not, and the reason matters more than the conclusion: **a raw advisory count measures
your dependency graph, not your exposure.** Acting on the count rather than on reachability
would have meant two major version bumps for vulnerable code paths this project never calls,
while the one genuinely useful action — a stale local virtualenv — was not on the list at all.

## The three views disagree, and the manifests are right

| View | What it audits | Finding |
| --- | --- | --- |
| `pip-audit` | the installed `.venv` | 38 vulns in 10 packages |
| Dependabot | committed manifests + lockfiles | 14 open (5 high, 5 medium, 4 low) |
| `npm audit` | `playground/web` | 1 vuln (postcss, high) |

`uv.lock` **already pins patched versions** of click, pillow, gradio, mcp, torch and
setuptools. The local `.venv` is stale and holds older builds, which is where roughly 30 of
pip-audit's 38 findings come from. They are an artifact of an unsynced environment, not
project exposure. `bleach` and `pymdown-extensions` are not in `uv.lock` at all — stray
ad-hoc installs.

One real inconsistency surfaced from this: the venv has `click 8.2.1` while `pyproject.toml`
declares `click>=8.3.3`. The environment violates the project's own floor.

## The 14 open advisories

| Package | Alerts | Severity | Direct? | Classification | Fixed in |
| --- | --- | --- | --- | --- | --- |
| starlette | 2 | high + low | direct | **already fixed — stale alert** | 1.3.1 *(pinned)* |
| python-multipart | 4 | high + 3 low | direct | **already fixed — stale alert** | 0.0.31 *(pinned)* |
| aiohttp | 3 | high + 2 med | transitive | dev-only | 3.14.3 |
| cryptography | 1 | high | transitive | not reachable | 50.0.0 *(major)* |
| postcss | 2 | high + med | transitive | dev-only | 8.5.23+ |
| pymdown-extensions | 2 | 2 med | direct | dev-only + not reachable | 11.0.0 *(major)* |

**Two of the five highs are false positives.** The deployed image installs from
`playground/api/requirements.txt`, which already pins `starlette==1.3.1` and
`python-multipart==0.0.31`. Checking each alert's own `vulnerable_version_range` against the
pinned version puts every one of the six starlette/python-multipart alerts *outside* the
vulnerable range. Commit `b166a28` applied those pins and is an ancestor of `main`; the alerts
were created before it and every one has `updated_at == created_at`, so Dependabot simply never
re-evaluated them.

Reachability was established per package by locating the *specific* vulnerable API, not by
package presence:

- **cryptography** — the advisory is scoped to `pkcs7_decrypt_der/_pem/_smime`. Zero hits for
  `pkcs7` anywhere in the repo. Arrives only via `Authlib`/`joserfc` in the `openenv` extra.
- **pymdown-extensions** — scoped to the `b64` extension. `docs/mkdocs.yml` enables
  `details`, `highlight`, `inlinehilite`, `snippets`, `superfences`. `b64` is not enabled.
- **aiohttp** — present only under the `all`/`dev` extras and as an `fsspec` dep. Zero imports
  in `dataforge/`. Dependabot labels the scope `runtime`, which is a `uv.lock` artifact and is
  wrong here.
- **postcss** — single path, `vite` → root `devDependencies`. Build-time CSS processing; the
  five runtime deps are `lucide-react`, `motion`, `papaparse`, `react`, `react-dom`.

The packages pip-audit flags that Dependabot does not include several that *are* genuinely
production-facing — and all are already patched in `uv.lock`, with the vulnerable API unused
regardless: `click.edit()` (0 hits), `torch.jit.script` (0 hits), gradio's `FileExplorer` and
audio-cache components (not used), and mcp's websocket transport and `enable_tasks()` (the
server defaults to `stdio`, with `streamable-http` opt-in).

## Actions, ordered by real risk rather than by severity label

1. **Re-sync the local `.venv`.** Highest actual value, no code change, closes
   ~30 pip-audit findings, and fixes the `click` floor violation. Non-breaking.
   *(Wording corrected 2026-08-28: this originally said "to `uv.lock`". The environment must
   satisfy **pyproject's** constraints, and the rebuild is `pip install -e ".[all]"`, not
   `uv sync` — see the correction section at the end. Executed 2026-08-28.)*
2. **`aiohttp` 3.14.1 → 3.14.3** in `uv.lock`. Patch bump, clears 3 alerts including a high.
3. **`postcss` → 8.5.26** in `playground/web/package-lock.json`. `vite` already declares
   `^8.5.15`, so this is a lockfile-only change. Use a targeted update, not `npm audit fix`.
4. **Get the 5 stale starlette/python-multipart alerts closed.** No code change; the pins are
   already correct. This removes 2 of the 5 highs from the board and stops them distorting the
   metric.
5. **`cryptography` → 50.0.0.** Major bump, potentially breaking `Authlib`/`joserfc`. The
   vulnerable path is unused, so schedule it deliberately rather than as a patch sweep.
6. **`pymdown-extensions` → 11.0.0.** Major bump, may break the docs build. Lowest real risk.

## Limits of this triage — stated rather than implied

- **The gradio version running on the Hugging Face Space was undetermined, and is now
  pinned.** `playground-model/README.md` declared `sdk: gradio` with no `sdk_version`, so
  Hugging Face chose the version at build time and neither the venv's 6.14.0 nor `uv.lock`'s
  6.20.0 necessarily governed it. Had the Space resolved below 6.15.0, CVE-2026-48545 (cookie
  injection) **would** have been production-reachable, because that advisory is framework-level
  rather than component-gated — it was the one finding that could have changed the headline
  verdict. `sdk_version: 6.20.0` is now pinned, which is above the fix line and matches the
  lockfile. **This still needs a Space rebuild to confirm** the pinned version resolves;
  until then the pin is asserted, not verified.
- **Reachability was established by static grep** of imports and vulnerable API names. Code
  reached via `importlib`, `getattr`, entry points or plugin loading would not appear. Risk is
  low here only because the vulnerable APIs are narrowly scoped.
- **Why Dependabot has not auto-closed the stale alerts is unknown.** The pins are provably
  patched and the fix is on `main`; Dependabot's internal scan state was not inspected.
- **`mkdocs-material` 9.6.23 compatibility with `pymdown-extensions` 11.0.0 was not
  resolved**, so action 6 may break the docs build.

## Re-confirmed 2026-08-27, and the same conclusion held

`scripts/ci/backend_gate.py --require-optional` failed on `pip-audit` and nothing else — every
other step, including all five package builds and the release gate, passed. The advisories were
pillow (9), sqlparse (4), pymdown-extensions (2), setuptools, pip and torch. Checked package by
package against `uv.lock` rather than acted on:

| Package | Installed in `.venv` | `uv.lock` pins | Advisory wants | Reading |
| --- | --- | --- | --- | --- |
| pillow | 12.2.0 | **12.3.0** | 12.3.0 | lock already patched; venv stale |
| setuptools | 81.0.0 | **83.0.0** | 83.0.0 | lock already patched; venv stale |
| torch | 2.12.0 | **2.13.0** | 2.13.0 | lock already patched; venv stale |
| sqlparse | 0.5.4 | *absent* | 0.6.0 | stray ad-hoc install, not a project dependency |
| pymdown-extensions | 10.21.3 | *absent* | 11.0.0 | stray ad-hoc install, as recorded above |
| pip | 26.1.2 | 26.1.2 | 26.2 | the installer, not a dependency of the product |

So the finding is still **the environment, not the project**: for the three packages that are
genuinely locked, the lockfile already carries the patched version the advisory asks for, and
the two that look worst are not in the lockfile at all. Action 1 above — re-sync `.venv` —
remains the correct and only response. No pin was changed on 2026-08-27, because changing a
pin to satisfy an advisory that the project's own constraints already satisfy would encode a
fix for a problem the project does not have.

Deliberately not done in the same pass: the environment rebuild itself. It would replace the
interpreter that every measurement in that cleanup was verified against, so it belongs in its
own change with its own before/after, not bundled into a deletion sweep.

Note on `.github/dependabot.yml`: it was **deleted** on 2026-08-27. It declared six ecosystems
and then set `open-pull-requests-limit: 0` on every one, so it raised no version-update PRs at
all while looking like dependency monitoring. Dependabot *security alerts* are a repository
setting rather than a manifest file, so the alert stream this document triages is unaffected by
that deletion.

**Correction to the paragraph above, 2026-08-28.** Two claims in the 2026-08-27 block were
wrong about mechanism, and correcting them matters more than the numbers they framed:

- It said CI "builds fresh from `uv.lock`". **It does not.** Every job in `.github/workflows/ci.yml`
  installs with plain pip (`pip install -e ".[dev]"`, lines 32, 53, 78, 94, 162). `uv.lock` is
  consumed in exactly one place in the repository: `Dockerfile.env:21`. So the reference the
  environment must satisfy is **pyproject's declared constraints**, not the lockfile. The
  lockfile agreeing with them was a coincidence of both being current, not the mechanism.
- It implied `uv sync` was the fix. It is the wrong tool here. `pyproject.toml` has no
  `[tool.uv]` table and no workspace members, so uv would treat only `dataforge_07` as the
  project and know nothing about `dataforge-mcp/` or `packages/*`. A default `uv sync` is
  exact, so it would have removed all the editable installs and every extra. Auditing the
  lockfile directly is not supported either: pip-audit's `--locked` reads only `pyproject.toml`
  and `pylock.*.toml`.

## Rebuilt 2026-08-28: 42 advisories to 1, and the click floor finally holds

Action 1 was executed. `.venv` was deleted and rebuilt on Python 3.12.10 via the Makefile path
(`pip install -e ".[all]"`, which already covers `[openenv]`), plus `docs/requirements.txt`,
`playground/api/requirements.txt`, `kaggle`, and editable installs of `dataforge-mcp`,
`packages/dataforge-dbt` and `packages/dataforge-agent-patterns`.

Measured with the reviewed exception list applied, before and after:

| | Vulnerabilities | Packages affected | Dependencies scanned |
| --- | --- | --- | --- |
| Before (stale venv) | **42** | **11** | 266 |
| After (rebuilt) | **1** | **1** | 257 |

The headline was never the count. `pyproject.toml:17-19` pins `click>=8.3.3` with the comment
*"Pin a security floor so the resolver cannot backtrack to click 8.2.1 (PYSEC-2026-2132)"* — and
the environment was running **click 8.2.1**, the exact version that pin exists to forbid, named
in the comment. The `train` extra pins `datasets==4.8.5`; the environment had **4.8.4**. Both
now hold. This document flagged the click divergence on 2026-08-07 and it had persisted for
three weeks, which is the real lesson: an environment finding with no forcing function does not
get fixed.

Every previously-flagged package resolved to at or above its fix version on a fresh install:
click 8.5.0, aiohttp 3.14.3, cryptography 50.0.1, pillow 12.3.0, datasets 4.8.5, mcp 1.29.1,
gradio 6.20.0, torch 2.13.0, setuptools 84.0.0, pip 26.2.1, bleach 6.4.0. `sqlparse` and the
old orphaned `aiohttp` had no dependents at all and simply disappeared; the stale `dataforge`
and `dataforge15` distributions went with them, the former having pointed at the
`data_quality_env` package deleted the previous day.

### The torch exception was retired, not renewed

`CVE-2025-3000` / `PYSEC-2025-194` (they are aliases; pip-audit's `--ignore-vuln` matches
aliases, which is why one ID suppressed the other) is scoped to `torch.jit.script` and affects
the range 0 to 2.6.0. A fresh resolve installs **torch 2.13.0**, far outside it. Verified by
re-running the audit *without* that ignore: torch does not appear. The exception was therefore
removed rather than re-dated. An ignore that no longer suppresses anything is not harmless — it
carried a 2026-10-14 expiry that would have failed `canonical-backend-gate` on its own, for a
vulnerability the environment no longer had.

### The one survivor, and why it is an exception rather than a fix

`datasets 4.8.5` / `PYSEC-2026-3716` (`CVE-2026-66807`) is a path traversal in datasets'
**folder-based** dataset builders: an unvalidated `file_name` metadata field is joined to the
dataset directory, letting a hostile dataset directory read arbitrary local files, which are
then embedded into output on `save_to_disk` or `push_to_hub`. Fixed in 5.0.1.

Not reachable here, established by locating the vulnerable construct rather than the package.
All eleven imports in the repository are `from datasets import Dataset`
(`scripts/remote/kaggle_*.py`, `training/kaggle_*_kernel/*.py`), and every construction is
`Dataset.from_list` over records already parsed from a local JSONL file. There is no
`load_dataset` call, no `imagefolder`/`audiofolder` builder, and so no `file_name` metadata path
to traverse. `dataforge/` imports the library zero times. CVSS 4.0 also scores `UI:A` — it needs
a user to actively load a hostile dataset directory.

The fix is not a patch bump: `train` pins `datasets==4.8.5` alongside `trl==1.4.0` and
`transformers==5.7.0`, so moving to 5.x is a deliberate training-stack revalidation. Recorded as
a scoped exception expiring 2026-11-26.

### Corrected reasoning on CVE-2026-67422

That exception justified itself as *"same blocker as PYSEC-2026-3609: b64 is not enabled"*. That
describes the wrong advisory. CVE-2026-67422 (= PYSEC-2026-3654) is an exponential-backtracking
ReDoS in `pymdownx.caret` (`SUP2`), `pymdownx.tilde` (`SUB2`), `pymdownx.betterem`
(`SMART_UNDER_EM2`, reached by the default `smart_enable='underscore'`) and `pymdownx.magiclink`
(`RE_LINK` host subexpression). Unlike the b64 issue, these fire in **default configuration**,
so "not enabled by default" was not an argument that applied.

The verdict survives on two independent grounds, both now stated so a reviewer can re-check
them. `docs/mkdocs.yml` enables only `admonition`, `attr_list`, `md_in_html`, `toc`,
`pymdownx.details`, `pymdownx.highlight`, `pymdownx.inlinehilite`, `pymdownx.snippets` and
`pymdownx.superfences` — none of the four, and not `pymdownx.extra`, which would pull in
`betterem`. And more durably: the impact is denial of service against **untrusted** Markdown,
while mkdocs renders only trusted, repo-authored content at build time. The advisory itself
concedes "Most Material/MkDocs usage renders trusted author content at build time." That second
reason is the one that still holds if somebody enables `caret` later. The 11.0.1 fix remains
blocked by `mkdocs-material 9.6.23`'s `pymdown-extensions~=10.2` requirement, verified from
installed metadata rather than assumed.

## Both pymdown exceptions DELETED 2026-08-28: the blocker cleared upstream

The section above ends by saying the fix "remains blocked". It is no longer blocked, and the
correct response was to take the fix rather than extend the expiry.

Checked every `mkdocs-material` release from 9.6.14 to 9.7.7 on PyPI:

| mkdocs-material | pymdown-extensions requirement |
| --- | --- |
| 9.6.14 through **9.6.23** | `~=10.2` (caps below 11) |
| **9.7.0** through 9.7.7 | `>=10.2` (permits 11.x) |

**9.7.0 is the exact release that relaxed the cap.** Both advisories have installable fixes:
`PYSEC-2026-3609` (= CVE-2026-61632, GHSA-9xwg-3r6f-jcx2, b64 path traversal) in **11.0.0**, and
`CVE-2026-67422` (= PYSEC-2026-3654, GHSA-gm37-52c6-37mw, exponential ReDoS) in **11.0.1**.
`docs/requirements.txt` now pins `mkdocs-material==9.7.7` with `pymdown-extensions==11.0.2`, and
both `PipAuditException` entries are gone.

An exception that no longer suppresses anything is not harmless: it still carries an expiry that
would have failed `canonical-backend-gate` on 2026-11-08 for vulnerabilities the toolchain no
longer had. Same reasoning that retired the torch entry. `PIP_AUDIT_EXCEPTIONS` is now a single
entry (`datasets`).

### Why the major bump was low risk, stated before it was taken

`pymdown-extensions 11.0` release notes list exactly one breaking change: b64 now restricts
relative links to `base_path` by default. That is **in an extension `docs/mkdocs.yml` does not
enable**, and it *is* the fix for PYSEC-2026-3609. The other 11.0 changes are dropping Python 3.9
(this project requires >=3.11) and a Tabbed bugfix. 11.0.1 and 11.0.2 are fixes only; 11.0.2
improves InlineHilite performance, an extension this site does use. `mkdocs-material 9.7.7`
requires `mkdocs<2,>=1.6`, so the `mkdocs==1.6.1` pin is unchanged.

Verified rather than assumed, because a `--strict` exit code can hide a silently dropped fence:
the rendered site was counted before and after the bump and is identical — **8 mermaid blocks, 13
highlight blocks, 22 HTML pages**, `mkdocs build --strict` exit 0 both times. And with only the
`datasets` ignore passed, pip-audit reports "No known vulnerabilities found, 1 ignored" with zero
pymdown rows, which is the check that the upgrade rather than a suppression is doing the work.

One informational warning now appears in the build: the Material team's notice about MkDocs 2.0
removing the plugin system. It is upstream advocacy about a future major version, does not fail
`--strict`, and both the `mkdocs==1.6.1` pin and mkdocs-material's own `mkdocs<2` cap already
prevent it from reaching this project.

### `docs/pyproject.toml` deleted

The docs toolchain was pinned in two places that had already diverged: `docs/requirements.txt` at
`mkdocs-material==9.6.23` and `docs/pyproject.toml` at `9.6.20`. Only the former is installed by
anything (`docs.yml:34` and `canonical-backend-gate`). The latter declared
`name = "dataforge-docs"` with `[tool.uv] package = false`, and its only known consumer was the
dependabot `/docs` uv entry removed on 2026-08-27. No tracked file referenced the path. Deleted,
so the pins cannot silently disagree again.

### `kaggle` is now an opt-in extra, and still absent from `[all]` by design

`pyproject.toml` declares `kaggle = ["kaggle>=2"]`. It is deliberately **not** a member of `all`,
so a from-scratch rebuild will still not install it — that is the intended behaviour, not an
oversight. It is an operational credential tool with exactly one consumer,
`scripts/preflight/check_kaggle_auth.py`, invoked by `dataforge/release/doctor.py` for the Kaggle
OAuth clean-config check. Adding it to `all` would install it for every developer and permanently
widen the pip-audit surface, since `kaggle` pulls `bleach`.

Two things not declared, for recorded reasons. `kagglesdk` arrives transitively — `kaggle 2.2.4`
requires `kagglesdk<1.0,>=0.1.35` — so naming it would duplicate a constraint upstream owns. And
`kaggle_secrets`, imported at 11 sites across `scripts/remote/` and `training/kaggle_*_kernel/`,
**is not on PyPI at all**: it exists only inside the Kaggle notebook runtime, which is why each of
those imports sits in a `try/except` that degrades to an explicit "unavailable" result. Declaring
it would be a false claim.

The gap this closes is discoverability, not correctness. Nothing was broken — the one `kaggle`
import is lazy and guarded by `ImportError` — but the requirement was stated only inside that
exception string. It now names the installable form: `pip install -e ".[kaggle]"`.

### Lint and type coverage extended to scripts/preflight and scripts/remote

`check_kaggle_auth.py` is invoked by the release doctor and covered by nine test references, yet
`scripts/preflight/` and `scripts/remote/` were in no Makefile lint or type target. Measured cost
before acting: ruff reported **0 errors** on both, so both were added to `make lint` (445 files now
formatted, up from 430). `mypy --strict` gave **2** errors on preflight, both missing stubs, so
`check_kaggle_auth.py` was added to `make type` (**176** files, up from 175) behind a
`[[tool.mypy.overrides]]` for `kaggle.*`/`kagglesdk.*` following the existing `torch` and `mcp`
pattern. `scripts/remote/` reported **46** errors across 12 files and is deliberately left out of
`make type`, recorded in a Makefile comment so the omission is a decision rather than an oversight.

The override uses submodule patterns only. Bare `kaggle`/`kagglesdk` entries were removed after
mypy reported them as `unused section(s)` — the real imports are `kaggle.api.kaggle_api_extended`
and `kagglesdk.kaggle_creds`. A pattern matching nothing is noise that trains readers to skim
type-check output.

## Acted on 2026-08-30: 14 alerts to 6, then to 0 pending rescan

The 2026-08-07 triage above ended with six numbered actions. Items 2, 3, 4 and 5 were still open
three weeks later, which is the same "no forcing function" pattern this document already named about
the click floor. They are now done. The split held exactly on re-checking: **6 of the 14 alerts were
real, 8 were stale.**

### The three real fixes

| Package | Was | Now | Alerts cleared | Where |
| --- | --- | --- | --- | --- |
| aiohttp | 3.14.1 | **3.14.3** | 86 high, 85 med, 84 med | `uv.lock` |
| cryptography | 49.0.0 | **50.0.1** | 87 high | `uv.lock` |
| postcss | 8.5.15 | **8.5.26** | 83 high, 88 med | `playground/web/package-lock.json` |

Both Python bumps were done as **floors in `pyproject.toml`**, not just lockfile edits, following the
`click>=8.3.3` precedent: the floor is what stops a resolver backtracking onto the vulnerable build
anywhere the lockfile is not the input, and CI installs with plain pip rather than from the lock.
`cryptography>=50.0.0` is set in both the `dev` and `openenv` extras, since Authlib in `openenv` is
the reason cryptography is named at all.

postcss went to **8.5.26 rather than the 8.5.23 the advisory names**. 8.5.23 alone carries a
`list.split()` regression introduced by the 8.5.17 visitor change; 8.5.25 and 8.5.26 fix it. Taking
the version the advisory names would have swapped a disclosure bug for a correctness bug.

### `uv.lock` was already out of sync with `pyproject.toml`, and that is the bigger finding

The re-lock changed far more than two packages, so it was worth establishing why before accepting it.
Measured rather than assumed: `uv lock --check` **fails on pristine `main`**. The lockfile carried
`ruff 0.15.12` against a declared `ruff>=0.16.2,<0.17`, and `mypy 2.1.0` against `mypy>=2.3,<3` --
it violated two of the project's own pins. Since `Dockerfile.env:21` runs `uv sync --frozen`, that
image build was already broken and nothing surfaced it. `uv lock --check` and a
`uv sync --frozen --no-editable --extra playground --dry-run` both now exit 0.

Narrowing the diff back to two packages was therefore the wrong instinct: it would have preserved a
lockfile that could not be installed by its only consumer.

One consequence to state plainly: the lock now also resolves the `kaggle` extra declared on
2026-08-28 (kaggle, kagglesdk, bleach, jupytext and their dependencies), which **widens the surface
Dependabot scans**. No image changes, because `Dockerfile.env` syncs `--extra playground` only. This
is the correct lock content -- `pyproject.toml` declares the extra -- but it means a future alert may
name a package no shipped artifact installs.

### An error of mine, recorded because the plan prescribed it

The plan specified `npm update postcss --package-lock-only`. **That command produced a lockfile that
fails `npm ci`.** It pruned the hoisted `@emnapi/core` and `@emnapi/runtime` peers but left an
orphaned top-level `@emnapi/wasi-threads@1.2.2`, which npm then validated against the registry's
current `@emnapi/core` and rejected: `lock file's @emnapi/wasi-threads@1.2.2 does not satisfy 1.2.3`.

Isolated rather than guessed at: `npm ci` against the pristine lock succeeded (226 packages, exit 0),
so the breakage was mine and not pre-existing. Dropping `--package-lock-only` did not fix it either --
a plain `npm install` afterwards did, reconciling `wasi-threads` to 1.2.3 and removing the orphan.
The lesson is narrow but real: for a transitive bump, verify with `npm ci` rather than trusting that
`npm update` leaves a consistent tree. `package.json` is byte-identical, so postcss stays transitive
under `vite`.

### The 8 stale alerts, dismissed with per-alert evidence

| Alerts | Package | Evidence |
| --- | --- | --- |
| 37, 36 | starlette | `origin/main` pins `starlette==1.3.1`; `uv.lock` resolves 1.3.1. Ranges `>=0.4.1,<1.3.1` and `<1.3.0` both exclude it. |
| 31, 29, 27, 25 | python-multipart | pinned `0.0.31`, lock resolves `0.0.32`. Ranges `<0.0.30` and `<0.0.31` exclude both. |
| 89, 81 | pymdown-extensions | manifest `docs/pyproject.toml` deleted in `0214b3c`; `docs/requirements.txt` pins `11.0.2`. |

Dismissed with `dismissed_reason=inaccurate` -- the honest reason, since each alert asserts a version
the repository does not have -- and a comment carrying the pin, the commit and the advisory range so
a reviewer can re-derive the verdict instead of trusting it. Dismissals are reversible.

A hypothesis this killed on the way: the phantom entries in GitHub's dependency-graph SBOM
(`starlette 1.1.0`, `python-multipart 0.0.27`, `pymdown-extensions 10.21.3`) looked at first like a
hidden vulnerable manifest. `git grep` finds **no tracked file** declaring any of those versions, so
they are GitHub-side retained state. That is what makes dismissal correct rather than a shortcut.

### No `.github/dependabot.yml` was added

Considered and declined. Security alerts are a repository setting and keep working without it, and the
file deleted on 2026-08-27 was worse than absent -- six ecosystems with `open-pull-requests-limit: 0`
on every one, so it produced the appearance of monitoring and none of the substance. Adding one back
would need a non-zero limit to be worth anything, and that is a deliberate decision about PR noise
rather than a side effect of a security pass.

### Limits

- **The dismissals are asserted against manifest pins, not against a running image.** The deployed
  Hugging Face Space was not rebuilt or inspected. If it installs from something other than
  `playground/api/requirements.txt`, the starlette and python-multipart reasoning needs re-checking.
- **The `datasets` pip-audit exception could not be evaluated here.** `datasets` is not installed in
  this venv (it lives in the `train` extra), so the clean `pip-audit` run does *not* show the
  exception is now redundant. It was left in place rather than retired on absent evidence.
- **The 6 real alerts are not closed yet, only fixed.** They close when Dependabot rescans the pushed
  lockfiles. Verified locally instead: `pip-audit` reports no vulnerabilities, and `npm audit`
  reports 0 for `playground/web`.
