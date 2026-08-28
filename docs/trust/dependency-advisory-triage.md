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
