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

1. **Re-sync the local `.venv` to `uv.lock`.** Highest actual value, no code change, closes
   ~30 pip-audit findings, and fixes the `click` floor violation. Non-breaking.
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
the two that look worst are not in the lockfile at all. Action 1 above — re-sync `.venv` to
`uv.lock` — remains the correct and only response. No pin was changed on 2026-08-27, because
changing a pin to satisfy an advisory that the lockfile already satisfies would encode a fix
for a problem the project does not have.

Deliberately not done in the same pass: the `uv sync` itself. It would rebuild the interpreter
that every measurement in this cleanup was verified against, so it belongs in its own change
with its own before/after, not bundled into a deletion sweep.

Note on `.github/dependabot.yml`: it was **deleted** on 2026-08-27. It declared six ecosystems
and then set `open-pull-requests-limit: 0` on every one, so it raised no version-update PRs at
all while looking like dependency monitoring. Dependabot *security alerts* are a repository
setting rather than a manifest file, so the alert stream this document triages is unaffected by
that deletion.
