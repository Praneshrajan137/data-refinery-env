# DataForge — Progress Log (append-only)

Ralph and manual iterations append here. Never delete entries.

---

## 2026-04-20 — Week 1: CLI shell + 3 detectors + profile command

### Shipped
- `dataforge/detectors/base.py` — `Severity` enum (SAFE/REVIEW/UNSAFE), `Issue` Pydantic model, `Schema`+`FunctionalDependency` models, `Detector` protocol.
- `dataforge/detectors/type_mismatch.py` — `TypeMismatchDetector` (classifies values as numeric/date/string, flags minority-type values).
- `dataforge/detectors/decimal_shift.py` — `DecimalShiftDetector` (power-of-10 outlier detection with expected-value suggestion).
- `dataforge/detectors/fd_violation.py` — `FDViolationDetector` (groups by determinant, flags multi-valued dependents).
- `dataforge/detectors/__init__.py` — `run_all_detectors()` convenience (dedup + severity sort).
- `dataforge/cli/profile.py` — `dataforge profile <csv> [--schema <yaml>]` subcommand.
- `dataforge/ui/profile_view.py` — Rich table rendering with color-coded severity.
- `dataforge/agent/providers.py` — async LLM client stub (groq + gemini with tenacity retry).
- `specs/SPEC_detectors.md` — full spec with 8 toy cases in Appendix A.
- `tests/fixtures/hospital_10rows.csv` + `hospital_schema.yaml` — synthetic fixture with 4 seeded issues.
- 69 tests, all green, 1.69s total.

### Gotchas encountered
1. **Typer single-command collapse**: with only one registered command, Typer auto-flattens the subcommand into the root. Fix: add `@app.callback(invoke_without_command=True)` to force group mode.
2. **dtype=str changes type_mismatch semantics**: loading CSV with `dtype=str` means phone numbers like `"217-555-0101"` classify as string (not numeric). Fixture must use all-numeric phone format to make `"not available"` detectable as a type_mismatch.
3. **Confidence scaling**: raw dominance ratio (e.g., 3/4 = 0.75) is too low for confidence. Added scarcity boost so a single outlier in a strongly-typed column gets confidence >= 0.85.
4. **AsyncMock vs MagicMock**: `response.json()` from `AsyncMock` returns a coroutine, not a dict. Use `MagicMock` for the response object and `AsyncMock` only for the async client.
5. **Legacy test file**: root `test_env.py` has 199 assertions (not 389 as prompt claimed). Tests the old hackathon RL environment — not portable to DataForge detectors.

## 2026-04-21 — Week 4: Real-world benchmark infrastructure

### Shipped
- `dataforge/datasets/registry.py` + `dataforge/datasets/real_world.py` — canonical metadata, cache-aware raw dataset loading, positional dirty/clean header alignment, and cell-level ground-truth diffs.
- `dataforge/bench/` — benchmark types, scoring, quota estimation, a benchmark-local Groq client, method runners, orchestration, and markdown report helpers.
- `dataforge/cli/bench.py` — `dataforge bench [--methods] [--datasets] [--seeds] [--really-run-big-bench]`.
- `scripts/bench/run_agent_comparison.py`, `run_sota_comparison.py`, and `generate_report.py` — thin wrappers for JSON and markdown artifact generation.
- `specs/SPEC_benchmarks.md` plus unit / integration coverage for dataset loading, quota gating, CLI wiring, cached heuristic runs, and README marker replacement.

### Gotchas encountered
1. **Upstream header mismatch is real**: Hospital and Beers dirty/clean CSVs do not share header names, so name-based diffing would silently corrupt ground truth.
2. **Benchmark quality and apply-path safety are different concerns**: Week 4 scores raw repair quality, not the post-safety apply path.
3. **README honesty required a bounded manual patch first**: the report generator needed stable markers, but the repository still described a Week 0 scaffold.

---

## Design-Partner Outreach Channels (Week 4 gate)

### Channels and message templates

1. **dbt Slack `#help`** — "Has anyone automated data-quality repair beyond detection? I'm building DataForge (open-source, CLI-first) and would love 15 minutes of feedback from anyone who's fought dirty CSVs in production."
2. **Locally Optimistic** — "Looking for data engineers willing to try a new open-source profiling + repair CLI against their own messy data. Free, Apache-2.0, no signup. DM me for the repo link."
3. **MLOps Community** — "DataForge is a CLI that detects and proposes reversible repairs for tabular data quality issues. Seeking design partners willing to run `dataforge profile` on a real CSV and share what surprised them."
4. **r/dataengineering** — "Looking for working data engineers willing to test a reversible CSV repair CLI on a real file and tell me where it breaks."
5. **r/dbt** — "Looking for dbt users who have messy upstream CSVs and want to try a profiling + dry-run repair tool before I ship the hosted playground."
6. **Targeted GitHub issue drive-bys** — Find repos with open issues mentioning "data quality", "dirty CSV", or "data cleaning". Leave a helpful comment first; mention DataForge only if it is genuinely relevant.

### Weekly tally

| Week | DMs sent | Replies | Partners onboarded | Issues filed | Quotes |
| ---- | -------- | ------- | ------------------ | ------------ | ------ |
| W4   |          |         |                    |              |        |
| W5   |          |         |                    |              |        |
| W6   |          |         |                    |              |        |

---

## 2026-04-21 — Week 5: Hosted playground + design-partner gate artifacts

### Implemented (deployment verification pending)
- `playground/api/app.py` — FastAPI backend with `/api/health`, `/api/profile`, `/api/repair`, `/api/samples/{name}` endpoints. Size-cap middleware (1 MB), slowapi rate limiting (10/min/IP, single-worker), CORS for `*.pages.dev` + localhost dev, advanced-mode gating, ephemeral transaction journals in `TemporaryDirectory`.
- `playground/api/Dockerfile` — multi-stage build (python:3.12-slim), non-root UID 1000, `${PORT:-7860}`, `--workers 1 --timeout-keep-alive 5`.
- `playground/api/samples/` — 3 deterministic 10-row CSVs (hospital, flights, beers) generated by `scripts/playground/build_samples.py` from the Raha upstream datasets.
- `playground/api/README.md` — HF Space YAML front-matter (`sdk: docker`, `app_port: 7860`).
- `playground/api/SPACE_SETUP.md` — one-time HF deploy runbook based on `scripts/playground/stage_space.py`.
- `playground/web/index.html` — single-page frontend with Pico.css, ARIA tabs (Profile / Repair / Revert Journal), upload widget, sample dropdown, cold-start banner, privacy notice.
- `playground/web/app.js` + `config.js` — vanilla frontend with runtime backend config, exponential-backoff health check, advanced capability detection, request-state locking, and keyboard-complete tabs.
- `playground/web/style.css` — severity badges, diff colors, dark-mode via `prefers-color-scheme`.
- `playground/web/DEPLOY.md` — Cloudflare Pages deploy runbook.
- `specs/SPEC_playground.md` — full spec with 6 Appendix A toy cases.
- `tests/integration/test_playground_smoke.py` — hardened smoke suite for backend health, advanced-mode gating, dry-run journals, and rate limiting.
- `tests/unit/test_playground_stage_space.py` + `tests/unit/test_playground_web_contract.py` — staging/runbook and frontend contract checks.
- `CONTRIBUTORS.md`, `.github_templates/DESIGN_PARTNERS.template.md`, `.github/ISSUE_TEMPLATE/design_partner_feedback.yml` — Week 4 gate artifacts.
- `DECISIONS.md` — 2 new entries (design-partner gate, Cloudflare+HF hosting).
- `ARCHITECTURE.md` — playground dependency justifications.
- `.github/workflows/ci.yml` — shared quality gate + `playground-smoke` CI with YAML validation and grep assertions.
- `scripts/ci/readme_truth.py` — README integrity checker.

### Post-deploy metrics (TBD — populate after Phase F)
- Docker image size: TBD (target <= 600 MB)
- Cold-start latency: TBD (target <= 30 s)
- Warm `/api/profile` latency: TBD (target <= 5 s)
- Lighthouse performance: TBD (target >= 90)
- Lighthouse accessibility: TBD (target >= 95)
- Live Space URL: TBD
- Live Pages URL: TBD
