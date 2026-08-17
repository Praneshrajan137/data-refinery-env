"""Static contract tests for the playground frontend."""

from __future__ import annotations

import json
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = PROJECT_ROOT / "playground" / "web" / "index.html"
CONFIG_PATH = PROJECT_ROOT / "playground" / "web" / "public" / "config.js"
SYNC_SCRIPT_PATH = PROJECT_ROOT / "playground" / "web" / "scripts" / "sync_runtime_config.mjs"

# The live AgentMotionState vocabulary. The prior 12-state palette had 10 states that were
# only ever drawn in a legend and never rendered on a live surface; it was deliberately
# retired (see dataforge motion.ts and docs/design/perceptual-language.md section 4.1).
# These tests kept iterating the retired list and failed for that reason alone, so the
# vocabulary now lives here once instead of being duplicated at three call sites.
AGENT_MOTION_STATES = (
    "verifying",
    "proposing",
    "proven",
    "held",
    "rejected",
    "asking",
    "done",
    "idle",
)
PACKAGE_PATH = PROJECT_ROOT / "playground" / "web" / "package.json"
SRC_DIR = PROJECT_ROOT / "playground" / "web" / "src"
COLOR_CSS_PATH = SRC_DIR / "design" / "color-system.generated.css"
COLOR_JSON_PATH = SRC_DIR / "design" / "color-system.generated.json"
COLOR_SPEC_PATH = PROJECT_ROOT / "specs" / "SPEC_color_system.md"
MOTION_SPEC_PATH = PROJECT_ROOT / "specs" / "SPEC_motion_system.md"


def test_index_uses_relative_asset_paths_and_config_contract() -> None:
    """The static frontend must be deployable from Cloudflare assets without HF static assumptions."""
    body = INDEX_PATH.read_text(encoding="utf-8")
    assert "/static/" not in body
    assert 'src="%BASE_URL%config.js"' in body
    assert 'src="/src/main.tsx"' in body


def _default_backend_url() -> str:
    """Return the backend host the sync script actually defaults to.

    Derived rather than duplicated: this literal previously lived in two test files and
    both went stale when the backend migrated from an HF Space to Azure Container Apps,
    failing for a reason unrelated to the contract under test. Reading it from the
    generator means the next host migration is a one-line change in one place.
    """
    source = SYNC_SCRIPT_PATH.read_text(encoding="utf-8")
    match = re.search(r'DEFAULT_BACKEND_URL\s*=\s*"([^"]+)"', source)
    assert match is not None, (
        f"{SYNC_SCRIPT_PATH.name} no longer declares DEFAULT_BACKEND_URL; the runtime "
        "config contract cannot be verified without it"
    )
    return match.group(1)


def test_config_js_exposes_backend_url_contract() -> None:
    """config.js defines the committed runtime contract for the backend URL."""
    body = CONFIG_PATH.read_text(encoding="utf-8")
    assert "window.__DATAFORGE_CONFIG__" in body
    assert "BACKEND_URL" in body
    assert 'BACKEND_URL: ""' not in body
    assert _default_backend_url() in body


def test_frontend_stays_storage_free_and_capability_aware() -> None:
    """The frontend remains storage-free, route-aware, and consumes capability metadata."""
    body = "\n".join(
        path.read_text(encoding="utf-8") for path in SRC_DIR.rglob("*") if path.is_file()
    )
    assert "normalizeBackendUrl" in body
    assert "advanced_available" in body
    assert "agent_available" in body
    assert "repair_mode" in body
    assert "streaming_available" in body
    assert "workflow_event_v1" in body
    assert "analyzeStream" in body
    assert "AbortController" in body
    assert "PRODUCT_ROUTES" in body
    assert "routeFromPathname" in body
    assert "pushState" in body
    assert "popstate" in body
    assert "MotionConfig" in body
    assert "useReducedMotion" in body
    assert "data-agent-motion" in body


def test_frontend_has_typed_vite_quality_gates() -> None:
    """The playground frontend is a typed Vite app with unit, browser, and budget gates."""
    body = PACKAGE_PATH.read_text(encoding="utf-8")
    budget = (
        PROJECT_ROOT / "playground" / "web" / "scripts" / "check_bundle_budget.mjs"
    ).read_text(encoding="utf-8")
    assert '"vite"' in body
    assert '"typescript"' in body
    assert '"@playwright/test"' in body
    assert '"@axe-core/playwright"' in body
    assert '"motion"' in body
    assert '"budget"' in body
    # This test used to assert the OPPOSITE: that the budget script contained
    # "Number.POSITIVE_INFINITY" and printed "Bundle budget is unbounded". It locked the
    # gate open -- a contract test enshrining the absence of the guarantee it named.
    #
    # The replacement forbids the ASSIGNMENT rather than the mention, so the script may
    # still explain the defect it removed. Whether the shipped ceilings are actually
    # enforceable is asserted where it can be checked properly, against the parsed table:
    # playground/web/scripts/check_bundle_budget.test.mjs.
    assert not re.search(r"=\s*Number\.POSITIVE_INFINITY", budget)
    assert "assertBudgetsAreEnforceable" in budget
    assert "totalJsBytes" in budget


def test_frontend_motion_system_contract() -> None:
    """The Observatory motion system is tokenized, reduced-motion aware, and agent-state complete."""
    package = json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
    motion_source = (SRC_DIR / "motion.ts").read_text(encoding="utf-8")
    motion_test = (SRC_DIR / "motion.test.ts").read_text(encoding="utf-8")
    styles = (SRC_DIR / "styles.css").read_text(encoding="utf-8")
    main = (SRC_DIR / "main.tsx").read_text(encoding="utf-8")

    assert MOTION_SPEC_PATH.exists()
    assert package["dependencies"]["motion"] == "^12.40.0"
    assert 'from "motion/react"' in main
    assert 'reducedMotion="user"' in main
    assert "motionDurations" in motion_source
    assert "motionEasings" in motion_source
    assert "motionSprings" in motion_source
    assert "workflowEventToMotion" in motion_source
    assert "reducedRouteVariants" in motion_source
    assert "@media (prefers-reduced-motion: reduce)" in styles
    assert "--df-motion-fast" in styles
    assert "df-rail-sweep" in styles
    assert "df-agent-breathe" in styles
    assert "transform" in styles
    assert "opacity" in styles

    for state in AGENT_MOTION_STATES:
        assert f"{state}:" in motion_source
        assert state in motion_test


def test_frontend_uses_generated_color_system_contract() -> None:
    """The perceptual color system is generated, checked, and kept out of the runtime bundle."""
    package = json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
    generated_css = COLOR_CSS_PATH.read_text(encoding="utf-8")
    generated_json = json.loads(COLOR_JSON_PATH.read_text(encoding="utf-8"))
    styles = (SRC_DIR / "styles.css").read_text(encoding="utf-8")

    assert COLOR_SPEC_PATH.exists()
    assert package["devDependencies"]["culori"] == "4.0.2"
    assert "culori" not in package["dependencies"]
    assert "colors:check" in package["scripts"]["build"]
    assert package["scripts"]["colors"] == "node scripts/generate_color_system.mjs"
    assert "audit:colors" in package["scripts"]["colors:check"]
    assert '@import "./design/color-system.generated.css";' in styles
    assert "@media (prefers-color-scheme: dark)" in generated_css
    assert "@media (prefers-contrast: more)" in generated_css
    assert "@media (color-gamut: p3)" in generated_css
    assert generated_json["toneStops"] == [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 98, 100]

    required_tokens = [
        "--df-bg",
        "--df-surface-1",
        "--df-surface-2",
        "--df-surface-3",
        "--df-text-1",
        "--df-text-2",
        "--df-line-strong",
        "--df-action-bg",
        "--df-focus-ring",
        "--df-info-bg",
        "--df-selection-bg",
        "--df-hover-bg",
        "--df-disabled-bg",
        "--df-loading-bg",
        "--df-status-safe-bg",
        "--df-status-review-bg",
        "--df-status-danger-bg",
        "--df-agent-bg",
        "--df-autonomy-bg",
        "--df-stage-active-bg",
        "--df-stage-complete-bg",
        "--df-stage-blocked-bg",
        "--df-stage-failed-bg",
        "--df-confidence-high-bg",
        "--df-confidence-medium-bg",
        "--df-confidence-low-bg",
        "--df-proof-bg",
        "--df-diff-old-bg",
        "--df-diff-new-bg",
    ]
    agent_states = list(AGENT_MOTION_STATES)
    for state in agent_states:
        required_tokens.extend(
            [
                f"--df-agent-{state}-bg",
                f"--df-agent-{state}-text",
                f"--df-agent-{state}-line",
            ]
        )
    for token in required_tokens:
        assert f"{token}:" in generated_css
        assert token in generated_json["semantic"]["light"]
        assert token in generated_json["semantic"]["dark"]

    assert generated_json["highContrast"]["light"]["--df-text-2"]["palette"] == "neutral-20"
    # brand-80 in light and brand-40 in dark, corrected 2026-08-14. These were swapped, and
    # the swap was a real accessibility defect: the light theme's --df-action-bg is brand-30
    # (#541507, dark), so brand-40 gave 1.46:1 against its own background -- below the 3:1
    # WCAG 1.4.11 requires for a control boundary, and a DOWNGRADE from the 3.43:1 of the
    # standard brand-60 border it overrides. Dark, whose action background is near-white
    # neutral-98, was 1.83:1. The mode that exists to raise contrast was lowering it in both
    # themes. It survived because auditContrast read system.semantic only and no axe scan ran
    # under prefers-contrast: more. Now 7.29:1 and 9.09:1, gated by auditHighContrastRatios.
    assert generated_json["highContrast"]["light"]["--df-action-border"]["palette"] == "brand-80"
    assert generated_json["highContrast"]["dark"]["--df-text-2"]["palette"] == "neutral-95"
    assert generated_json["highContrast"]["dark"]["--df-action-border"]["palette"] == "brand-40"


def test_apex_color_system_keeps_green_blue_and_black_out_of_primary_action() -> None:
    """Primary action is Aurelian Proof-led; black, blue, and green stay out of the identity."""
    generated_json = json.loads(COLOR_JSON_PATH.read_text(encoding="utf-8"))

    assert "brand" in generated_json["seeds"]
    assert "success" in generated_json["seeds"]
    assert "forge" not in generated_json["seeds"]
    assert "safe" not in generated_json["seeds"]
    assert generated_json["seeds"]["brand"]["c"] == 0.096
    assert generated_json["seeds"]["brand"]["h"] == 34
    assert generated_json["seeds"]["agent"]["c"] == 0.058
    assert generated_json["seeds"]["warning"]["c"] == 0.066
    assert generated_json["seeds"]["success"]["c"] <= 0.04
    assert 20 <= generated_json["seeds"]["brand"]["h"] <= 55
    assert not 190 <= generated_json["seeds"]["brand"]["h"] <= 270

    for theme in ("light", "dark"):
        semantic = generated_json["semantic"][theme]
        for token in ("--df-action-bg", "--df-action-bg-hover"):
            assert semantic[token]["palette"].startswith(("neutral-", "brand-"))
            assert not semantic[token]["palette"].startswith(
                ("data-", "success-", "safe-", "forge-")
            )
        assert semantic["--df-action-border"]["palette"].startswith("brand-")
        assert semantic["--df-status-safe-bg"]["palette"].startswith("neutral-")
        assert semantic["--df-status-safe-text"]["palette"].startswith("success-")

    assert generated_json["semantic"]["light"]["--df-action-bg"]["palette"] == "brand-30"
    assert generated_json["semantic"]["light"]["--df-action-bg-hover"]["palette"] == "brand-40"
    assert generated_json["semantic"]["light"]["--df-text-1"]["palette"] == "neutral-20"


def test_apex_color_system_avoids_light_theme_pastel_state_slabs() -> None:
    """Large light-mode state surfaces stay neutral; color is reserved for instrumentation."""
    generated_json = json.loads(COLOR_JSON_PATH.read_text(encoding="utf-8"))
    semantic = generated_json["semantic"]["light"]
    forbidden = {"brand-95", "data-95", "agent-95", "success-95", "warning-95", "danger-95"}
    large_state_backgrounds = [
        "--df-data-bg",
        "--df-info-bg",
        "--df-selection-bg",
        "--df-disabled-bg",
        "--df-loading-bg",
        "--df-agent-bg",
        "--df-autonomy-bg",
        "--df-stage-active-bg",
        "--df-stage-complete-bg",
        "--df-stage-blocked-bg",
        "--df-stage-failed-bg",
        "--df-confidence-high-bg",
        "--df-confidence-medium-bg",
        "--df-confidence-low-bg",
        "--df-proof-bg",
        "--df-status-safe-bg",
        "--df-status-review-bg",
        "--df-status-danger-bg",
        "--df-diff-old-bg",
        "--df-diff-new-bg",
    ]
    for state in AGENT_MOTION_STATES:
        large_state_backgrounds.append(f"--df-agent-{state}-bg")

    for token in large_state_backgrounds:
        palette = semantic[token]["palette"]
        assert palette.startswith("neutral-")
        assert palette not in forbidden


def test_frontend_has_no_raw_hand_authored_hex_colors() -> None:
    """All raw hex values must flow from generated color artifacts."""
    offenders: list[str] = []
    generated = {COLOR_CSS_PATH, COLOR_JSON_PATH}
    for path in SRC_DIR.rglob("*"):
        if path in generated or path.suffix not in {".css", ".ts", ".tsx"}:
            continue
        matches = re.findall(r"#[0-9a-fA-F]{3,8}\b", path.read_text(encoding="utf-8"))
        if matches:
            offenders.append(f"{path.relative_to(PROJECT_ROOT)}: {', '.join(matches)}")

    assert offenders == []
