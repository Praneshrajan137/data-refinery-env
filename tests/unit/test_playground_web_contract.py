"""Static contract tests for the playground frontend."""

from __future__ import annotations

import json
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = PROJECT_ROOT / "playground" / "web" / "index.html"
CONFIG_PATH = PROJECT_ROOT / "playground" / "web" / "public" / "config.js"
PACKAGE_PATH = PROJECT_ROOT / "playground" / "web" / "package.json"
SRC_DIR = PROJECT_ROOT / "playground" / "web" / "src"
COLOR_CSS_PATH = SRC_DIR / "design" / "color-system.generated.css"
COLOR_JSON_PATH = SRC_DIR / "design" / "color-system.generated.json"
COLOR_SPEC_PATH = PROJECT_ROOT / "specs" / "SPEC_color_system.md"


def test_index_uses_relative_asset_paths_and_config_contract() -> None:
    """The static frontend must be deployable from Cloudflare assets without HF static assumptions."""
    body = INDEX_PATH.read_text(encoding="utf-8")
    assert "/static/" not in body
    assert 'src="%BASE_URL%config.js"' in body
    assert 'src="/src/main.tsx"' in body


def test_config_js_exposes_backend_url_contract() -> None:
    """config.js defines the committed runtime contract for the backend URL."""
    body = CONFIG_PATH.read_text(encoding="utf-8")
    assert "window.__DATAFORGE_CONFIG__" in body
    assert "BACKEND_URL" in body
    assert 'BACKEND_URL: ""' not in body
    assert "https://Praneshrajan15-dataforge-playground.hf.space" in body


def test_frontend_stays_storage_free_and_capability_aware() -> None:
    """The frontend remains storage-free, route-aware, and consumes capability metadata."""
    body = "\n".join(
        path.read_text(encoding="utf-8") for path in SRC_DIR.rglob("*") if path.is_file()
    )
    assert "normalizeBackendUrl" in body
    assert "advanced_available" in body
    assert "streaming_available" in body
    assert "workflow_event_v1" in body
    assert "analyzeStream" in body
    assert "AbortController" in body
    assert "PRODUCT_ROUTES" in body
    assert "routeFromPathname" in body
    assert "pushState" in body
    assert "popstate" in body


def test_frontend_has_typed_vite_quality_gates() -> None:
    """The playground frontend is a typed Vite app with unit, browser, and budget gates."""
    body = PACKAGE_PATH.read_text(encoding="utf-8")
    assert '"vite"' in body
    assert '"typescript"' in body
    assert '"@playwright/test"' in body
    assert '"@axe-core/playwright"' in body
    assert '"budget"' in body


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
    for token in required_tokens:
        assert f"{token}:" in generated_css
        assert token in generated_json["semantic"]["light"]
        assert token in generated_json["semantic"]["dark"]


def test_apex_color_system_keeps_green_and_blue_out_of_primary_action() -> None:
    """Primary action is Mineral Intelligence-led; blue and green stay out of the identity."""
    generated_json = json.loads(COLOR_JSON_PATH.read_text(encoding="utf-8"))

    assert "brand" in generated_json["seeds"]
    assert "success" in generated_json["seeds"]
    assert "forge" not in generated_json["seeds"]
    assert "safe" not in generated_json["seeds"]
    assert generated_json["seeds"]["success"]["c"] <= 0.04
    assert 20 <= generated_json["seeds"]["brand"]["h"] <= 55
    assert not 190 <= generated_json["seeds"]["brand"]["h"] <= 270

    for theme in ("light", "dark"):
        semantic = generated_json["semantic"][theme]
        for token in ("--df-action-bg", "--df-action-bg-hover"):
            assert semantic[token]["palette"].startswith(("neutral-", "brand-"))
            assert not semantic[token]["palette"].startswith(("data-", "success-", "safe-", "forge-"))
        assert semantic["--df-action-border"]["palette"].startswith("brand-")
        assert semantic["--df-status-safe-bg"]["palette"].startswith("neutral-")
        assert semantic["--df-status-safe-text"]["palette"].startswith("success-")


def test_apex_color_system_avoids_light_theme_pastel_state_slabs() -> None:
    """Large light-mode state surfaces stay neutral; color is reserved for instrumentation."""
    generated_json = json.loads(COLOR_JSON_PATH.read_text(encoding="utf-8"))
    semantic = generated_json["semantic"]["light"]
    forbidden = {"brand-95", "data-95", "agent-95", "success-95", "warning-95", "danger-95"}
    large_state_backgrounds = [
        "--df-data-bg",
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
