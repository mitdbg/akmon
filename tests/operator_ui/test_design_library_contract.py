from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
UI_ROOT = ROOT / "src" / "okg" / "operator_ui" / "web" / "src" / "operator-ui"
STYLES_ROOT = UI_ROOT / "styles"
ARCHIVED_CONSOLE_PACKAGE = "@okg" + "/console"

# The UI tree carries binary assets (the Hellenic motif band). Scan only the
# text sources when grepping for forbidden imports; `read_text()` over a PNG
# raises UnicodeDecodeError and would fail the contract for the wrong reason.
TEXT_SOURCE_SUFFIXES = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".css", ".json", ".md", ".html"}


REQUIRED_TOKENS = {
    "--okg-color-bg",
    "--okg-color-surface",
    "--okg-color-surface-muted",
    "--okg-color-rail",
    "--okg-color-text",
    "--okg-color-muted",
    "--okg-color-line",
    "--okg-color-link",
    "--okg-color-ok",
    "--okg-color-warn",
    "--okg-color-bad",
    "--okg-color-info",
    "--okg-color-canvas",
    "--okg-color-canvas-grid",
    "--okg-font-sans",
    "--okg-font-mono",
    "--okg-text-xs",
    "--okg-text-sm",
    "--okg-text-md",
    "--okg-text-lg",
    "--okg-space-1",
    "--okg-space-2",
    "--okg-space-3",
    "--okg-space-4",
    "--okg-radius-sm",
    "--okg-radius-md",
    "--okg-radius-lg",
    "--okg-layout-rail",
    "--okg-layout-topbar",
}


def test_operator_ui_package_scripts_are_present() -> None:
    package = json.loads((ROOT / "package.json").read_text())

    assert package["private"] is True
    assert package["scripts"]["operator-ui:build"]
    assert package["scripts"]["operator-ui:test"]
    assert ARCHIVED_CONSOLE_PACKAGE not in package.get("dependencies", {})
    assert ARCHIVED_CONSOLE_PACKAGE not in package.get("devDependencies", {})


def test_token_contract_is_declared() -> None:
    tokens = (STYLES_ROOT / "tokens.css").read_text()

    missing = sorted(token for token in REQUIRED_TOKENS if token not in tokens)
    assert missing == []


CSS_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
# `var(--token, #fallback)` USES the token — the literal is the fallback the
# spec requires you to spell out. Only a literal standing on its own is untokenized.
VAR_FALLBACK_RE = re.compile(r"var\(\s*--[\w-]+\s*,[^()]*?\)")
HEX_RE = re.compile(r"#[0-9a-fA-F]{3,8}\b")

# The theme-picker chips preview the theme you are switching TO, so they must
# render that theme's colors while a DIFFERENT theme is active. A token resolves
# to the active theme, which is definitionally the wrong answer here.
THEME_SWATCH_RE = re.compile(r"\.okg-theme-swatch--[\w-]+\s*\{[^}]*\}", re.DOTALL)


def _untokenized_colors(css: str) -> list[str]:
    """Color literals that stand alone rather than backing a design token."""
    stripped = CSS_COMMENT_RE.sub(" ", css)
    stripped = THEME_SWATCH_RE.sub(" ", stripped)
    stripped = VAR_FALLBACK_RE.sub(" ", stripped)
    return HEX_RE.findall(stripped)


def test_primitive_css_uses_tokens_for_colors() -> None:
    primitives = (STYLES_ROOT / "primitives.css").read_text()

    assert _untokenized_colors(primitives) == []


def test_untokenized_color_rule_still_catches_a_raw_literal() -> None:
    """The exemptions above must not make the rule vacuous."""
    assert _untokenized_colors(".a { color: var(--okg-color-text, #abcdef); }") == []
    assert _untokenized_colors("/* was #abcdef */") == []
    assert _untokenized_colors(".a { color: #abcdef; }") == ["#abcdef"]
    assert _untokenized_colors(".a { border: 1px solid #123; }") == ["#123"]


def test_active_operator_ui_does_not_import_archived_ui() -> None:
    active_sources = "\n".join(
        path.read_text()
        for path in UI_ROOT.rglob("*")
        if path.is_file() and path.suffix in TEXT_SOURCE_SUFFIXES
    )

    forbidden = [
        "archive/ui/legacy-viz-2026-06-17",
        ARCHIVED_CONSOLE_PACKAGE,
        "supervisor_console",
        "src/okg/viz/web",
    ]
    for marker in forbidden:
        assert marker not in active_sources
