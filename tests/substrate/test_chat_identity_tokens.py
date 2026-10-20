"""The accent is an operator-console token, and only that (PACT chat-manifest-block).

This file was written to pin a SHARED identity token: one accent declaration
feeding both graphical surfaces, with a three-state precedence so the console
and the chat site could not drift. The recorded knob census falsified the
premise. Open WebUI v0.11.0 exposes no accent config key, no accent env var,
and no admin API; the only lever is a static ``custom.css`` that delivers
opaque bytes and cannot be asked what accent it set. ``theme.base`` is
browser-local. So the chat surface cannot receive an accent at all.

What survives is the coupling that existed before any of this work:
``viz.theme.accent`` → ``theme_accent`` on the console's deployment-switcher
card. The three-state precedence collapses to its first state, which these
tests still pin. The other two states described a second consumer that does
not exist, so what replaces them is a REFUSAL: a manifest declaring
``chat.ui.theme.accent`` is rejected rather than silently ignored, because an
operator who writes a color in the file and never sees it on the screen has
been lied to by the config format.

Whether losing per-deployment chat accent is acceptable is an open operator
decision on the program's census floor. These tests assert what was measured,
not what should be done about it.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.deployments import DeploymentConfigError, DeploymentManifest
from okg.substrate.deployments.chat_block import (
    ChatManifestError,
    resolve_theme_accent,
)
from okg.viz.registry import _read_manifest_meta, current_deployment

REPO_ROOT = Path(__file__).resolve().parents[2]
OKG_WORKSPACE_MANIFEST = (
    REPO_ROOT / "deployments" / "okg-workspace" / "deployment.yaml"
)

#: okg-workspace's accent as committed. This PACT modifies no in-repo
#: deployment manifest, so this value must survive the change untouched.
OKG_WORKSPACE_ACCENT = "#10b981"


def _chat_block(accent: str | None) -> dict:
    ui: dict = {"site_name": "Fixture Chat"}
    if accent is not None:
        ui["theme"] = {"base": "light", "accent": accent}
    return {
        "enabled": True,
        "ui": ui,
        "models": {"builtin": ["anthropic/claude-sonnet-5"]},
        "preset": {"display_name": "Fixture Chat"},
        "search_profile": "analyst",
        "mcp": {"port": 8101, "tools": ["inspect", "search"]},
    }


def _write_deployment(
    tmp_path: Path,
    *,
    viz: dict | None = None,
    chat: dict | None = None,
    name: str = "fixture-deployment",
) -> Path:
    dep = tmp_path / name
    (dep / "schema").mkdir(parents=True, exist_ok=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    document: dict = {
        "name": name,
        "description": "fixture",
        "schema_dir": "schema",
        "source_registry": "source_registry.yaml",
        "search": {"profiles": {"analyst": {"subtypes": ["chunk"]}}},
    }
    if viz is not None:
        document["viz"] = viz
    if chat is not None:
        document["chat"] = chat
    manifest_path = dep / "deployment.yaml"
    manifest_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return manifest_path


# ---------------------------------------------------------------------------
# What survives: the console coupling
# ---------------------------------------------------------------------------

def test_viz_theme_only_resolves_unchanged(tmp_path: Path) -> None:
    """The surviving state: `viz.theme` alone resolves exactly as it did before.

    This is the one arm of the original three-state precedence that describes
    a coupling which exists. It is also the arm that matters for
    behavior-preservation: the console must be untouched by all of this.
    """
    manifest_path = _write_deployment(
        tmp_path, viz={"port": 8901, "theme": {"accent": "#ff8800"}},
    )
    DeploymentManifest.from_yaml(manifest_path)

    meta = _read_manifest_meta(manifest_path)
    assert meta["theme_accent"] == "#ff8800"
    assert meta["port"] == 8901

    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert current_deployment(
        manifest=raw, bound_port=8901,
    )["theme_accent"] == "#ff8800"


def test_viz_theme_absent_resolves_to_no_accent(tmp_path: Path) -> None:
    """A manifest declaring neither surface's accent still resolves to None.

    Pins the untouched baseline, so the two states below are read as changes
    rather than as a resolver that returns something no matter what.
    """
    manifest_path = _write_deployment(tmp_path, viz={"port": 8901})
    assert _read_manifest_meta(manifest_path)["theme_accent"] is None


def test_chat_block_alone_supplies_no_accent(tmp_path: Path) -> None:
    """A valid chat block carries no accent, and the console is unaffected.

    Replaces the old "a chat block alone feeds both consumers": the census
    measured that a chat site cannot be given an accent at this version, so a
    chat-only deployment has no accent anywhere — not one the console
    borrows, and not one the chat site renders.
    """
    manifest_path = _write_deployment(tmp_path, chat=_chat_block(None))
    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.chat.enabled is True
    assert manifest.chat.accent is None
    assert not hasattr(manifest.chat.ui, "theme")
    assert _read_manifest_meta(manifest_path)["theme_accent"] is None


def test_chat_side_accent_is_refused_rather_than_ignored(
    tmp_path: Path,
) -> None:
    """Declaring `chat.ui.theme.accent` raises, naming the measured reason.

    Replaces the old "disagreeing accents raise". The refusal is now wider,
    because the reason is stronger: the value cannot render regardless of what
    the console declares. Silently dropping it would leave the operator
    looking at a color in their manifest that never reaches a screen.
    """
    manifest_path = _write_deployment(
        tmp_path,
        viz={"theme": {"accent": "#ff8800"}},
        chat=_chat_block("#10b981"),
    )

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    message = str(excinfo.value)
    assert "theme" in message
    assert "not drivable" in message.lower()
    # A DeploymentConfigError too, so every existing manifest-load caller —
    # `okg lint deployments` included — sees it as the load-contract
    # rejection it is rather than as an unexpected crash.
    assert isinstance(excinfo.value, DeploymentConfigError)


def test_agreeing_chat_accent_is_refused_too(tmp_path: Path) -> None:
    """Agreement with the console does not make the value renderable.

    This is the arm that changed meaning most. It used to assert that the
    same accent stated twice loads fine — one value, stated redundantly.
    After the census it must refuse: agreement says nothing about whether the
    chat site can apply the value, and it cannot.
    """
    manifest_path = _write_deployment(
        tmp_path,
        viz={"theme": {"accent": "#10b981"}},
        chat=_chat_block("#10b981"),
    )
    with pytest.raises(ChatManifestError):
        DeploymentManifest.from_yaml(manifest_path)


@pytest.mark.parametrize("falsy", ["", 0, False, None])
def test_a_falsy_chat_accent_is_still_a_declaration(falsy: object) -> None:
    """`accent: ""` is refused too — presence, not truthiness.

    Reading these as "undeclared" would let exactly the malformed
    declarations through unrefused, which is the silent drop this rule
    exists to prevent.
    """
    raw = {
        "name": "fixture-deployment",
        "chat": {"ui": {"theme": {"accent": falsy}}},
    }
    with pytest.raises(ChatManifestError):
        resolve_theme_accent(raw)


def test_a_chat_theme_without_an_accent_key_is_not_a_declaration() -> None:
    """`theme: {base: light}` declares no accent, so the resolver is silent.

    The parser still refuses the `theme` key itself; this pins that the
    resolver's rule is about the accent specifically and has not widened into
    "any chat block raises", which would make the refusal untestable.
    """
    raw = {
        "name": "fixture-deployment",
        "viz": {"theme": {"accent": "#ff8800"}},
        "chat": {"ui": {"theme": {"base": "light"}}},
    }
    assert resolve_theme_accent(raw) == "#ff8800"


def test_resolver_refuses_a_chat_accent_on_the_raw_path(
    tmp_path: Path,
) -> None:
    """The resolver refuses a chat accent even reading raw YAML directly.

    Two paths can see a manifest: the strict parser and the switcher's
    tolerant sibling enumeration, which never calls the parser. Both must
    refuse, or the console would end up rendering a value the schema rejects.
    """
    raw = {
        "name": "fixture-deployment",
        "viz": {"theme": {"accent": "#ff8800"}},
        "chat": {"ui": {"theme": {"accent": "#10b981"}}},
    }
    with pytest.raises(ChatManifestError):
        resolve_theme_accent(raw)

    # Same manifest through the switcher: no accent, and no winner picked.
    manifest_path = _write_deployment(
        tmp_path,
        viz={"theme": {"accent": "#ff8800"}},
        chat=_chat_block("#10b981"),
    )
    meta = _read_manifest_meta(manifest_path)
    assert meta["theme_accent"] is None
    assert meta["name"] == "fixture-deployment"


# ---------------------------------------------------------------------------
# Behavior preservation on the one live consumer
# ---------------------------------------------------------------------------

def test_okg_workspace_accent_resolves_to_current_value() -> None:
    """The live deployment's accent is byte-identical with a chat block present.

    okg-workspace now declares one (chat-sync-projection landed the first
    in-repo chat block). The interesting arm is therefore no longer "no chat
    block exists" but "a chat block exists and declares NO theme", which is
    what keeps the accent an operator-console token: the census measured the
    chat accent as not drivable at the pinned version, so nothing in the chat
    block may claim one.
    """
    raw = yaml.safe_load(OKG_WORKSPACE_MANIFEST.read_text(encoding="utf-8"))
    assert raw["viz"]["theme"]["accent"] == OKG_WORKSPACE_ACCENT
    assert "theme" not in raw["chat"]["ui"]

    assert resolve_theme_accent(raw) == OKG_WORKSPACE_ACCENT
    meta = _read_manifest_meta(OKG_WORKSPACE_MANIFEST)
    assert meta["theme_accent"] == OKG_WORKSPACE_ACCENT
    assert current_deployment(
        manifest=raw, bound_port=8765,
    )["theme_accent"] == OKG_WORKSPACE_ACCENT


def test_registry_tolerates_an_unreadable_manifest(tmp_path: Path) -> None:
    """A manifest that cannot be parsed still yields a renderable record."""
    dep = tmp_path / "broken"
    dep.mkdir(parents=True)
    manifest_path = dep / "deployment.yaml"
    manifest_path.write_text("name: [unclosed\n", encoding="utf-8")

    meta = _read_manifest_meta(manifest_path)
    assert meta["theme_accent"] is None
    assert meta["name"] == "broken"
