"""The console's deployment-theme read (PACT ``operator-ui-deployment-theme``).

The console reads deployment identity from ``/viz/api/deployments`` and from
nowhere else, so these tests exercise ``okg.viz.registry`` — the module that
endpoint calls — rather than a component fed a fixture prop, which would prove
nothing about the path the browser actually takes.

What is under test is the honesty of the states, not just the happy path. A
deployment that declares tokens must serve them verbatim; a deployment that
declares none must say so; a deployment whose manifest could not be read must
say THAT, distinctly, because the console's built-in palettes are the fallback
for both absences and only one of them is a claim about the operator's file.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.deployments import DeploymentManifest
from okg.substrate.deployments.chat_block import (
    VIZ_THEME_POLARITIES,
    VIZ_THEME_POLARITY_DARK,
    VIZ_THEME_POLARITY_LIGHT,
    VIZ_THEME_SURFACE_FAMILY,
    VIZ_THEME_TOKENS,
    VizThemeIncoherent,
    VizThemeInvalid,
    VizThemeState,
    _POLARITY_MIDPOINT_LUMINANCE,
    parse_viz_theme,
    theme_color_luminance,
    theme_polarity,
)
from okg.viz.registry import current_deployment, load_registry
from tests.mirror_scope import is_public_copy

#: A COHERENT full declaration: a dark ground with the surface family that
#: ground implies. Since the 2026-08-18 operator ruling, the dark five-token
#: version of this fixture is a load-time refusal rather than a served theme,
#: so it is not a valid stand-in for "a declared deployment" any more.
DECLARED_TOKENS = {
    "background": "#121417",
    "accent": "#ff8800",
    "fg": "#eef1f4",
    "fg_faint": "#8d98a5",
    "border": "#2a3138",
    "surface": "#1a1e23",
    "rail": "#0b0d10",
}

#: The whole served payload for a deployment nobody could read a manifest for.
#: Written out rather than spot-checked so a field added on the server without
#: a browser reader fails here instead of arriving unnoticed.
_UNRESOLVED = {"status": "unresolved", "tokens": {}, "polarity": None}


def _write_manifest(root: Path, name: str, body: str) -> Path:
    """Write a bare deployment.yaml for the REGISTRY path.

    The switcher reads sibling manifests as raw YAML and never calls the
    strict loader, so these fixtures deliberately carry only the keys under
    test — that tolerance is itself part of the contract.
    """
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / "deployment.yaml"
    manifest_path.write_text(body)
    return manifest_path


def _write_loadable_deployment(tmp_path: Path, viz: dict) -> Path:
    """Materialize a deployment dir the STRICT loader will accept.

    ``DeploymentManifest.from_yaml`` is the real manifest-load path, so the
    refusal tests must go through it rather than calling the parser directly.
    It requires the structural keys below; the ``viz`` block is the part under
    test.
    """
    dep = tmp_path / "fixture-deployment"
    (dep / "schema").mkdir(parents=True, exist_ok=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    manifest_path = dep / "deployment.yaml"
    manifest_path.write_text(
        yaml.safe_dump({
            "name": "fixture-deployment",
            "schema_dir": "schema",
            "source_registry": "source_registry.yaml",
            "viz": viz,
        }),
        encoding="utf-8",
    )
    return manifest_path


def _loaded_theme(manifest_path: Path) -> VizThemeState:
    """The theme state of a manifest that went through the STRICT loader.

    Two steps on purpose. ``DeploymentManifest.from_yaml`` is the real load
    path and proves the fixture is admissible at all — a fixture that only ever
    reached ``parse_viz_theme`` could be one the loader refuses. It keeps
    ``viz`` as the untyped dict it has always been, so the typed state is then
    parsed from that block, exactly as ``okg.viz.registry`` does when serving.
    """
    manifest = DeploymentManifest.from_yaml(manifest_path)
    return parse_viz_theme({"viz": manifest.viz}, manifest_path=manifest_path)


def _entry(entries: list[dict], name: str) -> dict:
    matches = [entry for entry in entries if entry["name"] == name]
    assert len(matches) == 1, f"expected exactly one {name!r} entry: {entries}"
    return matches[0]


# ---------------------------------------------------------------------------
# Declared: the tokens arrive verbatim
# ---------------------------------------------------------------------------

def test_a_declared_theme_is_served_verbatim_under_the_declared_marker(
    tmp_path: Path,
) -> None:
    """The whole point: what the operator wrote is what the console receives.

    Asserted as an equality on the entire token mapping rather than a spot
    check on the accent, so a token that is dropped, reordered into a
    different key, or "corrected" on the way out fails here.
    """
    _write_manifest(
        tmp_path,
        "themed",
        "name: themed\n"
        "viz:\n"
        "  port: 8770\n"
        "  theme:\n"
        + "".join(
            f'    {key}: "{value}"\n' for key, value in DECLARED_TOKENS.items()
        ),
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="themed", current_port=8770,
    )

    theme = _entry(entries, "themed")["viz_theme"]
    assert theme["status"] == "declared"
    assert theme["tokens"] == DECLARED_TOKENS


def test_a_partial_declaration_serves_only_what_was_declared(
    tmp_path: Path,
) -> None:
    """Each token is optional. The four undeclared ones are ABSENT, not
    filled from the console's palette — a default arriving under the
    `declared` marker would be the exact defect this change removes."""
    _write_manifest(
        tmp_path,
        "partial",
        'name: partial\nviz:\n  theme:\n    accent: "#ff8800"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="partial", current_port=8765,
    )

    theme = _entry(entries, "partial")["viz_theme"]
    assert theme["status"] == "declared"
    assert theme["tokens"] == {"accent": "#ff8800"}


def test_the_current_deployment_record_carries_the_declared_tokens() -> None:
    payload = current_deployment(
        manifest={"name": "themed", "viz": {"theme": dict(DECLARED_TOKENS)}},
        bound_port=8765,
    )

    assert payload["viz_theme"] == {
        "status": "declared",
        "tokens": DECLARED_TOKENS,
        "polarity": "dark",
    }


def test_the_flat_accent_field_still_serves_the_switcher_card(
    tmp_path: Path,
) -> None:
    """No regression on the read this endpoint already had.

    ``theme_accent`` predates the typed set and the deployment-switcher card
    still reads it; the typed tokens are served ALONGSIDE it, not instead.
    """
    _write_manifest(
        tmp_path,
        "themed",
        'name: themed\nviz:\n  theme:\n    accent: "#ff8800"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="themed", current_port=8765,
    )

    entry = _entry(entries, "themed")
    assert entry["theme_accent"] == "#ff8800"
    assert entry["viz_theme"]["tokens"]["accent"] == "#ff8800"


# ---------------------------------------------------------------------------
# Undeclared: an explicit state, with no token values
# ---------------------------------------------------------------------------

def test_a_manifest_without_a_theme_serves_the_undeclared_state(
    tmp_path: Path,
) -> None:
    _write_manifest(tmp_path, "bare", "name: bare\nviz:\n  port: 8765\n")

    entries = load_registry(
        deployments_root=tmp_path, current_name="bare", current_port=8765,
    )

    assert _entry(entries, "bare")["viz_theme"] == {
        "status": "undeclared",
        "tokens": {},
        "polarity": None,
    }


def test_a_manifest_with_no_viz_block_at_all_is_undeclared(
    tmp_path: Path,
) -> None:
    _write_manifest(tmp_path, "novis", "name: novis\n")

    entries = load_registry(
        deployments_root=tmp_path, current_name="novis", current_port=8765,
    )

    assert _entry(entries, "novis")["viz_theme"]["status"] == "undeclared"
    assert _entry(entries, "novis")["viz_theme"]["tokens"] == {}


def test_every_entry_carries_the_field_so_the_browser_sees_no_undefined(
    tmp_path: Path,
) -> None:
    """An absent KEY and an explicit state would be two different absences for
    the console to tell apart. Every entry carries the field."""
    _write_manifest(
        tmp_path, "a", 'name: a\nviz:\n  theme:\n    accent: "#ff8800"\n',
    )
    _write_manifest(tmp_path, "b", "name: b\n")

    entries = load_registry(
        deployments_root=tmp_path, current_name="a", current_port=8765,
    )

    assert all("viz_theme" in entry for entry in entries)
    assert {entry["name"]: entry["viz_theme"]["status"] for entry in entries} == {
        "a": "declared",
        "b": "undeclared",
    }


# ---------------------------------------------------------------------------
# Unresolved: no manifest was read, so nothing may be claimed about one
# ---------------------------------------------------------------------------

def test_synthesized_entries_report_unresolved_not_undeclared(
    tmp_path: Path,
) -> None:
    """The registry synthesizes an entry for a current deployment living
    outside the enumerated root, and a placeholder for an empty registry.
    Neither opened a file, so neither may say the deployment declares no
    theme."""
    outside = load_registry(
        deployments_root=tmp_path, current_name="external", current_port=9000,
    )
    empty = load_registry(
        deployments_root=None, current_name=None, current_port=None,
    )

    assert outside[0]["viz_theme"] == _UNRESOLVED
    assert empty[0]["viz_theme"] == _UNRESOLVED


def test_current_deployment_without_a_manifest_is_unresolved() -> None:
    payload = current_deployment(manifest=None, bound_port=8765)

    assert payload["viz_theme"] == _UNRESOLVED


def test_a_malformed_sibling_is_unresolved_and_does_not_hide_the_rest(
    tmp_path: Path,
) -> None:
    """The switcher enumerates OTHER deployments' manifests tolerantly. A
    sibling whose theme is refused serves `unresolved` — never `undeclared`,
    which would report a rejected file as declaring nothing — and the healthy
    sibling still renders.

    This also pins the ONE place the two accent reads disagree, so the
    rendering task does not discover it in a browser. ``theme_accent`` is the
    pre-existing flat field and is deliberately unchanged: it validates
    nothing and hands the switcher card whatever string the manifest held.
    The typed read refuses that same value. A consumer wanting the checked
    accent must take it from ``viz_theme``, not from ``theme_accent``.
    """
    _write_manifest(
        tmp_path, "broken", 'name: broken\nviz:\n  theme:\n    accent: "not a color!"\n',
    )
    _write_manifest(
        tmp_path, "healthy", 'name: healthy\nviz:\n  theme:\n    accent: "#ff8800"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="healthy", current_port=8765,
    )

    broken = _entry(entries, "broken")
    assert broken["viz_theme"] == _UNRESOLVED
    assert _entry(entries, "healthy")["viz_theme"]["status"] == "declared"

    # The documented divergence, pinned: the untyped flat field still carries
    # the unvalidated string the typed read just refused.
    assert broken["theme_accent"] == "not a color!"


def test_an_incoherent_sibling_is_unresolved_not_declared(tmp_path: Path) -> None:
    """The new refusal reaches the switcher on the same path the old one does.

    A deployment whose theme is refused for INCOHERENCE must serve
    `unresolved` like any other refusal — never `declared` with the half-set
    it wrote, which would put the console right back on the unreadable render
    the refusal exists to prevent.
    """
    _write_manifest(
        tmp_path,
        "half-dark",
        'name: half-dark\nviz:\n  theme:\n    background: "#121417"\n',
    )
    _write_manifest(
        tmp_path,
        "whole-dark",
        'name: whole-dark\nviz:\n  theme:\n'
        '    background: "#121417"\n    surface: "#1a1e23"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="whole-dark", current_port=8765,
    )

    assert _entry(entries, "half-dark")["viz_theme"] == _UNRESOLVED
    assert _entry(entries, "whole-dark")["viz_theme"] == {
        "status": "declared",
        "tokens": {"background": "#121417", "surface": "#1a1e23"},
        "polarity": "dark",
    }


# ---------------------------------------------------------------------------
# Malformed values are refused at manifest load, with a named error
# ---------------------------------------------------------------------------

MALFORMED_THEMES = {
    "unparseable_string": {"accent": "not a color!"},
    "truncated_hex": {"accent": "#12345"},
    "css_injection": {"accent": "#fff; background: url(http://evil)"},
    "number": {"accent": 16711680},
    "boolean": {"accent": True},
    "nested_mapping": {"accent": {"value": "#ff8800"}},
    "list": {"accent": ["#ff8800"]},
    "empty_string": {"accent": ""},
    "whitespace_only": {"accent": "   "},
    "unknown_token": {"acccent": "#ff8800"},
    "theme_not_a_mapping": "#ff8800",
    "empty_theme": {},
    "null_theme": None,
}


@pytest.mark.parametrize("case", sorted(MALFORMED_THEMES))
def test_a_malformed_token_is_refused_at_manifest_load(
    tmp_path: Path, case: str,
) -> None:
    """Refused where the operator can see it, by the named error.

    Driven through ``DeploymentManifest.from_yaml`` — the real load path — so
    this proves the refusal reaches the CLI, lint, publish and sync, not just
    a directly-called parser. ``VizThemeInvalid`` carries the stable
    ``viz_theme_invalid`` wire code and is a ``DeploymentConfigError``, so
    ``okg lint deployments`` catches it as the load-contract rejection it is.
    """
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": MALFORMED_THEMES[case]},
    )

    with pytest.raises(VizThemeInvalid) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.code == "viz_theme_invalid"
    assert excinfo.value.data["path"].startswith("viz.theme")


def test_the_refusal_names_the_offending_token_and_the_manifest(
    tmp_path: Path,
) -> None:
    """A refusal an operator cannot act on is a refusal that costs them a
    bisect. The message names the path and the file."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"fg_faint": "octarine!"}},
    )

    with pytest.raises(VizThemeInvalid) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.data["path"] == "viz.theme.fg_faint"
    assert "octarine!" in str(excinfo.value)
    assert str(manifest_path) in str(excinfo.value)


def test_a_malformed_token_is_never_normalized(tmp_path: Path) -> None:
    """The refusal is the whole behavior: nothing downstream receives a
    repaired value. Asserted as the absence of a successful load, because a
    normalizing implementation would pass every other test in this file."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"accent": "#12345"}},
    )

    with pytest.raises(VizThemeInvalid):
        DeploymentManifest.from_yaml(manifest_path)

    assert parse_viz_theme({"viz": {}}).tokens == {}


@pytest.mark.parametrize(
    "value",
    ["#fff", "#ffff", "#10b981", "#10b981ff", "rgb(16 185 129)",
     "rgba(16, 185, 129, 0.5)", "hsl(160 84% 39%)", "transparent",
     "rebeccapurple", "currentColor"],
)
def test_admissible_color_forms_load(tmp_path: Path, value: str) -> None:
    """The grammar admits every CSS color form an operator would plausibly
    write. Over-refusal here would reject working manifests."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"accent": value}},
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.viz["theme"]["accent"] == value
    assert parse_viz_theme({"viz": {"theme": {"accent": value}}}).tokens == {
        "accent": value,
    }


def test_the_shipped_okg_workspace_theme_still_loads() -> None:
    """The real manifest must not be refused by the typed validator. A
    validator that refuses the deployment we actually run is a validator
    that gets reverted, not adopted.

    Operator decision 2026-08-18: okg-workspace declares ACCENT-ONLY. The
    earlier dark token set predated the console reading this block at all,
    and once the base-firing semantics wired it up it flipped the whole
    console dark — a look nobody had chosen. Accent-only has no polarity,
    never moves the base palette, and this test fails if someone re-adds a
    ground token without going back through that decision.
    """
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = repo_root / "deployments" / "okg-workspace" / "deployment.yaml"
    if not manifest_path.exists():
        pytest.skip("okg-workspace manifest not present in this checkout")

    raw = yaml.safe_load(manifest_path.read_text())
    state = parse_viz_theme(raw, manifest_path=manifest_path)

    assert state.status == "declared"
    assert set(state.tokens) == {"accent"}, (
        "okg-workspace is accent-only by operator decision (2026-08-18); "
        f"declared: {sorted(state.tokens)}"
    )
    assert state.tokens["accent"] == "#10b981"


def test_every_checked_in_deployment_manifest_still_loads() -> None:
    """The sweep, standing rather than one-off.

    Widening a token set and adding a cross-token refusal can break a manifest
    that has nothing to do with the change. Only `okg-workspace` declares a
    theme today, but this is the check that notices when the next one does and
    declares it incoherently.
    """
    repo_root = Path(__file__).resolve().parents[2]
    manifests = sorted((repo_root / "deployments").glob("*/deployment.yaml"))
    if not manifests:
        pytest.skip("no deployment manifests in this checkout")

    declared = []
    for path in manifests:
        raw = yaml.safe_load(path.read_text())
        if not isinstance(raw, dict):
            continue
        # Raises VizThemeInvalid / VizThemeIncoherent on a broken manifest;
        # letting it propagate names the offender in the failure.
        if parse_viz_theme(raw, manifest_path=path).status == "declared":
            declared.append(path.parent.name)

    # okg-workspace is this repository's own deployment and is not in the
    # public copy, where no shipped deployment declares a theme yet.
    expected = [] if is_public_copy() else ["okg-workspace"]
    assert declared == expected, (
        "a second deployment now declares a theme — extend the browser proof "
        f"to cover it: {declared}"
    )


# ---------------------------------------------------------------------------
# Scope: the refusal stops at viz.theme's boundary
# ---------------------------------------------------------------------------

def test_a_viz_block_with_unrelated_keys_and_no_theme_still_loads(
    tmp_path: Path,
) -> None:
    """The scope check. ``viz`` stays an untyped pass-through dict: ports,
    presentation presets and layout tuning are neither typed nor refused by
    this change, and a ``viz`` with no theme at all loads exactly as before.
    """
    manifest_path = _write_loadable_deployment(
        tmp_path,
        {
            "port": 8770,
            "use_presentation_presets": True,
            "presentation": {"explore": "cluster", "ontology": "groups"},
            "some_future_key": [1, 2, 3],
        },
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.viz["port"] == 8770
    assert manifest.viz["presentation"] == {
        "explore": "cluster", "ontology": "groups",
    }
    assert manifest.viz["some_future_key"] == [1, 2, 3]
    assert parse_viz_theme({"viz": manifest.viz}).status == "undeclared"


def test_unrelated_viz_keys_survive_alongside_a_declared_theme(
    tmp_path: Path,
) -> None:
    """Typing the theme must not prune its neighbors out of the block."""
    manifest_path = _write_loadable_deployment(
        tmp_path,
        {
            "port": 8770,
            "some_future_key": "keep-me",
            "theme": {"accent": "#ff8800"},
        },
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.viz["some_future_key"] == "keep-me"
    assert manifest.viz["port"] == 8770
    assert parse_viz_theme({"viz": manifest.viz}).tokens == {"accent": "#ff8800"}


def test_the_token_set_is_the_seven_the_console_reads() -> None:
    """Pinned so a token added to the parser without a console consumer, or
    dropped from one without the other, fails here.

    The surface family is APPENDED. The browser asserts the same tuple in the
    same order (`VIZ_THEME_TOKENS` in `styles/deploymentTheme.ts`), so the two
    sides cannot drift silently.
    """
    assert VIZ_THEME_TOKENS == (
        "background", "accent", "fg", "fg_faint", "border", "surface", "rail",
    )
    assert VIZ_THEME_SURFACE_FAMILY == ("surface", "rail")
    assert set(VIZ_THEME_SURFACE_FAMILY) <= set(VIZ_THEME_TOKENS)


# ---------------------------------------------------------------------------
# Polarity coherence: a declared ground must name the surfaces standing on it
#
# The operator ruling of 2026-08-18 (recorded in the PACT's scope_rule). The
# measured defect: `okg-workspace` declared a dark background, the console's
# default palette is light, `--okg-color-surface` was outside the token set,
# and the accessibility audit went from 3 violations to 1102 — 1099 of them
# color-contrast, dark ink stranded on near-white cards. The set is now wide
# enough to express the coherent declaration, and the incoherent one is
# refused at load rather than rendered.
# ---------------------------------------------------------------------------

def test_a_dark_background_without_a_surface_is_refused(tmp_path: Path) -> None:
    """The exact declaration that produced the defect, now a load-time error.

    Driven through the real loader, so the refusal reaches the CLI, lint,
    publish and sync — not just a directly-called parser.
    """
    manifest_path = _write_loadable_deployment(
        tmp_path,
        {"theme": {"background": "#121417", "fg": "#eef1f4"}},
    )

    with pytest.raises(VizThemeIncoherent) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.code == "viz_theme_incoherent"
    assert excinfo.value.data["reason"] == "polarity_flip_without_surface"


def test_the_refusal_names_both_tokens_and_the_manifest(tmp_path: Path) -> None:
    """A refusal that named only the background would send the operator to
    delete the one token they meant. The message names the token that is
    wrong, the token that is missing, and the file."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": "#121417"}},
    )

    with pytest.raises(VizThemeIncoherent) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    message = str(excinfo.value)
    assert "viz.theme.background" in message
    assert "viz.theme.surface" in message
    assert str(manifest_path) in message
    assert excinfo.value.data["requires"] == "surface"


def test_declaring_the_surface_makes_the_dark_background_loadable(
    tmp_path: Path,
) -> None:
    """The refusal is not a ban on dark themes — it is a ban on HALF of one."""
    manifest_path = _write_loadable_deployment(
        tmp_path,
        {"theme": {"background": "#121417", "surface": "#1a1e23", "rail": "#0b0d10"}},
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert parse_viz_theme({"viz": manifest.viz}).tokens == {
        "background": "#121417", "surface": "#1a1e23", "rail": "#0b0d10",
    }


def test_a_light_background_alone_still_loads(tmp_path: Path) -> None:
    """Scoped to the polarity that actually flips. The console's default
    palette is light, so a light declared ground does not contradict the
    cards the manifest left undeclared, and refusing it would be over-refusal
    against a manifest that renders correctly."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": "#fbfdfe"}},
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert parse_viz_theme({"viz": manifest.viz}).tokens == {
        "background": "#fbfdfe",
    }


def test_a_background_with_no_computable_luminance_fails_closed(
    tmp_path: Path,
) -> None:
    """A check that cannot run must not pass.

    A bare CSS identifier resolves only against the browser's named-color
    table, which this parser deliberately does not copy. So the coherence
    question cannot be answered, and an unanswerable question is refused
    rather than waved through — absence read as agreement is the defect this
    whole change exists to remove. The message asks for either of the two
    things that would make it answerable.
    """
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": "rebeccapurple"}},
    )

    with pytest.raises(VizThemeIncoherent) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.data["reason"] == "luminance_not_computable"
    assert "viz.theme.surface" in str(excinfo.value)


@pytest.mark.parametrize(
    "value", ["rebeccapurple", "transparent", "currentColor", "#12141780",
              "rgba(18, 20, 23, 0.5)", "rgb(18 20 23 / 50%)"],
)
def test_uncomputable_and_translucent_backgrounds_all_fail_closed(
    tmp_path: Path, value: str,
) -> None:
    """Translucent colors join the identifiers: what a half-transparent ground
    actually paints depends on a backdrop the manifest does not contain."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": value}},
    )

    with pytest.raises(VizThemeIncoherent) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.data["reason"] == "luminance_not_computable"


@pytest.mark.parametrize(
    "value", ["rebeccapurple", "transparent", "#12141780"],
)
def test_an_uncomputable_background_loads_once_a_surface_is_declared(
    tmp_path: Path, value: str,
) -> None:
    """The fail-closed arm is a consequence of the question, not a new
    grammar: declaring the surface answers it, and the same value loads."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": value, "surface": "#1a1e23"}},
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert parse_viz_theme({"viz": manifest.viz}).tokens["background"] == value


@pytest.mark.parametrize(
    "value", ["#121417", "#000", "rgb(18 20 23)", "rgb(18, 20, 23)",
              "hsl(210 12% 8%)", "hsl(210, 12%, 8%)", "#121417ff"],
)
def test_the_polarity_check_reads_every_computable_dark_form(
    tmp_path: Path, value: str,
) -> None:
    """The rule is about the COLOR, not about the spelling. A manifest cannot
    evade it by writing the same dark ground in a different notation."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": value}},
    )

    with pytest.raises(VizThemeIncoherent) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.data["reason"] == "polarity_flip_without_surface"


@pytest.mark.parametrize(
    "value", ["#fbfdfe", "#fff", "rgb(251 253 254)", "hsl(210 12% 96%)",
              "#808080"],
)
def test_the_polarity_check_admits_every_computable_light_form(
    tmp_path: Path, value: str,
) -> None:
    """The other side of the same boundary, including mid-grey `#808080`,
    which a naive 0.5 threshold on sRGB bytes would have called dark. Its
    relative luminance is ~0.216, above the derived midpoint."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"background": value}},
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert parse_viz_theme({"viz": manifest.viz}).tokens == {"background": value}


def test_the_midpoint_is_the_equal_contrast_luminance() -> None:
    """The threshold is derived, not tuned.

    A surface at the midpoint has the same WCAG contrast against black ink as
    against white ink, which is exactly what "does this want light ink or dark
    ink" means. Asserted against the contrast formula itself, so a future edit
    that replaces the constant with a hand-picked number fails here.
    """
    from okg.substrate.deployments.chat_block import _POLARITY_MIDPOINT_LUMINANCE

    midpoint = _POLARITY_MIDPOINT_LUMINANCE
    against_white = 1.05 / (midpoint + 0.05)
    against_black = (midpoint + 0.05) / 0.05

    assert against_white == pytest.approx(against_black)
    assert midpoint == pytest.approx(0.1791, abs=1e-4)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("#000000", 0.0),
        ("#ffffff", 1.0),
        ("#fff", 1.0),
        ("rgb(255 255 255)", 1.0),
        ("rgb(100% 100% 100%)", 1.0),
        ("hsl(0 0% 100%)", 1.0),
        ("hsl(0turn 0% 0%)", 0.0),
        ("#ff0000", 0.2126),
        ("#00ff00", 0.7152),
        ("#0000ff", 0.0722),
    ],
)
def test_luminance_matches_the_wcag_definition(
    value: str, expected: float,
) -> None:
    """The primaries pin the WCAG coefficients directly: a channel swap or a
    dropped linearization step changes these numbers."""
    assert theme_color_luminance(value) == pytest.approx(expected, abs=1e-4)


@pytest.mark.parametrize(
    "value",
    ["rebeccapurple", "transparent", "currentColor", "#12141780",
     "rgba(1, 2, 3, 0.5)", "rgb(1 2 3 / 0.5)", "hsl(none 0% 0%)",
     "rgb(1 2)", "rgb(1 2 3 4 5)"],
)
def test_luminance_reports_not_computable_rather_than_guessing(
    value: str,
) -> None:
    """`None` is an answer this parser is required to give honestly. Returning
    a plausible-looking number for a value it cannot resolve would make the
    coherence check pass on a color nobody computed."""
    assert theme_color_luminance(value) is None


# ---------------------------------------------------------------------------
# Polarity, served: the base-firing ruling's server half
#
# The SECOND operator ruling of 2026-08-18. The surface-family widening cut the
# attributed regression from 1102 to 694 and stopped there: 98% of what was
# left was ink drawn by properties OUTSIDE the token set — every declared token
# has an undeclared sibling, and seven tokens cannot name forty properties. The
# same deployment rendered coherently under a dark built-in firing, so the fix
# is not more tokens but a matching BASE: the console picks the built-in firing
# of the declared polarity and layers the tokens on that. The polarity is
# computed here, where the luminance machinery already lives, and served — a
# second implementation in TypeScript would drift from the one that refuses.
# ---------------------------------------------------------------------------

def test_a_declared_dark_theme_serves_its_polarity(tmp_path: Path) -> None:
    """The console cannot pick a coherent base without this field."""
    _write_manifest(
        tmp_path,
        "themed",
        "name: themed\nviz:\n  theme:\n"
        + "".join(f'    {k}: "{v}"\n' for k, v in DECLARED_TOKENS.items()),
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="themed", current_port=8770,
    )

    assert _entry(entries, "themed")["viz_theme"]["polarity"] == "dark"


def test_a_declared_light_theme_serves_the_other_polarity(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "bright",
        'name: bright\nviz:\n  theme:\n    background: "#f7fafc"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="bright", current_port=8770,
    )

    assert _entry(entries, "bright")["viz_theme"]["polarity"] == "light"


def test_a_declaration_naming_no_ground_serves_no_polarity(
    tmp_path: Path,
) -> None:
    """An accent-only theme says nothing about the polarity of the page.

    ``None`` here is what stops the console repainting an operator's whole
    palette over one declared color: no polarity, no base-firing selection,
    and the tokens layer over whatever firing they chose. Guessing "light"
    because the console's default is light would be a default masquerading as
    a declaration.
    """
    _write_manifest(
        tmp_path, "accented", 'name: accented\nviz:\n  theme:\n    accent: "#ff8800"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="accented", current_port=8770,
    )

    theme = _entry(entries, "accented")["viz_theme"]
    assert theme["status"] == "declared"
    assert theme["polarity"] is None


def test_the_absences_carry_no_polarity(tmp_path: Path) -> None:
    """`undeclared` and `unresolved` both serve `None`, and must.

    A polarity on an absence would move the operator's firing on the strength
    of a manifest that declared nothing, or one nobody read.
    """
    _write_manifest(tmp_path, "bare", "name: bare\n")
    _write_manifest(
        tmp_path, "broken", 'name: broken\nviz:\n  theme:\n    accent: "not a color!"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="bare", current_port=8770,
    )

    assert _entry(entries, "bare")["viz_theme"]["polarity"] is None
    assert _entry(entries, "broken")["viz_theme"]["polarity"] is None


def test_every_entry_carries_a_polarity_key_so_the_browser_sees_no_undefined(
    tmp_path: Path,
) -> None:
    """An absent KEY and an explicit ``null`` would be two different absences.

    The browser reads a missing field as "no polarity" deliberately — that is
    how it stays compatible with a backend predating this ruling — so an entry
    silently omitting the key would be indistinguishable from an old server.
    """
    _write_manifest(
        tmp_path, "a", 'name: a\nviz:\n  theme:\n    accent: "#ff8800"\n',
    )
    _write_manifest(tmp_path, "b", "name: b\n")

    entries = load_registry(
        deployments_root=tmp_path, current_name="a", current_port=8765,
    )

    assert all("polarity" in entry["viz_theme"] for entry in entries)


def test_polarity_is_taken_from_the_background_first(tmp_path: Path) -> None:
    """The ground decides, because the ground is what the coherence refusal
    already reasons about.

    A manifest whose two planes disagree is accepted at load — the operator
    named both deliberately — and it must not resolve to a polarity different
    from the one its background was checked against, or it would load under one
    reading and render under another.
    """
    theme = _loaded_theme(
        _write_loadable_deployment(
            tmp_path, {"theme": {"background": "#f7fafc", "surface": "#121417"}},
        ),
    )

    assert theme.status == "declared"
    assert theme.polarity == "light"


def test_polarity_falls_back_to_the_surface_when_the_ground_cannot_answer(
    tmp_path: Path,
) -> None:
    """A declaration with no background, or an uncomputable one, still has a
    knowable card polarity — and the card is the plane the stranded ink is
    drawn ON.

    Both fixtures load today: the coherence refusal only fires on a background
    declared WITHOUT a surface. Leaving these at ``None`` would let a dark set
    keep landing on a light firing, which is the defect this ruling closes.
    """
    no_ground = _write_loadable_deployment(
        tmp_path / "a", {"theme": {"surface": "#1a1e23", "fg": "#eef1f4"}},
    )
    uncomputable_ground = _write_loadable_deployment(
        tmp_path / "b", {"theme": {"background": "rebeccapurple", "surface": "#1a1e23"}},
    )

    assert _loaded_theme(no_ground).polarity == "dark"
    assert _loaded_theme(uncomputable_ground).polarity == "dark"


def test_a_theme_naming_neither_plane_computably_has_no_polarity(
    tmp_path: Path,
) -> None:
    """Fails to ``None``, not to a guess. ``surface: rebeccapurple`` needs the
    browser's named-color table, which this parser deliberately does not
    copy."""
    manifest_path = _write_loadable_deployment(
        tmp_path, {"theme": {"surface": "rebeccapurple", "accent": "#ff8800"}},
    )

    assert _loaded_theme(manifest_path).polarity is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("#000000", "dark"),
        ("#121417", "dark"),
        ("#ffffff", "light"),
        ("#eef2f6", "light"),
        ("rgb(16 20 23)", "dark"),
        ("hsl(0 0% 100%)", "light"),
    ],
)
def test_polarity_splits_at_the_same_midpoint_the_refusal_uses(
    value: str, expected: str,
) -> None:
    """One threshold, one derivation, two callers.

    A second constant would let a manifest be refused as dark and then served
    as light.
    """
    assert theme_polarity({"background": value}) == expected
    luminance = theme_color_luminance(value)
    assert luminance is not None
    assert (luminance < _POLARITY_MIDPOINT_LUMINANCE) == (expected == "dark")


def test_the_shipped_okg_workspace_theme_serves_accent_only() -> None:
    """The deployment the audit runs against, end to end.

    Accent-only by operator decision (2026-08-18): no ground token means no
    polarity, and a null polarity NEVER moves the base firing — the console
    keeps the operator's chosen palette and only the accent is tinted. The
    dark end-to-end path stays covered by the constructed-fixture arms
    above; this pins the real manifest to the decided shape.
    """
    theme = _loaded_theme(Path("deployments/okg-workspace/deployment.yaml"))

    assert theme.status == "declared"
    assert theme.polarity is None
    assert theme.as_payload()["polarity"] is None


def test_the_polarity_vocabulary_is_the_two_the_console_maps() -> None:
    """Pinned against `DEFAULT_FIRING_FOR_POLARITY` in `styles/theme.ts`, whose
    keys are exactly these. A third polarity here would reach the browser as a
    key with no firing behind it."""
    assert VIZ_THEME_POLARITIES == ("light", "dark")
    assert VIZ_THEME_POLARITY_LIGHT == "light"
    assert VIZ_THEME_POLARITY_DARK == "dark"
