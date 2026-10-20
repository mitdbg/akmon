"""`agent_surface.images` (PACT first-class-images, req config).

The block is parsed strictly: every bound, every unknown key by name, and
the URL allowlist compared by parts. A deployment without the block gets the
defaults, which fetch no URL, and every checked-in manifest's parsed surface
is unchanged by the new key.
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
import yaml

from okg.substrate.images import config as c
from okg.substrate.images.config import ImagesConfig, ImagesConfigInvalid, UrlPrefix
from okg.substrate.images.values import url_problem
from okg.substrate.mcp.effective_surface import (
    AgentSurfaceInvalid,
    parse_agent_surface,
)

ROOT = Path(__file__).resolve().parents[3]


def _refusal(raw) -> ImagesConfigInvalid:
    with pytest.raises(ImagesConfigInvalid) as caught:
        ImagesConfig.parse(raw)
    assert caught.value.code == "images_config_invalid"
    return caught.value


# ── defaults ───────────────────────────────────────────────────────────

def test_defaults():
    d = ImagesConfig.default()
    assert d.render.dpi == 100
    assert d.render.max_side == 1600
    assert d.file_max_bytes == 50 * 1024 * 1024
    assert d.budget.max_panels == 4
    assert d.budget.max_bytes == 1_500_000
    assert d.url.prefixes == ()
    assert d.url.timeout_s == 10.0
    assert d.url.max_bytes == 10 * 1024 * 1024
    # No prefix, no fetch.
    assert d.url.fetch_enabled is False
    assert d.url.admits("https://cds.cern.ch/record/1/f.png") is False


@pytest.mark.parametrize("raw", [None, {}, {"render": {}, "budget": {}, "url": {}}])
def test_an_empty_or_absent_block_is_the_defaults(raw):
    assert ImagesConfig.parse(raw) == ImagesConfig.default()


def test_a_full_block_parses():
    got = ImagesConfig.parse({
        "render": {"dpi": 150, "max_side": 2048},
        "file_max_bytes": 1024,
        "budget": {"max_panels": 2, "max_bytes": 500_000},
        "url": {
            "prefixes": ["https://cds.cern.ch/record/", "https://Example.org:8443/imgs"],
            "timeout_s": 2.5,
            "max_bytes": 2048,
        },
    })
    assert got.render == c.RenderConfig(dpi=150, max_side=2048)
    assert got.file_max_bytes == 1024
    assert got.budget == c.BudgetConfig(max_panels=2, max_bytes=500_000)
    assert got.url.prefixes == (
        UrlPrefix(host="cds.cern.ch", port=443, path="/record/"),
        UrlPrefix(host="example.org", port=8443, path="/imgs/"),
    )
    assert got.url.timeout_s == 2.5 and got.url.max_bytes == 2048
    assert got.url.fetch_enabled


# ── bounds ─────────────────────────────────────────────────────────────

BOUNDED = [
    # (path to the key, low, high)
    (("render", "dpi"), 36, 300),
    (("render", "max_side"), c.MAX_SIDE_MIN, c.MAX_SIDE_MAX),
    (("file_max_bytes",), c.FILE_MAX_BYTES_MIN, c.FILE_MAX_BYTES_MAX),
    (("budget", "max_panels"), c.MAX_PANELS_MIN, c.MAX_PANELS_MAX),
    (("budget", "max_bytes"), c.BUDGET_BYTES_MIN, c.BUDGET_BYTES_MAX),
    (("url", "max_bytes"), c.URL_BYTES_MIN, c.URL_BYTES_MAX),
]


def _block(path, value):
    if len(path) == 1:
        return {path[0]: value}
    return {path[0]: {path[1]: value}}


def _get(cfg, path):
    obj = cfg
    for part in path:
        obj = getattr(obj, part)
    return obj


@pytest.mark.parametrize(("path", "low", "high"), BOUNDED)
def test_every_integer_bound_admits_its_edges(path, low, high):
    assert _get(ImagesConfig.parse(_block(path, low)), path) == low
    assert _get(ImagesConfig.parse(_block(path, high)), path) == high


@pytest.mark.parametrize(("path", "low", "high"), BOUNDED)
@pytest.mark.parametrize("side", ["below", "above"])
def test_every_integer_bound_refuses_outside_naming_the_field(path, low, high, side):
    value = low - 1 if side == "below" else high + 1
    err = _refusal(_block(path, value))
    name = "agent_surface.images." + ".".join(path)
    assert err.data["field"] == name
    assert name in err.message
    assert (err.data["min"], err.data["max"], err.data["value"]) == (low, high, value)


@pytest.mark.parametrize(("path", "low", "high"), BOUNDED)
@pytest.mark.parametrize("bad", [True, 100.0, "100", [100]])
def test_every_integer_field_refuses_a_non_integer(path, low, high, bad):
    err = _refusal(_block(path, bad))
    assert err.data["field"] == "agent_surface.images." + ".".join(path)
    assert "integer" in err.message


def test_dpi_bounds_are_36_to_300():
    assert ImagesConfig.parse({"render": {"dpi": 36}}).render.dpi == 36
    assert ImagesConfig.parse({"render": {"dpi": 300}}).render.dpi == 300
    _refusal({"render": {"dpi": 35}})
    _refusal({"render": {"dpi": 301}})


@pytest.mark.parametrize("seconds", [c.URL_TIMEOUT_MIN, 1, 10.5, c.URL_TIMEOUT_MAX])
def test_timeout_admits_numbers_in_range(seconds):
    assert ImagesConfig.parse({"url": {"timeout_s": seconds}}).url.timeout_s == float(seconds)


@pytest.mark.parametrize("seconds", [0, -1, 0.01, c.URL_TIMEOUT_MAX + 1, float("nan"), float("inf"), True, "10"])
def test_timeout_refuses_out_of_range_or_non_numbers(seconds):
    err = _refusal({"url": {"timeout_s": seconds}})
    assert err.data["field"] == "agent_surface.images.url.timeout_s"


# ── unknown keys and wrong shapes ──────────────────────────────────────

@pytest.mark.parametrize(("raw", "label", "unknown"), [
    ({"dpi": 100}, "agent_surface.images", ["dpi"]),
    ({"render": {"dpi": 100, "quality": 9}}, "agent_surface.images.render", ["quality"]),
    ({"budget": {"max_panel": 4}}, "agent_surface.images.budget", ["max_panel"]),
    ({"url": {"prefix": ["https://e.org/"]}}, "agent_surface.images.url", ["prefix"]),
    ({"url": {"allow": 1, "deny": 2}}, "agent_surface.images.url", ["allow", "deny"]),
])
def test_an_unknown_key_is_refused_by_name(raw, label, unknown):
    err = _refusal(raw)
    assert err.data["field"] == label
    assert err.data["unknown_keys"] == unknown
    for key in unknown:
        assert repr(key) in err.message


@pytest.mark.parametrize(("raw", "field"), [
    ([], "agent_surface.images"),
    ("on", "agent_surface.images"),
    ({"render": 100}, "agent_surface.images.render"),
    ({"budget": [4]}, "agent_surface.images.budget"),
    ({"url": "https://e.org/"}, "agent_surface.images.url"),
    ({"url": {"prefixes": "https://e.org/"}}, "agent_surface.images.url.prefixes"),
    ({"url": {"prefixes": [7]}}, "agent_surface.images.url.prefixes"),
])
def test_a_wrong_shape_is_refused_naming_the_field(raw, field):
    assert _refusal(raw).data["field"] == field


# ── the URL allowlist ──────────────────────────────────────────────────

@pytest.mark.parametrize("prefix", [
    "http://cds.cern.ch/record/",
    "ftp://cds.cern.ch/",
    "cds.cern.ch/record/",
    "https:///record/",
    "https://user:pw@cds.cern.ch/",
    "https://user@cds.cern.ch/",
    "https://@cds.cern.ch/",
    "https://cds.cern.ch/record/?x=1",
    "https://cds.cern.ch/record/#frag",
    "https://cds.cern.ch:notaport/",
    "https://cds.cern.ch/re cord/",
    "https://cds.cern.ch/\x00/",
])
def test_a_bad_prefix_is_refused(prefix):
    err = _refusal({"url": {"prefixes": ["https://ok.example/", prefix]}})
    assert err.data["field"] == "agent_surface.images.url.prefixes"
    assert err.data["index"] == 1


def test_prefixes_match_by_parts_not_by_string():
    cfg = ImagesConfig.parse({"url": {"prefixes": ["https://cds.cern.ch/record"]}})
    admits = cfg.url.admits
    assert admits("https://cds.cern.ch/record/1/files/f.png")
    assert admits("https://CDS.CERN.CH/record/1.png")
    assert admits("https://cds.cern.ch:443/record/1.png")
    # Host confusion: a string startswith would admit every one of these.
    assert not admits("https://cds.cern.ch.evil.example/record/1.png")
    assert not admits("https://cds.cern.ch@evil.example/record/1.png")
    assert not admits("https://cds.cern.ch:8443/record/1.png")
    # The prefix names a directory: /record does not admit /records-secret.
    assert not admits("https://cds.cern.ch/records-secret/1.png")
    assert not admits("https://cds.cern.ch/other/1.png")
    # Scheme and credentials.
    assert not admits("http://cds.cern.ch/record/1.png")
    assert not admits("https://u:p@cds.cern.ch/record/1.png")
    assert not admits("https://@cds.cern.ch/record/1.png")
    assert not admits("https://cds.cern.ch:bad/record/1.png")


@pytest.mark.parametrize("url", [
    "https://cds.cern.ch/record/../secret.png",
    "https://cds.cern.ch/record/./../secret.png",
    "https://cds.cern.ch/record/%2e%2e/secret.png",
    "https://cds.cern.ch/record/%2E%2E/secret.png",
    "https://cds.cern.ch/record/.%2e/secret.png",
    "https://cds.cern.ch/record/..%5csecret.png",
    "https://cds.cern.ch/record/..\\secret.png",
])
def test_a_dot_segment_cannot_climb_out_of_a_prefix(url):
    """A server resolves `..` before serving, so `/record/../secret.png`
    would pass a `/record/` prefix and fetch `/secret.png` (#3046 review)."""
    cfg = ImagesConfig.parse({"url": {"prefixes": ["https://cds.cern.ch/record/"]}})
    assert not cfg.url.admits(url)
    assert url_problem(url) is not None
    # non-vacuity: the same prefix still admits a plain path under it
    assert cfg.url.admits("https://cds.cern.ch/record/a..b/1.png")


# ── wiring into agent_surface ──────────────────────────────────────────

def test_agent_surface_parses_the_images_block():
    surface = parse_agent_surface({"agent_surface": {
        "served": ["query"],
        "images": {"render": {"dpi": 120}, "url": {"prefixes": ["https://e.org/i/"]}},
    }})
    assert surface.images is not None
    assert surface.images.render.dpi == 120
    assert surface.images_config is surface.images
    assert surface.images.url.admits("https://e.org/i/a.png")


def test_agent_surface_without_images_uses_the_defaults():
    surface = parse_agent_surface({"agent_surface": {"served": ["query"]}})
    assert surface.images is None
    assert surface.images_config == ImagesConfig.default()


def test_an_images_refusal_is_an_agent_surface_refusal():
    """Load and lint handle `agent_surface_invalid`; the image refusal is
    re-raised as one, keeps its field, and chains the original."""
    with pytest.raises(AgentSurfaceInvalid) as caught:
        parse_agent_surface({"agent_surface": {
            "served": ["query"], "images": {"render": {"dpi": 1000}},
        }}, manifest_path="deployments/x/deployment.yaml")
    err = caught.value
    assert err.code == "agent_surface_invalid"
    assert err.data["field"] == "agent_surface.images.render.dpi"
    assert "deployments/x/deployment.yaml" in err.message
    assert isinstance(err.__cause__, ImagesConfigInvalid)


def _surface(dpi):
    return {"agent_surface": {"served": ["query"], "images": {"render": {"dpi": dpi}}}}


def test_deployment_config_reads_the_raw_manifest_outside_a_sealed_environment(
    tmp_path, monkeypatch,
):
    import okg.substrate.deployments as deployments
    from okg.substrate.images.render import images_config_for_deployment

    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.setattr(
        deployments, "active_effective_manifest_for_path",
        lambda path: pytest.fail("composed manifest read outside a sealed env"),
    )
    assert images_config_for_deployment(tmp_path, _surface(120)).render.dpi == 120
    assert images_config_for_deployment(tmp_path, {}) == ImagesConfig.default()


def test_deployment_config_prefers_the_composed_manifest_when_sealed(
    tmp_path, monkeypatch,
):
    import okg.substrate.deployments as deployments
    from okg.substrate.images.render import images_config_for_deployment

    seen = []

    def composed(path):
        seen.append(path)
        return _surface(150) if len(seen) == 1 else None

    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "sealed.json")
    monkeypatch.setattr(deployments, "active_effective_manifest_for_path", composed)
    assert images_config_for_deployment(tmp_path, _surface(120)).render.dpi == 150
    # No composed manifest for this path: the raw manifest still applies.
    assert images_config_for_deployment(tmp_path, _surface(120)).render.dpi == 120
    assert seen == [tmp_path / "deployment.yaml"] * 2


def test_deployment_config_refuses_an_invalid_block(tmp_path, monkeypatch):
    from okg.substrate.images.render import images_config_for_deployment

    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    with pytest.raises(AgentSurfaceInvalid) as caught:
        images_config_for_deployment(tmp_path, _surface(1000))
    assert str(tmp_path / "deployment.yaml") in caught.value.message


def _checked_in_manifests():
    paths = sorted((ROOT / "deployments").glob("*/deployment.yaml"))
    paths += sorted((ROOT / "tests").rglob("fixtures/**/deployment.yaml"))
    out = []
    for path in paths:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and raw.get("agent_surface") is not None:
            out.append(pytest.param(path, raw, id=str(path.relative_to(ROOT))))
    return out


MANIFESTS = _checked_in_manifests()


def test_there_are_checked_in_surfaces_to_compare():
    assert len(MANIFESTS) >= 10


@pytest.mark.parametrize(("path", "raw"), MANIFESTS)
def test_every_checked_in_surface_parses_unchanged(path, raw):
    """No checked-in manifest declares `images`, so each parses exactly as
    before: every field the surface had is the same, and `images` is None."""
    assert "images" not in raw["agent_surface"]
    surface = parse_agent_surface(raw, manifest_path=path)
    assert surface.images is None
    assert dataclasses.replace(surface, images=None) == surface
    assert surface.images_config == ImagesConfig.default()
