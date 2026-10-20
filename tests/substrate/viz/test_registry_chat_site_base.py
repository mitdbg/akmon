"""The deployments read carries the chat site base (PACT chat-deep-links).

The console's reverse navigation link has exactly one data source: the
``chat_site_base`` field on each ``/viz/api/deployments`` entry, read from
the deployment manifest's ``chat.deep_links`` block. A component test fed a
fixture prop proves nothing about that path, so these tests exercise the
registry the endpoint actually calls.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.viz.registry import current_deployment, load_registry

CHAT_SITE = "https://chat.okg.example.org"
OPERATOR_SITE = "https://okg.example.org/operator/"


def _write_manifest(root: Path, name: str, body: str) -> None:
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "deployment.yaml").write_text(body)


def test_registry_surfaces_a_declared_chat_site_base(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "cms",
        f"""
name: cms
description: CMS deployment
viz:
  port: 8770
chat:
  deep_links:
    operator_ui_base: "{OPERATOR_SITE}"
    chat_site_base: "{CHAT_SITE}"
""",
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="cms", current_port=8770,
    )

    assert [entry["name"] for entry in entries] == ["cms"]
    assert entries[0]["chat_site_base"] == CHAT_SITE
    assert entries[0]["operator_ui_base"] == OPERATOR_SITE


@pytest.mark.parametrize(
    "manifest_body",
    [
        "name: bare\n",
        "name: bare\nchat: {}\n",
        "name: bare\nchat:\n  deep_links: {}\n",
        'name: bare\nchat:\n  deep_links:\n    chat_site_base: "   "\n',
        "name: bare\nchat:\n  deep_links:\n    chat_site_base: 17\n",
        "name: bare\nchat: not-a-mapping\n",
    ],
)
def test_a_deployment_without_a_declared_chat_site_reports_none(
    tmp_path: Path, manifest_body: str,
) -> None:
    """Absent, blank, or malformed all read as "undeclared" — never a default.

    The console renders a stated absence for None. A default host here would
    hand every deployment a link to somewhere it does not run.
    """
    _write_manifest(tmp_path, "bare", manifest_body)

    entries = load_registry(
        deployments_root=tmp_path, current_name="bare", current_port=8765,
    )

    assert entries[0]["chat_site_base"] is None


def test_every_registry_entry_carries_the_field_even_when_undeclared(
    tmp_path: Path,
) -> None:
    """The key is always present, so the console never sees `undefined` for a
    deployment it did enumerate — an absent KEY and a null VALUE would be two
    different absences for the browser to tell apart."""
    _write_manifest(tmp_path, "a", "name: a\n")
    _write_manifest(
        tmp_path, "b", f'name: b\nchat:\n  deep_links:\n    chat_site_base: "{CHAT_SITE}"\n',
    )

    entries = load_registry(
        deployments_root=tmp_path, current_name="a", current_port=8765,
    )

    assert all("chat_site_base" in entry for entry in entries)
    assert all("operator_ui_base" in entry for entry in entries)
    by_name = {entry["name"]: entry["chat_site_base"] for entry in entries}
    assert by_name == {"a": None, "b": CHAT_SITE}


def test_fallback_entries_declare_no_chat_site(tmp_path: Path) -> None:
    """A deployment discovered outside the root, and the empty-registry
    placeholder, both report an undeclared chat site rather than omitting the
    field."""
    outside = load_registry(
        deployments_root=tmp_path, current_name="external", current_port=9000,
    )
    empty = load_registry(
        deployments_root=None, current_name=None, current_port=None,
    )

    assert outside[0]["chat_site_base"] is None
    assert empty[0]["chat_site_base"] is None
    assert outside[0]["operator_ui_base"] is None
    assert empty[0]["operator_ui_base"] is None


def test_current_deployment_record_carries_the_chat_site_base() -> None:
    payload = current_deployment(
        manifest={
            "name": "cms",
            "chat": {"deep_links": {
                "chat_site_base": CHAT_SITE,
                "operator_ui_base": OPERATOR_SITE,
            }},
        },
        bound_port=8765,
    )

    assert payload["chat_site_base"] == CHAT_SITE
    assert payload["operator_ui_base"] == OPERATOR_SITE


def test_current_deployment_without_a_manifest_declares_no_chat_site() -> None:
    payload = current_deployment(manifest=None, bound_port=8765)

    assert payload["chat_site_base"] is None
    assert payload["operator_ui_base"] is None


# --- The current deployment served from outside the repo's deployments/ ----
#
# A deployment served with ``OKG_DEPLOYMENTS_DIR`` pointing elsewhere has no
# manifest under the root the registry enumerates. The singular read
# (``/viz/api/deployment``) reads the served manifest; the plural read must
# carry the same links for that entry, or the console top bar says "No chat
# site declared" for a deployment whose manifest declares one.

SERVED_MANIFEST = {
    "name": "cern-r3final",
    "chat": {"deep_links": {
        "chat_site_base": CHAT_SITE,
        "operator_ui_base": OPERATOR_SITE,
    }},
}


def test_current_deployment_outside_the_root_carries_its_served_links(
    tmp_path: Path,
) -> None:
    _write_manifest(tmp_path, "sibling", "name: sibling\n")

    entries = load_registry(
        deployments_root=tmp_path,
        current_name="cern-r3final",
        current_port=8797,
        current_manifest=SERVED_MANIFEST,
    )

    by_name = {entry["name"]: entry for entry in entries}
    assert by_name["cern-r3final"]["is_current"] is True
    assert by_name["cern-r3final"]["chat_site_base"] == CHAT_SITE
    assert by_name["cern-r3final"]["operator_ui_base"] == OPERATOR_SITE
    # A sibling that declares nothing still reads as undeclared.
    assert by_name["sibling"]["chat_site_base"] is None


def test_served_manifest_wins_over_a_same_named_copy_under_the_root(
    tmp_path: Path,
) -> None:
    """The served manifest is the one the singular read reports. A stale copy
    with the same name under the enumerated root must not contradict it."""
    _write_manifest(tmp_path, "cern-r3final", "name: cern-r3final\n")

    entries = load_registry(
        deployments_root=tmp_path,
        current_name="cern-r3final",
        current_port=8797,
        current_manifest=SERVED_MANIFEST,
    )

    assert [entry["name"] for entry in entries] == ["cern-r3final"]
    assert entries[0]["chat_site_base"] == CHAT_SITE
    assert entries[0]["operator_ui_base"] == OPERATOR_SITE


def test_served_manifest_without_links_reports_none(tmp_path: Path) -> None:
    entries = load_registry(
        deployments_root=tmp_path,
        current_name="plain",
        current_port=8797,
        current_manifest={"name": "plain"},
    )

    assert entries[0]["chat_site_base"] is None
    assert entries[0]["operator_ui_base"] is None


def test_plural_and_singular_endpoints_agree_on_the_chat_site_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end through the routes the console reads: the plural list's
    entry for the served deployment carries what the singular read serves."""
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from okg.viz import app as viz_app

    _write_manifest(tmp_path, "sibling", "name: sibling\n")

    class _Ctx:
        deployment_name = "cern-r3final"
        manifest = SERVED_MANIFEST

    monkeypatch.setattr(viz_app, "_deployments_root", lambda: tmp_path)
    monkeypatch.setattr(viz_app, "_bound_port", lambda: 8797)
    client = TestClient(viz_app.build_app(ctx_provider=_Ctx, is_ready=lambda: True))

    singular = client.get("/viz/api/deployment").json()
    plural = client.get("/viz/api/deployments").json()["entries"]
    current = [entry for entry in plural if entry["is_current"]]

    assert singular["chat_site_base"] == CHAT_SITE
    assert [entry["name"] for entry in current] == ["cern-r3final"]
    assert current[0]["chat_site_base"] == singular["chat_site_base"]
    assert current[0]["operator_ui_base"] == singular["operator_ui_base"]
