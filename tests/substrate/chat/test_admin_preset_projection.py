"""The source-admin preset is a written surface of ``okg chat sync``.

PACT ``chat-source-admin-writer``. ``project_admin_preset`` built a complete
admin preset that no shipped path wrote into an instance. These tests read
the result back off the fake instance's own ``/api/v1/models/list`` and
``/api/v1/configs/tool_servers`` responses, never off the payload the sync
run composed: the defect was that the payload was already being built.

The switch is ``chat.sso.admin_group`` -- the same one the write preset uses.
A deployment without it must behave exactly as before this change.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.source_admin import (
    AUTH_REQUIRED,
    ENFORCEMENT_INTERIM_LABEL,
    PER_CALLER_ARM_BLOCKED_ON,
    LiveToolSurface,
    project_admin_preset,
)
from okg.substrate.chat.sync import (
    ADMIN_PRESET_SUFFIX,
    SURFACE_ADMIN_PRESET,
    WRITTEN_SURFACES,
    ChatSyncError,
    InstanceClient,
    effective_tool_binding,
    sync,
)
from okg.substrate.mcp.operators import SOURCE_ADMIN_TOOLS

from .fake_openwebui import FakeInstance
from .test_write_connection_projection import (
    ADMIN_GROUP,
    AUTH_TOKEN_ENV,
    BASE_URL,
    READ_TOOLS,
    TOKEN,
    USER_GROUP,
    _make_inputs,
    _pin_endpoint,
    _raw_manifest,
)

#: A write-enabled admin endpoint: the read algebra plus the nine
#: source-admin tools. No `record`/`propose`, so the write connection stays
#: out of these arms and cannot be what refuses or succeeds.
ADMIN_CAPABLE_TOOLS = (*READ_TOOLS, "query", *SOURCE_ADMIN_TOOLS)

ADMIN_PRESET_ID = f"okg-fixture{ADMIN_PRESET_SUFFIX}"
ADMIN_SERVER_ID = f"okg-fixture{ADMIN_PRESET_SUFFIX}"


def _instance(*, with_admin_group: bool = True) -> FakeInstance:
    instance = FakeInstance(webui_name="Fixture Site")
    instance.add_group(USER_GROUP, "group-users")
    if with_admin_group:
        instance.add_group(ADMIN_GROUP, "group-admins")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    return instance


def _run(
    inputs: Any, instance: FakeInstance, *, advertised: tuple[str, ...], **kw: Any
) -> Any:
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=lambda _endpoint, _env: advertised,
        **kw,
    )


def _entry(instance: FakeInstance, server_id: str) -> dict[str, Any] | None:
    for entry in instance.tool_servers:
        if (entry.get("info") or {}).get("id") == server_id:
            return entry
    return None


def test_sync_writes_admin_preset_with_grants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=True))
    report = _run(inputs, instance, advertised=ADMIN_CAPABLE_TOOLS)
    assert report.ok, report.to_dict()

    # The expected wire form comes from the projection itself, not a literal.
    expected = project_admin_preset(
        deployment="fixture",
        preset_id=ADMIN_PRESET_ID,
        live=LiveToolSurface(
            endpoint=inputs.mcp_probe_url,
            tool_names=ADMIN_CAPABLE_TOOLS,
            auth_posture=AUTH_REQUIRED,
        ),
        access_group_names=[ADMIN_GROUP],
        auth_token_env=AUTH_TOKEN_ENV,
        host="host.docker.internal",
        access_group_ids={ADMIN_GROUP: "group-admins"},
        mcp_server_id=ADMIN_SERVER_ID,
    ).to_dict()

    row = instance.model_row(ADMIN_PRESET_ID)
    assert row["meta"]["toolIds"] == expected["meta_tool_ids"]
    assert [
        {k: g[k] for k in ("principal_type", "principal_id", "permission")}
        for g in row["access_grants"]
    ] == expected["access_grants"]
    assert {g["principal_id"] for g in row["access_grants"]} == {"group-admins"}

    # The tools reachable through that server id: the nine, read back off the
    # STORED filter through the vendor's own endswith predicate.
    entry = _entry(instance, ADMIN_SERVER_ID)
    assert entry is not None, instance.tool_servers
    stored = entry["config"]["function_name_filter_list"]
    reachable = effective_tool_binding(ADMIN_CAPABLE_TOOLS, stored.split(","))
    assert set(reachable) == set(SOURCE_ADMIN_TOOLS)

    # The general preset binds none of the admin server's ids.
    general = instance.model_row("okg-fixture")
    assert f"server:mcp:{ADMIN_SERVER_ID}" not in general["meta"]["toolIds"]

    payload = report.to_dict()
    assert SURFACE_ADMIN_PRESET in payload["written_surfaces"]
    assert payload["surfaces"][SURFACE_ADMIN_PRESET]["status"] == "verified"
    assert payload["written_surfaces_total"] == len(WRITTEN_SURFACES) + 1
    assert payload["written_surfaces_accounted"] == payload["written_surfaces_total"]
    # F7: never the swallowing sync route.
    assert not [p for _m, p in instance.requests if p.startswith("/api/v1/models/sync")]

    second = _run(inputs, instance, advertised=ADMIN_CAPABLE_TOOLS)
    assert second.ok, second.to_dict()
    assert second.surfaces_with_detected_changes == 0, second.to_dict()["surfaces"]


def test_written_preset_reports_specified_not_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=ADMIN_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()
    block = json.loads(json.dumps(report.to_dict()))["admin_preset"]
    assert block["status"] == "verified"
    assert block["gating_claim"] == "specified"
    projection = block["projection"]
    assert projection["enforcement_sufficient"] is False
    assert projection["enforcement_label"] == ENFORCEMENT_INTERIM_LABEL
    assert any(PER_CALLER_ARM_BLOCKED_ON in arm for arm in projection["blocked_arms"])
    # Visibility is bounded, never cited as gating.
    assert "Hiding a control is not denying it" in projection["visibility_bounds"]


def test_unsafe_transport_refuses_only_the_admin_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-loopback instance URL, endpoint observed OPEN: the surface refuses.

    The rest of the run is unchanged: every other written surface verifies,
    and the run is ok. Nothing admin-bearing reaches the instance.
    """
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=False)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=ADMIN_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()
    payload = report.to_dict()
    admin = payload["surfaces"][SURFACE_ADMIN_PRESET]
    assert admin["status"] == "refused_unsafe_transport"
    assert admin["refusal"]
    for name in WRITTEN_SURFACES:
        assert payload["surfaces"][name]["ok"] is True, payload["surfaces"][name]
    assert payload["written_surfaces_accounted"] == payload["written_surfaces_total"]
    assert SURFACE_ADMIN_PRESET in {
        d["surface"] for d in payload["written_surfaces_dropped"]
    }
    assert ADMIN_PRESET_ID not in instance.models
    assert _entry(instance, ADMIN_SERVER_ID) is None
    assert payload["admin_preset"]["status"] == "refused_unsafe_transport"


def test_unresolvable_admin_group_refuses_the_surface_with_no_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance(with_admin_group=False)
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=ADMIN_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()
    admin = report.to_dict()["surfaces"][SURFACE_ADMIN_PRESET]
    assert admin["status"] == "refused_unresolved_group"
    assert ADMIN_GROUP in admin["refusal"]
    assert ADMIN_PRESET_ID not in instance.models
    assert _entry(instance, ADMIN_SERVER_ID) is None


def test_absent_access_grants_on_readback_fails_the_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An omitted `access_grants` field is NOT-READ, never the empty set."""
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    real = InstanceClient.models_list

    def _strip(self: InstanceClient) -> list[Any]:
        rows = []
        for row in real(self):
            row = dict(row)
            if row.get("id") == ADMIN_PRESET_ID:
                row.pop("access_grants", None)
            rows.append(row)
        return rows

    monkeypatch.setattr(InstanceClient, "models_list", _strip)
    with pytest.raises(ChatSyncError) as err:
        _run(
            _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
            instance,
            advertised=ADMIN_CAPABLE_TOOLS,
        )
    assert err.value.data.get("surface") == SURFACE_ADMIN_PRESET
    assert err.value.data.get("key") == "admin_preset:grants"
    assert err.value.data.get("reason") == "readback_unreadable"
    assert err.value.partial_report.ok is False  # type: ignore[attr-defined]


def test_no_admin_group_behaves_exactly_as_before(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No `chat.sso.admin_group`: no admin surface, no extra dial, no row."""
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()

    def _never(_endpoint: str, _env: str | None) -> str:
        raise AssertionError("no admin group: the auth posture must not be dialled")

    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=False)),
        instance,
        advertised=ADMIN_CAPABLE_TOOLS,
        observe_auth=_never,
    )
    assert report.ok, report.to_dict()
    payload = report.to_dict()
    assert payload["written_surfaces"] == list(WRITTEN_SURFACES)
    assert payload["written_surfaces_total"] == len(WRITTEN_SURFACES)
    assert set(payload["surfaces"]) == set(WRITTEN_SURFACES)
    assert payload["written_surfaces_accounted"] == len(WRITTEN_SURFACES)
    assert ADMIN_PRESET_ID not in instance.models
    assert _entry(instance, ADMIN_SERVER_ID) is None
    assert not [p for _m, p in instance.requests if ADMIN_PRESET_SUFFIX in p]
    assert payload["admin_preset"] == {
        "declared": False,
        "status": "not_declared",
        "reason": "no_admin_group_declared",
        "stale_connection": None,
        "stale_row": None,
    }


def _admin_entry_from_elsewhere() -> dict[str, Any]:
    """A tool-server entry that happens to carry the admin id.

    Written by an operator by hand, or another deployment's read connection
    whose id collides (deployments ``fixture`` and ``fixture-admin`` on one
    instance). Sync must not touch it when this deployment declares no admin
    group.
    """
    return {
        "url": "http://elsewhere.test:9000/mcp",
        "path": "",
        "type": "mcp",
        "auth_type": "none",
        "headers": None,
        "key": "",
        "config": {
            "enable": True,
            "access_grants": [],
            "function_name_filter_list": "search",
        },
        "info": {"id": ADMIN_SERVER_ID, "name": "not ours", "description": "x"},
    }


def test_no_admin_group_preserves_an_existing_admin_id_entry_and_reports_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the switch, a `-admin` entry is kept byte for byte and reported."""
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    foreign = _admin_entry_from_elsewhere()
    instance.tool_servers.append(json.loads(json.dumps(foreign)))
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=False)),
        instance,
        advertised=ADMIN_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()
    # The preset surface rewrote the list on this first run, and kept it.
    assert any(
        p == "/api/v1/configs/tool_servers" for m, p in instance.requests if m == "POST"
    )
    assert _entry(instance, ADMIN_SERVER_ID) == foreign
    block = report.to_dict()["admin_preset"]
    assert block["status"] == "not_declared"
    assert block["stale_connection"] == ADMIN_SERVER_ID
    assert block["stale_row"] is None
    assert "fix_hint" in block and ADMIN_SERVER_ID in block["fix_hint"]


def test_a_refusal_after_a_successful_run_removes_the_connection_and_reports_the_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Written once; then the endpoint is observed OPEN. The connection goes."""
    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=True))
    first = _run(inputs, instance, advertised=ADMIN_CAPABLE_TOOLS)
    assert first.ok, first.to_dict()
    assert _entry(instance, ADMIN_SERVER_ID) is not None
    assert ADMIN_PRESET_ID in instance.models

    _pin_endpoint(monkeypatch, advertised=ADMIN_CAPABLE_TOOLS, enforces_auth=False)
    second = _run(inputs, instance, advertised=ADMIN_CAPABLE_TOOLS)
    assert second.ok, second.to_dict()
    payload = second.to_dict()
    assert payload["surfaces"][SURFACE_ADMIN_PRESET]["status"] == (
        "refused_unsafe_transport"
    )
    assert _entry(instance, ADMIN_SERVER_ID) is None, instance.tool_servers
    block = payload["admin_preset"]
    assert block["existing_row"] is True
    assert ADMIN_PRESET_ID in block["fix_hint"]


def test_a_read_only_endpoint_refuses_the_admin_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    advertised = (*READ_TOOLS, "query")
    _pin_endpoint(monkeypatch, advertised=advertised, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=advertised,
    )
    assert report.ok, report.to_dict()
    admin = report.to_dict()["surfaces"][SURFACE_ADMIN_PRESET]
    assert admin["status"] == "refused_read_only_endpoint"
    assert ADMIN_PRESET_ID not in instance.models
    assert _entry(instance, ADMIN_SERVER_ID) is None
    assert report.to_dict()["admin_preset"]["existing_row"] is False


def test_a_suffix_colliding_tool_name_refuses_the_admin_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`readd_source` ends with `add_source`: the filter would widen. Refused."""
    advertised = (*ADMIN_CAPABLE_TOOLS, "readd_source")
    _pin_endpoint(monkeypatch, advertised=advertised, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=advertised,
    )
    assert report.ok, report.to_dict()
    admin = report.to_dict()["surfaces"][SURFACE_ADMIN_PRESET]
    assert admin["status"] == "refused_filter_collision"
    assert "readd_source" in admin["refusal"]
    assert ADMIN_PRESET_ID not in instance.models
    assert _entry(instance, ADMIN_SERVER_ID) is None
