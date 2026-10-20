"""The admin preset's tool binding (PACT chat-source-admin).

``task.chat-source-admin.bind-tools``. The admitted source-admin surface
is not a manifest fact: it is a launch-time property of the serving
process (``allow_writes`` plus ``OKG_MCP_TOOL_ALLOWLIST``). So every
assertion here is against a LIVE ``tools/list`` readback, and the
expected set is the IMPORTED :data:`SOURCE_ADMIN_TOOLS` tuple rather
than a literal copied into this file — a change to the tool set must
fail a test, not drift past one.

Five silent failures shape these tests:

* a manifest naming a tool the live endpoint does not register, silently
  dropped, leaving an admin preset that cannot administer anything;
* an EMPTY readback, whose every membership test passes vacuously;
* a forbidden-set intersection that is empty because the forbidden set
  is, which reads as "nothing forbidden is bound" while checking nothing;
* a request body naming a field Open WebUI v0.11.0 does not have —
  ``ModelForm`` is ``ConfigDict(extra='ignore')``, so the write is
  accepted with HTTP 200 and the field never lands (issue #1086); and
* a model listing read at the wrong route, in the wrong shape, or one
  page short, each of which prints nothing and reads as "not found".

The last two are why the grant assertion here is a READBACK: an
assertion over the payload this code constructs is an assertion about our
own request, and #1086 shipped underneath exactly such a test.
"""

from __future__ import annotations

import builtins
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.source_admin import (
    ACCESS_GRANT_PERMISSION,
    ACCESS_GRANT_PRINCIPAL_TYPE,
    AUTH_REQUIRED,
    FORBIDDEN_SOURCE_ADMIN_SURFACE,
    OPENAI_COMPAT_MODEL_ROUTE,
    VISIBILITY_BOUNDS_ROLE_NOT_GROUP,
    WORKSPACE_MODEL_LIST_PAGE_SIZE,
    WORKSPACE_MODEL_LIST_ROUTE,
    ChatSourceAdminError,
    LiveToolSurface,
    mcp_tool_id,
    okg_mcp_tool_universe,
    parse_openai_compat_model_ids,
    parse_workspace_model_listing,
    project_admin_preset,
    resolve_admin_tool_binding,
    user_preset_tools,
    verify_admin_preset_readback,
)
from okg.substrate.mcp.operators import (
    AKMON_READ_TOOLS,
    QUERY_ESCAPE_TOOLS,
    SOURCE_ADMIN_TOOLS,
)


ADMIN_ENDPOINT = "http://127.0.0.1:8101/mcp"
READONLY_ENDPOINT = "http://127.0.0.1:8100/mcp"
ADMIN_PRESET_ID = "okg-workspace-admin"
USER_PRESET_ID = "okg-workspace"
CONTROL_PRESET_ID = "okg-workspace-control"
ADMIN_GROUP_NAME = "okg-workspace-admins"
ADMIN_GROUP_ID = "b3f1c0de-0000-4000-8000-00000000aaaa"
MCP_SERVER_ID = "okg-okg-workspace"


def _write_enabled_surface(
    *,
    endpoint: str = ADMIN_ENDPOINT,
    extra: tuple[str, ...] = (),
    drop: tuple[str, ...] = (),
) -> LiveToolSurface:
    """A readback shaped like a real ``--allow-writes`` endpoint."""
    names = tuple(
        name
        for name in (
            tuple(AKMON_READ_TOOLS)
            + tuple(QUERY_ESCAPE_TOOLS)
            + ("record", "propose")
            + tuple(SOURCE_ADMIN_TOOLS)
        )
        if name not in set(drop)
    ) + extra
    return LiveToolSurface(
        endpoint=endpoint,
        tool_names=names,
        auth_posture=AUTH_REQUIRED,
        read_back_at="2026-08-04T00:00:00+00:00",
    )


def _read_only_surface() -> LiveToolSurface:
    """A readback shaped like a default (read-only) endpoint.

    The nine are all-or-nothing with ``allow_writes`` absent an explicit
    allowlist, so a read-only server registers none of them — not even
    ``list_sources``.
    """
    return LiveToolSurface(
        endpoint=READONLY_ENDPOINT,
        tool_names=tuple(AKMON_READ_TOOLS) + tuple(QUERY_ESCAPE_TOOLS),
        auth_posture=AUTH_REQUIRED,
        read_back_at="2026-08-04T00:00:00+00:00",
    )


# ---------------------------------------------------------------------------
# The bound set
# ---------------------------------------------------------------------------

def test_bound_set_equals_source_admin_tools_intersect_live_readback() -> None:
    live = _write_enabled_surface()
    bound = resolve_admin_tool_binding(live=live)
    # Asserted against the IMPORTED tuple. A tenth tool, or a renamed
    # one, fails here instead of quietly changing what chat binds.
    assert set(bound) == set(SOURCE_ADMIN_TOOLS)
    assert bound == SOURCE_ADMIN_TOOLS


def test_bound_set_tracks_a_narrower_live_surface() -> None:
    """An OKG_MCP_TOOL_ALLOWLIST narrowing the endpoint narrows the preset.

    The bound set is the INTERSECTION, so an endpoint that registers
    seven of the nine yields seven — never nine with two that answer
    'unknown tool'.
    """
    live = _write_enabled_surface(drop=("remove_source", "connector_ingest_batch"))
    bound = resolve_admin_tool_binding(live=live)
    assert set(bound) == set(SOURCE_ADMIN_TOOLS) - {
        "remove_source",
        "connector_ingest_batch",
    }
    assert len(bound) == len(SOURCE_ADMIN_TOOLS) - 2


def test_absent_tool_named_in_the_manifest_fails_naming_that_tool() -> None:
    live = _write_enabled_surface(drop=("connector_ingest_batch",))
    with pytest.raises(ChatSourceAdminError) as excinfo:
        resolve_admin_tool_binding(
            live=live,
            requested=("list_sources", "connector_ingest_batch"),
        )
    message = str(excinfo.value)
    assert "connector_ingest_batch" in message
    # The MISSING tool is named, and only it: a failure that named the
    # whole requested list would not tell the operator what to fix.
    assert excinfo.value.data["missing"] == ["connector_ingest_batch"]


def test_the_cli_exits_non_zero_naming_the_absent_tool(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """The refusal reaches the operator as a NON-ZERO EXIT, not a log line.

    ``okg chat source-admin`` is driven in-process with the live readback
    stubbed, so the observed facts are the command's own return code and
    the payload it emits — not an exception this test caught itself.
    """
    import json
    from argparse import Namespace

    from okg.substrate.cli.chat import _cmd_source_admin

    def _args(**overrides: Any) -> Namespace:
        base: dict[str, Any] = {
            # A deployment name no manifest claims: the verb must not
            # read this repo's real deployments to decide a refusal.
            "deployment": "chat-source-admin-fixture",
            "endpoint": ADMIN_ENDPOINT,
            "host": "127.0.0.1",
            "auth_token_env": None,
            "group": [ADMIN_GROUP_NAME],
            "tool": ["connector_ingest_batch"],
            "user_tool": [],
            "preset_id": ADMIN_PRESET_ID,
        }
        base.update(overrides)
        return Namespace(**base)

    monkeypatch.setattr(
        "okg.substrate.chat.source_admin.read_live_tools",
        lambda endpoint, **_: _write_enabled_surface(
            endpoint=endpoint, drop=("connector_ingest_batch",),
        ),
    )
    assert _cmd_source_admin(_args(), as_json=True) == 1
    emitted = capsys.readouterr()
    assert "connector_ingest_batch" in emitted.err
    assert json.loads(emitted.err)["error"]["data"]["missing"] == [
        "connector_ingest_batch",
    ]

    # POSITIVE CONTROL: the same command, same stub, with the tool
    # REGISTERED — exit 0. Without it a verb that refused everything
    # would pass the assertion above.
    monkeypatch.setattr(
        "okg.substrate.chat.source_admin.read_live_tools",
        lambda endpoint, **_: _write_enabled_surface(endpoint=endpoint),
    )
    assert _cmd_source_admin(_args(), as_json=True) == 0
    ok_payload = json.loads(capsys.readouterr().out)
    assert ok_payload["ok"] is True
    assert set(ok_payload["bound_tools"]) == set(SOURCE_ADMIN_TOOLS)


def test_empty_readback_is_refused_not_read_as_no_tools_admitted() -> None:
    """An empty reference set fails every membership test.

    Without this arm the bound set would be empty, the forbidden-set
    intersection would be empty, and both would read as agreement.
    """
    empty = LiveToolSurface(
        endpoint=ADMIN_ENDPOINT,
        tool_names=(),
        auth_posture=AUTH_REQUIRED,
    )
    with pytest.raises(ChatSourceAdminError) as excinfo:
        resolve_admin_tool_binding(live=empty)
    assert excinfo.value.data["reason"] == "empty_readback"
    assert "EMPTY" in str(excinfo.value)


def test_read_only_endpoint_cannot_carry_the_admin_preset() -> None:
    with pytest.raises(ChatSourceAdminError) as excinfo:
        resolve_admin_tool_binding(live=_read_only_surface())
    assert excinfo.value.data["reason"] == "read_only_endpoint"
    for name in SOURCE_ADMIN_TOOLS:
        assert name in str(excinfo.value)


def test_non_admin_preset_binds_none_of_the_nine() -> None:
    ok = user_preset_tools(
        tuple(AKMON_READ_TOOLS), live=_read_only_surface(),
    )
    assert set(ok) & set(SOURCE_ADMIN_TOOLS) == set()
    with pytest.raises(ChatSourceAdminError) as excinfo:
        user_preset_tools(("search", "add_source"))
    assert "add_source" in str(excinfo.value)


# ---------------------------------------------------------------------------
# The forbidden source-admin surface, as a NEGATIVE check
# ---------------------------------------------------------------------------

def test_forbidden_surface_set_is_non_empty() -> None:
    """Guards every intersection test below from passing vacuously."""
    assert FORBIDDEN_SOURCE_ADMIN_SURFACE
    # The four categories design.md §6 names, each represented.
    assert "update_config" in FORBIDDEN_SOURCE_ADMIN_SURFACE
    assert "edit_agent_spec" in FORBIDDEN_SOURCE_ADMIN_SURFACE
    assert "grader_submit" in FORBIDDEN_SOURCE_ADMIN_SURFACE
    assert {"piazza_post", "mattermost_post", "redmine_issue"} <= (
        FORBIDDEN_SOURCE_ADMIN_SURFACE
    )


def test_no_preset_binds_forbidden_source_admin_surface() -> None:
    """The rendered tool set ∩ the forbidden set is EMPTY.

    Checked at three levels so the empty intersection is a fact about
    the system, not an artifact of a small sample: the rendered admin
    preset, the rendered user preset, and the whole OKG MCP tool
    universe. Runtime config editing and agent-spec editing have no tool
    to bind — deployment.yaml is canonical and audited, and specs are
    versioned deployment content changed by PR.
    """
    admin = project_admin_preset(
        deployment="okg-workspace",
        preset_id=ADMIN_PRESET_ID,
        live=_write_enabled_surface(),
        access_group_names=(ADMIN_GROUP_NAME,),
    )
    assert set(admin.bound_tools) & FORBIDDEN_SOURCE_ADMIN_SURFACE == set()
    user = user_preset_tools(tuple(AKMON_READ_TOOLS))
    assert set(user) & FORBIDDEN_SOURCE_ADMIN_SURFACE == set()
    assert okg_mcp_tool_universe() & FORBIDDEN_SOURCE_ADMIN_SURFACE == frozenset()


def test_forbidden_check_has_teeth_when_a_forbidden_tool_is_asked_for() -> None:
    """The negative check is not vacuous: asking for one is refused.

    A server that somehow registered ``update_config`` plus a preset
    naming it is the configuration the boundary exists to reject, and
    the refusal names the tool.
    """
    live = _write_enabled_surface(extra=("update_config",))
    with pytest.raises(ChatSourceAdminError) as excinfo:
        project_admin_preset(
            deployment="okg-workspace",
            preset_id="okg-workspace-admin",
            live=live,
            access_group_names=("okg-workspace-admins",),
            requested_tools=("update_config",),
        )
    assert "update_config" in str(excinfo.value)
    assert excinfo.value.data["forbidden"] == ["update_config"]


# ---------------------------------------------------------------------------
# No chat-local source state
# ---------------------------------------------------------------------------

def test_projection_writes_no_chat_local_source_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The projection is pure. It cannot hold a copy of source state.

    Proven mechanically rather than by reading the code: every write
    path is poisoned for the duration of the call, so a projection that
    persisted anything would raise. The deployment manifest stays the
    only source of registry truth.
    """
    # Import the transitively-used modules BEFORE poisoning, so an
    # import-time read is not mistaken for a projection-time write.
    import okg.substrate.mcp.server  # noqa: F401

    monkeypatch.chdir(tmp_path)
    real_open = builtins.open

    def _poisoned_open(file, mode="r", *args, **kwargs):  # type: ignore[no-untyped-def]
        if any(flag in str(mode) for flag in ("w", "a", "x", "+")):
            raise AssertionError(f"projection wrote to {file!r}")
        return real_open(file, mode, *args, **kwargs)

    def _poisoned_write(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("projection wrote through pathlib")

    monkeypatch.setattr(builtins, "open", _poisoned_open)
    monkeypatch.setattr(Path, "write_text", _poisoned_write)
    monkeypatch.setattr(Path, "write_bytes", _poisoned_write)
    monkeypatch.setattr(Path, "mkdir", _poisoned_write)

    projection = project_admin_preset(
        deployment="okg-workspace",
        preset_id="okg-workspace-admin",
        live=_write_enabled_surface(),
        access_group_names=("okg-workspace-admins",),
        requested_tools=SOURCE_ADMIN_TOOLS,
    )

    monkeypatch.undo()
    assert set(projection.bound_tools) == set(SOURCE_ADMIN_TOOLS)
    assert list(tmp_path.iterdir()) == []


def test_projection_refuses_an_ungranted_admin_preset() -> None:
    """No group grant is not 'admins only'; it is everyone."""
    with pytest.raises(ChatSourceAdminError) as excinfo:
        project_admin_preset(
            deployment="okg-workspace",
            preset_id="okg-workspace-admin",
            live=_write_enabled_surface(),
            access_group_names=(),
        )
    assert "no group grant" in str(excinfo.value)


def test_projection_records_the_interim_as_not_sufficient() -> None:
    projection = project_admin_preset(
        deployment="okg-workspace",
        preset_id=ADMIN_PRESET_ID,
        live=_write_enabled_surface(),
        access_group_names=(ADMIN_GROUP_NAME,),
    )
    payload = projection.to_dict()
    assert payload["enforcement_label"] == "transport-plus-visibility"
    assert payload["enforcement_sufficient"] is False
    assert any(
        "mcp-principal-mapping" in arm for arm in payload["blocked_arms"]
    )
    # ROLE, not group. Carried on the payload so a passing visibility
    # readback cannot later be cited as evidence that an instance admin
    # OUTSIDE the deployment's admin group is excluded — they are not.
    assert payload["visibility_bounds"] == VISIBILITY_BOUNDS_ROLE_NOT_GROUP
    assert "BYPASS_ADMIN_ACCESS_CONTROL" in payload["visibility_bounds"]


def test_unresolved_group_ids_are_named_not_rendered_as_names() -> None:
    """No grant is emitted at all until the ids exist.

    ``principal_id`` is an instance group ID at v0.11.0. Emitting the
    manifest's NAME there would produce a grant that matches nothing and
    reads back as "no grant" forever, which is not "admins only".
    """
    payload = project_admin_preset(
        deployment="okg-workspace",
        preset_id=ADMIN_PRESET_ID,
        live=_write_enabled_surface(),
        access_group_names=(ADMIN_GROUP_NAME,),
    ).to_dict()
    assert "access_grants" not in payload
    assert ADMIN_GROUP_NAME in payload["access_grants_unresolved"]
    assert payload["access_group_names"] == [ADMIN_GROUP_NAME]
    # Positive control for the same assertion: with the ids resolved the
    # key IS emitted, so its absence above is a fact about the input and
    # not about `to_dict` never emitting grants.
    resolved = project_admin_preset(
        deployment="okg-workspace",
        preset_id=ADMIN_PRESET_ID,
        live=_write_enabled_surface(),
        access_group_names=(ADMIN_GROUP_NAME,),
        access_group_ids={ADMIN_GROUP_NAME: ADMIN_GROUP_ID},
    ).to_dict()
    assert resolved["access_grants"] == [{
        "principal_type": ACCESS_GRANT_PRINCIPAL_TYPE,
        "principal_id": ADMIN_GROUP_ID,
        "permission": ACCESS_GRANT_PERMISSION,
    }]
    assert "access_grants_unresolved" not in resolved


def test_a_group_with_no_instance_id_refuses_the_projection() -> None:
    with pytest.raises(ChatSourceAdminError) as excinfo:
        project_admin_preset(
            deployment="okg-workspace",
            preset_id=ADMIN_PRESET_ID,
            live=_write_enabled_surface(),
            access_group_names=(ADMIN_GROUP_NAME, "okg-workspace-readers"),
            access_group_ids={ADMIN_GROUP_NAME: ADMIN_GROUP_ID},
        )
    assert excinfo.value.data["reason"] == "unresolved_group_ids"
    assert excinfo.value.data["unresolved_groups"] == ["okg-workspace-readers"]


def test_meta_tool_ids_are_the_server_id_never_the_tool_names() -> None:
    """`toolIds` can never equal the nine names, so we never emit them.

    For an MCP server the middleware matches the ``server:mcp:`` prefix
    and takes the WHOLE remainder as one server id
    (utils/middleware.py:2740,2742), then registers every tool that
    server advertises (:2758).
    """
    payload = project_admin_preset(
        deployment="okg-workspace",
        preset_id=ADMIN_PRESET_ID,
        live=_write_enabled_surface(),
        access_group_names=(ADMIN_GROUP_NAME,),
        mcp_server_id=MCP_SERVER_ID,
    ).to_dict()
    assert payload["meta_tool_ids"] == [f"server:mcp:{MCP_SERVER_ID}"]
    assert set(payload["meta_tool_ids"]) & set(SOURCE_ADMIN_TOOLS) == set()
    # The tool NAMES live under their own key, and are not confusable
    # with the wire field.
    assert set(payload["bound_tools"]) == set(SOURCE_ADMIN_TOOLS)
    assert payload["bound_tool_count"] == len(SOURCE_ADMIN_TOOLS)
    assert "toolIds" not in payload


def test_an_empty_server_id_is_refused_rather_than_rendered() -> None:
    """`server:mcp:` with nothing after it looks configured and is dead."""
    with pytest.raises(ChatSourceAdminError) as excinfo:
        mcp_tool_id("")
    assert excinfo.value.data["reason"] == "empty_server_id"


def test_the_tool_id_prefix_is_the_renderers_own() -> None:
    """One definition of the prefix, so the two cannot drift apart."""
    from okg.substrate.chat.sync import MCP_TOOL_ID_PREFIX

    assert mcp_tool_id(MCP_SERVER_ID) == f"{MCP_TOOL_ID_PREFIX}{MCP_SERVER_ID}"


# ---------------------------------------------------------------------------
# The instance readback (issue #1086)
# ---------------------------------------------------------------------------
#
# The bug this section exists to catch: `to_dict` emitted
# `access_control: {"read": {"group_names": [...]}}`. That field does not
# exist at Open WebUI v0.11.0 — per-model access is `access_grants`
# (models/access_grants.py:48-57) — and `ModelForm` is declared
# `ConfigDict(extra='ignore')`, so the write returned HTTP 200 and the
# grant never landed. The old test asserted the payload WE construct, so
# it could not see any of that.

#: ``ModelForm``'s declared fields at v0.11.0, MEASURED with
#: ``docker exec open-webui sed -n '170,190p'
#: /app/backend/open_webui/models/models.py`` against
#: ``ghcr.io/open-webui/open-webui:v0.11.0``. Everything else is dropped
#: by ``ConfigDict(extra='ignore')`` — accepted, 200, gone.
VENDOR_MODEL_FORM_FIELDS = frozenset({
    "id",
    "base_model_id",
    "name",
    "meta",
    "params",
    "access_grants",
    "is_active",
})


class VendorModelStore:
    """Open WebUI v0.11.0's model store, faithful where it matters.

    Three measured behaviors, and each one is why an assertion over our
    own request body proves nothing:

    * ``ModelForm(extra='ignore')`` DROPS every undeclared key;
    * ``set_access_grants`` REPLACES the whole grant set, and ``None``
      skips the replacement entirely; and
    * ``GET /api/v1/models/list`` returns ``{items, total}`` and
      paginates at ``PAGE_ITEM_COUNT`` = 30, with a ``?query=`` filter
      that never matches ``Model.id``.
    """

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.grants: dict[str, list[dict[str, Any]]] = {}

    def create(self, form: dict[str, Any]) -> dict[str, Any]:
        accepted = {
            key: value
            for key, value in form.items()
            if key in VENDOR_MODEL_FORM_FIELDS
        }
        model_id = accepted["id"]
        self.rows[model_id] = {
            key: value
            for key, value in accepted.items()
            if key != "access_grants"
        }
        # Server-owned fields the caller must never send.
        self.rows[model_id].update(
            {"user_id": "fake-admin", "created_at": 1, "updated_at": 1},
        )
        posted = accepted.get("access_grants")
        if posted is not None:
            self.grants[model_id] = [
                {
                    "id": f"grant-{model_id}-{index}",
                    "resource_type": "model",
                    "resource_id": model_id,
                    "principal_type": str(grant.get("principal_type")),
                    "principal_id": str(grant.get("principal_id")),
                    "permission": str(grant.get("permission")),
                    "created_at": 0,
                }
                for index, grant in enumerate(posted)
                if isinstance(grant, dict)
            ]
        return self.row(model_id)

    def row(self, model_id: str) -> dict[str, Any]:
        row = dict(self.rows[model_id])
        # ModelModel declares `access_grants` with a list default_factory,
        # so the FIELD is always present — empty when nothing granted.
        row["access_grants"] = [dict(g) for g in self.grants.get(model_id, [])]
        return row

    def list_page(self, page: int = 1) -> dict[str, Any]:
        """``GET /api/v1/models/list?page=<n>``."""
        ordered = sorted(self.rows)
        start = (max(1, page) - 1) * WORKSPACE_MODEL_LIST_PAGE_SIZE
        window = ordered[start:start + WORKSPACE_MODEL_LIST_PAGE_SIZE]
        return {
            "items": [self.row(model_id) for model_id in window],
            "total": len(ordered),
        }

    def openai_compat(self) -> dict[str, Any]:
        """``GET /api/v1/models`` — the OpenAI-compat shape."""
        return {"data": [{"id": model_id} for model_id in sorted(self.rows)]}


def _model_form(
    payload: dict[str, Any], *, name: str = "OKG admin",
) -> dict[str, Any]:
    """A ``ModelForm`` body built FROM the projection's own payload.

    Whatever ``to_dict`` says the grant is, that is what gets posted. A
    projection naming a field the vendor does not have therefore posts a
    body with no grant in it, which is exactly what #1086 did.
    """
    form: dict[str, Any] = {
        "id": payload["preset_id"],
        "name": name,
        "base_model_id": None,
        "meta": {
            "description": "OKG source administration",
            "toolIds": payload.get("meta_tool_ids", []),
        },
        "params": {},
        "is_active": True,
    }
    for key in ("access_grants", "access_control"):
        if key in payload:
            form[key] = payload[key]
    return form


def _control_form(preset_id: str = CONTROL_PRESET_ID) -> dict[str, Any]:
    """A preset KNOWN to carry a grant — the positive control."""
    return {
        "id": preset_id,
        "name": "OKG control",
        "base_model_id": None,
        "meta": {"toolIds": []},
        "params": {},
        "is_active": True,
        "access_grants": [{
            "principal_type": ACCESS_GRANT_PRINCIPAL_TYPE,
            "principal_id": ADMIN_GROUP_ID,
            "permission": ACCESS_GRANT_PERMISSION,
        }],
    }


def _user_form() -> dict[str, Any]:
    return {
        "id": USER_PRESET_ID,
        "name": "OKG",
        "base_model_id": None,
        "meta": {"toolIds": ["server:mcp:okg-okg-workspace-readonly"]},
        "params": {},
        "is_active": True,
        "access_grants": [],
    }


def _admin_projection_payload(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "deployment": "okg-workspace",
        "preset_id": ADMIN_PRESET_ID,
        "live": _write_enabled_surface(),
        "access_group_names": (ADMIN_GROUP_NAME,),
        "access_group_ids": {ADMIN_GROUP_NAME: ADMIN_GROUP_ID},
        "mcp_server_id": MCP_SERVER_ID,
    }
    kwargs.update(overrides)
    return project_admin_preset(**kwargs).to_dict()


def _verify(store: VendorModelStore, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "pages": [store.list_page(1)],
        "admin_preset_id": ADMIN_PRESET_ID,
        "mcp_server_id": MCP_SERVER_ID,
        "live_tool_names": _write_enabled_surface().tool_names,
        # The INSTANCE's own view of what that connection advertises.
        # A second, independent read: comparing the live tools/list to
        # itself would be a tautology.
        "connection_tool_names": _write_enabled_surface().tool_names,
        "admin_group_id": ADMIN_GROUP_ID,
        "positive_control_preset_id": CONTROL_PRESET_ID,
        "user_preset_id": USER_PRESET_ID,
    }
    kwargs.update(overrides)
    return verify_admin_preset_readback(**kwargs)


def test_vendor_form_drops_undeclared_fields() -> None:
    """The fake is faithful to `extra='ignore'`, and says so up front.

    Without this arm every readback assertion below could be passing
    because the store stored whatever it was handed.
    """
    assert "access_control" not in VENDOR_MODEL_FORM_FIELDS
    assert "access_grants" in VENDOR_MODEL_FORM_FIELDS
    store = VendorModelStore()
    store.create({
        **_control_form("probe"),
        "access_control": {"read": {"group_names": [ADMIN_GROUP_NAME]}},
        "some_field_the_vendor_never_had": 1,
    })
    row = store.row("probe")
    assert "access_control" not in row
    assert "some_field_the_vendor_never_had" not in row
    # ...and the DECLARED field did land, so the drop above is about the
    # field name and not about the store refusing everything.
    assert len(row["access_grants"]) == 1


def test_the_admin_grant_survives_the_vendor_form_and_reads_back() -> None:
    """END STATE, not our request: the grant is read off the listing."""
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    store.create(_user_form())

    verdict = _verify(store)
    assert verdict.grant_ok is True
    assert verdict.positive_control_grants == 1
    assert verdict.meta_tool_ids == (f"server:mcp:{MCP_SERVER_ID}",)
    assert verdict.meta_tool_ids_ok is True
    assert verdict.user_preset_clean is True
    assert verdict.reachable_tools_ok is True
    assert verdict.ok is True
    # The admitted subset is shown NON-EMPTY with its member count, which
    # is what stops `empty == empty` from reading as correct.
    assert verdict.admitted_count == len(SOURCE_ADMIN_TOOLS)
    assert set(verdict.admitted_tools) == set(SOURCE_ADMIN_TOOLS)


def test_a_connection_pointed_at_the_read_only_endpoint_fails_the_binding() -> None:
    """Assertion (ii) is TWO reads, so a mispointed connection is caught.

    The live tools/list still shows the nine (the write-enabled endpoint
    is up), but the INSTANCE's connection advertises none of them. A
    check that compared the live readback to itself would report this
    admin surface as correctly bound.
    """
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    verdict = _verify(
        store,
        user_preset_id=None,
        connection_tool_names=_read_only_surface().tool_names,
    )
    assert verdict.admitted_count == len(SOURCE_ADMIN_TOOLS)
    assert verdict.reachable_tools_ok is False
    assert verdict.ok is False
    assert any("reachable through MCP server" in f for f in verdict.findings)


def test_an_unread_connection_tool_set_does_not_score_the_arm_true() -> None:
    """Not measured is not proven — never True by default."""
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    verdict = _verify(
        store, user_preset_id=None, connection_tool_names=None,
    )
    assert verdict.reachable_tools_ok is False
    assert verdict.ok is False
    assert any("assertion (ii) was not made" in f for f in verdict.findings)


def test_the_shipped_access_control_payload_reads_back_with_no_grant() -> None:
    """Issue #1086, caught the only way it can be: from the instance.

    This is the payload ``to_dict`` used to emit. The write is accepted,
    the run reports success, and the preset ends up with NO grant — which
    means every non-admin sees the admin surface. The positive control in
    the same readback comes back non-empty, so the empty grant set here
    is a result about the preset rather than a broken query.
    """
    store = VendorModelStore()
    shipped = {
        "preset_id": ADMIN_PRESET_ID,
        "meta_tool_ids": [f"server:mcp:{MCP_SERVER_ID}"],
        # The exact pre-fix shape, from source_admin.py:754.
        "access_control": {"read": {"group_names": [ADMIN_GROUP_NAME]}},
    }
    accepted = store.create(_model_form(shipped))
    # HTTP 200, and the row simply has no grant.
    assert accepted["access_grants"] == []
    store.create(_control_form())
    store.create(_user_form())

    verdict = _verify(store)
    assert verdict.positive_control_grants == 1, (
        "the readback itself must be shown working before an empty grant "
        "set may be read as a result"
    )
    assert verdict.grant_ok is False
    assert verdict.ok is False
    assert any("grants are []" in f for f in verdict.findings)


def test_a_grant_naming_the_group_by_name_does_not_match() -> None:
    """`principal_id` is a uuid; a name there reads back as no grant."""
    store = VendorModelStore()
    payload = _admin_projection_payload()
    payload["access_grants"] = [{
        "principal_type": ACCESS_GRANT_PRINCIPAL_TYPE,
        "principal_id": ADMIN_GROUP_NAME,
        "permission": ACCESS_GRANT_PERMISSION,
    }]
    store.create(_model_form(payload))
    store.create(_control_form())
    verdict = _verify(store, user_preset_id=None)
    assert verdict.grant_ok is False
    assert verdict.positive_control_grants == 1


def test_a_broken_readback_refuses_instead_of_reporting_no_grant() -> None:
    """When the CONTROL comes back empty, nothing else is a result."""
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create({**_control_form(), "access_grants": []})
    with pytest.raises(ChatSourceAdminError) as excinfo:
        _verify(store, user_preset_id=None)
    assert excinfo.value.data["reason"] == "positive_control_empty"


def test_an_absent_admin_preset_is_a_failed_readback() -> None:
    store = VendorModelStore()
    store.create(_control_form())
    with pytest.raises(ChatSourceAdminError) as excinfo:
        _verify(store, user_preset_id=None)
    assert excinfo.value.data["reason"] == "admin_preset_absent"


def test_the_user_preset_binding_the_admin_server_fails_the_readback() -> None:
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    store.create({
        **_user_form(),
        "meta": {"toolIds": [f"server:mcp:{MCP_SERVER_ID}"]},
    })
    verdict = _verify(store)
    assert verdict.user_preset_clean is False
    assert verdict.ok is False


def test_tool_names_in_tool_ids_fail_the_wire_assertion() -> None:
    """A preset whose `toolIds` hold tool names is misconfigured.

    The middleware only ever resolves ``server:mcp:<id>``, so this shape
    registers nothing at all while looking bound.
    """
    store = VendorModelStore()
    payload = _admin_projection_payload()
    payload["meta_tool_ids"] = list(SOURCE_ADMIN_TOOLS)
    store.create(_model_form(payload))
    store.create(_control_form())
    verdict = _verify(store, user_preset_id=None)
    assert verdict.meta_tool_ids_ok is False
    assert verdict.ok is False


# ---------------------------------------------------------------------------
# ANTI-VACUITY: an empty admitted subset is not agreement
# ---------------------------------------------------------------------------

def test_an_empty_admitted_subset_is_refused_not_scored_as_equal() -> None:
    """`empty == empty` would report an admin surface binding ZERO tools.

    On a read-only launch the nine are unregistered, so the intersection
    of SOURCE_ADMIN_TOOLS with the live tools/list is empty — and an
    equality between two empty sets passes while proving nothing.
    """
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    with pytest.raises(ChatSourceAdminError) as excinfo:
        _verify(
            store,
            user_preset_id=None,
            live_tool_names=_read_only_surface().tool_names,
        )
    assert excinfo.value.data["reason"] == "empty_admitted_subset"


def test_a_narrowing_filter_list_narrows_the_admitted_subset() -> None:
    """`function_name_filter_list` is the only per-connection lever.

    The positive control for the arm above: a filter list that still
    admits some of the nine yields a NON-EMPTY subset with a smaller
    count, so the refusal is about emptiness and not about filtering.
    """
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    verdict = _verify(
        store,
        user_preset_id=None,
        connection_filter_list=("list_sources", "source_status", "search"),
    )
    assert set(verdict.admitted_tools) == {"list_sources", "source_status"}
    assert verdict.admitted_count == 2
    assert verdict.narrowing_observed is True

    with pytest.raises(ChatSourceAdminError) as excinfo:
        _verify(
            store,
            user_preset_id=None,
            connection_filter_list=("search",),
        )
    assert excinfo.value.data["reason"] == "empty_admitted_subset"


def test_an_unread_filter_list_is_named_as_an_unexercised_arm() -> None:
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    verdict = _verify(store, user_preset_id=None, connection_filter_list=None)
    assert verdict.narrowing_observed is False
    assert any("NOT READ" in finding for finding in verdict.findings)


# ---------------------------------------------------------------------------
# The routes, and the three ways a read of them is silently wrong
# ---------------------------------------------------------------------------

def test_a_truncated_listing_is_refused_not_read_as_absence() -> None:
    """One page short, and the preset on page two reads as missing."""
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_control_form())
    for index in range(WORKSPACE_MODEL_LIST_PAGE_SIZE):
        store.create(_control_form(f"zz-filler-{index:02d}"))

    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_workspace_model_listing([store.list_page(1)])
    assert excinfo.value.data["reason"] == "listing_truncated"
    assert excinfo.value.data["page_size"] == WORKSPACE_MODEL_LIST_PAGE_SIZE

    # POSITIVE CONTROL: every page read, and the same call succeeds and
    # finds the preset that page one omitted.
    rows = parse_workspace_model_listing(
        [store.list_page(1), store.list_page(2)]
    )
    assert len(rows) == store.list_page(1)["total"]
    assert ADMIN_PRESET_ID in {str(row["id"]) for row in rows}


def test_the_openai_compat_payload_is_refused_by_the_listing_parser() -> None:
    store = VendorModelStore()
    store.create(_control_form())
    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_workspace_model_listing([store.openai_compat()])
    assert excinfo.value.data["reason"] == "openai_compat_route"
    assert OPENAI_COMPAT_MODEL_ROUTE in str(excinfo.value)


def test_a_bare_list_payload_is_refused_by_both_parsers() -> None:
    """`jq '.[]'` over either route yields nothing — a pass by absence."""
    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_workspace_model_listing([[{"id": ADMIN_PRESET_ID}]])
    assert excinfo.value.data["reason"] == "bare_list_payload"
    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_openai_compat_model_ids([{"id": ADMIN_PRESET_ID}])
    assert excinfo.value.data["reason"] == "bare_list_payload"


def test_a_page_with_no_total_cannot_be_shown_complete() -> None:
    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_workspace_model_listing([{"items": []}])
    assert excinfo.value.data["reason"] == "no_total"


def test_no_page_at_all_is_not_an_empty_listing() -> None:
    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_workspace_model_listing([])
    assert excinfo.value.data["reason"] == "no_pages"


def test_the_visibility_read_uses_the_openai_compat_route() -> None:
    """And refuses the workspace listing, which is the other route."""
    store = VendorModelStore()
    store.create(_model_form(_admin_projection_payload()))
    store.create(_user_form())
    assert set(parse_openai_compat_model_ids(store.openai_compat())) == {
        ADMIN_PRESET_ID, USER_PRESET_ID,
    }
    with pytest.raises(ChatSourceAdminError) as excinfo:
        parse_openai_compat_model_ids(store.list_page(1))
    assert excinfo.value.data["reason"] == "workspace_listing_route"
    assert WORKSPACE_MODEL_LIST_ROUTE in str(excinfo.value)
