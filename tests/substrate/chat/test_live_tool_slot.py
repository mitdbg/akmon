"""The live-tool connection slot (#1183 S4, PACT ``chat-live-tool-slot``).

Umbrella decision 6: graph tools and live tools carry different authority,
and a subject allowed to search the graph does not thereby receive tools
that call operational systems. Open WebUI's per-tool filter belongs to the
CONNECTION, so the only mechanism that can hold both authorities separably
is two registrations — which is what this slice adds and what these tests
assert, on the WIRE the fake instance holds rather than on internal state.

Hermetic. No Postgres, no container. The fake grows a SECOND MCP endpoint
with its own credential and its own advertised names, so a live connection
registered with the graph connection's token fails verify here rather than
on a live site.
"""

from __future__ import annotations

import copy
import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.bundle_composition import (
    ChatBundleLiveToolConflict,
    ChatBundleManifestWidens,
    BundleChatSelection,
    compose_bundle_chat,
)
from okg.substrate.chat.live_tool_slot import (
    LIVE_CONNECTION_SUFFIX,
    ChatLiveToolSlotError,
    resolve_live_tool_slot,
)
from okg.substrate.chat.plan_apply import (
    chat_readback,
    plan_digest,
    resolve_chat_plan,
    write_chat_plan,
    write_chat_readback,
)
from okg.substrate.chat.sync import ChatSyncError, InstanceClient, sync
from okg.substrate.deployments.chat_block import ChatManifestError, parse_chat_block

from .fake_openwebui import FakeInstance
from .test_write_connection_projection import (
    AUTH_TOKEN_ENV,
    AUTH_TOKEN_VALUE,
    BASE_URL,
    READ_TOOLS,
    TOKEN,
    _instance,
    _make_inputs,
    _pin_endpoint,
    _raw_manifest,
)

GRAPH_SERVER_ID = "okg-fixture"
LIVE_SERVER_ID = f"{GRAPH_SERVER_ID}{LIVE_CONNECTION_SUFFIX}"
LIVE_URL = "http://live-tools.test:8790/mcp"
LIVE_TOKEN_ENV = "OKG_FIXTURE_LIVE_MCP_TOKEN"

#: Live-tool names. Deliberately NOT OKG operator names: a live tool reads a
#: current external system, so it is not an operator and must not borrow an
#: operator's name.
LIVE_NAMES = ("incident_status", "oncall_roster")


def _live_token() -> str:
    """A credential built at RUN TIME, so no line here looks like a secret."""
    return "live-" + uuid.uuid4().hex


def _manifest(
    *,
    names: tuple[str, ...] = LIVE_NAMES,
    grants: dict[str, list[str]] | None = None,
    auth_token_env: str | None = LIVE_TOKEN_ENV,
    slot: bool = True,
) -> dict[str, Any]:
    """The S1 fixture manifest plus a live-tool slot and audience grants."""
    manifest = _raw_manifest(sso=False)
    mcp = manifest["chat"]["mcp"]
    if slot:
        block: dict[str, Any] = {"names": list(names), "url": LIVE_URL}
        if auth_token_env is not None:
            block["auth_token_env"] = auth_token_env
        mcp["live_tools"] = block
    if grants is not None:
        # Every audience must declare the graph set too: `plan_tool_curation`
        # already refuses two audiences over one GRAPH connection, and this
        # slice does not change that rule.
        mcp["audiences"] = {
            name: {"tools": list(READ_TOOLS), "live_tools": list(live)}
            for name, live in grants.items()
        }
    return manifest


def _inputs(tmp_path: Path, manifest: dict[str, Any]) -> Any:
    return _make_inputs(tmp_path, manifest=manifest)


def _prepared(
    monkeypatch: pytest.MonkeyPatch, *, live_token: str | None,
) -> FakeInstance:
    """A fake instance serving BOTH endpoints, with the audience group known."""
    _pin_endpoint(monkeypatch, advertised=READ_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    instance.add_group("analysts", "group-analysts")
    instance.add_group("responders", "group-responders")
    instance.add_live_endpoint(LIVE_URL, token=live_token, specs=LIVE_NAMES)
    if live_token is not None:
        monkeypatch.setenv(LIVE_TOKEN_ENV, live_token)
    return instance


def _run(inputs: Any, instance: FakeInstance) -> Any:
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=lambda _endpoint, _env: READ_TOOLS,
    )


def _entry(instance: FakeInstance, server_id: str) -> dict[str, Any] | None:
    for entry in instance.tool_servers:
        if (entry.get("info") or {}).get("id") == server_id:
            return entry
    return None


# ---------------------------------------------------------------------------
# req.chat-live-tool-slot.separate-connection
# ---------------------------------------------------------------------------

def test_the_slot_registers_a_second_connection_with_its_own_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Its own id, its own URL, its own bearer — not the graph one's."""
    live_token = _live_token()
    instance = _prepared(monkeypatch, live_token=live_token)
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))

    _run(inputs, instance)

    live = _entry(instance, LIVE_SERVER_ID)
    graph = _entry(instance, GRAPH_SERVER_ID)
    assert live is not None and graph is not None
    assert live["url"] == LIVE_URL
    assert live["url"] != graph["url"]
    assert live["auth_type"] == "bearer"
    # THE separation, at the wire: two registrations, two credentials.
    assert live["key"] == live_token
    assert graph["key"] == AUTH_TOKEN_VALUE
    assert live["key"] != graph["key"]
    assert live["config"]["access_grants"] == [
        {
            "principal_type": "group",
            "principal_id": "group-analysts",
            "permission": "read",
        },
    ]


def test_the_two_filters_never_share_a_name_in_either_direction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asserted BOTH ways, off the stored filters rather than off the run."""
    instance = _prepared(monkeypatch, live_token=_live_token())
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))

    _run(inputs, instance)

    def _names(entry: dict[str, Any]) -> set[str]:
        raw = str(entry["config"]["function_name_filter_list"] or "")
        return {n.strip().lstrip("!") for n in raw.split(",") if n.strip()}

    graph_names = _names(_entry(instance, GRAPH_SERVER_ID))
    live_names = _names(_entry(instance, LIVE_SERVER_ID))
    assert live_names == set(LIVE_NAMES)
    assert graph_names & set(LIVE_NAMES) == set()
    assert live_names & set(READ_TOOLS) == set()
    assert live_names & graph_names == set()


def test_the_disjointness_assertion_fires_when_the_filters_are_forced_to_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard is real, not decoration.

    `resolve_live_tool_slot` refuses an operator name before this can
    happen, so the guard is reached by handing `assert_transports_disjoint`
    the two filters directly — which is exactly the shape an edit that
    dropped the earlier refusal would produce.
    """
    from okg.substrate.chat.live_tool_slot import assert_transports_disjoint

    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))
    slot = resolve_live_tool_slot(
        inputs.chat, deployment="fixture", mcp_server_id=GRAPH_SERVER_ID,
    )
    assert_transports_disjoint(READ_TOOLS, slot, deployment="fixture")
    with pytest.raises(ChatBundleLiveToolConflict) as excinfo:
        assert_transports_disjoint(
            (*READ_TOOLS, f"!{LIVE_NAMES[0]}"), slot, deployment="fixture",
        )
    # A DENY entry still means the graph connection knows the name.
    assert excinfo.value.data["tool"] == LIVE_NAMES[0]
    assert excinfo.value.code == "chat_bundle_live_tool_conflict"


# ---------------------------------------------------------------------------
# req.chat-live-tool-slot.audience-authority
# ---------------------------------------------------------------------------

def test_no_audience_grant_registers_no_live_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slot is a ceiling, never a grant. The default is nothing."""
    instance = _prepared(monkeypatch, live_token=_live_token())
    inputs = _inputs(tmp_path, _manifest())

    _run(inputs, instance)

    assert _entry(instance, LIVE_SERVER_ID) is None
    assert _entry(instance, GRAPH_SERVER_ID) is not None
    slot = resolve_live_tool_slot(
        inputs.chat, deployment="fixture", mcp_server_id=GRAPH_SERVER_ID,
    )
    assert slot is not None and slot.allowed == LIVE_NAMES
    assert slot.granted == () and slot.registered is False


def test_an_audience_granted_a_name_the_slot_does_not_allow_refuses(
    tmp_path: Path,
) -> None:
    inputs = _inputs(
        tmp_path,
        _manifest(names=("incident_status",), grants={"analysts": ["pager_page"]}),
    )
    with pytest.raises(ChatLiveToolSlotError) as excinfo:
        resolve_live_tool_slot(
            inputs.chat, deployment="fixture", mcp_server_id=GRAPH_SERVER_ID,
        )
    assert excinfo.value.data["reason"] == "audience_grants_undeclared_live_tool"
    assert excinfo.value.data["audience"] == "analysts"
    assert excinfo.value.code == "chat_live_tool_slot_refused"


def test_two_audiences_granted_different_live_sets_refuse(tmp_path: Path) -> None:
    """One live registration serves exactly one set. Same rule as the graph."""
    inputs = _inputs(
        tmp_path,
        _manifest(
            grants={
                "analysts": [LIVE_NAMES[0]],
                "responders": list(LIVE_NAMES),
            },
        ),
    )
    with pytest.raises(ChatLiveToolSlotError) as excinfo:
        resolve_live_tool_slot(
            inputs.chat, deployment="fixture", mcp_server_id=GRAPH_SERVER_ID,
        )
    assert excinfo.value.data["reason"] == "two_audiences_one_live_connection"


def test_a_live_tool_name_that_is_a_graph_operator_refuses(tmp_path: Path) -> None:
    """Reachable from a MANIFEST now — which S3 recorded that it was not."""
    inputs = _inputs(tmp_path, _manifest(names=("search",)))
    with pytest.raises(ChatBundleLiveToolConflict) as excinfo:
        resolve_live_tool_slot(
            inputs.chat, deployment="fixture", mcp_server_id=GRAPH_SERVER_ID,
        )
    assert excinfo.value.data["reason"] == "live_tool_shadows_graph_tool"
    assert excinfo.value.data["tool"] == "search"


def test_an_empty_slot_or_a_urlless_slot_refuses_at_parse(tmp_path: Path) -> None:
    for block, path in (
        ({"names": [], "url": LIVE_URL}, "chat.mcp.live_tools.names"),
        ({"names": list(LIVE_NAMES)}, "chat.mcp.live_tools.url"),
        ({"names": list(LIVE_NAMES), "url": "ftp://nope"}, "chat.mcp.live_tools.url"),
    ):
        manifest = _raw_manifest(sso=False)
        manifest["chat"]["mcp"]["live_tools"] = block
        with pytest.raises(ChatManifestError) as excinfo:
            parse_chat_block(manifest, manifest_path=tmp_path / "deployment.yaml")
        assert excinfo.value.data["path"] == path


# ---------------------------------------------------------------------------
# req.chat-live-tool-slot.bundle-ceiling
# ---------------------------------------------------------------------------

def _selection(live_tools: list[str]) -> BundleChatSelection:
    """A resolved contribution, built directly.

    `compose_bundle_chat` consumes the RESOLVED document, so the ceiling is
    exercised without rebuilding a package archive — the package path is
    S3's own suite and proves nothing more about this rule.
    """
    document = {
        "contribution": {"owner": "archi-test", "id": "chat-contribution"},
        "branding": {"name": "Bundle Site", "description": None, "logo": None},
        "instructions": [],
        "connections": [],
        "graph_tools": list(READ_TOOLS),
        "live_tools": sorted(live_tools),
    }
    return BundleChatSelection(
        bundle_id="archi-test:bundle",
        bundle_digest="sha256:bundle",
        release_digest="sha256:release",
        contribution={"schema": "okg.deployment_product.chat_contribution/v1"},
        document=document,
    )


def test_the_bundle_ceiling_narrows_and_the_manifest_may_ask_for_less(
    tmp_path: Path,
) -> None:
    chat = _inputs(tmp_path, _manifest(names=("incident_status",))).chat
    _effective, record = compose_bundle_chat(chat, _selection(list(LIVE_NAMES)))
    assert record["live_tools"]["declared"] == sorted(LIVE_NAMES)
    # DETERMINED, not the `undetermined` placeholder S3 recorded.
    assert record["live_tools"]["selected"] == ["incident_status"]


def test_a_manifest_naming_a_live_tool_the_bundle_did_not_declare_refuses(
    tmp_path: Path,
) -> None:
    """The widening arm S3 could not reach. `chat.mcp.live_tools.names` is
    deliberately not closed to the operator universe, so it is reachable."""
    chat = _inputs(tmp_path, _manifest()).chat
    with pytest.raises(ChatBundleManifestWidens) as excinfo:
        compose_bundle_chat(chat, _selection(["incident_status"]))
    assert excinfo.value.data["reason"] == "manifest_widens_live_tools"
    assert excinfo.value.data["surface"] == "mcp:live_tools"
    assert excinfo.value.data["undeclared"] == "oncall_roster"


def test_with_no_bundle_the_manifest_slot_stands_alone(tmp_path: Path) -> None:
    slot = resolve_live_tool_slot(
        _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)})).chat,
        deployment="fixture",
        mcp_server_id=GRAPH_SERVER_ID,
    )
    assert slot is not None
    assert slot.allowed == LIVE_NAMES
    assert slot.granted == tuple(sorted(LIVE_NAMES))
    assert slot.audiences == ("analysts",)


# ---------------------------------------------------------------------------
# req.chat-live-tool-slot.plan-and-readback
# ---------------------------------------------------------------------------

def test_the_plan_records_the_slot_and_the_digest_covers_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_token = _live_token()
    monkeypatch.setenv(AUTH_TOKEN_ENV, AUTH_TOKEN_VALUE)
    monkeypatch.setenv(LIVE_TOKEN_ENV, live_token)
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))

    first = Path(write_chat_plan(inputs, tmp_path / "a")["plan_path"])
    second = Path(write_chat_plan(inputs, tmp_path / "b")["plan_path"])
    assert first.read_bytes() == second.read_bytes()

    plan = json.loads(first.read_text())
    surface = plan["surfaces"]["live_tools"]
    assert surface["desired"]["live_connection:present"] is True
    assert surface["desired"]["live_connection:info.id"] == LIVE_SERVER_ID
    assert surface["desired"]["live_connection:url"] == LIVE_URL
    assert surface["desired"][
        "live_connection:config.function_name_filter_list"
    ] == ",".join(sorted(LIVE_NAMES))
    # Declarations: digested, never compared. See `_live_tools_surface`.
    assert surface["slot"] == {
        "allowed": sorted(LIVE_NAMES),
        "granted": sorted(LIVE_NAMES),
        "audiences": ["analysts"],
    }
    # The REFERENCE, never the value — and its own, not the graph one's.
    assert {
        "kind": "env", "variable": LIVE_TOKEN_ENV, "used_by": "live_connection:key",
    } in plan["credential_references"]
    raw = first.read_bytes()
    assert live_token.encode("utf-8") not in raw
    assert AUTH_TOKEN_VALUE.encode("utf-8") not in raw

    narrowed = resolve_chat_plan(
        _inputs(
            tmp_path,
            _manifest(
                names=("incident_status",), grants={"analysts": ["incident_status"]},
            ),
        ),
    )
    assert narrowed["plan_digest"] != plan["plan_digest"]
    assert plan["plan_digest"] == plan_digest(plan)


def test_an_ungranted_slot_does_not_require_its_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan and apply must agree about one manifest.

    A slot nobody is granted registers no connection, so its variable is
    never read. A plan that refused on it would refuse a manifest `sync`
    applies without complaint.
    """
    instance = _prepared(monkeypatch, live_token=None)
    monkeypatch.delenv(LIVE_TOKEN_ENV, raising=False)
    inputs = _inputs(tmp_path, _manifest())

    plan = resolve_chat_plan(inputs)
    assert plan["surfaces"]["live_tools"]["desired"] == {
        "live_connection:present": False,
    }
    assert plan["surfaces"]["live_tools"]["slot"]["allowed"] == sorted(LIVE_NAMES)
    assert all(
        ref["variable"] != LIVE_TOKEN_ENV
        for ref in plan["credential_references"]
    )
    _run(inputs, instance)
    assert _entry(instance, LIVE_SERVER_ID) is None


def test_a_declared_but_unset_live_credential_reference_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan AND sync, with nothing registered on either connection."""
    from okg.substrate.chat.plan_apply import ChatPlanError

    _pin_endpoint(monkeypatch, advertised=READ_TOOLS, enforces_auth=True)
    monkeypatch.delenv(LIVE_TOKEN_ENV, raising=False)
    instance = _instance()
    instance.add_group("analysts", "group-analysts")
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))

    with pytest.raises(ChatPlanError) as plan_exc:
        resolve_chat_plan(inputs)
    assert plan_exc.value.data["reason"] == "credential_reference_unresolved"
    assert plan_exc.value.data["variable"] == LIVE_TOKEN_ENV

    with pytest.raises(ChatSyncError) as sync_exc:
        _run(inputs, instance)
    assert sync_exc.value.data["reason"] == "mcp_connection_credential_unresolved"
    assert sync_exc.value.data["auth_token_env"] == LIVE_TOKEN_ENV
    assert instance.tool_servers == []


def test_the_readback_determines_the_live_connection_then_catches_a_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Match on a healthy site; deviation after an out-of-band edit."""
    live_token = _live_token()
    instance = _prepared(monkeypatch, live_token=live_token)
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))
    plan = resolve_chat_plan(inputs)
    _run(inputs, instance)

    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    document = chat_readback(inputs, client, plan)
    live = document["surfaces"]["live_tools"]
    assert live["status"] == "match", live
    assert "live_connection:present" in live["compared_keys"]
    assert document["live_verify_probe"]["state"] == "healthy"

    # An out-of-band edit: somebody widens the live filter by hand.
    entry = _entry(instance, LIVE_SERVER_ID)
    entry["config"]["function_name_filter_list"] = "incident_status,pager_page"
    after = chat_readback(inputs, client, plan)
    assert after["surfaces"]["live_tools"]["status"] == "deviation"
    assert after["ok"] is False
    keys = {d["key"] for d in after["deviations"]}
    assert "live_connection:config.function_name_filter_list" in keys

    written = write_chat_readback(after, tmp_path / "rb")
    raw = Path(written["readback_path"]).read_bytes()
    assert live_token.encode("utf-8") not in raw
    assert AUTH_TOKEN_VALUE.encode("utf-8") not in raw


def test_a_rotated_live_token_the_site_never_stored_is_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2287 on the LIVE connection: its own credential, its own check.

    The live endpoint and the operator's variable move to a new token without
    a sync. The verify probe dials with the new one and answers healthy; the
    stored credential is what a chat turn sends, and it is stale.
    """
    live_token = _live_token()
    instance = _prepared(monkeypatch, live_token=live_token)
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))
    plan = resolve_chat_plan(inputs)
    _run(inputs, instance)
    new_live = _live_token()
    instance.live_endpoints[LIVE_URL]["token"] = new_live
    monkeypatch.setenv(LIVE_TOKEN_ENV, new_live)

    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    document = chat_readback(inputs, client, plan)
    assert not document["ok"]
    assert [
        (d["surface"], d["key"], d["reason"], d["differing"])
        for d in document["deviations"]
    ] == [
        ("live_tools", "live_stored_credential",
         "mcp_connection_credential_stale", ["key"]),
    ]
    assert document["live_verify_probe"]["state"] == "healthy"
    assert document["live_verify_probe"]["stored_credential"]["state"] == "stale"

    written = write_chat_readback(document, tmp_path / "rb")
    raw = Path(written["readback_path"]).read_text(encoding="utf-8")
    for secret in (live_token, new_live, AUTH_TOKEN_VALUE, "hmac:"):
        assert secret not in raw


def test_a_live_connection_nobody_planned_reads_as_a_deviation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """"Never by default", measured — not asserted."""
    instance = _prepared(monkeypatch, live_token=None)
    inputs = _inputs(tmp_path, _manifest(slot=False))
    plan = resolve_chat_plan(inputs)
    assert plan["surfaces"]["live_tools"]["desired"] == {
        "live_connection:present": False,
    }
    _run(inputs, instance)
    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    assert chat_readback(inputs, client, plan)["surfaces"]["live_tools"][
        "status"
    ] == "match"

    instance.tool_servers.append({
        "url": LIVE_URL, "type": "mcp", "auth_type": "none", "key": "",
        "config": {"enable": True, "function_name_filter_list": ""},
        "info": {"id": LIVE_SERVER_ID, "name": "hand-added"},
    })
    after = chat_readback(inputs, client, plan)
    assert after["surfaces"]["live_tools"]["status"] == "deviation"
    assert after["deviations"][0]["key"] == "live_connection:present"


def _live_verify_payloads(instance: FakeInstance) -> list[dict[str, Any]]:
    """Every verify request the run made AGAINST the live endpoint."""
    return [
        payload
        for method, path, payload in instance.request_log
        if method == "POST"
        and path == "/api/v1/configs/tool_servers/verify"
        and isinstance(payload, dict)
        and payload.get("url") == LIVE_URL
    ]


def test_a_slot_without_its_own_credential_carries_none_and_never_the_graph_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unauthenticated live connection is a LEGAL state, and it is the
    state the plan already declares: `live_connection:auth_type: "none"`.

    `chat.mcp.live_tools.auth_token_env` is optional and parses to `None`.
    `_resolve_connection_credential` read that `None` as "use the default"
    and fell back to `chat.mcp.auth_token_env`, so the live endpoint was
    registered — and probed — with the GRAPH connection's bearer token: a
    credential minted for a different system, for a different authority,
    handed to whatever process answers the live URL, while the plan for the
    same manifest said the connection carried none. Plan and apply
    disagreeing about one manifest is the failure the plan verb exists to
    prevent, and leaking the graph token off-system is worse than the
    disagreement.

    Asserted at the WIRE in both directions: what is stored, and what was
    sent.
    """
    instance = _prepared(monkeypatch, live_token=None)
    monkeypatch.delenv(LIVE_TOKEN_ENV, raising=False)
    manifest = _manifest(
        auth_token_env=None, grants={"analysts": list(LIVE_NAMES)},
    )
    inputs = _inputs(tmp_path, manifest)

    # The PLAN's declaration.
    plan = resolve_chat_plan(inputs)
    desired = plan["surfaces"]["live_tools"]["desired"]
    assert desired["live_connection:present"] is True
    assert desired["live_connection:auth_type"] == "none"
    # No live credential is referenced, because none is declared. The graph
    # one is still referenced, and is a DIFFERENT variable.
    live_refs = [
        ref for ref in plan["credential_references"]
        if ref.get("used_by") == "live_connection:key"
    ]
    assert live_refs == []

    report = _run(inputs, instance)

    # What is STORED. `auth_type` agrees with the plan, and no key at all --
    # not the graph token under another name.
    live = _entry(instance, LIVE_SERVER_ID)
    graph = _entry(instance, GRAPH_SERVER_ID)
    assert live is not None and graph is not None
    assert live["auth_type"] == "none"
    assert not live["key"]
    assert live["key"] != graph["key"]
    assert graph["auth_type"] == "bearer"
    assert graph["key"] == AUTH_TOKEN_VALUE
    assert AUTH_TOKEN_VALUE not in json.dumps(live, default=str)
    # Plan and sync agree about this one manifest, key for key.
    assert live["url"] == desired["live_connection:url"]
    assert (live["info"] or {})["id"] == desired["live_connection:info.id"]
    assert live["config"]["function_name_filter_list"] == desired[
        "live_connection:config.function_name_filter_list"
    ]
    assert report.mcp["live_connection_auth_type"] == "none"
    assert report.mcp["live_connection_verified"] is True

    # What was SENT. The apply-time verify dialled the live endpoint with no
    # bearer, and the graph token appears in no request to the live URL.
    sent = _live_verify_payloads(instance)
    assert sent, "the live connection was never verified"
    for payload in sent:
        assert payload["auth_type"] == "none"
        assert not payload.get("key")
        assert AUTH_TOKEN_VALUE not in json.dumps(payload, default=str)

    # The READBACK's own probe, which resolves the credential a second time
    # and is the second site that inherited the graph variable.
    before = len(instance.request_log)
    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    document = chat_readback(inputs, client, plan)
    assert document["surfaces"]["live_tools"]["status"] == "match"
    assert document["live_verify_probe"]["state"] == "healthy"
    assert document["live_verify_probe"]["auth_type"] == "none"
    probed = [
        payload
        for method, path, payload in instance.request_log[before:]
        if method == "POST"
        and path == "/api/v1/configs/tool_servers/verify"
        and isinstance(payload, dict)
        and payload.get("url") == LIVE_URL
    ]
    assert probed, "the readback ran no live verify probe"
    for payload in probed:
        assert payload["auth_type"] == "none"
        assert not payload.get("key")
        assert AUTH_TOKEN_VALUE not in json.dumps(payload, default=str)
    # The GRAPH probe in the same readback still carries its own bearer, so
    # this is a narrowed inheritance and not a credential switched off.
    graph_probe = document["verify_probe"]
    assert graph_probe["auth_type"] == "bearer"
    assert graph_probe["state"] == "healthy"


def test_the_sync_report_names_the_live_probe_and_holds_no_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_token = _live_token()
    instance = _prepared(monkeypatch, live_token=live_token)
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))

    report = _run(inputs, instance)

    assert report.mcp["live_connection_verified"] is True
    assert report.mcp["live_connection_auth_type"] == "bearer"
    rendered = json.dumps(
        {"mcp": copy.deepcopy(report.mcp), "notes": [
            note for outcome in report.surfaces.values() for note in outcome.notes
        ]},
        default=str,
    )
    assert live_token not in rendered
    assert AUTH_TOKEN_VALUE not in rendered
    assert LIVE_SERVER_ID in rendered


def test_a_live_endpoint_rejecting_the_credential_refuses_the_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live probe is its OWN measurement: the graph one verifies fine."""
    instance = _prepared(monkeypatch, live_token=_live_token())
    # The endpoint now expects a DIFFERENT credential from the one the
    # manifest's variable holds.
    instance.add_live_endpoint(LIVE_URL, token=_live_token(), specs=LIVE_NAMES)
    inputs = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))

    with pytest.raises(ChatSyncError) as excinfo:
        _run(inputs, instance)
    assert excinfo.value.data["reason"] == "mcp_connection_credential_rejected"
    assert excinfo.value.data["server_id"] == LIVE_SERVER_ID
    assert excinfo.value.data["instance_endpoint"] == LIVE_URL


def test_an_earlier_release_live_connection_migrates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3960 review F5: with a live slot, `okg-fixture-live` becomes
    `okg_fixture_live`, keeping its own URL and credential."""
    import dataclasses

    from okg.substrate.chat import live_tool_slot

    live_token = _live_token()
    instance = _prepared(monkeypatch, live_token=live_token)
    old = _inputs(tmp_path, _manifest(grants={"analysts": list(LIVE_NAMES)}))
    monkeypatch.setattr(live_tool_slot, "LIVE_CONNECTION_SUFFIX", "-live")
    assert _run(old, instance).ok
    assert _entry(instance, "okg-fixture-live") is not None

    monkeypatch.setattr(live_tool_slot, "LIVE_CONNECTION_SUFFIX", "_live")
    new = dataclasses.replace(old, mcp_server_id="okg_fixture")
    assert _run(new, instance).ok
    ids = sorted(str((e.get("info") or {}).get("id")) for e in instance.tool_servers)
    assert ids == ["okg_fixture", "okg_fixture_live"]
    live = _entry(instance, "okg_fixture_live")
    assert live["url"] == LIVE_URL and live["key"] == live_token
    again = _run(new, instance)
    assert again.ok and again.changes == 0


def test_readback_sees_an_old_form_live_connection_nobody_planned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3960 review F2: sync keeps a hand-added `okg-<d>-live` when no slot is
    declared (it may be foreign), so readback must still flag it."""
    import dataclasses

    instance = _prepared(monkeypatch, live_token=None)
    inputs = dataclasses.replace(
        _inputs(tmp_path, _manifest(slot=False)), mcp_server_id="okg_fixture",
    )
    plan = resolve_chat_plan(inputs)
    _run(inputs, instance)
    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    assert chat_readback(inputs, client, plan)["surfaces"]["live_tools"][
        "status"
    ] == "match"
    instance.tool_servers.append({
        "url": LIVE_URL, "type": "mcp", "auth_type": "none", "key": "",
        "config": {"enable": True, "function_name_filter_list": ""},
        "info": {"id": "okg-fixture-live", "name": "hand-added"},
    })
    after = chat_readback(inputs, client, plan)
    assert after["surfaces"]["live_tools"]["status"] == "deviation"
    assert after["deviations"][0]["key"] == "live_connection:present"
