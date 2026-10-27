"""The write connection: `record` and `propose`, and only for the admin group.

PACT ``chat-record-affordance``, req.write-tools-bound and
req.authenticated-transport-only.

Every listing assertion below is a READBACK: the ``function_name_filter_list``
is read back off the fake instance's own ``/api/v1/configs/tool_servers``
response and run through the vendor's own ``endswith`` predicate against the
advertised tool names. Nothing asserts on the payload the projection composed,
because a projection that computed the right filter and stored the wrong one
has to fail here.

The auth posture is likewise MEASURED, not configured: the suite monkeypatches
``source_admin._dial_tools_list`` -- the single raw round trip -- so the real
``observe_auth_posture`` decision procedure runs against a fake ENDPOINT that
either refuses an anonymous read or does not. A test that handed the projection
a posture string would prove nothing about where the posture comes from, which
is the whole content of req.authenticated-transport-only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.data import chat_data_dir
from okg.substrate.chat import source_admin
from okg.substrate.chat.source_admin import AUTH_OPEN, AUTH_REQUIRED, AUTH_UNKNOWN
from okg.substrate.chat.sync import (
    MCP_HTTP_AUTH_PRODUCER,
    WRITE_CONNECTION_SUFFIX,
    WRITE_PRESET_SUFFIX,
    WRITE_UNAUTHORIZED_NO_ADMIN_GROUP,
    WRITE_UNAUTHORIZED_READ_ONLY_ENDPOINT,
    ChatSyncError,
    InstanceClient,
    SyncInputs,
    bind_write_connection,
    effective_tool_binding,
    sync,
    write_authorization,
)
from okg.substrate.deployments.chat_block import parse_chat_block
from okg.substrate.mcp.operators import (
    FORBIDDEN_WRITE_ANSWER_CLAIMS,
    WRITE_ANSWER_CONTRACT_INSTRUCTION,
    WRITE_EFFECT_TOOLS,
)

from .fake_openwebui import FakeInstance

REPO_ROOT = Path(__file__).resolve().parents[3]

TOKEN = "fake-admin-token-value-0123456789"
BASE_URL = "http://instance.test"
SYSTEM_PROMPT = "Use the OKG tools. Quote node ids."

#: The env var the fixture manifest NAMES. The value below is a test literal,
#: not a credential: the manifest carries the name, never the secret.
AUTH_TOKEN_ENV = "OKG_FIXTURE_MCP_TOKEN"
AUTH_TOKEN_VALUE = "fixture-bearer-token-0123456789"

READ_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "search")

#: What a write-enabled ``okg mcp-serve`` advertises: the read algebra plus the
#: two write-effect tools.
WRITE_CAPABLE_TOOLS = (*READ_TOOLS, "query", *WRITE_EFFECT_TOOLS)

#: The same server, read-only. This is what okg-workspace serves today.
READ_ONLY_TOOLS = (*READ_TOOLS, "query")

#: A write-capable server that also advertises a name ENDING IN ``record``.
#: Not invented for convenience: ``run_sql`` became ``query`` under
#: ``shrink-mcp-to-operator-surface``, so a later rename that lands a
#: ``prerecord`` or ``batch_record`` is the ordinary case.
COLLIDING_TOOLS = (*WRITE_CAPABLE_TOOLS, "prerecord")

ADMIN_GROUP = "okg-admins"
USER_GROUP = "okg-users"


def _sso_block() -> dict[str, Any]:
    """A COMPLETE multi-user identity declaration.

    Multi-user refuses on any missing element (chat_block ``_parse_sso``
    refusal 2), so the fixture declares all ten. ``admin_group`` is the one
    this suite is about: design.md section 5 mechanism 4 distinguishes admins
    by GROUP, and it is the group the write connection is granted to.
    """
    return {
        "mode": "multi_user",
        "provider_id": "fixture-idp",
        "issuer": "https://idp.test/",
        "audience": "okg-fixture",
        "discovery_endpoint": "https://idp.test/.well-known/openid-configuration",
        "client_id": "okg-fixture-client",
        "client_secret_env": "OKG_FIXTURE_OIDC_SECRET",
        "principal_header": "X-OKG-Principal",
        "groups_claim": "groups",
        "admin_group": ADMIN_GROUP,
        "user_group": USER_GROUP,
    }


def _raw_manifest(*, sso: bool, auth_token_env: str | None = AUTH_TOKEN_ENV) -> dict[str, Any]:
    mcp: dict[str, Any] = {"port": 8100, "tools": list(READ_TOOLS)}
    if auth_token_env is not None:
        mcp["auth_token_env"] = auth_token_env
    manifest: dict[str, Any] = {
        "name": "fixture",
        # UNMASKED, so `query` is not posture-excluded and the read
        # connection's own set stays stable across these arms.
        "nomos": {"runtime_enforcement": "audit"},
        "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        "chat": {
            "enabled": True,
            "ui": {
                "site_name": "Fixture Site",
                "banner": "Answers are pinned to one published generation.",
                "features": {
                    "web_search": False,
                    "image_generation": False,
                    "code_interpreter": False,
                    "arena": False,
                    "channels": False,
                    "byok": False,
                    "context_compaction": True,
                },
            },
            "models": {"builtin": ["base/model-a"], "default": "base/model-a"},
            "preset": {
                "display_name": "Fixture Preset",
                "description": "Ask the fixture graph.",
                "system_prompt_ref": "chat/system_prompt.md",
                "suggestions": ["What changed last week?"],
            },
            "search_profile": "analyst",
            "mcp": mcp,
            "access": {"groups": [USER_GROUP]},
            "deep_links": {
                "operator_ui_base": "http://console.test",
                "chat_site_base": "http://chat.test",
            },
        },
    }
    if sso:
        manifest["chat"]["sso"] = _sso_block()
    return manifest


def _unmasked_catalog() -> Any:
    class _Catalog:
        subtype_metadata: dict[str, dict[str, Any]] = {"document": {"other": 1}}

    return _Catalog()


def _make_inputs(tmp_path: Path, *, manifest: dict[str, Any]) -> SyncInputs:
    dep_dir = tmp_path / "deployment"
    (dep_dir / "chat").mkdir(parents=True, exist_ok=True)
    (dep_dir / "chat" / "system_prompt.md").write_text(SYSTEM_PROMPT, encoding="utf-8")
    chat = parse_chat_block(manifest, manifest_path=dep_dir / "deployment.yaml")
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep_dir,
        raw_manifest=manifest,
        chat=chat,
        mcp_instructions_path=None,
        chat_data_dir=chat_data_dir(),
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=tmp_path / "state",
        catalog_loader=_unmasked_catalog,
        agent_tools={},
    )


def _instance() -> FakeInstance:
    instance = FakeInstance(webui_name="Fixture Site")
    instance.add_group(USER_GROUP, "group-users")
    instance.add_group(ADMIN_GROUP, "group-admins")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    return instance


def _pin_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    *,
    advertised: tuple[str, ...],
    enforces_auth: bool,
) -> None:
    """Make the fake ENDPOINT decide the auth posture, not the configuration.

    Replaces the single raw round trip inside ``source_admin`` so the real
    ``observe_auth_posture`` runs: it dials anonymously, then with the bearer
    header, and decides from what the endpoint DID. An endpoint that answers an
    anonymous ``tools/list`` is open no matter what our manifest claims.
    """

    def _dial(
        endpoint: str, headers: Any, timeout_seconds: float,
    ) -> tuple[str, ...]:
        credentialed = bool(dict(headers or {}).get("Authorization"))
        if enforces_auth and not credentialed:
            raise RuntimeError("HTTP 401 Unauthorized")
        return tuple(advertised)

    monkeypatch.setattr(source_admin, "_dial_tools_list", _dial)
    monkeypatch.setenv(AUTH_TOKEN_ENV, AUTH_TOKEN_VALUE)


def _run(
    inputs: SyncInputs,
    instance: FakeInstance,
    *,
    advertised: tuple[str, ...],
) -> Any:
    """One full ``okg chat sync``, with the REAL auth observer."""
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=lambda _endpoint, _env: advertised,
    )


def _entry(instance: FakeInstance, server_id: str) -> dict[str, Any] | None:
    for entry in instance.tool_servers:
        if (entry.get("info") or {}).get("id") == server_id:
            return entry
    return None


def _listing(
    instance: FakeInstance, server_id: str, *, advertised: tuple[str, ...],
) -> tuple[str, ...]:
    """The tools/list READBACK for one connection, as a chat client sees it.

    Refuses to answer for a connection that is not registered: zero tools read
    off an ABSENT connection is the absence of a measurement, and letting it
    return ``()`` would make every "contains neither" assertion below pass on a
    broken projection.
    """
    entry = _entry(instance, server_id)
    if entry is None:
        raise AssertionError(
            f"no connection {server_id!r} is registered; an empty listing read "
            "off a missing connection would prove nothing"
        )
    stored = (entry.get("config") or {}).get("function_name_filter_list")
    assert isinstance(stored, str), stored
    return effective_tool_binding(advertised, stored.split(","))


WRITE_SERVER_ID = f"okg-fixture{WRITE_CONNECTION_SUFFIX}"


# ---------------------------------------------------------------------------
# req.write-tools-bound -- the three listing arms, one suite
# ---------------------------------------------------------------------------

def test_write_connection_listing_contains_record_and_propose(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()

    listing = _listing(instance, WRITE_SERVER_ID, advertised=WRITE_CAPABLE_TOOLS)
    assert set(listing) == set(WRITE_EFFECT_TOOLS), listing
    # ... and nothing else. A write connection that also served the read
    # algebra would satisfy a membership test while widening the surface.
    assert not set(listing) & set(READ_TOOLS)

    # The preset that binds it is granted to the ADMIN group and to no other,
    # read back off the instance's own model row.
    row = instance.model_row(f"okg-fixture{WRITE_PRESET_SUFFIX}")
    assert row["meta"]["toolIds"] == [f"server:mcp:{WRITE_SERVER_ID}"]
    granted = {g["principal_id"] for g in row["access_grants"]}
    assert granted == {"group-admins"}


def test_write_preset_carries_the_answer_contract_and_the_read_preset_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The instruction that makes the answer reach a user rides the write preset.

    Open WebUI never shows a model an MCP server's own ``instructions``, so
    ``params.system`` on the preset is the only channel that tells the model
    what to do with a held write's ``answer``. Read back off the instance's
    model row rather than off the text we composed.

    The read preset is the paired negative: it carries the same base prompt
    WITHOUT the write contract, so "the write preset carries it" is a
    difference between the two presets and not a sentence we append to
    everything.
    """
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()

    write_system = instance.model_row(f"okg-fixture{WRITE_PRESET_SUFFIX}")[
        "params"
    ]["system"]
    read_system = instance.model_row("okg-fixture")["params"]["system"]
    assert WRITE_ANSWER_CONTRACT_INSTRUCTION in write_system
    assert WRITE_ANSWER_CONTRACT_INSTRUCTION not in read_system
    # Both still carry the deployment's own prompt, so the difference is the
    # contract and not one preset having been left unconfigured.
    assert SYSTEM_PROMPT in write_system
    assert SYSTEM_PROMPT in read_system
    # The four claims the contract forbids are named in it, so an edit that
    # dropped the prohibition would be caught here and not only in the
    # renderer's own suite.
    for claim in FORBIDDEN_WRITE_ANSWER_CLAIMS:
        assert claim in write_system.lower(), claim


def test_read_connection_listing_contains_neither_and_a_non_empty_read_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )

    listing = _listing(instance, "okg-fixture", advertised=WRITE_CAPABLE_TOOLS)
    assert not set(listing) & set(WRITE_EFFECT_TOOLS), listing
    # The non-empty half of the same assertion: an all-empty read listing would
    # separate the surfaces perfectly and serve nobody anything.
    assert set(listing) == set(READ_TOOLS), listing

    # And the general preset binds only the read server.
    row = instance.model_row("okg-fixture")
    assert row["meta"]["toolIds"] == ["server:mcp:okg-fixture"]
    granted = {g["principal_id"] for g in row["access_grants"]}
    assert granted == {"group-users"}


def test_deployment_without_write_authorization_lists_no_write_tools_anywhere(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No admin group declared: no write connection, on a write-CAPABLE server.

    The endpoint advertises ``record`` and ``propose`` in this arm, so the
    absence is attributable to the authorization rather than to a server that
    had nothing to offer.
    """
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=False)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()

    assert _entry(instance, WRITE_SERVER_ID) is None
    for entry in instance.tool_servers:
        server_id = (entry.get("info") or {}).get("id")
        listing = _listing(instance, server_id, advertised=WRITE_CAPABLE_TOOLS)
        assert not set(listing) & set(WRITE_EFFECT_TOOLS), (server_id, listing)
    # The read operators are still served, so "no write tools" is not a broken
    # projection that served nothing.
    assert set(_listing(instance, "okg-fixture", advertised=WRITE_CAPABLE_TOOLS)) == set(
        READ_TOOLS
    )
    payload = json.loads(json.dumps(report.to_dict()))
    assert payload["write_connection"] is None
    assert payload["write_authorization"]["authorized"] is False
    assert (
        payload["write_authorization"]["reason"] == WRITE_UNAUTHORIZED_NO_ADMIN_GROUP
    )
    # The reason is not "the server had none": it advertised both.
    assert payload["write_authorization"]["advertised_write_tools"] == list(
        WRITE_EFFECT_TOOLS
    )


def test_a_read_only_endpoint_is_recorded_as_the_other_unauthorized_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control for the arm above: same admin group, read-only endpoint.

    Both halves of the authorization are required, and each absence is
    reported by its own name -- otherwise one reason would stand in for the
    other and neither would be attributable.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=READ_ONLY_TOOLS,
    )
    assert report.ok, report.to_dict()
    assert _entry(instance, WRITE_SERVER_ID) is None
    payload = report.to_dict()
    assert payload["write_authorization"]["reason"] == (
        WRITE_UNAUTHORIZED_READ_ONLY_ENDPOINT
    )
    assert payload["write_authorization"]["admin_group"] == ADMIN_GROUP


def test_empty_computed_write_set_refuses_and_emits_no_filter_list(
    tmp_path: Path,
) -> None:
    """An empty write set REFUSES; it never becomes an empty filter list.

    Open WebUI reads an empty ``function_name_filter_list`` as ALLOW ALL
    (``utils/misc.py:63-64``), so the natural encoding of "no write tools"
    would hand the write preset every tool the server advertises -- including
    the nine source-admin ones. Driven at the binder, which is where an
    emptied ``WRITE_EFFECT_TOOLS``, a future policy filter or an exclusion
    would arrive.
    """
    with pytest.raises(ChatSyncError) as excinfo:
        bind_write_connection(
            deployment="fixture",
            admin_group=ADMIN_GROUP,
            advertised=WRITE_CAPABLE_TOOLS,
            auth_posture=AUTH_REQUIRED,
            endpoint="http://127.0.0.1:8100/mcp",
            mcp_server_id="okg-fixture",
            preset_id="okg-fixture",
            write_tools=(),
        )
    assert excinfo.value.data["reason"] == "empty_write_tool_set"
    assert excinfo.value.data["filter_list_emitted"] is False
    assert "filter_list" not in excinfo.value.data
    # The trap this arm neutralises is real, asserted rather than described:
    # the empty list the refusal refuses to emit would admit everything.
    assert effective_tool_binding(WRITE_CAPABLE_TOOLS, []) == tuple(
        sorted(WRITE_CAPABLE_TOOLS)
    )


def test_suffix_colliding_filter_entry_is_rejected(tmp_path: Path) -> None:
    """``record`` as a filter entry also admits ``prerecord`` -- refused."""
    with pytest.raises(ChatSyncError) as excinfo:
        bind_write_connection(
            deployment="fixture",
            admin_group=ADMIN_GROUP,
            advertised=COLLIDING_TOOLS,
            auth_posture=AUTH_REQUIRED,
            endpoint="http://127.0.0.1:8100/mcp",
            mcp_server_id="okg-fixture",
            preset_id="okg-fixture",
        )
    assert excinfo.value.data["reason"] == "write_filter_admits_a_different_set"
    assert excinfo.value.data["extra"] == ["prerecord"]


def test_non_colliding_filter_entry_is_accepted(tmp_path: Path) -> None:
    """The positive control for the refusal above, same inputs minus the name."""
    connection = bind_write_connection(
        deployment="fixture",
        admin_group=ADMIN_GROUP,
        advertised=WRITE_CAPABLE_TOOLS,
        auth_posture=AUTH_REQUIRED,
        endpoint="http://127.0.0.1:8100/mcp",
        mcp_server_id="okg-fixture",
        preset_id="okg-fixture",
    )
    assert connection.admitted == tuple(sorted(WRITE_EFFECT_TOOLS))
    assert connection.filter_list == tuple(WRITE_EFFECT_TOOLS)
    assert connection.server_id == WRITE_SERVER_ID


# ---------------------------------------------------------------------------
# req.authenticated-transport-only
# ---------------------------------------------------------------------------

def test_unauthenticated_endpoint_refuses_and_names_mcp_http_auth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An endpoint that answers anonymously gets no write connection at all."""
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=False)
    instance = _instance()
    with pytest.raises(ChatSyncError) as excinfo:
        _run(
            _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
            instance,
            advertised=WRITE_CAPABLE_TOOLS,
        )
    assert excinfo.value.data["reason"] == "write_transport_unauthenticated"
    assert excinfo.value.data["missing_producer"] == MCP_HTTP_AUTH_PRODUCER
    assert excinfo.value.data["auth_posture"] == AUTH_OPEN
    assert MCP_HTTP_AUTH_PRODUCER in str(excinfo.value)
    # Nothing was registered: the refusal happens before any surface is
    # written, so no listing anywhere carries a write tool.
    assert _entry(instance, WRITE_SERVER_ID) is None
    for entry in instance.tool_servers:
        stored = (entry.get("config") or {}).get("function_name_filter_list") or ""
        admitted = effective_tool_binding(WRITE_CAPABLE_TOOLS, stored.split(","))
        assert not set(admitted) & set(WRITE_EFFECT_TOOLS)


def test_configuration_claiming_auth_against_a_non_enforcing_endpoint_still_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The manifest names a credential; the endpoint does not enforce it.

    This is the arm that decides whether the auth input is an OBSERVATION or a
    configuration read. ``chat.mcp.auth_token_env`` is declared and the
    variable is set -- so any check keyed on "are we configured for auth" would
    pass -- while the endpoint happily answers an anonymous ``tools/list``.
    """
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=False)
    manifest = _raw_manifest(sso=True, auth_token_env=AUTH_TOKEN_ENV)
    assert manifest["chat"]["mcp"]["auth_token_env"] == AUTH_TOKEN_ENV
    import os

    assert os.environ[AUTH_TOKEN_ENV] == AUTH_TOKEN_VALUE

    instance = _instance()
    with pytest.raises(ChatSyncError) as excinfo:
        _run(
            _make_inputs(tmp_path, manifest=manifest),
            instance,
            advertised=WRITE_CAPABLE_TOOLS,
        )
    assert excinfo.value.data["reason"] == "write_transport_unauthenticated"
    assert excinfo.value.data["auth_posture"] == AUTH_OPEN
    assert _entry(instance, WRITE_SERVER_ID) is None


def test_authenticated_endpoint_registers_the_write_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The positive control: same deployment, endpoint that refuses anonymity."""
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    report = _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )
    assert report.ok, report.to_dict()
    assert report.write_connection is not None
    assert report.write_connection.auth_posture == AUTH_REQUIRED
    assert set(
        _listing(instance, WRITE_SERVER_ID, advertised=WRITE_CAPABLE_TOOLS)
    ) == set(WRITE_EFFECT_TOOLS)


def test_an_unreachable_endpoint_is_not_read_as_authenticated(
    tmp_path: Path,
) -> None:
    """AUTH_UNKNOWN is not AUTH_REQUIRED.

    ``observe_auth_posture`` returns UNKNOWN when both dials fail, which is
    what an unreachable endpoint looks like. Reading a one-sided "the
    anonymous read failed" as proof of authentication is how an unreachable
    endpoint acquires a write surface.
    """
    with pytest.raises(ChatSyncError) as excinfo:
        bind_write_connection(
            deployment="fixture",
            admin_group=ADMIN_GROUP,
            advertised=WRITE_CAPABLE_TOOLS,
            auth_posture=AUTH_UNKNOWN,
            endpoint="http://127.0.0.1:8100/mcp",
            mcp_server_id="okg-fixture",
            preset_id="okg-fixture",
        )
    assert excinfo.value.data["reason"] == "write_transport_unauthenticated"


# ---------------------------------------------------------------------------
# The authorization predicate itself
# ---------------------------------------------------------------------------

def test_authorization_requires_both_halves(tmp_path: Path) -> None:
    """Neither half alone authorizes, and both together do."""
    with_sso = parse_chat_block(
        _raw_manifest(sso=True), manifest_path=tmp_path / "deployment.yaml",
    )
    without_sso = parse_chat_block(
        _raw_manifest(sso=False), manifest_path=tmp_path / "deployment.yaml",
    )
    assert (
        write_authorization(without_sso, advertised=WRITE_CAPABLE_TOOLS).reason
        == WRITE_UNAUTHORIZED_NO_ADMIN_GROUP
    )
    assert (
        write_authorization(with_sso, advertised=READ_ONLY_TOOLS).reason
        == WRITE_UNAUTHORIZED_READ_ONLY_ENDPOINT
    )
    both = write_authorization(with_sso, advertised=WRITE_CAPABLE_TOOLS)
    assert both.authorized is True
    assert both.admin_group == ADMIN_GROUP


def test_withdrawing_the_authorization_withdraws_the_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second run with no admin group removes the connection the first made.

    A withdrawal that leaves the registration in place would leave the write
    tools reachable by everyone the old grant covered, while the run reported a
    deployment that authorizes no writes.
    """
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=True)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )
    assert _entry(instance, WRITE_SERVER_ID) is not None

    _run(
        _make_inputs(tmp_path, manifest=_raw_manifest(sso=False)),
        instance,
        advertised=WRITE_CAPABLE_TOOLS,
    )
    assert _entry(instance, WRITE_SERVER_ID) is None
    assert set(
        _listing(instance, "okg-fixture", advertised=WRITE_CAPABLE_TOOLS)
    ) == set(READ_TOOLS)


def test_an_earlier_release_write_connection_migrates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3960 review F5: `okg-fixture-write` becomes `okg_fixture_write`.

    The earlier release is the current code with its old suffix; the write
    preset id (`okg-fixture-write`) does not move, its toolIds do.
    """
    import dataclasses

    from okg.substrate.chat import sync as sync_module

    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    old = _make_inputs(tmp_path, manifest=_raw_manifest(sso=True))
    monkeypatch.setattr(sync_module, "WRITE_CONNECTION_SUFFIX", "-write")
    assert _run(old, instance, advertised=WRITE_CAPABLE_TOOLS).ok
    ids = sorted(str((e.get("info") or {}).get("id")) for e in instance.tool_servers)
    assert ids == ["okg-fixture", "okg-fixture-write"]
    instance.models["hand-write"] = {
        "id": "hand-write", "name": "x", "base_model_id": "base/model-a",
        "meta": {"toolIds": ["server:mcp:okg-fixture-write"]}, "params": {},
    }

    monkeypatch.setattr(sync_module, "WRITE_CONNECTION_SUFFIX", "_write")
    new = dataclasses.replace(old, mcp_server_id="okg_fixture")
    migrated = _run(new, instance, advertised=WRITE_CAPABLE_TOOLS)
    assert migrated.ok, migrated.to_dict()
    ids = sorted(str((e.get("info") or {}).get("id")) for e in instance.tool_servers)
    assert ids == ["okg_fixture", "okg_fixture_write"]
    assert instance.model_row(f"okg-fixture{WRITE_PRESET_SUFFIX}")["meta"]["toolIds"] == [
        "server:mcp:okg_fixture_write"
    ]
    notes = " ".join(n for s in migrated.surfaces.values() for n in s.notes)
    assert "['hand-write'] still name ['server:mcp:okg-fixture-write']" in notes
    again = _run(new, instance, advertised=WRITE_CAPABLE_TOOLS)
    assert again.ok and again.changes == 0
