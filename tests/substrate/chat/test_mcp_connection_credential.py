"""The MCP tool-server connection carries a credential (issue #1738).

PACT ``chat-mcp-connection-credential``.

Every behavior asserted here was MEASURED against
``ghcr.io/open-webui/open-webui:v0.11.0`` (digest ``72c0ba64…``) in the spike
recorded on issue #1738:

* an entry registered with ``auth_type: "none"`` against an
  ``okg mcp-serve --auth-token-env`` makes the instance dial credential-less;
  ``mcp-serve`` answers ``401`` and the vendor's verify answers ``400``;
* the same entry with ``auth_type: "bearer"`` and ``key`` set verifies ``200``
  with the seven read operators;
* ``auth_type: "session"`` forwards Open WebUI's own user JWT and is rejected;
* ``GET /api/v1/configs/tool_servers`` returns ``key`` and any custom
  ``headers`` IN CLEAR.

The fixture instance reproduces exactly that, so a projection that stops
sending the credential fails here rather than on a live site. Assertions are
on the WIRE — the entry the fake instance holds — not on internal variables.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.sync import (
    CONNECTION_KEY_ABSENT,
    CONNECTION_KEY_MASKED,
    SURFACE_PRESET,
    ChatSyncError,
    InstanceClient,
    connection_key_digest,
    sync,
)

from .fake_openwebui import FakeInstance
from .test_write_connection_projection import (
    AUTH_TOKEN_ENV,
    AUTH_TOKEN_VALUE,
    BASE_URL,
    READ_ONLY_TOOLS,
    TOKEN,
    WRITE_CAPABLE_TOOLS,
    WRITE_SERVER_ID,
    _instance,
    _make_inputs,
    _pin_endpoint,
    _raw_manifest,
)

READ_SERVER_ID = "okg-fixture"

#: A credential that is NOT the one the deployment declares. Assembled at
#: runtime so no line of this file looks like a checked-in secret.
OTHER_TOKEN = "-".join(("fixture", "wrong", "credential", "0123456789"))


def _run(
    inputs: Any, instance: FakeInstance, *, advertised: tuple[str, ...],
) -> Any:
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=lambda _endpoint, _env: advertised,
    )


def _entry(instance: FakeInstance, server_id: str) -> dict[str, Any]:
    for entry in instance.tool_servers:
        if (entry.get("info") or {}).get("id") == server_id:
            return entry
    raise AssertionError(f"no connection {server_id!r} is registered")


# ---------------------------------------------------------------------------
# req.bearer-on-both-connections
# ---------------------------------------------------------------------------

def test_both_connections_register_the_bearer_from_the_declared_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read AND write. The read connection is unconditional and needs it too."""
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=True))

    _run(inputs, instance, advertised=WRITE_CAPABLE_TOOLS)

    for server_id in (READ_SERVER_ID, WRITE_SERVER_ID):
        entry = _entry(instance, server_id)
        assert entry["auth_type"] == "bearer", server_id
        assert entry["key"] == AUTH_TOKEN_VALUE, server_id
        # `headers` stays unset: the vendor reads `key` directly, while
        # `headers` goes through the per-user template interpolation.
        assert entry["headers"] is None, server_id


def test_a_read_only_deployment_registers_the_credential_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    report = _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    assert report.write_connection is None
    assert _entry(instance, READ_SERVER_ID)["auth_type"] == "bearer"


def test_a_declared_but_unset_variable_refuses_and_registers_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No silent fallback to ``none``: that is the defect, re-created quietly.

    Run WITHOUT a stubbed probe, on purpose. The MCP probe resolves the same
    variable inside ``read_live_tools`` and turns every failure into
    ``mcp_unreachable``, so with a stub in place this refusal could be
    unreachable in the real ``sync`` flow and the test would still pass. The
    credential is resolved before the probe, so the refusal here is the one
    an operator actually gets — and no dial is attempted to prove it.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=False)
    monkeypatch.delenv(AUTH_TOKEN_ENV, raising=False)
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    with pytest.raises(ChatSyncError) as caught:
        sync(inputs, InstanceClient(BASE_URL, TOKEN, opener=instance.opener))

    assert caught.value.data["reason"] == "mcp_connection_credential_unresolved"
    assert caught.value.data["auth_token_env"] == AUTH_TOKEN_ENV
    assert instance.tool_servers == []
    # Before the probe, not after it. The refusal names the variable rather
    # than an endpoint that did not answer, and nothing was written on any
    # surface — the run never got past reading the instance version.
    assert [m for m, _ in instance.requests if m != "GET"] == []


def test_an_unauthenticated_endpoint_keeps_auth_type_none_explicitly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stated posture, not an omission — and the entry still verifies."""
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=False)
    instance = _instance()
    instance.mcp_auth_token = None
    manifest = _raw_manifest(sso=False, auth_token_env=None)
    inputs = _make_inputs(tmp_path, manifest=manifest)

    report = _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    entry = _entry(instance, READ_SERVER_ID)
    assert entry["auth_type"] == "none"
    assert entry["key"] == ""
    assert report.mcp["registered_connection_auth_type"] == "none"


# ---------------------------------------------------------------------------
# req.credential-never-rendered
# ---------------------------------------------------------------------------

def test_the_readback_is_digested_before_any_caller_sees_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The redaction is at the boundary, so no call site can forget it."""
    instance = _instance()
    instance.tool_servers = [{
        "url": "http://host.docker.internal:8100/mcp",
        "type": "mcp",
        "auth_type": "bearer",
        "key": AUTH_TOKEN_VALUE,
        "headers": {"Authorization": f"Bearer {AUTH_TOKEN_VALUE}"},
        "info": {"id": READ_SERVER_ID},
    }]

    read_back = InstanceClient(BASE_URL, TOKEN, opener=instance.opener).tool_servers()

    assert read_back[0]["key"] == connection_key_digest(AUTH_TOKEN_VALUE)
    # The header carries `Bearer <token>`, so it is digested WHOLE — the
    # point is that no substring of the secret survives, not that the two
    # carriers produce the same digest.
    assert read_back[0]["headers"]["Authorization"] == connection_key_digest(
        f"Bearer {AUTH_TOKEN_VALUE}",
    )
    assert AUTH_TOKEN_VALUE not in json.dumps(read_back)
    # The fake still holds the clear value, so this is a real redaction and
    # not a fixture that never had a secret in it.
    assert instance.tool_servers[0]["key"] == AUTH_TOKEN_VALUE
    assert connection_key_digest(None) == CONNECTION_KEY_ABSENT


def test_the_token_reaches_only_the_two_routes_that_must_carry_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A grep of the WHOLE serialized report, and of every request BODY.

    Not a log grep: this module has no logger, so asserting over captured
    logging records would pass on a build that printed the token to stdout.
    The two surfaces that can carry it are the serialized report an operator
    reads and the request bodies this run puts on the wire, and both are
    read here.
    """
    _pin_endpoint(monkeypatch, advertised=WRITE_CAPABLE_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=True))

    report = _run(inputs, instance, advertised=WRITE_CAPABLE_TOOLS)

    rendered = json.dumps(report.to_dict(), sort_keys=True, default=str)
    assert AUTH_TOKEN_VALUE not in rendered
    # Not a vacuous grep: the run DID measure the credential. Both
    # connections read back as bearer, and the instance holds the value the
    # report refuses to print.
    assert report.mcp["registered_connection_auth_type"] == "bearer"
    assert _entry(instance, READ_SERVER_ID)["key"] == AUTH_TOKEN_VALUE
    assert AUTH_TOKEN_VALUE in json.dumps(instance.tool_servers)
    # And on the wire: the registration and the verify dial carry it, and
    # nothing else does. An extra route carrying the credential is a leak
    # even when every rendered field is clean.
    carried = {
        (method, path)
        for method, path, body in instance.request_log
        if AUTH_TOKEN_VALUE in json.dumps(body, default=str)
    }
    assert carried == {
        ("POST", "/api/v1/configs/tool_servers"),
        ("POST", "/api/v1/configs/tool_servers/verify"),
    }


def test_a_drifted_credential_is_reported_as_a_digest_not_a_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An out-of-band edit must not copy either secret into the deviation."""
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))
    _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    # Someone edits the stored key in the instance, and the projection is
    # then asked only to READ it back.
    _entry(instance, READ_SERVER_ID)["key"] = OTHER_TOKEN
    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    observed = next(
        e for e in client.tool_servers()
        if (e.get("info") or {}).get("id") == READ_SERVER_ID
    )

    assert observed["key"] == connection_key_digest(OTHER_TOKEN)
    assert observed["key"] != connection_key_digest(AUTH_TOKEN_VALUE)
    assert OTHER_TOKEN not in json.dumps(observed)


def test_a_foreign_tool_server_survives_the_rewrite_byte_identical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The redaction must not reach the round trip that REWRITES the list.

    ``POST /api/v1/configs/tool_servers`` replaces the WHOLE list, so this
    run writes back every entry it does not own. Reading that list through
    the redacted view would store the literal digest string as some other
    tool server's credential and destroy it — irreversibly, since the clear
    value existed only on the instance.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    foreign = {
        "url": "http://elsewhere.test/mcp",
        "path": "",
        "type": "mcp",
        "auth_type": "bearer",
        # Somebody else's credential. This run has no business knowing it,
        # and every business preserving it.
        "key": OTHER_TOKEN,
        "headers": {"X-Tenant": "other", "Authorization": f"Bearer {OTHER_TOKEN}"},
        "config": {"enable": True},
        "info": {"id": "someone-elses-server"},
    }
    instance.tool_servers = [copy.deepcopy(foreign)]
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    report = _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    # The run DID rewrite the list — otherwise this proves nothing.
    assert report.surfaces[SURFACE_PRESET].changes > 0
    assert _entry(instance, READ_SERVER_ID)["key"] == AUTH_TOKEN_VALUE
    assert _entry(instance, "someone-elses-server") == foreign


def test_a_masking_version_asserts_presence_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A version that masks ``key`` must converge, not refuse forever.

    v0.11.0 returns the key in clear (measured), but this module's own
    comparison contract has always warned that a server may mask it. A
    digest assertion against a mask can never converge: the run would rewrite
    the connection on every pass and then refuse, on a site whose credential
    is perfectly fine.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))
    _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    instance.mask_tool_server_key = True
    report = _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    preset = report.surfaces[SURFACE_PRESET]
    assert preset.ok is True
    # No churn: a comparison that cannot converge must not re-register the
    # connection on every run forever.
    assert preset.changes == 0
    assert report.mcp["connection_key_digest_compared"] is False
    assert any("MASKS the tool-server key" in note for note in preset.notes)
    # And the mask itself never becomes a digest of the mask.
    masked = next(
        e for e in InstanceClient(
            BASE_URL, TOKEN, opener=instance.opener,
        ).tool_servers()
        if (e.get("info") or {}).get("id") == READ_SERVER_ID
    )
    assert masked["key"] == CONNECTION_KEY_MASKED


# ---------------------------------------------------------------------------
# req.instance-side-probe
# ---------------------------------------------------------------------------

def test_the_instance_verifies_the_registered_connection_on_the_read_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface 6's credential half runs even with no write connection."""
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    report = _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    assert report.mcp["registered_connection_verified"] is True
    assert report.mcp["registered_connection_detail"] == len(instance.mcp_specs)
    assert ("POST", "/api/v1/configs/tool_servers/verify") in instance.requests


def test_a_dead_credential_fails_the_run_through_the_instances_own_verify(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The endpoint answers THIS process; only the instance's dial catches it.

    This is the arm the old surface-6 probe could not fail: it dialled with
    the CLI's own credential, which works, while the connection the instance
    stored was rejected.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    # The endpoint the INSTANCE reaches wants a different credential than the
    # one this deployment declares.
    instance.mcp_auth_token = OTHER_TOKEN
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    with pytest.raises(ChatSyncError) as caught:
        _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    assert caught.value.data["reason"] == "mcp_connection_credential_rejected"
    assert caught.value.data["detail"] == "HTTP 400"
    assert AUTH_TOKEN_VALUE not in str(caught.value)
    assert OTHER_TOKEN not in str(caught.value)


def test_a_version_without_the_verify_route_records_an_absent_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 404 measures nothing about the credential, so it refuses nothing.

    Reporting an absent route as a dead credential sends an operator to
    rotate a token that was never the problem.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    instance.verify_route_present = False
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    report = _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    assert report.surfaces[SURFACE_PRESET].ok is True
    assert report.mcp["registered_connection_probe"] == "unavailable"
    assert report.mcp["registered_connection_verified"] is None
    assert any(
        "UNAVAILABLE" in note
        for note in report.surfaces[SURFACE_PRESET].notes
    )
    # The registration itself still happened and still carries the token.
    assert _entry(instance, READ_SERVER_ID)["auth_type"] == "bearer"


def test_the_verify_dials_the_desired_connection_not_the_readback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Built from the DESIRED payload: clear key, ``headers`` unset.

    Built from the readback instead, the entry would present the instance a
    digest for anything the override missed — a stored
    ``headers.Authorization`` survives as ``hmac:…`` and gets dialled as one,
    which is a credential this deployment never chose.
    """
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))

    _run(inputs, instance, advertised=READ_ONLY_TOOLS)

    bodies = [
        body for method, path, body in instance.request_log
        if (method, path) == ("POST", "/api/v1/configs/tool_servers/verify")
    ]
    assert len(bodies) == 1
    assert bodies[0]["key"] == AUTH_TOKEN_VALUE
    assert bodies[0]["auth_type"] == "bearer"
    assert bodies[0]["headers"] is None
    assert connection_key_digest(AUTH_TOKEN_VALUE) not in json.dumps(bodies[0])


def test_the_fake_rejects_the_payload_this_slice_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The measured contract itself: ``none`` and ``session`` are rejected.

    Without this the fake could accept anything and every arm above would
    pass on a projection that sends no credential at all.
    """
    instance = _instance()
    instance.mcp_auth_token = AUTH_TOKEN_VALUE
    base = {"type": "mcp", "info": {"id": READ_SERVER_ID}}

    assert instance._verify_tool_server(
        {**base, "auth_type": "none", "key": "", "headers": None},
    )[0] == 400
    assert instance._verify_tool_server(
        {**base, "auth_type": "session", "key": ""},
    )[0] == 400
    assert instance._verify_tool_server(
        {**base, "auth_type": "bearer", "key": OTHER_TOKEN},
    )[0] == 400
    status, body = instance._verify_tool_server(
        {**base, "auth_type": "bearer", "key": AUTH_TOKEN_VALUE},
    )
    assert status == 200
    assert body["status"] is True
    assert len(body["specs"]) == 7
