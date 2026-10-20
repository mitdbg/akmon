"""No admin preset over an unauthenticated network transport.

``task.chat-source-admin.transport-refusal``. The server already refuses
a write-enabled MCP surface bound to a non-loopback host and only WARNS
on loopback, because the endpoint carries no authentication
(``_write_transport_guard``). The chat projection has to refuse in the
same cell, and — this is the load-bearing half — it must refuse rather
than project a DEGRADED admin preset that quietly omits the write tools
while still presenting itself as the admin surface. A quiet downgrade is
a silent fallback and a P0 defect.

Every assertion below is re-run against a PRE-FIX projector (one with no
transport check, which is what this module's absence would leave) and
must FAIL there. An assertion that passes against both paths is testing
nothing.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Any, Callable

import pytest

from okg.substrate.chat.source_admin import (
    AUTH_OPEN,
    AUTH_REQUIRED,
    AUTH_UNKNOWN,
    ChatSourceAdminError,
    LiveToolSurface,
    admin_transport_precondition,
    project_admin_preset,
    require_admin_transport,
)
from okg.substrate.mcp.operators import AKMON_READ_TOOLS, SOURCE_ADMIN_TOOLS


NETWORK_ENDPOINT = "http://10.1.2.3:8101/mcp"
LOOPBACK_ENDPOINT = "http://127.0.0.1:8101/mcp"
ALL_INTERFACES_ENDPOINT = "http://0.0.0.0:8101/mcp"


def _surface(
    endpoint: str, *, auth_posture: str = AUTH_UNKNOWN,
) -> LiveToolSurface:
    return LiveToolSurface(
        endpoint=endpoint,
        tool_names=tuple(AKMON_READ_TOOLS) + tuple(SOURCE_ADMIN_TOOLS),
        auth_posture=auth_posture,
        read_back_at="2026-08-04T00:00:00+00:00",
    )


def _prefix_projector(endpoint: str, **_: Any) -> dict[str, Any]:
    """What the projection looked like BEFORE this PACT.

    No transport check at all: it renders the preset from the live tool
    surface and returns it. Used only to prove the assertions below have
    teeth.
    """
    live = _surface(endpoint)
    return {
        "preset_id": "okg-workspace-admin",
        "bound_tools": list(live.source_admin_tools),
    }


def _fixed_projector(
    endpoint: str, *, auth_posture: str = AUTH_UNKNOWN, **kwargs: Any,
) -> dict[str, Any]:
    projection = project_admin_preset(
        deployment="okg-workspace",
        preset_id="okg-workspace-admin",
        live=_surface(endpoint, auth_posture=auth_posture),
        access_group_names=("okg-workspace-admins",),
        **kwargs,
    )
    return projection.to_dict()


def _assert_refuses_naming_the_precondition(
    projector: Callable[..., dict[str, Any]],
) -> None:
    """The assertion under test, applied to whichever projector.

    Raises ``AssertionError`` when the projector does NOT refuse — which
    is exactly what the pre-fix projector does.
    """
    try:
        projector(NETWORK_ENDPOINT)
    except ChatSourceAdminError as err:
        message = str(err)
        assert "non-loopback" in message
        assert "10.1.2.3" in message
        assert err.data["unmet_precondition"] == (
            "authenticated_or_loopback_transport"
        )
        return
    raise AssertionError(
        "the projector did not refuse a non-loopback unauthenticated "
        "endpoint"
    )


def _assert_no_partial_preset(
    projector: Callable[..., dict[str, Any]],
) -> None:
    """Nothing tool-bearing survives the refusal.

    The pre-fix projector returns a preset carrying the nine, which is
    the "present and degraded" state this requirement forbids.
    """
    try:
        result = projector(NETWORK_ENDPOINT)
    except ChatSourceAdminError:
        return
    raise AssertionError(
        f"a preset survived the refusal: {result}"
    )


# ---------------------------------------------------------------------------
# The refusal
# ---------------------------------------------------------------------------

def test_non_loopback_unauthenticated_endpoint_is_refused() -> None:
    _assert_refuses_naming_the_precondition(_fixed_projector)


def test_no_partially_projected_admin_preset_remains() -> None:
    _assert_no_partial_preset(_fixed_projector)


def test_all_interfaces_bind_is_not_loopback() -> None:
    """0.0.0.0 accepts connections from anywhere; it is not loopback.

    A host-prefix check would pass this and expose graph mutation to the
    network, so the loopback predicate is the server's own IP-literal
    one, imported rather than restated.
    """
    with pytest.raises(ChatSourceAdminError):
        _fixed_projector(ALL_INTERFACES_ENDPOINT)


def test_assertions_are_not_vacuous_against_the_pre_fix_path() -> None:
    """Both assertions FAIL against a projector with no transport check.

    Recorded in the evidence summary: without this arm, a projection
    that never refused anything would pass the two tests above only
    because they never ran the branch they claim to cover.
    """
    with pytest.raises(AssertionError, match="did not refuse"):
        _assert_refuses_naming_the_precondition(_prefix_projector)
    with pytest.raises(AssertionError, match="survived the refusal"):
        _assert_no_partial_preset(_prefix_projector)


# ---------------------------------------------------------------------------
# The permitted cells
# ---------------------------------------------------------------------------

def test_loopback_interim_is_permitted_and_carries_the_server_warning() -> None:
    """The loopback interim is allowed and RECORDS the server's warning.

    The guard's warn tuple otherwise reaches only ``run_server``'s log;
    carrying it here is what makes the interim's exposure visible
    instead of suppressed.
    """
    projection = _fixed_projector(LOOPBACK_ENDPOINT)
    warning = projection["transport"]["loopback_warning"]
    assert warning
    assert "unauthenticated" in warning
    assert "network namespace" in warning
    assert projection["transport"]["loopback"] is True
    assert projection["transport"]["authenticated"] is False
    assert set(projection["bound_tools"]) == set(SOURCE_ADMIN_TOOLS)


def test_authenticated_network_endpoint_is_permitted_without_a_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """What mcp-http-auth bought: a routable admin endpoint, authenticated.

    "Authenticated" is the OBSERVED posture (:data:`AUTH_REQUIRED`), not
    the presence of a credential. The warning is SUPPRESSED here on
    purpose — its text asserts the endpoint is unauthenticated, which is
    now false, and a warning that states a falsehood trains operators to
    ignore warnings.
    """
    monkeypatch.setenv("OKG_TEST_MCP_TOKEN", "x" * 64)
    projection = _fixed_projector(
        NETWORK_ENDPOINT,
        auth_token_env="OKG_TEST_MCP_TOKEN",
        auth_posture=AUTH_REQUIRED,
    )
    assert projection["transport"]["authenticated"] is True
    assert projection["transport"]["loopback_warning"] is None


@pytest.mark.parametrize("posture", [AUTH_OPEN, AUTH_UNKNOWN])
def test_holding_a_credential_does_not_authenticate_the_endpoint(
    posture: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolvable token is the OPERATOR's fact, not the SERVER's.

    ``_write_transport_guard`` requires its ``auth_active`` input to be
    derived from the served app, "never from a flag or an environment
    variable -- a credential that resolved but was never wired would
    otherwise unlock the routable write surface". So a routable endpoint
    that answered an ANONYMOUS read (AUTH_OPEN), or that could not be
    shown to demand one (AUTH_UNKNOWN), is still refused even though the
    operator holds a perfectly good token.
    """
    monkeypatch.setenv("OKG_TEST_MCP_TOKEN", "x" * 64)
    with pytest.raises(ChatSourceAdminError) as excinfo:
        _fixed_projector(
            NETWORK_ENDPOINT,
            auth_token_env="OKG_TEST_MCP_TOKEN",
            auth_posture=posture,
        )
    assert excinfo.value.data["unmet_precondition"] == (
        "authenticated_or_loopback_transport"
    )


def test_observe_auth_posture_reads_the_wire_not_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The three postures, each from what the endpoint actually did."""
    from okg.substrate.chat import source_admin as mod

    monkeypatch.setenv("OKG_TEST_MCP_TOKEN", "x" * 64)
    calls: list[dict[str, str]] = []

    def _dial(endpoint: str, headers: Any, timeout: float) -> tuple[str, ...]:
        calls.append(dict(headers))
        if not headers:
            raise RuntimeError("401 Unauthorized")
        return ("search",)

    monkeypatch.setattr(mod, "_dial_tools_list", _dial)
    assert mod.observe_auth_posture(
        NETWORK_ENDPOINT, auth_token_env="OKG_TEST_MCP_TOKEN",
    ) == AUTH_REQUIRED
    assert calls[0] == {}, "the anonymous probe must go first"

    # An endpoint that answers anonymously is OPEN, credential or not.
    monkeypatch.setattr(
        mod, "_dial_tools_list", lambda *a, **k: ("search",),
    )
    assert mod.observe_auth_posture(
        NETWORK_ENDPOINT, auth_token_env="OKG_TEST_MCP_TOKEN",
    ) == AUTH_OPEN

    # Unreachable: BOTH reads fail. A one-sided "the anonymous read
    # failed" would otherwise read as proof of authentication.
    def _dead(*_a: Any, **_k: Any) -> tuple[str, ...]:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(mod, "_dial_tools_list", _dead)
    assert mod.observe_auth_posture(
        NETWORK_ENDPOINT, auth_token_env="OKG_TEST_MCP_TOKEN",
    ) == AUTH_UNKNOWN


def test_no_uncredentialed_dial_to_a_routable_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every dial is loopback, credentialed, or the explicit probe.

    This is what makes the egress-coverage exemption for this module
    true rather than aspirational.
    """
    from okg.substrate.chat import source_admin as mod

    monkeypatch.setattr(
        mod,
        "_dial_tools_list",
        lambda *a, **k: pytest.fail("dialed without a guard"),
    )
    with pytest.raises(ChatSourceAdminError) as excinfo:
        mod.read_live_tools(NETWORK_ENDPOINT)
    assert excinfo.value.data["reason"] == "uncredentialed_routable_dial"
    with pytest.raises(ChatSourceAdminError) as excinfo:
        mod.call_live_tool(NETWORK_ENDPOINT, "list_sources", {})
    assert excinfo.value.data["reason"] == "uncredentialed_routable_dial"


def test_unresolvable_credential_refuses_rather_than_degrading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A named-but-unset variable is not "unauthenticated"; it is an error.

    Degrading to the unauthenticated arms would hand an operator who
    asked for authentication a decision computed as if they had not.
    """
    monkeypatch.delenv("OKG_TEST_MCP_TOKEN", raising=False)
    with pytest.raises(Exception) as excinfo:
        admin_transport_precondition(
            host="10.1.2.3", auth_token_env="OKG_TEST_MCP_TOKEN",
        )
    assert "not set" in str(excinfo.value)
    assert "refusing to start unauthenticated" in str(excinfo.value)


def test_endpoint_with_no_host_is_refused() -> None:
    """A posture that cannot be decided is refused, never assumed safe."""
    with pytest.raises(ChatSourceAdminError) as excinfo:
        require_admin_transport(
            deployment="okg-workspace",
            preset_id="okg-workspace-admin",
            endpoint="not-a-url",
        )
    assert excinfo.value.data["reason"] == "no_host"


# ---------------------------------------------------------------------------
# The CLI surface: a non-zero exit naming the precondition
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_cli_refuses_with_a_non_zero_exit_naming_the_precondition() -> None:
    """`okg chat source-admin` exits non-zero and names what is unmet.

    Run as a subprocess so the EXIT CODE is the observed fact. The
    refusal happens before any connection attempt, so this test needs no
    server: a run that tried to dial 10.1.2.3 would fail with a
    readback error instead, which is the wrong refusal.

    okg-workspace's manifest declares chat.mcp.auth_token_env (2026-08-18),
    and an UNSET credential variable refuses earlier than the transport
    rule — a different, separately-tested refusal. Satisfying it here
    keeps THIS arm pointed at the transport precondition, which still
    refuses a non-loopback endpoint even with a token set (the unit arm
    above proves that ordering).
    """
    completed = subprocess.run(
        [
            sys.executable, "-m", "okg.substrate.cli.main",
            "chat", "source-admin",
            "--deployment", "okg-workspace",
            "--endpoint", NETWORK_ENDPOINT,
            "--group", "okg-workspace-admins",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
        env={**os.environ, "OKG_WORKSPACE_MCP_TOKEN": "x" * 64},
    )
    assert completed.returncode != 0, completed.stdout
    combined = completed.stdout + completed.stderr
    assert "authenticated_or_loopback_transport" in combined
    assert "non-loopback" in combined
    # No preset, degraded or otherwise, was emitted. Asserted against the
    # key the projection ACTUALLY emits (`bound_tools`), pinned to the
    # payload rather than typed here: a key that no longer exists would
    # make this assertion pass while checking nothing.
    payload_keys = _fixed_projector(LOOPBACK_ENDPOINT)
    assert "bound_tools" in payload_keys
    assert "bound_tools" not in completed.stdout
