"""PACT ``chat-oidc-login`` -- a sync with ``chat.oidc_login`` CONVERGES.

The full apply runs twice against :class:`FakeInstance`: the second run must
find nothing to change. A connection written as `system_oauth` while the
expectation still said `bearer`/`none` would show up here as drift on every
run, which is exactly what a unit test of the payload alone cannot see.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.data import chat_data_dir
from okg.substrate.chat.plan_apply import resolve_chat_plan
from okg.substrate.chat.sync import ChatSyncError, InstanceClient, SyncInputs, sync
from okg.substrate.deployments.chat_block import parse_chat_block

from .fake_openwebui import FakeInstance

REPO_ROOT = Path(__file__).resolve().parents[3]
LIVE_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "query", "search")
ISSUER = "https://auth.example.org/realms/team"
CLIENT = "okg-chat"
ROLES = "resource_access.okg-chat.roles"
SECRET_ENV = "OKG_TEST_CHAT_OIDC_CLIENT_SECRET"


def _manifest(**login_overrides: Any) -> dict[str, Any]:
    login = {
        "issuer": ISSUER,
        "client_id": CLIENT,
        "client_secret_env": SECRET_ENV,
        "roles_claim": ROLES,
        "allowed_roles": ["default-role"],
        "admin_roles": ["archi-admins"],
    }
    login.update(login_overrides)
    return {
        "name": "fixture",
        "nomos": {
            "runtime_enforcement": "enforce",
            "oidc_auth": {
                "provider_id": "team-sso",
                "issuer": ISSUER,
                "audience": CLIENT,
                "subject_claim": "email",
                "roles_claim": ROLES,
                "principal_kind": "human_operator",
            },
        },
        "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        "chat": {
            "enabled": True,
            "ui": {"site_name": "Fixture Site"},
            "models": {"builtin": ["gpt-5"], "default": "gpt-5"},
            "preset": {
                "display_name": "Fixture",
                "system_prompt_ref": "chat/system_prompt.md",
                "suggestions": ["What changed?"],
            },
            "search_profile": "analyst",
            "mcp": {"port": 8100, "tools": list(LIVE_TOOLS)},
            "oidc_login": login,
        },
    }


def _inputs(tmp_path: Path, raw: dict[str, Any]) -> SyncInputs:
    dep_dir = tmp_path / "deployment"
    (dep_dir / "chat").mkdir(parents=True, exist_ok=True)
    (dep_dir / "chat" / "system_prompt.md").write_text(
        "Use the OKG tools. Quote node ids.", encoding="utf-8",
    )
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep_dir,
        raw_manifest=raw,
        chat=parse_chat_block(raw, manifest_path=dep_dir / "deployment.yaml"),
        mcp_instructions_path=None,
        chat_data_dir=chat_data_dir(),
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=tmp_path / "state",
    )


def _instance(*, login_offered: bool = True) -> FakeInstance:
    instance = FakeInstance(webui_name="Fixture Site")
    if login_offered:
        instance.oauth_providers = {"oidc": "Team SSO"}
    # The endpoint authenticates, as an OIDC-mode mcp-serve does. The fake's
    # verify then answers 400 to a `system_oauth` connection driven by an
    # operator credential, exactly as the measured instance did: a sync that
    # asked the instance to verify that connection would fail here.
    instance.mcp_auth_token = "what-only-a-signed-in-person-holds"
    return instance


def _apply(inputs: SyncInputs, instance: FakeInstance) -> Any:
    client = InstanceClient("http://instance.test", "fake-admin-token-0123456789", opener=instance.opener)
    return sync(inputs, client, probe_tools=lambda endpoint, env: LIVE_TOOLS)


def _changes(report: Any) -> dict[str, int]:
    return {
        name: getattr(outcome, "changes", 0)
        for name, outcome in report.surfaces.items()
        if getattr(outcome, "changes", 0)
    }


@pytest.fixture(autouse=True)
def _client_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(SECRET_ENV, "fixture-client-secret")


def test_oidc_sync_converges_on_the_second_run(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path, _manifest())
    instance = _instance()
    first = _apply(inputs, instance)
    assert first.ok, first.surfaces
    # Registered, proven reachable with the service token, and honestly NOT
    # verified by this run: only a signed-in person's token can do that.
    assert first.mcp["registered_connection_probe"] == "not_probed"
    assert first.mcp["registered_connection_verified"] is None
    second = _apply(inputs, instance)
    assert second.ok, second.surfaces
    assert _changes(second) == {}, "a converged OIDC sync must find nothing to change"
    graph = next(
        entry for entry in instance.tool_servers
        if (entry.get("info") or {}).get("id") == "okg-fixture"
    )
    assert graph["auth_type"] == "system_oauth"
    assert graph["key"] == ""


def test_the_plan_expects_what_the_apply_writes(tmp_path: Path) -> None:
    raw = _manifest()
    plan = resolve_chat_plan(_inputs(tmp_path, raw))
    rendered = repr(plan)
    assert "system_oauth" in rendered


def test_sync_refuses_an_instance_without_the_declared_login(tmp_path: Path) -> None:
    instance = _instance(login_offered=False)
    with pytest.raises(ChatSyncError) as excinfo:
        _apply(_inputs(tmp_path, _manifest()), instance)
    assert excinfo.value.data["reason"] == "instance_oidc_login_absent"
    assert instance.tool_servers == []


def test_sync_refuses_login_blocks_that_disagree(tmp_path: Path) -> None:
    from okg.substrate.errors import SubstrateError

    instance = _instance()
    with pytest.raises(SubstrateError) as excinfo:
        _apply(_inputs(tmp_path, _manifest(client_id="another-client")), instance)
    assert excinfo.value.code == "mcp_oidc_login_blocks_disagree"
    assert instance.tool_servers == []
