"""``okg chat plan`` / apply / ``okg chat readback`` (PACT chat-plan-apply-readback).

Every test drives the real functions against
:class:`tests.substrate.chat.fake_openwebui.FakeInstance`, and the
assertions are on the BYTES written and on the fake's request log — not on
internal variables. Two of them could not be written any other way:

* the plan carries no secret VALUE. The token is built at run time and
  exported, then searched for in the plan's bytes, so a plan that started
  embedding a resolved credential fails here rather than in review.
* the readback issues no write. The fake records method and path for every
  call, so "reads only" is checked against the wire instead of asserted.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.data import chat_data_dir
from okg.substrate.chat.plan_apply import (
    CHAT_PLAN_SCHEMA,
    CHAT_READBACK_SCHEMA,
    PROBE_ERROR,
    ChatPlanDrift,
    ChatPlanError,
    ChatPlanOutputPathExists,
    ChatReadbackDeviation,
    chat_readback,
    load_chat_plan,
    plan_digest,
    resolve_chat_plan,
    write_chat_plan,
    write_chat_readback,
)
from okg.substrate.chat.sync import (
    InstanceClient,
    SyncInputs,
    sync,
)
from okg.substrate.deployments.chat_block import parse_chat_block
from tests.substrate.chat.fake_openwebui import FakeInstance

REPO_ROOT = Path(__file__).resolve().parents[2]
TOKEN = "fake-admin-token-value-0123456789"
BASE_URL = "http://instance.test"
LIVE_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "query", "search")

#: The variable the fixture manifest names for the MCP bearer credential.
MCP_TOKEN_ENV = "OKG_TEST_CHAT_MCP_TOKEN"


def _raw_manifest(**chat_overrides: Any) -> dict[str, Any]:
    chat: dict[str, Any] = {
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
        "mcp": {
            "port": 8100,
            "tools": list(LIVE_TOOLS),
            "auth_token_env": MCP_TOKEN_ENV,
        },
        "access": {"groups": ["okg-users"]},
        "deep_links": {
            "operator_ui_base": "http://console.test",
            "chat_site_base": "http://chat.test",
        },
    }
    chat.update(chat_overrides)
    return {
        "name": "fixture",
        "nomos": {"runtime_enforcement": "enforce"},
        "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        "chat": chat,
    }


def _make_inputs(tmp_path: Path, manifest: dict[str, Any] | None = None) -> SyncInputs:
    raw = manifest if manifest is not None else _raw_manifest()
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


def _instance() -> FakeInstance:
    instance = FakeInstance(webui_name="Fixture Site")
    instance.add_group("okg-users", "group-users")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    return instance


def _client(instance: FakeInstance) -> InstanceClient:
    return InstanceClient(BASE_URL, TOKEN, opener=instance.opener)


def _probe(_endpoint: str, _token_env: str | None) -> tuple[str, ...]:
    return LIVE_TOOLS


@pytest.fixture
def mcp_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> str:
    """A credential built at RUN TIME, so it cannot be a literal anywhere.

    A hard-coded credential-shaped string in a test is both a CodeQL
    finding and a weaker check: a plan could match it by accident. This
    one is fresh per test, which makes the secret-freedom assertion a real
    search rather than a comparison against a constant the code could not
    have contained.
    """
    value = "mcp-" + uuid.uuid4().hex
    monkeypatch.setenv(MCP_TOKEN_ENV, value)
    return value


def _apply(inputs: SyncInputs, instance: FakeInstance, **kwargs: Any) -> Any:
    return sync(inputs, _client(instance), probe_tools=_probe, **kwargs)


# ---------------------------------------------------------------------------
# (a) the plan
# ---------------------------------------------------------------------------

def test_plan_is_byte_identical_on_rerun(tmp_path: Path, mcp_token: str) -> None:
    """req.chat-plan-apply-readback.plan-deterministic."""
    inputs = _make_inputs(tmp_path)
    first = write_chat_plan(inputs, tmp_path / "a")["plan_path"]
    second = write_chat_plan(inputs, tmp_path / "b")["plan_path"]
    assert Path(first).read_bytes() == Path(second).read_bytes()
    plan = json.loads(Path(first).read_text())
    assert plan["schema"] == CHAT_PLAN_SCHEMA
    assert plan["plan_digest"] == plan_digest(plan)
    assert sorted(plan["surfaces"]) == [
        # `live_tools` is #1183 S4's surface. It is present on EVERY plan,
        # including this manifest's, which declares no slot: its desired
        # `live_connection:present` is False, and the readback confirming
        # that is how "no live tool by default" is measured rather than
        # assumed.
        "deep_links", "instance_ui", "live_tools", "mcp", "models", "preset",
        "prompts",
    ]


def test_plan_carries_the_credential_reference_and_no_value(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.plan-secret-free."""
    written = write_chat_plan(_make_inputs(tmp_path), tmp_path / "out")
    raw = Path(written["plan_path"]).read_bytes()
    assert mcp_token.encode("utf-8") not in raw
    assert TOKEN.encode("utf-8") not in raw
    plan = json.loads(raw)
    assert plan["credential_references"] == [
        {"kind": "env", "variable": MCP_TOKEN_ENV, "used_by": "connection:key"},
    ]
    assert plan["surfaces"]["preset"]["desired"]["connection:auth_type"] == "bearer"


def test_plan_contacts_no_instance(tmp_path: Path, mcp_token: str) -> None:
    """req.chat-plan-apply-readback.plan-offline.

    The instance exists and would answer; the plan is written without it,
    and its request log stays empty.
    """
    instance = _instance()
    write_chat_plan(_make_inputs(tmp_path), tmp_path / "out")
    assert instance.requests == []


def test_plan_refuses_a_pre_existing_output_path(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.plan-no-overwrite."""
    inputs = _make_inputs(tmp_path)
    write_chat_plan(inputs, tmp_path / "out")
    with pytest.raises(ChatPlanOutputPathExists) as caught:
        write_chat_plan(inputs, tmp_path / "out")
    assert caught.value.code == "chat_plan_output_path_exists"
    assert caught.value.data["reason"] == "output_path_exists"


def test_plan_refuses_a_declared_but_unset_credential_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(MCP_TOKEN_ENV, raising=False)
    with pytest.raises(ChatPlanError) as caught:
        resolve_chat_plan(_make_inputs(tmp_path))
    assert caught.value.data["reason"] == "credential_reference_unresolved"
    assert caught.value.data["variable"] == MCP_TOKEN_ENV


def test_plan_names_what_it_cannot_determine(tmp_path: Path, mcp_token: str) -> None:
    """A plan that silently omitted these would read as a complete one."""
    plan = resolve_chat_plan(_make_inputs(tmp_path))
    preset = {u["key"] for u in plan["surfaces"]["preset"]["undetermined"]}
    assert "preset:params.system" in preset
    assert "connection:url" in preset
    assert "preset:grants" in preset
    ui = {u["key"] for u in plan["surfaces"]["instance_ui"]["undetermined"]}
    assert "features.context_compaction" in ui


def test_an_edited_plan_is_refused_on_load(tmp_path: Path, mcp_token: str) -> None:
    written = write_chat_plan(_make_inputs(tmp_path), tmp_path / "out")
    path = Path(written["plan_path"])
    doctored = json.loads(path.read_text())
    doctored["surfaces"]["models"]["desired"]["ui.default_models"] = "other/model"
    path.write_text(json.dumps(doctored))
    with pytest.raises(ChatPlanError) as caught:
        load_chat_plan(path)
    assert caught.value.data["reason"] == "plan_digest_self_mismatch"


# ---------------------------------------------------------------------------
# (b) apply
# ---------------------------------------------------------------------------

def test_apply_records_the_digest_of_the_plan_it_applied(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.apply-records-digest."""
    inputs = _make_inputs(tmp_path)
    plan = load_chat_plan(write_chat_plan(inputs, tmp_path / "out")["plan_path"])
    report = _apply(inputs, _instance(), expected_plan=plan)
    assert report.ok
    assert report.plan_digest == plan["plan_digest"]
    assert report.to_dict()["plan_digest"] == plan["plan_digest"]


def test_apply_without_a_plan_still_records_the_manifest_digest(
    tmp_path: Path, mcp_token: str,
) -> None:
    inputs = _make_inputs(tmp_path)
    report = _apply(inputs, _instance())
    assert report.plan_digest == resolve_chat_plan(inputs)["plan_digest"]


def test_apply_refuses_a_plan_that_drifted_from_the_manifest(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.apply-refuses-drift.

    The refusal fires BEFORE the instance is dialled, which is asserted on
    the fake's request log rather than inferred from the message.
    """
    plan = load_chat_plan(
        write_chat_plan(_make_inputs(tmp_path), tmp_path / "out")["plan_path"],
    )
    edited = _raw_manifest()
    edited["chat"]["models"]["default"] = "base/model-b"
    edited["chat"]["models"]["builtin"] = ["base/model-b"]
    instance = _instance()
    with pytest.raises(ChatPlanDrift) as caught:
        _apply(_make_inputs(tmp_path / "second", edited), instance, expected_plan=plan)
    assert caught.value.code == "chat_plan_drift"
    assert caught.value.data["reason"] == "plan_manifest_drift"
    assert caught.value.data["plan_digest"] != caught.value.data["manifest_digest"]
    assert instance.requests == []


# ---------------------------------------------------------------------------
# (c) readback
# ---------------------------------------------------------------------------

def _applied(tmp_path: Path) -> tuple[SyncInputs, FakeInstance, dict[str, Any]]:
    inputs = _make_inputs(tmp_path)
    plan = load_chat_plan(write_chat_plan(inputs, tmp_path / "out")["plan_path"])
    instance = _instance()
    report = _apply(inputs, instance, expected_plan=plan)
    assert report.ok
    return inputs, instance, plan


def test_readback_matches_every_surface_on_a_healthy_instance(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.readback-matches."""
    inputs, instance, plan = _applied(tmp_path)
    document = chat_readback(inputs, _client(instance), plan)
    assert document["schema"] == CHAT_READBACK_SCHEMA
    assert document["ok"], document["deviations"]
    assert document["code"] is None
    assert document["plan_digest"] == plan["plan_digest"]
    assert sorted(document["surfaces"]) == [
        # `live_tools` is #1183 S4's surface. It is present on EVERY plan,
        # including this manifest's, which declares no slot: its desired
        # `live_connection:present` is False, and the readback confirming
        # that is how "no live tool by default" is measured rather than
        # assumed.
        "deep_links", "instance_ui", "live_tools", "mcp", "models", "preset",
        "prompts",
    ]
    assert all(entry["ok"] for entry in document["surfaces"].values())
    # The prompts surface is DROPPED at this repo state, and a dropped
    # surface is the specified outcome — not a deviation.
    assert document["surfaces"]["prompts"]["status"] == "dropped"
    assert document["verify_probe"]["state"] == "healthy"
    assert document["verify_probe"]["auth_type"] == "bearer"


def test_readback_reports_exactly_the_surface_mutated_out_of_band(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.readback-detects-drift."""
    inputs, instance, plan = _applied(tmp_path)
    instance.models["okg-fixture"]["name"] = "Renamed By Hand"
    document = chat_readback(inputs, _client(instance), plan)
    assert not document["ok"]
    assert document["code"] == ChatReadbackDeviation.code
    assert [d["surface"] for d in document["deviations"]] == ["preset"]
    deviation = document["deviations"][0]
    assert deviation["key"] == "preset:name"
    assert deviation["expected"] == "Fixture Preset"
    assert deviation["observed"] == "Renamed By Hand"
    assert document["surfaces"]["preset"]["status"] == "deviation"
    assert [
        name for name, entry in document["surfaces"].items() if not entry["ok"]
    ] == ["preset"]


def test_readback_writes_nothing_to_the_instance(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-plan-apply-readback.readback-read-only.

    Checked on the WIRE. Every request the readback issues is a GET, with
    the single exception of the vendor's own verify route, which dials
    outward and stores nothing.
    """
    inputs, instance, plan = _applied(tmp_path)
    del instance.requests[:]
    chat_readback(inputs, _client(instance), plan)
    assert instance.requests
    non_get = [
        (method, path) for method, path in instance.requests if method != "GET"
    ]
    assert non_get == [("POST", "/api/v1/configs/tool_servers/verify")]


def test_a_version_without_the_verify_route_is_not_a_deviation(
    tmp_path: Path, mcp_token: str,
) -> None:
    """An ABSENT measurement is not a bad credential.

    S1's rule, carried into the readback: a version with no verify route
    has measured nothing about the credential, and reporting that as drift
    sends an operator to rotate a token that was never the problem.
    """
    inputs, instance, plan = _applied(tmp_path)
    instance.verify_route_present = False
    document = chat_readback(inputs, _client(instance), plan)
    assert document["ok"], document["deviations"]
    assert document["verify_probe"]["state"] == "unavailable"


def test_a_failing_verify_route_is_reported_apart_from_an_absent_one(
    tmp_path: Path, mcp_token: str,
) -> None:
    """The two are different facts and must not share a state.

    `verify_tool_server` answers 404 with `unavailable` itself and RAISES
    on every other route failure. Folding those raises into `unavailable`
    would report an unmeasured credential as a measured absence, so they
    get their own state — and, because the readback's one credential
    measurement did not run, a deviation rather than a pass.
    """
    inputs, instance, plan = _applied(tmp_path)
    instance.fail_paths.add("/api/v1/configs/tool_servers/verify")
    document = chat_readback(inputs, _client(instance), plan)
    assert not document["ok"]
    assert document["verify_probe"]["state"] == PROBE_ERROR
    assert [d["key"] for d in document["deviations"]] == ["verify_probe"]
    assert document["deviations"][0]["reason"] == "verify_probe_unmeasured"


def test_readback_refuses_a_plan_edited_after_it_was_written(
    tmp_path: Path, mcp_token: str,
) -> None:
    inputs, instance, plan = _applied(tmp_path)
    doctored = dict(plan)
    doctored["deployment"] = "somewhere-else"
    with pytest.raises(ChatPlanError) as caught:
        chat_readback(inputs, _client(instance), doctored)
    assert caught.value.data["reason"] == "plan_digest_self_mismatch"


def test_readback_document_refuses_a_pre_existing_path(
    tmp_path: Path, mcp_token: str,
) -> None:
    inputs, instance, plan = _applied(tmp_path)
    document = chat_readback(inputs, _client(instance), plan)
    write_chat_readback(document, tmp_path / "rb")
    with pytest.raises(ChatPlanOutputPathExists):
        write_chat_readback(document, tmp_path / "rb")


# ---------------------------------------------------------------------------
# (d) the stored credential after a token rotation (#2287)
# ---------------------------------------------------------------------------

def _applied_with_endpoint_token(
    tmp_path: Path, token: str,
) -> tuple[SyncInputs, FakeInstance, dict[str, Any]]:
    inputs = _make_inputs(tmp_path)
    plan = load_chat_plan(write_chat_plan(inputs, tmp_path / "out")["plan_path"])
    instance = _instance()
    instance.mcp_auth_token = token
    report = _apply(inputs, instance, expected_plan=plan)
    assert report.ok
    return inputs, instance, plan


def _rotated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, old_token: str,
) -> tuple[SyncInputs, FakeInstance, dict[str, Any], str]:
    """Sync with ``old_token``, then rotate mcp-serve and the operator env.

    The rotation the reference docs describe: restart ``mcp-serve`` with a
    new token and export it, WITHOUT re-running ``okg chat sync``. The site
    still stores the old token, so every chat turn gets 401.
    """
    inputs, instance, plan = _applied_with_endpoint_token(tmp_path, old_token)
    new_token = "mcp-" + uuid.uuid4().hex
    instance.mcp_auth_token = new_token
    monkeypatch.setenv(MCP_TOKEN_ENV, new_token)
    return inputs, instance, plan, new_token


def test_readback_reports_a_rotated_token_the_site_never_stored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mcp_token: str,
) -> None:
    """#2287: readback was clean while every chat turn got 401.

    The verify route dials with the credential in the REQUEST, which the
    readback builds from the operator's environment, so it still answers
    healthy. The deviation has to come from the stored credential.
    """
    inputs, instance, plan, _new = _rotated(tmp_path, monkeypatch, mcp_token)
    document = chat_readback(inputs, _client(instance), plan)
    assert not document["ok"]
    assert document["code"] == ChatReadbackDeviation.code
    assert [(d["surface"], d["key"], d["reason"]) for d in document["deviations"]] == [
        ("mcp", "stored_credential", "mcp_connection_credential_stale"),
    ]
    assert document["deviations"][0]["differing"] == ["key"]
    assert document["surfaces"]["mcp"]["status"] == "deviation"
    # The operator's own credential works; that is what the probe measured.
    assert document["verify_probe"]["state"] == "healthy"
    assert document["verify_probe"]["stored_credential"] == {
        "state": "stale", "differing": ["key"],
    }


def test_a_sync_after_rotation_clears_the_stale_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mcp_token: str,
) -> None:
    inputs, instance, plan, _new = _rotated(tmp_path, monkeypatch, mcp_token)
    assert _apply(inputs, instance, expected_plan=plan).ok
    document = chat_readback(inputs, _client(instance), plan)
    assert document["ok"], document["deviations"]
    assert document["verify_probe"]["stored_credential"] == {
        "state": "matches", "differing": [],
    }


def test_no_token_reaches_the_readback_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mcp_token: str,
) -> None:
    """Neither the old nor the new token, nor any digest, is written."""
    inputs, instance, plan, new_token = _rotated(tmp_path, monkeypatch, mcp_token)
    document = chat_readback(inputs, _client(instance), plan)
    written = write_chat_readback(document, tmp_path / "rb")
    raw = Path(written["readback_path"]).read_text(encoding="utf-8")
    assert "mcp_connection_credential_stale" in raw
    for secret in (mcp_token, new_token):
        assert secret not in raw
    assert "hmac:" not in raw


def test_a_hand_edited_credential_header_is_stale(
    tmp_path: Path, mcp_token: str,
) -> None:
    """The vendor merges `headers` over the key; sync never writes one."""
    inputs, instance, plan = _applied(tmp_path)
    entry = next(
        e for e in instance.tool_servers
        if (e.get("info") or {}).get("id") == "okg-fixture"
    )
    entry["headers"] = {"Authorization": "Bearer " + uuid.uuid4().hex}
    document = chat_readback(inputs, _client(instance), plan)
    assert [d["reason"] for d in document["deviations"]] == [
        "mcp_connection_credential_stale",
    ]
    assert document["deviations"][0]["differing"] == ["headers"]


def test_a_masking_version_reports_the_key_unmeasured(
    tmp_path: Path, mcp_token: str,
) -> None:
    """A masked key cannot be compared, so readback fails closed.

    Not `matches`, and not a pass: an unmeasured credential has not been
    shown current, as `verify_probe_unmeasured` already treats a failed
    verify route.
    """
    inputs, instance, plan = _applied(tmp_path)
    instance.mask_tool_server_key = True
    document = chat_readback(inputs, _client(instance), plan)
    assert not document["ok"]
    assert [(d["surface"], d["key"], d["reason"]) for d in document["deviations"]] == [
        ("mcp", "stored_credential", "mcp_connection_credential_unmeasured"),
    ]
    assert "hides the stored key" in document["deviations"][0]["detail"]
    assert document["verify_probe"]["stored_credential"] == {
        "state": "masked", "differing": [],
    }


def test_a_masking_version_after_rotation_is_not_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mcp_token: str,
) -> None:
    """The case a masking vendor would otherwise read as healthy."""
    inputs, instance, plan, new_token = _rotated(tmp_path, monkeypatch, mcp_token)
    instance.mask_tool_server_key = True
    document = chat_readback(inputs, _client(instance), plan)
    assert not document["ok"]
    assert [d["reason"] for d in document["deviations"]] == [
        "mcp_connection_credential_unmeasured",
    ]
    raw = json.dumps(document)
    for secret in (mcp_token, new_token, "hmac:"):
        assert secret not in raw


def test_two_connections_sharing_the_server_id_are_a_deviation(
    tmp_path: Path, mcp_token: str,
) -> None:
    """A stale second entry must not hide behind a current first one."""
    import copy

    inputs, instance, plan = _applied(tmp_path)
    first = next(
        e for e in instance.tool_servers
        if (e.get("info") or {}).get("id") == "okg-fixture"
    )
    duplicate = copy.deepcopy(first)
    duplicate["key"] = "mcp-" + uuid.uuid4().hex
    instance.tool_servers.append(duplicate)
    document = chat_readback(inputs, _client(instance), plan)
    assert not document["ok"]
    assert [(d["surface"], d["key"], d["reason"]) for d in document["deviations"]] == [
        ("mcp", "stored_credential", "mcp_connection_duplicate_id"),
    ]
    assert document["verify_probe"]["stored_credential"] == {
        "state": "duplicate", "differing": [], "entries": 2,
    }
    raw = json.dumps(document)
    for secret in (mcp_token, duplicate["key"], "hmac:"):
        assert secret not in raw


def test_readback_refuses_when_the_operator_credential_is_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mcp_token: str,
) -> None:
    """No credential to compare against is a refusal, never a clean pass."""
    from okg.substrate.chat.sync import ChatSyncError

    inputs, instance, plan = _applied(tmp_path)
    monkeypatch.delenv(MCP_TOKEN_ENV)
    with pytest.raises(ChatSyncError) as caught:
        chat_readback(inputs, _client(instance), plan)
    assert caught.value.data["reason"] == "mcp_connection_credential_unresolved"
