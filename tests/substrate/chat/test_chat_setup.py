"""``okg chat setup`` (issue #1792): one entry point, bootstrapped admin access,
provider registration by secret reference, convergence apart from readiness.

Everything here drives the REAL decision code in ``chat.setup`` against the
in-process Open WebUI fake, whose auth, provider, model and completion routes
model the semantics measured on ``ghcr.io/open-webui/open-webui:v0.11.0``
(``fake_openwebui.py``). The container runtime, the graph endpoint and the
sync run are injected seams, each recorded so a test can assert what was and
was not done. The live arm — a real container, this machine's model server
and a real ``mcp-serve`` — is a separate operator report, not this file.

The secrets in this file are SENTINELS: every rendered output of a run (the
JSON envelope, the human text, the error payload) is swept for each of them.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.chat.instance import (
    MCP_STATE_DOWN,
    MCP_STATE_UP,
    McpObservation,
)
from okg.substrate.chat.setup import (
    ENV_ADMIN_EMAIL,
    ENV_ADMIN_PASSWORD,
    ENV_ADMIN_TOKEN,
    PROVIDER_TOOL_RECORD_KEY,
    REASON_GRAPH_TOOLS_FAILED,
    REASON_GRAPH_TOOLS_REFUSED,
    REASON_PROVIDER_TOOLS_FAILED,
    REASON_TOOL_USE_UNOBSERVED,
    STEPS,
    TOOL_EVIDENCE_INSTANCE,
    TOOL_EVIDENCE_PROVIDER,
    SetupOptions,
    default_admin_email,
    mcp_serve_argv,
    normalize_provider_url,
    render_human,
    run_setup,
)
from okg.substrate.chat.sync import MCP_TOOL_ID_PREFIX
from okg.substrate.deployments.chat_block import (
    ChatManifestError,
    parse_chat_block,
    schema_field_census,
)

from .fake_openwebui import FakeInstance
from .instance_harness import VALID_CHAT

DEPLOYMENT = "fixture-deployment"
PRESET_ID = f"okg-{DEPLOYMENT}"
BASE_URL = "http://instance.test"
DEFAULT_MODEL = "anthropic/claude-sonnet-5"
PROVIDER_URL = "http://host.docker.internal:11434/v1"
PROVIDER_KEY_ENV = "OKG_FIXTURE_PROVIDER_KEY"
MCP_TOKEN_ENV = "OKG_FIXTURE_MCP_TOKEN"

# Sentinels. Deliberately SHORT and punctuated where the operator would
# choose them, so the shape-based redaction cannot be what hides them.
PASSWORD = "pw!Sentinel-7"  # noqa: S105 - test sentinel
ENV_TOKEN = "sk-env-token-sentinel-0123456789"  # noqa: S105 - test sentinel
PROVIDER_KEY = "prov-key!sentinel"
MCP_TOKEN = "mcp-bearer-sentinel-0123456789"  # noqa: S105 - test sentinel
APP_DSN = "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_chat_app"

#: One graph tool call the fake's model makes; the fake records it the way
#: Open WebUI 0.11 does (``fake_openwebui.FakeInstance.completion_tool_runs``).
TOOL_RUN = {
    "tool": "search",
    "arguments": {"query": "records"},
    "result": '{"generation_id": 41, "hits": []}',
}
#: The vendor's name for that call: ``<tool server id>_<tool>``.
GRAPH_SEARCH = f"{PRESET_ID}_search"


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def _chat(*, provider: dict[str, Any] | None = "default", auth_env: str | None = MCP_TOKEN_ENV):
    raw = copy.deepcopy(VALID_CHAT)
    raw["mcp"] = {"port": 8101, "tools": ["inspect", "search"]}
    if auth_env:
        raw["mcp"]["auth_token_env"] = auth_env
    if provider == "default":
        raw["provider"] = {"base_url": PROVIDER_URL, "api_key_env": PROVIDER_KEY_ENV}
    elif provider is not None:
        raw["provider"] = provider
    manifest = {
        "name": DEPLOYMENT,
        "search": {"default_profile": "analyst", "profiles": {"analyst": {"subtypes": ["chunk"]}}},
        "chat": raw,
    }
    return parse_chat_block(manifest, manifest_path=None)


def _instance(*, onboarding: bool = True) -> FakeInstance:
    instance = FakeInstance()
    instance.provider_endpoints[(PROVIDER_URL, PROVIDER_KEY)] = [DEFAULT_MODEL, "other/model"]
    instance.served_models = [DEFAULT_MODEL, "other/model"]
    instance.completion_tool_runs = [copy.deepcopy(TOOL_RUN)]
    if not onboarding:
        instance.add_admin("existing@example.invalid", PASSWORD)
    return instance


def _env(**extra: str) -> dict[str, str]:
    env = {
        "OKG_CHAT_APP_DATABASE_URL": APP_DSN,
        "OKG_CHAT_WEBUI_SECRET_KEY": "a-long-random-fixture-key",
        PROVIDER_KEY_ENV: PROVIDER_KEY,
        MCP_TOKEN_ENV: MCP_TOKEN,
        ENV_ADMIN_PASSWORD: PASSWORD,
    }
    env.update(extra)
    return env


class Seams:
    """The injected halves, each recording what setup asked of it."""

    def __init__(self, *, mcp_up: bool = True, sync_ok: bool = True) -> None:
        self.stand_up_calls: list[dict[str, Any]] = []
        self.launched: list[tuple[list[str], Path]] = []
        self.sync_calls: list[tuple[Any, Any]] = []
        self.mcp_up = mcp_up
        self.sync_ok = sync_ok
        self.probed: list[str] = []
        #: The fake instance; a sync that runs creates the preset on it, as
        #: the real sync does (`/api/v1/models/create`).
        self.instance: FakeInstance | None = None

    def stand_up(self, deployment: str, **kwargs: Any) -> Any:
        self.stand_up_calls.append(kwargs)
        return SimpleNamespace(
            base_url=BASE_URL, outcome="already_running", version="0.11.0",
            container=f"okg-chat-{deployment}", image="ghcr.io/open-webui/open-webui:v0.11.0",
            notes=(),
        )

    def probe(self, endpoint: str) -> McpObservation:
        self.probed.append(endpoint)
        state = MCP_STATE_UP if self.mcp_up else MCP_STATE_DOWN
        return McpObservation(state=state, endpoint=endpoint, detail=f"fixture {state}")

    def launch(self, argv: list[str], log_path: Path) -> int:
        self.launched.append((list(argv), log_path))
        self.mcp_up = True
        return 4242

    def load_inputs(self, deployment: str, **kwargs: Any) -> Any:
        return SimpleNamespace(
            deployment=deployment, preset_id=f"okg-{deployment}",
            mcp_server_id=f"okg-{deployment}", **kwargs,
        )

    def run_sync(self, inputs: Any, client: Any) -> Any:
        self.sync_calls.append((inputs, client))
        if self.instance is not None and inputs.preset_id not in self.instance.served_models:
            self.instance.served_models.append(inputs.preset_id)
        return SimpleNamespace(
            ok=self.sync_ok, plan_digest="sha256:fixture", changes=2,
            written_surfaces_verified=5, mcp={"tools": ["inspect", "search"]},
            to_dict=lambda: {"ok": self.sync_ok},
        )


def _run(
    instance: FakeInstance,
    env: dict[str, str],
    tmp_path: Path,
    *,
    chat: Any = None,
    seams: Seams | None = None,
    options: SetupOptions | None = None,
    state_root: Path | None = None,
) -> tuple[Any, Seams]:
    seams = seams or Seams()
    seams.instance = instance
    block = chat if chat is not None else _chat()
    report = run_setup(
        DEPLOYMENT,
        options=options,
        environ=env,
        state_root=state_root or tmp_path / "state",
        load_manifest=lambda: (block, "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_graph"),
        stand_up=seams.stand_up,
        opener=instance.opener,
        load_inputs=seams.load_inputs,
        run_sync=seams.run_sync,
        mcp_probe=seams.probe,
        mcp_launcher=seams.launch,
        sleep=lambda _s: None,
    )
    return report, seams


def _posts(instance: FakeInstance, path: str) -> list[Any]:
    return [body for method, p, body in instance.request_log if method == "POST" and p == path]


def report_chat_id(instance: FakeInstance) -> str:
    (chat_id,) = instance.chats
    return chat_id


def _rendered(report: Any) -> str:
    return json.dumps(report.to_dict(), sort_keys=True, default=str) + "\n" + render_human(report)


# ---------------------------------------------------------------------------
# One run, from nothing to a conversation that used the graph
# ---------------------------------------------------------------------------

def test_first_run_creates_the_admin_registers_the_provider_and_is_ready(tmp_path: Path) -> None:
    instance = _instance()
    report, seams = _run(instance, _env(), tmp_path)

    assert report.ok, report.to_dict()
    assert report.converged and report.conversation_ready
    # No chat answers were given, so the configure step (issue #2288) left
    # the manifest alone and every step after it ran.
    assert STEPS[0] == "configure"
    assert [report.steps[s].status for s in STEPS] == ["skipped"] + ["ok"] * (len(STEPS) - 1)
    # The first account is the admin the vendor makes it, from the two
    # named variables; the session token is what the rest dialled with.
    assert [u["role"] for u in instance.users] == ["admin"]
    assert instance.users[0]["email"] == default_admin_email(DEPLOYMENT)
    assert report.credential_source == "signup"
    assert report.admin_email == default_admin_email(DEPLOYMENT)
    # The provider was verified BEFORE it was registered, and registered
    # once, with the key from the environment, other entries untouched.
    assert _posts(instance, "/openai/verify") == [{"url": PROVIDER_URL, "key": PROVIDER_KEY}]
    assert instance.provider_config["OPENAI_API_BASE_URLS"] == ["https://api.openai.com/v1", PROVIDER_URL]
    assert instance.provider_config["OPENAI_API_KEYS"] == ["", PROVIDER_KEY]
    assert instance.provider_config["OPENAI_API_CONFIGS"]["1"] == {"enable": True}
    assert any("registered" in c for c in report.steps["provider"].changes)
    assert report.steps["model"].data["default"] == DEFAULT_MODEL
    # The instance-side MCP URL is the gateway alias, and sync ran ONCE with
    # the client setup built.
    (inputs, _client), = seams.sync_calls
    assert inputs.mcp_instance_url == "http://host.docker.internal:8101/mcp"
    assert inputs.mcp_probe_url == "http://127.0.0.1:8101/mcp"
    assert report.preset_id == PRESET_ID
    assert report.site_url == f"{BASE_URL}/?models={PRESET_ID}"
    assert report.conversation["tools_invoked"] == [GRAPH_SEARCH]
    assert report.conversation["tool_call_count"] == 1
    assert report.conversation["generation_evidence"] is True
    # The test question is asked in a stored chat, which stays for the
    # operator to open.
    assert list(instance.chats) == [report.conversation["chat_id"]]
    assert report.next_action is None


def test_the_conversation_probe_lists_the_tool_server_explicitly(tmp_path: Path) -> None:
    """The pinned vendor attaches a preset's tools through the API only when
    the caller lists them; selecting the preset is not attachment."""
    instance = _instance()
    _run(instance, _env(), tmp_path)

    (completion,) = _posts(instance, "/api/chat/completions")
    assert completion["model"] == PRESET_ID
    assert completion["tool_ids"] == [f"{MCP_TOOL_ID_PREFIX}{PRESET_ID}"]
    assert completion["messages"][0]["content"] == VALID_CHAT["preset"]["suggestions"][0]
    # Issue #2907: Open WebUI 0.11 runs tools only in a streamed turn of a
    # stored chat, so the question names the chat and the assistant message,
    # streams, and pins the vendor's native tool loop.
    (created,) = _posts(instance, "/api/v1/chats/new")
    assert completion["chat_id"] == report_chat_id(instance)
    assert completion["stream"] is True
    assert completion["id"] and completion["user_message"]["childrenIds"] == [completion["id"]]
    assert completion["params"] == {"function_calling": "native"}
    assert "session_id" not in completion
    assert created["chat"]["models"] == [PRESET_ID]


# ---------------------------------------------------------------------------
# Reapply preserves accounts and configuration
# ---------------------------------------------------------------------------

def test_second_run_signs_in_and_rewrites_neither_accounts_nor_the_provider(tmp_path: Path) -> None:
    instance = _instance()
    first, _ = _run(instance, _env(), tmp_path)
    assert first.ok
    instance.request_log.clear()

    second, _ = _run(instance, _env(), tmp_path)

    assert second.ok, second.to_dict()
    assert second.credential_source == "signin"
    assert len(instance.users) == 1
    assert _posts(instance, "/api/v1/auths/signup") == []
    assert _posts(instance, "/openai/config/update") == []
    assert second.steps["provider"].changes == []
    assert "already connection 1" in second.steps["provider"].detail
    assert second.steps["admin"].changes == []


def test_a_wrong_password_on_an_existing_account_refuses_and_never_resets(tmp_path: Path) -> None:
    instance = _instance(onboarding=False)
    before = (copy.deepcopy(instance.users), dict(instance.passwords))
    env = _env(**{ENV_ADMIN_PASSWORD: "not-the-password", ENV_ADMIN_EMAIL: "existing@example.invalid"})

    report, _ = _run(instance, env, tmp_path)

    assert not report.ok and report.failed_step == "admin"
    assert report.error["data"]["reason"] == "admin_credential_rejected"
    assert "never resets" in report.error["message"]
    assert ENV_ADMIN_PASSWORD in report.error["fix_hint"]
    assert (instance.users, instance.passwords) == before
    assert _posts(instance, "/api/v1/auths/signup") == []
    assert [report.steps[s].status for s in STEPS[3:]] == ["not_attempted"] * (len(STEPS) - 3)
    assert "re-run" in report.next_action and DEPLOYMENT in report.next_action


def test_a_replaced_provider_key_is_a_reported_change_and_other_entries_survive(tmp_path: Path) -> None:
    instance = _instance()
    instance.provider_config = {
        "ENABLE_OPENAI_API": False,
        "OPENAI_API_BASE_URLS": ["https://foreign.example/v1", PROVIDER_URL + "/"],
        "OPENAI_API_KEYS": ["foreign-key", "old-provider-key"],
        "OPENAI_API_CONFIGS": {"0": {"enable": True, "prefix_id": "f"}, "1": {"enable": False}},
    }

    report, _ = _run(instance, _env(), tmp_path)

    assert report.ok, report.to_dict()
    config = instance.provider_config
    assert config["OPENAI_API_BASE_URLS"] == ["https://foreign.example/v1", PROVIDER_URL]
    assert config["OPENAI_API_KEYS"] == ["foreign-key", PROVIDER_KEY]
    assert config["OPENAI_API_CONFIGS"]["0"] == {"enable": True, "prefix_id": "f"}
    assert config["OPENAI_API_CONFIGS"]["1"] == {"enable": True}
    assert config["ENABLE_OPENAI_API"] is True
    changes = "\n".join(report.steps["provider"].changes)
    assert "replaced" in changes and PROVIDER_KEY_ENV in changes
    assert "enabled connection 1" in changes
    assert "old-provider-key" not in _rendered(report)


def test_two_entries_for_the_declared_url_refuse_rather_than_guess(tmp_path: Path) -> None:
    instance = _instance()
    instance.provider_config["OPENAI_API_BASE_URLS"] = [PROVIDER_URL, PROVIDER_URL.replace("host.", "HOST.") + "/"]
    instance.provider_config["OPENAI_API_KEYS"] = ["a", "b"]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.failed_step == "provider"
    assert report.error["data"]["reason"] == "provider_entry_ambiguous"
    assert _posts(instance, "/openai/config/update") == []


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------

def test_an_operator_token_takes_precedence_and_neither_signs_up_nor_in(tmp_path: Path) -> None:
    instance = _instance(onboarding=False)
    instance.api_keys["u-1"] = ENV_TOKEN

    report, _ = _run(instance, _env(**{ENV_ADMIN_TOKEN: ENV_TOKEN}), tmp_path)

    assert report.ok, report.to_dict()
    assert report.credential_source == "env"
    assert _posts(instance, "/api/v1/auths/signup") == []
    assert _posts(instance, "/api/v1/auths/signin") == []


def test_a_rejected_operator_token_refuses_without_creating_anything(tmp_path: Path) -> None:
    instance = _instance()

    report, seams = _run(instance, _env(**{ENV_ADMIN_TOKEN: "sk-not-a-credential-0123456789"}), tmp_path)

    assert report.failed_step == "admin"
    assert report.error["data"]["reason"] == "admin_credential_rejected"
    assert report.error["data"]["source"] == ENV_ADMIN_TOKEN
    assert instance.users == []
    assert seams.sync_calls == []


def test_a_missing_password_on_an_empty_instance_names_the_variable(tmp_path: Path) -> None:
    instance = _instance()
    env = _env()
    del env[ENV_ADMIN_PASSWORD]

    report, _ = _run(instance, env, tmp_path)

    assert report.failed_step == "admin"
    assert report.error["data"] == {
        "reason": "admin_credential_missing", "step": "admin",
        "instance_has_accounts": False, "email": default_admin_email(DEPLOYMENT),
    }
    assert ENV_ADMIN_PASSWORD in report.error["fix_hint"]
    assert instance.users == []


def test_a_missing_credential_on_a_populated_instance_names_all_three_ways(tmp_path: Path) -> None:
    instance = _instance(onboarding=False)
    env = _env()
    del env[ENV_ADMIN_PASSWORD]

    report, _ = _run(instance, env, tmp_path)

    assert report.error["data"]["reason"] == "admin_credential_missing"
    assert report.error["data"]["instance_has_accounts"] is True
    for name in (ENV_ADMIN_TOKEN, ENV_ADMIN_EMAIL, ENV_ADMIN_PASSWORD):
        assert name in report.error["fix_hint"]


def test_a_non_admin_credential_is_refused(tmp_path: Path) -> None:
    instance = _instance(onboarding=False)
    instance.users[0]["role"] = "user"
    instance.api_keys["u-1"] = ENV_TOKEN

    report, _ = _run(instance, _env(**{ENV_ADMIN_TOKEN: ENV_TOKEN}), tmp_path)

    assert report.error["data"]["reason"] == "admin_credential_not_admin"


# ---------------------------------------------------------------------------
# Provider and model readiness
# ---------------------------------------------------------------------------

def test_an_unset_provider_key_variable_refuses_by_name_before_dialling(tmp_path: Path) -> None:
    instance = _instance()
    env = _env()
    del env[PROVIDER_KEY_ENV]

    report, _ = _run(instance, env, tmp_path)

    assert report.failed_step == "provider"
    assert report.error["data"]["reason"] == "provider_credential_unresolved"
    assert report.error["data"]["api_key_env"] == PROVIDER_KEY_ENV
    assert f"export {PROVIDER_KEY_ENV}=" in report.error["fix_hint"]
    assert _posts(instance, "/openai/verify") == []
    assert _posts(instance, "/openai/config/update") == []


def test_a_rejected_provider_key_is_an_actionable_failure(tmp_path: Path) -> None:
    instance = _instance()
    instance.provider_endpoints[(PROVIDER_URL, PROVIDER_KEY)] = 401

    report, _ = _run(instance, _env(), tmp_path)

    assert report.error["data"]["reason"] == "provider_credential_rejected"
    assert report.error["data"]["http_status"] == 401
    assert PROVIDER_KEY_ENV in report.error["fix_hint"]
    assert _posts(instance, "/openai/config/update") == []


def test_a_rejected_provider_key_echoed_in_the_body_is_scrubbed(tmp_path: Path) -> None:
    """The vendor's 401 body reflects the submitted key; nothing renders it."""
    instance = _instance()
    instance.provider_endpoints[(PROVIDER_URL, PROVIDER_KEY)] = 401
    instance.verify_echoes_key = True

    report, _ = _run(instance, _env(), tmp_path)

    assert report.error["data"]["reason"] == "provider_credential_rejected"
    assert PROVIDER_KEY not in _rendered(report)
    assert PROVIDER_KEY not in json.dumps(report.error)


def test_an_unreachable_provider_is_named_as_such_not_as_a_bad_key(tmp_path: Path) -> None:
    instance = _instance()
    del instance.provider_endpoints[(PROVIDER_URL, PROVIDER_KEY)]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.error["data"]["reason"] == "provider_unreachable"
    assert "host.docker.internal" in report.error["fix_hint"]


def test_a_keyless_local_provider_is_a_declared_configuration(tmp_path: Path) -> None:
    instance = _instance()
    instance.provider_endpoints[(PROVIDER_URL, "")] = [DEFAULT_MODEL]
    chat = _chat(provider={"base_url": PROVIDER_URL})

    report, _ = _run(instance, _env(), tmp_path, chat=chat)

    assert report.ok, report.to_dict()
    assert report.steps["provider"].data["credential"] == "none"
    assert instance.provider_config["OPENAI_API_KEYS"][-1] == ""


def test_an_undeclared_provider_is_skipped_and_the_model_is_still_checked(tmp_path: Path) -> None:
    instance = _instance()
    instance.served_models = ["other/model"]

    report, _ = _run(instance, _env(), tmp_path, chat=_chat(provider=None))

    assert report.steps["provider"].status == "skipped"
    assert report.failed_step == "model"
    assert report.error["data"]["reason"] == "provider_model_missing"
    assert report.error["data"]["served"] == ["other/model"]
    assert "chat.provider" in report.error["fix_hint"]


# ---------------------------------------------------------------------------
# The graph endpoint
# ---------------------------------------------------------------------------

def test_a_dead_graph_endpoint_is_started_awaited_and_recorded(tmp_path: Path) -> None:
    instance = _instance()
    seams = Seams(mcp_up=False)

    report, seams = _run(instance, _env(), tmp_path, seams=seams)

    assert report.ok, report.to_dict()
    (argv, log_path), = seams.launched
    assert argv == mcp_serve_argv(
        DEPLOYMENT, port=8101, host="127.0.0.1", auth_token_env=MCP_TOKEN_ENV,
        allowed_hosts=["127.0.0.1:*", "host.docker.internal:*"],
    )
    # mcp-serve's DNS-rebinding guard must be told BOTH the host this process
    # dials and the gateway alias the container dials (measured: 421/400).
    allowed = [argv[i + 1] for i, a in enumerate(argv) if a == "--allowed-host"]
    assert allowed == ["127.0.0.1:*", "host.docker.internal:*"]
    assert "--auth-token-env" in argv and MCP_TOKEN not in " ".join(argv)
    assert log_path == tmp_path / "state" / DEPLOYMENT / "mcp-serve.log"
    record = json.loads((tmp_path / "state" / DEPLOYMENT / "mcp-serve.json").read_text())
    assert record["pid"] == 4242 and record["endpoint"] == "http://127.0.0.1:8101/mcp"
    assert report.steps["graph"].data["started"] is True
    # Issue #2301: the running okg, never the first `okg` on PATH.
    assert argv[:4] == running_okg_argv()
    started = f"started `{' '.join(running_okg_argv())} mcp-serve"
    assert any(started in c for c in report.steps["graph"].changes)
    assert len(seams.probed) >= 2


def test_an_answering_graph_endpoint_is_not_started_again(tmp_path: Path) -> None:
    report, seams = _run(_instance(), _env(), tmp_path)

    assert report.ok
    assert seams.launched == []
    assert report.steps["graph"].data["started"] is False


def test_a_dead_endpoint_refuses_naming_the_command_when_starting_is_off(tmp_path: Path) -> None:
    seams = Seams(mcp_up=False)

    report, seams = _run(
        _instance(), _env(), tmp_path, seams=seams,
        options=SetupOptions(start_mcp_serve=False),
    )

    assert report.failed_step == "graph"
    assert report.error["data"]["reason"] == "mcp_endpoint_dead"
    assert (
        f"{' '.join(running_okg_argv())} mcp-serve --deployment fixture-deployment"
        in report.error["fix_hint"]
    )
    assert seams.launched == []


def test_an_unset_graph_credential_refuses_before_starting_unauthenticated(tmp_path: Path) -> None:
    env = _env()
    del env[MCP_TOKEN_ENV]
    seams = Seams(mcp_up=False)

    report, seams = _run(_instance(), env, tmp_path, seams=seams)

    assert report.error["data"]["reason"] == "mcp_credential_unresolved"
    assert report.error["data"]["auth_token_env"] == MCP_TOKEN_ENV
    assert seams.launched == []


def test_a_started_endpoint_that_never_answers_names_its_log(tmp_path: Path) -> None:
    seams = Seams(mcp_up=False)
    seams.launch = lambda argv, log_path: 7  # type: ignore[assignment]

    report, _ = _run(
        _instance(), _env(), tmp_path, seams=seams,
        options=SetupOptions(mcp_ready_timeout=0.0),
    )

    assert report.error["data"]["reason"] == "mcp_serve_not_answering"
    assert report.error["data"]["log"].endswith("mcp-serve.log")


# ---------------------------------------------------------------------------
# Convergence is not readiness
# ---------------------------------------------------------------------------

def test_a_reply_without_a_tool_source_is_converged_but_not_ready(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_tool_runs = []

    report, _ = _run(instance, _env(), tmp_path)

    assert report.converged is True
    assert report.conversation_ready is False
    assert report.ok is False
    assert report.failed_step == "conversation"
    assert report.conversation["reason"] == REASON_TOOL_USE_UNOBSERVED
    assert report.conversation["replied"] is True
    assert report.error["data"]["reason"] == REASON_TOOL_USE_UNOBSERVED
    text = render_human(report)
    assert "configuration converged: yes" in text
    assert "conversation ready: no" in text


# ---------------------------------------------------------------------------
# chat.provider.tools: provider -- a provider that brings its own tools
# ---------------------------------------------------------------------------

PROVIDER_TOOLS_CHAT = {"base_url": PROVIDER_URL, "api_key_env": PROVIDER_KEY_ENV,
                       "tools": "provider"}


def _provider_record(*statuses: str) -> dict[str, Any]:
    return {PROVIDER_TOOL_RECORD_KEY: {
        "sandbox": "read-only",
        "tool_calls": [{"server": "okg", "tool": f"t{i}", "status": s}
                       for i, s in enumerate(statuses)],
    }}


def test_provider_tools_ready_on_an_answered_provider_call(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_tool_runs = []  # the site's tool loop never ran
    instance.completion_extra = _provider_record("refused", "completed")

    report, _ = _run(instance, _env(), tmp_path, chat=_chat(provider=PROVIDER_TOOLS_CHAT))

    assert report.ok is True
    assert report.conversation_ready is True
    assert report.conversation["tool_evidence"] == TOOL_EVIDENCE_PROVIDER
    assert report.conversation["tools_invoked"] == ["t1"]
    # The provider's record rides only on a plain, non-streamed completion.
    (completion,) = _posts(instance, "/api/chat/completions")
    assert completion["stream"] is False and "chat_id" not in completion
    assert "provider-reported" in report.steps["conversation"].detail


def test_provider_tools_not_ready_when_every_provider_call_failed(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_tool_runs = []
    instance.completion_extra = _provider_record("failed", "refused")

    report, _ = _run(instance, _env(), tmp_path, chat=_chat(provider=PROVIDER_TOOLS_CHAT))

    assert report.converged is True
    assert report.conversation_ready is False
    assert report.conversation["reason"] == REASON_PROVIDER_TOOLS_FAILED
    assert report.error["data"]["reason"] == REASON_PROVIDER_TOOLS_FAILED
    assert "t0=failed" in report.error["message"]


def test_provider_tools_not_ready_without_a_provider_record(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_tool_runs = []

    report, _ = _run(instance, _env(), tmp_path, chat=_chat(provider=PROVIDER_TOOLS_CHAT))

    assert report.conversation_ready is False
    assert report.conversation["reason"] == REASON_TOOL_USE_UNOBSERVED
    assert report.conversation["tool_evidence"] == TOOL_EVIDENCE_PROVIDER


def test_instance_tools_never_trust_a_provider_record(tmp_path: Path) -> None:
    """Without the declaration, a provider's self-report is not evidence."""
    instance = _instance()
    instance.completion_tool_runs = []
    instance.completion_extra = _provider_record("completed")

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is False
    assert report.conversation["reason"] == REASON_TOOL_USE_UNOBSERVED
    assert report.conversation["tool_evidence"] == TOOL_EVIDENCE_INSTANCE


def test_a_tool_that_is_not_a_graph_tool_does_not_count(tmp_path: Path) -> None:
    """The vendor's own tools (knowledge-base search) are not the graph."""
    instance = _instance()
    instance.completion_tool_runs = [
        {"name": "query_knowledge_files", "tool": "x", "result": '{"hits": []}'},
    ]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is False
    assert report.conversation["reason"] == REASON_TOOL_USE_UNOBSERVED
    assert report.conversation["tool_call_count"] == 0


def test_graph_calls_that_never_answered_are_not_ready(tmp_path: Path) -> None:
    """A call the vendor could not complete records its failure as text,
    not a graph answer (``middleware.py:4969``): the graph was not consulted."""
    instance = _instance()
    instance.completion_tool_runs = [
        {"tool": "search", "result": 'Error: Tool "okg-fixture-deployment_search" not found.'},
        {"tool": "inspect", "result": "All connection attempts failed"},
    ]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is False
    assert report.conversation["reason"] == REASON_GRAPH_TOOLS_FAILED
    assert report.conversation["tool_call_count"] == 2
    assert report.conversation["tools_invoked"] == []
    assert report.error["data"]["reason"] == REASON_GRAPH_TOOLS_FAILED
    assert f"{PRESET_ID}_inspect" in report.error["message"]


#: What okg returns for a refused call, in the two forms Open WebUI 0.11
#: records it (#2911 review): JSON when the refusal is an ordinary result,
#: and a Python dict literal when the MCP result carries ``isError`` (the
#: vendor raises ``Exception(<dict>)`` and stores ``str(e)``).
REFUSED_JSON = '{"status": "refused", "error": {"code": "semantic_search_unavailable"}}'
REFUSED_IS_ERROR = (
    "{'status': 'refused', 'operator_name': 'inspect', "
    "'error': {'code': 'invalid_operator_arguments'}, "
    "'session_default_generation_id': 'okg:x:gen:1'}"
)


def test_a_graph_that_refuses_every_call_is_not_ready(tmp_path: Path) -> None:
    """Reaching the graph is not consulting it: a refusal answers nothing."""
    instance = _instance()
    instance.completion_tool_runs = [
        {"tool": "search", "result": REFUSED_JSON},
        {"tool": "inspect", "result": REFUSED_IS_ERROR},
    ]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is False
    assert report.conversation["reason"] == REASON_GRAPH_TOOLS_REFUSED
    assert report.conversation["tool_refusal_count"] == 2
    assert report.conversation["tool_result_count"] == 0
    assert report.conversation["tools_invoked"] == []
    # A generation id inside a refusal is not evidence of a read.
    assert report.conversation["generation_evidence"] is False
    assert report.conversation["refusal_codes"] == [
        "invalid_operator_arguments", "semantic_search_unavailable",
    ]
    assert report.error["data"]["reason"] == REASON_GRAPH_TOOLS_REFUSED
    assert "semantic_search_unavailable" in report.error["message"]


def test_one_graph_answer_among_refusals_is_ready(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_tool_runs = [
        {"tool": "inspect", "result": REFUSED_IS_ERROR},
        copy.deepcopy(TOOL_RUN),
    ]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is True
    assert report.conversation["tools_invoked"] == [GRAPH_SEARCH]
    assert report.conversation["tool_refusal_count"] == 1
    assert report.conversation["generation_evidence"] is True
    assert "1 other call(s) refused" in report.steps["conversation"].detail


def test_a_completion_that_times_out_is_judged_once_the_stored_turn_finishes(
    tmp_path: Path,
) -> None:
    """The tool loop runs inside the completion request; a slow turn outlives
    the client's timeout while the site keeps going. Setup reads the stored
    turn until it is done instead of reporting a dead instance."""
    instance = _instance()
    instance.completion_times_out = True
    instance.turn_finishes_after_reads = 3

    report, _ = _run(instance, _env(), tmp_path)

    assert report.ok, report.to_dict()
    assert report.conversation["tools_invoked"] == [GRAPH_SEARCH]
    reads = [r for r in instance.requests if r[0] == "GET" and r[1].startswith("/api/v1/chats/")]
    assert len(reads) == 3


def test_a_turn_that_never_finishes_is_a_timeout_not_a_dead_model(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_times_out = True
    instance.turn_finishes_after_reads = None

    report, _ = _run(
        instance, _env(), tmp_path, options=SetupOptions(completion_timeout=20.0),
    )

    assert report.converged is True and report.conversation_ready is False
    data = report.error["data"]
    assert data["reason"] == "probe_turn_timeout"
    assert data["completion_timed_out"] is True
    assert data["waited_s"] == 20
    reads = [r for r in instance.requests if r[0] == "GET" and r[1].startswith("/api/v1/chats/")]
    assert len(reads) == 5  # one read, then four more five seconds apart


def test_a_failed_readback_is_its_own_reason(tmp_path: Path) -> None:
    instance = _instance()
    route = instance._route

    def broken_readback(method: str, path: str, query: Any, payload: Any) -> Any:
        if method == "GET" and path.startswith("/api/v1/chats/"):
            return 500, {"detail": "Internal Server Error"}
        return route(method, path, query, payload)

    instance._route = broken_readback  # type: ignore[method-assign]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is False
    assert report.error["data"]["reason"] == "probe_readback_failed"
    assert report.error["data"]["http_status"] == 500
    assert report.error["data"]["chat_id"] == report_chat_id(instance)


def test_a_refused_completion_request_is_a_model_reply_failure(tmp_path: Path) -> None:
    instance = _instance()
    instance.fail_paths.add("/api/chat/completions")

    report, _ = _run(instance, _env(), tmp_path)

    assert report.conversation_ready is False
    assert report.error["data"]["reason"] == "model_reply_failed"
    assert report.error["data"]["http_status"] == 422


def test_a_failed_model_reply_is_actionable_and_leaves_convergence_intact(tmp_path: Path) -> None:
    instance = _instance()
    instance.completion_status = 500

    report, _ = _run(instance, _env(), tmp_path)

    assert report.converged is True and report.conversation_ready is False
    # The vendor stores a provider error on the assistant message and
    # answers 200 (``main.py:1586``); setup reads it from there.
    assert report.error["data"]["reason"] == "model_reply_failed"
    assert report.error["data"]["detail"] == "upstream error"
    assert "provider key" in report.error["fix_hint"]


def test_a_chat_the_instance_refuses_to_create_stops_before_the_question(tmp_path: Path) -> None:
    instance = _instance()
    instance.fail_paths.add("/api/v1/chats/new")

    report, _ = _run(instance, _env(), tmp_path)

    assert report.converged is True and report.conversation_ready is False
    assert report.error["data"]["reason"] == "probe_chat_not_created"
    assert report.error["data"]["http_status"] == 422
    assert _posts(instance, "/api/chat/completions") == []


def test_a_failed_sync_is_not_converged_and_the_conversation_is_not_attempted(tmp_path: Path) -> None:
    instance = _instance()

    report, _ = _run(instance, _env(), tmp_path, seams=Seams(sync_ok=False))

    assert report.converged is False and report.failed_step == "sync"
    assert report.steps["conversation"].status == "not_attempted"
    assert _posts(instance, "/api/chat/completions") == []


# ---------------------------------------------------------------------------
# Secrets never render
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("arm", ["ready", "rejected_provider", "wrong_password", "not_ready"])
def test_no_secret_reaches_any_rendered_output(tmp_path: Path, arm: str) -> None:
    instance = _instance(onboarding=arm != "wrong_password")
    env = _env(**{ENV_ADMIN_EMAIL: "existing@example.invalid"})
    if arm == "rejected_provider":
        instance.provider_endpoints[(PROVIDER_URL, PROVIDER_KEY)] = 401
    if arm == "wrong_password":
        env[ENV_ADMIN_PASSWORD] = "wrong!pw-9"
    if arm == "not_ready":
        instance.completion_tool_runs = []
        instance.completion_reply = f"I used {PROVIDER_KEY} and {PASSWORD}"

    report, _ = _run(instance, env, tmp_path)

    rendered = _rendered(report)
    sessions = list(instance.sessions)
    assert sessions or arm == "wrong_password"
    for secret in (PASSWORD, PROVIDER_KEY, MCP_TOKEN, "wrong!pw-9", *sessions):
        assert secret not in rendered, (arm, secret)
    # The redaction is by identity: the variable NAMES are allowed to appear.
    assert ENV_ADMIN_PASSWORD in rendered or arm == "ready"


def test_the_signup_and_signin_bodies_carry_the_password_only_on_the_wire(tmp_path: Path) -> None:
    """The fake saw the password (it must — that is the protocol); nothing
    setup returned did."""
    instance = _instance()
    report, _ = _run(instance, _env(), tmp_path)

    (signup,) = _posts(instance, "/api/v1/auths/signup")
    assert signup["password"] == PASSWORD
    assert PASSWORD not in _rendered(report)
    assert PASSWORD not in json.dumps(report.steps["admin"].as_dict())


# ---------------------------------------------------------------------------
# The manifest block
# ---------------------------------------------------------------------------

def test_chat_provider_is_admitted_and_names_only_a_variable() -> None:
    chat = _chat()
    assert chat.provider.base_url == PROVIDER_URL
    assert chat.provider.api_key_env == PROVIDER_KEY_ENV
    assert schema_field_census()["chat.provider"] == frozenset(
        {"base_url", "api_key_env", "tools"}
    )
    assert chat.provider.tools == "instance"
    assert _chat(provider=None).provider is None


def test_chat_provider_tools_accepts_provider_and_refuses_anything_else() -> None:
    chat = _chat(provider={"base_url": PROVIDER_URL, "api_key_env": PROVIDER_KEY_ENV,
                           "tools": "provider"})
    assert chat.provider.tools == "provider"
    with pytest.raises(ChatManifestError) as excinfo:
        _chat(provider={"base_url": PROVIDER_URL, "tools": "codex"})
    assert excinfo.value.data["path"] == "chat.provider.tools"


@pytest.mark.parametrize(
    "block, path",
    [
        ({"base_url": PROVIDER_URL, "api_key": "sk-literal"}, "chat.provider"),
        ({"base_url": "host.docker.internal:11434/v1"}, "chat.provider.base_url"),
        ({"api_key_env": "OKG_KEY"}, "chat.provider.base_url"),
        ({"base_url": PROVIDER_URL, "api_key_env": "sk-pasted-secret"}, "chat.provider.api_key_env"),
    ],
)
def test_chat_provider_refuses_a_literal_key_and_malformed_fields(block: dict[str, Any], path: str) -> None:
    with pytest.raises(ChatManifestError) as excinfo:
        _chat(provider=block)
    assert excinfo.value.data["path"] == path
    assert "sk-pasted-secret" not in str(excinfo.value)


def test_provider_urls_compare_by_normal_form() -> None:
    assert normalize_provider_url("HTTP://Host.Docker.Internal:11434/v1/") == normalize_provider_url(
        "http://host.docker.internal:11434/v1"
    )
    assert normalize_provider_url("http://a/v1") != normalize_provider_url("http://a/v2")


# ---------------------------------------------------------------------------
# The CLI wrapper
# ---------------------------------------------------------------------------

def test_cli_exits_nonzero_and_prints_the_envelope_on_a_stopped_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import argparse

    from okg.substrate.cli.chat import _cmd_chat_setup

    instance = _instance()
    instance.completion_tool_runs = []
    captured: dict[str, Any] = {}

    def fake_run_setup(deployment: str, *, options: SetupOptions) -> Any:
        captured["options"] = options
        report, _ = _run(instance, _env(), tmp_path)
        return report

    monkeypatch.setattr("okg.substrate.chat.setup.run_setup", fake_run_setup)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    args = argparse.Namespace(
        deployment=DEPLOYMENT, app_database_url_env="OKG_CHAT_APP_DATABASE_URL",
        admin_secret_env="OKG_CHAT_WEBUI_SECRET_KEY", instance_port=None, image=None,
        host="127.0.0.1", host_gateway_alias="host.docker.internal", container_runtime="docker",
        container_network="auto",
        ready_timeout=120.0, mcp_probe_url=None, mcp_instance_url=None, mcp_host="127.0.0.1",
        install_plan=None, admin_email=None, question="What changed?", no_start_mcp_serve=True,
        mcp_ready_timeout=60.0, open_browser=False, provider=None,
        provider_base_url=None, provider_key_env=None, model=None, mcp_port=None,
        search_profile=None, search_subtype=None, configure_only=False,
    )
    code = _cmd_chat_setup(args, as_json=True)
    out = capsys.readouterr().out

    assert code == 1
    envelope = json.loads(out)
    assert envelope["surface"] == "chat-setup"
    assert envelope["converged"] is True and envelope["conversation_ready"] is False
    assert captured["options"].start_mcp_serve is False
    assert captured["options"].question == "What changed?"


# ---------------------------------------------------------------------------
# req.chat-provider-url-no-credential.output-masked (#2344): setup never
# prints a provider base URL with a credential in it, even one that reached
# it without the loader (the loader refuses these now; this is the second
# wall).
# ---------------------------------------------------------------------------

URL_SECRET = "hunter2-Sentinel-2344"
#: A fake 64-hex-digit token with no key prefix (#2735).
URL_HEX_TOKEN = hashlib.sha256(b"okg-fake-token-2735").hexdigest()
SECRET_URLS = {
    "userinfo": f"http://okg:{URL_SECRET}@host.docker.internal:11434/v1",
    "query": f"http://host.docker.internal:11434/v1?api_key={URL_SECRET}",
    "fragment": f"http://host.docker.internal:11434/v1#token={URL_SECRET}",
    # #2735: a `;name=value` path parameter, a percent-encoded key prefix
    # (`%73` is `s`), and a bare token with no known prefix.
    "path-param": f"http://host.docker.internal:11434/v1;api_key={URL_SECRET}",
    "encoded-key-prefix": f"http://host.docker.internal:11434/%73k-proj-{URL_SECRET}/v1",
    "hex-token": f"http://host.docker.internal:11434/{URL_HEX_TOKEN}/v1",
}
#: The secret each SECRET_URLS case carries, where it is not URL_SECRET.
SECRET_OF = {"hex-token": URL_HEX_TOKEN}


def _chat_with_raw_provider_url(url: str) -> Any:
    import dataclasses

    from okg.substrate.deployments.chat_block import ChatProvider

    return dataclasses.replace(
        _chat(), provider=ChatProvider(base_url=url, api_key_env=PROVIDER_KEY_ENV),
    )


@pytest.mark.parametrize("case", sorted(SECRET_URLS))
@pytest.mark.parametrize("arm", ["registers", "already-registered", "rewritten", "unreachable", "ambiguous"])
def test_setup_never_prints_a_credential_carried_in_the_provider_url(
    tmp_path: Path, case: str, arm: str,
) -> None:
    url = SECRET_URLS[case]
    secret = SECRET_OF.get(case, URL_SECRET)
    instance = _instance()
    if arm != "unreachable":
        instance.provider_endpoints[(url, PROVIDER_KEY)] = [DEFAULT_MODEL]
    if arm == "already-registered":
        instance.provider_config["OPENAI_API_BASE_URLS"] = [url]
        instance.provider_config["OPENAI_API_KEYS"] = [PROVIDER_KEY]
        instance.provider_config["OPENAI_API_CONFIGS"] = {"0": {"enable": True}}
    elif arm == "rewritten":
        # The same connection, registered under a form that normalizes equal.
        other = url.replace("host.docker.internal", "HOST.docker.internal")
        instance.provider_config["OPENAI_API_BASE_URLS"] = [other]
        instance.provider_config["OPENAI_API_KEYS"] = ["old-provider-key"]
    elif arm == "ambiguous":
        other = url.replace("host.docker.internal", "HOST.docker.internal")
        instance.provider_config["OPENAI_API_BASE_URLS"] = [url, other]
        instance.provider_config["OPENAI_API_KEYS"] = ["a", "b"]

    report, _ = _run(instance, _env(), tmp_path, chat=_chat_with_raw_provider_url(url))

    rendered = _rendered(report)
    assert secret not in rendered, arm
    step = report.steps["provider"]
    if arm in ("unreachable", "ambiguous"):
        assert report.failed_step == "provider"
        assert report.error["data"]["base_url"].startswith("http://")
        assert "<withheld>" in report.error["data"]["base_url"]
    else:
        assert step.status == "ok", report.to_dict()
        assert "<withheld>" in step.data["base_url"]
        assert "<withheld>" in step.detail
        # The instance got the real URL: masking is for output only.
        assert url in instance.provider_config["OPENAI_API_BASE_URLS"]


def test_setup_prints_a_clean_provider_url_unchanged(tmp_path: Path) -> None:
    report, _ = _run(_instance(), _env(), tmp_path)

    step = report.steps["provider"]
    assert step.data["base_url"] == PROVIDER_URL
    assert any(PROVIDER_URL in change for change in step.changes)
    assert "<withheld>" not in _rendered(report)


def test_a_vendor_error_that_echoes_the_registered_url_is_scrubbed(tmp_path: Path) -> None:
    """The URL already on the instance is scrubbed too, not just the declared one.

    The registered form differs from the clean declared URL only by a
    fragment carrying a sentinel, which the comparison ignores, so setup
    rewrites that connection; the vendor refuses the write with a body that
    echoes the connection it holds.
    """
    # Short and punctuated, so the shape pass in `redact` cannot be what
    # hides it; only scrubbing the registered URL's pieces by identity can.
    fragment_secret = "fr!Sent-4"
    registered = f"{PROVIDER_URL}#{fragment_secret}"
    instance = _instance()
    instance.provider_config["OPENAI_API_BASE_URLS"] = [registered]
    instance.provider_config["OPENAI_API_KEYS"] = [PROVIDER_KEY]
    instance.provider_config["OPENAI_API_CONFIGS"] = {"0": {"enable": True}}
    route = instance._route

    def echoing_route(method: str, path: str, query: Any, payload: Any) -> Any:
        if path == "/openai/config/update" and method == "POST":
            return 500, {"detail": f"cannot save; the instance holds {registered}"}
        return route(method, path, query, payload)

    instance._route = echoing_route  # type: ignore[method-assign]

    report, _ = _run(instance, _env(), tmp_path)

    assert report.failed_step == "provider", report.to_dict()
    rendered = _rendered(report)
    assert "cannot save" in rendered
    assert fragment_secret not in rendered


# ---------------------------------------------------------------------------
# Issue #2874: the vendor's cached model list predates the preset
# ---------------------------------------------------------------------------

def _cache_models_like_the_vendor(instance: FakeInstance) -> None:
    """Model Open WebUI 0.11.0's model cache on the fake.

    ``GET /api/models`` rebuilds the cache from what the instance serves
    (``utils/models.py:146``, ``:426``); a completion loads it only while it
    is empty and answers 400 ``Model not found`` for an id it does not hold
    (``main.py:1059``, ``:1070``).
    """
    state: dict[str, Any] = {"cache": None}
    route = instance._route

    def cached_route(method: str, path: str, query: Any, payload: Any) -> Any:
        if path == "/api/models" and method == "GET":
            state["cache"] = set(instance.served_models)
        if path == "/api/chat/completions" and method == "POST":
            if not state["cache"]:
                state["cache"] = set(instance.served_models)
            if payload.get("model") not in state["cache"]:
                return 400, {"detail": "Model not found"}
        return route(method, path, query, payload)

    instance._route = cached_route  # type: ignore[method-assign]


def test_first_run_reloads_the_model_list_before_the_test_question(tmp_path: Path) -> None:
    """Fail-before: the model step loaded the cache before sync created the
    preset, so the first run's test question answered 'Model not found'."""
    instance = _instance()
    _cache_models_like_the_vendor(instance)

    report, _ = _run(instance, _env(), tmp_path)

    assert report.ok, report.to_dict()
    completion = instance.requests.index(("POST", "/api/chat/completions"))
    assert instance.requests[completion - 2 : completion] == [
        ("GET", "/api/models"), ("POST", "/api/v1/chats/new"),
    ]
    assert any(
        "reloaded the instance's model list" in note
        for note in report.steps["sync"].notes
    )


def test_an_empty_reply_reports_its_shape_but_no_content(tmp_path: Path) -> None:
    """An empty reply says what its stored turn still recorded (item types,
    tool calls, a reasoning item) as counts and booleans, never text."""
    thought = "reasoning-sentinel-7"
    instance = _instance()
    instance.completion_reply = None
    route = instance._route

    def thinking_route(method: str, path: str, query: Any, payload: Any) -> Any:
        status, body = route(method, path, query, payload)
        if path == "/api/chat/completions" and method == "POST":
            message = instance.chats[payload["chat_id"]]["chat"]["history"]["messages"][
                payload["id"]
            ]
            message["output"].insert(0, {
                "type": "reasoning", "status": "completed",
                "content": [{"type": "output_text", "text": thought}],
            })
        return status, body

    instance._route = thinking_route  # type: ignore[method-assign]

    report, _ = _run(instance, _env(), tmp_path)

    data = report.error["data"]
    assert data["reason"] == "model_reply_empty"
    assert data["turn_finished"] is True
    assert data["output_item_types"] == ["function_call", "function_call_output", "reasoning"]
    assert data["tool_call_count"] == 1 and data["tool_result_count"] == 1
    assert data["reasoning_present"] is True
    assert thought not in _rendered(report)


class _NoPresetSeams(Seams):
    """A sync that reports success but leaves no preset the instance serves."""

    def run_sync(self, inputs: Any, client: Any) -> Any:
        instance, self.instance = self.instance, None
        try:
            return super().run_sync(inputs, client)
        finally:
            self.instance = instance


def test_a_preset_the_reloaded_list_does_not_serve_refuses_before_the_question(
    tmp_path: Path,
) -> None:
    instance = _instance()

    report, _ = _run(instance, _env(), tmp_path, seams=_NoPresetSeams())

    assert report.converged is True
    assert report.failed_step == "conversation"
    assert report.error["data"]["reason"] == "preset_not_served"
    assert report.error["data"]["preset_id"] == PRESET_ID
    assert ("POST", "/api/chat/completions") not in instance.requests


# ---------------------------------------------------------------------------
# Issue #2876 item 1: a plain `okg chat sync` keeps setup's graph URL
# ---------------------------------------------------------------------------

def _sync_inputs(tmp_path: Path, probe: str) -> Any:
    from okg.substrate.chat.sync import SyncInputs

    return SyncInputs(
        deployment=DEPLOYMENT, deployment_dir=tmp_path, raw_manifest={},
        chat=_chat(), mcp_instructions_path=None, chat_data_dir=None,
        mcp_probe_url=probe, mcp_instance_url=probe, preset_id=PRESET_ID,
        mcp_server_id=PRESET_ID,
    )


#: The container instance's own address, as `stand_up` reports it.
CONTAINER_URL = "http://127.0.0.1:3000"


class _ContainerSeams(Seams):
    """`stand_up` answers the container's own host port and records it, as
    the real `stand_up` does."""

    def __init__(self, state_root: Path) -> None:
        super().__init__()
        self.state_root = state_root

    def stand_up(self, deployment: str, **kwargs: Any) -> Any:
        _record_container(self.state_root)
        return SimpleNamespace(
            **{**vars(super().stand_up(deployment, **kwargs)), "base_url": CONTAINER_URL},
        )


def _record_container(state_root: Path, *, host: str = "127.0.0.1") -> None:
    from okg.substrate.chat.instance import InstanceRecord, normalize_dsn, record_instance

    record_instance(
        InstanceRecord(
            deployment=DEPLOYMENT, container=f"okg-chat-{DEPLOYMENT}",
            host=host, port=3000, image="img",
            app_database=normalize_dsn(APP_DSN, label="app"),
            port_source="operator_flag", recorded_at="2026-09-28T00:00:00Z",
        ),
        state_root=state_root,
    )


def _plain_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe: str,
    instance_url: str = CONTAINER_URL,
) -> tuple[int, dict[str, Any]]:
    """Drive `okg chat sync --json` against `instance_url` with no
    --mcp-instance-url; record what it would register and whether it
    reached the instance at all."""
    import argparse

    from okg.substrate.chat import sync as sync_module
    from okg.substrate.cli import chat as cli_chat

    seen: dict[str, Any] = {"clients": 0, "synced": []}

    def client(*_a: Any, **_k: Any) -> Any:
        seen["clients"] += 1
        return object()

    def fake_sync(inputs: Any, _client: Any, **_k: Any) -> Any:
        seen["synced"].append(inputs.mcp_instance_url)
        return SimpleNamespace(ok=True, to_dict=lambda: {"ok": True})

    monkeypatch.setattr(
        sync_module, "resolve_operator_environment", lambda: (instance_url, "t"),
    )
    monkeypatch.setattr(
        sync_module, "load_inputs", lambda *a, **k: _sync_inputs(tmp_path, probe),
    )
    monkeypatch.setattr(sync_module, "InstanceClient", client)
    monkeypatch.setattr(sync_module, "sync", fake_sync)
    args = argparse.Namespace(
        deployment=DEPLOYMENT, mcp_probe_url=None, mcp_instance_url=None,
        mcp_host="127.0.0.1",
    )
    return cli_chat._cmd_sync(args, as_json=True), seen


def test_plain_sync_after_setup_reuses_the_graph_url_setup_registered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail-before: plain sync re-registered 127.0.0.1, which the container
    cannot reach, and broke the graph connection setup made."""
    report, probe, registered = _setup_with_container(tmp_path, monkeypatch)
    record_path = tmp_path / "home" / "chat-instances" / DEPLOYMENT / "graph-endpoint.json"
    assert json.loads(record_path.read_text())["instance_base_url"] == CONTAINER_URL

    code, seen = _plain_sync(tmp_path, monkeypatch, probe)

    assert code == 0
    assert seen["synced"] == [registered]


def _setup_with_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, str, str]:
    """A setup run whose instance is a recorded container at CONTAINER_URL."""
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    state_root = tmp_path / "home" / "chat-instances"
    report, seams = _run(
        _instance(), _env(), tmp_path, state_root=state_root,
        seams=_ContainerSeams(state_root),
    )
    assert report.ok, report.to_dict()
    probe = report.steps["graph"].data["endpoint"]
    registered = seams.sync_calls[0][0].mcp_instance_url
    assert probe == "http://127.0.0.1:8101/mcp"
    assert registered == "http://host.docker.internal:8101/mcp"
    return report, probe, registered


def test_a_recorded_url_is_not_reused_after_the_container_is_gone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail-before (at 6a9b62dcbd): the container is torn down and sync points
    at an Open WebUI on the host; the saved container-only URL was reused."""
    from okg.substrate.chat.instance import forget_instance

    _report, probe, _registered = _setup_with_container(tmp_path, monkeypatch)
    state_root = tmp_path / "home" / "chat-instances"
    forget_instance(DEPLOYMENT, state_root=state_root)
    assert not (state_root / DEPLOYMENT / "graph-endpoint.json").exists()

    code, seen = _plain_sync(tmp_path, monkeypatch, probe, "http://127.0.0.1:8080")

    assert code == 0
    assert seen["synced"] == [probe]


def test_another_address_with_a_loopback_probe_refuses_naming_both(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Fail-before: at 6a9b62dcbd the container's saved URL was reused for
    another instance, and at fc76534710 the loopback probe URL was registered
    silently. A front door or LAN name may be the same container, so neither
    is safe: refuse, naming both URLs, before contacting the instance."""
    _report, probe, _registered = _setup_with_container(tmp_path, monkeypatch)
    capsys.readouterr()

    code, seen = _plain_sync(tmp_path, monkeypatch, probe, "https://chat.example.invalid")

    assert code != 0
    assert seen == {"clients": 0, "synced": []}
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["data"]["reason"] == "mcp_instance_url_unresolved"
    assert error["data"]["instance_url"] == "https://chat.example.invalid"
    assert error["data"]["container_url"] == CONTAINER_URL
    assert "--mcp-instance-url" in error["fix_hint"]
    # The container-side URL setup saved, ready to copy (fail-before at
    # eb03327893: the hint did not carry it).
    assert "http://host.docker.internal:8101/mcp" in error["fix_hint"]


def test_another_instance_with_a_non_loopback_probe_uses_the_probe_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: a probe URL any instance can reach is registered as is."""
    _setup_with_container(tmp_path, monkeypatch)
    probe = "http://graph.lan.example.invalid:8101/mcp"

    code, seen = _plain_sync(tmp_path, monkeypatch, probe, "https://chat.example.invalid")

    assert code == 0
    assert seen["synced"] == [probe]


@pytest.mark.parametrize(
    "host, base_url",
    [("0.0.0.0", "http://0.0.0.0:3000"), ("::", "http://[::]:3000")],
    ids=["ipv4-wildcard", "ipv6-wildcard"],
)
def test_a_wildcard_published_container_matches_its_loopback_address(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, host: str, base_url: str,
) -> None:
    """Fail-before: at fc76534710 a container recorded on 0.0.0.0 did not
    match 127.0.0.1 and the saved URL was not reused; at eb03327893 a
    container recorded on :: built `http://:::3000`, which does not parse."""
    from okg.substrate.chat.setup import record_graph_endpoint

    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    state_root = tmp_path / "home" / "chat-instances"
    _record_container(state_root, host=host)
    probe = "http://127.0.0.1:8101/mcp"
    saved = "http://host.docker.internal:8101/mcp"
    record_graph_endpoint(
        DEPLOYMENT, probe_url=probe, instance_url=saved,
        instance_base_url=base_url, state_root=state_root,
    )

    code, seen = _plain_sync(tmp_path, monkeypatch, probe, "http://127.0.0.1:3000/")

    assert code == 0
    assert seen["synced"] == [saved]


def test_an_unreadable_saved_record_keeps_the_address_mismatch_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Fail-before (at da399f22c4): the refusal became
    graph_endpoint_record_unreadable, hiding the address mismatch."""
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    state_root = tmp_path / "home" / "chat-instances"
    _record_container(state_root)
    (state_root / DEPLOYMENT / "graph-endpoint.json").write_text("{not json")

    code, seen = _plain_sync(
        tmp_path, monkeypatch, "http://127.0.0.1:8101/mcp", "https://chat.example.invalid",
    )

    assert code != 0
    assert seen == {"clients": 0, "synced": []}
    data = json.loads(capsys.readouterr().err)["error"]["data"]
    assert data["reason"] == "mcp_instance_url_unresolved"
    assert data["saved_record_unreadable"] is True
    assert data["saved_instance_url"] is None


def test_an_ipv6_instance_base_url_is_bracketed() -> None:
    from okg.substrate.chat.instance import instance_base_url

    assert instance_base_url("::", 3000) == "http://[::]:3000"
    assert instance_base_url("127.0.0.1", 3000) == "http://127.0.0.1:3000"


def test_plain_sync_without_a_recorded_url_refuses_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """No record, a loopback probe URL and sync aimed at the container: refuse,
    and never reach the instance."""
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    _record_container(tmp_path / "home" / "chat-instances")

    code, seen = _plain_sync(tmp_path, monkeypatch, "http://127.0.0.1:8101/mcp")

    assert code != 0
    assert seen == {"clients": 0, "synced": []}
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["data"]["reason"] == "mcp_instance_url_unresolved"
    assert "--mcp-instance-url" in error["fix_hint"]
    assert "Linux" in error["fix_hint"]


def test_plain_sync_with_no_container_instance_keeps_the_probe_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: an instance on the host itself still gets the probe URL."""
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))

    code, seen = _plain_sync(tmp_path, monkeypatch, "http://127.0.0.1:8101/mcp")

    assert code == 0
    assert seen["synced"] == ["http://127.0.0.1:8101/mcp"]
