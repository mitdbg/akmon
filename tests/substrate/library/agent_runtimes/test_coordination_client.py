from __future__ import annotations

import json
import os
import getpass
import shlex
import socket
import subprocess
import sys
import sysconfig
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.cli.main import main
from okg.substrate.library.agent_runtimes.coordination_client import (
    ClientRegistry,
    ClientRoute,
    ClientWorkspace,
    CoordinationClient,
    CoordinationClientError,
    MAX_RESPONSE_BYTES,
    _bounded_json,
    _run_bounded_ssh,
    _safe_remote_command,
    _ssh_argv,
    _validate_directory_ancestors,
    build_mcp_server,
    load_client_registry,
    run_client_bridge,
)


ROUTES = ("local-claude", "local-codex", "remote-a", "remote-b")


def _registry() -> ClientRegistry:
    return ClientRegistry(
        routes={
            route: ClientRoute(
                route=route,
                host=f"{route}.coordination.invalid",
                user="coordination",
                # One registry can expose several logical endpoints only
                # when they share the same local SSH authority.
                identity_file="/run/okg/coordination.key",
                known_hosts_file="/run/okg/known_hosts",
            )
            for route in ROUTES
        },
        workspaces={
            f"grant:{route}": ClientWorkspace(grant_id=f"grant:{route}", route=route)
            for route in ROUTES
        },
    )


class _Runner:
    def __init__(self, output: object | None = None, returncode: int = 0) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self.output = output if output is not None else b'{"ok":true}\n'
        self.returncode = returncode

    def __call__(self, argv: list[str], **kwargs: object) -> SimpleNamespace:
        self.calls.append((argv, kwargs))
        return SimpleNamespace(stdout=self.output, returncode=self.returncode)


def _client(route: str, runner: _Runner) -> CoordinationClient:
    return CoordinationClient(
        registry=_registry(),
        route=route,
        grant_id=f"grant:{route}",
        runner=runner,
    )


def _status_result() -> dict[str, object]:
    assignment_ref = "sha256:" + "a" * 64
    return {
        "ok": True,
        "operation": "status",
        "cursor": "sha256:" + "b" * 64,
        "changed": True,
        "deltas": [
            {
                "assignment_ref": assignment_ref,
                "kind": "pact",
                "generation": 1,
                "status": "active",
                "revision": 3,
                "lease": {"state": "active", "fresh": True, "status": "available"},
                "workspace": {
                    "source": "last_known",
                    "status": "available",
                    "dirty": False,
                },
            }
        ],
        "exceptions": [],
        "blockers": [
            {
                "assignment_ref": assignment_ref,
                "state": "blocked",
                "code": "native_blocker",
                "reason_ref": "sha256:" + "c" * 64,
            }
        ],
        "freshness": {
            "status": "fresh",
            "live_revision": 3,
            "latest_published_revision": 3,
            "live": {"status": "available", "revision": 3},
            "latest_published": {"status": "available", "revision": 3},
            "assignments": [
                {
                    "assignment_ref": assignment_ref,
                    "live_revision": 3,
                    "published_revision": 3,
                }
            ],
        },
        "next_action": {"action": "continue"},
    }


def _close_plan() -> dict[str, object]:
    return {
        "assignment_id": "assignment:close",
        "assignment_generation": 2,
        "close_attempt_id": "close-attempt-1",
        "readiness_digest": "sha256:" + "a" * 64,
        "pair_digest": "sha256:" + "b" * 64,
        "start_plan": {
            "assignment_id": "assignment:close",
            "assignment_generation": 2,
            "authenticated_host": "logical:remote-b",
            "repo_path": "/srv/repo",
            "worktree_path": "/srv/worktree",
            "git_branch": "coord/close/g2",
            "git_base_commit": "1" * 40,
            "graph_deployment": "okg-workspace",
            "graph_base_branch": "default",
            "graph_base_generation": "generation-1",
            "graph_base_checkpoint": None,
            "graph_branch": "coord/close/g2",
            "mcp_route": "opaque:remote-b",
            "grant_id": "grant:assignment:close",
            "config_path": None,
            "provision_attempt_id": "start-attempt-close",
        },
    }


def _close_prepare_result() -> dict[str, object]:
    return {
        "ok": True,
        "status": "closing",
        "event_id": "close-event-1",
        "assignment_id": "assignment:close",
        "assignment_generation": 2,
        "revision": 7,
        "inserted": True,
        "duplicate": False,
        "close_attempt_id": "close-attempt-1",
        "readiness_digest": "sha256:" + "a" * 64,
        "close_plan": _close_plan(),
    }


def _close_finalize_result() -> dict[str, object]:
    return {
        "ok": True,
        "status": "retired",
        "event_id": "close-event-2",
        "assignment_id": "assignment:close",
        "assignment_generation": 2,
        "revision": 8,
        "inserted": True,
        "duplicate": False,
        "close_attempt_id": "close-attempt-1",
        "readiness_digest": "sha256:" + "a" * 64,
        "cleanup_digest": "sha256:" + "e" * 64,
    }


def _resume_result() -> dict[str, object]:
    assignment_ref = "sha256:" + "a" * 64
    assignment = {
        "assignment_id": "assignment:one",
        "assignment_ref": assignment_ref,
        "kind": "pact",
        "generation": 1,
        "status": "active",
        "revision": 3,
        "owner_ref": "sha256:" + "c" * 64,
        "session_ref": "sha256:" + "d" * 64,
        "lease": {"state": "active", "fresh": True, "status": "available"},
        "workspace": {"source": "live", "status": "available", "dirty": False},
        "git": {"branch_ref": "sha256:" + "e" * 64, "branch": "coord/status"},
        "graph": {"branch_ref": "sha256:" + "f" * 64, "branch": "default"},
        "pact": {
            "change_id": "change:one",
            "task_id": "task:one",
            "status": "completed",
            "state": "completed",
            "done": True,
            "authority": "live",
            "task_done": False,
            "proof_attached": False,
            "review_state": "pending",
            "merge_ready": False,
            "finalized_state": "open",
        },
    }
    return {
        "ok": True,
        "operation": "resume",
        "assignment": assignment,
        "assignments": [assignment],
        "hierarchy": {
            "role": "worker",
            "scope": {"change_ref": "sha256:" + "1" * 64},
            "self_ref": assignment_ref,
            "self_id": "assignment:one",
            "descendant_count": 0,
            "descendants": [],
            "active_children": [],
        },
        "route": {
            "status": "current",
            "assignment_ref": assignment_ref,
            "generation": 1,
            "parent_ref": None,
            "host_ref": "sha256:" + "2" * 64,
            "credential_ref": "sha256:" + "3" * 64,
            "worktree_ref": "sha256:" + "4" * 64,
            "branch_ref": "sha256:" + "5" * 64,
            "target_route_ref": "sha256:" + "9" * 64,
            "route_generation": 1,
            "route_digest": "sha256:" + "0" * 64,
            "assignment_id": "assignment:one",
            "parent_assignment_id": None,
            "parent_generation": None,
        },
        "lease": {"state": "active", "fresh": True, "status": "available"},
        "reservations": [],
        "pending_approvals": [],
        "messages": [],
        "blockers": [],
        "capacity": {
            "status": "known",
            "lane_usage": {},
            "named_resource_usage": {},
            "lane_capacity": {},
            "named_resource_capacity": {},
        },
        "jobs": {"status": "not_configured"},
        "pair": {
            "status": "known",
            "proof_ref": "sha256:" + "7" * 64,
            "pair_digest": "sha256:" + "8" * 64,
            "workspace_observation": "live",
            "git": {"branch_ref": "sha256:" + "e" * 64},
            "graph": {"branch_ref": "sha256:" + "f" * 64},
            "live_workspace": {"status": "available", "dirty": False},
        },
        "freshness": {
            "status": "fresh",
            "live_revision": 3,
            "latest_published_revision": 3,
            "live": {"status": "available", "revision": 3},
            "latest_published": {"status": "available", "revision": 3},
            "assignments": [],
        },
        "exceptions": [],
        "next_action": {"action": "continue"},
    }


def test_each_logical_route_uses_registry_and_fixed_ssh_argv() -> None:
    for route in ROUTES:
        runner = _Runner()
        client = _client(route, runner)
        assert client.coordination_event(
            event_type="heartbeat", idempotency_key="event-1"
        ) == {"ok": True}
        assert len(runner.calls) == 1
        argv, kwargs = runner.calls[0]
        assert argv[0] == "/usr/bin/ssh"
        assert "-F" in argv and argv[argv.index("-F") + 1] == "/dev/null"
        assert "BatchMode=yes" in argv
        assert "StrictHostKeyChecking=yes" in argv
        assert "GlobalKnownHostsFile=/dev/null" in argv
        assert "IdentitiesOnly=yes" in argv
        assert "IdentityAgent=none" in argv
        assert "ForwardAgent=no" in argv
        assert "ClearAllForwardings=yes" in argv
        assert "RequestTTY=no" in argv
        assert "PermitLocalCommand=no" in argv
        assert "ProxyCommand=none" in argv
        assert "ProxyJump=none" in argv
        assert "Tunnel=no" in argv
        assert f"coordination@{route}.coordination.invalid" in argv
        assert argv[-4:] == ["okg", "agent-ssh-run", "--route", route]
        assert kwargs["input"] == (
            b'{"event_type":"heartbeat","idempotency_key":"event-1",'
            b'"operation":"event"}\n'
        )
        assert kwargs["timeout"] == 15
        assert kwargs["check"] is False
        assert kwargs["env"] == {
            "PATH": "/usr/bin:/bin",
            "LC_ALL": "C",
            "LANG": "C",
        }


def test_existing_event_and_message_operations_use_bounded_server_wire() -> None:
    runner = _Runner()
    client = _client("local-claude", runner)
    assert client.coordination_event(
        event_type="heartbeat",
        idempotency_key="event-1",
        payload={"status": "alive"},
    ) == {"ok": True}
    assert client.send_message(
        recipient_assignment_id="assignment:recipient",
        recipient_generation=7,
        kind="work_result",
        idempotency_key="message-1",
        payload={"result": "ready"},
    ) == {"ok": True}
    assert client.ack_message(message_id="message-1", idempotency_key="ack-1") == {
        "ok": True
    }
    assert len(runner.calls) == 3
    requests = [json.loads(call[1]["input"].decode()) for call in runner.calls]
    assert requests == [
        {
            "event_type": "heartbeat",
            "idempotency_key": "event-1",
            "operation": "event",
            "payload": {"status": "alive"},
        },
        {
            "idempotency_key": "message-1",
            "kind": "work_result",
            "operation": "send_message",
            "payload": {"result": "ready"},
            "recipient_assignment_id": "assignment:recipient",
            "recipient_generation": 7,
        },
        {
            "idempotency_key": "ack-1",
            "message_id": "message-1",
            "operation": "ack_message",
        },
    ]


def test_status_and_resume_use_closed_requests_and_native_response_schema() -> None:
    status_runner = _Runner(output=json.dumps(_status_result()).encode() + b"\n")
    status_client = _client("local-claude", status_runner)
    status = status_client.status(cursor="sha256:" + "9" * 64)
    assert status["operation"] == "status"
    assert status["freshness"]["assignments"][0]["live_revision"] == 3
    assert status["blockers"][0]["code"] == "native_blocker"
    assert status["next_action"] == {"action": "continue"}
    status_argv, status_kwargs = status_runner.calls[0]
    assert status_kwargs["input"] == (
        b'{"cursor":"sha256:' + b"9" * 64 + b'","operation":"status"}\n'
    )
    assert status_argv[-4:] == ["okg", "agent-ssh-run", "--route", "local-claude"]

    resume_runner = _Runner(output=json.dumps(_resume_result()).encode() + b"\n")
    resume_client = _client("local-claude", resume_runner)
    resume = resume_client.resume()
    assert resume["operation"] == "resume"
    assert resume["assignment"]["assignment_id"] == "assignment:one"
    assert resume["assignment"]["pact"]["authority"] == "live"
    assert resume["assignments"][0]["git"]["branch"] == "coord/status"
    assert resume["pair"]["proof_ref"].startswith("sha256:")
    resume_argv, resume_kwargs = resume_runner.calls[0]
    assert resume_kwargs["input"] == b'{"operation":"resume"}\n'
    assert resume_argv[-4:] == ["okg", "agent-ssh-run", "--route", "local-claude"]


def test_status_and_resume_client_reject_invalid_cursor_and_leaked_response_fields() -> (
    None
):
    runner = _Runner()
    client = _client("local-claude", runner)
    with pytest.raises(CoordinationClientError, match="invalid_cursor"):
        client.status(cursor={"revision": 3})  # type: ignore[arg-type]
    for cursor in (1, "cursor-1", "SHA256:" + "a" * 64):
        with pytest.raises(CoordinationClientError, match="invalid_cursor"):
            client.status(cursor=cursor)  # type: ignore[arg-type]
    assert not runner.calls

    leaked_status = _status_result()
    leaked_status["freshness"] = {
        **leaked_status["freshness"],  # type: ignore[dict-item]
        "live": {"status": "available", "event_payload": {"secret": "x"}},
    }
    runner = _Runner(output=json.dumps(leaked_status).encode() + b"\n")
    with pytest.raises(CoordinationClientError, match="invalid_response"):
        _client("local-claude", runner).status()

    leaked_resume = _resume_result()
    leaked_resume["assignment"] = {
        **leaked_resume["assignment"],  # type: ignore[dict-item]
        "workspace": {"status": "available", "raw_path": "/private/worktree"},
    }
    runner = _Runner(output=json.dumps(leaked_resume).encode() + b"\n")
    with pytest.raises(CoordinationClientError, match="invalid_response"):
        _client("local-claude", runner).resume()


def _close_proof() -> dict[str, object]:
    return {
        "assignment_id": "assignment:close",
        "assignment_generation": 2,
        "close_attempt_id": "close-attempt-1",
        "readiness_digest": "sha256:" + "a" * 64,
        "git_removed": True,
        "graph_removed": True,
        "config_removed": True,
        "complete": True,
        "disposition": "complete",
        "errors": [],
    }


def test_close_assignment_sends_exact_two_phase_requests_and_safe_receipts() -> None:
    prepare_runner = _Runner(
        output=json.dumps(_close_prepare_result()).encode() + b"\n"
    )
    client = _client("local-claude", prepare_runner)
    prepared = client._prepare_assignment_close(idempotency_key="close-request-1")
    assert prepared["status"] == "closing"
    assert prepared["close_plan"] == _close_plan()
    assert prepare_runner.calls[0][1]["input"] == (
        b'{"idempotency_key":"close-request-1",'
        b'"operation":"prepare_assignment_close"}\n'
    )
    assert prepare_runner.calls[0][0][-4:] == [
        "okg",
        "agent-ssh-run",
        "--route",
        "local-claude",
    ]

    finalize_runner = _Runner(
        output=json.dumps(_close_finalize_result()).encode() + b"\n"
    )
    client = _client("local-claude", finalize_runner)
    finalized = client._finalize_assignment_close(
        close_attempt_id="close-attempt-1",
        readiness_digest="sha256:" + "a" * 64,
        cleanup_digest="sha256:" + "e" * 64,
        cleanup_proof=_close_proof(),
        idempotency_key="close-request-1",
    )
    assert finalized["status"] == "retired"
    assert finalize_runner.calls[0][1]["input"] == (
        b'{"cleanup_digest":"sha256:'
        + b"e" * 64
        + b'","cleanup_proof":{"assignment_generation":2,'
        b'"assignment_id":"assignment:close","close_attempt_id":"close-attempt-1",'
        b'"complete":true,"config_removed":true,"disposition":"complete",'
        b'"errors":[],"git_removed":true,"graph_removed":true,"readiness_digest":"sha256:'
        + b"a"
        * 64
        + b'"},"close_attempt_id":"close-attempt-1","idempotency_key":"close-request-1",'
        b'"operation":"finalize_assignment_close","readiness_digest":"sha256:'
        + b"a" * 64
        + b'"}\n'
    )


def test_close_assignment_uses_server_completion_without_local_factory() -> None:
    completed = _close_finalize_result()
    runner = _Runner(output=json.dumps(completed).encode() + b"\n")
    client = _client("local-claude", runner)

    result = client.close_assignment(idempotency_key="close-request-1")
    assert result["status"] == "retired"
    assert result["assignment_id"] == completed["assignment_id"]
    assert result["cleanup_digest"] == completed["cleanup_digest"]
    assert len(runner.calls) == 1
    assert runner.calls[0][1]["input"] == (
        b'{"idempotency_key":"close-request-1",'
        b'"operation":"prepare_assignment_close"}\n'
    )


def test_close_assignment_requires_server_completion_or_local_factory() -> None:
    runner = _Runner(output=json.dumps(_close_prepare_result()).encode() + b"\n")
    client = _client("local-claude", runner)
    with pytest.raises(CoordinationClientError, match="assignment_close_unavailable"):
        client.close_assignment(idempotency_key="close-request-1")
    assert len(runner.calls) == 1


def test_close_assignment_rejects_bad_proof_or_result() -> None:
    runner = _Runner()
    client = _client("local-claude", runner)

    with pytest.raises(CoordinationClientError, match="cleanup_proof_invalid"):
        client._finalize_assignment_close(
            close_attempt_id="close-attempt-1",
            readiness_digest="sha256:" + "a" * 64,
            cleanup_digest="sha256:" + "e" * 64,
            cleanup_proof={**_close_proof(), "errors": ["/private/path"]},
            idempotency_key="close-request-1",
        )
    assert not runner.calls

    leaked = _close_prepare_result()
    leaked["close_plan"] = {
        **_close_plan(),
        "start_plan": {
            **_close_plan()["start_plan"],
            "repo_path": "private/secret",
        },
    }
    leaked_runner = _Runner(output=json.dumps(leaked).encode() + b"\n")
    with pytest.raises(CoordinationClientError, match="invalid_response"):
        _client("local-claude", leaked_runner)._prepare_assignment_close(
            idempotency_key="close-request-1"
        )


def test_assignment_request_exposes_only_scope_profile_and_opaque_target() -> None:
    runner = _Runner(
        output=(
            b'{"assignment_generation":1,"assignment_id":"assignment:child",'
            b'"duplicate":false,"ok":true,"status":"pending"}\n'
        )
    )
    client = _client("local-claude", runner)

    result = client.request_assignment(
        child_kind="pact",
        target_route="opaque:remote-b",
        scope={"change_id": "change-1"},
        execution_profile="database-critical",
        idempotency_key="assignment-request-1",
    )

    assert result["status"] == "pending"
    request = json.loads(runner.calls[0][1]["input"].decode())
    assert request == {
        "child_kind": "pact",
        "execution_profile": "database-critical",
        "idempotency_key": "assignment-request-1",
        "operation": "request_assignment",
        "scope": {"change_id": "change-1"},
        "target_route": "opaque:remote-b",
    }
    assert not any(
        field in request
        for field in (
            "parent_assignment_id",
            "owner_principal",
            "owner_session_id",
            "credential_id",
            "assignment_generation",
            "workspace_id",
        )
    )


def test_assignment_request_profile_shape_and_start_availability_fail_closed() -> None:
    runner = _Runner()
    client = _client("local-claude", runner)
    with pytest.raises(CoordinationClientError, match="execution_profile_invalid"):
        client.request_assignment(
            child_kind="sprint",
            target_route="opaque:remote-a",
            scope={"sprint_id": "sprint-1"},
            execution_profile="default-light",
            idempotency_key="sprint-request-1",
        )
    with pytest.raises(CoordinationClientError, match="child_kind_invalid"):
        client.request_assignment(
            child_kind="delegation",
            target_route="opaque:remote-a",
            scope={"task_id": "task-1"},
            execution_profile="default-light",
            idempotency_key="delegation-request-1",
        )
    assert not runner.calls

    starting = {
        "ok": True,
        "status": "starting",
        "assignment_id": "assignment:child",
        "assignment_generation": 1,
        "start_attempt_id": "attempt-1",
        "start_plan": {
            **_close_plan()["start_plan"],
            "assignment_id": "assignment:child",
            "assignment_generation": 1,
            "provision_attempt_id": "attempt-1",
        },
    }
    runner = _Runner(output=json.dumps(starting).encode() + b"\n")
    with pytest.raises(CoordinationClientError, match="assignment_start_unavailable"):
        _client("local-claude", runner).start_assignment(
            assignment_id="assignment:child", idempotency_key="start-1"
        )
    assert len(runner.calls) == 1


def test_start_assignment_uses_server_completion_without_local_factory() -> None:
    completed = {
        "ok": True,
        "status": "active",
        "assignment_id": "assignment:child",
        "assignment_generation": 1,
        "start_attempt_id": "attempt-1",
        "pair_digest": "sha256:" + "a" * 64,
    }
    runner = _Runner(output=json.dumps(completed).encode() + b"\n")

    assert (
        _client("local-claude", runner).start_assignment(
            assignment_id="assignment:child", idempotency_key="start-1"
        )
        == completed
    )
    assert len(runner.calls) == 1


@pytest.mark.parametrize(
    "field",
    ["host", "user", "key", "identity", "dsn", "principal", "session", "assignment"],
)
def test_tool_payload_cannot_supply_authority_or_transport_fields(field: str) -> None:
    runner = _Runner()
    client = _client("local-claude", runner)
    with pytest.raises(CoordinationClientError, match="caller_fields_rejected"):
        client.coordination_event(
            event_type="heartbeat",
            idempotency_key="event-1",
            payload={field: "attacker-value"},
        )
    assert not runner.calls


def test_send_message_allows_only_server_selected_recipient_shape() -> None:
    runner = _Runner()
    client = _client("local-claude", runner)
    with pytest.raises(CoordinationClientError, match="caller_fields_rejected"):
        client.send_message(
            recipient_assignment_id="assignment:recipient",
            recipient_generation=1,
            kind="work",
            idempotency_key="message-1",
            payload={"owner_principal": "forged"},
        )
    assert not runner.calls


def test_response_is_bounded_and_sanitized() -> None:
    oversized = _Runner(output=b"{" + b"a" * MAX_RESPONSE_BYTES)
    with pytest.raises(CoordinationClientError, match="response_too_large"):
        _client("local-claude", oversized).ack_message(
            message_id="message-1", idempotency_key="ack-1"
        )

    malformed = _Runner(output=b"not-json")
    with pytest.raises(CoordinationClientError, match="invalid_response"):
        _client("local-claude", malformed).ack_message(
            message_id="message-1", idempotency_key="ack-1"
        )

    leaked = _Runner(output=b'{"ok":true,"dsn":"postgresql://secret"}\n')
    with pytest.raises(CoordinationClientError, match="invalid_response"):
        _client("local-claude", leaked).ack_message(
            message_id="message-1", idempotency_key="ack-1"
        )

    leaked_reason = _Runner(
        output=b'{"ok":false,"status":"rejected",'
        b'"reason_code":"postgresql://secret"}\n',
        returncode=1,
    )
    with pytest.raises(CoordinationClientError, match="invalid_response"):
        _client("local-claude", leaked_reason).ack_message(
            message_id="message-1", idempotency_key="ack-1"
        )


def test_ssh_return_code_cannot_turn_transport_failure_into_success() -> None:
    failed_success = _Runner(output=b'{"ok":true}\n', returncode=1)
    with pytest.raises(CoordinationClientError, match="ssh_transport_failed"):
        _client("local-claude", failed_success).ack_message(
            message_id="message-1", idempotency_key="ack-1"
        )

    failed_refusal = _Runner(
        output=b'{"ok":false,"status":"rejected","reason_code":"expired_lease"}\n',
        returncode=1,
    )
    assert _client("local-claude", failed_refusal).ack_message(
        message_id="message-1", idempotency_key="ack-1"
    ) == {
        "ok": False,
        "status": "rejected",
        "reason_code": "expired_lease",
    }

    successful_refusal = _Runner(
        output=b'{"ok":false,"status":"rejected","reason_code":"refused"}\n',
        returncode=0,
    )
    with pytest.raises(CoordinationClientError, match="ssh_transport_failed"):
        _client("local-claude", successful_refusal).ack_message(
            message_id="message-1", idempotency_key="ack-1"
        )


def test_registry_is_fixed_shape_and_private(tmp_path: Path) -> None:
    path = tmp_path / "client-registry.json"
    identity = tmp_path / "coord.key"
    known_hosts = tmp_path / "known_hosts"
    identity.write_text("private-key-fixture", encoding="utf-8")
    known_hosts.write_text("known-host-fixture", encoding="utf-8")
    identity.chmod(0o640)
    known_hosts.chmod(0o644)
    path.write_text(
        json.dumps(
            {
                "routes": {
                    "local-claude": {
                        "host": "coordination.invalid",
                        "user": "coordination",
                        "identity_file": str(identity),
                        "known_hosts_file": str(known_hosts),
                        "port": 2222,
                    }
                },
                "workspaces": {
                    "grant:one": {"route": "local-claude"},
                },
            }
        )
    )
    path.chmod(0o600)
    registry = load_client_registry(path, expected_uid=os.geteuid())
    assert registry.routes["local-claude"].port == 2222
    assert registry.workspaces["grant:one"].route == "local-claude"

    path.chmod(0o660)
    with pytest.raises(CoordinationClientError, match="registry_unavailable"):
        load_client_registry(path, expected_uid=os.geteuid())


def test_registry_rejects_nonsticky_writable_ancestor(tmp_path: Path) -> None:
    writable = tmp_path / "writable"
    writable.mkdir()
    writable.chmod(0o777)

    with pytest.raises(CoordinationClientError, match="registry_unavailable"):
        _validate_directory_ancestors(
            writable / "client-registry.json", expected_uid=os.geteuid()
        )


def test_registry_is_one_os_principal_authority_boundary(
    tmp_path: Path,
) -> None:
    """A bridge registry cannot mix credentials for different local principals.

    Multiple logical route keys may share one enrolled identity, but adding a
    second identity to the same registry is rejected.  This boundary is per
    fixed registry and OS principal; it does not pretend to isolate grants
    when an operator deliberately gives one Unix user the same key for them.
    """
    identity_a = tmp_path / "coord-a.key"
    identity_b = tmp_path / "coord-b.key"
    known_hosts = tmp_path / "known_hosts"
    for path in (identity_a, identity_b, known_hosts):
        path.write_text("fixture", encoding="utf-8")
        path.chmod(0o640)
    path = tmp_path / "client-registry.json"
    path.write_text(
        json.dumps(
            {
                "routes": {
                    "local-claude": {
                        "host": "claude.invalid",
                        "user": "coordination",
                        "identity_file": str(identity_a),
                        "known_hosts_file": str(known_hosts),
                        "port": 22,
                    },
                    "local-codex": {
                        "host": "codex.invalid",
                        "user": "coordination",
                        "identity_file": str(identity_b),
                        "known_hosts_file": str(known_hosts),
                        "port": 22,
                    },
                },
                "workspaces": {
                    "grant:claude": {"route": "local-claude"},
                    "grant:codex": {"route": "local-codex"},
                },
            }
        )
    )
    path.chmod(0o640)
    with pytest.raises(CoordinationClientError, match="registry_authority_mismatch"):
        load_client_registry(path, expected_uid=os.geteuid())


def test_environment_cannot_override_registry_or_supply_transport_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_AGENT_SSH_ROUTE", "local-claude")
    monkeypatch.setenv("OKG_AGENT_SSH_GRANT", "grant:local-claude")
    monkeypatch.setenv("OKG_AGENT_SSH_HOST", "attacker.invalid")
    monkeypatch.setenv("OKG_AGENT_SSH_KEY", "/tmp/attacker.key")
    with pytest.raises(CoordinationClientError, match="registry_unavailable"):
        CoordinationClient.from_environment(registry_path=tmp_path / "missing.json")


def test_client_bridge_sanitizes_unexpected_startup_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _raise_startup_error(cls) -> CoordinationClient:
        raise RuntimeError("postgresql://secret@db.invalid/private/path")

    monkeypatch.setattr(
        CoordinationClient,
        "from_environment",
        classmethod(_raise_startup_error),
    )

    assert run_client_bridge() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "coordination client refused: internal_error\n"
    assert "secret" not in captured.err
    assert "postgresql://" not in captured.err


def test_mcp_server_registers_exact_eight_tools() -> None:
    server = build_mcp_server(_client("local-claude", _Runner()))
    tools = getattr(server, "_tool_manager")._tools
    assert set(tools) == {
        "coordination_event",
        "coordination_status",
        "coordination_resume",
        "close_assignment",
        "send_message",
        "ack_message",
        "request_assignment",
        "start_assignment",
    }


@pytest.mark.parametrize(
    "argv",
    [
        ["agent-ssh-client", "--host", "attacker.invalid"],
        ["agent-ssh-client", "--registry", "/tmp/attacker.json"],
        ["prefix", "agent-ssh-client"],
    ],
)
def test_client_cli_rejects_caller_options_and_repositioning(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(argv) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "ok": False,
        "reason_code": "request_arguments_rejected",
        "status": "rejected",
    }


def test_production_runner_caps_stdout_before_buffering() -> None:
    with pytest.raises(CoordinationClientError, match="response_too_large"):
        _run_bounded_ssh(
            [
                sys.executable,
                "-c",
                "import sys; sys.stdout.write('x' * 70000); sys.stdout.flush()",
            ],
            input_bytes=b"",
            timeout_seconds=5,
        )


def test_production_runner_terminates_and_reaps_stalled_child() -> None:
    with pytest.raises(CoordinationClientError, match="ssh_timeout"):
        _run_bounded_ssh(
            [sys.executable, "-c", "import time; time.sleep(5)"],
            input_bytes=b"",
            timeout_seconds=1,
        )


def test_real_ssh_client_reaches_four_forced_coordination_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise real SSH, sshd evidence, and the forced-command dispatcher.

    Each route has its own high-port sshd with a fixed ForceCommand argument,
    while all four routes deliberately share one client key.  The fixture
    imports the production parser/dispatcher and injects only the config,
    policy, evidence-owner, and writer seams needed for a non-root test host.
    Production ownership checks are unchanged; the root-owned ExposeAuthInfo
    cutover proof remains an operations check.
    """

    ssh = Path("/usr/bin/ssh")
    sshd = Path("/usr/sbin/sshd")
    keygen = Path("/usr/bin/ssh-keygen")
    keyscan = Path("/usr/bin/ssh-keyscan")
    if not all(path.is_file() for path in (ssh, sshd, keygen, keyscan)):
        pytest.skip("OpenSSH fixture binaries are unavailable")
    if os.name == "nt":  # pragma: no cover - SSH fixture is POSIX-only
        ssh_user = getpass.getuser()
    else:
        import pwd

        account = pwd.getpwuid(os.geteuid())
        ssh_user = account.pw_name
        login_shell = Path(account.pw_shell)
        if not login_shell.is_file() or not os.access(login_shell, os.X_OK):
            pytest.skip(
                "real SSH fixture requires an executable login shell for "
                f"{ssh_user!r}; passwd declares {account.pw_shell!r}"
            )
        try:
            shell_probe = subprocess.run(
                [str(login_shell), "-c", ":"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except subprocess.TimeoutExpired:
            pytest.skip(
                "real SSH fixture requires the effective account's login shell "
                f"to execute promptly (user={ssh_user!r}, shell={account.pw_shell!r})"
            )
        if shell_probe.returncode != 0:
            pytest.skip(
                "real SSH fixture requires the effective account's login shell "
                f"to execute sshd ForceCommand (user={ssh_user!r}, "
                f"shell={account.pw_shell!r}, rc={shell_probe.returncode})"
            )
    # GitHub's self-hosted service may export the runner account in USER and
    # LOGNAME while executing the job as a different locked service account.
    # Keep that poisoned environment in the regression so getpass.getuser()
    # would reproduce the authentication failure this fixture must avoid.
    monkeypatch.setenv("USER", "runner")
    monkeypatch.setenv("LOGNAME", "runner")
    # The system Python intentionally need not carry this checkout's package
    # dependencies.  Run the forced fixture with the same hermetic interpreter
    # that is executing the test instead of treating that as an SSH failure.
    fixture_python = Path(sys.executable)

    routes = ROUTES
    host_key = tmp_path / "host_key"
    client_key = tmp_path / "client_key"
    authorized = tmp_path / "authorized_keys"
    fixture = tmp_path / "forced_coordination_fixture.py"
    known_hosts = tmp_path / "known_hosts"
    for key_path in (host_key, client_key):
        subprocess.run(
            [str(keygen), "-q", "-t", "ed25519", "-N", "", "-f", str(key_path)],
            check=True,
            capture_output=True,
        )
        key_path.chmod(0o600)
    public_key = subprocess.run(
        [str(keygen), "-y", "-f", str(client_key)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    authorized.write_text(public_key + "\n", encoding="utf-8")
    authorized.chmod(0o600)

    # The script's route argument is supplied by each sshd's fixed
    # ForceCommand.  SSH_ORIGINAL_COMMAND is checked only as an argv proof;
    # it is never used to select the authoritative route.
    source_root = Path(__file__).resolve().parents[4] / "src"
    fixture.write_text(
        "from __future__ import annotations\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        f"sys.path.insert(0, {str(source_root)!r})\n"
        "from okg.substrate.library.agent_runtimes.coordination_ssh import (\n"
        "    CoordinationServerConfig, CoordinationServerRoute, run_forced_command,\n"
        ")\n"
        "from okg.substrate.library.agent_runtimes.ssh_stdio import read_sshd_peer_evidence\n"
        "from okg.substrate.pact.coordination import CoordinationPolicyBinding\n"
        "\n"
        "ROUTES = " + repr(routes) + "\n"
        "route = sys.argv[1] if len(sys.argv) == 2 else ''\n"
        "if route not in ROUTES:\n"
        "    raise SystemExit(91)\n"
        "if os.environ.get('SSH_ORIGINAL_COMMAND') != (\n"
        "    f'okg agent-ssh-run --route {route}'\n"
        "):\n"
        '    sys.stdout.write(\'{"ok":false,"status":"rejected","reason_code":"argv_mismatch"}\\n\')\n'
        "    sys.stdout.flush()\n"
        "    raise SystemExit(1)\n"
        "\n"
        "class Writer:\n"
        "    def __init__(self, selected_route):\n"
        "        self.selected_route = selected_route\n"
        "\n"
        "    def write(self, evidence, **kwargs):\n"
        "        return {\n"
        "            'ok': True,\n"
        "            'status': 'accepted',\n"
        "            'event_id': f'event:{self.selected_route}',\n"
        "            'event_type': kwargs['event_type'],\n"
        "        }\n"
        "\n"
        "    def send_message_from_peer(self, evidence, **kwargs):\n"
        "        return {\n"
        "            'ok': True,\n"
        "            'status': 'pending',\n"
        "            'message_id': f'message:{self.selected_route}',\n"
        "            'duplicate': False,\n"
        "        }\n"
        "\n"
        "    def acknowledge_message_from_peer(self, evidence, **kwargs):\n"
        "        return {\n"
        "            'ok': True,\n"
        "            'status': 'acknowledged',\n"
        "            'message_id': kwargs['message_id'],\n"
        "            'duplicate': False,\n"
        "        }\n"
        "\n"
        "server_config = CoordinationServerConfig(routes={\n"
        "    item: CoordinationServerRoute(\n"
        "        route=item, deployment_name='fixture', repo_root=__import__('pathlib').Path('/tmp'),\n"
        "        expected_git_commit='0' * 40, dsn='test-only',\n"
        "    ) for item in ROUTES\n"
        "})\n"
        "policy = CoordinationPolicyBinding(\n"
        "    agent_transport='restricted_ssh_stdio',\n"
        "    lease_ttl_seconds={'chief': 1, 'sprint': 1, 'pact': 1, 'delegation': 1},\n"
        "    policy_digest='sha256:' + '1' * 64,\n"
        "    project_digest='sha256:' + '2' * 64,\n"
        "    source='test-only',\n"
        ")\n"
        "rc = run_forced_command(\n"
        "    route=route, stdin=sys.stdin.buffer, stdout=sys.stdout.buffer,\n"
        "    config_loader=lambda: server_config,\n"
        "    writer_factory=lambda selected: Writer(selected.route),\n"
        "    evidence_reader=lambda env: read_sshd_peer_evidence(\n"
        "        env, expected_uid=os.geteuid()\n"
        "    ),\n"
        "    policy_loader=lambda _: policy,\n"
        ")\n"
        "raise SystemExit(rc)\n",
        encoding="utf-8",
    )
    fixture.chmod(0o700)

    # ``sshd`` starts the forced command with a minimal environment.  The
    # hosted setup Python therefore needs its own shared-library and package
    # roots made explicit; otherwise the SSH session succeeds but importing
    # the fixture's ``yaml`` dependency fails before the production parser is
    # reached.  Keep the route fixed in each sshd config and quote every path
    # in the generated launcher so this remains the same trusted command.
    if os.name != "nt":
        purelib = sysconfig.get_path("purelib")
        assert purelib is not None
        launcher = tmp_path / "forced_coordination_launcher.sh"
        launcher.write_text(
            "#!/bin/sh\n"
            f"export LD_LIBRARY_PATH={shlex.quote(str(Path(sys.prefix) / 'lib'))}\n"
            f"export PYTHONPATH={shlex.quote(purelib)}\n"
            f'exec {shlex.quote(str(fixture_python))} {shlex.quote(str(fixture))} "$@"\n',
            encoding="utf-8",
        )
        launcher.chmod(0o700)
        assert launcher.stat().st_mode & 0o777 == 0o700
    else:  # pragma: no cover - SSH fixture is POSIX-only
        launcher = fixture_python

    ports: dict[str, int] = {}
    for route in routes:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            ports[route] = reservation.getsockname()[1]
    configs: dict[str, Path] = {}
    servers: dict[str, subprocess.Popen[str]] = {}
    for route in routes:
        config = tmp_path / f"sshd-{route}.config"
        configs[route] = config
        config.write_text(
            "\n".join(
                (
                    f"Port {ports[route]}",
                    "ListenAddress 127.0.0.1",
                    f"HostKey {host_key}",
                    f"AuthorizedKeysFile {authorized}",
                    "PubkeyAuthentication yes",
                    "PasswordAuthentication no",
                    "KbdInteractiveAuthentication no",
                    # Self-hosted runner service accounts commonly have a locked
                    # password hash.  PAM account processing still permits the
                    # public-key-only session while password auth remains disabled.
                    "UsePAM yes",
                    "PermitTTY no",
                    "AllowAgentForwarding no",
                    "AllowTcpForwarding no",
                    "X11Forwarding no",
                    "ExposeAuthInfo yes",
                    "StrictModes no",
                    f"ForceCommand {shlex.quote(str(launcher))} {shlex.quote(route)}",
                    f"PidFile {tmp_path / f'{route}.pid'}",
                    "LogLevel VERBOSE",
                )
            )
            + "\n",
            encoding="utf-8",
        )
        config.chmod(0o600)
        subprocess.run([str(sshd), "-t", "-f", str(config)], check=True)

    try:
        for route in routes:
            servers[route] = subprocess.Popen(
                [str(sshd), "-D", "-e", "-f", str(configs[route])],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
        for route in routes:
            server = servers[route]
            for _ in range(100):
                if server.poll() is not None:
                    details = (server.stderr.read() if server.stderr else "")[-500:]
                    pytest.fail(
                        f"sshd fixture {route} exited before listening: {details}"
                    )
                try:
                    with socket.create_connection(
                        ("127.0.0.1", ports[route]), timeout=0.1
                    ):
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                pytest.fail(f"sshd fixture {route} did not start listening")

        scan_lines: list[str] = []
        for route in routes:
            scan = subprocess.run(
                [
                    str(keyscan),
                    "-T",
                    "2",
                    "-p",
                    str(ports[route]),
                    "127.0.0.1",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            scan_lines.extend(scan.stdout.splitlines())
        known_hosts.write_text("\n".join(scan_lines) + "\n", encoding="utf-8")
        known_hosts.chmod(0o644)

        registry_path = tmp_path / "client-registry.json"
        # ``getpass.getuser()`` prefers USER/LOGNAME, which can name the
        # service account that launched a self-hosted runner rather than the
        # account the test process actually runs as.  Resolve the SSH login
        # from the kernel UID so the fixture remains valid on those hosts.
        registry_path.write_text(
            json.dumps(
                {
                    "routes": {
                        route: {
                            "host": "127.0.0.1",
                            "user": ssh_user,
                            "identity_file": str(client_key),
                            "known_hosts_file": str(known_hosts),
                            "port": ports[route],
                        }
                        for route in routes
                    },
                    "workspaces": {
                        f"grant:{route}": {"route": route} for route in routes
                    },
                }
            ),
            encoding="utf-8",
        )
        registry_path.chmod(0o640)
        registry = load_client_registry(registry_path, expected_uid=os.geteuid())

        # Prove that this host can establish the rootless loopback session
        # before asking the production runner to discard SSH diagnostics.  A
        # failure here stays red and reports only bounded, path-scrubbed test
        # evidence; product transport failures below remain unchanged.
        probe_route = "local-claude"
        probe = subprocess.run(
            [
                *_ssh_argv(registry.routes[probe_route]),
                *_safe_remote_command(probe_route),
            ],
            input=_bounded_json(
                {
                    "operation": "event",
                    "event_type": "heartbeat",
                    "idempotency_key": "real-ssh-host-capability",
                }
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=10,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"},
        )
        try:
            probe_result = json.loads(probe.stdout)
        except (UnicodeDecodeError, json.JSONDecodeError):
            probe_result = None
        if (
            probe.returncode != 0
            or not isinstance(probe_result, dict)
            or probe_result.get("ok") is not True
        ):
            server = servers[probe_route]
            server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=3)
            server_stderr = server.stderr.read() if server.stderr else ""

            def scrub(value: str | bytes) -> str:
                text = (
                    value.decode("utf-8", errors="replace")
                    if isinstance(value, bytes)
                    else value
                )
                return text.replace(str(tmp_path), "<tmp>")[-2000:]

            pytest.fail(
                "rootless SSH host-capability probe failed before the production "
                f"runner (returncode={probe.returncode}, "
                f"stdout={scrub(probe.stdout)!r}, "
                f"stderr={scrub(probe.stderr)!r}, "
                f"sshd={scrub(server_stderr)!r})"
            )

        results = {
            "local-claude": CoordinationClient(
                registry=registry,
                route="local-claude",
                grant_id="grant:local-claude",
            ).coordination_event(
                event_type="heartbeat", idempotency_key="real-ssh-event"
            ),
            "local-codex": CoordinationClient(
                registry=registry,
                route="local-codex",
                grant_id="grant:local-codex",
            ).send_message(
                recipient_assignment_id="assignment:recipient",
                recipient_generation=1,
                kind="work_result",
                idempotency_key="real-ssh-send",
                payload={"result": "ready"},
            ),
            "remote-a": CoordinationClient(
                registry=registry,
                route="remote-a",
                grant_id="grant:remote-a",
            ).ack_message(
                message_id="message:remote-a", idempotency_key="real-ssh-ack"
            ),
            "remote-b": CoordinationClient(
                registry=registry,
                route="remote-b",
                grant_id="grant:remote-b",
            ).coordination_event(
                event_type="heartbeat", idempotency_key="real-ssh-event-2"
            ),
        }
        assert results == {
            "local-claude": {
                "ok": True,
                "status": "accepted",
                "event_id": "event:local-claude",
                "event_type": "heartbeat",
            },
            "local-codex": {
                "ok": True,
                "status": "pending",
                "message_id": "message:local-codex",
                "duplicate": False,
            },
            "remote-a": {
                "ok": True,
                "status": "acknowledged",
                "message_id": "message:remote-a",
                "duplicate": False,
            },
            "remote-b": {
                "ok": True,
                "status": "accepted",
                "event_id": "event:remote-b",
                "event_type": "heartbeat",
            },
        }
    finally:
        for server in servers.values():
            server.terminate()
        for server in servers.values():
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=3)
