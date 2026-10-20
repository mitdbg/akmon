from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from okg.substrate.cli.main import main
from okg.substrate.library.agent_runtimes import coordination_ssh
from okg.substrate.library.agent_runtimes.ssh_stdio import SshdPeerEvidence
from okg.substrate.pact.coordination import CoordinationPolicyBinding


def test_forced_command_request_wait_is_bounded() -> None:
    read_fd, write_fd = os.pipe()
    try:
        with os.fdopen(read_fd, "rb") as stream:
            with pytest.raises(
                coordination_ssh.CoordinationSshError, match="request_timeout"
            ):
                coordination_ssh.read_bounded_request(
                    stream,
                    max_wait_seconds=0.01,
                )
    finally:
        os.close(write_fd)


def test_forced_command_stops_at_one_json_line_without_waiting_for_eof() -> None:
    read_fd, write_fd = os.pipe()
    try:
        os.write(
            write_fd,
            b'{"event_type":"heartbeat","idempotency_key":"line-1"}\n',
        )
        with os.fdopen(read_fd, "rb") as stream:
            request = coordination_ssh.read_bounded_request(
                stream,
                max_wait_seconds=0.1,
            )
        assert request["idempotency_key"] == "line-1"
    finally:
        os.close(write_fd)


def _route(tmp_path: Path) -> coordination_ssh.CoordinationServerRoute:
    return coordination_ssh.CoordinationServerRoute(
        route="opaque-route",
        deployment_name="okg-workspace",
        repo_root=tmp_path,
        expected_git_commit="0" * 40,
        dsn="postgresql://coord-agent:CANARY@db.invalid/coord",
    )


def _config(tmp_path: Path) -> dict[str, object]:
    return {"routes": {"opaque-route": _route(tmp_path)}}


def _evidence() -> SshdPeerEvidence:
    return SshdPeerEvidence(
        credential_id="ssh-auth:sha256:credential-not-for-wire",
        source_address="192.0.2.10",
        source_port=41000,
        target_address="198.51.100.8",
        target_port=22,
    )


def _policy() -> CoordinationPolicyBinding:
    return CoordinationPolicyBinding(
        agent_transport="restricted_ssh_stdio",
        lease_ttl_seconds={"chief": 11, "sprint": 22, "pact": 33, "delegation": 44},
        policy_digest="sha256:" + "1" * 64,
        project_digest="sha256:" + "2" * 64,
        source="pact/project.yaml",
    )


class _Writer:
    def __init__(
        self, *, result: object | None = None, error: Exception | None = None
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.result = result or {
            "ok": True,
            "status": "accepted",
            "event_id": "event-1",
            "event_type": "heartbeat",
            "assignment_id": "assignment-1",
            "assignment_generation": 4,
            "revision": 9,
            "inserted": True,
            "lease_expires_at": "2026-08-27T12:00:00+00:00",
            "dsn": "postgresql://should-never-be-returned",
            "peer": "198.51.100.8",
        }
        self.error = error

    def write(self, evidence: object, **kwargs: object) -> object:
        self.calls.append({"evidence": evidence, **kwargs})
        if self.error is not None:
            raise self.error
        return self.result


class _MessageWriter:
    def __init__(
        self, *, send_result: object | None = None, ack_result: object | None = None
    ) -> None:
        self.send_calls: list[dict[str, object]] = []
        self.ack_calls: list[dict[str, object]] = []
        self.send_result = send_result or {
            "ok": True,
            "status": "pending",
            "message_id": "message-1",
            "inserted": True,
            "duplicate": False,
            "recipient_principal": "subject:must-not-cross-wire",
            "recipient_session_id": "session:must-not-cross-wire",
            "host": "198.51.100.8",
            "credential": "postgresql://must-not-cross-wire",
        }
        self.ack_result = ack_result or {
            "ok": True,
            "status": "acknowledged",
            "message_id": "message-1",
            "duplicate": False,
            "sender_principal": "subject:must-not-cross-wire",
            "sender_session_id": "session:must-not-cross-wire",
            "dsn": "postgresql://must-not-cross-wire",
        }

    def send_message_from_peer(self, evidence: object, **kwargs: object) -> object:
        self.send_calls.append({"evidence": evidence, **kwargs})
        return self.send_result

    def acknowledge_message_from_peer(
        self, evidence: object, **kwargs: object
    ) -> object:
        self.ack_calls.append({"evidence": evidence, **kwargs})
        return self.ack_result


class _AssignmentWriter:
    def __init__(self, tmp_path: Path) -> None:
        branch = "coord/assignment-child/g1"
        self.start_plan = {
            "assignment_id": "assignment:child",
            "assignment_generation": 1,
            "authenticated_host": "logical:remote-b",
            "repo_path": str((tmp_path / "repo").resolve()),
            "worktree_path": str((tmp_path / "worktree").resolve()),
            "git_branch": branch,
            "git_base_commit": "1" * 40,
            "graph_deployment": "okg-workspace",
            "graph_base_branch": "default",
            "graph_base_generation": "generation-1",
            "graph_base_checkpoint": None,
            "graph_branch": branch,
            "mcp_route": "opaque:remote-b",
            "grant_id": "grant:assignment:child",
            "config_path": None,
            "provision_attempt_id": "start-attempt-1",
        }
        self.request_calls: list[dict[str, object]] = []
        self.prepare_calls: list[dict[str, object]] = []
        self.finalize_calls: list[dict[str, object]] = []

    def request_assignment_from_peer(
        self, evidence: object, **kwargs: object
    ) -> dict[str, object]:
        self.request_calls.append({"evidence": evidence, **kwargs})
        return {
            "ok": True,
            "status": "pending",
            "event_id": "assignment-event-1",
            "assignment_id": "assignment:child",
            "assignment_generation": 1,
            "revision": 0,
            "inserted": True,
            "duplicate": False,
            "owner_principal": "must-not-cross-wire",
            "credential_id": "must-not-cross-wire",
        }

    def prepare_assignment_start_from_peer(
        self, evidence: object, **kwargs: object
    ) -> dict[str, object]:
        self.prepare_calls.append({"evidence": evidence, **kwargs})
        return {
            "ok": True,
            "status": "starting",
            "event_id": "assignment-event-2",
            "assignment_id": "assignment:child",
            "assignment_generation": 1,
            "revision": 1,
            "inserted": True,
            "duplicate": False,
            "start_attempt_id": "start-attempt-1",
            "start_plan": self.start_plan,
            "dsn": "postgresql://must-not-cross-wire",
        }

    def finalize_assignment_start_from_peer(
        self, evidence: object, **kwargs: object
    ) -> dict[str, object]:
        self.finalize_calls.append({"evidence": evidence, **kwargs})
        return {
            "ok": True,
            "status": "active",
            "event_id": "assignment-event-3",
            "assignment_id": "assignment:child",
            "assignment_generation": 1,
            "revision": 2,
            "inserted": True,
            "duplicate": False,
            "start_attempt_id": "start-attempt-1",
            "pair_digest": kwargs["pair_digest"],
            "principal": "must-not-cross-wire",
        }


class _CloseWriter:
    def __init__(self, tmp_path: Path) -> None:
        self.start_plan = {
            "assignment_id": "assignment:close",
            "assignment_generation": 2,
            "authenticated_host": "logical:remote-b",
            "repo_path": str((tmp_path / "repo").resolve()),
            "worktree_path": str((tmp_path / "worktree").resolve()),
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
        }
        self.close_plan = {
            "assignment_id": "assignment:close",
            "assignment_generation": 2,
            "close_attempt_id": "close-attempt-1",
            "readiness_digest": "sha256:" + "a" * 64,
            "pair_digest": "sha256:" + "b" * 64,
            "start_plan": self.start_plan,
        }
        self.prepare_calls: list[dict[str, object]] = []
        self.finalize_calls: list[dict[str, object]] = []

    def prepare_assignment_close_from_peer(
        self, evidence: object, **kwargs: object
    ) -> dict[str, object]:
        self.prepare_calls.append({"evidence": evidence, **kwargs})
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
            "close_plan": self.close_plan,
            "request_digest": "sha256:" + "c" * 64,
        }

    def finalize_assignment_close_from_peer(
        self, evidence: object, **kwargs: object
    ) -> dict[str, object]:
        self.finalize_calls.append({"evidence": evidence, **kwargs})
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
            "cleanup_digest": kwargs["cleanup_digest"],
            "request_digest": "sha256:" + "d" * 64,
        }


class _ReadWriter:
    def __init__(self) -> None:
        self.status_calls: list[dict[str, object]] = []
        self.resume_calls: list[dict[str, object]] = []

    def status_from_peer(self, evidence: object, *, cursor: str | int | None) -> object:
        self.status_calls.append({"evidence": evidence, "cursor": cursor})
        return {
            "ok": True,
            "operation": "status",
            "cursor": "sha256:" + "a" * 64,
            "changed": True,
            "deltas": [
                {
                    "assignment_ref": "sha256:" + "b" * 64,
                    "kind": "pact",
                    "generation": 1,
                    "status": "active",
                    "revision": 3,
                    "owner_ref": "sha256:" + "c" * 64,
                    "session_ref": "sha256:" + "d" * 64,
                    "lease": {"state": "active", "fresh": True, "status": "available"},
                    "resource_demand": {"graph_branch": 1},
                    "workspace": {"status": "available", "dirty": False},
                },
                {
                    "dimension": "assignments",
                    "count": 2,
                    "statuses": ["active"],
                    "blocked_count": 1,
                },
            ],
            "exceptions": [],
            "blockers": [
                {
                    "assignment_ref": "sha256:" + "b" * 64,
                    "state": "blocked",
                    "code": "native_blocker",
                    "reason_ref": "sha256:" + "e" * 64,
                }
            ],
            "freshness": {
                "status": "fresh",
                "live_revision": 3,
                "latest_published_revision": 3,
                "live": {"status": "available", "revision": 3},
                "latest_published": {
                    "status": "available",
                    "revision": 3,
                    "generation": "generation-1",
                },
                "assignments": [
                    {
                        "assignment_ref": "sha256:" + "b" * 64,
                        "live_revision": 3,
                        "published_revision": 3,
                    }
                ],
            },
            "next_action": {"action": "continue"},
        }

    def resume_from_peer(self, evidence: object) -> object:
        self.resume_calls.append({"evidence": evidence})
        return {
            "ok": True,
            "operation": "resume",
            "assignment": {
                "assignment_id": "assignment-1",
                "assignment_ref": "sha256:" + "b" * 64,
                "kind": "pact",
                "generation": 1,
                "status": "active",
                "revision": 3,
                "owner_ref": "sha256:" + "c" * 64,
                "session_ref": "sha256:" + "d" * 64,
                "lease": {"state": "active", "fresh": True, "status": "available"},
                "workspace": {"status": "available", "dirty": False},
                "git": {"branch_ref": "sha256:" + "e" * 64},
                "graph": {"branch_ref": "sha256:" + "f" * 64},
                "pact": {
                    "change_id": "change-1",
                    "task_id": "task-1",
                    "status": "completed",
                    "state": "completed",
                    "done": True,
                    "authority": "live",
                },
                "close": {
                    "phase": "prepared",
                    "next_action": "resume_close_cleanup",
                    "close_attempt_id": "close-attempt-1",
                    "close_readiness_digest": "sha256:" + "9" * 64,
                },
            },
            "assignments": [],
            "hierarchy": {
                "role": "worker",
                "scope": {"change_ref": "sha256:" + "1" * 64},
                "descendant_count": 0,
                "descendants": [],
                "active_children": [],
            },
            "route": {
                "status": "current",
                "assignment_ref": "sha256:" + "b" * 64,
                "generation": 1,
                "host_ref": "sha256:" + "2" * 64,
                "credential_ref": "sha256:" + "3" * 64,
            },
            "lease": {"state": "active", "fresh": True, "status": "available"},
            "reservations": [],
            "pending_approvals": [],
            "messages": [],
            "blockers": [],
            "capacity": {
                "status": "known",
                "lane_usage": {"light": 1},
                "named_resource_usage": {"graph_branch": 1},
                "lane_capacity": {"light": 2},
                "named_resource_capacity": {"graph_branch": 2},
                "reservation_count": 1,
                "overage": False,
            },
            "jobs": {"status": "not_configured"},
            "pair": {
                "status": "known",
                "proof_ref": "sha256:" + "4" * 64,
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
            },
            "exceptions": [],
            "next_action": {"action": "continue"},
        }


def _run(
    tmp_path: Path,
    request: object,
    *,
    writer: _Writer | None = None,
    route: str = "opaque-route",
) -> tuple[int, dict[str, object], _Writer]:
    selected_writer = writer or _Writer()
    out = io.StringIO()
    rc = coordination_ssh.run_forced_command(
        route=route,
        stdin=io.BytesIO(json.dumps(request).encode("utf-8")),
        stdout=out,
        environ={"OKG_PRINCIPAL": "caller-forged"},
        config_loader=lambda: _config(tmp_path),
        writer_factory=lambda _: selected_writer,
        evidence_reader=lambda _: _evidence(),
        policy_loader=lambda _: _policy(),
    )
    return rc, json.loads(out.getvalue()), selected_writer


def test_status_and_resume_use_only_authenticated_evidence_and_project_native_shape(
    tmp_path: Path,
) -> None:
    writer = _ReadWriter()
    rc, status, _ = _run(
        tmp_path,
        {"operation": "status", "cursor": "sha256:" + "1" * 64},
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 0
    assert status["operation"] == "status"
    assert status["changed"] is True
    assert status["deltas"][0]["workspace"]["dirty"] is False
    assert status["deltas"][0]["owner_ref"].startswith("sha256:")
    assert status["deltas"][1]["blocked_count"] == 1
    assert status["blockers"] == [
        {
            "assignment_ref": "sha256:" + "b" * 64,
            "state": "blocked",
            "code": "native_blocker",
            "reason_ref": "sha256:" + "e" * 64,
        }
    ]
    assert writer.status_calls[0]["cursor"] == "sha256:" + "1" * 64
    assert isinstance(writer.status_calls[0]["evidence"], SshdPeerEvidence)

    rc, resume, _ = _run(
        tmp_path,
        {"operation": "resume"},
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 0
    assert resume["operation"] == "resume"
    assert resume["assignment"]["assignment_id"] == "assignment-1"
    assert resume["pending_approvals"] == []
    assert resume["jobs"] == {"status": "not_configured"}
    assert resume["assignment"]["pact"] == {
        "authority": "live",
        "change_id": "change-1",
        "done": True,
        "state": "completed",
        "status": "completed",
        "task_id": "task-1",
    }
    assert resume["assignment"]["close"]["phase"] == "prepared"
    assert resume["next_action"] == {"action": "continue"}
    assert isinstance(writer.resume_calls[0]["evidence"], SshdPeerEvidence)


@pytest.mark.parametrize(
    "request_body",
    [
        {"operation": "status", "cursor": {"revision": 3}},
        {"operation": "status", "cursor": 1},
        {"operation": "status", "cursor": "cursor-1"},
        {"operation": "status", "cursor": "SHA256:" + "a" * 64},
        {"operation": "status", "owner_session_id": "forged"},
        {"operation": "status", "identity": "forged"},
        {"operation": "resume", "cursor": "sha256:" + "1" * 64},
        {"operation": "resume", "assignment_id": "forged"},
    ],
)
def test_status_resume_request_shapes_reject_identity_and_nested_caller_fields(
    tmp_path: Path, request_body: object
) -> None:
    writer = _ReadWriter()
    rc, payload, _ = _run(
        tmp_path,
        request_body,
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 1
    assert payload["reason_code"] == "request_fields_rejected"
    assert not writer.status_calls
    assert not writer.resume_calls


def test_read_result_projector_rejects_secrets_paths_and_unknown_nested_keys() -> None:
    value = {
        "ok": True,
        "operation": "status",
        "cursor": "sha256:" + "a" * 64,
        "changed": False,
        "deltas": [],
        "exceptions": [],
        "blockers": [],
        "freshness": {
            "status": "fresh",
            "live_revision": 1,
            "latest_published_revision": 1,
            "live": {"status": "available", "revision": 1},
            "latest_published": {"status": "available", "revision": 1},
        },
        "next_action": {"action": "continue"},
    }
    assert (
        coordination_ssh._safe_result(value, operation="status")["operation"]
        == "status"
    )
    for leaked in (
        {"dsn": "postgresql://secret"},
        {"cursor": 1},
        {"freshness": {"raw_path": "/private/worktree"}},
        {"freshness": {"live": {"event_payload": {"x": 1}}}},
    ):
        with pytest.raises(
            coordination_ssh.CoordinationSshError, match="writer_refused"
        ):
            coordination_ssh._safe_result({**value, **leaked}, operation="status")


def test_status_failure_keeps_native_bounded_shape_without_blockers() -> None:
    failure = {
        "ok": False,
        "operation": "status",
        "exceptions": [
            {"code": "credential_not_enrolled", "dimension": "authentication"}
        ],
        "freshness": {
            "status": "unavailable",
            "live": {"status": "unavailable"},
            "latest_published": {"status": "unavailable"},
        },
        "next_action": {"action": "reauthenticate"},
        "cursor": None,
        "changed": False,
        "deltas": [],
    }

    assert coordination_ssh._safe_result(failure, operation="status") == failure


def test_valid_request_uses_typed_evidence_and_emits_sanitized_receipt(
    tmp_path: Path,
) -> None:
    rc, payload, writer = _run(
        tmp_path,
        {
            "event_type": "heartbeat",
            "idempotency_key": "request-1",
            "payload": {"status": "alive"},
        },
    )

    assert rc == 0
    assert payload["ok"] is True
    assert payload["event_id"] == "event-1"
    assert payload["assignment_generation"] == 4
    assert len(writer.calls) == 1
    assert isinstance(writer.calls[0]["evidence"], SshdPeerEvidence)
    assert writer.calls[0]["payload"] == {"status": "alive"}
    serialized = json.dumps(payload)
    assert "postgresql://" not in serialized
    assert "198.51.100.8" not in serialized
    assert "credential-not-for-wire" not in serialized


def test_explicit_event_operation_keeps_legacy_writer_route(tmp_path: Path) -> None:
    writer = _Writer()
    rc, payload, selected = _run(
        tmp_path,
        {
            "operation": "event",
            "event_type": "heartbeat",
            "idempotency_key": "request-explicit-event",
        },
        writer=writer,
    )

    assert rc == 0
    assert payload["event_id"] == "event-1"
    assert len(selected.calls) == 1


def test_send_message_uses_only_authenticated_peer_writer_and_safe_receipt(
    tmp_path: Path,
) -> None:
    writer = _MessageWriter()
    rc, payload, _ = _run(
        tmp_path,
        {
            "operation": "send_message",
            "recipient_assignment_id": "assignment:recipient",
            "recipient_generation": 7,
            "kind": "work_result",
            "idempotency_key": "message-request-1",
            "payload": {"result": "ready"},
        },
        writer=writer,  # type: ignore[arg-type]
    )

    assert rc == 0
    assert payload == {
        "duplicate": False,
        "message_id": "message-1",
        "ok": True,
        "status": "pending",
    }
    assert len(writer.send_calls) == 1
    assert isinstance(writer.send_calls[0]["evidence"], SshdPeerEvidence)
    assert writer.send_calls[0]["recipient_assignment_id"] == "assignment:recipient"
    assert writer.send_calls[0]["recipient_generation"] == 7
    assert writer.send_calls[0]["kind"] == "work_result"
    assert writer.send_calls[0]["idempotency_key"] == "message-request-1"
    assert writer.send_calls[0]["payload"] == {"result": "ready"}
    assert not any(
        field in writer.send_calls[0]
        for field in (
            "sender_assignment_id",
            "sender_generation",
            "sender_principal",
            "sender_session_id",
            "parent_assignment_id",
            "lease_expires_at",
            "workspace",
        )
    )
    serialized = json.dumps(payload)
    assert "must-not-cross-wire" not in serialized
    assert "postgresql://" not in serialized


def test_ack_message_derives_recipient_from_peer_and_redacts_route(
    tmp_path: Path,
) -> None:
    writer = _MessageWriter()
    rc, payload, _ = _run(
        tmp_path,
        {
            "operation": "ack_message",
            "message_id": "message-1",
            "idempotency_key": "ack-request-1",
        },
        writer=writer,  # type: ignore[arg-type]
    )

    assert rc == 0
    assert payload == {
        "duplicate": False,
        "message_id": "message-1",
        "ok": True,
        "status": "acknowledged",
    }
    assert len(writer.ack_calls) == 1
    assert writer.ack_calls[0]["message_id"] == "message-1"
    assert writer.ack_calls[0]["idempotency_key"] == "ack-request-1"
    assert set(writer.ack_calls[0]) == {"evidence", "message_id", "idempotency_key"}


def test_assignment_handshake_uses_authenticated_writer_and_closed_pair_proof(
    tmp_path: Path,
) -> None:
    writer = _AssignmentWriter(tmp_path)
    rc, reserved, _ = _run(
        tmp_path,
        {
            "operation": "request_assignment",
            "child_kind": "pact",
            "target_route": "opaque:remote-b",
            "scope": {"change_id": "change-1"},
            "execution_profile": "database-critical",
            "idempotency_key": "request-1",
        },
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 0
    assert reserved["status"] == "pending"
    assert writer.request_calls[0]["scope"] == {"change_id": "change-1"}
    assert "owner_principal" not in reserved

    rc, prepared, _ = _run(
        tmp_path,
        {
            "operation": "prepare_assignment_start",
            "assignment_id": "assignment:child",
            "idempotency_key": "start-1",
        },
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 0
    assert prepared["status"] == "starting"
    assert prepared["start_plan"] == writer.start_plan
    assert "dsn" not in prepared

    pair_proof = {
        "assignment_id": "assignment:child",
        "assignment_generation": 1,
        "lifecycle_status": "active",
        "provision_attempt_id": "start-attempt-1",
        "git_worktree_id": writer.start_plan["worktree_path"],
        "git_branch": writer.start_plan["git_branch"],
        "git_base_commit": "1" * 40,
        "git_head_commit": "1" * 40,
        "git_dirty": False,
        "okg_branch_id": writer.start_plan["graph_branch"],
        "graph_deployment": "okg-workspace",
        "graph_base_branch": "default",
        "graph_base_generation": "generation-1",
        "graph_base_checkpoint": None,
        "graph_current_ref": "ref:coord/assignment-child/g1:0",
        "mcp_route": "opaque:remote-b",
        "grant_id": "grant:assignment:child",
    }
    rc, finalized, _ = _run(
        tmp_path,
        {
            "operation": "finalize_assignment_start",
            "assignment_id": "assignment:child",
            "start_attempt_id": "start-attempt-1",
            "pair_digest": "a" * 64,
            "pair_proof": pair_proof,
            "idempotency_key": "start-1",
        },
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 0
    assert finalized["status"] == "active"
    assert finalized["pair_digest"] == "a" * 64
    assert writer.finalize_calls[0]["pair_proof"] == pair_proof
    assert isinstance(writer.finalize_calls[0]["evidence"], SshdPeerEvidence)


def _cleanup_proof() -> dict[str, object]:
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


def test_assignment_close_uses_two_closed_authenticated_phases(
    tmp_path: Path,
) -> None:
    writer = _CloseWriter(tmp_path)
    rc, prepared, _ = _run(
        tmp_path,
        {
            "operation": "prepare_assignment_close",
            "idempotency_key": "close-request-1",
        },
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 0
    assert prepared["status"] == "closing"
    assert prepared["close_plan"] == writer.close_plan
    assert set(writer.prepare_calls[0]) == {"evidence", "idempotency_key"}
    assert isinstance(writer.prepare_calls[0]["evidence"], SshdPeerEvidence)

    request = {
        "operation": "finalize_assignment_close",
        "close_attempt_id": "close-attempt-1",
        "readiness_digest": "sha256:" + "a" * 64,
        "cleanup_digest": "sha256:" + "e" * 64,
        "cleanup_proof": _cleanup_proof(),
        "idempotency_key": "close-request-1",
    }
    rc, finalized, _ = _run(tmp_path, request, writer=writer)  # type: ignore[arg-type]
    assert rc == 0
    assert finalized == {
        "assignment_generation": 2,
        "assignment_id": "assignment:close",
        "close_attempt_id": "close-attempt-1",
        "cleanup_digest": "sha256:" + "e" * 64,
        "ok": True,
        "readiness_digest": "sha256:" + "a" * 64,
        "status": "retired",
    }
    assert set(writer.finalize_calls[0]) == {
        "evidence",
        "close_attempt_id",
        "readiness_digest",
        "cleanup_digest",
        "cleanup_proof",
        "idempotency_key",
    }
    assert writer.finalize_calls[0]["cleanup_proof"] == request["cleanup_proof"]
    assert isinstance(writer.finalize_calls[0]["evidence"], SshdPeerEvidence)


@pytest.mark.parametrize(
    "request_body",
    [
        {
            "operation": "prepare_assignment_close",
            "idempotency_key": "x",
            "assignment_id": "forged",
        },
        {
            "operation": "finalize_assignment_close",
            "close_attempt_id": "attempt",
            "readiness_digest": "sha256:" + "a" * 64,
            "cleanup_digest": "sha256:" + "b" * 64,
            "cleanup_proof": _cleanup_proof(),
            "idempotency_key": "x",
            "owner_principal": "forged",
        },
        {
            "operation": "finalize_assignment_close",
            "close_attempt_id": "attempt",
            "readiness_digest": "sha256:" + "a" * 64,
            "cleanup_digest": "sha256:" + "b" * 64,
            "cleanup_proof": {**_cleanup_proof(), "errors": ["/secret/path"]},
            "idempotency_key": "x",
        },
    ],
)
def test_assignment_close_request_rejects_identity_and_malformed_proof(
    tmp_path: Path, request_body: object
) -> None:
    writer = _CloseWriter(tmp_path)
    rc, payload, _ = _run(
        tmp_path,
        request_body,
        writer=writer,  # type: ignore[arg-type]
    )
    assert rc == 1
    assert payload["reason_code"] == "request_fields_rejected"
    assert not writer.prepare_calls
    assert not writer.finalize_calls


def test_assignment_close_projector_rejects_plan_on_finalize_and_leaks() -> None:
    with pytest.raises(coordination_ssh.CoordinationSshError, match="writer_refused"):
        coordination_ssh._safe_result(
            {
                "ok": True,
                "status": "retired",
                "close_plan": {},
            },
            operation=coordination_ssh.OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
        )

    with pytest.raises(coordination_ssh.CoordinationSshError, match="writer_refused"):
        coordination_ssh._safe_result(
            {
                "ok": True,
                "status": "closing",
                "close_plan": {
                    "assignment_id": "assignment:close",
                    "assignment_generation": 2,
                    "close_attempt_id": "attempt",
                    "readiness_digest": "sha256:" + "a" * 64,
                    "pair_digest": "sha256:" + "b" * 64,
                    "start_plan": {"repo_path": "/private/secret"},
                },
            },
            operation=coordination_ssh.OPERATION_PREPARE_ASSIGNMENT_CLOSE,
        )


@pytest.mark.parametrize(
    ("operation", "receipt"),
    [
        (
            coordination_ssh.OPERATION_REQUEST_ASSIGNMENT,
            {"ok": True, "status": "pending"},
        ),
        (
            coordination_ssh.OPERATION_PREPARE_ASSIGNMENT_START,
            {
                "ok": True,
                "status": "starting",
                "assignment_id": "assignment:child",
                "assignment_generation": 1,
                "start_attempt_id": "attempt-1",
            },
        ),
        (
            coordination_ssh.OPERATION_FINALIZE_ASSIGNMENT_START,
            {
                "ok": True,
                "status": "active",
                "assignment_id": "assignment:child",
                "assignment_generation": 1,
                "start_attempt_id": "attempt-1",
            },
        ),
        (
            coordination_ssh.OPERATION_FINALIZE_ASSIGNMENT_START,
            {"ok": False, "status": "active", "rejection_code": "stale"},
        ),
    ],
)
def test_assignment_wire_projection_refuses_incomplete_or_inconsistent_receipts(
    operation: str,
    receipt: dict[str, object],
) -> None:
    with pytest.raises(coordination_ssh.CoordinationSshError, match="writer_refused"):
        coordination_ssh._safe_result(receipt, operation=operation)


@pytest.mark.parametrize(
    "request_body",
    [
        {
            "operation": "send_message",
            "recipient_assignment_id": "assignment:recipient",
            "recipient_generation": 7,
            "kind": "work",
            "idempotency_key": "x",
            "sender_assignment_id": "forged",
        },
        {
            "operation": "send_message",
            "recipient_assignment_id": "assignment:recipient",
            "recipient_generation": 7,
            "kind": "work",
            "idempotency_key": "x",
            "payload": {"owner_principal": "forged"},
        },
        {
            "operation": "ack_message",
            "message_id": "message-1",
            "idempotency_key": "x",
            "recipient_principal": "forged",
        },
        {
            "operation": "ack_message",
            "message_id": "message-1",
            "idempotency_key": "x",
            "recipient_assignment_id": "forged",
        },
        {
            "operation": "request_assignment",
            "child_kind": "pact",
            "target_route": "opaque:remote-b",
            "scope": {"change_id": "change-1"},
            "execution_profile": "default-light",
            "idempotency_key": "x",
            "owner_principal": "forged",
        },
        {
            "operation": "request_assignment",
            "child_kind": "delegation",
            "target_route": "opaque:remote-b",
            "scope": {"task_id": "task-1"},
            "execution_profile": "default-light",
            "idempotency_key": "x",
        },
        {"operation": "send_message", "idempotency_key": "x"},
        {"operation": "ack_message", "message_id": "message-1"},
        {"operation": "shell", "event_type": "heartbeat", "idempotency_key": "x"},
    ],
)
def test_message_shapes_and_operations_refuse_before_writer(
    tmp_path: Path, request_body: object
) -> None:
    writer = _MessageWriter()
    rc, payload, _ = _run(
        tmp_path,
        request_body,
        writer=writer,  # type: ignore[arg-type]
    )

    assert rc == 1
    assert payload["status"] == "rejected"
    assert not writer.send_calls
    assert not writer.ack_calls


def test_message_payload_bound_and_writer_method_are_fail_closed(
    tmp_path: Path,
) -> None:
    oversized = {
        "operation": "send_message",
        "recipient_assignment_id": "assignment:recipient",
        "recipient_generation": 7,
        "kind": "work",
        "idempotency_key": "x",
        "payload": {"body": "x" * coordination_ssh.MAX_PAYLOAD_BYTES},
    }
    rc, payload, legacy_writer = _run(tmp_path, oversized)
    assert rc == 1
    assert payload["reason_code"] == "payload_too_large"
    assert not legacy_writer.calls

    # A writer exposing only the legacy event method cannot be used for a
    # message operation, and must not be called as a fallback.
    rc, payload, legacy_writer = _run(
        tmp_path,
        {
            "operation": "send_message",
            "recipient_assignment_id": "assignment:recipient",
            "recipient_generation": 7,
            "kind": "work",
            "idempotency_key": "x",
        },
    )
    assert rc == 1
    assert payload["reason_code"] == "writer_unavailable"
    assert not legacy_writer.calls


@pytest.mark.parametrize(
    "request_body",
    [
        {"event_type": "heartbeat", "idempotency_key": "x", "actor": "forged"},
        {"event_type": "heartbeat"},
        {"event_type": "block", "idempotency_key": "x"},
        {
            "event_type": "heartbeat",
            "idempotency_key": "x",
            "payload": {"owner_session_id": "forged"},
        },
    ],
)
def test_closed_request_and_payload_fields_fail_before_writer(
    tmp_path: Path, request_body: object
) -> None:
    rc, payload, writer = _run(tmp_path, request_body)

    assert rc == 1
    assert payload["ok"] is False
    assert payload["status"] == "rejected"
    assert not writer.calls
    assert set(payload) == {"ok", "reason_code", "status"}


def test_oversized_stdin_is_rejected_before_json_parse(tmp_path: Path) -> None:
    out = io.StringIO()
    writer = _Writer()
    rc = coordination_ssh.run_forced_command(
        route="opaque-route",
        stdin=io.BytesIO(b"{" + b"x" * coordination_ssh.MAX_REQUEST_BYTES),
        stdout=out,
        config_loader=lambda: _config(tmp_path),
        writer_factory=lambda _: writer,
        evidence_reader=lambda _: _evidence(),
        policy_loader=lambda _: _policy(),
    )

    assert rc == 1
    payload = json.loads(out.getvalue())
    assert payload == {
        "ok": False,
        "reason_code": "request_too_large",
        "status": "rejected",
    }
    assert not writer.calls


def test_unknown_route_and_writer_failure_do_not_echo_server_details(
    tmp_path: Path,
) -> None:
    rc, payload, writer = _run(
        tmp_path,
        {"event_type": "heartbeat", "idempotency_key": "x"},
        route="wrong-route",
    )
    assert rc == 1
    assert payload["reason_code"] == "route_not_configured"
    assert not writer.calls

    failing = _Writer(error=RuntimeError("postgresql://secret@db.invalid"))
    rc, payload, _ = _run(
        tmp_path,
        {"event_type": "heartbeat", "idempotency_key": "x"},
        writer=failing,
    )
    assert rc == 1
    assert payload["reason_code"] == "internal_error"
    assert "postgresql://" not in json.dumps(payload)


def test_root_owned_config_and_dsn_files_are_checked(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / "pact").mkdir(parents=True)
    (repo_root / "pact" / "project.yaml").write_text(
        "coordination: {}\n", encoding="utf-8"
    )
    subprocess.run(
        ["/usr/bin/git", "init", "-q", str(repo_root)],
        check=True,
    )
    subprocess.run(
        ["/usr/bin/git", "-C", str(repo_root), "add", "pact/project.yaml"],
        check=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Coordination Test",
            "-c",
            "user.email=coordination@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    expected_commit = subprocess.run(
        ["/usr/bin/git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    fsmonitor_marker = tmp_path / "fsmonitor-executed"
    fsmonitor = tmp_path / "hostile-fsmonitor"
    fsmonitor.write_text(
        f"#!/bin/sh\ntouch {fsmonitor_marker}\nexit 0\n",
        encoding="utf-8",
    )
    os.chmod(fsmonitor, 0o700)
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(repo_root),
            "config",
            "core.fsmonitor",
            str(fsmonitor),
        ],
        check=True,
    )
    dsn_file = tmp_path / "coordination.dsn"
    dsn_file.write_text("postgresql://server-only@db.invalid/coord\n", encoding="utf-8")
    config_file = tmp_path / "coordination-server.json"
    config_file.write_text(
        json.dumps(
            {
                "routes": {
                    "opaque-route": {
                        "deployment_name": "okg-workspace",
                        "repo_root": str(repo_root),
                        "expected_git_commit": expected_commit,
                        "dsn_file": str(dsn_file),
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    os.chmod(dsn_file, 0o600)
    os.chmod(config_file, 0o600)

    config = coordination_ssh.load_server_config(
        config_file,
        expected_uid=os.getuid(),
    )
    resolved = coordination_ssh.resolve_server_route(config, "opaque-route")
    assert resolved.deployment_name == "okg-workspace"
    assert resolved.repo_root == repo_root.resolve()
    assert resolved.expected_git_commit == expected_commit
    assert resolved.dsn.startswith("postgresql://")
    assert not fsmonitor_marker.exists()

    (repo_root / "pact" / "project.yaml").write_text(
        "coordination: {changed: true}\n",
        encoding="utf-8",
    )
    with pytest.raises(coordination_ssh.CoordinationSshError):
        coordination_ssh.load_server_config(
            config_file,
            expected_uid=os.getuid(),
        )
    subprocess.run(
        ["/usr/bin/git", "-C", str(repo_root), "restore", "pact/project.yaml"],
        check=True,
    )

    os.chmod(config_file, 0o640)
    with pytest.raises(coordination_ssh.CoordinationSshError):
        coordination_ssh.load_server_config(config_file, expected_uid=os.getuid())


def test_cli_registers_exact_forced_command_without_loading_local_env(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("{}"))
    assert main(["agent-ssh-run", "--route", "missing-route"]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "rejected"
    assert "coordination-server" not in output.out
    assert output.err == ""


def test_cli_forced_command_rejects_extra_secret_arguments_without_echo(
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "postgresql://agent:CANARY@db.invalid/coord"
    assert main(["agent-ssh-run", "--route", "opaque-route", "--dsn", secret]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["reason_code"] == "request_arguments_rejected"
    assert secret not in output.out
    assert output.err == ""


def test_cli_forced_command_rejects_option_shaped_route_without_argparse_leak(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["agent-ssh-run", "--route", "--not-a-route"]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["reason_code"] == "request_arguments_rejected"
    assert output.err == ""


def test_manifest_identity_and_cached_deployment_module_are_route_scoped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_coordination_modules = {
        module_name: module
        for module_name, module in sys.modules.items()
        if module_name == "coordination" or module_name.startswith("coordination.")
    }
    with pytest.raises(coordination_ssh.CoordinationSshError):
        coordination_ssh._require_manifest_identity("other", "expected")

    deployments = tmp_path / "repo" / "deployments"
    first = deployments / "first"
    second = deployments / "second"
    for directory, marker in ((first, "first"), (second, "second")):
        package = directory / "coordination"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "native_state.py").write_text(
            f"ROUTE_MARKER = {marker!r}\n",
            encoding="utf-8",
        )

    try:
        monkeypatch.syspath_prepend(str(first))
        first_module = coordination_ssh._import_route_native(first)
        assert first_module.ROUTE_MARKER == "first"

        monkeypatch.syspath_prepend(str(second))
        second_module = coordination_ssh._import_route_native(second)
        assert second_module.ROUTE_MARKER == "second"
        assert second_module is not first_module
    finally:
        for module_name in tuple(sys.modules):
            if module_name == "coordination" or module_name.startswith("coordination."):
                sys.modules.pop(module_name, None)
        sys.modules.update(original_coordination_modules)


def test_default_writer_composes_root_selected_production_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_authority, deployments
    from okg.substrate.library.agent_runtimes import production_authority

    repo = tmp_path / "repo"
    manifest_dir = repo / "deployments" / "deployment-a"
    coordination_dir = manifest_dir / "coordination"
    coordination_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "deployment.yaml"
    manifest_path.write_text(
        "name: deployment-a\npostgres:\n  database: db-a\n",
        encoding="utf-8",
    )
    (coordination_dir / "native_state.py").write_text("", encoding="utf-8")
    route = coordination_ssh.CoordinationServerRoute(
        route="opaque:target-a",
        deployment_name="deployment-a",
        repo_root=repo,
        expected_git_commit="a" * 40,
        dsn="postgresql://coord-agent:secret@db.example/db-a",
    )

    class NativeWriter:
        pass

    captured: dict[str, object] = {}
    sentinel = object()

    def resolve_target(dsn: str, **kwargs: object) -> dict[str, str]:
        captured["dsn"] = dsn
        captured["identity"] = kwargs
        return {"identity": "ok"}

    def build(route_value: object, writer_cls: type[object]) -> object:
        captured["route"] = route_value
        captured["writer_cls"] = writer_cls
        return sentinel

    monkeypatch.setattr(coordination_ssh, "_trusted_repo_root", lambda *_a, **_k: repo)
    monkeypatch.setattr(
        coordination_ssh,
        "_import_route_native",
        lambda _path: SimpleNamespace(GuardedCoordinationWriter=NativeWriter),
    )
    monkeypatch.setattr(
        Path,
        "lstat",
        lambda _path: SimpleNamespace(st_uid=0, st_mode=0),
    )
    monkeypatch.setattr(
        deployments.DeploymentManifest,
        "from_yaml",
        lambda _path: SimpleNamespace(name="deployment-a", deployment_dir=manifest_dir),
    )
    monkeypatch.setattr(deployments, "inject_deployment_path", lambda _path: None)
    monkeypatch.setattr(deployment_authority, "resolved_target", resolve_target)
    monkeypatch.setattr(production_authority, "build_production_authority", build)

    assert coordination_ssh._default_writer_factory(route) is sentinel
    assert captured["route"] is route
    assert captured["writer_cls"] is NativeWriter
    assert captured["dsn"] == route.dsn
    assert captured["identity"] == {
        "deployment": "deployment-a",
        "authority_mode": "forced_command",
        "expected_database": "db-a",
        "expected_host": None,
        "expected_port": None,
        "manifest_readable": True,
    }
