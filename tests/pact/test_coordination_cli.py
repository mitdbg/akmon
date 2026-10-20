from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.agent_runtimes.ssh_stdio import SshdPeerEvidence
from okg.substrate.pact import coordination
from okg.substrate.cli.main import main
from okg.substrate.pact.project_settings import coordination_policy


def _repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "pact").mkdir()
    (root / "pact" / "project.yaml").write_text(
        yaml.safe_dump(
            {
                "coordination": {
                    "agent_transport": "restricted_ssh_stdio",
                    "operator_transport": "viz_operator_token",
                    "lease_ttl_seconds": {
                        "chief": 11,
                        "sprint": 22,
                        "pact": 33,
                        "delegation": 44,
                    },
                    "execution": {
                        "lane_capacity": {"critical": 2, "light": 6},
                        "named_resource_capacity": {
                            "database": 1,
                            "shared_storage": 2,
                        },
                        "profiles": {
                            "default-light": {"lane": "light", "resources": {}},
                            "database-critical": {
                                "lane": "critical",
                                "resources": {
                                    "database": 1,
                                    "shared_storage": 1,
                                },
                            },
                        },
                        "helper_budgets": {
                            "chief": {
                                "profile": "default-light",
                                "max_active": 2,
                            },
                            "sprint": {
                                "profile": "default-light",
                                "max_active": 2,
                            },
                            "pact": {
                                "profile": "database-critical",
                                "max_active": 2,
                            },
                        },
                    },
                    "client_routes": {
                        "local-claude": "opaque:local-claude",
                        "local-codex": "opaque:local-codex",
                        "remote-a": "opaque:remote-a",
                        "remote-b": "opaque:remote-b",
                    },
                    "action_registry": {"read": "allow"},
                    "human_involvement_profiles": {
                        "exception_only": {"overrides": {}}
                    },
                    "default_profile": "exception_only",
                    "escalation_route": {
                        "hops": ["worker", "coordinator", "chief", "operator"]
                    },
                    "decision_suppression": {
                        "digest_fields": [
                            "assignment_id",
                            "assignment_generation",
                            "principal",
                            "assignment_session_id",
                            "mandate",
                            "action_digest",
                            "resource_digest",
                            "scope_digest",
                            "human_policy_digest",
                        ],
                        "terminal_statuses": [
                            "granted",
                            "denied",
                            "revoked",
                            "consumed",
                        ],
                    },
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return root


def _evidence() -> SshdPeerEvidence:
    return SshdPeerEvidence(
        credential_id="ssh-auth:sha256:not-for-output",
        source_address="192.0.2.1",
        source_port=41000,
        target_address="198.51.100.8",
        target_port=22,
    )


def _json_output(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    return json.loads(capsys.readouterr().out)


def test_status_is_policy_bound_and_exposes_only_selected_transport(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _repo(tmp_path)

    assert main(["pact", "--root", str(root), "coord", "status", "--json"]) == 0
    payload = _json_output(capsys)
    policy = coordination_policy(root)
    coordination_status = payload["coordination"]
    assert isinstance(coordination_status, dict)
    assert coordination_status["policy"]["policy_digest"] == policy.policy_digest
    assert coordination_status["policy"]["project_digest"] == policy.project_digest
    assert coordination_status["policy"]["capacity_digest"] == policy.capacity_digest
    assert coordination_status["policy"]["lease_ttl_seconds"] == {
        "chief": 11,
        "sprint": 22,
        "pact": 33,
        "delegation": 44,
    }
    assert coordination_status["transport"]["agent_transport"] == (
        "restricted_ssh_stdio"
    )
    assert (
        coordination_status["transport"]["server_entrypoint"]
        == "available_unconfigured"
    )
    assert coordination_status["transport"]["host_configuration"] == "required"
    assert "viz_operator_token" not in json.dumps(payload)


def test_agent_event_ignores_identity_environment_and_requires_sshd_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _repo(tmp_path)
    monkeypatch.setenv("OKG_PRINCIPAL", "subject:forged")
    monkeypatch.setenv("OKG_SESSION_ID", "session:forged")
    monkeypatch.setenv("OKG_ASSIGNMENT_ID", "assignment:forged")
    monkeypatch.setenv("OKG_COORDINATION_LEASE_TTL_SECONDS", "1")
    monkeypatch.delenv("SSH_USER_AUTH", raising=False)
    monkeypatch.delenv("SSH_CONNECTION", raising=False)

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "coord",
            "agent-event",
            "claim",
            "--idempotency-key",
            "request-1",
            "--json",
        ]
    )
    assert rc == 1
    payload = _json_output(capsys)
    assert payload["blocked_by"] == ["transport_evidence_missing"]
    serialized = json.dumps(payload)
    assert "subject:forged" not in serialized
    assert "assignment:forged" not in serialized
    assert "OKG_COORDINATION_LEASE_TTL_SECONDS" not in serialized


@pytest.mark.parametrize(
    "option",
    [
        "--principal",
        "--session",
        "--assignment",
        "--generation",
        "--host",
        "--workspace",
        "--scope",
        "--caller-timestamp",
        "--dsn",
        "--database-credential",
        "--lease-ttl-seconds",
    ],
)
def test_forbidden_agent_flags_are_refused_without_echoing_values(
    tmp_path: Path,
    option: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _repo(tmp_path)
    secret_or_identity = (
        "postgresql://coord-agent:CANARY-PASSWORD@db.invalid/coord"
        if option in {"--dsn", "--database-credential"}
        else "caller-controlled-value"
    )
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "coord",
            "agent-event",
            "claim",
            "--idempotency-key",
            "request-1",
            option,
            secret_or_identity,
            "--json",
        ]
    )
    assert rc == 1
    output = capsys.readouterr()
    assert "caller_fields_rejected" in output.out
    assert secret_or_identity not in output.out
    assert secret_or_identity not in output.err


def test_forbidden_payload_fields_and_values_are_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _repo(tmp_path)
    for payload in (
        {"principal": "subject:forged"},
        {"assignment_generation": 999},
        {"workspace_id": "workspace:forged"},
        {"observed_at": "2000-01-01T00:00:00Z"},
        {"dsn": "postgresql://user:password@db.invalid/coord"},
        {"nested": {"lease_ttl_seconds": 1}},
    ):
        rc = main(
            [
                "pact",
                "--root",
                str(root),
                "coord",
                "agent-event",
                "heartbeat",
                "--idempotency-key",
                "request-1",
                "--payload-json",
                json.dumps(payload),
                "--json",
            ]
        )
        assert rc == 1
        output = capsys.readouterr()
        assert "caller_fields_rejected" in output.out
        assert "password@db.invalid" not in output.out
        assert "2000-01-01" not in output.out


def test_guarded_event_contract_binds_policy_digests_and_uses_policy_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    monkeypatch.setattr(coordination, "read_sshd_peer_evidence", lambda _: _evidence())
    binding = coordination.load_coordination_binding(root)
    event = coordination.build_guarded_event(
        root,
        event_type="heartbeat",
        idempotency_key="request-2",
        payload={"status": "alive"},
        environ={"OKG_PRINCIPAL": "ignored"},
        expected_policy_digest=binding.policy_digest,
        expected_project_digest=binding.project_digest,
    )
    wire = event.to_json()
    assert event.policy.lease_seconds("pact") == 33
    assert wire["policy"]["policy_digest"] == binding.policy_digest
    assert wire["policy"]["project_digest"] == binding.project_digest
    assert wire["payload"] == {"status": "alive"}
    serialized = json.dumps(wire)
    assert "not-for-output" not in serialized
    assert "OKG_DSN" not in serialized
    assert "postgresql://" not in serialized

    with pytest.raises(SubstrateError, match="stale"):
        coordination.build_guarded_event(
            root,
            event_type="heartbeat",
            idempotency_key="request-3",
            environ={},
            expected_policy_digest="sha256:stale",
        )


@pytest.mark.parametrize("event_type", sorted(coordination.GUARDED_EVENT_TYPES))
def test_only_guarded_event_verbs_reach_the_validated_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    event_type: str,
) -> None:
    root = _repo(tmp_path)
    monkeypatch.setattr(coordination, "read_sshd_peer_evidence", lambda _: _evidence())
    event = coordination.build_guarded_event(
        root,
        event_type=event_type,
        idempotency_key=f"request-{event_type}",
        environ={},
    )
    assert event.event_type == event_type
    with pytest.raises(SubstrateError, match="not installed"):
        coordination.submit_guarded_event(event)


def test_unsupported_event_verb_fails_closed_before_transport_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    called = False

    def _unexpected(_: Mapping[str, str]) -> SshdPeerEvidence:
        nonlocal called
        called = True
        return _evidence()

    monkeypatch.setattr(coordination, "read_sshd_peer_evidence", _unexpected)
    with pytest.raises(SubstrateError, match="unsupported"):
        coordination.build_guarded_event(
            root,
            event_type="block",
            idempotency_key="request-bad",
            environ={},
        )
    assert called is False
