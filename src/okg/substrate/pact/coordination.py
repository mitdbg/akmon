"""Bounded coordination command contracts.

The native coordination ledger owns assignment state.  This module only
adapts the selected transport and the tracked project policy into a small
request shape that a server-owned forced-command entrypoint can consume.  It
deliberately does not create a database connection, load a DSN, or implement
another ledger.

The forced-command entrypoint is shipped as ``okg agent-ssh-run``. Host setup
still has to install that command behind sshd and a root-owned route file.
Submission without that server-owned configuration remains an explicit
refusal.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Callable, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.library.agent_runtimes.ssh_stdio import (
    SshTransportAuthenticationError,
    SshdPeerEvidence,
    read_sshd_peer_evidence,
)
from okg.substrate.pact.coordination_contract import (
    COORDINATOR_ASSIGNMENT_KINDS,
    MUTATION_ASSIGNMENT_KINDS,
    MUTATION_LEASE_EVENT_TYPES,
    REPORTED_AGENT_STATES,
)
from okg.substrate.pact.project_settings import (
    CoordinationPolicy,
    coordination_policy,
)


GUARDED_EVENT_TYPES: frozenset[str] = frozenset(
    {"claim", "heartbeat", "renew", "release"}
)
AGENT_TRANSPORT = "restricted_ssh_stdio"
FORCED_COMMAND = "okg agent-ssh-run"
TRANSPORT_ENV_KEYS: frozenset[str] = frozenset({"SSH_USER_AUTH", "SSH_CONNECTION"})

# These fields are owned by the authenticated transport, assignment row, or
# trusted writer.  Rejecting them in this adapter keeps a caller from turning
# a JSON body into a second identity/session/lease protocol.
FORBIDDEN_PAYLOAD_FIELDS: frozenset[str] = frozenset(
    {
        "actor_id",
        "actor_session_id",
        "assignment",
        "assignment_generation",
        "assignment_id",
        "assignment_kind",
        "authenticated_host",
        "caller_time",
        "caller_timestamp",
        "credential",
        "credential_id",
        "database_credential",
        "database_password",
        "dsn",
        "event_kind",
        "event_type",
        "generation",
        "host",
        "lease_expires_at",
        "lease_seconds",
        "lease_ttl",
        "lease_ttl_seconds",
        "observed_at",
        "owner_principal",
        "owner_session_id",
        "principal",
        "received_at",
        "scope",
        "session",
        "session_id",
        "time",
        "timestamp",
        "workspace",
        "workspace_id",
        "workspace_path",
    }
)
FORBIDDEN_PAYLOAD_MARKERS: tuple[str, ...] = (
    "principal",
    "session",
    "assignment",
    "generation",
    "workspace",
    "scope",
    "observed_at",
    "received_at",
    "caller_time",
    "timestamp",
    "event_kind",
    "event_type",
    "lease",
    "password",
    "private_key",
    "secret",
    "token",
    "dsn",
    "credential",
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)(?:postgres(?:ql)?|mysql|mongodb|redis)://|-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"
)


@dataclass(frozen=True, slots=True)
class CoordinationPolicyBinding:
    """Public, non-secret part of the tracked coordination policy."""

    agent_transport: str
    lease_ttl_seconds: Mapping[str, int]
    policy_digest: str
    project_digest: str
    source: str
    # Capacity is tracked separately from the coordination and whole-project
    # digests.  It is public policy metadata; no capacity values are accepted
    # on the agent request wire.
    capacity_digest: str = ""

    @classmethod
    def from_policy(cls, policy: CoordinationPolicy) -> "CoordinationPolicyBinding":
        if policy.agent_transport != AGENT_TRANSPORT:
            raise SubstrateError(
                "coordination agent transport is not the selected restricted SSH/stdio route",
                data={"cause": "unsupported_coordination_transport"},
            )
        return cls(
            agent_transport=policy.agent_transport,
            lease_ttl_seconds=dict(policy.lease_ttl_seconds.to_json()),
            policy_digest=policy.policy_digest,
            project_digest=policy.project_digest,
            source=policy.source,
            capacity_digest=getattr(policy, "capacity_digest", ""),
        )

    def lease_seconds(self, assignment_kind: str) -> int:
        """Return a policy TTL for server-selected assignment kind.

        ``assignment_kind`` is intentionally not accepted by the CLI.  The
        server obtains it from the locked native assignment row.
        """
        kind = str(assignment_kind or "").strip().lower()
        try:
            value = self.lease_ttl_seconds[kind]
        except KeyError as exc:
            raise SubstrateError(
                f"unknown coordination assignment kind: {assignment_kind!r}",
                data={"cause": "unknown_assignment_kind"},
            ) from exc
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise SubstrateError(
                "coordination policy contains an invalid lease TTL",
                data={"cause": "invalid_coordination_policy"},
            )
        return value

    def to_json(self) -> dict[str, Any]:
        """Serialize only policy metadata; never credentials or transport IDs."""
        return {
            "agent_transport": self.agent_transport,
            "lease_ttl_seconds": dict(self.lease_ttl_seconds),
            "policy_digest": self.policy_digest,
            "project_digest": self.project_digest,
            "capacity_digest": self.capacity_digest,
            "source": self.source,
        }


def load_coordination_binding(repo_root: Any) -> CoordinationPolicyBinding:
    """Load the required tracked policy and bind both policy digests."""
    try:
        policy = coordination_policy(repo_root)
    except SubstrateError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise SubstrateError(
            "tracked coordination policy could not be loaded",
            data={"cause": "coordination_policy_unavailable"},
        ) from exc
    return CoordinationPolicyBinding.from_policy(policy)


def _normalize_field_name(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


def _validate_payload_value(value: Any, *, path: str) -> Any:
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, item in value.items():
            name = _normalize_field_name(key)
            if name in FORBIDDEN_PAYLOAD_FIELDS or any(
                marker in name for marker in FORBIDDEN_PAYLOAD_MARKERS
            ):
                raise SubstrateError(
                    f"coordination payload field {key!r} is server-owned or sensitive",
                    data={"cause": "caller_fields_rejected", "path": path},
                )
            clean[str(key)] = _validate_payload_value(item, path=f"{path}.{key}")
        return clean
    if isinstance(value, (list, tuple)):
        return [
            _validate_payload_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        raise SubstrateError(
            "coordination payload contains credential material",
            data={"cause": "caller_fields_rejected", "path": path},
        )
    return value


def parse_event_payload(value: Any = None) -> dict[str, Any]:
    """Parse one JSON object and reject identity, scope, time, or secret input."""
    if value is None or value == "":
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise SubstrateError(
                "coordination event payload is not valid JSON",
                data={"cause": "invalid_coordination_payload"},
            ) from exc
    if not isinstance(value, Mapping):
        raise SubstrateError(
            "coordination event payload must be a JSON object",
            data={"cause": "invalid_coordination_payload"},
        )
    parsed = _validate_payload_value(value, path="$")
    return dict(parsed)


def read_transport_evidence(environ: Mapping[str, str] | None) -> SshdPeerEvidence:
    """Read only sshd-owned evidence; all agent identity variables are ignored."""
    values = environ if environ is not None else {}
    try:
        return read_sshd_peer_evidence(values)
    except SshTransportAuthenticationError as exc:
        raise SubstrateError(
            "authenticated restricted SSH transport evidence is missing or invalid",
            data={"cause": "transport_evidence_missing"},
        ) from exc


@dataclass(frozen=True, slots=True)
class GuardedAgentEvent:
    """Validated request handed to a server-owned guarded writer."""

    event_type: str
    idempotency_key: str
    payload: Mapping[str, Any]
    evidence: SshdPeerEvidence
    policy: CoordinationPolicyBinding

    def to_json(self) -> dict[str, Any]:
        """Return the agent-safe contract without credential or peer details."""
        return {
            "event_type": self.event_type,
            "idempotency_key": self.idempotency_key,
            "payload": dict(self.payload),
            "transport": {
                "agent_transport": self.policy.agent_transport,
                "verified_by": self.evidence.verified_by,
                "forced_command": FORCED_COMMAND,
            },
            "policy": self.policy.to_json(),
        }


def build_guarded_event(
    repo_root: Any,
    *,
    event_type: str,
    idempotency_key: str,
    payload: Any = None,
    environ: Mapping[str, str] | None = None,
    expected_policy_digest: str | None = None,
    expected_project_digest: str | None = None,
) -> GuardedAgentEvent:
    """Validate a guarded event against current policy and SSH evidence.

    Expected digests are server-internal compare values.  They are not CLI
    options and therefore cannot be supplied by an agent invocation.
    """
    normalized_event = _normalize_field_name(event_type)
    if normalized_event not in GUARDED_EVENT_TYPES:
        raise SubstrateError(
            f"unsupported guarded coordination event: {event_type!r}",
            data={"cause": "event_not_allowed"},
        )
    key = str(idempotency_key or "").strip()
    if not key:
        raise SubstrateError(
            "coordination event idempotency key is required",
            data={"cause": "idempotency_key_required"},
        )
    if len(key) > 512:
        raise SubstrateError(
            "coordination event idempotency key is too long",
            data={"cause": "idempotency_key_invalid"},
        )
    policy = load_coordination_binding(repo_root)
    if expected_policy_digest and expected_policy_digest != policy.policy_digest:
        raise SubstrateError(
            "coordination policy digest is stale",
            data={"cause": "stale_coordination_policy"},
        )
    if expected_project_digest and expected_project_digest != policy.project_digest:
        raise SubstrateError(
            "tracked project policy digest is stale",
            data={"cause": "stale_coordination_project"},
        )
    # Reject a caller-owned identity or secret before attempting transport
    # lookup.  This keeps malformed requests deterministic while still
    # refusing all writes when sshd evidence is unavailable.
    parsed_payload = parse_event_payload(payload)
    evidence = read_transport_evidence(environ)
    return GuardedAgentEvent(
        event_type=normalized_event,
        idempotency_key=key,
        payload=parsed_payload,
        evidence=evidence,
        policy=policy,
    )


def submit_guarded_event(
    event: GuardedAgentEvent,
    writer: Callable[..., Any] | None = None,
) -> Any:
    """Submit through an injected server-owned writer, never by opening a DB.

    The CLI leaves ``writer`` unset until the real forced-command server is
    installed.  Keeping this seam explicit lets that server pass the native
    ``GuardedCoordinationWriter.write`` method without making it agent input.
    """
    if writer is None:
        raise SubstrateError(
            "guarded coordination forced-command writer is not installed",
            data={"cause": "forced_command_server_missing"},
        )
    return writer(
        event.evidence,
        event_type=event.event_type,
        idempotency_key=event.idempotency_key,
        payload=event.payload,
    )


def transport_status(binding: CoordinationPolicyBinding) -> dict[str, Any]:
    """Describe the selected route without claiming the server is installed."""
    return {
        "agent_transport": binding.agent_transport,
        "capacity_digest": binding.capacity_digest,
        "forced_command": FORCED_COMMAND,
        "evidence": sorted(TRANSPORT_ENV_KEYS),
        "guarded_event_types": sorted(GUARDED_EVENT_TYPES),
        # Coordinator contract vocabulary.  Coordinator kinds never take or
        # extend a mutation lease, and idle/blocked are reported events that
        # carry a reason rather than states the operator has to discover.
        "coordinator_assignment_kinds": sorted(COORDINATOR_ASSIGNMENT_KINDS),
        "mutation_assignment_kinds": sorted(MUTATION_ASSIGNMENT_KINDS),
        "mutation_lease_event_types": sorted(MUTATION_LEASE_EVENT_TYPES),
        "reported_agent_states": sorted(REPORTED_AGENT_STATES),
        "server_entrypoint": "available_unconfigured",
        "host_configuration": "required",
    }
