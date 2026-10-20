"""The server side of the restricted coordination SSH route.

``paired_workspace`` deliberately writes only an opaque route and grant to an
agent's local MCP configuration.  This module is the other half of that
contract: sshd starts ``okg agent-ssh-run --route <route>`` and this process
accepts one small JSON request on stdin.  The process is not a general shell
and it never accepts identity, assignment, scope, time, lease, or credential
claims from the caller.

The real deployment supplies a root-owned server configuration and DSN file.
Tests inject those readers and the writer, so the request parser and wire
contract can be exercised without a database.  Host installation (sshd's
``ForceCommand``, key enrollment, and forwarding restrictions) remains an
operations concern; this module does not attempt to mutate a host.
"""

from __future__ import annotations

import importlib
import io
import json
import os
import re
import select
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, TextIO

from okg.substrate.library.agent_runtimes.assignment_handshake import (
    AssignmentHandshakeError,
    AssignmentStartPlan,
    PAIR_PROOF_FIELDS,
)
from okg.substrate.pact.coordination import (
    GUARDED_EVENT_TYPES,
    GuardedAgentEvent,
    CoordinationPolicyBinding,
    load_coordination_binding,
    parse_event_payload,
    submit_guarded_event,
)
from okg.substrate.library.agent_runtimes.ssh_stdio import (
    SshTransportAuthenticationError,
    SshdPeerEvidence,
    read_sshd_peer_evidence,
)


MAX_REQUEST_BYTES = 64 * 1024
# Compatibility spelling used by transport/runbook callers.
MAX_STDIN_BYTES = MAX_REQUEST_BYTES
MAX_RESPONSE_BYTES = 64 * 1024
MAX_REQUEST_WAIT_SECONDS = 30.0
MAX_PAYLOAD_BYTES = 16 * 1024
MAX_CONFIG_BYTES = 64 * 1024
MAX_DSN_BYTES = 16 * 1024
DEFAULT_SERVER_CONFIG_PATH = Path("/etc/okg/coordination-server.json")

# ``operation`` was added after the event-only route shipped.  Omitting it is
# deliberately equivalent to ``event`` so existing forced-command requests
# remain wire-compatible.  Message operations have separate closed shapes
# below; this union is only the first, non-sensitive field-name gate.
OPERATION_EVENT = "event"
OPERATION_SEND_MESSAGE = "send_message"
OPERATION_ACK_MESSAGE = "ack_message"
OPERATION_REQUEST_ASSIGNMENT = "request_assignment"
OPERATION_PREPARE_ASSIGNMENT_START = "prepare_assignment_start"
OPERATION_FINALIZE_ASSIGNMENT_START = "finalize_assignment_start"
OPERATION_STATUS = "status"
OPERATION_RESUME = "resume"
OPERATION_PREPARE_ASSIGNMENT_CLOSE = "prepare_assignment_close"
OPERATION_FINALIZE_ASSIGNMENT_CLOSE = "finalize_assignment_close"
REQUEST_FIELDS = frozenset(
    {
        "operation",
        "event_type",
        "idempotency_key",
        "payload",
        "recipient_assignment_id",
        "recipient_generation",
        "kind",
        "message_id",
        "child_kind",
        "target_route",
        "scope",
        "execution_profile",
        "assignment_id",
        "start_attempt_id",
        "pair_digest",
        "pair_proof",
        "cursor",
        "close_attempt_id",
        "readiness_digest",
        "cleanup_digest",
        "cleanup_proof",
        "close_plan",
    }
)
_OPERATION_ALIASES = {
    OPERATION_EVENT: OPERATION_EVENT,
    "guarded_event": OPERATION_EVENT,
    OPERATION_SEND_MESSAGE: OPERATION_SEND_MESSAGE,
    "message_send": OPERATION_SEND_MESSAGE,
    OPERATION_ACK_MESSAGE: OPERATION_ACK_MESSAGE,
    "message_ack": OPERATION_ACK_MESSAGE,
    "acknowledge_message": OPERATION_ACK_MESSAGE,
    OPERATION_REQUEST_ASSIGNMENT: OPERATION_REQUEST_ASSIGNMENT,
    OPERATION_PREPARE_ASSIGNMENT_START: OPERATION_PREPARE_ASSIGNMENT_START,
    OPERATION_FINALIZE_ASSIGNMENT_START: OPERATION_FINALIZE_ASSIGNMENT_START,
    OPERATION_STATUS: OPERATION_STATUS,
    OPERATION_RESUME: OPERATION_RESUME,
    OPERATION_PREPARE_ASSIGNMENT_CLOSE: OPERATION_PREPARE_ASSIGNMENT_CLOSE,
    OPERATION_FINALIZE_ASSIGNMENT_CLOSE: OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
}
_EVENT_REQUEST_FIELDS = frozenset(
    {"operation", "event_type", "idempotency_key", "payload"}
)
_LEGACY_EVENT_REQUEST_FIELDS = frozenset({"event_type", "idempotency_key", "payload"})
_SEND_MESSAGE_FIELDS = frozenset(
    {
        "operation",
        "recipient_assignment_id",
        "recipient_generation",
        "kind",
        "idempotency_key",
        "payload",
    }
)
_ACK_MESSAGE_FIELDS = frozenset({"operation", "message_id", "idempotency_key"})
_REQUEST_ASSIGNMENT_FIELDS = frozenset(
    {
        "operation",
        "child_kind",
        "target_route",
        "scope",
        "execution_profile",
        "idempotency_key",
    }
)
_PREPARE_ASSIGNMENT_START_FIELDS = frozenset(
    {"operation", "assignment_id", "idempotency_key"}
)
_FINALIZE_ASSIGNMENT_START_FIELDS = frozenset(
    {
        "operation",
        "assignment_id",
        "start_attempt_id",
        "pair_digest",
        "pair_proof",
        "idempotency_key",
    }
)
_STATUS_REQUEST_FIELDS = frozenset({"operation", "cursor"})
_RESUME_REQUEST_FIELDS = frozenset({"operation"})
_PREPARE_ASSIGNMENT_CLOSE_FIELDS = frozenset({"operation", "idempotency_key"})
_FINALIZE_ASSIGNMENT_CLOSE_FIELDS = frozenset(
    {
        "operation",
        "close_attempt_id",
        "readiness_digest",
        "cleanup_digest",
        "cleanup_proof",
        "idempotency_key",
    }
)
_ASSIGNMENT_SCOPE_FIELDS = frozenset(
    {"project_id", "sprint_id", "change_id", "task_id"}
)
_CHILD_KINDS = frozenset({"sprint", "pact"})
_MESSAGE_CAPACITY_FIELDS = frozenset(
    {
        "capacity",
        "capacity_digest",
        "execution_lane",
        "execution_profile",
        "lane_capacity",
        "named_resource_demand",
        "resource_capacity",
    }
)
ROUTE_FIELDS = frozenset(
    {"deployment_name", "repo_root", "expected_git_commit", "dsn_file"}
)
CONFIG_FIELDS = frozenset({"routes"})


class CoordinationSshError(RuntimeError):
    """A fail-closed forced-command refusal.

    The ``reason_code`` is an intentionally small, non-sensitive vocabulary.
    Callers never receive the underlying filesystem, parser, SSH, or database
    exception because those often contain paths, peer details, or DSNs.
    """

    def __init__(self, reason_code: str) -> None:
        self.reason_code = _safe_reason_code(reason_code)
        super().__init__(self.reason_code)


@dataclass(frozen=True, slots=True)
class CoordinationServerRoute:
    """Server-owned route resolution.

    ``dsn`` is intentionally excluded from repr/equality so a config object
    cannot accidentally put the database credential into a receipt, test
    failure, or log line.
    """

    route: str
    deployment_name: str
    repo_root: Path
    expected_git_commit: str
    dsn: str = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class CoordinationServerConfig:
    routes: Mapping[str, CoordinationServerRoute]


def _safe_reason_code(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if not text or len(text) > 96 or not text.replace("_", "").isalnum():
        return "coordination_refused"
    return text


def _safe_route(value: Any) -> str:
    if not isinstance(value, str):
        raise CoordinationSshError("invalid_route")
    route = value.strip()
    if (
        not route
        or len(route) > 256
        or route.startswith("-")
        or any(char.isspace() or char in "/\\" for char in route)
    ):
        raise CoordinationSshError("invalid_route")
    return route


def _read_server_file(
    path: Path,
    *,
    max_bytes: int,
    expected_uid: int | None = 0,
) -> bytes:
    """Read one root-owned regular file without following a symlink.

    ``expected_uid=None`` is not a production default; it is a narrow test
    seam for hosts whose fixture owner is not uid 0.  Production callers use
    the default and therefore require root ownership and private permissions.
    """

    if not isinstance(path, Path) or not path.is_absolute():
        raise CoordinationSshError("server_configuration_unavailable")
    if not _safe_server_directory(path.parent, expected_uid=expected_uid):
        raise CoordinationSshError("server_configuration_unavailable")
    try:
        initial = path.lstat()
    except OSError as exc:
        raise CoordinationSshError("server_configuration_unavailable") from exc
    if not _safe_server_file_stat(initial, expected_uid=expected_uid):
        raise CoordinationSshError("server_configuration_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise CoordinationSshError("server_configuration_unavailable") from exc
    try:
        opened = os.fstat(fd)
        if not _safe_server_file_stat(opened, expected_uid=expected_uid):
            raise CoordinationSshError("server_configuration_unavailable")
        chunks: list[bytes] = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(fd, min(65536, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > max_bytes:
            raise CoordinationSshError("server_configuration_unavailable")
        return b"".join(chunks)
    except CoordinationSshError:
        raise
    except OSError as exc:
        raise CoordinationSshError("server_configuration_unavailable") from exc
    finally:
        os.close(fd)


def _safe_server_file_stat(value: os.stat_result, *, expected_uid: int | None) -> bool:
    if not stat.S_ISREG(value.st_mode) or stat.S_ISLNK(value.st_mode):
        return False
    if expected_uid is not None and value.st_uid != expected_uid:
        return False
    # Group/world readable config or DSN files let an unrelated local process
    # read the server credential.  The server route is intentionally stricter
    # than an ordinary application config.
    return not bool(value.st_mode & 0o077)


def _safe_server_directory(path: Path, *, expected_uid: int | None) -> bool:
    """Require the containing directory to resist file replacement."""

    try:
        value = path.lstat()
    except OSError:
        return False
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        return False
    if expected_uid is not None and value.st_uid != expected_uid:
        return False
    return not bool(value.st_mode & 0o022)


def _decode_json(raw: bytes, *, reason_code: str) -> Any:
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, CoordinationSshError) as exc:
        raise CoordinationSshError(reason_code) from exc


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CoordinationSshError("invalid_request")
        result[key] = value
    return result


def load_server_config(
    path: Path = DEFAULT_SERVER_CONFIG_PATH,
    *,
    expected_uid: int | None = 0,
) -> CoordinationServerConfig:
    """Load route metadata from the root-owned server configuration.

    The config contains no inline DSN.  Each route points at a second
    root-owned DSN file, which keeps the credential out of process arguments,
    route receipts, and ordinary configuration dumps.
    """

    raw = _read_server_file(
        Path(path), max_bytes=MAX_CONFIG_BYTES, expected_uid=expected_uid
    )
    decoded = _decode_json(raw, reason_code="server_configuration_unavailable")
    if not isinstance(decoded, Mapping) or set(decoded) != CONFIG_FIELDS:
        raise CoordinationSshError("server_configuration_unavailable")
    routes_raw = decoded.get("routes")
    if not isinstance(routes_raw, Mapping) or not routes_raw:
        raise CoordinationSshError("server_configuration_unavailable")
    routes: dict[str, CoordinationServerRoute] = {}
    for raw_route, raw_config in routes_raw.items():
        route = _safe_route(raw_route)
        if route in routes or not isinstance(raw_config, Mapping):
            raise CoordinationSshError("server_configuration_unavailable")
        if set(raw_config) != ROUTE_FIELDS:
            raise CoordinationSshError("server_configuration_unavailable")
        deployment_name = raw_config.get("deployment_name")
        repo_root_raw = raw_config.get("repo_root")
        expected_git_commit = raw_config.get("expected_git_commit")
        dsn_file_raw = raw_config.get("dsn_file")
        if (
            not isinstance(deployment_name, str)
            or not deployment_name.strip()
            or not isinstance(repo_root_raw, str)
            or not isinstance(expected_git_commit, str)
            or not isinstance(dsn_file_raw, str)
        ):
            raise CoordinationSshError("server_configuration_unavailable")
        expected_git_commit = _safe_git_commit(expected_git_commit)
        repo_root = _trusted_repo_root(
            repo_root_raw,
            expected_git_commit=expected_git_commit,
            expected_uid=expected_uid,
        )
        dsn_file = Path(dsn_file_raw)
        dsn_raw = _read_server_file(
            dsn_file, max_bytes=MAX_DSN_BYTES, expected_uid=expected_uid
        )
        try:
            dsn = dsn_raw.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise CoordinationSshError("server_configuration_unavailable") from exc
        if not dsn or any(char in dsn for char in "\r\n") or "${" in dsn:
            raise CoordinationSshError("server_configuration_unavailable")
        routes[route] = CoordinationServerRoute(
            route=route,
            deployment_name=deployment_name.strip(),
            repo_root=repo_root,
            expected_git_commit=expected_git_commit,
            dsn=dsn,
        )
    return CoordinationServerConfig(routes=routes)


def _safe_git_commit(value: Any) -> str:
    commit = str(value or "").strip().lower()
    if len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        raise CoordinationSshError("server_configuration_unavailable")
    return commit


def _trusted_repo_root(
    value: str,
    *,
    expected_git_commit: str,
    expected_uid: int | None = 0,
) -> Path:
    path = Path(value.strip())
    if not path.is_absolute():
        raise CoordinationSshError("server_configuration_unavailable")
    try:
        unresolved = path.lstat()
        if stat.S_ISLNK(unresolved.st_mode):
            raise CoordinationSshError("server_configuration_unavailable")
        root = path.resolve(strict=True)
        pact_file = root / "pact" / "project.yaml"
        if not root.is_dir() or not pact_file.is_file():
            raise CoordinationSshError("server_configuration_unavailable")
    except OSError as exc:
        raise CoordinationSshError("server_configuration_unavailable") from exc
    for trusted_path in (root, root / "pact", pact_file):
        try:
            value_stat = trusted_path.lstat()
        except OSError as exc:
            raise CoordinationSshError("server_configuration_unavailable") from exc
        if stat.S_ISLNK(value_stat.st_mode):
            raise CoordinationSshError("server_configuration_unavailable")
        if expected_uid is not None and value_stat.st_uid != expected_uid:
            raise CoordinationSshError("server_configuration_unavailable")
        if value_stat.st_mode & 0o022:
            raise CoordinationSshError("server_configuration_unavailable")
    expected_git_commit = _safe_git_commit(expected_git_commit)
    git = Path("/usr/bin/git")
    if not git.is_file():
        raise CoordinationSshError("server_configuration_unavailable")
    safe_env = {
        "PATH": "/usr/bin:/bin",
        "LC_ALL": "C",
        "LANG": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
    }
    git_probe = [
        str(git),
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-C",
        str(root),
    ]
    try:
        head = (
            subprocess.run(
                [*git_probe, "rev-parse", "--verify", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
                env=safe_env,
            )
            .stdout.strip()
            .lower()
        )
        dirty = subprocess.run(
            [*git_probe, "status", "--porcelain=v1", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
            env=safe_env,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise CoordinationSshError("server_configuration_unavailable") from exc
    if head != expected_git_commit or dirty:
        raise CoordinationSshError("server_configuration_unavailable")
    return root


def resolve_server_route(
    config: CoordinationServerConfig | Mapping[str, Any], route: str
) -> CoordinationServerRoute:
    """Resolve an opaque route without exposing route configuration."""

    route = _safe_route(route)
    if isinstance(config, CoordinationServerConfig):
        resolved = config.routes.get(route)
    elif isinstance(config, Mapping):
        # Injection is intentionally typed to the same public dataclass in
        # production.  This mapping branch keeps hermetic tests convenient but
        # still validates through the exact config parser rules.
        try:
            parsed = _coerce_injected_config(config)
        except Exception as exc:  # noqa: BLE001 - normalize all config errors
            raise CoordinationSshError("server_configuration_unavailable") from exc
        resolved = parsed.routes.get(route)
    else:
        resolved = None
    if resolved is None:
        raise CoordinationSshError("route_not_configured")
    return resolved


def _coerce_injected_config(value: Mapping[str, Any]) -> CoordinationServerConfig:
    routes_raw = value.get("routes")
    if not isinstance(routes_raw, Mapping):
        raise ValueError("routes")
    routes: dict[str, CoordinationServerRoute] = {}
    for raw_route, raw_value in routes_raw.items():
        route = _safe_route(raw_route)
        if isinstance(raw_value, CoordinationServerRoute):
            routes[route] = raw_value
            continue
        if not isinstance(raw_value, Mapping):
            raise ValueError("route")
        deployment_name = raw_value.get("deployment_name")
        repo_root = raw_value.get("repo_root")
        expected_git_commit = raw_value.get("expected_git_commit")
        dsn = raw_value.get("dsn")
        if not all(
            isinstance(item, str) and item.strip()
            for item in (deployment_name, expected_git_commit, dsn)
        ):
            raise ValueError("route")
        root = Path(repo_root) if isinstance(repo_root, str) else repo_root
        if not isinstance(root, Path):
            raise ValueError("repo_root")
        if "${" in dsn or any(char in dsn for char in "\r\n"):
            raise ValueError("dsn")
        routes[route] = CoordinationServerRoute(
            route=route,
            deployment_name=deployment_name.strip(),
            repo_root=root,
            expected_git_commit=_safe_git_commit(expected_git_commit),
            dsn=dsn.strip(),
        )
    return CoordinationServerConfig(routes=routes)


def read_bounded_request(
    stdin: io.BufferedIOBase | io.BytesIO | TextIO,
    *,
    max_bytes: int = MAX_REQUEST_BYTES,
    max_wait_seconds: float = MAX_REQUEST_WAIT_SECONDS,
) -> Mapping[str, Any]:
    """Read one closed JSON request with a byte and initial-read bound.

    OpenSSH provides the forced command a pipe/pty-backed stdin, so ``select``
    can bound the wait before the first request byte arrives.  In-memory test
    streams do not expose a file descriptor and are read directly.
    """

    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if (
        isinstance(max_wait_seconds, bool)
        or not isinstance(max_wait_seconds, (int, float))
        or max_wait_seconds <= 0
    ):
        raise ValueError("max_wait_seconds must be positive")
    try:
        fileno = stdin.fileno()  # type: ignore[attr-defined]
    except (AttributeError, OSError, ValueError):
        fileno = None
    if fileno is None:
        try:
            raw = stdin.read(max_bytes + 1)
        except (OSError, ValueError) as exc:
            raise CoordinationSshError("invalid_request") from exc
    else:
        # Do not call buffered ``read(max_bytes + 1)`` on a live SSH pipe: it
        # can wait forever for EOF after receiving a valid JSON line.  Read in
        # bounded chunks under one deadline and stop once the request line is
        # complete; the forced command's one-request contract makes trailing
        # protocol data invalid below.
        chunks: list[bytes] = []
        total = 0
        deadline = time.monotonic() + float(max_wait_seconds)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CoordinationSshError("request_timeout")
            try:
                ready, _, _ = select.select([fileno], [], [], remaining)
            except (OSError, ValueError) as exc:
                raise CoordinationSshError("request_timeout") from exc
            if not ready:
                raise CoordinationSshError("request_timeout")
            try:
                chunk = os.read(fileno, min(65536, max_bytes + 1 - total))
            except OSError as exc:
                raise CoordinationSshError("invalid_request") from exc
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise CoordinationSshError("request_too_large")
            # The client writes one compact JSON document followed by a
            # newline.  Parsing at that boundary prevents a caller that keeps
            # stdin open from holding the forced command indefinitely.
            if b"\n" in chunk:
                break
        raw = b"".join(chunks)
    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    elif isinstance(raw, bytes):
        raw_bytes = raw
    else:
        raise CoordinationSshError("invalid_request")
    if len(raw_bytes) > max_bytes:
        raise CoordinationSshError("request_too_large")
    decoded = _decode_json(raw_bytes, reason_code="invalid_request")
    if not isinstance(decoded, Mapping) or not set(decoded).issubset(REQUEST_FIELDS):
        raise CoordinationSshError("request_fields_rejected")
    operation = _request_operation(decoded)
    if operation == OPERATION_EVENT:
        allowed = (
            _LEGACY_EVENT_REQUEST_FIELDS
            if "operation" not in decoded
            else _EVENT_REQUEST_FIELDS
        )
        if not {"event_type", "idempotency_key"}.issubset(decoded) or not set(
            decoded
        ).issubset(allowed):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_SEND_MESSAGE:
        if not {
            "recipient_assignment_id",
            "recipient_generation",
            "kind",
            "idempotency_key",
        }.issubset(decoded) or not set(decoded).issubset(_SEND_MESSAGE_FIELDS):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_ACK_MESSAGE:
        if not {"message_id", "idempotency_key"}.issubset(decoded) or not set(
            decoded
        ).issubset(_ACK_MESSAGE_FIELDS):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_REQUEST_ASSIGNMENT:
        if not {
            "child_kind",
            "target_route",
            "scope",
            "idempotency_key",
        }.issubset(decoded) or not set(decoded).issubset(_REQUEST_ASSIGNMENT_FIELDS):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_PREPARE_ASSIGNMENT_START:
        if not {"assignment_id", "idempotency_key"}.issubset(decoded) or not set(
            decoded
        ).issubset(_PREPARE_ASSIGNMENT_START_FIELDS):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_FINALIZE_ASSIGNMENT_START:
        if not {
            "assignment_id",
            "start_attempt_id",
            "pair_digest",
            "pair_proof",
            "idempotency_key",
        }.issubset(decoded) or not set(decoded).issubset(
            _FINALIZE_ASSIGNMENT_START_FIELDS
        ):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_STATUS:
        if not set(decoded).issubset(_STATUS_REQUEST_FIELDS):
            raise CoordinationSshError("request_fields_rejected")
        if "cursor" in decoded:
            _request_cursor(decoded["cursor"])
    elif operation == OPERATION_RESUME:
        # Resume is deliberately a fresh authenticated read. A stale or
        # caller-created cursor must not select a different assignment view.
        if not set(decoded).issubset(_RESUME_REQUEST_FIELDS):
            raise CoordinationSshError("request_fields_rejected")
    elif operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE:
        if not {"idempotency_key"}.issubset(decoded) or not set(decoded).issubset(
            _PREPARE_ASSIGNMENT_CLOSE_FIELDS
        ):
            raise CoordinationSshError("request_fields_rejected")
        _request_text(decoded["idempotency_key"], field="idempotency_key")
    elif operation == OPERATION_FINALIZE_ASSIGNMENT_CLOSE:
        if not {
            "close_attempt_id",
            "readiness_digest",
            "cleanup_digest",
            "cleanup_proof",
            "idempotency_key",
        }.issubset(decoded) or not set(decoded).issubset(
            _FINALIZE_ASSIGNMENT_CLOSE_FIELDS
        ):
            raise CoordinationSshError("request_fields_rejected")
        _build_assignment_close_finalize(decoded)
    return decoded


def _request_operation(request: Mapping[str, Any]) -> str:
    """Resolve the closed operation name, retaining event-only compatibility."""

    if "operation" not in request:
        return OPERATION_EVENT
    raw_operation = request["operation"]
    if not isinstance(raw_operation, str):
        raise CoordinationSshError("operation_not_allowed")
    operation = _OPERATION_ALIASES.get(raw_operation.strip().lower().replace("-", "_"))
    if operation is None:
        raise CoordinationSshError("operation_not_allowed")
    return operation


def _bounded_payload(value: Any) -> dict[str, Any]:
    """Validate the caller payload and enforce the native message bound."""

    try:
        payload = parse_event_payload(value)
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except Exception as exc:  # noqa: BLE001 - caller details stay off wire
        raise CoordinationSshError("caller_fields_rejected") from exc
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise CoordinationSshError("payload_too_large")
    _reject_message_capacity_fields(payload, path="$")
    return payload


def _reject_message_capacity_fields(value: Any, *, path: str) -> None:
    """Keep capacity policy server-owned, including inside nested payloads."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key).strip().lower().replace("-", "_")
            if name in _MESSAGE_CAPACITY_FIELDS or "capacity" in name:
                raise CoordinationSshError("caller_fields_rejected")
            _reject_message_capacity_fields(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_message_capacity_fields(item, path=f"{path}[{index}]")


def _request_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise CoordinationSshError("request_fields_rejected")
    result = value.strip()
    if not result or len(result) > 512 or "\n" in result or "\r" in result:
        raise CoordinationSshError("request_fields_rejected")
    return result


def _request_generation(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise CoordinationSshError("request_fields_rejected")
    return value


def _request_cursor(value: Any) -> str:
    """Validate one small status cursor without accepting nested caller data."""

    if not isinstance(value, str):
        raise CoordinationSshError("request_fields_rejected")
    cursor = value.strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", cursor):
        raise CoordinationSshError("request_fields_rejected")
    return cursor


_CLOSE_PROOF_FIELDS = frozenset(
    {
        "assignment_id",
        "assignment_generation",
        "close_attempt_id",
        "readiness_digest",
        "git_removed",
        "graph_removed",
        "config_removed",
        "complete",
        "disposition",
        "errors",
    }
)
_SHA256_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def _close_identifier(value: Any, *, field: str) -> str:
    result = _request_text(value, field=field)
    lowered = result.lower()
    if (
        result.startswith(("/", "~"))
        or "/" in result
        or "\\" in result
        or "://" in lowered
        or any(
            marker in lowered for marker in ("password", "secret", "credential", "dsn")
        )
    ):
        raise CoordinationSshError("request_fields_rejected")
    return result


def _close_digest(value: Any, *, field: str) -> str:
    result = _close_identifier(value, field=field).lower()
    if not _SHA256_DIGEST_RE.fullmatch(result):
        raise CoordinationSshError("request_fields_rejected")
    return result


def _build_assignment_close_prepare(
    request: Mapping[str, Any],
) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_PREPARE_ASSIGNMENT_CLOSE:
        raise CoordinationSshError("request_fields_rejected")
    return {
        "idempotency_key": _close_identifier(
            request.get("idempotency_key"), field="idempotency_key"
        )
    }


def _build_assignment_close_finalize(
    request: Mapping[str, Any],
) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_FINALIZE_ASSIGNMENT_CLOSE:
        raise CoordinationSshError("request_fields_rejected")
    proof = request.get("cleanup_proof")
    if not isinstance(proof, Mapping) or set(proof) != _CLOSE_PROOF_FIELDS:
        raise CoordinationSshError("request_fields_rejected")
    assignment_generation = proof.get("assignment_generation")
    if (
        isinstance(assignment_generation, bool)
        or not isinstance(assignment_generation, int)
        or assignment_generation < 1
    ):
        raise CoordinationSshError("request_fields_rejected")
    safe_proof = {
        "assignment_id": _close_identifier(
            proof.get("assignment_id"), field="cleanup_proof.assignment_id"
        ),
        "assignment_generation": assignment_generation,
        "close_attempt_id": _close_identifier(
            proof.get("close_attempt_id"), field="cleanup_proof.close_attempt_id"
        ),
        "readiness_digest": _close_digest(
            proof.get("readiness_digest"), field="cleanup_proof.readiness_digest"
        ),
    }
    for proof_field in ("git_removed", "graph_removed", "config_removed", "complete"):
        value = proof.get(proof_field)
        if not isinstance(value, bool):
            raise CoordinationSshError("request_fields_rejected")
        safe_proof[proof_field] = value
    if safe_proof["complete"] is not True or proof.get("disposition") != "complete":
        raise CoordinationSshError("request_fields_rejected")
    safe_proof["disposition"] = "complete"
    errors = proof["errors"]
    if not isinstance(errors, list) or errors:
        raise CoordinationSshError("request_fields_rejected")
    safe_proof["errors"] = []
    return {
        "close_attempt_id": _close_identifier(
            request.get("close_attempt_id"), field="close_attempt_id"
        ),
        "readiness_digest": _close_digest(
            request.get("readiness_digest"), field="readiness_digest"
        ),
        "cleanup_digest": _close_digest(
            request.get("cleanup_digest"), field="cleanup_digest"
        ),
        "cleanup_proof": safe_proof,
        "idempotency_key": _close_identifier(
            request.get("idempotency_key"), field="idempotency_key"
        ),
    }


def _build_event(
    request: Mapping[str, Any],
    *,
    evidence: SshdPeerEvidence,
    policy: CoordinationPolicyBinding,
) -> GuardedAgentEvent:
    operation = _request_operation(request)
    allowed_fields = (
        _LEGACY_EVENT_REQUEST_FIELDS
        if "operation" not in request
        else _EVENT_REQUEST_FIELDS
    )
    if operation != OPERATION_EVENT or not set(request).issubset(allowed_fields):
        raise CoordinationSshError("request_fields_rejected")
    event_type = request.get("event_type")
    key = request.get("idempotency_key")
    if not isinstance(event_type, str) or not isinstance(key, str):
        raise CoordinationSshError("request_fields_rejected")
    event_type = event_type.strip().replace("-", "_").lower()
    key = key.strip()
    if event_type not in GUARDED_EVENT_TYPES:
        raise CoordinationSshError("event_not_allowed")
    if not key or len(key) > 512:
        raise CoordinationSshError("idempotency_key_invalid")
    try:
        # Keep the original event payload parser and its request-size bound;
        # the tighter native message payload limit applies only to sends.
        payload = parse_event_payload(request.get("payload"))
    except Exception as exc:  # noqa: BLE001 - caller detail must not cross wire
        raise CoordinationSshError("caller_fields_rejected") from exc
    return GuardedAgentEvent(
        event_type=event_type,
        idempotency_key=key,
        payload=payload,
        evidence=evidence,
        policy=policy,
    )


def _build_message_send(request: Mapping[str, Any]) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_SEND_MESSAGE or not set(
        request
    ).issubset(_SEND_MESSAGE_FIELDS):
        raise CoordinationSshError("request_fields_rejected")
    required = (
        "recipient_assignment_id",
        "recipient_generation",
        "kind",
        "idempotency_key",
    )
    if any(field not in request for field in required):
        raise CoordinationSshError("request_fields_rejected")
    return {
        "recipient_assignment_id": _request_text(
            request.get("recipient_assignment_id"), field="recipient_assignment_id"
        ),
        "recipient_generation": _request_generation(
            request.get("recipient_generation")
        ),
        "kind": _request_text(request.get("kind"), field="kind").lower(),
        "idempotency_key": _request_text(
            request.get("idempotency_key"), field="idempotency_key"
        ),
        "payload": _bounded_payload(request.get("payload")),
    }


def _build_message_ack(request: Mapping[str, Any]) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_ACK_MESSAGE or not set(
        request
    ).issubset(_ACK_MESSAGE_FIELDS):
        raise CoordinationSshError("request_fields_rejected")
    required = ("message_id", "idempotency_key")
    if any(field not in request for field in required):
        raise CoordinationSshError("request_fields_rejected")
    return {
        "message_id": _request_text(request.get("message_id"), field="message_id"),
        "idempotency_key": _request_text(
            request.get("idempotency_key"), field="idempotency_key"
        ),
    }


def _build_assignment_request(request: Mapping[str, Any]) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_REQUEST_ASSIGNMENT or not set(
        request
    ).issubset(_REQUEST_ASSIGNMENT_FIELDS):
        raise CoordinationSshError("request_fields_rejected")
    child_kind = _request_text(request.get("child_kind"), field="child_kind").lower()
    if child_kind not in _CHILD_KINDS:
        raise CoordinationSshError("child_kind_invalid")
    raw_scope = request.get("scope")
    if not isinstance(raw_scope, Mapping) or not set(raw_scope).issubset(
        _ASSIGNMENT_SCOPE_FIELDS
    ):
        raise CoordinationSshError("scope_invalid")
    scope = {
        str(field): _request_text(value, field=str(field))
        for field, value in raw_scope.items()
    }
    execution_profile = request.get("execution_profile")
    if child_kind == "sprint":
        if execution_profile is not None:
            raise CoordinationSshError("execution_profile_invalid")
    else:
        execution_profile = _request_text(execution_profile, field="execution_profile")
    return {
        "child_kind": child_kind,
        "target_route": _safe_route(request.get("target_route")),
        "scope": scope,
        "execution_profile": execution_profile,
        "idempotency_key": _request_text(
            request.get("idempotency_key"), field="idempotency_key"
        ),
    }


def _build_assignment_start_prepare(request: Mapping[str, Any]) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_PREPARE_ASSIGNMENT_START or not set(
        request
    ).issubset(_PREPARE_ASSIGNMENT_START_FIELDS):
        raise CoordinationSshError("request_fields_rejected")
    return {
        "assignment_id": _request_text(
            request.get("assignment_id"), field="assignment_id"
        ),
        "idempotency_key": _request_text(
            request.get("idempotency_key"), field="idempotency_key"
        ),
    }


def _build_pair_proof(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != PAIR_PROOF_FIELDS:
        raise CoordinationSshError("pair_proof_invalid")
    result: dict[str, Any] = {}
    for field_name, item in value.items():
        if field_name == "assignment_generation":
            result[field_name] = _request_generation(item)
        elif field_name == "git_dirty":
            if not isinstance(item, bool):
                raise CoordinationSshError("pair_proof_invalid")
            result[field_name] = item
        elif (
            field_name
            in {
                "graph_base_generation",
                "graph_base_checkpoint",
            }
            and item is None
        ):
            result[field_name] = None
        else:
            result[field_name] = _request_text(item, field=str(field_name))
    if result.get("lifecycle_status") != "active" or result.get("git_dirty"):
        raise CoordinationSshError("pair_proof_invalid")
    return result


def _build_assignment_start_finalize(request: Mapping[str, Any]) -> dict[str, Any]:
    if _request_operation(request) != OPERATION_FINALIZE_ASSIGNMENT_START or not set(
        request
    ).issubset(_FINALIZE_ASSIGNMENT_START_FIELDS):
        raise CoordinationSshError("request_fields_rejected")
    return {
        "assignment_id": _request_text(
            request.get("assignment_id"), field="assignment_id"
        ),
        "start_attempt_id": _request_text(
            request.get("start_attempt_id"), field="start_attempt_id"
        ),
        "pair_digest": _request_text(request.get("pair_digest"), field="pair_digest"),
        "pair_proof": _build_pair_proof(request.get("pair_proof")),
        "idempotency_key": _request_text(
            request.get("idempotency_key"), field="idempotency_key"
        ),
    }


def _default_writer_factory(route: CoordinationServerRoute) -> Any:
    """Instantiate the deployment's native guarded writer.

    Deployment loading is explicit and constrained to the root selected by the
    root-owned route config.  The returned writer is the existing native
    ledger writer; no second coordination store is introduced here.
    """

    from okg.substrate import fastyaml
    from okg.substrate.deployment_authority import resolved_target
    from okg.substrate.deployments import DeploymentManifest, inject_deployment_path
    from okg.substrate.library.agent_runtimes.production_authority import (
        ProductionAuthorityError,
        build_production_authority,
    )

    try:
        trusted_root = _trusted_repo_root(
            str(route.repo_root),
            expected_git_commit=route.expected_git_commit,
        )
        deployment_root = (trusted_root / "deployments").resolve(strict=True)
    except (OSError, CoordinationSshError) as exc:
        raise CoordinationSshError("route_not_configured") from exc
    deployment_name = route.deployment_name
    if (
        not deployment_name
        or deployment_name in {".", ".."}
        or any(char in deployment_name for char in "/\\")
    ):
        raise CoordinationSshError("route_not_configured")
    try:
        manifest_dir = (deployment_root / deployment_name).resolve(strict=True)
        manifest_dir.relative_to(deployment_root)
    except (OSError, ValueError) as exc:
        raise CoordinationSshError("route_not_configured") from exc
    manifest_path = manifest_dir / "deployment.yaml"
    if not manifest_path.is_file():
        raise CoordinationSshError("route_not_configured")
    try:
        # Validate the root-selected DSN against the selected deployment's
        # declared identity before importing or constructing native code.  An
        # interpolated declaration intentionally pins only its database name;
        # a literal declaration pins the complete host/port/database triple.
        raw_manifest = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        postgres = (
            raw_manifest.get("postgres") if isinstance(raw_manifest, Mapping) else None
        )
        expected_database = (
            postgres.get("database")
            if isinstance(postgres, Mapping)
            and isinstance(postgres.get("database"), str)
            else None
        )
        expected_host: str | None = None
        expected_port: int | None = None
        declared_dsn = postgres.get("dsn") if isinstance(postgres, Mapping) else None
        if (
            isinstance(declared_dsn, str)
            and declared_dsn.strip()
            and "${" not in declared_dsn
        ):
            from urllib.parse import urlsplit

            declared_parts = urlsplit(declared_dsn.strip())
            expected_host = declared_parts.hostname
            expected_port = declared_parts.port
        identity = resolved_target(
            route.dsn,
            deployment=deployment_name,
            authority_mode="forced_command",
            expected_database=expected_database,
            expected_host=expected_host,
            expected_port=expected_port,
            manifest_readable=True,
        )
        if identity.get("identity") != "ok":
            raise CoordinationSshError("writer_unavailable")
        manifest = DeploymentManifest.from_yaml(manifest_path)
        _require_manifest_identity(manifest.name, deployment_name)
        inject_deployment_path(manifest.deployment_dir)
    except CoordinationSshError:
        raise
    except Exception as exc:  # noqa: BLE001 - deployment details stay server-side
        raise CoordinationSshError("route_not_configured") from exc
    try:
        resolved_manifest_dir = manifest.deployment_dir.resolve(strict=True)
        resolved_manifest_dir.relative_to(deployment_root)
    except (AttributeError, OSError, ValueError) as exc:
        raise CoordinationSshError("route_not_configured") from exc
    native = _import_route_native(manifest_dir)
    expected_native_path = (manifest_dir / "coordination" / "native_state.py").resolve(
        strict=True
    )
    try:
        for trusted_path in (
            deployment_root,
            manifest_dir,
            manifest_path,
            expected_native_path.parent,
            expected_native_path,
        ):
            path_stat = trusted_path.lstat()
            if path_stat.st_uid != 0 or path_stat.st_mode & 0o022:
                raise ValueError("native writer checkout is not operator-owned")
    except (OSError, ValueError) as exc:
        raise CoordinationSshError("writer_unavailable") from exc
    writer_cls = getattr(native, "GuardedCoordinationWriter", None)
    if writer_cls is None:
        raise CoordinationSshError("writer_unavailable")
    try:
        return build_production_authority(route, writer_cls)
    except ProductionAuthorityError as exc:
        raise CoordinationSshError("writer_unavailable") from exc
    except Exception as exc:  # noqa: BLE001 - no DB/config details on wire
        raise CoordinationSshError("writer_unavailable") from exc


def _require_manifest_identity(actual: Any, expected: str) -> None:
    if not isinstance(actual, str) or actual != expected:
        raise CoordinationSshError("route_not_configured")


def _import_route_native(manifest_dir: Path) -> Any:
    """Import the exact deployment module without process-global route reuse."""

    # This command is one-shot under sshd, but embedding tests and accidental
    # long-lived wrappers must not reuse another route's process-global
    # deployment package. Load the exact selected deployment every time.
    for module_name in tuple(sys.modules):
        if module_name == "coordination" or module_name.startswith("coordination."):
            sys.modules.pop(module_name, None)
    importlib.invalidate_caches()
    native = importlib.import_module("coordination.native_state")
    try:
        native_path = Path(native.__file__).resolve(strict=True)
        expected_native_path = (
            manifest_dir / "coordination" / "native_state.py"
        ).resolve(strict=True)
        if native_path != expected_native_path:
            raise ValueError("native writer path does not match route deployment")
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        # Policy and writer code must come from the same server-selected
        # checkout.  This also refuses a stale import from another configured
        # route in a long-lived embedding process.
        raise CoordinationSshError("writer_unavailable") from exc
    return native


_MAX_READ_ITEMS = 128
_MAX_READ_KEYS = 32
_READ_FORBIDDEN_NAMES = frozenset(
    {
        "credential",
        "credential_id",
        "database",
        "database_password",
        "dsn",
        "event_payload",
        "owner",
        "owner_principal",
        "owner_session",
        "owner_session_id",
        "password",
        "payload",
        "peer",
        "private_key",
        "principal",
        "secret",
        "session",
        "session_id",
        "token",
        "workspace_path",
        "worktree_path",
        "repo_path",
        "graph_path",
    }
)
_READ_FORBIDDEN_MARKERS = (
    "credential",
    "database",
    "dsn",
    "password",
    "private_key",
    "principal",
    "session",
    "secret",
    "payload",
    "owner",
    "path",
)


_READ_SCALAR = None


def _read_fields(*names: str) -> dict[str, Any]:
    return {name: _READ_SCALAR for name in names}


def _read_list(schema: Any) -> tuple[str, Any]:
    return ("list", schema)


def _read_map(schema: Mapping[str, Any]) -> tuple[str, Any]:
    return ("mapping", schema)


# Native read-model fields are explicitly allow-listed. A schema value of
# None accepts one bounded scalar; nested maps/lists name their own schema.
_READ_WORKSPACE_SCHEMA = _read_fields(
    "source",
    "status",
    "dirty",
    "dirty_tracked_count",
    "dirty_untracked_count",
    "dirty_ignored_count",
    "branch_ref",
    "head_commit",
    "fresh",
)
_READ_LEASE_SCHEMA = _read_fields(
    "state",
    "fresh",
    "expired",
    "status",
    "expires_at",
    "last_heartbeat_at",
)
_READ_DELTA_SCHEMA = {
    **_read_fields(
        "assignment_ref",
        "kind",
        "generation",
        "status",
        "revision",
        "project_ref",
        "sprint_ref",
        "change_ref",
        "task_ref",
        "parent_ref",
        "owner_ref",
        "session_ref",
        "blocked",
        "blocker_ref",
        "published_revision",
        "claim_state",
        "execution_lane",
        "execution_profile",
        "dimension",
        "claim_state",
    ),
    "count": _READ_SCALAR,
    "blocked_count": _READ_SCALAR,
    "statuses": _read_list(_READ_SCALAR),
    "lease": _read_map(_READ_LEASE_SCHEMA),
    "resource_demand": ("dynamic",),
    "workspace": _read_map(_READ_WORKSPACE_SCHEMA),
}
_READ_EXCEPTION_SCHEMA = _read_fields(
    "code",
    "dimension",
    "reason",
    "summary",
    "message",
    "severity",
    "status",
    "retryable",
)
_READ_FRESHNESS_COMPARISON_SCHEMA = _read_fields(
    "assignment_ref",
    "live_revision",
    "published_revision",
)
_READ_FRESHNESS_SCHEMA = {
    **_read_fields("status", "live_revision", "latest_published_revision"),
    "live": _read_map(_read_fields("status", "revision")),
    "latest_published": _read_map(_read_fields("status", "revision", "generation")),
    "assignments": _read_list(_read_map(_READ_FRESHNESS_COMPARISON_SCHEMA)),
}
_READ_NEXT_ACTION_SCHEMA = _read_fields(
    "action",
    "assignment_ref",
    "assignment_id",
    "message_ref",
    "message_id",
    "kind",
    "status",
    "reason",
    "summary",
    "required",
    "retryable",
)
_READ_HIERARCHY_SCHEMA = {
    **_read_fields(
        "role",
        "self_ref",
        "self_id",
        "descendant_count",
        "descendants",
        "active_children",
    ),
    "scope": _read_map(
        _read_fields(
            "project_ref",
            "sprint_ref",
            "change_ref",
            "project_id",
            "sprint_id",
            "change_id",
        )
    ),
    "descendants": _read_list(_READ_SCALAR),
    "active_children": _read_list(_READ_SCALAR),
}
_READ_ASSIGNMENT_SCHEMA = {
    **_READ_DELTA_SCHEMA,
    "assignment_id": _READ_SCALAR,
    "parent_assignment_id": _READ_SCALAR,
    "project_id": _READ_SCALAR,
    "sprint_id": _READ_SCALAR,
    "change_id": _READ_SCALAR,
    "task_id": _READ_SCALAR,
    "last_heartbeat_at": _READ_SCALAR,
    "parent_assignment_generation": _READ_SCALAR,
    "pact": _read_map(
        _read_fields(
            "change_id",
            "task_id",
            "status",
            "state",
            "done",
            "authority",
            "task_done",
            "proof_attached",
            "review_state",
            "merge_ready",
            "finalized_state",
        )
    ),
    "git": _read_map(
        _read_fields(
            "worktree_ref",
            "branch_ref",
            "head_commit",
            "base_commit",
            "branch",
        )
    ),
    "graph": _read_map(
        _read_fields(
            "branch_ref",
            "checkpoint_ref",
            "generation",
            "base_checkpoint",
            "current_ref",
            "branch",
        )
    ),
    "close": _read_map(
        _read_fields(
            "phase",
            "next_action",
            "close_attempt_id",
            "close_readiness_digest",
            "close_cleanup_digest",
        )
    ),
}
_READ_ROUTE_SCHEMA = _read_fields(
    "status",
    "assignment_ref",
    "generation",
    "parent_ref",
    "host_ref",
    "credential_ref",
    "worktree_ref",
    "branch_ref",
    "target_route_ref",
    "route_generation",
    "route_digest",
    "assignment_id",
    "parent_assignment_id",
    "parent_generation",
)
_READ_RESERVATION_SCHEMA = {
    **_read_fields(
        "assignment_ref",
        "generation",
        "kind",
        "status",
        "lane",
        "assignment_id",
        "parent_assignment_id",
        "lease_expires_at",
    ),
    "resource_demand": ("dynamic",),
}
_READ_APPROVAL_SCHEMA = _read_fields(
    "approval_id",
    "request_digest",
    "requester_assignment_id",
    "requester_generation",
    "action",
    "status",
    "route_state",
    "blocker_code",
)
_READ_MESSAGE_SCHEMA = _read_fields(
    "message_id",
    "message_ref",
    "sender_assignment_id",
    "sender_generation",
    "recipient_assignment_id",
    "recipient_generation",
    "kind",
    "status",
)
_READ_BLOCKER_SCHEMA = _read_fields(
    "assignment_ref",
    "state",
    "code",
    "reason_ref",
    "assignment_id",
    "reason_code",
)
_READ_CAPACITY_SCHEMA = {
    **_read_fields("status", "reservation_count", "capacity_ref", "overage"),
    "lane_usage": ("dynamic",),
    "named_resource_usage": ("dynamic",),
    "lane_capacity": ("dynamic",),
    "named_resource_capacity": ("dynamic",),
}
_READ_PAIR_SCHEMA = {
    **_read_fields("status", "proof_ref", "pair_digest", "workspace_observation"),
    "git": _read_map(
        _read_fields(
            "worktree_ref",
            "branch_ref",
            "head_commit",
            "base_commit",
            "branch",
        )
    ),
    "graph": _read_map(
        _read_fields(
            "branch_ref",
            "checkpoint_ref",
            "generation",
            "base_checkpoint",
            "current_ref",
            "branch",
        )
    ),
    "live_workspace": _read_map(
        _read_fields(
            "status",
            "dirty",
            "branch_ref",
            "head_commit",
            "fresh",
        )
    ),
}
_READ_JOB_SCHEMA = _read_fields("status")


def _read_wire_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise CoordinationSshError("writer_refused")
    result = value.strip()
    lowered = result.lower()
    if (
        not result
        or len(result) > 512
        or "\x00" in result
        or "\n" in result
        or "\r" in result
        or result.startswith(("/", "~"))
        or "://" in lowered
        or "postgres" in lowered
        or "password=" in lowered
        or "secret=" in lowered
    ):
        raise CoordinationSshError("writer_refused")
    return result


def _read_wire_reason(value: Any, *, field: str) -> str:
    result = _read_wire_text(value, field=field)
    if _safe_reason_code(result) != result:
        raise CoordinationSshError("writer_refused")
    return result


def _read_wire_cursor(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise CoordinationSshError("writer_refused")
    return value


def _read_wire_int(value: Any, *, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**63 - 1
    ):
        raise CoordinationSshError("writer_refused")
    return value


def _read_wire_number(value: Any, *, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CoordinationSshError("writer_refused")
    if value < 0 or value != value or value in {float("inf"), float("-inf")}:
        raise CoordinationSshError("writer_refused")
    return value


def _read_wire_time(value: Any, *, field: str) -> str | int | float:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _read_wire_number(value, field=field)
    return _read_wire_text(value, field=field)


def _read_wire_value(value: Any, spec: Any, *, field: str) -> Any:
    if value is None:
        return None
    if spec is _READ_SCALAR:
        if isinstance(value, str):
            return _read_wire_text(value, field=field)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return _read_wire_number(value, field=field)
        raise CoordinationSshError("writer_refused")
    if spec == "text":
        return _read_wire_text(value, field=field)
    if spec == "reason":
        return _read_wire_reason(value, field=field)
    if spec == "cursor":
        return _read_wire_cursor(value, field=field)
    if spec == "bool":
        if not isinstance(value, bool):
            raise CoordinationSshError("writer_refused")
        return value
    if spec == "int":
        return _read_wire_int(value, field=field)
    if spec == "number":
        return _read_wire_number(value, field=field)
    if spec == "time":
        return _read_wire_time(value, field=field)
    if isinstance(spec, tuple) and len(spec) == 2 and spec[0] == "list":
        if not isinstance(value, (list, tuple)) or len(value) > _MAX_READ_ITEMS:
            raise CoordinationSshError("writer_refused")
        return [
            _read_wire_value(item, spec[1], field=f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(spec, tuple) and len(spec) == 2 and spec[0] == "mapping":
        return _project_read_mapping(value, spec[1], field=field)
    if isinstance(spec, tuple) and len(spec) == 1 and spec[0] == "dynamic":
        if not isinstance(value, Mapping) or len(value) > _MAX_READ_KEYS:
            raise CoordinationSshError("writer_refused")
        result: dict[str, int] = {}
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or not key
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", key)
            ):
                raise CoordinationSshError("writer_refused")
            result[key] = _read_wire_int(item, field=f"{field}.{key}")
        return result
    raise CoordinationSshError("writer_refused")


def _project_read_mapping(
    value: Any,
    schema: Mapping[str, Any],
    *,
    field: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or len(value) > _MAX_READ_KEYS:
        raise CoordinationSshError("writer_refused")
    result: dict[str, Any] = {}
    for raw_key, item in value.items():
        if not isinstance(raw_key, str):
            raise CoordinationSshError("writer_refused")
        key = raw_key.strip()
        normalized = key.lower().replace("-", "_")
        if (
            not key
            or normalized in _READ_FORBIDDEN_NAMES
            or (
                not normalized.endswith("_ref")
                and any(marker in normalized for marker in _READ_FORBIDDEN_MARKERS)
            )
            or key not in schema
        ):
            raise CoordinationSshError("writer_refused")
        result[key] = _read_wire_value(item, schema[key], field=f"{field}.{key}")
    return result


_CLOSE_RESULT_FIELDS = frozenset(
    {
        "ok",
        "status",
        "assignment_id",
        "assignment_generation",
        "close_attempt_id",
        "readiness_digest",
        "pair_digest",
        "cleanup_digest",
        "close_plan",
        "event_id",
        "revision",
        "inserted",
        "duplicate",
        "rejection_reason",
        "request_digest",
        "rejection_code",
        "reason_code",
    }
)


def _safe_close_result(value: Any, *, operation: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not set(value).issubset(_CLOSE_RESULT_FIELDS):
        raise CoordinationSshError("writer_refused")
    ok = value.get("ok")
    status = value.get("status")
    if not isinstance(ok, bool) or not isinstance(status, str):
        raise CoordinationSshError("writer_refused")
    status = status.strip().lower()
    if status not in {"closing", "retired", "rejected"}:
        raise CoordinationSshError("writer_refused")
    if not ok or status == "rejected":
        if ok or status != "rejected":
            raise CoordinationSshError("writer_refused")
        return {
            "ok": False,
            "status": "rejected",
            "reason_code": _safe_reason_code(
                value.get("rejection_code") or value.get("reason_code")
            ),
        }
    if operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE and status not in {
        "closing",
        "retired",
    }:
        raise CoordinationSshError("writer_refused")
    if operation == OPERATION_FINALIZE_ASSIGNMENT_CLOSE and status != "retired":
        raise CoordinationSshError("writer_refused")
    required = {
        "ok",
        "status",
        "assignment_id",
        "assignment_generation",
        "close_attempt_id",
        "readiness_digest",
    }
    if operation == OPERATION_FINALIZE_ASSIGNMENT_CLOSE:
        required.add("cleanup_digest")
    if operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE and status == "closing":
        required.add("close_plan")
    if not required.issubset(value):
        raise CoordinationSshError("writer_refused")
    if status == "retired" and operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE:
        # An idempotent prepare may observe an already-retired assignment,
        # but it must never smuggle a stale plan to the local lifecycle.
        if "close_plan" in value:
            raise CoordinationSshError("writer_refused")
    if operation == OPERATION_FINALIZE_ASSIGNMENT_CLOSE and "close_plan" in value:
        raise CoordinationSshError("writer_refused")
    if operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE and status == "closing":
        raw_plan = value.get("close_plan")
        if raw_plan is None:
            raise CoordinationSshError("writer_refused")
        try:
            from okg.substrate.library.agent_runtimes.assignment_close import (
                AssignmentClosePlan,
            )

            close_plan = AssignmentClosePlan.from_mapping(raw_plan).as_dict()
        except Exception as exc:  # noqa: BLE001 - plan is untrusted writer data
            raise CoordinationSshError("writer_refused") from exc
    result: dict[str, Any] = {"ok": True, "status": status}
    for key in (
        "assignment_id",
        "close_attempt_id",
    ):
        if key in value:
            result[key] = _close_identifier(value[key], field=key)
    if "assignment_generation" in value:
        generation = value["assignment_generation"]
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
        ):
            raise CoordinationSshError("writer_refused")
        result["assignment_generation"] = generation
    for key in ("readiness_digest", "pair_digest", "cleanup_digest"):
        if key in value:
            result[key] = _close_digest(value[key], field=key)
    if operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE and status == "closing":
        result["close_plan"] = close_plan
    return result


def _safe_read_result(value: Any, *, operation: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CoordinationSshError("writer_refused")
    if operation == OPERATION_STATUS:
        schema: dict[str, Any] = {
            "ok": "bool",
            "operation": "text",
            "cursor": "cursor",
            "changed": "bool",
            "deltas": ("list", ("mapping", _READ_DELTA_SCHEMA)),
            "exceptions": ("list", ("mapping", _READ_EXCEPTION_SCHEMA)),
            "blockers": ("list", ("mapping", _READ_BLOCKER_SCHEMA)),
            "freshness": ("mapping", _READ_FRESHNESS_SCHEMA),
            "next_action": ("mapping", _READ_NEXT_ACTION_SCHEMA),
        }
        required = {
            "ok",
            "operation",
            "cursor",
            "changed",
            "deltas",
            "exceptions",
            "freshness",
            "next_action",
        }
    elif operation == OPERATION_RESUME:
        schema = {
            "ok": "bool",
            "operation": "text",
            "assignment": ("mapping", _READ_ASSIGNMENT_SCHEMA),
            "assignments": ("list", ("mapping", _READ_ASSIGNMENT_SCHEMA)),
            "hierarchy": ("mapping", _READ_HIERARCHY_SCHEMA),
            "route": ("mapping", _READ_ROUTE_SCHEMA),
            "lease": ("mapping", _READ_LEASE_SCHEMA),
            "reservations": ("list", ("mapping", _READ_RESERVATION_SCHEMA)),
            "pending_approvals": ("list", ("mapping", _READ_APPROVAL_SCHEMA)),
            "messages": ("list", ("mapping", _READ_MESSAGE_SCHEMA)),
            "blockers": ("list", ("mapping", _READ_BLOCKER_SCHEMA)),
            "capacity": ("mapping", _READ_CAPACITY_SCHEMA),
            "jobs": ("mapping", _READ_JOB_SCHEMA),
            "pair": ("mapping", _READ_PAIR_SCHEMA),
            "exceptions": ("list", ("mapping", _READ_EXCEPTION_SCHEMA)),
            "freshness": ("mapping", _READ_FRESHNESS_SCHEMA),
            "next_action": ("mapping", _READ_NEXT_ACTION_SCHEMA),
        }
        required = {
            "ok",
            "operation",
            "assignment",
            "hierarchy",
            "route",
            "lease",
            "reservations",
            "pending_approvals",
            "messages",
            "blockers",
            "capacity",
            "jobs",
            "pair",
            "exceptions",
            "freshness",
            "next_action",
        }
    else:
        raise CoordinationSshError("writer_refused")
    if not set(value).issubset(schema) or not required.issubset(value):
        raise CoordinationSshError("writer_refused")
    # Native status refusals are a deliberately smaller read result and do
    # not compute blockers. Successful status views always carry the bounded
    # blocker projection, so absence on an ``ok`` result is still a schema
    # violation rather than an implicit empty fallback.
    if (
        operation == OPERATION_STATUS
        and value.get("ok") is True
        and "blockers" not in value
    ):
        raise CoordinationSshError("writer_refused")
    raw_operation = value.get("operation")
    if raw_operation is not None and raw_operation != operation:
        raise CoordinationSshError("writer_refused")
    result: dict[str, Any] = {"ok": _read_wire_value(value["ok"], "bool", field="ok")}
    result["operation"] = operation
    for key, spec in schema.items():
        if key in {"ok", "operation"} or key not in value:
            continue
        result[key] = _read_wire_value(value[key], spec, field=key)
    return result


def _safe_result(value: Any, *, operation: str = OPERATION_EVENT) -> dict[str, Any]:
    """Project a native receipt onto the agent-safe wire schema."""

    if hasattr(value, "as_dict"):
        try:
            raw = value.as_dict()
        except Exception as exc:  # noqa: BLE001 - receipt is not trusted input
            raise CoordinationSshError("writer_refused") from exc
    elif isinstance(value, Mapping):
        raw = value
    else:
        result_fields = (
            (
                "ok",
                "status",
                "message_id",
                "duplicate",
                "rejection_code",
                "reason_code",
                "current_assignment_id",
                "current_generation",
            )
            if operation in {OPERATION_SEND_MESSAGE, OPERATION_ACK_MESSAGE}
            else (
                "ok",
                "status",
                "event_id",
                "event_type",
                "assignment_id",
                "assignment_generation",
                "revision",
                "inserted",
                "duplicate",
                "rejection_code",
                "lease_expires_at",
            )
        )
        raw = {key: getattr(value, key, None) for key in result_fields}
    if not isinstance(raw, Mapping):
        raise CoordinationSshError("writer_refused")
    if operation in {
        OPERATION_PREPARE_ASSIGNMENT_CLOSE,
        OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
    }:
        return _safe_close_result(raw, operation=operation)
    if operation in {OPERATION_STATUS, OPERATION_RESUME}:
        return _safe_read_result(raw, operation=operation)
    ok = bool(raw.get("ok"))
    is_handshake = operation in {
        OPERATION_REQUEST_ASSIGNMENT,
        OPERATION_PREPARE_ASSIGNMENT_START,
        OPERATION_FINALIZE_ASSIGNMENT_START,
    }
    is_message = operation in {OPERATION_SEND_MESSAGE, OPERATION_ACK_MESSAGE}
    if is_handshake:
        status = str(raw.get("status") or "").strip().lower()
        if status not in {"pending", "starting", "active", "rejected"}:
            raise CoordinationSshError("writer_refused")
        result = {"ok": ok, "status": status}
        for key in (
            "event_id",
            "assignment_id",
            "start_attempt_id",
            "pair_digest",
        ):
            item = raw.get(key)
            if item is not None:
                result[key] = _request_text(item, field=key)
        for key in ("assignment_generation", "revision"):
            item = raw.get(key)
            if isinstance(item, int) and not isinstance(item, bool) and item >= 0:
                result[key] = item
        for key in ("inserted", "duplicate"):
            if raw.get(key) is not None:
                result[key] = bool(raw.get(key))
        start_plan = raw.get("start_plan")
        if start_plan is not None:
            if operation != OPERATION_PREPARE_ASSIGNMENT_START:
                raise CoordinationSshError("writer_refused")
            try:
                result["start_plan"] = AssignmentStartPlan.from_mapping(
                    start_plan
                ).as_dict()
            except AssignmentHandshakeError as exc:
                raise CoordinationSshError("writer_refused") from exc
        if ok:
            if (
                not isinstance(result.get("assignment_id"), str)
                or not isinstance(result.get("assignment_generation"), int)
                or result["assignment_generation"] < 1
            ):
                raise CoordinationSshError("writer_refused")
            expected_status = {
                OPERATION_REQUEST_ASSIGNMENT: "pending",
                OPERATION_FINALIZE_ASSIGNMENT_START: "active",
            }.get(operation)
            if expected_status is not None and status != expected_status:
                raise CoordinationSshError("writer_refused")
            if operation == OPERATION_PREPARE_ASSIGNMENT_START:
                if status not in {"starting", "active"}:
                    raise CoordinationSshError("writer_refused")
                if status == "starting" and (
                    "start_plan" not in result
                    or not isinstance(result.get("start_attempt_id"), str)
                ):
                    raise CoordinationSshError("writer_refused")
            if operation == OPERATION_FINALIZE_ASSIGNMENT_START and (
                not isinstance(result.get("start_attempt_id"), str)
                or not isinstance(result.get("pair_digest"), str)
            ):
                raise CoordinationSshError("writer_refused")
        elif status != "rejected":
            raise CoordinationSshError("writer_refused")
        if not ok:
            result["reason_code"] = _safe_reason_code(
                raw.get("rejection_code") or raw.get("reason_code")
            )
        return result
    if is_message:
        raw_status = raw.get("status")
        status = str(raw_status).strip().lower()
        if status not in {"pending", "acknowledged", "accepted", "rejected"}:
            status = "accepted" if ok else "rejected"
        result: dict[str, Any] = {
            "ok": ok,
            "status": status,
            "duplicate": bool(raw.get("duplicate")),
        }
    else:
        result = {
            "ok": ok,
            "status": "accepted" if ok else "rejected",
        }
    if is_message:
        message_id = raw.get("message_id")
        if message_id is None:
            raise CoordinationSshError("writer_refused")
        message_id = str(message_id)
        if (
            not message_id
            or len(message_id) > 512
            or "\n" in message_id
            or "\r" in message_id
        ):
            raise CoordinationSshError("writer_refused")
        result["message_id"] = message_id
        current_assignment_id = raw.get("current_assignment_id")
        if current_assignment_id is not None:
            current_assignment_id = str(current_assignment_id)
            if (
                len(current_assignment_id) <= 512
                and "\n" not in current_assignment_id
                and "\r" not in current_assignment_id
            ):
                result["current_assignment_id"] = current_assignment_id
        current_generation = raw.get("current_generation")
        if isinstance(current_generation, int) and not isinstance(
            current_generation, bool
        ):
            result["current_generation"] = current_generation
        if not ok:
            result["reason_code"] = _safe_reason_code(
                raw.get("rejection_code") or raw.get("reason_code")
            )
        return result
    for key in (
        "event_id",
        "event_type",
        "assignment_id",
        "assignment_generation",
        "revision",
        "inserted",
        "duplicate",
        "lease_expires_at",
    ):
        item = raw.get(key)
        if item is None:
            continue
        if key in {"assignment_generation", "revision"}:
            if isinstance(item, bool) or not isinstance(item, int):
                continue
            result[key] = item
        elif key in {"inserted", "duplicate"}:
            result[key] = bool(item)
        elif key == "lease_expires_at":
            if isinstance(item, (datetime, date)):
                result[key] = item.isoformat()
            elif isinstance(item, (str, int, float)):
                result[key] = item
        else:
            text = str(item)
            if len(text) <= 512 and "\n" not in text and "\r" not in text:
                result[key] = text
    if not result["ok"]:
        result["reason_code"] = _safe_reason_code(raw.get("rejection_code"))
    return result


def _refusal(reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "rejected",
        "reason_code": _safe_reason_code(reason_code),
    }


def _write_wire(stdout: Any, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n"
    if len(encoded.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise CoordinationSshError("response_too_large")
    try:
        stdout.write(encoded)
    except TypeError:
        stdout.write(encoded.encode("utf-8"))
    try:
        stdout.flush()
    except (AttributeError, OSError):
        pass


def run_forced_command(
    *,
    route: str,
    stdin: io.BufferedIOBase | io.BytesIO | TextIO,
    stdout: Any,
    environ: Mapping[str, str] | None = None,
    config_loader: Callable[[], CoordinationServerConfig | Mapping[str, Any]]
    | None = None,
    writer_factory: Callable[[CoordinationServerRoute], Any] | None = None,
    evidence_reader: Callable[[Mapping[str, str]], SshdPeerEvidence] | None = None,
    policy_loader: Callable[[Path], CoordinationPolicyBinding] | None = None,
    max_request_bytes: int = MAX_REQUEST_BYTES,
) -> int:
    """Run one forced-command request and emit only a sanitized JSON result.

    The route configuration, sshd evidence, tracked policy, and native writer
    all belong to the server process.  Legacy event requests carry only
    ``event_type``, ``idempotency_key``, and a bounded payload.  Message send
    requests additionally carry only the recipient assignment/generation and
    message kind; acknowledgements carry only a message id and key.
    """

    exit_code: int | None = None
    try:
        route = _safe_route(route)
        request = read_bounded_request(stdin, max_bytes=max_request_bytes)
        loader = config_loader or (lambda: load_server_config())
        config = loader()
        resolved = resolve_server_route(config, route)
        env = environ if environ is not None else os.environ
        try:
            evidence = (evidence_reader or read_sshd_peer_evidence)(env)
        except (SshTransportAuthenticationError, OSError, ValueError) as exc:
            raise CoordinationSshError("transport_authentication_failed") from exc
        operation = _request_operation(request)
        if operation == OPERATION_EVENT:
            try:
                policy = (policy_loader or load_coordination_binding)(
                    resolved.repo_root
                )
            except Exception as exc:  # noqa: BLE001 - tracked policy details stay server-side
                raise CoordinationSshError("policy_unavailable") from exc
            event = _build_event(request, evidence=evidence, policy=policy)
        elif operation == OPERATION_SEND_MESSAGE:
            message = _build_message_send(request)
        elif operation == OPERATION_ACK_MESSAGE:
            acknowledgement = _build_message_ack(request)
        elif operation == OPERATION_REQUEST_ASSIGNMENT:
            assignment_request = _build_assignment_request(request)
        elif operation == OPERATION_PREPARE_ASSIGNMENT_START:
            assignment_start = _build_assignment_start_prepare(request)
        elif operation == OPERATION_FINALIZE_ASSIGNMENT_START:
            assignment_finalize = _build_assignment_start_finalize(request)
        elif operation in {OPERATION_STATUS, OPERATION_RESUME}:
            # The read operations have no caller fields beyond the request
            # shape checked by ``read_bounded_request``.
            pass
        elif operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE:
            assignment_close_prepare = _build_assignment_close_prepare(request)
        elif operation == OPERATION_FINALIZE_ASSIGNMENT_CLOSE:
            assignment_close_finalize = _build_assignment_close_finalize(request)
        else:  # pragma: no cover - _request_operation is closed above
            raise CoordinationSshError("operation_not_allowed")
        factory = writer_factory or _default_writer_factory
        writer = factory(resolved)
        if operation == OPERATION_EVENT:
            receipt = submit_guarded_event(event, writer=writer.write)
        elif operation == OPERATION_SEND_MESSAGE:
            method = getattr(writer, "send_message_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **message)
        elif operation == OPERATION_ACK_MESSAGE:
            method = getattr(writer, "acknowledge_message_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **acknowledgement)
        elif operation == OPERATION_REQUEST_ASSIGNMENT:
            method = getattr(writer, "request_assignment_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **assignment_request)
        elif operation == OPERATION_PREPARE_ASSIGNMENT_START:
            method = getattr(writer, "prepare_assignment_start_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **assignment_start)
        elif operation == OPERATION_FINALIZE_ASSIGNMENT_START:
            method = getattr(writer, "finalize_assignment_start_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **assignment_finalize)
        elif operation == OPERATION_STATUS:
            method = getattr(writer, "status_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(
                evidence,
                cursor=_request_cursor(request["cursor"])
                if "cursor" in request
                else None,
            )
        elif operation == OPERATION_PREPARE_ASSIGNMENT_CLOSE:
            method = getattr(writer, "prepare_assignment_close_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **assignment_close_prepare)
        elif operation == OPERATION_FINALIZE_ASSIGNMENT_CLOSE:
            method = getattr(writer, "finalize_assignment_close_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence, **assignment_close_finalize)
        else:
            method = getattr(writer, "resume_from_peer", None)
            if not callable(method):
                raise CoordinationSshError("writer_unavailable")
            receipt = method(evidence)
        safe_receipt = _safe_result(receipt, operation=operation)
        _write_wire(stdout, safe_receipt)
        return 0 if safe_receipt["ok"] else 1
    except CoordinationSshError as exc:
        _write_wire(stdout, _refusal(exc.reason_code))
        return 1
    except Exception:
        # The final boundary must not serialize arbitrary exception text: DB
        # drivers and path errors commonly include credentials or peer data.
        _write_wire(stdout, _refusal("internal_error"))
        exit_code = 1
    if exit_code is None:
        raise AssertionError("forced command exited without a result")
    return exit_code


# Descriptive alias for embedding callers that do not use CLI terminology.
handle_forced_command = run_forced_command


__all__ = [
    "CONFIG_FIELDS",
    "DEFAULT_SERVER_CONFIG_PATH",
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "MAX_REQUEST_WAIT_SECONDS",
    "MAX_STDIN_BYTES",
    "CoordinationServerConfig",
    "CoordinationServerRoute",
    "CoordinationSshError",
    "MAX_PAYLOAD_BYTES",
    "OPERATION_ACK_MESSAGE",
    "OPERATION_EVENT",
    "OPERATION_FINALIZE_ASSIGNMENT_START",
    "OPERATION_FINALIZE_ASSIGNMENT_CLOSE",
    "OPERATION_PREPARE_ASSIGNMENT_START",
    "OPERATION_PREPARE_ASSIGNMENT_CLOSE",
    "OPERATION_REQUEST_ASSIGNMENT",
    "OPERATION_RESUME",
    "OPERATION_SEND_MESSAGE",
    "OPERATION_STATUS",
    "REQUEST_FIELDS",
    "handle_forced_command",
    "load_server_config",
    "read_bounded_request",
    "resolve_server_route",
    "run_forced_command",
]
