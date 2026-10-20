"""Agent-side MCP bridge for the restricted coordination SSH endpoint.

The server-side ``agent-ssh-run`` command is an sshd ``ForceCommand``.  It is
not a useful MCP command on an agent workstation: invoking it locally would
skip SSH authentication entirely.  This module supplies the small local
bridge that MCP runtimes start instead.  The bridge speaks MCP over stdio and
uses one fixed, bounded ``/usr/bin/ssh`` invocation for each coordination tool
call.

Only the opaque route and workspace grant are read from the assignment-local
MCP environment.  They are routing metadata, not authority.  Host, user,
identity, known-hosts file, port, and SSH options come from the operator-owned
client registry at :data:`DEFAULT_CLIENT_REGISTRY_PATH`; no MCP tool argument,
MCP config field, or command-line option can supply them.
"""

from __future__ import annotations

import json
import os
import re
import selectors
import signal
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from okg.substrate.library.agent_runtimes.assignment_handshake import (
    AssignmentHandshakeError,
    AssignmentStartOrchestrator,
    AssignmentStartPlan,
    PAIR_PROOF_FIELDS,
)
from okg.substrate.library.agent_runtimes.paired_workspace import (
    PairedWorkspaceLifecycle,
)
from okg.substrate.library.agent_runtimes.ssh_stdio import (
    validate_coordination_route,
)
from okg.substrate.library.agent_runtimes.coordination_ssh import (
    CoordinationSshError,
    OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
    OPERATION_PREPARE_ASSIGNMENT_CLOSE,
    _safe_close_result,
    _safe_read_result,
)


MAX_REGISTRY_BYTES = 64 * 1024
MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
DEFAULT_SSH_TIMEOUT_SECONDS = 15
DEFAULT_CLIENT_REGISTRY_PATH = Path("/etc/okg/coordination/client-registry.json")

# No caller-controlled SSH options are accepted.  Keep this list explicit so
# a future option cannot accidentally become an ambient escape hatch.
_REGISTRY_ROUTE_FIELDS = frozenset(
    {"host", "user", "identity_file", "known_hosts_file", "port"}
)
_REQUIRED_ROUTE_FIELDS = _REGISTRY_ROUTE_FIELDS - {"port"}
_REGISTRY_WORKSPACE_FIELDS = frozenset({"route"})
_FORBIDDEN_BODY_NAMES = frozenset(
    {
        "actor_id",
        "actor_session_id",
        "assignment",
        "assignment_epoch",
        "assignment_generation",
        "assignment_id",
        "assignment_kind",
        "authenticated_host",
        "credential",
        "credential_id",
        "database",
        "database_credential",
        "database_password",
        "dsn",
        "generation",
        "grant",
        "host",
        "identity",
        "identity_file",
        "key",
        "lease",
        "owner",
        "owner_principal",
        "owner_session",
        "password",
        "principal",
        "private_key",
        "route",
        "scope",
        "session",
        "session_id",
        "ssh_options",
        "timestamp",
        "user",
        "workspace",
        "workspace_id",
        "workspace_path",
    }
)
_FORBIDDEN_BODY_MARKERS = (
    "credential",
    "database",
    "dsn",
    "identity",
    "private_key",
    "principal",
    "session",
    "workspace",
)
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$")
_USER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_HOST_RE = re.compile(r"^[A-Za-z0-9_.:[\]-]{1,255}$")
_RESPONSE_FIELDS = frozenset(
    {
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
        "rejection_reason",
        "reason_code",
        "request_digest",
        "lease_expires_at",
        "message_id",
        "current_assignment_id",
        "current_generation",
        "start_attempt_id",
        "pair_digest",
        "start_plan",
        "operation",
        "cursor",
        "changed",
        "deltas",
        "exceptions",
        "freshness",
        "next_action",
        "assignment",
        "assignments",
        "hierarchy",
        "route",
        "lease",
        "reservations",
        "pending_approvals",
        "decisions",
        "messages",
        "blockers",
        "capacity",
        "pair",
        "workspace",
        "jobs",
        "close_attempt_id",
        "readiness_digest",
        "cleanup_digest",
        "cleanup_proof",
        "close_plan",
    }
)
_SCOPE_FIELDS = frozenset({"project_id", "sprint_id", "change_id", "task_id"})
_CHILD_KINDS = frozenset({"sprint", "pact"})
_OPERATION_STATUS = "status"
_OPERATION_RESUME = "resume"


class CoordinationClientError(RuntimeError):
    """A sanitized local bridge refusal."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = _safe_reason_code(reason_code)
        super().__init__(self.reason_code)


@dataclass(frozen=True, slots=True)
class ClientRoute:
    """One operator-selected SSH destination."""

    route: str
    host: str
    user: str
    identity_file: str
    known_hosts_file: str
    port: int = 22


@dataclass(frozen=True, slots=True)
class ClientWorkspace:
    """Opaque assignment-local reference bound to one configured route."""

    grant_id: str
    route: str


@dataclass(frozen=True, slots=True)
class ClientRegistry:
    """Validated operator-owned route and workspace metadata."""

    routes: Mapping[str, ClientRoute]
    workspaces: Mapping[str, ClientWorkspace]
    # ``None`` marks the in-memory test seam.  Registries loaded from disk
    # retain the uid against which their referenced files must be rechecked.
    operator_uid: int | None = None


def _safe_reason_code(value: Any) -> str:
    value = str(value or "").strip().lower().replace("-", "_")
    if not value or len(value) > 96 or not value.replace("_", "").isalnum():
        return "coordination_client_refused"
    return value


def _safe_token(value: Any, *, reason: str = "invalid_route") -> str:
    if not isinstance(value, str):
        raise CoordinationClientError(reason)
    value = value.strip()
    if not _TOKEN_RE.fullmatch(value):
        raise CoordinationClientError(reason)
    return value


def _safe_cursor(value: Any) -> str:
    if not isinstance(value, str):
        raise CoordinationClientError("invalid_cursor")
    value = value.strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise CoordinationClientError("invalid_cursor")
    return value


_CLOSE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def _safe_close_identifier(value: Any, *, reason: str) -> str:
    if not isinstance(value, str):
        raise CoordinationClientError(reason)
    value = value.strip()
    if (
        not _TOKEN_RE.fullmatch(value)
        or value.startswith(("/", "~"))
        or any(
            marker in value.lower()
            for marker in ("secret", "password", "credential", "dsn")
        )
    ):
        raise CoordinationClientError(reason)
    return value


def _safe_close_digest(value: Any, *, reason: str) -> str:
    value = _safe_close_identifier(value, reason=reason).lower()
    if not _CLOSE_DIGEST_RE.fullmatch(value):
        raise CoordinationClientError(reason)
    return value


def _safe_cleanup_proof(value: Any) -> dict[str, Any]:
    try:
        from okg.substrate.library.agent_runtimes.assignment_close import CleanupProof

        proof = CleanupProof.from_mapping(value)
        proof.validate_complete()
        return proof.as_dict()
    except Exception as exc:  # noqa: BLE001 - proof is untrusted local data
        raise CoordinationClientError("cleanup_proof_invalid") from exc


def _safe_path(value: Any, *, reason: str = "registry_unavailable") -> str:
    if not isinstance(value, str):
        raise CoordinationClientError(reason)
    value = value.strip()
    if (
        not value
        or not value.startswith("/")
        or "\x00" in value
        or "\n" in value
        or "\r" in value
        or any(part in {".", ".."} for part in Path(value).parts)
    ):
        raise CoordinationClientError(reason)
    return value


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CoordinationClientError("registry_unavailable")
        result[key] = value
    return result


def _owner_allowed(owner: int, expected_uid: int | None) -> bool:
    # A root-owned /etc chain is valid in hermetic tests running as the
    # unprivileged test uid, while production's expected uid=0 remains exact.
    return expected_uid is None or owner in {0, expected_uid}


def _safe_readable_mode(mode: int) -> bool:
    """Allow owner/group read, but no world read, writes, or execute bits."""

    return (
        bool(mode & 0o400)
        and not bool(mode & 0o007)
        and not bool(mode & 0o033)
        and not bool(mode & 0o111)
    )


def _safe_directory_ancestor(
    directory: os.stat_result, *, expected_uid: int | None
) -> bool:
    """Allow trusted ancestors and the system's shared temporary directory.

    A Linux runner commonly places pytest's private directory below the
    root-owned, sticky ``01777`` ``/tmp``.  The sticky bit prevents another
    principal from replacing entries owned by the test user; the registry
    and referenced files still undergo exact owner/mode/inode checks below.
    Other writable ancestors remain refused.
    """

    if not stat.S_ISDIR(directory.st_mode) or stat.S_ISLNK(directory.st_mode):
        return False
    if not _owner_allowed(directory.st_uid, expected_uid):
        return False
    mode = stat.S_IMODE(directory.st_mode)
    return not mode & 0o022 or (directory.st_uid == 0 and mode == 0o1777)


def _validate_directory_ancestors(path: Path, *, expected_uid: int | None) -> None:
    """Prove a fixed-path directory chain is owned and not redirectable."""

    current = Path(path.anchor)
    try:
        for component in path.parent.parts[1:]:
            current /= component
            directory = current.lstat()
            if not _safe_directory_ancestor(directory, expected_uid=expected_uid):
                raise CoordinationClientError("registry_unavailable")
    except CoordinationClientError:
        raise
    except OSError as exc:
        raise CoordinationClientError("registry_unavailable") from exc


def _read_registry_file(
    path: Path,
    *,
    expected_uid: int | None = 0,
) -> bytes:
    """Read a fixed registry without links, writes, or oversized input."""

    if not path.is_absolute():
        raise CoordinationClientError("registry_unavailable")
    try:
        metadata = path.lstat()
        _validate_directory_ancestors(path, expected_uid=expected_uid)
    except CoordinationClientError:
        raise
    except OSError as exc:
        raise CoordinationClientError("registry_unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or not _owner_allowed(metadata.st_uid, expected_uid)
        or not _safe_readable_mode(stat.S_IMODE(metadata.st_mode))
    ):
        raise CoordinationClientError("registry_unavailable")
    try:
        fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise CoordinationClientError("registry_unavailable") from exc
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not _owner_allowed(opened.st_uid, expected_uid)
            or not _safe_readable_mode(stat.S_IMODE(opened.st_mode))
            or (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
        ):
            raise CoordinationClientError("registry_unavailable")
        chunks: list[bytes] = []
        size = 0
        while size <= MAX_REGISTRY_BYTES:
            chunk = os.read(fd, min(65536, MAX_REGISTRY_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        if size > MAX_REGISTRY_BYTES:
            raise CoordinationClientError("registry_too_large")
        return b"".join(chunks)
    except CoordinationClientError:
        raise
    except OSError as exc:
        raise CoordinationClientError("registry_unavailable") from exc
    finally:
        os.close(fd)


def _validate_referenced_file(
    path: str,
    *,
    expected_uid: int | None,
    private: bool,
) -> None:
    """Validate one identity/known-host file before handing it to SSH.

    The open-and-fstat check closes the common replacement race between the
    initial metadata check and the actual read.  SSH still opens the path
    itself, so this check is repeated immediately before every invocation.
    """

    target = Path(path)
    try:
        _validate_directory_ancestors(target, expected_uid=expected_uid)
        metadata = target.lstat()
    except (CoordinationClientError, OSError) as exc:
        raise CoordinationClientError("registry_reference_unavailable") from exc
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or not _owner_allowed(metadata.st_uid, expected_uid)
        or mode & 0o022
        or not mode & 0o444
        or (private and not mode & 0o400)
        or (private and mode & 0o004)
    ):
        raise CoordinationClientError("registry_reference_unavailable")
    try:
        fd = os.open(
            target,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise CoordinationClientError("registry_reference_unavailable") from exc
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not _owner_allowed(opened.st_uid, expected_uid)
            or stat.S_IMODE(opened.st_mode) != mode
            or (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
        ):
            raise CoordinationClientError("registry_reference_unavailable")
    except CoordinationClientError:
        raise
    except OSError as exc:
        raise CoordinationClientError("registry_reference_unavailable") from exc
    finally:
        os.close(fd)


def _validate_route_references(
    config: ClientRoute,
    *,
    expected_uid: int | None,
) -> None:
    _validate_referenced_file(
        config.identity_file,
        expected_uid=expected_uid,
        private=True,
    )
    _validate_referenced_file(
        config.known_hosts_file,
        expected_uid=expected_uid,
        private=False,
    )


def load_client_registry(
    path: Path = DEFAULT_CLIENT_REGISTRY_PATH,
    *,
    expected_uid: int | None = 0,
) -> ClientRegistry:
    """Load and validate one fixed-path, operator-managed registry.

    ``path`` and ``expected_uid`` are test seams for hermetic fixtures.  The
    CLI always uses :data:`DEFAULT_CLIENT_REGISTRY_PATH` with ``expected_uid=0``.
    """

    raw = _read_registry_file(Path(path), expected_uid=expected_uid)
    try:
        decoded = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except CoordinationClientError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CoordinationClientError("registry_unavailable") from exc
    if not isinstance(decoded, Mapping) or set(decoded) != {"routes", "workspaces"}:
        raise CoordinationClientError("registry_unavailable")

    routes_raw = decoded.get("routes")
    workspaces_raw = decoded.get("workspaces")
    if not isinstance(routes_raw, Mapping) or not routes_raw:
        raise CoordinationClientError("registry_unavailable")
    if not isinstance(workspaces_raw, Mapping) or not workspaces_raw:
        raise CoordinationClientError("registry_unavailable")

    routes: dict[str, ClientRoute] = {}
    for raw_route, raw_value in routes_raw.items():
        route = _safe_token(raw_route)
        if route in routes or not isinstance(raw_value, Mapping):
            raise CoordinationClientError("registry_unavailable")
        if not _REQUIRED_ROUTE_FIELDS.issubset(raw_value) or not set(
            raw_value
        ).issubset(_REGISTRY_ROUTE_FIELDS):
            raise CoordinationClientError("registry_unavailable")
        host = raw_value.get("host")
        user = raw_value.get("user")
        if (
            not isinstance(host, str)
            or not _HOST_RE.fullmatch(host.strip())
            or any(char.isspace() for char in host)
            or not isinstance(user, str)
            or not _USER_RE.fullmatch(user.strip())
        ):
            raise CoordinationClientError("registry_unavailable")
        identity_file = _safe_path(raw_value.get("identity_file"))
        known_hosts_file = _safe_path(raw_value.get("known_hosts_file"))
        port = raw_value.get("port")
        if port is None:
            port = 22
        if (
            isinstance(port, bool)
            or not isinstance(port, int)
            or not 1 <= port <= 65535
        ):
            raise CoordinationClientError("registry_unavailable")
        routes[route] = ClientRoute(
            route=route,
            host=host.strip(),
            user=user.strip(),
            identity_file=identity_file,
            known_hosts_file=known_hosts_file,
            port=port,
        )
    # One registry file is one local authority boundary.  It may expose
    # several logical endpoints (for example local Claude and local Codex),
    # but all must use the same enrolled SSH identity.  A route/grant switch
    # can therefore never select a second local principal's credential.
    authority_bindings = {(item.user, item.identity_file) for item in routes.values()}
    if len(authority_bindings) != 1:
        raise CoordinationClientError("registry_authority_mismatch")
    for route in routes.values():
        _validate_route_references(route, expected_uid=expected_uid)

    workspaces: dict[str, ClientWorkspace] = {}
    for raw_grant, raw_value in workspaces_raw.items():
        grant = _safe_token(raw_grant, reason="workspace_unavailable")
        if grant in workspaces or not isinstance(raw_value, Mapping):
            raise CoordinationClientError("registry_unavailable")
        if set(raw_value) != _REGISTRY_WORKSPACE_FIELDS:
            raise CoordinationClientError("registry_unavailable")
        route = _safe_token(raw_value.get("route"))
        if route not in routes:
            raise CoordinationClientError("registry_unavailable")
        workspaces[grant] = ClientWorkspace(grant_id=grant, route=route)
    return ClientRegistry(
        routes=routes,
        workspaces=workspaces,
        operator_uid=expected_uid,
    )


def _body_name(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


def _validate_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            name = _body_name(key)
            if name in _FORBIDDEN_BODY_NAMES or any(
                marker in name for marker in _FORBIDDEN_BODY_MARKERS
            ):
                raise CoordinationClientError("caller_fields_rejected")
            result[str(key)] = _validate_payload(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_validate_payload(item) for item in value]
    return value


def _bounded_json(value: Mapping[str, Any]) -> bytes:
    try:
        encoded = json.dumps(
            dict(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CoordinationClientError("invalid_request") from exc
    if len(encoded) + 1 > MAX_REQUEST_BYTES:
        raise CoordinationClientError("request_too_large")
    return encoded + b"\n"


def _parse_read_response(raw: Mapping[str, Any], *, operation: str) -> dict[str, Any]:
    try:
        return _safe_read_result(raw, operation=operation)
    except CoordinationSshError as exc:
        raise CoordinationClientError("invalid_response") from exc


def _parse_close_response(raw: Mapping[str, Any], *, operation: str) -> dict[str, Any]:
    try:
        return _safe_close_result(raw, operation=operation)
    except CoordinationSshError as exc:
        raise CoordinationClientError("invalid_response") from exc


def _parse_response(
    raw: Any,
    *,
    operation: str | None = None,
) -> dict[str, Any]:
    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    elif isinstance(raw, bytes):
        raw_bytes = raw
    else:
        raise CoordinationClientError("invalid_response")
    if len(raw_bytes) > MAX_RESPONSE_BYTES:
        raise CoordinationClientError("response_too_large")
    try:
        decoded = json.loads(
            raw_bytes.decode("utf-8"), object_pairs_hook=_no_duplicate_keys
        )
    except (CoordinationClientError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CoordinationClientError("invalid_response") from exc
    if not isinstance(decoded, Mapping) or not set(decoded).issubset(_RESPONSE_FIELDS):
        raise CoordinationClientError("invalid_response")
    if operation in {_OPERATION_STATUS, _OPERATION_RESUME}:
        return _parse_read_response(decoded, operation=operation)
    if operation in {
        OPERATION_PREPARE_ASSIGNMENT_CLOSE,
        OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
    }:
        return _parse_close_response(decoded, operation=operation)
    result: dict[str, Any] = {}
    for key, value in decoded.items():
        if key == "start_plan":
            try:
                result[key] = AssignmentStartPlan.from_mapping(value).as_dict()
            except AssignmentHandshakeError as exc:
                raise CoordinationClientError("invalid_response") from exc
        elif isinstance(value, str):
            if len(value) > 512 or "\n" in value or "\r" in value:
                raise CoordinationClientError("invalid_response")
            if key in {"reason_code", "rejection_code"} and (
                _safe_reason_code(value) != value
            ):
                raise CoordinationClientError("invalid_response")
        elif isinstance(value, bool):
            pass
        elif isinstance(value, int):
            if key in {"assignment_generation", "revision", "current_generation"}:
                if value < 0:
                    raise CoordinationClientError("invalid_response")
            elif key not in {"lease_expires_at"}:
                raise CoordinationClientError("invalid_response")
        elif isinstance(value, float):
            if key != "lease_expires_at":
                raise CoordinationClientError("invalid_response")
        elif value is not None:
            raise CoordinationClientError("invalid_response")
        result[str(key)] = value
    if "ok" not in result or not isinstance(result["ok"], bool):
        raise CoordinationClientError("invalid_response")
    return result


def _safe_remote_command(route: str) -> tuple[str, ...]:
    # Separate argv entries are intentional: no shell is involved and the
    # route is already constrained to one opaque registry token.
    return ("okg", "agent-ssh-run", "--route", route)


def _ssh_argv(config: ClientRoute) -> list[str]:
    """Build the one fixed SSH argv shape used by every tool call."""

    return [
        "/usr/bin/ssh",
        "-F",
        "/dev/null",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "IdentityAgent=none",
        "-o",
        "ForwardAgent=no",
        "-o",
        "ClearAllForwardings=yes",
        "-o",
        "ForwardX11=no",
        "-o",
        "ForwardX11Trusted=no",
        "-o",
        "RequestTTY=no",
        "-o",
        "PermitLocalCommand=no",
        "-o",
        "ProxyCommand=none",
        "-o",
        "ProxyJump=none",
        "-o",
        "Tunnel=no",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        f"UserKnownHostsFile={config.known_hosts_file}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-i",
        config.identity_file,
        "-p",
        str(config.port),
        f"{config.user}@{config.host}",
    ]


def _terminate_and_reap(process: subprocess.Popen[bytes]) -> None:
    """Stop a timed-out child and reap it without surfacing process details."""

    try:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            process.kill()
    except (OSError, ProcessLookupError):
        pass
    try:
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _run_bounded_ssh(
    argv: list[str],
    *,
    input_bytes: bytes,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[bytes]:
    """Run SSH while capping stdout before it can consume unbounded memory."""

    safe_env = {"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"}
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env=safe_env,
        )
    except (OSError, ValueError) as exc:
        raise CoordinationClientError("ssh_transport_failed") from exc
    if process.stdin is None or process.stdout is None:
        _terminate_and_reap(process)
        raise CoordinationClientError("ssh_transport_failed")
    selector = selectors.DefaultSelector()
    output = bytearray()
    pending = memoryview(input_bytes)
    try:
        stdin_fd = process.stdin.fileno()
        stdout_fd = process.stdout.fileno()
        os.set_blocking(stdin_fd, False)
        os.set_blocking(stdout_fd, False)
        selector.register(stdout_fd, selectors.EVENT_READ, "stdout")
        selector.register(stdin_fd, selectors.EVENT_WRITE, "stdin")
        deadline = time.monotonic() + timeout_seconds
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CoordinationClientError("ssh_timeout")
            events = selector.select(remaining)
            if not events:
                raise CoordinationClientError("ssh_timeout")
            for key, mask in events:
                if key.data == "stdin" and mask & selectors.EVENT_WRITE:
                    try:
                        written = os.write(key.fd, pending)
                    except (BrokenPipeError, OSError):
                        written = 0
                    if written:
                        pending = pending[written:]
                    if not pending or not written:
                        selector.unregister(key.fd)
                        process.stdin.close()
                elif key.data == "stdout" and mask & selectors.EVENT_READ:
                    try:
                        chunk = os.read(key.fd, 65536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fd)
                        process.stdout.close()
                        continue
                    output.extend(chunk)
                    if len(output) > MAX_RESPONSE_BYTES:
                        raise CoordinationClientError("response_too_large")
        try:
            process.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise CoordinationClientError("ssh_timeout") from exc
        return subprocess.CompletedProcess(argv, process.returncode, bytes(output), b"")
    except CoordinationClientError:
        _terminate_and_reap(process)
        raise
    except (OSError, ValueError) as exc:
        _terminate_and_reap(process)
        raise CoordinationClientError("ssh_transport_failed") from exc
    finally:
        selector.close()
        for stream in (process.stdin, process.stdout):
            try:
                if stream is not None and not stream.closed:
                    stream.close()
            except OSError:
                pass


class CoordinationClient:
    """Synchronous bridge backing the local coordination MCP tools."""

    def __init__(
        self,
        *,
        registry: ClientRegistry,
        route: str,
        grant_id: str,
        runner: Callable[..., Any] | None = None,
        start_lifecycle_factory: Callable[
            [AssignmentStartPlan], PairedWorkspaceLifecycle
        ]
        | None = None,
        close_lifecycle_factory: Callable[
            [AssignmentStartPlan], PairedWorkspaceLifecycle
        ]
        | None = None,
        timeout_seconds: int = DEFAULT_SSH_TIMEOUT_SECONDS,
    ) -> None:
        try:
            route = validate_coordination_route(route)
        except ValueError as exc:
            raise CoordinationClientError("invalid_route") from exc
        grant_id = _safe_token(grant_id, reason="workspace_unavailable")
        if route not in registry.routes:
            raise CoordinationClientError("route_not_configured")
        authority_bindings = {
            (item.user, item.identity_file) for item in registry.routes.values()
        }
        if len(authority_bindings) != 1:
            raise CoordinationClientError("registry_authority_mismatch")
        workspace = registry.workspaces.get(grant_id)
        if workspace is None or workspace.route != route:
            raise CoordinationClientError("workspace_route_mismatch")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int)
            or timeout_seconds <= 0
            or timeout_seconds > 60
        ):
            raise ValueError("timeout_seconds must be between 1 and 60")
        self.registry = registry
        self.route = route
        self.grant_id = grant_id
        # ``None`` selects the production bounded runner.  A callable remains
        # as a narrow hermetic test seam; production never accepts one from
        # MCP config or CLI argv.
        self._runner = runner
        self._start_lifecycle_factory = start_lifecycle_factory
        self._close_lifecycle_factory = close_lifecycle_factory
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_environment(
        cls,
        *,
        registry_path: Path = DEFAULT_CLIENT_REGISTRY_PATH,
        runner: Callable[..., Any] | None = None,
        start_lifecycle_factory: Callable[
            [AssignmentStartPlan], PairedWorkspaceLifecycle
        ]
        | None = None,
        close_lifecycle_factory: Callable[
            [AssignmentStartPlan], PairedWorkspaceLifecycle
        ]
        | None = None,
    ) -> "CoordinationClient":
        """Construct from only the two assignment-local opaque values."""

        route = os.environ.get("OKG_AGENT_SSH_ROUTE")
        grant_id = os.environ.get("OKG_AGENT_SSH_GRANT")
        if route is None or grant_id is None:
            raise CoordinationClientError("workspace_reference_missing")
        return cls(
            registry=load_client_registry(registry_path),
            route=route,
            grant_id=grant_id,
            runner=runner,
            start_lifecycle_factory=start_lifecycle_factory,
            close_lifecycle_factory=close_lifecycle_factory,
        )

    def _call(self, request: Mapping[str, Any]) -> dict[str, Any]:
        encoded = _bounded_json(request)
        route_config = self.registry.routes[self.route]
        if self.registry.operator_uid is not None:
            _validate_route_references(
                route_config,
                expected_uid=self.registry.operator_uid,
            )
        argv = [*_ssh_argv(route_config), *_safe_remote_command(self.route)]
        safe_env = {"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"}
        try:
            if self._runner is None:
                completed = _run_bounded_ssh(
                    argv,
                    input_bytes=encoded,
                    timeout_seconds=self.timeout_seconds,
                )
            else:
                completed = self._runner(
                    argv,
                    input=encoded,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=self.timeout_seconds,
                    env=safe_env,
                )
        except subprocess.TimeoutExpired as exc:
            raise CoordinationClientError("ssh_timeout") from exc
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise CoordinationClientError("ssh_transport_failed") from exc
        stdout = getattr(completed, "stdout", None)
        request_operation = request.get("operation")
        operation = (
            request_operation
            if request_operation
            in {
                _OPERATION_STATUS,
                _OPERATION_RESUME,
                OPERATION_PREPARE_ASSIGNMENT_CLOSE,
                OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
            }
            else None
        )
        response = _parse_response(stdout, operation=operation)
        returncode = getattr(completed, "returncode", None)
        if isinstance(returncode, bool) or not isinstance(returncode, int):
            raise CoordinationClientError("ssh_transport_failed")
        if operation in {_OPERATION_STATUS, _OPERATION_RESUME}:
            # Read failures are still bounded, authenticated read results.
            # The server reserves exit 1 for ``ok: false`` and exit 0 for a
            # successful view; preserve the closed response so callers can
            # inspect its redacted exceptions and next action.
            expected_returncode = 0 if response.get("ok") is True else 1
            if returncode == expected_returncode:
                return response
            raise CoordinationClientError("ssh_transport_failed")
        if operation in {
            OPERATION_PREPARE_ASSIGNMENT_CLOSE,
            OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
        }:
            expected_returncode = 0 if response.get("ok") is True else 1
            if returncode == expected_returncode:
                return response
            raise CoordinationClientError("ssh_transport_failed")
        if returncode == 0:
            if response.get("ok") is not True:
                raise CoordinationClientError("ssh_transport_failed")
            return response
        # The forced command reserves exit 1 for a sanitized, explicit
        # refusal.  Never let a failed transport look like successful work;
        # preserve only that exact server refusal shape for MCP callers.
        if (
            returncode == 1
            and response.get("ok") is False
            and response.get("status") == "rejected"
            and isinstance(response.get("reason_code"), str)
            and bool(response["reason_code"])
        ):
            return response
        raise CoordinationClientError("ssh_transport_failed")

    def coordination_event(
        self,
        *,
        event_type: str,
        idempotency_key: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Submit one existing bounded coordination event."""

        event_type = _safe_token(event_type, reason="event_not_allowed")
        idempotency_key = _safe_token(idempotency_key, reason="idempotency_key_invalid")
        body: dict[str, Any] = {
            "operation": "event",
            "event_type": event_type,
            "idempotency_key": idempotency_key,
        }
        if payload is not None:
            checked = _validate_payload(payload)
            if not isinstance(checked, Mapping):
                raise CoordinationClientError("invalid_payload")
            body["payload"] = dict(checked)
        return self._call(body)

    def send_message(
        self,
        *,
        recipient_assignment_id: str,
        recipient_generation: int,
        kind: str,
        idempotency_key: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send one directed message to a server-resolved recipient."""

        recipient_assignment_id = _safe_token(
            recipient_assignment_id, reason="recipient_invalid"
        )
        kind = _safe_token(kind, reason="message_kind_invalid")
        idempotency_key = _safe_token(idempotency_key, reason="idempotency_key_invalid")
        if (
            isinstance(recipient_generation, bool)
            or not isinstance(recipient_generation, int)
            or recipient_generation < 1
        ):
            raise CoordinationClientError("recipient_invalid")
        body: dict[str, Any] = {
            "operation": "send_message",
            "recipient_assignment_id": recipient_assignment_id,
            "recipient_generation": recipient_generation,
            "kind": kind,
            "idempotency_key": idempotency_key,
        }
        if payload is not None:
            checked = _validate_payload(payload)
            if not isinstance(checked, Mapping):
                raise CoordinationClientError("invalid_payload")
            body["payload"] = dict(checked)
        return self._call(body)

    def ack_message(
        self,
        *,
        message_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Acknowledge one message; recipient identity stays server-derived."""

        return self._call(
            {
                "operation": "ack_message",
                "message_id": _safe_token(message_id, reason="message_invalid"),
                "idempotency_key": _safe_token(
                    idempotency_key, reason="idempotency_key_invalid"
                ),
            }
        )

    def status(self, cursor: str | None = None) -> dict[str, Any]:
        """Read role-scoped deltas through the authenticated SSH peer."""

        body: dict[str, Any] = {"operation": _OPERATION_STATUS}
        if cursor is not None:
            body["cursor"] = _safe_cursor(cursor)
        return self._call(body)

    def resume(self) -> dict[str, Any]:
        """Recover the current assignment view without transcript inputs."""

        return self._call({"operation": _OPERATION_RESUME})

    def _prepare_assignment_close(self, *, idempotency_key: str) -> dict[str, Any]:
        return self._call(
            {
                "operation": OPERATION_PREPARE_ASSIGNMENT_CLOSE,
                "idempotency_key": _safe_close_identifier(
                    idempotency_key, reason="idempotency_key_invalid"
                ),
            }
        )

    def _finalize_assignment_close(
        self,
        *,
        close_attempt_id: str,
        readiness_digest: str,
        cleanup_digest: str,
        cleanup_proof: Mapping[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self._call(
            {
                "operation": OPERATION_FINALIZE_ASSIGNMENT_CLOSE,
                "close_attempt_id": _safe_close_identifier(
                    close_attempt_id, reason="close_attempt_invalid"
                ),
                "readiness_digest": _safe_close_digest(
                    readiness_digest, reason="readiness_digest_invalid"
                ),
                "cleanup_digest": _safe_close_digest(
                    cleanup_digest, reason="cleanup_digest_invalid"
                ),
                "cleanup_proof": _safe_cleanup_proof(cleanup_proof),
                "idempotency_key": _safe_close_identifier(
                    idempotency_key, reason="idempotency_key_invalid"
                ),
            }
        )

    def close_assignment(self, *, idempotency_key: str) -> dict[str, Any]:
        """Close through the protected server, with a legacy local seam.

        Production forced-command routes perform the complete target-local
        close under their server-owned Git/graph authority and return the
        terminal receipt from the prepare operation.  The injected lifecycle
        factory remains only for hermetic tests and older embedded callers
        whose server still returns an intermediate close plan.
        """

        if self._close_lifecycle_factory is None:
            result = self._prepare_assignment_close(idempotency_key=idempotency_key)
            if result.get("ok") is not True or result.get("status") == "retired":
                return result
            raise CoordinationClientError("assignment_close_unavailable")
        try:
            from okg.substrate.library.agent_runtimes.assignment_close import (
                AssignmentCloseError,
                AssignmentCloseOrchestrator,
            )

            return AssignmentCloseOrchestrator(
                prepare=self._prepare_assignment_close,
                finalize=self._finalize_assignment_close,
                lifecycle_factory=self._close_lifecycle_factory,
            ).close(
                idempotency_key=_safe_close_identifier(
                    idempotency_key, reason="idempotency_key_invalid"
                )
            )
        except CoordinationClientError:
            raise
        except AssignmentCloseError as exc:
            raise CoordinationClientError("assignment_close_recovery_required") from exc
        except Exception as exc:  # noqa: BLE001 - close recovery is fail-closed
            raise CoordinationClientError("assignment_close_recovery_required") from exc

    def request_assignment(
        self,
        *,
        child_kind: str,
        target_route: str,
        scope: Mapping[str, Any],
        execution_profile: str | None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Ask the authenticated current assignment to reserve one child."""

        child_kind = _safe_token(child_kind, reason="child_kind_invalid").lower()
        if child_kind not in _CHILD_KINDS:
            raise CoordinationClientError("child_kind_invalid")
        try:
            target_route = validate_coordination_route(target_route)
        except ValueError as exc:
            raise CoordinationClientError("target_route_invalid") from exc
        if not isinstance(scope, Mapping) or not set(scope).issubset(_SCOPE_FIELDS):
            raise CoordinationClientError("scope_invalid")
        safe_scope: dict[str, str] = {}
        for field, value in scope.items():
            safe_scope[str(field)] = _safe_token(value, reason="scope_invalid")
        if child_kind == "sprint":
            if execution_profile is not None:
                raise CoordinationClientError("execution_profile_invalid")
        else:
            execution_profile = _safe_token(
                execution_profile, reason="execution_profile_invalid"
            )
        body: dict[str, Any] = {
            "operation": "request_assignment",
            "child_kind": child_kind,
            "target_route": target_route,
            "scope": safe_scope,
            "idempotency_key": _safe_token(
                idempotency_key, reason="idempotency_key_invalid"
            ),
        }
        if execution_profile is not None:
            body["execution_profile"] = execution_profile
        return self._call(body)

    def _prepare_assignment_start(
        self, *, assignment_id: str, idempotency_key: str
    ) -> dict[str, Any]:
        return self._call(
            {
                "operation": "prepare_assignment_start",
                "assignment_id": _safe_token(
                    assignment_id, reason="assignment_invalid"
                ),
                "idempotency_key": _safe_token(
                    idempotency_key, reason="idempotency_key_invalid"
                ),
            }
        )

    def _finalize_assignment_start(
        self,
        *,
        assignment_id: str,
        start_attempt_id: str,
        pair_digest: str,
        pair_proof: Mapping[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not isinstance(pair_proof, Mapping) or set(pair_proof) != PAIR_PROOF_FIELDS:
            raise CoordinationClientError("pair_proof_invalid")
        return self._call(
            {
                "operation": "finalize_assignment_start",
                "assignment_id": _safe_token(
                    assignment_id, reason="assignment_invalid"
                ),
                "start_attempt_id": _safe_token(
                    start_attempt_id, reason="start_attempt_invalid"
                ),
                "pair_digest": _safe_token(pair_digest, reason="pair_digest_invalid"),
                "pair_proof": dict(pair_proof),
                "idempotency_key": _safe_token(
                    idempotency_key, reason="idempotency_key_invalid"
                ),
            }
        )

    def start_assignment(
        self, *, assignment_id: str, idempotency_key: str
    ) -> dict[str, Any]:
        """Start through the protected server, with a legacy local seam.

        An ordinary MCP bridge carries no database credential or filesystem
        authority.  Its production route therefore completes target-local
        staging and verification inside the forced-command process.  A local
        lifecycle factory remains available only to tests and older embedded
        callers that receive an intermediate start plan.
        """

        if self._start_lifecycle_factory is None:
            result = self._prepare_assignment_start(
                assignment_id=assignment_id,
                idempotency_key=idempotency_key,
            )
            if result.get("ok") is not True or result.get("status") == "active":
                return result
            raise CoordinationClientError("assignment_start_unavailable")
        return AssignmentStartOrchestrator(
            prepare=self._prepare_assignment_start,
            finalize=self._finalize_assignment_start,
            lifecycle_factory=self._start_lifecycle_factory,
        ).start(assignment_id=assignment_id, idempotency_key=idempotency_key)


def build_mcp_server(client: CoordinationClient) -> Any:
    """Build the FastMCP stdio server without duplicating the ledger."""

    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("okg-coordination")

    @mcp.tool(name="coordination_event")
    def coordination_event(
        event_type: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return client.coordination_event(
            event_type=event_type,
            idempotency_key=idempotency_key,
            payload=payload,
        )

    @mcp.tool(name="coordination_status")
    def coordination_status(cursor: str | None = None) -> dict[str, Any]:
        return client.status(cursor=cursor)

    @mcp.tool(name="coordination_resume")
    def coordination_resume() -> dict[str, Any]:
        return client.resume()

    @mcp.tool(name="close_assignment")
    def close_assignment(idempotency_key: str) -> dict[str, Any]:
        return client.close_assignment(idempotency_key=idempotency_key)

    @mcp.tool(name="send_message")
    def send_message(
        recipient_assignment_id: str,
        recipient_generation: int,
        kind: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return client.send_message(
            recipient_assignment_id=recipient_assignment_id,
            recipient_generation=recipient_generation,
            kind=kind,
            idempotency_key=idempotency_key,
            payload=payload,
        )

    @mcp.tool(name="ack_message")
    def ack_message(message_id: str, idempotency_key: str) -> dict[str, Any]:
        return client.ack_message(
            message_id=message_id,
            idempotency_key=idempotency_key,
        )

    @mcp.tool(name="request_assignment")
    def request_assignment(
        child_kind: str,
        target_route: str,
        scope: dict[str, Any],
        idempotency_key: str,
        execution_profile: str | None = None,
    ) -> dict[str, Any]:
        return client.request_assignment(
            child_kind=child_kind,
            target_route=target_route,
            scope=scope,
            execution_profile=execution_profile,
            idempotency_key=idempotency_key,
        )

    @mcp.tool(name="start_assignment")
    def start_assignment(assignment_id: str, idempotency_key: str) -> dict[str, Any]:
        return client.start_assignment(
            assignment_id=assignment_id,
            idempotency_key=idempotency_key,
        )

    return mcp


def run_client_bridge() -> int:
    """Run the local bridge's real MCP stdio lifecycle."""

    exit_code = 0
    try:
        client = CoordinationClient.from_environment()
        server = build_mcp_server(client)
        server.run(transport="stdio")
    except CoordinationClientError as exc:
        # CLI callers receive a stable refusal without registry paths, keys,
        # hosts, or subprocess diagnostics.
        print(f"coordination client refused: {exc.reason_code}", file=sys.stderr)
        exit_code = 1
    except Exception:
        print("coordination client refused: internal_error", file=sys.stderr)
        exit_code = 1
    return exit_code


__all__ = [
    "ClientRegistry",
    "ClientRoute",
    "ClientWorkspace",
    "CoordinationClient",
    "CoordinationClientError",
    "DEFAULT_CLIENT_REGISTRY_PATH",
    "MAX_RESPONSE_BYTES",
    "build_mcp_server",
    "load_client_registry",
    "run_client_bridge",
]
