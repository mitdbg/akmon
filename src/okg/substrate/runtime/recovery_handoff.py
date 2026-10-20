"""Attested handoff for a supervised DBOS worker restart.

The handoff is deliberately file based: the verifier must attest the already
running Python worker, whose environment cannot be changed by rewriting its
service definition.  Requests and responses live in a private per-user
directory, use deterministic deployment/PID paths, and are never followed
through symlinks.
"""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import time
from typing import Any, Mapping, Sequence


__all__ = [
    "CONTROLLED_RECOVERY_DIRECTORY_ENV",
    "CONTROLLED_RECOVERY_EXIT_CODE",
    "CONTROLLED_RECOVERY_MARKER_SCHEMA",
    "CONTROLLED_RECOVERY_REQUEST_SCHEMA",
    "CONTROLLED_RECOVERY_RESPONSE_SCHEMA",
    "ControlledRecoveryHandoffError",
    "ControlledRecoveryRequest",
    "ControlledRecoveryResponse",
    "attest_controlled_recovery_response",
    "cleanup_controlled_recovery_handoff",
    "controlled_recovery_request_path",
    "controlled_recovery_response_path",
    "create_controlled_recovery_request",
    "emit_controlled_recovery_marker",
    "parse_controlled_recovery_marker",
    "read_controlled_recovery_request",
    "read_controlled_recovery_response",
]


CONTROLLED_RECOVERY_REQUEST_SCHEMA = "okg.runtime.controlled_recovery.request.v1"
CONTROLLED_RECOVERY_RESPONSE_SCHEMA = "okg.runtime.controlled_recovery.response.v2"
CONTROLLED_RECOVERY_MARKER_SCHEMA = "okg.runtime.controlled_recovery.marker.v2"
CONTROLLED_RECOVERY_EXIT_CODE = 75
CONTROLLED_RECOVERY_DEFAULT_TTL_SECONDS = 300.0
CONTROLLED_RECOVERY_MAX_TTL_SECONDS = 600.0
CONTROLLED_RECOVERY_DIRECTORY_ENV = "OKG_CONTROLLED_RECOVERY_DIRECTORY"

_MAX_HANDOFF_BYTES = 64 * 1024
_REQUEST_KEYS = {
    "schema",
    "nonce",
    "deployment",
    "worker_pid",
    "restart_cutoff_epoch_ms",
    "expected_active_workflow_ids",
    "requested_at_epoch_ms",
    "expires_at_epoch_ms",
}
_RESPONSE_KEYS = _REQUEST_KEYS | {
    "status",
    "active_workflow_ids",
    "producers_stopped",
    "dispatch_stopped",
    "dbos_destroyed",
    "attested_at_epoch_ms",
}


class ControlledRecoveryHandoffError(RuntimeError):
    """A handoff artifact was absent from the trusted file contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ControlledRecoveryRequest:
    nonce: str
    deployment: str
    worker_pid: int
    restart_cutoff_epoch_ms: int
    expected_active_workflow_ids: tuple[str, ...]
    requested_at_epoch_ms: int
    expires_at_epoch_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": CONTROLLED_RECOVERY_REQUEST_SCHEMA,
            "nonce": self.nonce,
            "deployment": self.deployment,
            "worker_pid": self.worker_pid,
            "restart_cutoff_epoch_ms": self.restart_cutoff_epoch_ms,
            "expected_active_workflow_ids": list(self.expected_active_workflow_ids),
            "requested_at_epoch_ms": self.requested_at_epoch_ms,
            "expires_at_epoch_ms": self.expires_at_epoch_ms,
        }


@dataclass(frozen=True)
class ControlledRecoveryResponse:
    request: ControlledRecoveryRequest
    active_workflow_ids: tuple[str, ...]
    attested_at_epoch_ms: int
    dbos_destroyed: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.request.to_dict(),
            "schema": CONTROLLED_RECOVERY_RESPONSE_SCHEMA,
            "status": "attested" if self.dbos_destroyed else "interrupted",
            "active_workflow_ids": list(self.active_workflow_ids),
            "producers_stopped": True,
            "dispatch_stopped": True,
            "dbos_destroyed": self.dbos_destroyed,
            "attested_at_epoch_ms": self.attested_at_epoch_ms,
        }


def _epoch_ms() -> int:
    return time.time_ns() // 1_000_000


def _require_int(value: object, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ControlledRecoveryHandoffError(
            "handoff_field_invalid",
            f"{field} must be an integer",
        )
    if value < minimum:
        raise ControlledRecoveryHandoffError(
            "handoff_field_invalid",
            f"{field} must be at least {minimum}",
        )
    return value


def _workflow_ids(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ControlledRecoveryHandoffError(
            "handoff_field_invalid",
            f"{field} must be a list",
        )
    rows: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item or len(item) > 512:
            raise ControlledRecoveryHandoffError(
                "handoff_field_invalid",
                f"{field} contains an invalid workflow id",
            )
        rows.append(item)
    canonical = tuple(sorted(set(rows)))
    if tuple(rows) != canonical:
        raise ControlledRecoveryHandoffError(
            "handoff_field_noncanonical",
            f"{field} must be sorted and unique",
        )
    return canonical


def _canonical_workflow_ids(value: object, *, field: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise ControlledRecoveryHandoffError(
            "handoff_field_invalid",
            f"{field} must be a list",
        )
    rows: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item or len(item) > 512:
            raise ControlledRecoveryHandoffError(
                "handoff_field_invalid",
                f"{field} contains an invalid workflow id",
            )
        rows.append(item)
    return tuple(sorted(set(rows)))


def _nonce(value: object) -> str:
    if not isinstance(value, str) or not 32 <= len(value) <= 128:
        raise ControlledRecoveryHandoffError(
            "handoff_nonce_invalid",
            "nonce must be a 32-128 character URL-safe token",
        )
    if any(not (char.isascii() and (char.isalnum() or char in "_-")) for char in value):
        raise ControlledRecoveryHandoffError(
            "handoff_nonce_invalid",
            "nonce must be a 32-128 character URL-safe token",
        )
    return value


def _deployment(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ControlledRecoveryHandoffError(
            "handoff_deployment_invalid",
            "deployment must be a non-empty string",
        )
    return value


def _directory_path(directory: Path | None = None) -> Path:
    if directory is not None:
        return Path(directory)
    configured = os.environ.get(CONTROLLED_RECOVERY_DIRECTORY_ENV)
    if configured:
        return Path(configured)
    return Path("/tmp") / f"okg-runtime-recovery-{os.getuid()}"


def _validate_directory(path: Path) -> None:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise ControlledRecoveryHandoffError(
            "handoff_directory_missing",
            f"controlled recovery directory does not exist: {path}",
        ) from exc
    if not stat.S_ISDIR(info.st_mode):
        raise ControlledRecoveryHandoffError(
            "handoff_directory_invalid",
            f"controlled recovery directory is not a directory: {path}",
        )
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ControlledRecoveryHandoffError(
            "handoff_directory_insecure",
            "controlled recovery directory must be owned by this user with mode 0700",
        )


def _ensure_directory(directory: Path | None = None) -> Path:
    path = _directory_path(directory)
    if not path.is_absolute():
        raise ControlledRecoveryHandoffError(
            "handoff_directory_relative",
            "controlled recovery directory must be absolute",
        )
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        # An existing directory is validated immediately below.
        pass
    except OSError as exc:
        raise ControlledRecoveryHandoffError(
            "handoff_directory_create_failed",
            f"failed to create controlled recovery directory: {exc}",
        ) from exc
    _validate_directory(path)
    return path


def _path_token(deployment: str) -> str:
    return hashlib.sha256(deployment.encode("utf-8")).hexdigest()[:16]


def controlled_recovery_request_path(
    deployment: str,
    worker_pid: int,
    *,
    directory: Path | None = None,
) -> Path:
    deployment = _deployment(deployment)
    worker_pid = _require_int(worker_pid, field="worker_pid", minimum=1)
    root = _directory_path(directory)
    return root / f"{_path_token(deployment)}.{worker_pid}.request.json"


def controlled_recovery_response_path(
    deployment: str,
    worker_pid: int,
    *,
    directory: Path | None = None,
) -> Path:
    deployment = _deployment(deployment)
    worker_pid = _require_int(worker_pid, field="worker_pid", minimum=1)
    root = _directory_path(directory)
    return root / f"{_path_token(deployment)}.{worker_pid}.response.json"


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("short write while persisting recovery handoff")
        offset += written


def _encoded(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _write_exclusive(
    path: Path,
    payload: Mapping[str, Any],
    *,
    admit_exact_readable_on_failure: bool = False,
) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    created = False

    def handle_failure(
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        nonlocal descriptor
        if exc is None:
            return False
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                # The descriptor is already unusable; preserve the root error.
                pass
            descriptor = None
        if created and admit_exact_readable_on_failure:
            try:
                readable = _read_secure(path)
            except ControlledRecoveryHandoffError:
                readable = None
            if readable is not None and dict(readable) == dict(payload):
                return True
        if created:
            try:
                os.unlink(path)
            except OSError:
                # Best-effort rollback must not replace the write failure.
                pass
        if isinstance(exc, ControlledRecoveryHandoffError):
            return False
        raise ControlledRecoveryHandoffError(
            "handoff_create_failed",
            f"failed to create controlled recovery handoff: {exc}",
        ) from exc

    with ExitStack() as failure_boundary:
        # Exit callbacks receive every BaseException subtype without an
        # analyzer-hostile catch-all clause.  Returning true is limited to the
        # existing exact-readable admission case.
        failure_boundary.push(handle_failure)
        descriptor = os.open(path, flags, 0o600)
        created = True
        _write_all(descriptor, _encoded(payload))
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        _fsync_directory(path.parent)


def _write_atomic_then_marker(
    path: Path,
    response: "ControlledRecoveryResponse",
    *,
    marker_descriptor: int,
) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp")
    _write_exclusive(temporary, response.to_dict())
    linked = False

    def handle_failure(
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        if exc is None:
            return False
        try:
            os.unlink(temporary)
        except OSError:
            # Preserve the publication error when cleanup also fails.
            pass
        if linked:
            try:
                os.unlink(path)
            except OSError:
                # Preserve the publication error when rollback also fails.
                pass
        if isinstance(exc, ControlledRecoveryHandoffError):
            return False
        raise ControlledRecoveryHandoffError(
            "handoff_publish_failed",
            f"failed to publish controlled recovery response: {exc}",
        ) from exc

    with ExitStack() as failure_boundary:
        failure_boundary.push(handle_failure)
        os.link(temporary, path, follow_symlinks=False)
        linked = True
        os.unlink(temporary)
        _fsync_directory(path.parent)
        emit_controlled_recovery_marker(
            response,
            descriptor=marker_descriptor,
        )


def _read_secure(path: Path) -> Mapping[str, Any] | None:
    try:
        before = os.lstat(path)
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_file_insecure",
            "controlled recovery handoff must be an owned 0600 regular file",
        )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ControlledRecoveryHandoffError(
            "handoff_open_failed",
            f"failed to open controlled recovery handoff: {exc}",
        ) from exc
    try:
        after = os.fstat(descriptor)
        if (
            after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or not stat.S_ISREG(after.st_mode)
            or after.st_uid != os.getuid()
            or stat.S_IMODE(after.st_mode) != 0o600
            or after.st_nlink != 1
        ):
            raise ControlledRecoveryHandoffError(
                "handoff_file_changed",
                "controlled recovery handoff changed while opening",
            )
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, min(8192, _MAX_HANDOFF_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > _MAX_HANDOFF_BYTES:
                raise ControlledRecoveryHandoffError(
                    "handoff_file_too_large",
                    "controlled recovery handoff exceeds the size limit",
                )
    finally:
        os.close(descriptor)
    try:
        decoded = json.loads(b"".join(chunks))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ControlledRecoveryHandoffError(
            "handoff_json_invalid",
            "controlled recovery handoff is not valid JSON",
        ) from exc
    if not isinstance(decoded, Mapping):
        raise ControlledRecoveryHandoffError(
            "handoff_json_invalid",
            "controlled recovery handoff must contain a JSON object",
        )
    return decoded


def _parse_request(
    payload: Mapping[str, Any],
    *,
    deployment: str,
    worker_pid: int,
    now_epoch_ms: int,
    require_fresh: bool,
) -> ControlledRecoveryRequest:
    if set(payload) != _REQUEST_KEYS:
        raise ControlledRecoveryHandoffError(
            "handoff_request_shape_invalid",
            "controlled recovery request fields do not match the schema",
        )
    if payload.get("schema") != CONTROLLED_RECOVERY_REQUEST_SCHEMA:
        raise ControlledRecoveryHandoffError(
            "handoff_request_schema_invalid",
            "controlled recovery request schema is unsupported",
        )
    parsed = ControlledRecoveryRequest(
        nonce=_nonce(payload.get("nonce")),
        deployment=_deployment(payload.get("deployment")),
        worker_pid=_require_int(
            payload.get("worker_pid"), field="worker_pid", minimum=1
        ),
        restart_cutoff_epoch_ms=_require_int(
            payload.get("restart_cutoff_epoch_ms"),
            field="restart_cutoff_epoch_ms",
            minimum=1,
        ),
        expected_active_workflow_ids=_workflow_ids(
            payload.get("expected_active_workflow_ids"),
            field="expected_active_workflow_ids",
        ),
        requested_at_epoch_ms=_require_int(
            payload.get("requested_at_epoch_ms"),
            field="requested_at_epoch_ms",
            minimum=1,
        ),
        expires_at_epoch_ms=_require_int(
            payload.get("expires_at_epoch_ms"),
            field="expires_at_epoch_ms",
            minimum=1,
        ),
    )
    if parsed.deployment != deployment or parsed.worker_pid != worker_pid:
        raise ControlledRecoveryHandoffError(
            "handoff_request_identity_mismatch",
            "controlled recovery request does not match this worker",
        )
    ttl_ms = parsed.expires_at_epoch_ms - parsed.requested_at_epoch_ms
    if ttl_ms <= 0 or ttl_ms > int(CONTROLLED_RECOVERY_MAX_TTL_SECONDS * 1000):
        raise ControlledRecoveryHandoffError(
            "handoff_request_ttl_invalid",
            "controlled recovery request TTL is outside the allowed bound",
        )
    if require_fresh and not (
        parsed.requested_at_epoch_ms <= now_epoch_ms <= parsed.expires_at_epoch_ms
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_request_stale",
            "controlled recovery request is not fresh",
        )
    return parsed


def create_controlled_recovery_request(
    *,
    deployment: str,
    worker_pid: int,
    restart_cutoff_epoch_ms: int,
    expected_active_workflow_ids: Sequence[str],
    ttl_seconds: float = CONTROLLED_RECOVERY_DEFAULT_TTL_SECONDS,
    nonce: str | None = None,
    now_epoch_ms: int | None = None,
    directory: Path | None = None,
) -> ControlledRecoveryRequest:
    """Create the verifier-owned request for one already-running worker."""
    deployment = _deployment(deployment)
    worker_pid = _require_int(worker_pid, field="worker_pid", minimum=1)
    restart_cutoff_epoch_ms = _require_int(
        restart_cutoff_epoch_ms,
        field="restart_cutoff_epoch_ms",
        minimum=1,
    )
    if not 0 < ttl_seconds <= CONTROLLED_RECOVERY_MAX_TTL_SECONDS:
        raise ControlledRecoveryHandoffError(
            "handoff_request_ttl_invalid",
            "controlled recovery request TTL is outside the allowed bound",
        )
    requested_at = now_epoch_ms if now_epoch_ms is not None else _epoch_ms()
    requested_at = _require_int(requested_at, field="requested_at_epoch_ms", minimum=1)
    request = ControlledRecoveryRequest(
        nonce=_nonce(secrets.token_urlsafe(32) if nonce is None else nonce),
        deployment=deployment,
        worker_pid=worker_pid,
        restart_cutoff_epoch_ms=restart_cutoff_epoch_ms,
        expected_active_workflow_ids=_canonical_workflow_ids(
            list(expected_active_workflow_ids),
            field="expected_active_workflow_ids",
        ),
        requested_at_epoch_ms=requested_at,
        expires_at_epoch_ms=requested_at + int(ttl_seconds * 1000),
    )
    _ensure_directory(directory)
    path = controlled_recovery_request_path(deployment, worker_pid, directory=directory)
    _write_exclusive(
        path,
        request.to_dict(),
        admit_exact_readable_on_failure=True,
    )
    return request


def read_controlled_recovery_request(
    deployment: str,
    worker_pid: int,
    *,
    now_epoch_ms: int | None = None,
    directory: Path | None = None,
) -> ControlledRecoveryRequest | None:
    """Read and fully validate a fresh request for ``worker_pid``."""
    deployment = _deployment(deployment)
    worker_pid = _require_int(worker_pid, field="worker_pid", minimum=1)
    root = _directory_path(directory)
    if not root.exists():
        return None
    _validate_directory(root)
    path = controlled_recovery_request_path(deployment, worker_pid, directory=directory)
    payload = _read_secure(path)
    if payload is None:
        return None
    return _parse_request(
        payload,
        deployment=deployment,
        worker_pid=worker_pid,
        now_epoch_ms=now_epoch_ms if now_epoch_ms is not None else _epoch_ms(),
        require_fresh=True,
    )


def attest_controlled_recovery_response(
    request: ControlledRecoveryRequest,
    *,
    active_workflow_ids: Sequence[str],
    producers_stopped: bool,
    dispatch_stopped: bool,
    dbos_destroyed: bool,
    now_epoch_ms: int | None = None,
    directory: Path | None = None,
    marker_descriptor: int = 2,
) -> ControlledRecoveryResponse:
    """Persist a response only for the exact safe controlled-exit boundary."""
    active = _canonical_workflow_ids(
        list(active_workflow_ids), field="active_workflow_ids"
    )
    if not active or active != request.expected_active_workflow_ids:
        raise ControlledRecoveryHandoffError(
            "handoff_active_workflows_mismatch",
            "active workflow ids do not exactly match the recovery request",
        )
    if not producers_stopped or not dispatch_stopped:
        raise ControlledRecoveryHandoffError(
            "handoff_shutdown_incomplete",
            "controlled recovery requires successful producer and dispatch shutdown",
        )
    if not isinstance(dbos_destroyed, bool):
        raise ControlledRecoveryHandoffError(
            "handoff_shutdown_invalid",
            "dbos_destroyed must be a boolean",
        )
    attested_at = now_epoch_ms if now_epoch_ms is not None else _epoch_ms()
    attested_at = _require_int(attested_at, field="attested_at_epoch_ms", minimum=1)
    if not (
        request.requested_at_epoch_ms <= attested_at <= request.expires_at_epoch_ms
        and attested_at >= request.restart_cutoff_epoch_ms
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_request_stale",
            "controlled recovery attestation is outside its request/cutoff window",
        )
    response = ControlledRecoveryResponse(
        request=request,
        active_workflow_ids=active,
        attested_at_epoch_ms=attested_at,
        dbos_destroyed=dbos_destroyed,
    )
    _ensure_directory(directory)
    path = controlled_recovery_response_path(
        request.deployment, request.worker_pid, directory=directory
    )
    _write_atomic_then_marker(
        path,
        response,
        marker_descriptor=marker_descriptor,
    )
    return response


def _parse_response_payload(
    payload: Mapping[str, Any],
    *,
    expected_request: ControlledRecoveryRequest,
) -> ControlledRecoveryResponse:
    if set(payload) != _RESPONSE_KEYS:
        raise ControlledRecoveryHandoffError(
            "handoff_response_shape_invalid",
            "controlled recovery response fields do not match the schema",
        )
    if payload.get("schema") != CONTROLLED_RECOVERY_RESPONSE_SCHEMA:
        raise ControlledRecoveryHandoffError(
            "handoff_response_schema_invalid",
            "controlled recovery response schema is unsupported",
        )
    request_payload = {key: payload[key] for key in _REQUEST_KEYS}
    request_payload["schema"] = CONTROLLED_RECOVERY_REQUEST_SCHEMA
    request = _parse_request(
        request_payload,
        deployment=expected_request.deployment,
        worker_pid=expected_request.worker_pid,
        now_epoch_ms=_epoch_ms(),
        require_fresh=False,
    )
    if request != expected_request:
        raise ControlledRecoveryHandoffError(
            "handoff_response_request_mismatch",
            "controlled recovery response does not echo the expected request",
        )
    destroyed = payload.get("dbos_destroyed")
    expected_status = "attested" if destroyed is True else "interrupted"
    if (
        not isinstance(destroyed, bool)
        or payload.get("status") != expected_status
        or payload.get("producers_stopped") is not True
        or payload.get("dispatch_stopped") is not True
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_response_unattested",
            "controlled recovery response does not attest successful shutdown",
        )
    active = _workflow_ids(
        payload.get("active_workflow_ids"), field="active_workflow_ids"
    )
    attested_at = _require_int(
        payload.get("attested_at_epoch_ms"),
        field="attested_at_epoch_ms",
        minimum=1,
    )
    if (
        not active
        or active != request.expected_active_workflow_ids
        or not request.requested_at_epoch_ms
        <= attested_at
        <= request.expires_at_epoch_ms
        or attested_at < request.restart_cutoff_epoch_ms
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_response_attestation_mismatch",
            "controlled recovery response attestation does not match the request",
        )
    return ControlledRecoveryResponse(
        request=request,
        active_workflow_ids=active,
        attested_at_epoch_ms=attested_at,
        dbos_destroyed=destroyed,
    )


def parse_controlled_recovery_marker(
    line: str | bytes,
    *,
    expected_request: ControlledRecoveryRequest,
) -> ControlledRecoveryResponse:
    """Parse one exact post-restart log line into a terminal receipt."""
    encoded = line.encode("utf-8") if isinstance(line, str) else line
    if not encoded or len(encoded) > _MAX_HANDOFF_BYTES:
        raise ControlledRecoveryHandoffError(
            "handoff_marker_invalid",
            "controlled recovery marker is empty or oversized",
        )
    try:
        payload = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ControlledRecoveryHandoffError(
            "handoff_marker_invalid",
            "controlled recovery marker is not valid JSON",
        ) from exc
    if not isinstance(payload, Mapping) or set(payload) != {
        "schema",
        "event",
        "response",
    }:
        raise ControlledRecoveryHandoffError(
            "handoff_marker_shape_invalid",
            "controlled recovery marker fields do not match the schema",
        )
    if (
        payload.get("schema") != CONTROLLED_RECOVERY_MARKER_SCHEMA
        or payload.get("event") != "controlled_recovery_exit"
        or not isinstance(payload.get("response"), Mapping)
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_marker_unattested",
            "controlled recovery marker is not a terminal attestation",
        )
    return _parse_response_payload(
        payload["response"],
        expected_request=expected_request,
    )


def read_controlled_recovery_response(
    deployment: str,
    worker_pid: int,
    *,
    expected_request: ControlledRecoveryRequest,
    expected_marker: ControlledRecoveryResponse,
    directory: Path | None = None,
) -> ControlledRecoveryResponse | None:
    """Read a response bound to the request and terminal log marker."""
    deployment = _deployment(deployment)
    worker_pid = _require_int(worker_pid, field="worker_pid", minimum=1)
    if (
        expected_request.deployment != deployment
        or expected_request.worker_pid != worker_pid
        or expected_marker.request != expected_request
    ):
        raise ControlledRecoveryHandoffError(
            "handoff_response_expected_identity_mismatch",
            "expected request/marker do not match the requested worker",
        )
    root = _directory_path(directory)
    if not root.exists():
        return None
    _validate_directory(root)
    path = controlled_recovery_response_path(
        deployment, worker_pid, directory=directory
    )
    payload = _read_secure(path)
    if payload is None:
        return None
    response = _parse_response_payload(
        payload,
        expected_request=expected_request,
    )
    if response != expected_marker:
        raise ControlledRecoveryHandoffError(
            "handoff_response_marker_mismatch",
            "controlled recovery response does not match the terminal marker",
        )
    return response


def emit_controlled_recovery_marker(
    response: ControlledRecoveryResponse,
    *,
    descriptor: int = 2,
) -> None:
    """Emit the final marker without buffered Python I/O."""
    marker = {
        "schema": CONTROLLED_RECOVERY_MARKER_SCHEMA,
        "event": "controlled_recovery_exit",
        "response": response.to_dict(),
    }
    try:
        _write_all(descriptor, _encoded(marker))
    except OSError as exc:
        raise ControlledRecoveryHandoffError(
            "handoff_marker_write_failed",
            f"failed to emit controlled recovery marker: {exc}",
        ) from exc


def cleanup_controlled_recovery_handoff(
    deployment: str,
    worker_pid: int,
    *,
    expected_nonce: str | None = None,
    directory: Path | None = None,
) -> dict[str, list[str]]:
    """Remove exact owned request/response files after verifier admission."""
    deployment = _deployment(deployment)
    worker_pid = _require_int(worker_pid, field="worker_pid", minimum=1)
    expected_nonce = _nonce(expected_nonce) if expected_nonce is not None else None
    root = _directory_path(directory)
    if not root.exists():
        return {"removed": []}
    _validate_directory(root)
    paths = (
        controlled_recovery_request_path(deployment, worker_pid, directory=directory),
        controlled_recovery_response_path(deployment, worker_pid, directory=directory),
    )
    admitted: list[Path] = []
    for path in paths:
        payload = _read_secure(path)
        if payload is None:
            continue
        if expected_nonce is not None and payload.get("nonce") != expected_nonce:
            raise ControlledRecoveryHandoffError(
                "handoff_cleanup_nonce_mismatch",
                "refusing to remove a handoff for a different nonce",
            )
        admitted.append(path)
    removed: list[str] = []
    for path in admitted:
        try:
            os.unlink(path)
        except OSError as exc:
            raise ControlledRecoveryHandoffError(
                "handoff_cleanup_failed",
                f"failed to remove controlled recovery handoff: {exc}",
            ) from exc
        removed.append(str(path))
    if removed:
        _fsync_directory(root)
    return {"removed": removed}
