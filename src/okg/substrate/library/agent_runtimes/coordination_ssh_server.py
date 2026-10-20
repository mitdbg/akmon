"""Fail-closed identity admission for a fixed coordination SSH route.

This module is only valid behind an OpenSSH ``ForceCommand`` that supplies the
route as a literal argument and enables ``ExposeAuthInfo``.  The login account
must be dedicated to this boundary, with forwarding, TTYs, user environment,
and user rc files disabled.  An auth-info file is owned by that login UID, so
file metadata alone cannot defend against untrusted code running as the same
UID; the fixed command and dedicated-account boundary are part of the trust
model.

This is deliberately not a client bridge, command wrapper, or deployment
installer.  It returns only the identity enrolled for the authenticated SSH
key and normalized connection tuple.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


_MAX_AUTH_INFO_BYTES = 64 * 1024
_MAX_AUTH_INFO_LINE_BYTES = 16 * 1024
_MAX_IDENTITY_TEXT = 256
_ROUTE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$")
_KEY_ALGORITHM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9@._+-]{0,255}$")


class CoordinationSshAuthenticationError(RuntimeError):
    """One sanitized, fail-closed SSH transport refusal."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class CoordinationSshEnrollment:
    """Server-owned binding for exactly one SSH public-key credential."""

    credential_id: str
    principal: str
    session_id: str
    allowed_routes: tuple[str, ...]
    allowed_source_addresses: tuple[str, ...]
    allowed_target_addresses: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CoordinationSshServerIdentity:
    """Identity admitted from sshd evidence and closed enrollment state."""

    credential_id: str
    principal: str
    session_id: str
    route: str
    source_address: str
    source_port: int
    target_address: str
    target_port: int
    verified_by: str = "sshd:expose-auth-info"


@dataclass(frozen=True, slots=True)
class _AuthInfoSnapshot:
    device: int
    inode: int
    uid: int
    mode: int
    link_count: int
    size: int
    mtime_ns: int
    ctime_ns: int


def coordination_ssh_public_key_credential_id(public_key: str) -> str:
    """Return the canonical credential identifier for one SSH public key."""

    if not isinstance(public_key, str):
        _refuse("invalid_public_key")
    try:
        encoded = public_key.encode("ascii")
    except UnicodeEncodeError:
        _refuse("invalid_public_key")
    if (
        not encoded
        or len(encoded) > _MAX_AUTH_INFO_LINE_BYTES
        or b"\n" in encoded
        or b"\r" in encoded
    ):
        _refuse("invalid_public_key")
    fields = encoded.split()
    if len(fields) < 2:
        _refuse("invalid_public_key")
    algorithm = _decode_ascii_token(fields[0], _KEY_ALGORITHM_RE, "invalid_public_key")
    blob = _decode_public_key_blob(fields[1], algorithm)
    return _credential_id(blob)


def authenticate_coordination_ssh_session(
    environ: Mapping[str, str],
    *,
    route: str,
    expected_auth_file_uid: int,
    enrollments: Sequence[CoordinationSshEnrollment],
) -> CoordinationSshServerIdentity:
    """Admit one fixed-route sshd session through explicit enrollment.

    Caller-provided principal or session variables are never read.  Any
    presence of ``SSH_ORIGINAL_COMMAND`` is refused, including an empty value.
    """

    if "SSH_ORIGINAL_COMMAND" in environ:
        _refuse("caller_command_refused")
    normalized_route = _normalize_route(route)
    _validate_expected_uid(expected_auth_file_uid)

    auth_path_text = environ.get("SSH_USER_AUTH")
    connection_text = environ.get("SSH_CONNECTION")
    if not isinstance(auth_path_text, str) or not auth_path_text:
        _refuse("authentication_evidence_missing")
    if not isinstance(connection_text, str) or not connection_text.strip():
        _refuse("authentication_evidence_missing")

    auth_info = _read_auth_info(
        Path(auth_path_text), expected_uid=expected_auth_file_uid
    )
    credential_id = _credential_id_from_auth_info(auth_info)
    source_address, source_port, target_address, target_port = _parse_connection(
        connection_text
    )
    enrollment = _unique_enrollment(credential_id, enrollments)

    if normalized_route not in enrollment.allowed_routes:
        _refuse("route_not_enrolled")
    if source_address not in enrollment.allowed_source_addresses:
        _refuse("source_not_enrolled")
    if target_address not in enrollment.allowed_target_addresses:
        _refuse("target_not_enrolled")

    return CoordinationSshServerIdentity(
        credential_id=credential_id,
        principal=enrollment.principal,
        session_id=enrollment.session_id,
        route=normalized_route,
        source_address=source_address,
        source_port=source_port,
        target_address=target_address,
        target_port=target_port,
    )


def _refuse(reason_code: str) -> None:
    raise CoordinationSshAuthenticationError(reason_code)


def _normalize_route(value: object) -> str:
    if not isinstance(value, str):
        _refuse("invalid_route")
    route = value.strip()
    if route != value or not _ROUTE_RE.fullmatch(route):
        _refuse("invalid_route")
    return route


def _normalize_identity_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        _refuse("invalid_enrollment")
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > _MAX_IDENTITY_TEXT
        or any(
            ord(character) < 0x20 or ord(character) == 0x7F for character in normalized
        )
    ):
        _refuse("invalid_enrollment")
    if field == "credential_id" and normalized != value:
        _refuse("invalid_enrollment")
    return normalized


def _validate_expected_uid(expected_uid: object) -> None:
    if isinstance(expected_uid, bool) or not isinstance(expected_uid, int):
        _refuse("invalid_expected_uid")
    try:
        real_uid = os.getuid()
        effective_uid = os.geteuid()
    except (AttributeError, OSError):
        _refuse("unsupported_platform")
    if expected_uid < 0 or expected_uid != real_uid or expected_uid != effective_uid:
        _refuse("invalid_expected_uid")


def _snapshot(file_stat: os.stat_result) -> _AuthInfoSnapshot:
    return _AuthInfoSnapshot(
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
        uid=file_stat.st_uid,
        mode=stat.S_IMODE(file_stat.st_mode),
        link_count=file_stat.st_nlink,
        size=file_stat.st_size,
        mtime_ns=file_stat.st_mtime_ns,
        ctime_ns=file_stat.st_ctime_ns,
    )


def _validate_auth_stat(file_stat: os.stat_result, *, expected_uid: int) -> None:
    if (
        not stat.S_ISREG(file_stat.st_mode)
        or file_stat.st_uid != expected_uid
        or stat.S_IMODE(file_stat.st_mode) not in {0o400, 0o600}
        or file_stat.st_nlink != 1
        or file_stat.st_size <= 0
        or file_stat.st_size > _MAX_AUTH_INFO_BYTES
    ):
        _refuse("unsafe_authentication_evidence")


def _read_auth_info(path: Path, *, expected_uid: int) -> bytes:
    if not path.is_absolute():
        _refuse("authentication_evidence_unavailable")
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_NONBLOCK"):
        _refuse("unsupported_platform")
    try:
        initial_stat = path.lstat()
    except (OSError, ValueError):
        raise CoordinationSshAuthenticationError(
            "authentication_evidence_unavailable"
        ) from None
    _validate_auth_stat(initial_stat, expected_uid=expected_uid)
    initial = _snapshot(initial_stat)

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except (OSError, ValueError):
        raise CoordinationSshAuthenticationError(
            "authentication_evidence_unavailable"
        ) from None
    try:
        opened_stat = os.fstat(descriptor)
        _validate_auth_stat(opened_stat, expected_uid=expected_uid)
        if _snapshot(opened_stat) != initial:
            _refuse("changed_authentication_evidence")

        chunks: list[bytes] = []
        remaining = _MAX_AUTH_INFO_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(8192, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        auth_info = b"".join(chunks)

        final_stat = os.fstat(descriptor)
        _validate_auth_stat(final_stat, expected_uid=expected_uid)
        if _snapshot(final_stat) != initial:
            _refuse("changed_authentication_evidence")
    except CoordinationSshAuthenticationError:
        raise
    except OSError:
        raise CoordinationSshAuthenticationError(
            "authentication_evidence_unavailable"
        ) from None
    finally:
        os.close(descriptor)

    if (
        not auth_info
        or len(auth_info) > _MAX_AUTH_INFO_BYTES
        or len(auth_info) != initial.size
    ):
        _refuse("invalid_authentication_record")
    return auth_info


def _credential_id_from_auth_info(auth_info: bytes) -> str:
    lines = [line for line in auth_info.splitlines() if line]
    if len(lines) != 1 or len(lines[0]) > _MAX_AUTH_INFO_LINE_BYTES:
        _refuse("invalid_authentication_record")
    if any(value < 0x20 or value > 0x7E for value in lines[0]):
        _refuse("invalid_authentication_record")
    fields = lines[0].split()
    if len(fields) < 3 or fields[0] != b"publickey":
        _refuse("invalid_authentication_record")
    algorithm = _decode_ascii_token(
        fields[1], _KEY_ALGORITHM_RE, "invalid_authentication_record"
    )
    blob = _decode_public_key_blob(
        fields[2], algorithm, reason_code="invalid_authentication_record"
    )
    return _credential_id(blob)


def _decode_ascii_token(
    value: bytes, pattern: re.Pattern[str], reason_code: str
) -> str:
    try:
        token = value.decode("ascii")
    except UnicodeDecodeError:
        _refuse(reason_code)
    if not pattern.fullmatch(token):
        _refuse(reason_code)
    return token


def _decode_public_key_blob(
    encoded: bytes,
    algorithm: str,
    *,
    reason_code: str = "invalid_public_key",
) -> bytes:
    try:
        blob = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        _refuse(reason_code)
    if len(blob) < 5 or len(blob) > _MAX_AUTH_INFO_BYTES:
        _refuse(reason_code)
    algorithm_size = int.from_bytes(blob[:4], "big")
    if algorithm_size <= 0 or 4 + algorithm_size >= len(blob):
        _refuse(reason_code)
    blob_algorithm = _decode_ascii_token(
        blob[4 : 4 + algorithm_size], _KEY_ALGORITHM_RE, reason_code
    )
    if blob_algorithm != algorithm:
        _refuse(reason_code)
    return blob


def _credential_id(blob: bytes) -> str:
    digest = hashlib.sha256(blob).digest()
    fingerprint = base64.b64encode(digest).decode("ascii").rstrip("=")
    return f"ssh-key:sha256:{fingerprint}"


def _parse_connection(value: str) -> tuple[str, int, str, int]:
    fields = value.split()
    if len(fields) != 4:
        _refuse("invalid_connection_evidence")
    source_text, source_port_text, target_text, target_port_text = fields
    source = _normalize_address(source_text, reason_code="invalid_connection_evidence")
    target = _normalize_address(target_text, reason_code="invalid_connection_evidence")
    source_port = _parse_port(source_port_text)
    target_port = _parse_port(target_port_text)
    return source, source_port, target, target_port


def _parse_port(value: str) -> int:
    if (
        not value
        or len(value) > 5
        or not value.isascii()
        or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        _refuse("invalid_connection_evidence")
    port = int(value, 10)
    if not 1 <= port <= 65535:
        _refuse("invalid_connection_evidence")
    return port


def _normalize_address(value: object, *, reason_code: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or "%" in value
    ):
        _refuse(reason_code)
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        raise CoordinationSshAuthenticationError(reason_code) from None


def _normalize_closed_values(
    values: object,
    *,
    normalizer: object,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        _refuse("invalid_enrollment")
    normalized: list[str] = []
    for value in values:
        if normalizer == "route":
            item = _normalize_route(value)
        else:
            item = _normalize_address(value, reason_code="invalid_enrollment")
        if item in normalized:
            _refuse("invalid_enrollment")
        normalized.append(item)
    if not normalized:
        _refuse("invalid_enrollment")
    return tuple(normalized)


def _normalize_enrollment(
    enrollment: object,
) -> CoordinationSshEnrollment:
    if not isinstance(enrollment, CoordinationSshEnrollment):
        _refuse("invalid_enrollment")
    credential_id = _normalize_identity_text(
        enrollment.credential_id, field="credential_id"
    )
    if not credential_id.startswith("ssh-key:sha256:"):
        _refuse("invalid_enrollment")
    return CoordinationSshEnrollment(
        credential_id=credential_id,
        principal=_normalize_identity_text(enrollment.principal, field="principal"),
        session_id=_normalize_identity_text(enrollment.session_id, field="session_id"),
        allowed_routes=_normalize_closed_values(
            enrollment.allowed_routes, normalizer="route"
        ),
        allowed_source_addresses=_normalize_closed_values(
            enrollment.allowed_source_addresses, normalizer="address"
        ),
        allowed_target_addresses=_normalize_closed_values(
            enrollment.allowed_target_addresses, normalizer="address"
        ),
    )


def _unique_enrollment(
    credential_id: str,
    enrollments: Sequence[CoordinationSshEnrollment],
) -> CoordinationSshEnrollment:
    if isinstance(enrollments, (str, bytes)) or not isinstance(enrollments, Sequence):
        _refuse("invalid_enrollment")
    normalized = tuple(_normalize_enrollment(item) for item in enrollments)
    matches = [item for item in normalized if item.credential_id == credential_id]
    if len(matches) != 1:
        _refuse("credential_not_uniquely_enrolled")
    return matches[0]
