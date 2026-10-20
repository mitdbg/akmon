"""Trusted SSH identity and credential-free stdio configuration primitives.

This module is deliberately smaller than the eventual coordination wrapper. It
defines the two security boundaries selected by the coordination topology
spike:

* server-side identity comes from OpenSSH ``ExposeAuthInfo`` plus the observed
  TCP peer, then resolves through an enrolled credential binding; and
* agent-visible MCP configuration invokes the local SSH/MCP bridge by opaque
  route and workspace grant, and never accepts a database connection string.

The caller must install ``okg agent-ssh-run`` as an sshd ``ForceCommand`` with
``ExposeAuthInfo yes`` and forwarding disabled. The agent-side config invokes
``okg agent-ssh-client``; that process resolves the host and credential from
an operator-managed client registry and then starts the real SSH connection.
This module does not make a normal shell invocation trusted merely because it
supplied similarly named environment variables.
"""

from __future__ import annotations

import hashlib
import ipaddress
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


class SshTransportAuthenticationError(RuntimeError):
    """The sshd-authenticated peer could not be proved or enrolled."""


# Route names are opaque registry keys.  Keep one grammar shared by the
# producer, client, and forced-command reader so a value written into an
# assignment-local config is accepted or rejected consistently everywhere.
COORDINATION_ROUTE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$")


def validate_coordination_route(value: object) -> str:
    """Validate one route token without interpreting it as a host or command."""

    if not isinstance(value, str):
        raise ValueError("coordination route must be one non-option token")
    route = value.strip()
    if not COORDINATION_ROUTE_RE.fullmatch(route):
        raise ValueError("coordination route must be one non-option token")
    return route


@dataclass(frozen=True)
class SshdPeerEvidence:
    """Identity evidence emitted by sshd, before assignment lookup."""

    credential_id: str
    source_address: str
    source_port: int
    target_address: str
    target_port: int
    verified_by: str = "sshd:expose-auth-info"
    # This is the normalized SSH_CONNECTION target address. A non-standard
    # environment variable is not trusted to name the authenticated host.
    authenticated_host: str | None = None


@dataclass(frozen=True)
class EnrolledSshCredential:
    """Server-owned mapping from an SSH credential to a coordination subject."""

    credential_id: str
    subject_binding_id: str
    allowed_source_addresses: tuple[str, ...] = ()
    allowed_target_addresses: tuple[str, ...] = ()
    allowed_hosts: tuple[str, ...] = ()


@dataclass(frozen=True)
class AuthenticatedSshPrincipal:
    """Transport-authenticated identity safe to pass to assignment lookup."""

    subject_binding_id: str
    credential_id: str
    source_address: str
    verified_by: str
    authenticated_host: str | None = None


def read_sshd_peer_evidence(
    environ: Mapping[str, str],
    *,
    expected_uid: int | None = None,
) -> SshdPeerEvidence:
    """Read only the sshd-owned authentication file and connection tuple.

    Principal, assignment, role, session, workspace, and write scope fields in
    ``environ`` are intentionally ignored. The function is valid only inside a
    root-owned sshd ``ForceCommand`` boundary; callers cannot turn an ordinary
    process into that boundary by setting these variables themselves.
    """
    auth_path_text = str(environ.get("SSH_USER_AUTH") or "").strip()
    connection_text = str(environ.get("SSH_CONNECTION") or "").strip()
    if not auth_path_text or not connection_text:
        raise SshTransportAuthenticationError("sshd authentication evidence is missing")

    auth_path = Path(auth_path_text)
    if not auth_path.is_absolute():
        raise SshTransportAuthenticationError(
            "sshd authentication evidence path must be absolute"
        )
    try:
        auth_stat = auth_path.lstat()
    except OSError as exc:
        raise SshTransportAuthenticationError(
            "sshd authentication evidence is unreadable"
        ) from exc
    # expected_uid is retained as an explicit hermetic-test hook. Production
    # calls leave it unset and therefore require root ownership.
    owner = None if expected_uid is None else expected_uid
    safe_owner = (
        auth_stat.st_uid == 0
        if owner is None
        else auth_stat.st_uid in {0, owner}
    )
    if (
        not stat.S_ISREG(auth_stat.st_mode)
        or stat.S_ISLNK(auth_stat.st_mode)
        or not safe_owner
        or auth_stat.st_mode & 0o077
    ):
        raise SshTransportAuthenticationError(
            "sshd authentication evidence has unsafe ownership or permissions"
        )
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        auth_fd = os.open(auth_path, flags)
        try:
            opened_stat = os.fstat(auth_fd)
            safe_owner = (
                opened_stat.st_uid == 0
                if owner is None
                else opened_stat.st_uid in {0, owner}
            )
            if (
                not stat.S_ISREG(opened_stat.st_mode)
                or not safe_owner
                or opened_stat.st_mode & 0o077
            ):
                raise SshTransportAuthenticationError(
                    "sshd authentication evidence changed ownership or permissions"
                )
            auth_info = b""
            while len(auth_info) <= 65536:
                chunk = os.read(auth_fd, 65536 - len(auth_info) + 1)
                if not chunk:
                    break
                auth_info += chunk
        finally:
            os.close(auth_fd)
    except SshTransportAuthenticationError:
        raise
    except OSError as exc:
        raise SshTransportAuthenticationError(
            "sshd authentication evidence is unreadable"
        ) from exc
    if not auth_info or len(auth_info) > 65536:
        raise SshTransportAuthenticationError(
            "sshd authentication evidence has an invalid size"
        )
    public_key_lines = [
        line for line in auth_info.splitlines() if line.startswith(b"publickey ")
    ]
    if not public_key_lines:
        raise SshTransportAuthenticationError(
            "sshd authentication evidence contains no public key"
        )

    source_address, source_port, target_address, target_port = _parse_ssh_connection(
        connection_text
    )
    credential_digest = hashlib.sha256(b"\n".join(public_key_lines)).hexdigest()
    return SshdPeerEvidence(
        credential_id=f"ssh-auth:sha256:{credential_digest}",
        source_address=source_address,
        source_port=source_port,
        target_address=target_address,
        target_port=target_port,
        authenticated_host=_normalize_host(target_address),
    )


def bind_enrolled_ssh_principal(
    evidence: SshdPeerEvidence,
    enrolled: Sequence[EnrolledSshCredential],
) -> AuthenticatedSshPrincipal:
    """Resolve transport evidence through server-owned enrollment state."""
    matches = [
        item for item in enrolled if item.credential_id == evidence.credential_id
    ]
    if len(matches) != 1:
        raise SshTransportAuthenticationError("SSH credential is not uniquely enrolled")
    binding = matches[0]
    source = _normalize_address(evidence.source_address)
    target = _normalize_address(evidence.target_address)
    host = _normalize_host(evidence.authenticated_host or target)
    allowed_sources = {
        _normalize_address(item) for item in binding.allowed_source_addresses
    }
    allowed_targets = {
        _normalize_address(item) for item in binding.allowed_target_addresses
    }
    allowed_hosts = {_normalize_host(item) for item in binding.allowed_hosts}
    if allowed_sources and source not in allowed_sources:
        raise SshTransportAuthenticationError(
            "SSH credential is not enrolled for this source address"
        )
    if allowed_targets and target not in allowed_targets:
        raise SshTransportAuthenticationError(
            "SSH credential is not enrolled for this target address"
        )
    if allowed_hosts and host not in allowed_hosts:
        raise SshTransportAuthenticationError(
            "SSH credential is not enrolled for this authenticated host"
        )
    return AuthenticatedSshPrincipal(
        subject_binding_id=binding.subject_binding_id,
        credential_id=evidence.credential_id,
        source_address=source,
        verified_by=evidence.verified_by,
        authenticated_host=host,
    )


def restricted_ssh_stdio_mcp_spec(
    route: str,
    *,
    grant_id: str,
    wrapper_command: str = "okg",
) -> dict[str, object]:
    """Return the only agent-visible config for the selected SSH/stdio route.

    The local bridge resolves the opaque route and grant through its fixed
    client registry. No host, user, key, SSH option, DSN, database password,
    principal, role, assignment, or write scope is an accepted input to this
    config builder.
    """
    route = validate_coordination_route(route)
    grant_id = str(grant_id).strip()
    if not grant_id or any(char.isspace() for char in grant_id):
        raise ValueError("restricted SSH grant id must be one non-empty token")
    return {
        "command": wrapper_command,
        "args": ["agent-ssh-client"],
        "env": {
            "OKG_AGENT_SSH_ROUTE": route,
            "OKG_AGENT_SSH_GRANT": grant_id,
        },
    }


def _parse_ssh_connection(value: str) -> tuple[str, int, str, int]:
    fields = value.split()
    if len(fields) != 4:
        raise SshTransportAuthenticationError(
            "sshd connection evidence has an invalid shape"
        )
    source_address, source_port_text, target_address, target_port_text = fields
    try:
        source_address = str(ipaddress.ip_address(source_address))
        target_address = str(ipaddress.ip_address(target_address))
        source_port = int(source_port_text)
        target_port = int(target_port_text)
    except (ValueError, TypeError) as exc:
        raise SshTransportAuthenticationError(
            "sshd connection evidence is invalid"
        ) from exc
    if not 0 < source_port < 65536 or not 0 < target_port < 65536:
        raise SshTransportAuthenticationError(
            "sshd connection evidence contains an invalid port"
        )
    return source_address, source_port, target_address, target_port


def _normalize_address(value: str) -> str:
    try:
        return str(ipaddress.ip_address(str(value).strip()))
    except (ValueError, TypeError) as exc:
        raise SshTransportAuthenticationError("SSH peer address is invalid") from exc


def _normalize_host(value: str) -> str:
    host = str(value or "").strip().rstrip(".").lower()
    if not host:
        raise SshTransportAuthenticationError("SSH authenticated host is missing")
    try:
        return _normalize_address(host)
    except SshTransportAuthenticationError:
        try:
            return host.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise SshTransportAuthenticationError(
                "SSH authenticated host is invalid"
            ) from exc
