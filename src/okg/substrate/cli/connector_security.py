"""Connector-security probes for `okg doctor` —
`--check listener-exposure` and `--check token-redaction`.

Per `harden-connector-route-vetting`: the 2026-06 audit's
most-exploited field failure was a trusted-localhost control
plane reachable from outside (OpenClaw: 21-30k exposed
gateways). These probes make the two cheap invariants
mechanical:

  1. every substrate-owned TCP listener is loopback-bound or
     explicitly auth-gated — a non-loopback unauthenticated
     bind is a FAILING check, not a warning;
  2. no token material from the keyring store appears in the
     read-surface payloads that report credentialed-source
     state.
"""
from __future__ import annotations

import ipaddress
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Any, Iterable, Optional


# Process-name markers that identify substrate-owned listeners in
# the lsof scan. Deliberately broad: a marker miss means a silent
# pass, so we prefer false positives the operator can allowlist.
SUBSTRATE_PROCESS_MARKERS = ("okg", "supervisor_console", "dbos")

# Operator declaration that a non-loopback listener is auth-gated
# (bearer / OAuth in front). Comma-separated port list.
AUTH_GATED_PORTS_ENV = "OKG_AUTH_GATED_PORTS"

# Watched DB ports (per harden-graph-store-exposure): the graph
# Postgres is published by Docker's proxy process, whose name does
# NOT match the substrate markers above — so the check would miss
# the most sensitive listener (it holds decrypted mail/calendar
# data). A watched port is evaluated regardless of owning process.
# Comma-separated; the deployment's resolved DB port is added by the
# doctor handler.
WATCHED_DB_PORTS_ENV = "OKG_WATCHED_DB_PORTS"


@dataclass(frozen=True)
class ListenerInfo:
    """One observed TCP listener owned by a substrate process or a
    watched DB port."""

    name: str
    pid: int
    host: str
    port: int
    auth_gated: bool = False
    watched_db_port: bool = False


def _is_loopback(host: str) -> bool:
    if host in ("localhost", "*loopback*"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def evaluate_listeners(
    listeners: Iterable[ListenerInfo],
) -> dict[str, Any]:
    """Pure evaluation: loopback or auth-gated passes; a
    non-loopback unauthenticated bind fails (the exposed-gateway
    failure mode)."""
    rows: list[dict[str, Any]] = []
    failures = 0
    for listener in listeners:
        loopback = _is_loopback(listener.host)
        # A watched DB port (the graph store) must NEVER be
        # auth-gate-excused: a database password is not a transport
        # auth layer, and the data behind it is the decrypted graph.
        # Non-loopback => fail, unconditionally (evaluated before the
        # auth-gate excuse so the failure is actually counted).
        if not loopback and listener.watched_db_port:
            ok = False
        else:
            ok = loopback or listener.auth_gated
        if not ok:
            failures += 1
        rows.append({
            "name": listener.name,
            "pid": listener.pid,
            "host": listener.host,
            "port": listener.port,
            "loopback": loopback,
            "auth_gated": listener.auth_gated,
            "watched_db_port": listener.watched_db_port,
            "ok": ok,
            "detail": (
                "loopback-bound"
                if loopback
                else (
                    "WATCHED DB PORT ON NON-LOOPBACK BIND "
                    "(graph store exposed off-host)"
                    if listener.watched_db_port
                    else "auth-gated (operator-declared)"
                    if listener.auth_gated
                    else "NON-LOOPBACK UNAUTHENTICATED BIND"
                )
            ),
        })
    return {
        "ok": failures == 0,
        "listeners": rows,
        "failures": failures,
    }


def parse_lsof_listeners(
    output: str,
    *,
    auth_gated_ports: Optional[set[int]] = None,
    process_markers: tuple[str, ...] = SUBSTRATE_PROCESS_MARKERS,
    include_pids: Optional[set[int]] = None,
    watched_db_ports: Optional[set[int]] = None,
) -> list[ListenerInfo]:
    """Parse `lsof -nP -iTCP -sTCP:LISTEN` output into the
    substrate-owned listener set.

    A listener is included when its command name carries a marker,
    its pid is explicitly included (tests include their own pid),
    OR its port is a watched DB port (the graph store, published by
    Docker's proxy under a foreign process name — the blind spot
    this argument closes, per harden-graph-store-exposure)."""
    auth_gated_ports = auth_gated_ports or set()
    watched_db_ports = watched_db_ports or set()
    listeners: list[ListenerInfo] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 9 or parts[-1] != "(LISTEN)":
            continue
        name, pid_raw = parts[0], parts[1]
        try:
            pid = int(pid_raw)
        except ValueError:
            continue
        addr = parts[-2]  # e.g. 127.0.0.1:8765, *:8080, [::1]:9000
        host, _, port_raw = addr.rpartition(":")
        try:
            port = int(port_raw)
        except ValueError:
            continue
        owned = any(m in name.lower() for m in process_markers) or (
            include_pids is not None and pid in include_pids
        ) or port in watched_db_ports
        if not owned:
            continue
        host = host.strip("[]")
        if host == "*":
            host = "0.0.0.0"
        listeners.append(ListenerInfo(
            name=name,
            pid=pid,
            host=host,
            port=port,
            auth_gated=port in auth_gated_ports,
            watched_db_port=port in watched_db_ports,
        ))
    return listeners


_SS_PROCESS = re.compile(r'\("(?P<name>[^"]+)",pid=(?P<pid>\d+)')


def parse_ss_listeners(
    output: str,
    *,
    auth_gated_ports: Optional[set[int]] = None,
    process_markers: tuple[str, ...] = SUBSTRATE_PROCESS_MARKERS,
    include_pids: Optional[set[int]] = None,
    watched_db_ports: Optional[set[int]] = None,
) -> list[ListenerInfo]:
    """Parse `ss -ltnp` output into the substrate-owned listener set.

    Same selection rules as :func:`parse_lsof_listeners`. This exists because
    the current Ubuntu 24.04 cloud image no longer ships lsof, while iproute2
    is always present; without a fallback the listener-exposure check would
    fail closed on a host that is actually fine.

    A watched DB port with no readable process column (ss omits it without
    privilege) is still reported, with an empty name and pid 0. Dropping it
    would turn the most sensitive listener into a silent pass.
    """

    auth_gated_ports = auth_gated_ports or set()
    watched_db_ports = watched_db_ports or set()
    listeners: list[ListenerInfo] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 4 or parts[0] != "LISTEN":
            continue
        addr = parts[3]
        host, _, port_raw = addr.rpartition(":")
        try:
            port = int(port_raw)
        except ValueError:
            continue
        host = host.strip("[]")
        if host in ("*", "::"):
            host = "0.0.0.0"
        found = _SS_PROCESS.findall(line)
        if not found and port in watched_db_ports:
            found = [("", "0")]
        # ss puts every process sharing a socket on ONE line. Emitting a row
        # each would report one exposed port as N failures, so the first
        # owner represents the listener.
        found = found[:1]
        for name, pid_raw in found:
            try:
                pid = int(pid_raw)
            except ValueError:
                continue
            owned = (
                any(m in name.lower() for m in process_markers)
                or (include_pids is not None and pid in include_pids)
                or port in watched_db_ports
            )
            if not owned:
                continue
            listeners.append(
                ListenerInfo(
                    name=name,
                    pid=pid,
                    host=host,
                    port=port,
                    auth_gated=port in auth_gated_ports,
                    watched_db_port=port in watched_db_ports,
                )
            )
    return listeners


def collect_substrate_listeners(
    *,
    include_pids: Optional[set[int]] = None,
    watched_db_ports: Optional[set[int]] = None,
) -> list[ListenerInfo]:
    """Scan live TCP listeners; filter to substrate-owned processes + watched
    DB ports. Auth-gated ports come from OKG_AUTH_GATED_PORTS; watched DB ports
    merge the caller's set with OKG_WATCHED_DB_PORTS.

    Uses lsof, falling back to `ss -ltnp` when lsof is absent (the current
    Ubuntu 24.04 image ships no lsof). A scan that cannot be trusted RAISES:
    returning an empty list would make evaluate_listeners report ok, which is
    exactly the false pass this probe exists to prevent."""
    return scan_substrate_listeners(
        include_pids=include_pids,
        watched_db_ports=watched_db_ports,
    )[0]


def scan_substrate_listeners(
    *,
    include_pids: Optional[set[int]] = None,
    watched_db_ports: Optional[set[int]] = None,
) -> tuple[list[ListenerInfo], str]:
    """Same scan as :func:`collect_substrate_listeners`, also reporting which
    scanner produced the answer so the operator can tell an lsof host from a
    fallback host."""

    auth_gated = _ports_from_env(AUTH_GATED_PORTS_ENV)
    watched = set(watched_db_ports or set()) | _ports_from_env(
        WATCHED_DB_PORTS_ENV,
    )
    try:
        proc = subprocess.run(
            ["lsof", "-nP", "-iTCP", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except FileNotFoundError:
        return (
            _collect_via_ss(
                auth_gated=auth_gated,
                include_pids=include_pids,
                watched=watched,
            ),
            "ss",
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"lsof scan failed: {exc}") from exc
    if proc.returncode != 0 and proc.stderr.strip():
        raise RuntimeError(
            f"lsof scan failed: exit {proc.returncode}: {proc.stderr.strip()}"
        )
    return (
        parse_lsof_listeners(
            proc.stdout,
            auth_gated_ports=auth_gated,
            include_pids=include_pids,
            watched_db_ports=watched,
        ),
        "lsof",
    )


def _collect_via_ss(
    *,
    auth_gated: set[int],
    include_pids: Optional[set[int]],
    watched: set[int],
) -> list[ListenerInfo]:
    try:
        proc = subprocess.run(
            ["ss", "-ltnp"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(
            f"listener scan failed: lsof is absent and ss failed: {exc}"
        ) from exc
    # ss reports an empty table with exit 0, so any non-zero exit is an error.
    if proc.returncode != 0:
        raise RuntimeError(
            "listener scan failed: lsof is absent and ss exited "
            f"{proc.returncode}: {proc.stderr.strip()}"
        )
    return parse_ss_listeners(
        proc.stdout,
        auth_gated_ports=auth_gated,
        include_pids=include_pids,
        watched_db_ports=watched,
    )


def _ports_from_env(var: str) -> set[int]:
    return {
        int(p)
        for p in os.environ.get(var, "").split(",")
        if p.strip().isdigit()
    }


# -- token redaction ---------------------------------------------------


def token_windows(token: str, *, min_len: int = 8) -> list[str]:
    """The substrings of `token` a leak check must look for: the
    whole token when short, else every window of `min_len`."""
    if len(token) <= min_len:
        return [token] if token else []
    return [
        token[i: i + min_len]
        for i in range(0, len(token) - min_len + 1)
    ]


def find_token_leaks(
    payloads: Iterable[tuple[str, str]],
    tokens: Iterable[str],
    *,
    min_len: int = 8,
) -> list[dict[str, Any]]:
    """Cross-check serialized read-surface payloads against token
    material. `payloads` is (label, serialized_text). Any window of
    a token >= min_len appearing in a payload is a leak."""
    leaks: list[dict[str, Any]] = []
    for token in tokens:
        windows = token_windows(token, min_len=min_len)
        for label, text in payloads:
            for window in windows:
                if window in text:
                    leaks.append({
                        "payload": label,
                        # Report a fingerprint, never the leaked
                        # material itself.
                        "token_window_sha8": _sha8(window),
                        "window_length": len(window),
                    })
                    break
    return leaks


def _sha8(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
