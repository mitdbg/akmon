"""harden-graph-store-exposure: the graph Postgres binds loopback
with a per-deployment password, and the listener-exposure check
catches a Docker-published DB port despite the foreign process
name."""
from __future__ import annotations

from pathlib import Path

import yaml

import importlib.util

from okg.substrate.cli.connector_security import (
    ListenerInfo,
    evaluate_listeners,
    parse_lsof_listeners,
)


def _install_module():
    """Load the academic-researcher profile's install.py by path — the
    profile dir is hyphenated, so it isn't a normal import."""
    import okg.substrate.library.profiles as profiles_pkg
    install_path = (
        Path(list(profiles_pkg.__path__)[0])
        / "academic-researcher" / "cli" / "install.py"
    )
    spec = importlib.util.spec_from_file_location(
        "academic_researcher_install_under_test", install_path,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# -- req.gse.bind ----------------------------------------------------


def test_password_resolves_per_deployment_and_threads_into_dsn() -> None:
    """A fresh deployment gets a high-entropy per-deployment
    password (not the shared `okg` literal), and _default_dsn
    embeds it on a loopback host. (Password generation +
    reuse-on-reinstall is `_resolve_pg_password`, shipped on dev as
    `harden-okg-network-and-secrets`; this PACT's net-new is the
    watched-port doctor check below.)"""
    install = _install_module()
    pw = install._resolve_pg_password("no_such_deployment_xyz")
    assert pw != "okg"
    assert len(pw) >= 16
    # Fresh installs get a random secret each time.
    assert install._resolve_pg_password("no_such_deployment_xyz2") != pw

    dsn = install._default_dsn("madden_pi", 5440, pw)
    assert f":{pw}@localhost:5440/okg_madden_pi" in dsn
    assert ":okg@" not in dsn
    assert "@localhost" in dsn, "host must be loopback"


def test_compose_binds_loopback_and_password_is_parameterized() -> None:
    """The shipped academic-researcher compose publishes 127.0.0.1 only and
    its POSTGRES_PASSWORD is env-parameterized (install supplies a
    per-deployment secret) — never a bare hardcoded literal. The
    loopback bind is the real off-host control; the env-driven
    password removes the shared world-identical credential."""
    compose_path = _install_module()._find_compose_yaml()
    raw = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    pg = raw["services"]["postgres"]

    ports = [str(p) for p in pg["ports"]]
    assert ports, "no ports published"
    for entry in ports:
        assert entry.startswith("127.0.0.1:"), (
            f"non-loopback publish: {entry!r}"
        )

    password = str(pg["environment"]["POSTGRES_PASSWORD"])
    # Env-parameterized (install passes the real per-deployment
    # value), not a bare `POSTGRES_PASSWORD: okg` literal.
    assert password.startswith("${POSTGRES_PASSWORD"), password
    assert password not in ("okg", "")


# -- req.gse.doctor --------------------------------------------------


_LSOF_DOCKER_PG = """\
COMMAND   PID  USER   FD   TYPE DEVICE SIZE/OFF NODE NAME
com.docke 999 riley   50u  IPv6 0x9999      0t0  TCP *:5433 (LISTEN)
chrome   7777 riley   11u  IPv4 0x0005      0t0  TCP 127.0.0.1:5353 (LISTEN)
"""

_LSOF_DOCKER_PG_LOOPBACK = """\
COMMAND   PID  USER   FD   TYPE DEVICE SIZE/OFF NODE NAME
com.docke 999 riley   50u  IPv4 0x9999      0t0  TCP 127.0.0.1:5433 (LISTEN)
"""


def test_watched_db_port_nonloopback_fails() -> None:
    """A docker-proxy 0.0.0.0 bind of a watched DB port is collected
    despite the foreign process name and fails with the port named —
    the exact blind spot that hid the graph store."""
    listeners = parse_lsof_listeners(
        _LSOF_DOCKER_PG, watched_db_ports={5433},
    )
    assert any(
        listener.port == 5433 and listener.watched_db_port for listener in listeners
    ), "docker-proxy listener on the watched port was not collected"
    report = evaluate_listeners(listeners)
    assert report["ok"] is False
    row = next(r for r in report["listeners"] if r["port"] == 5433)
    assert row["ok"] is False
    assert "graph store exposed" in row["detail"]


def test_watched_db_port_loopback_passes() -> None:
    """The same watched port bound to loopback passes; a watched port
    not listening at all is simply absent, not a failure."""
    listeners = parse_lsof_listeners(
        _LSOF_DOCKER_PG_LOOPBACK, watched_db_ports={5433},
    )
    report = evaluate_listeners(listeners)
    assert report["ok"] is True

    # Not listening => no row, no failure.
    empty = parse_lsof_listeners("", watched_db_ports={5433})
    assert empty == []
    assert evaluate_listeners(empty)["ok"] is True


def test_watched_db_port_not_excused_by_auth_gate() -> None:
    """A DB password is not a transport auth layer: a watched DB port
    on a non-loopback bind fails even if its port is auth-gated."""
    listener = ListenerInfo(
        name="com.docke", pid=1, host="0.0.0.0", port=5433,
        auth_gated=True, watched_db_port=True,
    )
    report = evaluate_listeners([listener])
    assert report["ok"] is False


def test_non_watched_substrate_listener_unaffected() -> None:
    """Regression: a normal substrate MCP listener on loopback still
    passes, and the watched-port machinery doesn't change it."""
    listeners = parse_lsof_listeners(
        "okg-mcp  42 riley 7u IPv4 0x1 0t0 TCP 127.0.0.1:8765 (LISTEN)\n",
    )
    report = evaluate_listeners(listeners)
    assert report["ok"] is True
    assert report["listeners"][0]["watched_db_port"] is False
