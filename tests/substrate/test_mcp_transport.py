"""Transport selection for the substrate MCP server entry points.

Isolated from any database: build_server is stubbed so these tests assert only
the transport plumbing in run_server/run_stdio -- which transport FastMCP.run
is asked for, and that host/port are set on mcp.settings for http.
"""
from __future__ import annotations

import logging
import signal

import pytest

import okg.substrate.mcp.server as server_mod


class _FakeSettings:
    def __init__(self) -> None:
        self.host = None
        self.port = None
        self.transport_security = None


class _FakeMCP:
    def __init__(self) -> None:
        self.settings = _FakeSettings()
        self.run_calls: list[dict] = []

    def run(self, *args, **kwargs) -> None:
        # FastMCP.run() takes transport as the first positional/keyword arg;
        # record how it was invoked. No transport arg == stdio default.
        self.run_calls.append({"args": args, "kwargs": kwargs})


@pytest.fixture
def stop_guards(monkeypatch):
    """Record every stop guard run_server arms, and disarm them afterwards.

    A triggered guard stays armed until the process exits (the SIGTERM it saw
    was SIG_DFL), so an in-process test that fakes SIGTERM must stop it, or
    it would end the pytest process MCP_STOP_GUARD_SECONDS later.
    """
    created: list = []
    real = server_mod._StopGuard

    class _RecordedStopGuard(real):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(server_mod, "_StopGuard", _RecordedStopGuard)
    try:
        yield created
    finally:
        for guard in created:
            guard.disarm()


def _patch_build_server(monkeypatch) -> _FakeMCP:
    fake = _FakeMCP()
    monkeypatch.setattr(server_mod, "build_server", lambda **_kw: fake)
    return fake


def test_run_stdio_uses_stdio_default(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    server_mod.run_stdio(deployment_name="cms")
    assert len(fake.run_calls) == 1
    call = fake.run_calls[0]
    # stdio path calls mcp.run() with no transport argument (unchanged behavior).
    assert call["args"] == ()
    assert "transport" not in call["kwargs"]
    # stdio never binds a socket, so host/port stay untouched.
    assert fake.settings.host is None
    assert fake.settings.port is None


def test_run_server_stdio_matches_stdio(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    server_mod.run_server(transport="stdio", host="0.0.0.0", port=9999)
    assert len(fake.run_calls) == 1
    call = fake.run_calls[0]
    assert call["args"] == ()
    assert "transport" not in call["kwargs"]
    # Even when host/port are passed, stdio ignores them.
    assert fake.settings.host is None
    assert fake.settings.port is None


def test_run_server_streamable_http_binds_and_selects_transport(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    server_mod.run_server(
        transport="streamable-http",
        host="0.0.0.0",
        port=8123,
        deployment_name="cms",
    )
    assert len(fake.run_calls) == 1
    call = fake.run_calls[0]
    transport = call["kwargs"].get("transport")
    if transport is None and call["args"]:
        transport = call["args"][0]
    assert transport == "streamable-http"
    # http must bind the requested address before handing off to run().
    assert fake.settings.host == "0.0.0.0"
    assert fake.settings.port == 8123


def test_run_server_configures_network_host_allowlist(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    server_mod.run_server(
        transport="streamable-http",
        host="0.0.0.0",
        port=8123,
        allowed_hosts=("okg-mcp:*", "okg-mcp.namespace.svc:*"),
    )
    security = fake.settings.transport_security
    assert security.enable_dns_rebinding_protection is True
    assert security.allowed_hosts == [
        "okg-mcp:*",
        "okg-mcp.namespace.svc:*",
    ]


def test_run_server_rejects_unsafe_network_host_allowlist(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    with pytest.raises(ValueError, match="invalid MCP allowed Host"):
        server_mod.run_server(
            transport="streamable-http",
            host="0.0.0.0",
            allowed_hosts=("https://example.com",),
        )
    assert fake.run_calls == []


# ── write-surface exposure guard ──────────────────────────────────────


def test_write_transport_guard_matrix(monkeypatch) -> None:
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    g = server_mod._write_transport_guard
    # stdio is always safe (local subprocess), even with writes on.
    assert g(allow_writes=True, transport="stdio", host="0.0.0.0") is None
    # read-only over http is fine on any host (guard only covers writes).
    assert g(allow_writes=False, transport="streamable-http", host="0.0.0.0") is None
    # writes over http on a public bind -> refuse.
    assert g(allow_writes=True, transport="streamable-http", host="0.0.0.0")[0] == "refuse"
    # writes over http on loopback -> allowed but warned (v4, whole 127/8, v6, name).
    assert g(allow_writes=True, transport="streamable-http", host="127.0.0.1")[0] == "warn"
    assert g(allow_writes=True, transport="streamable-http", host="127.5.5.5")[0] == "warn"
    assert g(allow_writes=True, transport="streamable-http", host="::1")[0] == "warn"
    assert g(allow_writes=True, transport="streamable-http", host="localhost")[0] == "warn"
    # a hostname that merely starts with "127." is NOT loopback -> refuse.
    assert g(allow_writes=True, transport="streamable-http", host="127.evil.example.com")[0] == "refuse"
    # all-interfaces spellings are non-loopback -> refuse.
    assert g(allow_writes=True, transport="streamable-http", host="::")[0] == "refuse"


def test_run_server_refuses_writes_over_public_http(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    with pytest.raises(ValueError, match="non-loopback"):
        server_mod.run_server(
            transport="streamable-http", host="0.0.0.0", allow_writes=True
        )
    # refused before building/serving anything.
    assert fake.run_calls == []


def test_run_server_refuses_writes_enabled_via_env(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    monkeypatch.setenv("OKG_MCP_ALLOW_WRITES", "1")
    # allow_writes arg is False, but the env var enables writes -> still refused.
    with pytest.raises(ValueError, match="non-loopback"):
        server_mod.run_server(
            transport="streamable-http", host="0.0.0.0", allow_writes=False
        )
    assert fake.run_calls == []


def test_run_server_warns_but_serves_writes_on_loopback(monkeypatch, caplog) -> None:
    fake = _patch_build_server(monkeypatch)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    with caplog.at_level(logging.WARNING, logger=server_mod.log.name):
        server_mod.run_server(
            transport="streamable-http",
            host="127.0.0.1",
            port=8123,
            allow_writes=True,
        )
    # loopback: not refused -- it serves, but logs a caution.
    assert len(fake.run_calls) == 1
    assert any("write-enabled" in r.message for r in caplog.records)


def test_run_server_readonly_public_http_is_allowed(monkeypatch) -> None:
    fake = _patch_build_server(monkeypatch)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    # read-only over http on 0.0.0.0 is a supported deployment (e.g. sidecar).
    server_mod.run_server(
        transport="streamable-http", host="0.0.0.0", port=8123, allow_writes=False
    )
    assert len(fake.run_calls) == 1


# ── auth-aware exposure guard (PACT mcp-http-auth) ────────────────────
#
# The decision space is 2x2x2: (loopback | non-loopback) x (writes on | off) x
# (auth active | inactive). `_write_transport_guard` owns the four WRITE cells;
# `_readonly_transport_warning` owns the read-only cells. Both are asserted
# together below so no cell is left unspecified.


_TOKEN_ENV = "OKG_TEST_MCP_AUTH_TOKEN"
_TOKEN = "c1a7f0e93b2d4856a7c9e0f1b2d3a4c5e6f70819a2b3c4d5e6f708192a3b4c5d"


class _AuthAwareFakeMCP(_FakeMCP):
    """Fake whose `_token_verifier` mirrors what build_server would install.

    run_server derives auth-active from the CONSTRUCTED server, so a fake that
    always looked unauthenticated would make every auth cell unreachable, and
    one that always looked authenticated would make the refusal cells vacuous.
    That the REAL build path installs the verifier (and wraps the /mcp route in
    the SDK's auth middleware) is proven separately by
    tests/substrate/test_mcp_http_auth.py::test_served_app_wraps_mcp_route_in_auth_middleware.
    """

    def __init__(self) -> None:
        super().__init__()
        self._token_verifier = None


def _patch_build_server_auth_aware(monkeypatch) -> _AuthAwareFakeMCP:
    fake = _AuthAwareFakeMCP()

    def _build(**kwargs):
        fake._token_verifier = object() if kwargs.get("auth_token") else None
        return fake

    monkeypatch.setattr(server_mod, "build_server", _build)
    return fake


def _guard_cell(*, loopback: bool, writes: bool, auth: bool):
    """Return (write-guard level or None, read-warning present) for one cell."""
    host = "127.0.0.1" if loopback else "0.0.0.0"
    guard = server_mod._write_transport_guard(
        allow_writes=writes,
        transport="streamable-http",
        host=host,
        auth_active=auth,
    )
    warning = server_mod._readonly_transport_warning(
        allow_writes=writes,
        transport="streamable-http",
        host=host,
        auth_active=auth,
    )
    return (None if guard is None else guard[0], warning is not None)


def test_write_transport_guard_eight_cell_matrix(monkeypatch) -> None:
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    # (loopback, writes, auth) -> (write-guard level, read-only warning emitted)
    expected = {
        # Writes on. Auth replaces the topology proxy: the non-loopback bind
        # opens, and the loopback warning goes away because its own text
        # asserts the endpoint is unauthenticated, which is then false.
        (True, True, True): (None, False),
        (True, True, False): ("warn", False),
        (False, True, True): (None, False),
        (False, True, False): ("refuse", False),
        # Read-only. Allowed in every cell; the routable unauthenticated one
        # warns, because anyone who can reach the port can read the graph.
        (True, False, True): (None, False),
        (True, False, False): (None, False),
        (False, False, True): (None, False),
        (False, False, False): (None, True),
    }
    actual = {
        cell: _guard_cell(loopback=cell[0], writes=cell[1], auth=cell[2])
        for cell in expected
    }
    assert actual == expected

    # The loopback+writes+auth cell must not emit the now-false warning.
    served = server_mod._write_transport_guard(
        allow_writes=True,
        transport="streamable-http",
        host="127.0.0.1",
        auth_active=True,
    )
    assert served is None
    # ...and the no-auth loopback warning still says what it always said.
    warned = server_mod._write_transport_guard(
        allow_writes=True, transport="streamable-http", host="127.0.0.1"
    )
    assert warned is not None and "unauthenticated" in warned[1]

    # The non-loopback read-only warning names the exposure it describes.
    read_warning = server_mod._readonly_transport_warning(
        allow_writes=False, transport="streamable-http", host="0.0.0.0"
    )
    assert read_warning is not None
    assert "read-only" in read_warning and "no authentication" in read_warning

    # stdio short-circuits both, in every cell.
    for writes in (True, False):
        for auth in (True, False):
            assert (
                server_mod._write_transport_guard(
                    allow_writes=writes,
                    transport="stdio",
                    host="0.0.0.0",
                    auth_active=auth,
                )
                is None
            )
            assert (
                server_mod._readonly_transport_warning(
                    allow_writes=writes,
                    transport="stdio",
                    host="0.0.0.0",
                    auth_active=auth,
                )
                is None
            )


def test_write_transport_guard_permits_non_loopback_writes_only_with_auth(
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    g = server_mod._write_transport_guard
    for host in ("0.0.0.0", "::", "10.1.2.3", "127.evil.example.com"):
        assert g(
            allow_writes=True,
            transport="streamable-http",
            host=host,
            auth_active=True,
        ) is None, host
        assert g(
            allow_writes=True,
            transport="streamable-http",
            host=host,
            auth_active=False,
        )[0] == "refuse", host


def test_write_transport_guard_refuses_non_loopback_writes_without_auth(
    monkeypatch,
) -> None:
    """The no-auth refusal text is the one shipped today, byte for byte."""
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    level, message = server_mod._write_transport_guard(
        allow_writes=True, transport="streamable-http", host="0.0.0.0"
    )
    assert level == "refuse"
    assert message == (
        "refusing to serve the write-enabled MCP surface over streamable-http "
        "bound to non-loopback host '0.0.0.0': the MCP endpoint is "
        "unauthenticated, so this exposes graph mutation to anyone who can "
        "reach the port. Use stdio, bind --host 127.0.0.1, or drop "
        "--allow-writes."
    )


def test_run_server_permits_non_loopback_writes_with_auth(monkeypatch) -> None:
    fake = _patch_build_server_auth_aware(monkeypatch)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    server_mod.run_server(
        transport="streamable-http",
        host="0.0.0.0",
        port=8124,
        allow_writes=True,
        auth_token_env=_TOKEN_ENV,
    )
    assert len(fake.run_calls) == 1
    assert fake._token_verifier is not None


def test_run_server_refuses_when_credential_resolves_but_is_not_wired(
    monkeypatch,
) -> None:
    """A resolved-but-unwired credential must not unlock the routable surface.

    This is the failure the "derive auth-active from the CONSTRUCTED server"
    rule exists to catch: the flag parsed, the variable resolved, and the
    server still has no verifier installed.
    """
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    fake = _patch_build_server(monkeypatch)  # never installs a verifier
    with pytest.raises(ValueError, match="no token verifier"):
        server_mod.run_server(
            transport="streamable-http",
            host="0.0.0.0",
            port=8124,
            allow_writes=True,
            auth_token_env=_TOKEN_ENV,
        )
    assert fake.run_calls == []


def test_auth_flag_with_stdio_transport_is_refused(monkeypatch) -> None:
    """Never silently ignored: stdio carries no Authorization header."""
    from okg.substrate.errors import SubstrateError

    fake = _patch_build_server_auth_aware(monkeypatch)
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    with pytest.raises(SubstrateError) as exc:
        server_mod.run_server(transport="stdio", auth_token_env=_TOKEN_ENV)
    assert "incompatible with --transport stdio" in str(exc.value)
    assert fake.run_calls == []


def test_run_server_fails_closed_when_auth_token_env_is_unset(monkeypatch) -> None:
    """A configured-but-unresolvable credential refuses to start.

    Never "start unauthenticated with the write surface bound to a routable
    address" -- the absence of a token is a distinct fail-closed state, not a
    value that compares equal to "no auth requested".
    """
    from okg.substrate.errors import SubstrateError

    fake = _patch_build_server_auth_aware(monkeypatch)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    for value in (None, "", "   "):
        if value is None:
            monkeypatch.delenv(_TOKEN_ENV, raising=False)
        else:
            monkeypatch.setenv(_TOKEN_ENV, value)
        with pytest.raises(SubstrateError) as exc:
            server_mod.run_server(
                transport="streamable-http",
                host="0.0.0.0",
                port=8124,
                allow_writes=True,
                auth_token_env=_TOKEN_ENV,
            )
        assert _TOKEN_ENV in str(exc.value)
        assert "refusing to start unauthenticated" in str(exc.value)
        # No socket, no pool, no server: refused before anything was built.
        assert fake.run_calls == []


def test_cli_and_run_server_agree_on_guard_decision(monkeypatch) -> None:
    """The CLI pre-check and the programmatic path decide every cell alike.

    `_write_transport_guard` has two callers: `_cmd_mcp_serve`, which refuses
    before any DB work, and `run_server`. A disagreement would mean the CLI
    refuses a bind run_server permits (or worse, the reverse).
    """
    import argparse

    from okg.substrate.cli import main as cli_main
    from okg.substrate.errors import SubstrateError

    class _FakeDsn:
        dsn = "postgresql://postgres:postgres@127.0.0.1:1/nodb"
        authority = {}
        target = "test"

    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_read_dsn",
        lambda *a, **kw: _FakeDsn(),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.enforce_release_contract_if_declared",
        lambda **kw: None,
    )
    # The CLI arm now also answers "is this database ready" before it serves
    # (issue #2128). This test compares the two arms on the WRITE TRANSPORT
    # guard only, and `_FakeDsn` names no reachable database, so the
    # readiness precondition is stubbed to its success answer like the other
    # seams above; leaving it live would make the CLI arm refuse in every
    # cell for an unrelated reason. The stub answers with a readiness
    # object, not None: the caller now reports what it is handed.
    from okg.substrate.db.readiness import DatabaseReadiness

    monkeypatch.setattr(
        "okg.substrate.db.readiness.require_database_ready",
        lambda dsn, **kwargs: DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )

    # Capture the real entry point BEFORE the CLI arm stubs the module
    # attribute out; otherwise the programmatic arm would call the stub and
    # agree with the CLI vacuously.
    real_run_server = server_mod.run_server

    def _cli_refuses(*, host, writes, auth) -> bool:
        # _cmd_mcp_serve imports run_server from the server module at call
        # time, so the module attribute is the seam.
        monkeypatch.setattr(server_mod, "run_server", lambda **kw: None)
        args = argparse.Namespace(
            deployment="default", dsn=None, embedder=None, branch="default",
            allow_writes=writes, transport="streamable-http", host=host,
            port=8124, allowed_host=[],
            auth_token_env=(_TOKEN_ENV if auth else None), auth_token=None,
        )
        try:
            return cli_main._cmd_mcp_serve(args, as_json=True) != 0
        finally:
            monkeypatch.setattr(server_mod, "run_server", real_run_server)

    def _run_server_refuses(*, host, writes, auth) -> bool:
        _patch_build_server_auth_aware(monkeypatch)
        try:
            real_run_server(
                transport="streamable-http", host=host, port=8124,
                allow_writes=writes,
                auth_token_env=(_TOKEN_ENV if auth else None),
            )
        except (ValueError, SubstrateError):
            return True
        return False

    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    disagreements = []
    for host in ("127.0.0.1", "0.0.0.0"):
        for writes in (True, False):
            for auth in (True, False):
                cli = _cli_refuses(host=host, writes=writes, auth=auth)
                prog = _run_server_refuses(host=host, writes=writes, auth=auth)
                if cli != prog:
                    disagreements.append((host, writes, auth, cli, prog))
    assert not disagreements, disagreements

    # And the agreement is not the trivial "neither ever refuses": exactly the
    # non-loopback + writes + no-auth cell refuses on both paths.
    assert _cli_refuses(host="0.0.0.0", writes=True, auth=False) is True
    assert _run_server_refuses(host="0.0.0.0", writes=True, auth=False) is True
    assert _cli_refuses(host="0.0.0.0", writes=True, auth=True) is False


def test_run_server_warns_on_unauthenticated_routable_read_surface(
    monkeypatch, caplog
) -> None:
    """The read-only cell's warning must reach the operator, not just exist.

    A guard function that returns the right string is not the same as a server
    that logs it; this covers the wiring in run_server.
    """
    fake = _patch_build_server_auth_aware(monkeypatch)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    with caplog.at_level(logging.WARNING, logger=server_mod.log.name):
        server_mod.run_server(
            transport="streamable-http", host="0.0.0.0", port=8123,
            allow_writes=False,
        )
    assert len(fake.run_calls) == 1
    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "read-only MCP surface" in m and "no authentication" in m
        for m in messages
    ), messages

    # With auth active the same bind serves without that warning.
    caplog.clear()
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    with caplog.at_level(logging.WARNING, logger=server_mod.log.name):
        server_mod.run_server(
            transport="streamable-http", host="0.0.0.0", port=8123,
            allow_writes=False, auth_token_env=_TOKEN_ENV,
        )
    assert not any(
        "read-only MCP surface" in r.getMessage() for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_operator_facing_auth_strings_name_the_auth_remedy(
    monkeypatch, capsys
) -> None:
    """The three shipped strings that described the auth posture were updated.

    Each previously asserted the endpoint is unauthenticated, unconditionally.
    None may still say that without qualification now that a verifier can be
    installed and make the claim false.
    """
    import argparse

    import okg.substrate.cli.main as cli_main

    # (1) --host help: still warns for the no-auth case, but names the flag
    # that changes the answer rather than asserting unauthenticatedness flatly.
    with pytest.raises(SystemExit):
        cli_main._build_parser().parse_args(["mcp-serve", "--help"])
    host_help = capsys.readouterr().out
    assert "Without --auth-token-env the MCP endpoint is unauthenticated" in (
        " ".join(host_help.split())
    )
    assert "With --auth-token-env every request must present" in (
        " ".join(host_help.split())
    )

    # (2) the CLI fix_hint on the refusal path names the auth remedy.
    class _FakeDsn:
        dsn = "postgresql://postgres:postgres@127.0.0.1:1/nodb"
        authority = {}
        target = "test"

    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_read_dsn",
        lambda *a, **kw: _FakeDsn(),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.enforce_release_contract_if_declared",
        lambda **kw: None,
    )
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    captured: dict = {}

    def _capture_error(err, *, as_json):
        captured.setdefault("fix_hint", getattr(err, "fix_hint", "") or "")
        return 1

    monkeypatch.setattr(cli_main, "_emit_error", _capture_error)
    args = argparse.Namespace(
        deployment="default", dsn=None, embedder=None, branch="default",
        allow_writes=True, transport="streamable-http", host="0.0.0.0",
        port=8124, allowed_host=[], auth_token_env=None, auth_token=None,
    )
    cli_main._cmd_mcp_serve(args, as_json=True)
    assert "--auth-token-env" in captured.get("fix_hint", "")

    # (3) the loopback write warning is suppressed once a verifier is installed.
    assert server_mod._write_transport_guard(
        allow_writes=True, transport="streamable-http", host="127.0.0.1",
        auth_active=True,
    ) is None


def test_cli_refuses_auth_flag_with_stdio(monkeypatch) -> None:
    import argparse

    from okg.substrate.cli import main as cli_main

    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    args = argparse.Namespace(
        deployment="default", dsn=None, embedder=None, branch="default",
        allow_writes=False, transport="stdio", host="127.0.0.1", port=8100,
        allowed_host=[], auth_token_env=_TOKEN_ENV, auth_token=None,
    )
    assert cli_main._cmd_mcp_serve(args, as_json=True) != 0


# ── unresolvable deployment is a typed refusal, not a traceback ───────
#
# PACT `mcp-serve-typed-deployment-error`. `--deployment` defaults to the
# literal string `default`, so `okg mcp-serve --dsn <dsn>` still asks for a
# deployment BY NAME. The release-contract check resolves that manifest before
# it can tell whether a contract was declared at all, and the
# `DeploymentResolutionError` it raises is a plain `Exception` -- so it used to
# walk through `except SubstrateError` and out of `main()` as a traceback with
# no code and no hint about the flag that was actually omitted.
#
# Both tests below call the real `enforce_release_contract_if_declared` (only
# the DSN authority is stubbed) and let the real `_emit_error` render, so they
# fail on the pre-change code by RAISING out of the handler rather than by
# comparing strings against a path that never ran.


def _unresolvable_deployment_args(monkeypatch, tmp_path):
    """Namespace + env for an `mcp-serve --dsn ...` whose deployment is absent."""
    import argparse

    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)

    class _FakeDsn:
        dsn = "postgresql://nobody@127.0.0.1:1/none"
        authority = {}
        target = "test"

    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_read_dsn",
        lambda *a, **kw: _FakeDsn(),
    )
    return argparse.Namespace(
        deployment="default", dsn=_FakeDsn.dsn, embedder=None,
        branch="default", allow_writes=False, transport="stdio",
        host="127.0.0.1", port=8100, allowed_host=[],
        auth_token_env=None, auth_token=None,
    ), deployments_root


def test_mcp_serve_unresolvable_deployment_refuses_with_envelope(
    monkeypatch, tmp_path, capsys,
) -> None:
    """`--json`: a coded envelope naming the deployment and both flags."""
    import json

    from okg.substrate.cli import main as cli_main

    args, deployments_root = _unresolvable_deployment_args(monkeypatch, tmp_path)

    rc = cli_main._cmd_mcp_serve(args, as_json=True)
    assert rc != 0, "an unresolvable deployment must not exit 0"

    captured = capsys.readouterr()
    assert "Traceback" not in captured.err, captured.err
    # stdio transport hands stdout to the MCP protocol stream; a refusal
    # printed there would corrupt it. The envelope belongs on stderr.
    assert captured.out == "", captured.out
    envelope = json.loads(captured.err.strip())["error"]

    # (1) a code -- not the bare `substrate_error` catch-all.
    assert envelope["code"] == "deployment_not_found", envelope

    # (2) the deployment name that failed, and the searched manifest paths the
    # resolution failure already carried, both survive the translation.
    assert "default" in envelope["message"], envelope
    assert envelope["data"]["deployment"] == "default", envelope
    resolution_error = envelope["data"]["resolution_error"]
    assert str(deployments_root / "default" / "deployment.yaml") in (
        resolution_error
    ), envelope
    assert resolution_error in envelope["message"], (
        "the searched paths must reach the operator in the rendered message, "
        "not only in the structured data block"
    )

    # (3) the fix hint names BOTH flags: the operator who hit this supplied
    # --dsn and cannot otherwise learn that --deployment is the missing input.
    assert "--deployment" in envelope["fix_hint"], envelope
    assert "--dsn" in envelope["fix_hint"], envelope


def test_mcp_serve_deployment_resolution_refusal_renders_in_text_mode(
    monkeypatch, tmp_path, capsys,
) -> None:
    """Text mode: `error [<code>]: ...` plus the fix hint, and no traceback."""
    from okg.substrate.cli import main as cli_main

    args, _ = _unresolvable_deployment_args(monkeypatch, tmp_path)

    rc = cli_main._cmd_mcp_serve(args, as_json=False)
    assert rc != 0

    captured = capsys.readouterr()
    assert captured.out == "", captured.out
    err = captured.err
    assert "Traceback" not in err, err
    assert "DeploymentResolutionError" not in err, err
    assert "error [deployment_not_found]:" in err, err
    assert "'default'" in err, err
    assert "fix_hint:" in err, err
    fix_hint_line = next(
        line for line in err.splitlines() if "fix_hint:" in line
    )
    assert "--deployment" in fix_hint_line, err
    assert "--dsn" in fix_hint_line, err


# ── capacity-reservation release on every exit path (from dev) ────────


def test_build_server_setup_failure_releases_reservation(monkeypatch) -> None:
    from okg.substrate.db import capacity

    released: list[str] = []
    monkeypatch.setattr(
        capacity,
        "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )
    claimed: list[bool] = []

    def fail_after_claim(**kwargs):
        # The build asked for its claim, then failed opening a pool.
        if claimed:
            kwargs["claim_attempted"].append(True)
        raise RuntimeError("setup failed")

    monkeypatch.setattr(server_mod, "_build_server", fail_after_claim)

    # A build that failed before it asked for a claim releases nothing: the
    # claim may be another server's of this deployment in this process.
    with pytest.raises(RuntimeError, match="setup failed"):
        server_mod.build_server(deployment_name="cms")
    assert released == []

    claimed.append(True)
    with pytest.raises(RuntimeError, match="setup failed"):
        server_mod.build_server(deployment_name="cms")
    assert released == ["okg-mcp-cms"]


def test_run_server_exception_releases_reservation(monkeypatch) -> None:
    from okg.substrate.db import capacity

    fake = _patch_build_server(monkeypatch)
    released: list[str] = []
    monkeypatch.setattr(
        capacity,
        "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )

    def fail_run(*_args, **_kwargs) -> None:
        raise RuntimeError("run failed")

    fake.run = fail_run
    with pytest.raises(RuntimeError, match="run failed"):
        server_mod.run_server(deployment_name="cms")

    assert released == ["okg-mcp-cms"]


def test_default_sigterm_unwinds_and_releases_reservation(
    monkeypatch, stop_guards
) -> None:
    from okg.substrate.db import capacity

    fake = _patch_build_server(monkeypatch)
    released: list[str] = []
    handlers = {signal.SIGTERM: signal.SIG_DFL}

    def fake_getsignal(signum):
        return handlers[signum]

    def fake_signal(signum, handler):
        previous = handlers[signum]
        handlers[signum] = handler
        return previous

    def raise_sigterm(*_args, **_kwargs) -> None:
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    fake.run = raise_sigterm
    monkeypatch.setattr(server_mod.signal, "getsignal", fake_getsignal)
    monkeypatch.setattr(server_mod.signal, "signal", fake_signal)
    monkeypatch.setattr(
        capacity,
        "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )

    with pytest.raises(SystemExit) as exc:
        server_mod.run_server(deployment_name="cms")

    assert exc.value.code == 128 + signal.SIGTERM
    assert released == ["okg-mcp-cms"]
    assert handlers[signal.SIGTERM] is signal.SIG_DFL


def test_default_sigterm_during_build_releases_reservation(
    monkeypatch, stop_guards
) -> None:
    from okg.substrate.db import capacity

    released: list[str] = []
    handlers = {signal.SIGTERM: signal.SIG_DFL}

    def fake_getsignal(signum):
        return handlers[signum]

    def fake_signal(signum, handler):
        previous = handlers[signum]
        handlers[signum] = handler
        return previous

    def interrupt_build(**kwargs):
        # SIGTERM arrives after the build asked for its claim.
        kwargs["claim_attempted"].append(True)
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(server_mod.signal, "getsignal", fake_getsignal)
    monkeypatch.setattr(server_mod.signal, "signal", fake_signal)
    monkeypatch.setattr(server_mod, "_build_server", interrupt_build)
    monkeypatch.setattr(
        capacity,
        "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )

    with pytest.raises(SystemExit) as exc:
        server_mod.run_server(deployment_name="cms")

    assert exc.value.code == 128 + signal.SIGTERM
    # build_server releases on its own way out, and the stop guard releases
    # early on SIGTERM; both calls are idempotent no-ops once the row is gone.
    assert released == ["okg-mcp-cms", "okg-mcp-cms"]
    assert handlers[signal.SIGTERM] is signal.SIG_DFL


# ── stop guard: early release + bounded shutdown (lane-h-mcp-serve-stop-guard)


def test_stop_guard_releases_from_its_own_thread_before_unwind(monkeypatch) -> None:
    import threading

    released: list[str] = []
    released_event = threading.Event()

    def release() -> None:
        released.append(threading.current_thread().name)
        released_event.set()

    exits: list[int] = []
    monkeypatch.setattr(server_mod.os, "_exit", exits.append)
    guard = server_mod._StopGuard(release, armed=True)
    try:
        guard.trigger(128 + signal.SIGTERM)
        # The main thread does nothing here: the guard thread alone must drop
        # the claim, as it must when the main thread is stuck in teardown.
        assert released_event.wait(2.0)
        assert released == ["okg-mcp-stop-guard-release"]
        guard.release()  # run_server's finally: a no-op now
        assert released == ["okg-mcp-stop-guard-release"]
    finally:
        guard.disarm()
    assert exits == []


def test_stop_guard_exits_143_when_unwind_is_stuck(monkeypatch) -> None:
    import threading

    monkeypatch.setattr(server_mod, "MCP_STOP_GUARD_SECONDS", 0.2)
    exited = threading.Event()
    exits: list[int] = []
    order: list[str] = []

    def fake_exit(code: int) -> None:
        order.append("exit")
        exits.append(code)
        exited.set()

    monkeypatch.setattr(server_mod.os, "_exit", fake_exit)
    monkeypatch.setattr(
        server_mod, "_flush_before_hard_exit", lambda *_a: order.append("flush")
    )
    guard = server_mod._StopGuard(lambda: order.append("release"), armed=True)
    try:
        guard.trigger(128 + signal.SIGTERM)
        assert exited.wait(3.0)
    finally:
        guard.disarm()
    assert exits == [143]
    # The release runs on its own thread, so its place relative to the
    # diagnostics is not fixed; only that both ran and the exit came last.
    assert sorted(order) == ["exit", "flush", "release"]
    assert order[-1] == "exit"


def test_stop_guard_deadline_ignores_a_blocked_release_and_diagnostics(
    monkeypatch,
) -> None:
    """A paused database or a stuck log handler cannot move the deadline."""
    import threading
    import time

    monkeypatch.setattr(server_mod, "MCP_STOP_GUARD_SECONDS", 0.3)
    monkeypatch.setattr(server_mod, "_STOP_GUARD_DIAGNOSTICS_SECONDS", 0.2)
    exited = threading.Event()
    exits: list[int] = []
    unblock = threading.Event()

    def fake_exit(code: int) -> None:
        exits.append(code)
        exited.set()

    monkeypatch.setattr(server_mod.os, "_exit", fake_exit)
    monkeypatch.setattr(
        server_mod, "_flush_before_hard_exit", lambda *_a: unblock.wait(10.0)
    )
    guard = server_mod._StopGuard(lambda: unblock.wait(10.0), armed=True)
    try:
        started = time.monotonic()
        guard.trigger(128 + signal.SIGTERM)
        assert exited.wait(3.0)
        elapsed = time.monotonic() - started
    finally:
        unblock.set()
        guard.disarm()
    assert exits == [143]
    # 0.3 s bound + 0.2 s diagnostics cap, never the 10 s the blockers hold.
    assert 0.3 <= elapsed < 1.5, elapsed


def test_stop_guard_disarmed_by_a_finished_unwind_never_exits(monkeypatch) -> None:
    import time

    monkeypatch.setattr(server_mod, "MCP_STOP_GUARD_SECONDS", 0.3)
    exits: list[int] = []
    monkeypatch.setattr(server_mod.os, "_exit", exits.append)
    guard = server_mod._StopGuard(lambda: None, armed=True)
    guard.trigger(128 + signal.SIGTERM)
    guard.release()
    guard.disarm()
    time.sleep(0.6)
    assert exits == []


def test_sigterm_keeps_the_guard_armed_after_the_unwind(monkeypatch, stop_guards) -> None:
    """Interpreter exit can hang too (a non-daemon thread blocked on stdin)."""
    import threading

    monkeypatch.setattr(server_mod, "MCP_STOP_GUARD_SECONDS", 0.3)
    exited = threading.Event()
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        exited.set()

    monkeypatch.setattr(server_mod.os, "_exit", fake_exit)
    monkeypatch.setattr(server_mod, "_flush_before_hard_exit", lambda *_a: None)
    handlers = {signal.SIGTERM: signal.SIG_DFL}
    monkeypatch.setattr(server_mod.signal, "getsignal", lambda signum: handlers[signum])

    def fake_signal(signum, handler):
        previous = handlers[signum]
        handlers[signum] = handler
        return previous

    monkeypatch.setattr(server_mod.signal, "signal", fake_signal)
    released: list[str] = []
    with pytest.raises(SystemExit):
        with server_mod._unwind_default_sigterm(lambda: released.append("claim")):
            handlers[signal.SIGTERM](signal.SIGTERM, None)
    # The unwind finished and the claim is gone, but the process did not exit:
    # the guard ends it at the bound.
    assert released == ["claim"]
    assert handlers[signal.SIGTERM] is signal.SIG_DFL
    assert exited.wait(3.0)
    assert exits == [143]


def test_untriggered_guard_is_disarmed_when_the_unwind_finishes(stop_guards) -> None:
    with server_mod._unwind_default_sigterm(lambda: None) as guard:
        pass
    if guard._thread is not None:
        guard._thread.join(2.0)
        assert not guard._thread.is_alive()


def test_stop_guard_concurrent_release_waits_for_the_first(monkeypatch) -> None:
    import threading

    gate = threading.Event()
    calls: list[int] = []

    def slow_release() -> None:
        calls.append(1)
        gate.wait(2.0)

    guard = server_mod._StopGuard(slow_release, armed=False)
    first = threading.Thread(target=guard.release)
    first.start()
    while not calls:
        pass
    second_done = threading.Event()
    second = threading.Thread(target=lambda: (guard.release(), second_done.set()))
    second.start()
    # The second caller must not return while the first release is in flight.
    assert not second_done.wait(0.2)
    gate.set()
    first.join(2.0)
    assert second_done.wait(2.0)
    assert calls == [1]


def test_mcp_owner_start_failure_does_not_start_unreserved(monkeypatch) -> None:
    from types import SimpleNamespace

    from okg.substrate.db import capacity

    monkeypatch.delenv("OKG_MCP_SURFACE_PROBE", raising=False)
    monkeypatch.setattr(server_mod, "_derive_role_dsn", lambda *_a, **_kw: "dsn")
    monkeypatch.setattr(server_mod, "_load_readside_metadata", lambda _name: {})
    monkeypatch.setattr(server_mod, "mcp_surface_report", lambda **_kwargs: {})
    monkeypatch.setattr(server_mod, "FastMCP", lambda *_a, **_kw: SimpleNamespace())
    monkeypatch.setattr(
        capacity,
        "reserve_process_demand",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            capacity.ReservationLifecycleError("owner did not start")
        ),
    )

    with pytest.raises(capacity.ReservationLifecycleError, match="did not start"):
        server_mod._build_server(
            dsn_authority={"dsn": "postgresql://not-used"},
            deployment_name="cms",
            max_conns=1,
        )
