"""Where the write-surface posture refusal sits in `okg mcp-serve` (PACT mcp-http-auth).

`req.mcp-http-auth.write-guard-gated-on-auth` describes `_cmd_mcp_serve` as the
guard caller "which refuses before any DB work", and the code says the same at
the call site ("Refuse enabling the write surface over a non-loopback network
transport before doing any DB work"). That claim is what these tests measure.

It matters because the refusal is a PURE ARGV decision -- (non-loopback host) x
(writes on) x (no credential) -- and the operator who typed that invocation
asked to expose graph mutation to anyone who can reach the port. Telling them
about something else instead does not open the write surface (the guard still
refuses once reached), but it does mean the security-posture message can be
masked by an unrelated operational one.

The existing matrix test in tests/substrate/test_mcp_transport.py cannot see
this: it stubs `enforce_release_contract_if_declared` to a no-op
(test_mcp_transport.py:445-448), so the step that precedes the guard is
removed before the ordering is measured.
"""
from __future__ import annotations

import argparse

from okg.substrate.cli import main as cli_main
from okg.substrate.errors import SubstrateError

#: The invocation the guard exists to refuse: routable bind, writes on, no
#: credential. Every field is fixed so the two tests below differ in exactly
#: one thing -- whether the release-contract check raises.
_EXPOSED_WRITE_INVOCATION = dict(
    deployment="default",
    dsn=None,
    embedder=None,
    branch="default",
    allow_writes=True,
    transport="streamable-http",
    host="0.0.0.0",
    port=8124,
    allowed_host=[],
    auth_token_env=None,
    auth_token=None,
)

#: A fragment of the shipped guard refusal, which
#: tests/substrate/test_mcp_transport.py pins byte for byte.
_GUARD_TEXT = "refusing to serve the write-enabled MCP surface"


class _FakeDsn:
    dsn = "postgresql://postgres:postgres@127.0.0.1:1/nodb"
    authority: dict = {}
    target = "test"


def _run_cmd_mcp_serve(monkeypatch, capsys, *, release_check):
    """Invoke `_cmd_mcp_serve` for the exposed-write invocation.

    ``release_check`` is substituted for the real release-contract enforcement
    so each test controls the one variable it is about. run_server is stubbed
    so a permitted cell would return instead of blocking on a socket.
    """
    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_read_dsn",
        lambda *a, **kw: _FakeDsn(),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.enforce_release_contract_if_declared",
        release_check,
    )
    import okg.substrate.mcp.server as server_mod

    monkeypatch.setattr(server_mod, "run_server", lambda **kw: None)

    code = cli_main._cmd_mcp_serve(
        argparse.Namespace(**_EXPOSED_WRITE_INVOCATION), as_json=True
    )
    captured = capsys.readouterr()
    return code, captured.out + captured.err


def test_exposed_write_invocation_is_refused_by_the_guard_when_release_is_clean(
    monkeypatch, capsys
):
    """Positive control for the ordering test below.

    With the release check passing, the guard is reached and its message is
    what the operator sees. Without this control, an xfail on the next test
    could be blamed on broken plumbing rather than on the ordering.
    """
    code, output = _run_cmd_mcp_serve(
        monkeypatch, capsys, release_check=lambda **kw: None
    )
    assert code != 0
    assert _GUARD_TEXT in output, output


def test_exposed_write_invocation_is_refused_even_when_the_release_check_fails(
    monkeypatch, capsys
):
    """The posture refusal must not be masked by an operational error.

    Same invocation as the control above; the only difference is that the
    release-contract check raises. The operator still asked to expose the
    write surface, so the guard's message is still the right answer.

    This was an xfail(strict=True) while the guard sat below DSN resolution
    and the release-contract check. The guard pre-check now runs beside the
    other argv-decidable posture refusals, so the skew no longer preempts it.
    """

    def _skewed(**_kw):
        raise SubstrateError(
            "deployment release/version skew detected",
            fix_hint="run `okg up default`",
        )

    code, output = _run_cmd_mcp_serve(monkeypatch, capsys, release_check=_skewed)
    assert code != 0
    assert _GUARD_TEXT in output, output
    # The skew message must not be what the operator is shown instead.
    assert "release/version skew" not in output, output


def test_exposed_write_invocation_is_refused_before_any_dsn_resolution(
    monkeypatch, capsys
):
    """The refusal is decidable from argv, so it must precede DB-facing work.

    Both the requirement and the call-site comment say `_cmd_mcp_serve`
    refuses "before any DB work". Assert it directly: DSN resolution and the
    release-contract check are replaced with functions that fail the test if
    they are ever called.
    """

    def _must_not_run(*_a, **_kw):
        raise AssertionError(
            "DSN/release work ran before the write-surface posture refusal"
        )

    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_read_dsn", _must_not_run
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.enforce_release_contract_if_declared",
        _must_not_run,
    )
    import okg.substrate.mcp.server as server_mod

    monkeypatch.setattr(server_mod, "run_server", lambda **kw: None)

    code = cli_main._cmd_mcp_serve(
        argparse.Namespace(**_EXPOSED_WRITE_INVOCATION), as_json=True
    )
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert code != 0
    assert _GUARD_TEXT in output, output
