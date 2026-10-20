"""harden-connector-route-vetting: the two doctor probes.

Listener exposure: loopback or auth-gated passes; a non-loopback
unauthenticated bind fails. Token redaction: any >=8-char window
of keyring token material appearing in a read-surface payload is
a leak."""
from __future__ import annotations

import os
import socket
import subprocess

import pytest

from okg.substrate.cli import connector_security
from okg.substrate.cli.connector_security import (
    ListenerInfo,
    collect_substrate_listeners,
    evaluate_listeners,
    find_token_leaks,
    parse_lsof_listeners,
    parse_ss_listeners,
    token_windows,
)
from tests.substrate._fake_credentials import fake_credential

#: A fake OAuth access token in the real shape; built at runtime.
SENTINEL_TOKEN = fake_credential("ya29.", "SENTINEL-TOKEN-MATERIAL-0001")


def test_nonloopback_unauth_bind_fails_check() -> None:
    """The evaluator matrix from the test plan: 127.0.0.1 passes,
    0.0.0.0 without auth fails, 0.0.0.0 with declared auth passes."""
    report = evaluate_listeners([
        ListenerInfo(name="okg-mcp", pid=1, host="127.0.0.1", port=8765),
        ListenerInfo(name="okg-console", pid=2, host="0.0.0.0", port=8080),
        ListenerInfo(
            name="okg-http", pid=3, host="0.0.0.0", port=9090,
            auth_gated=True,
        ),
        ListenerInfo(name="okg-v6", pid=4, host="::1", port=7000),
    ])
    by_port = {row["port"]: row for row in report["listeners"]}
    assert by_port[8765]["ok"] is True
    assert by_port[8080]["ok"] is False
    assert "NON-LOOPBACK" in by_port[8080]["detail"]
    assert by_port[9090]["ok"] is True
    assert by_port[7000]["ok"] is True
    assert report["ok"] is False
    assert report["failures"] == 1


def test_all_loopback_reports_clean() -> None:
    report = evaluate_listeners([
        ListenerInfo(name="okg", pid=1, host="localhost", port=1234),
    ])
    assert report["ok"] is True and report["failures"] == 0


_LSOF_SAMPLE = """\
COMMAND   PID  USER   FD   TYPE DEVICE SIZE/OFF NODE NAME
okg-mcp  4242 riley    7u  IPv4 0x0001      0t0  TCP 127.0.0.1:8765 (LISTEN)
okg-mcp  4242 riley    8u  IPv6 0x0002      0t0  TCP [::1]:8765 (LISTEN)
python   5555 riley    9u  IPv4 0x0003      0t0  TCP *:9999 (LISTEN)
okgwork  6666 riley   10u  IPv4 0x0004      0t0  TCP 0.0.0.0:8080 (LISTEN)
chrome   7777 riley   11u  IPv4 0x0005      0t0  TCP 127.0.0.1:5353 (LISTEN)
"""


def test_parse_lsof_filters_to_substrate_processes() -> None:
    listeners = parse_lsof_listeners(
        _LSOF_SAMPLE, auth_gated_ports={8080},
    )
    # okg-mcp x2 + okgwork; python and chrome are not substrate-owned.
    assert {(listener.name, listener.host, listener.port) for listener in listeners} == {
        ("okg-mcp", "127.0.0.1", 8765),
        ("okg-mcp", "::1", 8765),
        ("okgwork", "0.0.0.0", 8080),
    }
    gated = next(listener for listener in listeners if listener.port == 8080)
    assert gated.auth_gated is True


def test_parse_lsof_include_pids_overrides_name_filter() -> None:
    listeners = parse_lsof_listeners(_LSOF_SAMPLE, include_pids={5555})
    assert ("python", "0.0.0.0", 9999) in {
        (listener.name, listener.host, listener.port) for listener in listeners
    }


def test_collector_sees_real_in_process_bind() -> None:
    """A real listener in THIS process is collected via include_pids."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = sock.getsockname()[1]
        listeners = collect_substrate_listeners(
            include_pids={os.getpid()},
        )
        mine = [listener for listener in listeners if listener.port == port]
        assert mine, "collector did not observe the test bind"
        report = evaluate_listeners(mine)
        assert report["ok"] is True
    finally:
        sock.close()


# -- token redaction -------------------------------------------------


def test_token_windows() -> None:
    assert token_windows("short", min_len=8) == ["short"]
    windows = token_windows("0123456789ab", min_len=8)
    assert windows[0] == "01234567"
    assert windows[-1] == "456789ab"
    assert token_windows("", min_len=8) == []


def test_find_token_leaks_detects_full_and_partial() -> None:
    token = SENTINEL_TOKEN
    leaks = find_token_leaks(
        payloads=[
            ("status:gmail", '{"detail": "' + SENTINEL_TOKEN + '"}'),
            ("status:slack", '{"detail": "TOKEN-MATERIAL leaked partially"}'),
            ("status:clean", '{"detail": "env vars set + token in keyring"}'),
        ],
        tokens=[token],
    )
    leaked_payloads = {leak["payload"] for leak in leaks}
    assert leaked_payloads == {"status:gmail", "status:slack"}
    # The leak report itself never echoes the material.
    for leak in leaks:
        assert token not in str(leak)


def test_find_token_leaks_clean_payloads() -> None:
    assert find_token_leaks(
        payloads=[("status:gmail", '{"ok": true}')],
        tokens=[SENTINEL_TOKEN],
    ) == []


def test_preflight_surface_is_redacted_with_seeded_sentinel(
    monkeypatch,
) -> None:
    """End-to-end shape of the doctor probe: a sentinel token seeded
    into a stubbed TokenStore must not appear in the serialized
    preflight payloads the probe checks."""
    import json

    from okg.substrate.library.oauth import preflight as preflight_mod
    from okg.substrate.library.oauth.preflight import oauth_preflight
    from okg.substrate.library.oauth.providers import GMAIL

    sentinel = SENTINEL_TOKEN

    class StubStore:
        def get(self, *, deployment, provider):
            return {
                "access_token": sentinel,
                "refresh_token": "1//SENTINEL-REFRESH-0002",
            }

    monkeypatch.setattr(preflight_mod, "TokenStore", StubStore)
    monkeypatch.setenv("OKG_GMAIL_CLIENT_ID", "cid")
    monkeypatch.setenv("OKG_GMAIL_CLIENT_SECRET", "csecret")

    result = oauth_preflight(
        source_name="gmail_api",
        deployment_name="test",
        provider=GMAIL,
    )
    payload = json.dumps(result.as_dict(), default=str)
    leaks = find_token_leaks(
        payloads=[("preflight:gmail", payload)],
        tokens=[sentinel, "1//SENTINEL-REFRESH-0002"],
    )
    assert leaks == [], "preflight read surface leaked token material"
    assert result.status == "ok"


_SS_SAMPLE = """State  Recv-Q Send-Q Local Address:Port  Peer Address:Port Process
LISTEN 0      4096   127.0.0.1:8765      0.0.0.0:*         users:(("okg-console",pid=4242,fd=7))
LISTEN 0      4096   0.0.0.0:9999        0.0.0.0:*         users:(("sshd",pid=900,fd=3))
LISTEN 0      4096   [::1]:9000          [::]:*            users:(("dbos-worker",pid=4243,fd=9))
LISTEN 0      4096   0.0.0.0:55432       0.0.0.0:*
"""


def test_parse_ss_filters_to_substrate_processes() -> None:
    listeners = parse_ss_listeners(_SS_SAMPLE)

    assert {(item.name, item.pid, item.host, item.port) for item in listeners} == {
        ("okg-console", 4242, "127.0.0.1", 8765),
        ("dbos-worker", 4243, "::1", 9000),
    }


def test_parse_ss_reports_a_watched_db_port_without_process_detail() -> None:
    """ss omits the process column without privilege.

    Dropping the row would turn the most sensitive listener into a silent
    pass, so it is reported with an empty name and pid 0 instead.
    """

    listeners = parse_ss_listeners(_SS_SAMPLE, watched_db_ports={55432})

    watched = [item for item in listeners if item.port == 55432]
    assert len(watched) == 1
    assert watched[0].watched_db_port is True
    assert watched[0].host == "0.0.0.0"
    assert watched[0].pid == 0


def test_parse_ss_include_pids_overrides_name_filter() -> None:
    listeners = parse_ss_listeners(_SS_SAMPLE, include_pids={900})

    assert any(item.name == "sshd" and item.port == 9999 for item in listeners)


def test_collector_falls_back_to_ss_when_lsof_is_absent(monkeypatch) -> None:
    """A host without lsof must still be scannable.

    The current Ubuntu 24.04 image ships no lsof, so before this fallback the
    check failed closed on hosts that were actually fine.
    """

    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        if argv[0] == "lsof":
            raise FileNotFoundError(2, "No such file or directory", "lsof")
        return subprocess.CompletedProcess(argv, 0, _SS_SAMPLE, "")

    monkeypatch.setattr(connector_security.subprocess, "run", fake_run)

    listeners = connector_security.collect_substrate_listeners()

    assert [argv[0] for argv in calls] == ["lsof", "ss"]
    assert {item.name for item in listeners} == {"okg-console", "dbos-worker"}


def test_collector_still_raises_when_ss_also_fails(monkeypatch) -> None:
    """Only a MISSING lsof falls back; a broken scan never reads as empty."""

    def fake_run(argv, **kwargs):
        if argv[0] == "lsof":
            raise FileNotFoundError(2, "No such file or directory", "lsof")
        raise OSError("ss exploded")

    monkeypatch.setattr(connector_security.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="lsof is absent and ss failed"):
        connector_security.collect_substrate_listeners()


def test_collector_keeps_raising_on_a_non_missing_lsof_failure(monkeypatch) -> None:
    def fake_run(argv, **kwargs):
        if argv[0] == "lsof":
            raise OSError("permission denied")
        raise AssertionError("ss must not be reached")

    monkeypatch.setattr(connector_security.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="lsof scan failed"):
        connector_security.collect_substrate_listeners()


def test_collector_raises_when_ss_exits_nonzero(monkeypatch) -> None:
    """A scanner that fails with empty output must not read as "no listeners".

    evaluate_listeners reports ok=True for an empty list, so swallowing a
    non-zero exit would produce exactly the false pass this probe exists to
    prevent.
    """

    def fake_run(argv, **kwargs):
        if argv[0] == "lsof":
            raise FileNotFoundError(2, "No such file or directory", "lsof")
        return subprocess.CompletedProcess(argv, 1, "", "ss: something broke")

    monkeypatch.setattr(connector_security.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="ss exited 1"):
        connector_security.collect_substrate_listeners()


def test_collector_raises_when_lsof_fails_with_stderr(monkeypatch) -> None:
    def fake_run(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, "", "lsof: permission denied")

    monkeypatch.setattr(connector_security.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="lsof scan failed: exit 1"):
        connector_security.collect_substrate_listeners()


def test_collector_accepts_lsofs_empty_result_exit_code(monkeypatch) -> None:
    """lsof exits 1 with EMPTY stderr when it matched nothing.

    That is a legitimate empty answer, not a failure, so it must not raise --
    otherwise a host with no substrate listeners could never pass.
    """

    def fake_run(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, "", "")

    monkeypatch.setattr(connector_security.subprocess, "run", fake_run)

    assert connector_security.collect_substrate_listeners() == []


def test_scan_reports_which_scanner_answered(monkeypatch) -> None:
    """The operator must be able to tell an lsof host from a fallback host."""

    def with_lsof(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 0, _LSOF_SAMPLE, "")

    monkeypatch.setattr(connector_security.subprocess, "run", with_lsof)
    _, scanner = connector_security.scan_substrate_listeners()
    assert scanner == "lsof"

    def without_lsof(argv, **kwargs):
        if argv[0] == "lsof":
            raise FileNotFoundError(2, "No such file or directory", "lsof")
        return subprocess.CompletedProcess(argv, 0, _SS_SAMPLE, "")

    monkeypatch.setattr(connector_security.subprocess, "run", without_lsof)
    _, scanner = connector_security.scan_substrate_listeners()
    assert scanner == "ss"


def test_parse_ss_reports_one_row_for_a_socket_shared_by_two_processes() -> None:
    """One exposed port is one failure, not N.

    ss lists every process sharing a socket on a single line; emitting a row
    each would multiply the failure count for a single exposure.
    """

    shared = (
        'LISTEN 0 4096 0.0.0.0:8080 0.0.0.0:* '
        'users:(("okg-web",pid=11,fd=7),("okg-web",pid=12,fd=7))\n'
    )

    listeners = parse_ss_listeners(shared)

    assert len(listeners) == 1
    assert listeners[0].port == 8080
