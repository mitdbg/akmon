"""probe_host ssh argv construction + ssh_opts handling (remote command exec).

probe_host shells out over ssh, so it is security-sensitive: these unit tests
pin the argv shape, ssh_opts ordering/expansion, BatchMode (fail-closed), the
error contract, and the local path. None need a database — subprocess.run is
stubbed — so this file is deliberately not postgres-marked.
"""
from __future__ import annotations

import os
import subprocess

from okg.substrate.external_backed.collector import (
    PROBE_SNIPPET,
    Host,
    load_hosts,
    probe_host,
)

_OK_STDOUT = (
    "load1=1\ncpu_count=4\n"
    "disk_used_pct=5\ndisk_used_bytes=500\ndisk_total_bytes=10000\n"
    "mem_used_pct=6\nmem_used_bytes=600\nmem_total_bytes=10000\n"
)


class _Result:
    def __init__(self, returncode: int = 0, stdout: str = _OK_STDOUT) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def test_ssh_argv_shape_and_ssh_opts_ordering(monkeypatch) -> None:
    captured: dict = {}

    def fake_run(argv, **kw):
        captured["argv"] = argv
        captured["input"] = kw.get("input")
        return _Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    host = Host("ostack", "host:ostack", "ubuntu@203.0.113.4",
                ssh_opts=("-i", "~/.ssh/key", "-J", "jump"))
    reachable, metrics = probe_host(host, timeout=10)

    assert reachable is True
    assert metrics == {
        "load1": 1.0,
        "cpu_count": 4.0,
        "disk_used_pct": 5.0,
        "disk_used_bytes": 500.0,
        "disk_total_bytes": 10_000.0,
        "mem_used_pct": 6.0,
        "mem_used_bytes": 600.0,
        "mem_total_bytes": 10_000.0,
    }
    argv = captured["argv"]
    expanded_key = os.path.expanduser("~/.ssh/key")
    # BatchMode (fail-closed, no password hang) + ConnectTimeout present; ssh_opts
    # inserted BEFORE the target (an opt after the target ssh sends as a remote
    # command); path-like opt ~-expanded, flag opts left untouched.
    assert argv == [
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
        "-i", expanded_key, "-J", "jump", "ubuntu@203.0.113.4", "sh",
    ]
    assert argv.index("ubuntu@203.0.113.4") > argv.index(expanded_key)
    assert captured["input"] == PROBE_SNIPPET


def test_local_target_uses_sh_not_ssh(monkeypatch) -> None:
    captured: dict = {}

    def fake_run(argv, **kw):
        captured["argv"] = argv
        return _Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    reachable, _ = probe_host(Host("laptop", "host:laptop", "local"), timeout=5)
    assert reachable is True
    assert captured["argv"] == ["sh", "-s"]


def test_nonzero_exit_is_unreachable(monkeypatch) -> None:
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: _Result(returncode=255, stdout=""),
    )
    assert probe_host(Host("h", "host:h", "u@h")) == (False, {})


def test_timeout_is_unreachable(monkeypatch) -> None:
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="ssh", timeout=10)

    monkeypatch.setattr(subprocess, "run", boom)
    assert probe_host(Host("h", "host:h", "u@h")) == (False, {})


def test_oserror_is_unreachable(monkeypatch) -> None:
    def boom(*a, **k):
        raise OSError("ssh executable not found")

    monkeypatch.setattr(subprocess, "run", boom)
    assert probe_host(Host("h", "host:h", "u@h")) == (False, {})


_SYNTHETIC_HOSTS_YAML = """\
metrics: [load1, reachable]
hosts:
  - name: laptop
    node_id: "host:laptop"
    target: local
    label: "Operator laptop"
  - name: vm-with-key
    node_id: "host:vm-with-key"
    target: "ubuntu@192.0.2.10"
    label: "Example VM (TEST-NET-1 address)"
    ssh_opts: ["-i", "~/.ssh/example_rsa"]
"""


def test_load_hosts_round_trips_ssh_opts(tmp_path) -> None:
    # A synthetic hosts file, so the test pins the loader's contract and not
    # one deployment's real host inventory. 192.0.2.0/24 is reserved for
    # documentation (RFC 5737).
    hosts_file = tmp_path / "monitored_hosts.yaml"
    hosts_file.write_text(_SYNTHETIC_HOSTS_YAML, encoding="utf-8")
    hosts = load_hosts(hosts_file)
    by_name = {h.name: h for h in hosts}
    vm = by_name["vm-with-key"]
    assert vm.target == "ubuntu@192.0.2.10"
    assert vm.ssh_opts == ("-i", "~/.ssh/example_rsa")
    # a host without ssh_opts gets an empty tuple, never None
    assert by_name["laptop"].ssh_opts == ()
