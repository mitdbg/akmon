"""The source child's database-socket counter must never dup a descriptor.

On macOS a system library can guard a descriptor; ``dup`` on a guarded one
makes the kernel SIGKILL the process. The first tree-sitter grammar download
opens such a descriptor, so the old counter (``os.dup`` of every open
descriptor, once a second) killed the ``code_structure`` child on a fresh
Mac and the first ``okg install`` exited 1 (PACT macos-first-install-kill).
"""

from __future__ import annotations

import os
import socket
import tempfile
from pathlib import Path

import pytest

from okg.substrate.ingest import source_jobs


def _forbid_dup(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []

    def _dup(fd: int, *args, **kwargs):
        calls.append(fd)
        raise AssertionError(f"count_database_sockets called os.dup({fd})")

    monkeypatch.setattr(os, "dup", _dup)
    return calls


def test_counts_tcp_peers_without_dup_and_leaves_them_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(4)
    port = server.getsockname()[1]
    clients = [socket.create_connection(("127.0.0.1", port)) for _ in range(2)]
    accepted = [server.accept()[0] for _ in clients]
    try:
        dup_calls = _forbid_dup(monkeypatch)
        # Two client sockets have the port as their peer. The listening
        # socket has no peer, and the accepted sockets' peers are the
        # clients' ephemeral ports.
        assert source_jobs.count_database_sockets(port) == 2
        assert dup_calls == []
        # Counting borrowed each descriptor and handed it back open.
        for client, peer in zip(clients, accepted):
            client.sendall(b"x")
            assert peer.recv(1) == b"x"
    finally:
        for sock in (*clients, *accepted, server):
            sock.close()


def test_counts_postgres_unix_socket_peers(monkeypatch: pytest.MonkeyPatch) -> None:
    port = 54329
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / f".s.PGSQL.{port}")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(path)
        server.listen(1)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(path)
        accepted, _ = server.accept()
        try:
            _forbid_dup(monkeypatch)
            assert source_jobs.count_database_sockets(port) == 1
            assert source_jobs.count_database_sockets(port + 1) == 0
        finally:
            for sock in (client, accepted, server):
                sock.close()


def test_only_socket_descriptors_are_wrapped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A non-socket descriptor is only fstat-ed, never wrapped or closed."""
    regular = os.open(tmp_path / "plain.txt", os.O_CREAT | os.O_RDWR)
    read_end, write_end = os.pipe()
    wrapped: list[int] = []
    real_socket = socket.socket

    class _Recording(real_socket):
        def __init__(self, *args, fileno=None, **kwargs):
            if fileno is not None:
                wrapped.append(fileno)
            super().__init__(*args, fileno=fileno, **kwargs)

    monkeypatch.setattr(socket, "socket", _Recording)
    _forbid_dup(monkeypatch)
    try:
        source_jobs.count_database_sockets(5432)
        assert regular not in wrapped
        assert read_end not in wrapped
        assert write_end not in wrapped
        # Still open: fstat succeeds on each.
        for fd in (regular, read_end, write_end):
            os.fstat(fd)
    finally:
        for fd in (regular, read_end, write_end):
            os.close(fd)


def test_sampler_reports_through_the_new_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The once-a-second sampler keeps reporting current and peak counts."""
    import threading

    _forbid_dup(monkeypatch)
    sent: list[tuple[str, object]] = []

    class _Pipe:
        def send(self, message):
            sent.append(message)

    stop = threading.Event()
    stop.set()
    source_jobs._sample_connections(_Pipe(), 1, stop)
    assert sent == [("connections", {"current": 0, "peak": 0})]
