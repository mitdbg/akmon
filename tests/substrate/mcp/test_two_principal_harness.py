"""Hermetic resource-ownership regressions for the real HTTP proof harness."""

from __future__ import annotations

import subprocess
import sys

import pytest

from tests.substrate.mcp import test_multi_user_http_e2e as harness


def test_identical_session_configuration_cannot_share_a_database(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    dsn = "postgresql://postgres@127.0.0.1/okg_test_shared_base"

    first = harness._database_name(dsn)
    second = harness._database_name(dsn)

    assert first != second
    for name in (first, second):
        assert name.startswith(harness._DB_PREFIX)
        assert len(name) <= 63


def test_readiness_timeout_terminates_and_reaps_server(
    monkeypatch, tmp_path_factory
) -> None:
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(600)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    def timeout(*args):
        raise AssertionError("synthetic readiness timeout")

    monkeypatch.setattr(harness, "_start_server", lambda *args: proc)
    monkeypatch.setattr(harness, "_wait_until_listening", timeout)
    fixture = harness.two_principal_server.__wrapped__(
        "postgresql://unused/okg_test_unused", tmp_path_factory
    )
    try:
        with pytest.raises(AssertionError, match="synthetic readiness timeout"):
            next(fixture)
        assert proc.poll() is not None, "readiness failure left the server running"
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)
