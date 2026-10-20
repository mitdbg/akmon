"""Isolation lifecycle + ownership lease expiry
(standardize-deployment-authority req.deploy.isolation-lifecycle)."""

from __future__ import annotations

import builtins
import datetime
import hashlib
import json
import os
import socket
import subprocess
import sys
import traceback
from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate.catalog.ownership as ownership
import okg.substrate.deployment_isolation as deployment_isolation

CatalogIdentity = ownership.CatalogIdentity
_live_host_port = ownership._live_host_port
_machine_id = ownership._machine_id
_recommended_commands = ownership._recommended_commands
owner_is_stale = ownership.owner_is_stale
live_worktree_suffixes = deployment_isolation.live_worktree_suffixes


def test_owner_with_dead_pid_on_this_host_is_stale() -> None:
    # Isolate the throwaway child from the pytest-xdist worker's session and
    # control channel (#703): its own session/process group means no stray
    # signal can reach the worker group, and DEVNULL stdio keeps it off the
    # worker's inherited execnet fds.
    proc = subprocess.Popen(
        ["true"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    proc.wait()
    owner = {
        "metadata": {"hostname": socket.gethostname(), "pid": proc.pid},
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    stale, reason = owner_is_stale(owner)
    assert stale is True
    assert str(proc.pid) in reason


def test_owner_with_live_pid_is_not_stale() -> None:
    # A legitimate holder's process starts BEFORE its claim heartbeat.
    owner = {
        "metadata": {"hostname": socket.gethostname(), "pid": os.getpid()},
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    stale, _ = owner_is_stale(owner)
    assert stale is False


def test_owner_with_recycled_pid_is_stale() -> None:
    # The pid is alive (it is us) but started years after the claim's
    # last heartbeat - a recycled pid is not the holder. Without the
    # start-time check, os.kill(pid, 0) keeps such a claim immortal on
    # its own host and the TTL fallback never runs.
    owner = {
        "metadata": {"hostname": socket.gethostname(), "pid": os.getpid()},
        "claimed_at": "2020-01-01T00:00:00+00:00",
    }
    stale, reason = owner_is_stale(owner)
    assert stale is True
    assert "recycled" in reason


def test_remote_owner_expires_by_ttl_only() -> None:
    fresh = {
        "metadata": {"hostname": "other-host", "pid": 1},
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    assert owner_is_stale(fresh) == (False, None)
    old = {
        "metadata": {"hostname": "other-host", "pid": 1},
        "claimed_at": "2020-01-01T00:00:00+00:00",
    }
    stale, reason = owner_is_stale(old)
    assert stale is True
    assert "TTL" in reason


def _dead_pid() -> int:
    """A pid that provably no longer exists (same isolation as above)."""
    proc = subprocess.Popen(
        ["true"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    proc.wait()
    return proc.pid


def test_dead_holder_is_stale_when_hostname_roamed_but_machine_id_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # #825: the same MacBook reports a different network-derived hostname
    # at probe time than at claim time; the stable machine id must still
    # route the claim through the definitive pid-liveness probe.
    monkeypatch.setattr(ownership, "_machine_id", lambda: "sha256:machine-a")
    pid = _dead_pid()
    owner = {
        "metadata": {
            "hostname": "dhcp-10-29-215-244.dyn.example.edu",
            "machine_id": "sha256:machine-a",
            "pid": pid,
        },
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    assert owner["metadata"]["hostname"] != socket.gethostname()
    stale, reason = owner_is_stale(owner)
    assert stale is True
    assert str(pid) in reason
    # A live holder under the same machine id is NOT stale, hostname
    # roaming or not.
    owner["metadata"]["pid"] = os.getpid()
    assert owner_is_stale(owner) == (False, None)


def test_machine_id_mismatch_forces_cross_host_ttl_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Differing machine ids mean cross-host even when the hostnames
    # coincide - a local pid probe against a foreign claim's pid number
    # would probe the wrong machine's process table.
    monkeypatch.setattr(ownership, "_machine_id", lambda: "sha256:machine-a")
    fresh = {
        "metadata": {
            "hostname": socket.gethostname(),
            "machine_id": "sha256:machine-b",
            "pid": os.getpid(),
        },
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    assert owner_is_stale(fresh) == (False, None)
    old = {
        "metadata": dict(fresh["metadata"]),
        "claimed_at": "2020-01-01T00:00:00+00:00",
    }
    stale, reason = owner_is_stale(old)
    assert stale is True
    # TTL reason, not the same-host recycled-pid reason.
    assert "TTL" in reason


def test_legacy_row_without_machine_id_keeps_hostname_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Rows claimed before #825 carry no machine id; the hostname-keyed
    # behavior must be preserved for them.
    monkeypatch.setattr(ownership, "_machine_id", lambda: "sha256:machine-a")
    pid = _dead_pid()
    owner = {
        "metadata": {"hostname": socket.gethostname(), "pid": pid},
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    stale, reason = owner_is_stale(owner)
    assert stale is True
    assert str(pid) in reason


def test_unreadable_local_machine_id_falls_back_to_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # When this process cannot read a stable machine id, the recorded id
    # is unusable for comparison and the hostname decides (documented,
    # warned degradation - never a silent hard failure).
    monkeypatch.setattr(ownership, "_machine_id", lambda: None)
    pid = _dead_pid()
    owner = {
        "metadata": {
            "hostname": socket.gethostname(),
            "machine_id": "sha256:machine-b",
            "pid": pid,
        },
        "claimed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    stale, reason = owner_is_stale(owner)
    assert stale is True
    assert str(pid) in reason


def test_ttl_reason_reports_age_in_hours() -> None:
    # A 100h-old cross-host claim must read as "100.0h old (TTL 72h)",
    # not "4d old (TTL 72h)" - a truncated day count against an hours
    # TTL invites should-have-expired-already misreads (#825).
    claimed = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        hours=100
    )
    owner = {
        "metadata": {"hostname": "other-host", "pid": 1},
        "claimed_at": claimed.isoformat(),
    }
    stale, reason = owner_is_stale(owner)
    assert stale is True
    assert "100.0h old" in reason
    assert f"TTL {ownership.OWNER_STALE_TTL_HOURS}h" in reason
    assert "d old" not in reason


def test_machine_id_is_readable_on_supported_platforms() -> None:
    # Unmocked smoke test: a silently-unreadable machine id would revert
    # the whole fix to the roaming hostname on exactly the platforms it
    # targets (e.g. ioreg not found because it lives in /usr/sbin).
    if not (sys.platform == "darwin" or sys.platform.startswith("linux")):
        pytest.skip("no stable machine id source on this platform")
    if sys.platform.startswith("linux") and not any(
        os.path.exists(p) for p in ("/etc/machine-id", "/var/lib/dbus/machine-id")
    ):
        # Minimal containers without a machine id are the designed,
        # warned degradation - not a smoke failure.
        pytest.skip("no machine-id file on this minimal Linux")
    value = _machine_id()
    assert value is not None
    assert value.startswith("sha256:")


def test_catalog_identity_provenance_stamps_machine_id(tmp_path) -> None:
    # The staleness fix is inert unless real claims carry the id:
    # catalog_identity_for's provenance is what claim metadata and
    # last_seen_provenance heartbeats are built from.
    from okg.substrate.catalog.modules_compose import ComposedCatalog

    d = tmp_path / "iso-stamp-test"
    d.mkdir()
    (d / "deployment.yaml").write_text("name: iso-stamp-test\nmodules: []\n")
    identity = ownership.catalog_identity_for(
        composed=ComposedCatalog(
            deployment_name="iso-stamp-test",
            modules=[],
            subtypes=set(),
            subtype_origin={},
            narrowings=[],
        ),
        deployment_dir=d,
        command="test",
    )
    assert identity.provenance["machine_id"] == _machine_id()
    if sys.platform == "darwin" or sys.platform.startswith("linux"):
        # Hashed, never the raw hardware identifier.
        assert identity.provenance["machine_id"].startswith("sha256:")


def test_live_host_port_derivation_edges() -> None:
    # TCP connections pass through; unix-socket and hostless
    # connections keep the loopback default while preserving the real
    # port; missing everything keeps the status-quo 127.0.0.1:5433.
    assert _live_host_port(SimpleNamespace(host="db.example", port=5435)) == (
        "db.example",
        5435,
    )
    assert _live_host_port(SimpleNamespace(host="/tmp/.s.PGSQL.5435", port=5435)) == (
        "127.0.0.1",
        5435,
    )
    assert _live_host_port(SimpleNamespace(host=None, port=None)) == ("127.0.0.1", 5433)

    identity = CatalogIdentity(
        deployment_name="iso-dsn-test",
        catalog_fingerprint="sha256:x",
        schema_fingerprint="sha256:x",
        writer_fingerprint="sha256:x",
        deployment_root_hash=None,
        worktree_root_hash=None,
        provenance={},
    )
    commands = _recommended_commands(identity, "ok", host="127.0.0.1", port=5435)
    assert any(":5435/" in c for c in commands)
    assert not any(":5433/" in c for c in commands)


def test_live_worktree_suffixes_cover_this_worktree() -> None:
    import hashlib
    from pathlib import Path

    suffixes = live_worktree_suffixes()
    here = hashlib.sha256(str(Path.cwd().resolve()).encode("utf-8")).hexdigest()[:10]
    # Running from a worktree (or the main checkout), our own root must be
    # in the live set - the property that keeps gc from dropping the DB of
    # the very worktree running it.
    assert here in suffixes


@pytest.mark.parametrize(
    "failure_kind",
    [
        "oserror",
        "timeout",
        "called_process_error",
        "decode",
        "bytes",
        "empty",
        "malformed",
        "whitespace",
        "truncated",
        "blank_path",
        "relative_path",
        "invalid_head",
        "blank_branch",
    ],
)
def test_live_worktree_inventory_failure_is_typed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    failure_kind: str,
) -> None:
    """Git probe failures never become the successful empty inventory ``{}``."""

    def fake_run(*_args, **_kwargs):
        if failure_kind == "oserror":
            raise OSError("git executable unavailable")
        if failure_kind == "timeout":
            raise subprocess.TimeoutExpired("git worktree list", 10)
        if failure_kind == "called_process_error":
            raise subprocess.CalledProcessError(128, "git worktree list")
        if failure_kind == "decode":
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad byte")
        if failure_kind == "bytes":
            return SimpleNamespace(stdout=b"\xff")
        if failure_kind == "empty":
            return SimpleNamespace(stdout="")
        if failure_kind == "whitespace":
            return SimpleNamespace(stdout=" \n\t")
        if failure_kind == "truncated":
            return SimpleNamespace(
                stdout=f"worktree {tmp_path}\0HEAD {'0' * 40}\0detached\0"
            )
        if failure_kind == "blank_path":
            return SimpleNamespace(
                stdout=f"worktree    \0HEAD {'0' * 40}\0detached\0\0"
            )
        if failure_kind == "relative_path":
            return SimpleNamespace(
                stdout=f"worktree relative\0HEAD {'0' * 40}\0detached\0\0"
            )
        if failure_kind == "invalid_head":
            return SimpleNamespace(
                stdout=f"worktree {tmp_path}\0HEAD garbage\0detached\0\0"
            )
        if failure_kind == "blank_branch":
            return SimpleNamespace(
                stdout=(f"worktree {tmp_path}\0HEAD {'0' * 40}\0branch    \0\0")
            )
        return SimpleNamespace(stdout="not git worktree porcelain\n")

    monkeypatch.setattr(deployment_isolation.subprocess, "run", fake_run)

    with pytest.raises(deployment_isolation.WorktreeInventoryUnavailable) as caught:
        deployment_isolation.live_worktree_suffixes(tmp_path)

    error = caught.value
    assert error.code == "worktree_inventory_unavailable"
    assert error.cause_type
    assert len(error.detail) <= 160


def test_live_worktree_empty_inventory_is_typed_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        deployment_isolation.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=""),
    )
    with pytest.raises(deployment_isolation.WorktreeInventoryUnavailable) as caught:
        deployment_isolation.live_worktree_suffixes(tmp_path)
    assert caught.value.code == "worktree_inventory_unavailable"


@pytest.mark.parametrize(
    "metadata",
    [
        f"HEAD {'0' * 40}\0branch refs/heads/main",
        "bare",
    ],
)
def test_live_worktree_inventory_success_is_dict_without_trimming_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    metadata: str,
) -> None:
    worktree = str(tmp_path / "worktree-with-trailing-space ")
    calls: list[list[str]] = []

    def fake_run(argv, **_kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout=f"worktree {worktree}\0{metadata}\0\0")

    monkeypatch.setattr(deployment_isolation.subprocess, "run", fake_run)
    suffixes = deployment_isolation.live_worktree_suffixes(tmp_path)
    expected = hashlib.sha256(
        str(Path(worktree).resolve()).encode("utf-8")
    ).hexdigest()[:10]

    assert suffixes == {expected: worktree}
    assert calls == [["git", "worktree", "list", "--porcelain", "-z"]]


def test_live_worktree_inventory_diagnostic_redacts_dsn_and_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args, **_kwargs):
        raise OSError(
            "git failed postgres://operator:SUPERSECRET@db/okg password='hunter\\'two'"
        )

    monkeypatch.setattr(deployment_isolation.subprocess, "run", fail)
    with pytest.raises(deployment_isolation.WorktreeInventoryUnavailable) as caught:
        deployment_isolation.live_worktree_suffixes()

    assert "SUPERSECRET" not in caught.value.detail
    assert "hunter" not in caught.value.detail
    assert "two" not in caught.value.detail
    assert "postgres://" not in caught.value.detail
    assert "<redacted-dsn>" in caught.value.detail
    assert "password=<redacted>" in caught.value.detail
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert caught.value.cause.type == "OSError"
    assert caught.value.cause.message == caught.value.detail
    assert "SUPERSECRET" not in repr(caught.value.cause)
    assert "hunter" not in repr(caught.value.cause)
    with pytest.raises(AttributeError):
        caught.value.cause.message = "unsafe"  # type: ignore[misc]


@pytest.mark.parametrize(
    "secret_key",
    ["PGPASSWORD", "PGSSLPASSWORD", "sslpassword", "Password", "PassFile"],
)
def test_live_worktree_inventory_diagnostic_redacts_libpq_secret_assignments(
    secret_key: str,
) -> None:
    detail = deployment_isolation._bounded_inventory_diagnostic(
        f"git failed {secret_key}=SUPERSECRET sentinel"
    )

    assert "SUPERSECRET" not in detail
    assert "sentinel" not in detail
    assert f"{secret_key}=<redacted>" in detail


def test_live_worktree_inventory_diagnostic_does_not_chain_process_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process_error = subprocess.CalledProcessError(
        128,
        ["git", "worktree", "list"],
        output="stdout-secret postgres://operator:SUPERSECRET@db/okg",
        stderr="stderr-secret password=hunter",
    )

    def fail(*_args, **_kwargs):
        raise process_error

    monkeypatch.setattr(deployment_isolation.subprocess, "run", fail)
    with pytest.raises(deployment_isolation.WorktreeInventoryUnavailable) as caught:
        deployment_isolation.live_worktree_suffixes()

    error = caught.value
    rendered = "".join(traceback.format_exception(error))
    assert error.cause.type == "CalledProcessError"
    assert error.cause.message == "CalledProcessError"
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "stdout-secret" not in rendered
    assert "stderr-secret" not in rendered
    assert "SUPERSECRET" not in rendered
    assert "hunter" not in rendered


def test_live_worktree_inventory_cause_type_is_stable_for_hostile_class_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hostile_name = "postgres://operator:SUPERSECRET@db/" + ("x" * 5_000)
    HostileInventoryError = type(hostile_name, (RuntimeError,), {})

    def fail(*_args, **_kwargs):
        raise HostileInventoryError("git probe failed")

    monkeypatch.setattr(deployment_isolation.subprocess, "run", fail)
    with pytest.raises(deployment_isolation.WorktreeInventoryUnavailable) as caught:
        deployment_isolation.live_worktree_suffixes()

    error = caught.value
    assert error.cause_type == "RuntimeError"
    assert error.cause.type == "RuntimeError"
    assert len(error.cause.type) <= 64
    assert "SUPERSECRET" not in repr(error)
    assert "SUPERSECRET" not in repr(error.cause)


def test_live_worktree_inventory_diagnostic_redacts_truncated_quoted_secret() -> None:
    detail = deployment_isolation._bounded_inventory_diagnostic(
        "git failed password='SUPERSECRET TOPSECRET" + (" Q" * 500) + "'"
    )

    assert detail == "git failed password=<redacted>"
    assert "SUPERSECRET" not in detail
    assert "TOPSECRET" not in detail


def test_live_worktree_inventory_diagnostic_bounds_before_stringification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostileDiagnosticError(RuntimeError):
        def __str__(self) -> str:
            raise AssertionError("arbitrary exception stringification is unsafe")

    def fail(*_args, **_kwargs):
        raise HostileDiagnosticError("bounded detail " + ("x" * 10_000))

    monkeypatch.setattr(deployment_isolation.subprocess, "run", fail)
    with pytest.raises(deployment_isolation.WorktreeInventoryUnavailable) as caught:
        deployment_isolation.live_worktree_suffixes()

    assert caught.value.detail.startswith("bounded detail ")
    assert len(caught.value.detail) <= 160


def test_inventory_failure_precedes_transitive_psycopg_import() -> None:
    script = """
import json
import sys
import okg.substrate.deployment_isolation as isolation

before = "psycopg" in sys.modules
def fail(*_args, **_kwargs):
    raise OSError("git unavailable")
isolation.subprocess.run = fail
report = isolation.list_isolated(
    "okg-workspace", "postgres://operator:secret@db/okg_workspace"
)
print(json.dumps({
    "before": before,
    "after": "psycopg" in sys.modules,
    "code": report.get("code"),
}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    observed = json.loads(result.stdout)
    assert observed == {
        "before": False,
        "after": False,
        "code": "worktree_inventory_unavailable",
    }


class _IsolationCursor:
    def __init__(
        self,
        rows: list[tuple[str, int]],
        calls: list[tuple[str, object]],
        fail_drop_names: set[str] | None = None,
    ) -> None:
        self.rows = rows
        self.calls = calls
        self.fail_drop_names = fail_drop_names or set()
        self._selected: list[tuple[str, int]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query: str, params=None) -> None:
        self.calls.append((query, params))
        normalized = " ".join(query.split())
        if "FROM pg_database" in normalized:
            declared = params[1]
            self._selected = sorted(
                (
                    row
                    for row in self.rows
                    if row[0].startswith(f"{declared}_") and row[0] != declared
                ),
                key=lambda row: row[0],
            )
            return
        if "DROP DATABASE" in normalized:
            name = query.split('DROP DATABASE "', 1)[1].split('"', 1)[0]
            if name in self.fail_drop_names:
                raise RuntimeError(f"drop failed for {name}")

    def fetchall(self) -> list[tuple[str, int]]:
        return self._selected


class _IsolationConnection:
    def __init__(
        self,
        rows: list[tuple[str, int]],
        calls: list[tuple[str, object]],
        fail_drop_names: set[str] | None = None,
    ) -> None:
        self.rows = rows
        self.calls = calls
        self.fail_drop_names = fail_drop_names or set()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self) -> _IsolationCursor:
        return _IsolationCursor(self.rows, self.calls, self.fail_drop_names)

    def close(self) -> None:
        self.closed = True


def test_inventory_failure_refuses_gc_without_database_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unavailable Git authority blocks list and both GC modes pre-DB."""
    import psycopg

    monkeypatch.setattr(
        deployment_isolation,
        "_declared_database",
        lambda _deployment: "okg_demo",
    )
    connect_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: (
            connect_calls.append((args, kwargs))
            or (_ for _ in ()).throw(AssertionError("database connect forbidden"))
        ),
    )

    real_import = builtins.__import__

    def forbid_psycopg_import(name, *args, **kwargs):
        if name in {"psycopg", "okg.substrate.catalog.ownership"}:
            raise AssertionError("database modules forbidden before Git authority")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", forbid_psycopg_import)

    hostile_name = "postgres://operator:SUPERSECRET@db/" + ("x" * 5_000)
    HostileInventoryError = type(hostile_name, (RuntimeError,), {})
    for inventory_failure in ("exception", "empty", "hostile", "prefixed-secret"):
        if inventory_failure == "exception":
            monkeypatch.setattr(
                deployment_isolation.subprocess,
                "run",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("git worktree list unavailable")
                ),
            )
        elif inventory_failure == "empty":
            monkeypatch.setattr(
                deployment_isolation.subprocess,
                "run",
                lambda *_args, **_kwargs: SimpleNamespace(stdout=""),
            )
        elif inventory_failure == "hostile":

            def raise_hostile(*_args, **_kwargs):
                raise HostileInventoryError("git probe failed")

            monkeypatch.setattr(
                deployment_isolation.subprocess,
                "run",
                raise_hostile,
            )
        else:
            monkeypatch.setattr(
                deployment_isolation.subprocess,
                "run",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("git probe failed PGPASSWORD=SUPERSECRET")
                ),
            )

        reports = [
            deployment_isolation.list_isolated("demo", "postgres://db/okg_demo"),
            deployment_isolation.gc_isolated(
                "demo",
                "postgres://db/okg_demo",
                yes=False,
            ),
            deployment_isolation.gc_isolated(
                "demo",
                "postgres://db/okg_demo",
                yes=True,
            ),
        ]
        for report in reports:
            assert report["ok"] is False
            assert report["status"] == "unavailable"
            assert report["code"] == "worktree_inventory_unavailable"
            assert "refusing" in report["reason"].lower()
            assert "isolated databases" in report["reason"].lower()
            assert report["deployment"] == "demo"
            assert "isolated" not in report
            assert "would_drop" not in report
            assert "dropped" not in report
            if inventory_failure == "hostile":
                assert report["error"]["type"] == "RuntimeError"
                assert len(report["error"]["type"]) <= 64
                assert "SUPERSECRET" not in str(report)
            if inventory_failure == "prefixed-secret":
                assert "SUPERSECRET" not in str(report)
    assert connect_calls == []


def test_isolated_gc_compatibility_matrix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live/orphan listing, dry-run, drop, scan and per-DB errors stay intact."""
    import psycopg

    declared = "okg_demo"
    canonical_dsn = "postgres://postgres:okg@host1:5432/okg_demo"
    clone_dsn = "postgres://postgres:okg@host2:5433/okg_demo_clone"
    admin1 = "postgres://postgres:okg@host1:5432/postgres"
    admin2 = "postgres://postgres:okg@host2:5433/postgres"
    rows = [
        ("okg_demo_live123", 100),
        ("okg_demo_dead000", 200),
        ("okg_demo", 999),  # PostgreSQL query must exclude the canonical DB.
    ]
    live = {"live123": "/repo/live-worktree"}
    calls: list[tuple[str, object]] = []
    connections: list[_IsolationConnection] = []

    monkeypatch.setattr(deployment_isolation, "_declared_database", lambda _: declared)
    monkeypatch.setattr(
        deployment_isolation,
        "live_worktree_suffixes",
        lambda start=None: dict(live),
    )
    monkeypatch.setattr(
        ownership,
        "suggested_worktree_dsn",
        lambda **_kwargs: clone_dsn,
    )

    def connect(admin: str, **_kwargs):
        if admin == admin2:
            raise OSError("second Postgres server is offline")
        conn = _IsolationConnection(rows, calls)
        connections.append(conn)
        return conn

    monkeypatch.setattr(psycopg, "connect", connect)

    listed = deployment_isolation.list_isolated("demo", canonical_dsn)
    assert listed["ok"] is True
    assert listed["servers_scanned"] == ["host1:5432", "host2:5433"]
    assert listed["scan_errors"] == [
        {"server": "host2:5433", "error": "second Postgres server is offline"},
    ]
    assert listed["count"] == 2
    assert listed["orphans"] == 1
    assert listed["orphan_bytes"] == 200
    assert listed["live_worktrees_checked"] == 1
    assert {
        (row["database"], row["status"], row["size_bytes"], row["worktree"])
        for row in listed["isolated"]
    } == {
        ("okg_demo_dead000", "orphan", 200, None),
        ("okg_demo_live123", "live", 100, "/repo/live-worktree"),
    }
    select_calls = [call for call in calls if "FROM pg_database" in call[0]]
    assert select_calls
    assert select_calls[0][1] == (r"okg_demo\_%", "okg_demo")
    assert "AND datname <> %s" in select_calls[0][0]

    calls.clear()
    dry_run = deployment_isolation.gc_isolated("demo", canonical_dsn)
    assert dry_run["ok"] is True
    assert dry_run["mode"] == "dry_run"
    assert dry_run["would_drop"] == ["okg_demo_dead000"]
    assert "dropped" not in dry_run
    assert not any(
        "pg_terminate_backend" in query or "DROP DATABASE" in query
        for query, _ in calls
    )

    calls.clear()
    dropped = deployment_isolation.gc_isolated("demo", canonical_dsn, yes=True)
    assert dropped["ok"] is True
    assert dropped["dropped"] == ["okg_demo_dead000@host1:5432"]
    assert dropped["errors"] == []
    assert not any('DROP DATABASE "okg_demo"' in query for query, _ in calls)
    assert any("pg_terminate_backend" in query for query, _ in calls)
    assert any('DROP DATABASE "okg_demo_dead000"' in query for query, _ in calls)
    assert connections[-1].closed

    # A failed DROP is reported per database and does not erase successful
    # results for another true orphan.
    rows_with_two_orphans = rows + [("okg_demo_dead111", 300)]
    calls.clear()
    connections.clear()

    def connect_with_drop_error(admin: str, **_kwargs):
        conn_rows = rows_with_two_orphans if admin == admin1 else []
        conn = _IsolationConnection(
            conn_rows,
            calls,
            fail_drop_names={"okg_demo_dead111"},
        )
        connections.append(conn)
        return conn

    monkeypatch.setattr(psycopg, "connect", connect_with_drop_error)
    partial = deployment_isolation.gc_isolated("demo", canonical_dsn, yes=True)
    assert partial["ok"] is False
    assert partial["dropped"] == ["okg_demo_dead000@host1:5432"]
    assert partial["errors"] == [
        {"database": "okg_demo_dead111", "error": "drop failed for okg_demo_dead111"},
    ]

    # An undeclared deployment remains a pre-DB refusal.
    monkeypatch.setattr(deployment_isolation, "_declared_database", lambda _: None)
    calls.clear()
    undeclared = deployment_isolation.list_isolated("no-db", canonical_dsn)
    assert undeclared["ok"] is False
    assert "postgres.database" in undeclared["reason"]
    assert calls == []
