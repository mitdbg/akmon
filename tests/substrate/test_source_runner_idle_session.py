"""Issue #2920: the source runner holds no transaction open across adapter.run.

Before the fix, `SourceRunner._chronos_branch_incarnation` read the Chronos
branch row on the Chronos session without ending the transaction that read
opened. The runner makes that read before it calls the source adapter, so the
session sat "idle in transaction" for the whole adapter run. When the adapter
took longer than the idle bound (300 s by default), PostgreSQL killed the
session and the run failed with IdleInTransactionSessionTimeout (SQLSTATE
25P03). Every full run after the first failed this way, because only a run
with a prior record-set proof makes the read.

These tests use a real PostgreSQL server, a 2 s idle bound, and an adapter
that sleeps past it.
"""
from __future__ import annotations

import functools
import subprocess
import time
from pathlib import Path

import psycopg
import pytest

from okg.substrate.db import connection as conn_mod
from okg.substrate.db.pool import close_all
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.git_graph_files import GitFilesSource
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

_IDLE_BOUND_MS = 2000
_ADAPTER_DELAY_S = 4.0


def _git_repo(root: Path) -> Path:
    repo = root / "r"
    repo.mkdir()
    for cmd in (
        ["git", "init", "-q", "-b", "main", str(repo)],
        ["git", "-C", str(repo), "config", "user.email", "a@b.c"],
        ["git", "-C", str(repo), "config", "user.name", "a"],
    ):
        subprocess.run(cmd, check=True)
    (repo / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "seed"], check=True)
    return repo


def _idle_in_transaction_sessions(dsn: str) -> list[tuple[str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT application_name, left(query, 200)
              FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND state LIKE 'idle in transaction%%'
            """
        ).fetchall()
    return [(str(name), str(query)) for name, query in rows]


@pytest.fixture
def short_idle_bound(monkeypatch: pytest.MonkeyPatch):
    """Apply a 2 s idle-in-transaction bound to every session opened here."""
    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    monkeypatch.setenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, str(_IDLE_BOUND_MS))
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    conn_mod._MANIFEST_IDLE_TIMEOUT_CACHE.clear()
    close_all()  # pooled sessions opened before the override carry no bound
    yield
    close_all()
    conn_mod._MANIFEST_IDLE_TIMEOUT_CACHE.clear()


def test_second_full_run_with_slow_adapter_publishes(
    clean_runtime: str,
    short_idle_bound: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    repo = _git_repo(tmp_path)
    source = GitFilesSource(repo="r", repo_root=str(repo))
    runner = SourceRunner(
        clean_runtime, source_name="idle.gf", write_fact_log=True,
    )
    first = runner.run(source, mode="scope_complete")
    assert first.completed_scope is True
    assert first.nodes_appended > 0

    # The second run has a prior record-set proof, so the runner reads the
    # Chronos branch incarnation before it calls the adapter.
    (repo / "a.py").write_text("x = 2\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-qam", "edit"], check=True)

    held_during_adapter: list[tuple[str, str]] = []
    original_run = GitFilesSource.run

    @functools.wraps(original_run)
    def slow_run(self, *args, **kwargs):
        held_during_adapter.extend(_idle_in_transaction_sessions(clean_runtime))
        time.sleep(_ADAPTER_DELAY_S)
        return original_run(self, *args, **kwargs)

    monkeypatch.setattr(GitFilesSource, "run", slow_run)
    second = runner.run(
        GitFilesSource(repo="r", repo_root=str(repo)), mode="scope_complete",
    )

    assert held_during_adapter == [], (
        "a session was idle in transaction while the adapter ran: "
        f"{held_during_adapter}"
    )
    assert second.completed_scope is True
    assert second.nodes_appended > 0
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = graph.checkout().query(
            "SELECT run_id FROM okg.graph_nodes "
            "WHERE source = :source AND node_id = :node_id",
            {"source": "idle.gf", "node_id": "file:r:a.py"},
        )
    finally:
        graph.close()
    assert [row["run_id"] for row in rows] == [second.run_id], (
        "the second run's edit never reached the graph branch"
    )


def test_incarnation_read_leaves_a_caller_transaction_open(
    clean_runtime: str,
    short_idle_bound: None,
    tmp_path: Path,
) -> None:
    """The read ends only the transaction it opened, never a caller's."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    repo = _git_repo(tmp_path)
    runner = SourceRunner(
        clean_runtime, source_name="idle.gf", write_fact_log=True,
    )
    runner.run(GitFilesSource(repo="r", repo_root=str(repo)), mode="scope_complete")
    store = runner._chronos_graph_store()
    assert store is not None
    db = store.context.db

    assert runner._chronos_branch_incarnation()
    assert db.in_transaction is False

    # A caller that owns the transaction turns Chronos autocommit off, so
    # checkout does not commit it either.
    store.context.autocommit = False
    try:
        db.execute("SELECT 1").fetchone()
        assert db.in_transaction is True
        assert runner._chronos_branch_incarnation()
        assert db.in_transaction is True
    finally:
        db.rollback()
        store.context.autocommit = True
