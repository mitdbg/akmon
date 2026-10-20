"""DB-level backstop for the merge decider-boundary (worktree-fork-merge-back).

merge_guard refuses an unauthorized merge into default at the application layer, but
`store.context.merge_apply(fork, "default")` could bypass that guard by calling the chronos
primitive directly. This test proves the bypass is still blocked at the Postgres layer: a
fork holding only the least-privilege app_rw role CANNOT write default's chronos branch
tables (the public._chronos_branch_* tables merge_apply must INSERT/UPDATE), so a direct
merge_apply onto default is denied regardless of the app-layer guard.

The guard (app layer) and this grant boundary (DB layer) together make the decider an
enforced boundary, not merely advisory.
"""
from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres

# The public chronos tables merge_apply writes (INSERT/UPDATE) when landing rows onto a
# branch -- the writes a direct merge_apply(fork, "default") would perform.
_MERGE_WRITE_TABLES = (
    "_chronos_branch_interval_segments",
    "_chronos_branch_interval_branches",
)


def _app_rw_dsn(owner_dsn: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(owner_dsn)
    return "postgres://app_rw:okg_rw@{host}:{port}/{db}".format(
        host=info.get("host", "localhost"),
        port=info.get("port", "5432"),
        db=info["dbname"],
    )


def _ensure_app_rw(owner_dsn: str) -> None:
    """Create app_rw as the deployment initdb does (it is NOT created by migrate), then
    re-run migrate so views.sql's `FOR ROLE app_rw` default-privilege grants attach. Does
    NOT grant app_rw on schema public -- default's chronos tables stay postgres-owned."""
    from okg.substrate.db import migrate as _migrate_mod

    with psycopg.connect(owner_dsn, autocommit=True) as owner:
        with owner.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'app_rw'")
            if cur.fetchone() is None:
                cur.execute("CREATE ROLE app_rw LOGIN PASSWORD 'okg_rw'")

    original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        _migrate_mod.migrate(owner_dsn)
    finally:
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(original)


def _postgres_owned_chronos_tables(owner_dsn: str) -> set[str]:
    with psycopg.connect(owner_dsn, autocommit=True) as owner:
        with owner.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename LIKE '\\_chronos%' "
                "AND tableowner = 'postgres'"
            )
            return {r[0] for r in cur.fetchall()}


def test_app_rw_cannot_write_default_chronos_branch_tables(clean_runtime: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    # Materialize default's chronos branch tables (postgres-owned under clean_runtime).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:seed", "person", {}, {"id": "n:seed"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )

    _ensure_app_rw(clean_runtime)

    # Test-validity guard: the tables merge_apply writes exist and are postgres-owned
    # (NOT app_rw-owned), so the denial below is meaningful rather than vacuous.
    owned = _postgres_owned_chronos_tables(clean_runtime)
    present = [t for t in _MERGE_WRITE_TABLES if t in owned]
    assert present, f"expected postgres-owned merge-write chronos tables; owned={sorted(owned)}"

    with psycopg.connect(_app_rw_dsn(clean_runtime), autocommit=True) as rw:
        with rw.cursor() as cur:
            # Identity: we are genuinely app_rw, not the owner.
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == "app_rw"
            # Positive control: app_rw CAN read a table it is granted -> the connection is
            # alive and restricted, not broken (so the denial is about privilege).
            cur.execute("SELECT count(*) FROM okg.node_facts")
            cur.fetchone()

        # Core: app_rw cannot WRITE default's chronos branch tables -> a direct
        # merge_apply onto default is DB-denied even if it bypassed merge_guard.
        for table in present:
            with rw.cursor() as cur:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    cur.execute(f'DELETE FROM public."{table}" WHERE false')
                rw.rollback()
