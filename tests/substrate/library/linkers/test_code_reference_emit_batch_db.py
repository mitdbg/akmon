"""PostgreSQL proof for issue #2941: how the code-reference resolver writes
its edge facts.

* One INSERT per batch of edges, not one INSERT (and, on the publisher's
  autocommit connection, one transaction) per edge.
* All of one call's batches commit together or not at all.
* Every row carries the enrichment program's commit witness, so the runtime
  role (``app_rw``, neither superuser nor table owner) can write them. The
  publisher sets the witness with a transaction-local ``set_config`` on an
  autocommit connection, which is gone before the linker's first INSERT.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest
from psycopg import sql


pytestmark = pytest.mark.postgres

SOURCE = "_enricher:code_reference"


class _CountingCursor:
    """Pass every call through; count the INSERTs into okg.edge_facts."""

    def __init__(self, cur: psycopg.Cursor) -> None:
        self._cur = cur
        self.inserts = 0

    def execute(self, query: Any, params: Any = None) -> Any:
        if "INSERT INTO okg.edge_facts" in str(query):
            self.inserts += 1
        return self._cur.execute(query, params)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cur, name)


def _edges(count: int) -> list[tuple[str, str, str, dict[str, Any]]]:
    return [
        (
            f"reference:demo:a.py:f:{index}",
            "references",
            f"symbol:demo:b.py:g:{index}",
            {"linker": "code_reference", "resolution": "path_unique"},
        )
        for index in range(count)
    ]


def _written(dsn: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT src, edge_type, dst, attrs, provenance, confidence, "
            "source_record_id, source_revision, op, run_id, "
            "source_program_checksum "
            "FROM okg.edge_facts WHERE source = %s ORDER BY fact_id",
            (SOURCE,),
        ).fetchall()


def _emit(
    dsn: str,
    edges: list[tuple[str, str, str, dict[str, Any]]],
    *,
    publisher_witness: bool = False,
):
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )

    # The publisher runs enrichers on an autocommit connection.
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        if publisher_witness:
            # The publisher's own statement before each enricher
            # (Publisher._run_enrichers, "set_program_checksum").
            from okg.substrate.transform.programs import enrichment_program

            cur.execute(
                "SELECT set_config('okg.source_program_checksum', %s, true)",
                (enrichment_program().checksum(),),
            )
        counting = _CountingCursor(cur)
        result = CodeReferenceResolver()._emit_edges(
            counting, edges=edges, generation_id=0,
        )
    return result, counting.inserts


def test_emit_writes_edges_in_batches_in_sorted_order(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.linkers import code_reference
    from okg.substrate.transform.programs import enrichment_program

    monkeypatch.setattr(code_reference, "_EMIT_BATCH_ROWS", 2)
    edges = _edges(5)

    (emitted, skipped), inserts = _emit(clean_runtime, list(reversed(edges)))

    assert (emitted, skipped) == (5, 0)
    # 5 edges at 2 per batch: three statements, not five.
    assert inserts == 3
    rows = _written(clean_runtime)
    # Fact ids follow the resolver's sorted order, as they did per row.
    assert [(row[0], row[1], row[2]) for row in rows] == sorted(
        (src, edge_type, dst) for src, edge_type, dst, _attrs in edges
    )
    for row, (src, _edge_type, dst, attrs) in zip(rows, sorted(edges)):
        assert row[3] == attrs
        assert row[4] == "derived_deterministic"
        assert row[5] == pytest.approx(1.0)
        assert row[6] == {
            "src_node_id": src,
            "dst_node_id": dst,
            "linker": "code_reference",
        }
        assert row[7] == {"generation_id": 0}
        assert row[8] == "I"
        assert row[10] == enrichment_program().checksum()
    assert len({row[9] for row in rows}) == 1

    # Re-deriving the same edges writes nothing.
    (emitted, skipped), inserts = _emit(clean_runtime, edges)
    assert (emitted, skipped) == (0, 5)
    assert inserts == 0
    assert len(_written(clean_runtime)) == 5

    # A changed attribute is written again, in one statement.
    changed = [
        (src, edge_type, dst, {**attrs, "resolution": "import_symbol"})
        if index == 3 else (src, edge_type, dst, attrs)
        for index, (src, edge_type, dst, attrs) in enumerate(edges)
    ]
    (emitted, skipped), inserts = _emit(clean_runtime, changed)
    assert (emitted, skipped) == (1, 4)
    assert inserts == 1
    assert _written(clean_runtime)[-1][3]["resolution"] == "import_symbol"


def test_emit_failure_in_a_later_batch_writes_nothing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row the database refuses fails the whole call. The refused row sits
    in the second of three batches, so a batch-per-transaction write would
    have left the first batch committed; one INSERT per edge on an autocommit
    connection left every row sorted before it written."""
    from okg.substrate.library.linkers import code_reference

    monkeypatch.setattr(code_reference, "_EMIT_BATCH_ROWS", 2)
    edges = _edges(5)
    bad_src = sorted(edges)[3][0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            CREATE FUNCTION public.issue2941_refuse_row() RETURNS trigger
            LANGUAGE plpgsql AS $fn$
            BEGIN
              IF NEW.src = TG_ARGV[0] THEN
                RAISE EXCEPTION 'issue2941 refused %', NEW.src;
              END IF;
              RETURN NEW;
            END
            $fn$
            """
        )
        conn.execute(
            sql.SQL(
                "CREATE TRIGGER issue2941_refuse_row BEFORE INSERT ON "
                "okg.edge_facts FOR EACH ROW EXECUTE FUNCTION "
                "public.issue2941_refuse_row({})"
            ).format(sql.Literal(bad_src))
        )
    try:
        with pytest.raises(psycopg.errors.RaiseException, match="issue2941"):
            _emit(clean_runtime, edges)
        assert _written(clean_runtime) == []
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "DROP TRIGGER IF EXISTS issue2941_refuse_row ON okg.edge_facts"
            )
            conn.execute("DROP FUNCTION IF EXISTS public.issue2941_refuse_row()")


def _runtime_role_dsn(owner_dsn: str) -> str:
    """The runtime writer ``app_rw`` on the same database, created the way
    ops/pg/initdb/00-extensions.sql creates it. Migrate grants it INSERT on
    the fact tables through default privileges; a database migrated before
    the role existed is migrated again so the grant attaches."""
    from okg.substrate.db import migrate as _migrate_mod

    with psycopg.connect(owner_dsn, autocommit=True) as owner:
        if owner.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = 'app_rw'"
        ).fetchone() is None:
            owner.execute("CREATE ROLE app_rw LOGIN PASSWORD 'okg_rw'")
        granted = owner.execute(
            "SELECT has_table_privilege('app_rw', 'okg.edge_facts', 'INSERT')"
        ).fetchone()[0]
    if not granted:
        original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
        _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
        try:
            _migrate_mod.migrate(owner_dsn)
        finally:
            _migrate_mod.REQUIRED_EXTENSIONS.clear()
            _migrate_mod.REQUIRED_EXTENSIONS.update(original)
    info = psycopg.conninfo.conninfo_to_dict(owner_dsn)
    return "postgres://app_rw:okg_rw@{host}:{port}/{db}".format(
        host=info.get("host", "localhost"),
        port=info.get("port", "5432"),
        db=info["dbname"],
    )


def test_emit_under_runtime_role_carries_the_enrichment_witness(
    clean_runtime: str,
) -> None:
    """Issue #2941: under the runtime role, okg.enforce_commit_witness refuses
    a fact whose source_program_checksum is not in okg.ir_programs. The
    publisher ledgers the enrichment program and pins its checksum with
    set_config(..., true) on an autocommit connection, so the setting has
    ended before the linker writes. The linker's rows must still resolve."""
    from okg.substrate.transform.commit import persist_transform_program
    from okg.substrate.transform.programs import enrichment_program

    runtime_dsn = _runtime_role_dsn(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        # As the publisher does once per cycle, before any enricher runs.
        persist_transform_program(owner, enrichment_program())
    with psycopg.connect(runtime_dsn, autocommit=True) as rw:
        identity = rw.execute(
            "SELECT current_user, r.rolsuper, t.tableowner "
            "FROM pg_roles r, pg_tables t "
            "WHERE r.rolname = current_user AND t.schemaname = 'okg' "
            "AND t.tablename = 'edge_facts'"
        ).fetchone()
    # The trigger exempts a superuser and the table owner; this role is
    # neither, so the witness check below is real.
    assert identity[0] == "app_rw"
    assert identity[1] is False
    assert identity[2] != "app_rw"

    (emitted, skipped), _inserts = _emit(
        runtime_dsn, _edges(3), publisher_witness=True,
    )

    assert (emitted, skipped) == (3, 0)
    rows = _written(clean_runtime)
    assert len(rows) == 3
    assert {row[10] for row in rows} == {enrichment_program().checksum()}
