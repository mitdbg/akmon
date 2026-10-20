"""The capability probe against a real PostgreSQL graph (#3151 slice 1).

PACT `prompt-compiler-3151-slice1`. A real migrate, a real publish, and a real
server start with no embedder: the published `search` schema lacks `semantic`
and `alias`, `select` lists its fields, the refusals name the method before
any query, and `inspect(kind="surface")` reports the probe. Then the same
catalog version gains an alias and a passage rollup, and a restarted server
publishes both: the probe reads the database, it does not assume.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.capability_probe import NOT_AVAILABLE_HEADER
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate.mcp._result_payload import call_json, payload

pytestmark = [pytest.mark.anyio, pytest.mark.postgres]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _seed_and_publish(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
                "VALUES ('page', '{}'::jsonb, %s)",
                (cv,),
            )
            cur.execute(
                "INSERT INTO okg.subtype_ancestors "
                "(catalog_version_id, subtype_name, ancestor_name, depth) "
                "VALUES (%s, 'page', 'page', 0) ON CONFLICT DO NOTHING",
                (cv,),
            )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:a", "page", {"text": "alpha"}, {"id": "n:a"}, {"v": 1}),
                NodeFact("n:b", "page", {"text": "beta"}, {"id": "n:b"}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    # The catalog version the published generation pins, which is what the
    # probe reads (the test catalog writer may apply its own version).
    from okg.substrate.mcp.session import resolve_generation

    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = resolve_generation(conn, "default")[1]
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.aliases WHERE catalog_version_id = %s", (cv,),
        )
        assert cur.fetchone()[0] == 0, "the seeded catalog must start alias-free"
    return int(cv)


def _close(mcp) -> None:
    mcp._okg_pool.close()
    write_pool = getattr(mcp, "_okg_write_pool", None)
    if write_pool is not None:
        write_pool.close()


async def _search_schema(mcp) -> dict:
    tool = next(t for t in await mcp.list_tools() if t.name == "search")
    return tool.inputSchema["properties"]


async def test_a_bare_graph_publishes_only_what_it_can_run(clean_runtime: str) -> None:
    cv = _seed_and_publish(clean_runtime)
    mcp = build_server(dsn=clean_runtime, allow_writes=True)
    try:
        props = await _search_schema(mcp)
        # The lean read surface (#3666, M10) shows lexical and grep in words
        # and hides rank_by; what the probe lets the server run is in the
        # surface report, and the guard serves exactly that.
        report = mcp._okg_surface_freshness["capabilities"]["search"]
        assert report["published_methods"] == ["lexical", "identifier_mentions", "attrs", "grep"]
        assert report["published_rank_by"] == ["records", "self"]
        assert props["method"]["description"] == "lexical or grep."
        assert "rank_by" not in props
        # The select fields in words, no items enum (#3666).
        assert "excerpt" in props["select"]["description"]
        assert "excerpt" in mcp._okg_capabilities.published_select_fields()

        caps = mcp._okg_capabilities
        assert caps.mode == "live"
        assert caps.pinned["catalog_version_id"] == cv
        assert caps.reads == ("pin", "aliases", "passage_rollups")
        assert f"catalog version {cv}" in caps.method("alias").reason

        surface = payload(await call_json(
            mcp, "inspect", {"target": {"kind": "surface"}},
        ))
        assert surface["capabilities"]["mode"] == "live"
        assert surface["capabilities"]["search"]["methods"]["alias"]["status"] == (
            "unavailable"
        )
        assert surface["capabilities"]["probe_ms"] is not None
        assert mcp.instructions.endswith(
            f'- search rank_by "passages": no subtype declares a passage '
            f"rollup at catalog version {cv}."
        )

        # Before ANY database work: with the pools closed, a call that reached
        # the database would fail on the pool, not with the named refusal.
        _close(mcp)
        alias = payload(await call_json(
            mcp, "search", {"query": "alpha", "method": "alias"},
        ))
        assert alias["error"]["code"] == "search_method_unavailable"
        semantic = payload(await call_json(
            mcp, "search", {"query": "alpha", "method": "semantic"},
        ))
        assert semantic["error"]["code"] == "semantic_search_unavailable"
        assert semantic["error"]["capability"]["reason"] == (
            "no embedder is configured"
        )
    finally:
        _close(mcp)


async def test_a_restart_after_aliases_and_a_rollup_publishes_both(
    clean_runtime: str,
) -> None:
    cv = _seed_and_publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'okg' AND table_name = 'aliases' "
            "AND is_nullable = 'NO' AND column_default IS NULL",
        )
        required = {row[0] for row in cur.fetchall()}
        values = {
            "catalog_version_id": cv,
            "alias": "a",
            "normalized_alias": "a",
            "canonical": "n:a",
            "scope": "global",
        }
        missing = required - set(values)
        assert not missing, f"okg.aliases needs columns this test omits: {missing}"
        columns = sorted(values)
        cur.execute(
            f"INSERT INTO okg.aliases ({', '.join(columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))})",
            [values[c] for c in columns],
        )
        cur.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb) "
            "|| '{\"passage_rollup\": {\"edge_type\": \"links\", "
            "\"child_subtype\": \"page\"}}'::jsonb "
            "WHERE catalog_version_id = %s AND name = (SELECT min(name) "
            "FROM okg.subtypes WHERE catalog_version_id = %s) RETURNING name",
            (cv, cv),
        )
        rollup_subtype = cur.fetchone()[0]
    mcp = build_server(dsn=clean_runtime)
    try:
        await _search_schema(mcp)
        report = mcp._okg_surface_freshness["capabilities"]["search"]
        assert report["published_methods"] == [
            "lexical", "alias", "identifier_mentions", "attrs", "grep",
        ]
        assert report["published_rank_by"] == ["records", "self", "passages"]
        assert mcp._okg_capabilities.rollup_subtypes == (rollup_subtype,)
        # Still no embedder, but semantic is a method the published schema
        # hides, so no generated block is served (prompt redesign, #3666).
        assert "semantic" not in mcp.instructions
        assert NOT_AVAILABLE_HEADER not in mcp.instructions
    finally:
        _close(mcp)


def _probe_while_locked(dsn: str, lock_sql: str):
    """Run the probe while another session holds `lock_sql`'s locks, so the
    probe's read waits until its statement timeout cancels it."""
    from okg.substrate.mcp.capability_probe import probe_capabilities

    with psycopg.connect(dsn) as holder:
        with holder.cursor() as cur:
            cur.execute(lock_sql)
        with psycopg.connect(dsn, autocommit=True) as conn:
            caps = probe_capabilities(
                conn, deployment_name="default", embedder_configured=False,
                timeout_ms=300,
            )
            # The session-level timeout the pin read sets is reset after it.
            with conn.cursor() as cur:
                cur.execute("SHOW statement_timeout")
                after = cur.fetchone()[0]
        holder.rollback()
    return caps, after


def test_a_locked_alias_table_times_out_to_unknown(clean_runtime: str) -> None:
    """Decision 2 (b) on a real database: a read that waits past its
    statement timeout publishes the capability as unknown and the probe
    goes on to the next read."""
    _seed_and_publish(clean_runtime)
    caps, after = _probe_while_locked(
        clean_runtime, "LOCK TABLE okg.aliases IN ACCESS EXCLUSIVE MODE",
    )
    assert caps.method("alias").status == "unknown"
    assert "timed out after 300 ms" in caps.method("alias").reason
    assert caps.rank_by_value("passages").status == "unavailable"
    assert caps.reads == ("pin", "aliases", "passage_rollups")
    assert after == "0"


def test_a_locked_pin_times_out_to_unknown_not_a_refusal(clean_runtime: str) -> None:
    """The pin read on a real database. Its resolver swallows a failed fast
    path and falls back to a second query; a timeout must still read as a
    timeout (unknown), never as an aborted-transaction error that refuses
    the start."""
    _seed_and_publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT quote_ident(schemaname) || '.' || quote_ident(tablename) "
            "FROM pg_tables WHERE tablename LIKE '\\_chronos%' "
            "ORDER BY 1"
        )
        tables = [row[0] for row in cur.fetchall()]
    assert tables, "expected the Chronos checkpoint tables"
    caps, after = _probe_while_locked(
        clean_runtime,
        f"LOCK TABLE {', '.join(tables)} IN ACCESS EXCLUSIVE MODE",
    )
    assert caps.pinned is None
    assert caps.method("alias").status == "unknown"
    assert "pin read timed out" in caps.method("alias").reason
    assert after == "0"
