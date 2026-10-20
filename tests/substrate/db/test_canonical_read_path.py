"""Acceptance tests for `schema-canonical-read-path`.

The change these pin: views.sql and schema.sql now contain the read path that
is actually installed. Before it, the canonical SQL files described the
pre-Chronos live-table architecture and `db/management.py` spliced the real
Chronos-native definitions over them at migrate time — so an OSS reader of
views.sql learned a read path that served zero rows on every supported
deployment, and the definitions that DID ship were never reviewed as SQL.

Three requirements, and the shape of the evidence for each:

  req.views-real        views.sql defines the graph read path, no Python stub
                        generation survives, no promoted view exposes a column
                        that can never carry a value, and the pin carries the
                        TEXT checkpoint ref (D2).
  req.history-contract  D6 option (b): v_nodes_history / v_edges_history /
                        v_changes are absent from a fresh install and from the
                        grant surface.
  req.legacy-quarantine D6's sibling: the nodes_live/edges_live complex —
                        tables, indexes, GiST exclusions, triggers and the
                        projectable_* views — is gone from schema.sql, and a
                        fresh install has none of it.

A note on how the graph family is read back, because the obvious way is wrong.
`okg.v_nodes` cannot be read bare: okg.graph_nodes is a Chronos checkout
surface that is never materialized, so okg.require_graph_checkout raises rather
than reporting an empty graph (issue #644). Nor does a raw Chronos checkout
help by itself — it rewrites registered table names in the SUBMITTED SQL and
cannot see through a Postgres view. The production read path
(`okg.substrate.mcp.run_sql`) is what closes that gap: it inlines composite
okg.v_* views as subqueries, maps the base v_nodes/v_edges onto the physical
graph tables, and executes the result inside the checkout. So the read-back
test drives run_sql — the real path — rather than a psql session that could
only ever prove the guard fires.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import psycopg
import pytest
from psycopg import errors

from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

REPO_ROOT = Path(__file__).resolve().parents[3]
DB_DIR = REPO_ROOT / "src" / "okg" / "substrate" / "db"
VIEWS_SQL = DB_DIR / "views.sql"
SCHEMA_SQL = DB_DIR / "schema.sql"
MANAGEMENT_PY = DB_DIR / "management.py"

#: Retired by the schema design rules in docs/reference/database-schema.md.
#: Each was a view a reader could ask a
#: question that it structurally could not answer.
RETIRED_VIEWS = (
    "okg.v_nodes_history",
    "okg.v_edges_history",
    "okg.v_changes",
    "okg.projectable_node_facts",
    "okg.projectable_edge_facts",
)

LEGACY_LIVE_TABLES = ("okg.nodes_live", "okg.edges_live")


def _sql_without_line_comments(path: Path) -> str:
    return "\n".join(
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )


# --------------------------------------------------------------------------- #
# req.views-real — the canonical file is the read path                        #
# --------------------------------------------------------------------------- #


def test_views_sql_defines_the_chronos_graph_read_path() -> None:
    """views.sql, not Python, defines v_nodes / v_edges."""
    views = _sql_without_line_comments(VIEWS_SQL)

    assert "CREATE OR REPLACE FUNCTION okg.require_graph_checkout(" in views
    for view in ("okg.v_nodes", "okg.v_edges"):
        start = views.index(f"CREATE OR REPLACE VIEW {view} AS")
        body = views[start : views.index(";", start)]
        physical = "okg.graph_nodes" if view == "okg.v_nodes" else "okg.graph_edges"
        assert f"FROM {physical}" in body, f"{view} must read {physical}"
        assert f"okg.require_graph_checkout('{physical}')" in body, (
            f"{view} must carry the honest-read guard arm"
        )
        # The constant-NULL contract columns the Python stubs projected.
        assert "valid_from_gen" not in body, (
            f"{view} must not re-freeze a constant-NULL generation column"
        )
        assert "source_program_checksum" not in body


def test_management_py_generates_no_views() -> None:
    """The stub-generation path is deleted, not merely unused.

    Grepping management.py finds no v_nodes/v_edges CREATE (the acceptance
    criterion's own wording) and none of the rewriters that spliced them in.

    Comment lines are stripped first: the module keeps a note NAMING what was
    removed and why, which is the record a future reader needs, and a raw grep
    would read that note as the thing itself.
    """
    source = "\n".join(
        line
        for line in MANAGEMENT_PY.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )

    for view in ("okg.v_nodes", "okg.v_edges", "okg.v_generations"):
        assert f"CREATE OR REPLACE VIEW {view}" not in source, (
            f"management.py still generates {view}"
        )
    for symbol in (
        "_CHRONOS_GRAPH_READ_VIEW_SQL",
        "def views_without_legacy_graph_storage",
        "def schema_without_legacy_graph_storage",
        "def schema_without_legacy_live_table_compatibility",
    ):
        assert symbol not in source, f"management.py still defines {symbol}"


def test_pin_accessor_carries_the_text_checkpoint_ref(okg_migrated: str) -> None:
    """D2: okg.current_pin() returns the TEXT ref, not a bigint ordinal.

    The function is EXECUTE-granted to the read roles, which puts it in the
    freeze surface, which is why its bigint return was a registered identity
    divergence owned by this change. Retiring the divergence means DELETING its
    row from identity_divergences.yaml — asserted below, so the register cannot
    decay into a standing allowlist.
    """
    import yaml

    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_get_function_result(p.oid)
              FROM pg_proc p
              JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg' AND p.proname = 'current_pin'
            """
        )
        assert cur.fetchone()[0] == "text"

        # A real Chronos checkpoint ref, not a stringified ordinal: the point
        # of D2 is that the pin carries an identity the graph store minted,
        # and the old bigint signature could not hold one. `set_config`
        # rather than `SET LOCAL` because the latter is a utility statement
        # and takes no bound parameter.
        ref = "okg:test:branch:default:gen:20260811T000000000000Z:abcdef123456"
        cur.execute("SELECT set_config('okg.pin_generation', %s, true)", (ref,))
        cur.execute("SELECT okg.current_pin()")
        assert cur.fetchone()[0] == ref

    registry = yaml.safe_load(
        (DB_DIR / "identity_divergences.yaml").read_text(encoding="utf-8")
    )
    assert registry.get("generation_functions") in (None, [], {}), (
        "okg.current_pin's divergence row must be deleted, not edited: the "
        "identity gate reds on a registered divergence that no longer exists"
    )


def test_unset_pin_raises_rather_than_defaulting(okg_migrated: str) -> None:
    """No silent fallback. An unset pin is an error, never generation zero."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute("SELECT set_config('okg.pin_generation', '', true)")
        with pytest.raises(errors.UndefinedParameter) as raised:
            cur.execute("SELECT okg.current_pin()")
    assert "okg.pin_generation GUC not set" in str(raised.value)


# --------------------------------------------------------------------------- #
# req.views-real — the honest-read guard, per view                            #
# --------------------------------------------------------------------------- #


def _granted_graph_reading_views(cur: psycopg.Cursor) -> list[str]:
    """Granted okg.v_* views whose dependency closure reaches the graph tables."""
    cur.execute(
        """
        WITH RECURSIVE granted AS (
          SELECT c.oid, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'okg'
             AND c.relkind = 'v'
             AND has_table_privilege('app_ro', c.oid, 'SELECT')
        ),
        closure AS (
          SELECT oid AS root, oid AS reached FROM granted
          UNION
          SELECT cl.root, d.refobjid
            FROM closure cl
            JOIN pg_rewrite r ON r.ev_class = cl.reached
            JOIN pg_depend d ON d.objid = r.oid
                            AND d.classid = 'pg_rewrite'::regclass
                            AND d.refclassid = 'pg_class'::regclass
           WHERE d.refobjid <> cl.reached
        )
        SELECT DISTINCT g.relname
          FROM closure cl
          JOIN granted g ON g.oid = cl.root
          JOIN pg_class t ON t.oid = cl.reached
          JOIN pg_namespace tn ON tn.oid = t.relnamespace
         WHERE tn.nspname = 'okg'
           AND t.relname IN ('graph_nodes', 'graph_edges')
         ORDER BY 1
        """
    )
    return [str(row[0]) for row in cur.fetchall()]


def test_every_granted_graph_reading_view_carries_the_guard(
    okg_migrated: str,
) -> None:
    """The guard-gap this change closed, as a structural property.

    `okg.require_graph_checkout` was attached to v_nodes and v_edges only.
    `okg.v_branch_layered_nodes` / `v_branch_layered_edges` also read the graph
    tables and are ALSO granted — the `okg.v_*` sweep at the end of views.sql
    grants whatever exists — so a bare read of them returned the confident 0
    the guard exists to prevent.

    Written over the dependency CLOSURE rather than a list of names, so a view
    added later inherits the requirement instead of quietly opting out of it.
    The closure is also why `okg.v_branch_layered_nodes` passes without naming
    the guard in its own body: it is defined over `okg.v_nodes`, which is the
    point — the arm cannot be forgotten because it is not copied.
    """
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        views = _granted_graph_reading_views(cur)
        assert set(views) >= {
            "v_nodes",
            "v_edges",
            "v_branch_layered_nodes",
            "v_branch_layered_edges",
        }, f"expected the graph-reading family in the granted surface, got {views}"

        unguarded = [
            view
            for view in views
            if not _reaches_guard(cur, view)
        ]

    assert not unguarded, (
        "these granted views reach the Chronos graph tables without reaching "
        "okg.require_graph_checkout, so a bare read of them reports an empty "
        "graph on a healthy deployment: " + ", ".join(unguarded)
    )


def _reaches_guard(cur: psycopg.Cursor, view: str) -> bool:
    """Does this view consult the guard, directly or through another view?"""
    seen: set[str] = set()
    pending = [view]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        cur.execute("SELECT pg_get_viewdef(%s::regclass)", (f"okg.{current}",))
        if "require_graph_checkout" in str(cur.fetchone()[0]):
            return True
        cur.execute(
            """
            SELECT c.relname
              FROM pg_rewrite r
              JOIN pg_depend d ON d.objid = r.oid
                              AND d.classid = 'pg_rewrite'::regclass
                              AND d.refclassid = 'pg_class'::regclass
              JOIN pg_class c ON c.oid = d.refobjid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE r.ev_class = %s::regclass
               AND c.relkind = 'v'
               AND n.nspname = 'okg'
               AND c.relname <> %s
            """,
            (f"okg.{current}", current),
        )
        pending.extend(str(row[0]) for row in cur.fetchall())
    return False


#: Views whose bare read the guard actually REACHES at runtime.
#:
#: The guard rides a constant UNION ALL arm, so it is evaluated only when the
#: planner keeps that arm. A view that filters — `WHERE n.subtype = 'document'`
#: in v_documents, and the same in the chunk / table-cell / agent family —
#: constant-folds the all-NULL arm away, and its bare read returns 0 silently.
#: That limitation is the guard mechanism's, documented at its definition in
#: views.sql, and predates this change; it is recorded here as a named gap
#: rather than papered over by omitting the negative test.
_GUARD_REACHABLE_VIEWS = (
    "v_nodes",
    "v_edges",
    "v_branch_layered_nodes",
    "v_branch_layered_edges",
)


@pytest.mark.parametrize("view", _GUARD_REACHABLE_VIEWS)
def test_bare_read_of_a_graph_view_raises(okg_migrated: str, view: str) -> None:
    """Per-view negative test: a bare read refuses instead of reporting empty.

    A bare aggregate is the query shape used deliberately — it is the
    diagnosis query an operator actually types when asking "is the graph
    empty?", and it is the shape that always evaluates the guard arm.
    """
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        with pytest.raises(errors.ObjectNotInPrerequisiteState) as raised:
            cur.execute(f"SELECT count(*) FROM okg.{view}")  # noqa: S608
    assert "requires a Chronos checkout" in str(raised.value)


# --------------------------------------------------------------------------- #
# req.history-contract — D6 option (b), retired                               #
# --------------------------------------------------------------------------- #


def test_retired_views_are_absent_from_views_sql() -> None:
    views = _sql_without_line_comments(VIEWS_SQL)
    for name in RETIRED_VIEWS:
        assert f"CREATE OR REPLACE VIEW {name}" not in views, f"{name} still authored"
        assert f"VIEW {name} AS" not in views
        assert f"GRANT SELECT ON {name}" not in views, f"{name} still granted"


def test_retired_views_are_absent_from_a_fresh_install(okg_migrated: str) -> None:
    """D6 acceptance: absent from the install AND from the grant surface."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        for name in RETIRED_VIEWS:
            cur.execute("SELECT to_regclass(%s)", (name,))
            assert cur.fetchone()[0] is None, f"{name} is still installed"

        # The grant sweep grants every okg.v_* view that EXISTS, so absence
        # from the schema is what keeps them out of the grant surface. Assert
        # the consequence directly rather than trusting the mechanism.
        cur.execute(
            """
            SELECT c.relname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND has_table_privilege('app_ro', c.oid, 'SELECT')
               AND c.relname = ANY(%s)
            """,
            ([name.split(".", 1)[1] for name in RETIRED_VIEWS],),
        )
        assert cur.fetchall() == []


def test_retirement_decision_is_recorded_in_the_code() -> None:
    """The decision is cited where a reader of the schema will meet it.

    D6 was an explicit ledger decision, not a default. A retirement whose
    reason lives only in a design document is one a future contributor
    re-litigates by re-adding the view.
    """
    views = VIEWS_SQL.read_text(encoding="utf-8")
    migration = (DB_DIR / "migrations" / "0024_canonical_read_path_view_cleanup.sql")

    assert "D6" in views
    assert "database-schema.md" in views
    # An applied migration is immutable, so it keeps the design-document path
    # it cited when it shipped; the docs later moved that section to
    # docs/reference/database-schema.md, which views.sql cites.
    migration_text = migration.read_text(encoding="utf-8")
    assert "D6" in migration_text
    assert "db-schema-v1-design.md" in migration_text


# --------------------------------------------------------------------------- #
# req.legacy-quarantine — the live complex is deleted                         #
# --------------------------------------------------------------------------- #


def test_schema_sql_has_no_legacy_live_complex() -> None:
    schema = _sql_without_line_comments(SCHEMA_SQL)
    for table in LEGACY_LIVE_TABLES:
        assert table not in schema, f"schema.sql still installs {table}"
    assert "nodes_live" not in schema
    assert "edges_live" not in schema


def test_fresh_install_has_no_legacy_live_relations(okg_migrated: str) -> None:
    """Tables, their ~14 indexes, both GiST exclusions, and the triggers."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        for table in LEGACY_LIVE_TABLES:
            cur.execute("SELECT to_regclass(%s)", (table,))
            assert cur.fetchone()[0] is None, f"{table} exists on a fresh install"

        cur.execute(
            """
            SELECT c.relname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind = 'i'
               AND (c.relname LIKE 'nodes\\_live%' OR c.relname LIKE 'edges\\_live%')
            """
        )
        assert cur.fetchall() == [], "legacy live-table indexes survive"

        cur.execute(
            """
            SELECT conname
              FROM pg_constraint
             WHERE connamespace = 'okg'::regnamespace
               AND conname IN (
                 'nodes_live_no_validity_overlap_excl',
                 'edges_live_no_validity_overlap_excl',
                 'nodes_live_valid_interval_chk',
                 'edges_live_valid_interval_chk'
               )
            """
        )
        assert cur.fetchall() == [], "legacy GiST exclusions / CHECKs survive"

        cur.execute(
            """
            SELECT tgname
              FROM pg_trigger
             WHERE NOT tgisinternal
               AND tgname IN (
                 'nodes_live_fill_branch_id_tg',
                 'edges_live_fill_branch_id_tg',
                 'edges_live_endpoint_check',
                 'edges_live_narrowing_check',
                 'edge_facts_narrowing_check'
               )
            """
        )
        assert cur.fetchall() == [], "legacy live-table triggers survive"


# --------------------------------------------------------------------------- #
# The read-back: publish a generation, read every promoted view               #
# --------------------------------------------------------------------------- #


_FIXTURE = {
    "nodes": [
        {
            "node_id": "doc:canonical",
            "subtype": "document",
            "attrs": {
                "path": "canonical.md",
                "repo": "canonical-read-path",
                "format": "md",
                "title": "Canonical read path",
                "content_sha256": "sha256:doc",
                "byte_count": "42",
                "text": "canonical read path document",
            },
        },
        {
            "node_id": "chunk:canonical",
            "subtype": "document_chunk",
            "attrs": {
                "text": "the canonical read path serves rows",
                "heading_path": "Intro",
                "char_offset": "0",
                "char_length": "35",
                "char_end": "35",
                "chunk_kind": "prose",
                "chunker_name": "test",
                "content_sha256": "sha256:chunk",
            },
        },
        # A parsed table occurrence, so okg.v_table_cells has something to
        # project. Without it that view returns zero rows for a reason that
        # has nothing to do with the read path, and "every promoted view
        # returns rows" would be satisfied only by not asking.
        {
            "node_id": "chunk:canonical-table",
            "subtype": "document_chunk",
            "attrs": {
                "text": "| metric | value |",
                "heading_path": "Results",
                "char_offset": "40",
                "char_length": "18",
                "char_end": "58",
                "chunk_kind": "table",
                "chunker_name": "test",
                "content_sha256": "sha256:table",
                "table_grid_status": "parsed",
                "table_extraction_lane": "test-lane",
                "source_page_index": "1",
                "source_content_sha256": "sha256:table-src",
                "cache_signature": "sig:table",
                "source_bbox": [0, 0, 10, 10],
                "table_grid": {
                    "rows": [
                        {
                            "row_index": 0,
                            "row_label": "throughput",
                            "cells": [
                                {
                                    "column_index": 0,
                                    "column_header": "metric",
                                    "text": "throughput",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                                {
                                    "column_index": 1,
                                    "column_header": "value",
                                    "text": "42",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                            ],
                        }
                    ]
                },
            },
        },
        # A table occurrence whose grid FAILED to parse, so the error lane of
        # okg.v_doc_chunks (table_grid_error_code) carries a value too. A
        # per-column sweep over a corpus that only exercises the happy path
        # reports the error columns as constant-NULL and cannot tell that
        # apart from a column which never carries a value.
        {
            "node_id": "chunk:canonical-table-error",
            "subtype": "document_chunk",
            "attrs": {
                "text": "| broken",
                "heading_path": "Results",
                "char_offset": "60",
                "char_length": "8",
                "char_end": "68",
                "chunk_kind": "table",
                "chunker_name": "test",
                "content_sha256": "sha256:table-error",
                "table_grid_status": "error",
                "table_extraction_lane": "test-lane",
                "table_grid_error_code": "grid_parse_failed",
            },
        },
    ],
    "edges": [
        # okg.v_doc_chunks.chunk_ordinal reads the member_of edge's ordinal.
        {
            "src": "chunk:canonical",
            "dst": "doc:canonical",
            "edge_type": "member_of",
            "attrs": {"chunk_ordinal": 0},
        },
        {"src": "doc:canonical", "dst": "chunk:canonical", "edge_type": "contains"},
        {
            "src": "chunk:canonical-table",
            "dst": "doc:canonical",
            "edge_type": "member_of",
        },
        {
            "src": "doc:canonical",
            "dst": "chunk:canonical-table",
            "edge_type": "contains",
        },
        {
            "src": "chunk:canonical-table-error",
            "dst": "doc:canonical",
            "edge_type": "member_of",
        },
        {
            "src": "doc:canonical",
            "dst": "chunk:canonical-table-error",
            "edge_type": "contains",
        },
    ],
}


def _publish_fixture(dsn: str, tmp_path: Path) -> dict:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.docsite import DocsiteSource

    fixture = tmp_path / "canonical_read_path.json"
    fixture.write_text(json.dumps(_FIXTURE))
    SourceRunner(dsn, source_name="canonical_read_path").run(
        DocsiteSource(fixture_path=str(fixture)), mode="scope_complete"
    )
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out

    # okg.v_subtypes LEFT JOINs the catalog for per-subtype metadata, so
    # `catalog_metadata` is NULL exactly when the applied catalog does not
    # declare the subtype. Whether it does depends on which ontology this
    # deployment applied — which makes the column's NULL-ness a property of
    # the fixture's ORDER rather than of the schema, and an ordering-dependent
    # column is the one thing a per-column sweep must not have. Declare the
    # two subtypes the fixture publishes, so the sweep measures the view.
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            SELECT
              subtype,
              jsonb_build_object('archetype', 'document_like'),
              (SELECT max(catalog_version_id) FROM okg.catalog_versions
                WHERE status = 'applied')
            FROM unnest(ARRAY['document', 'document_chunk']) AS subtype
            ON CONFLICT (name, catalog_version_id) DO UPDATE
              SET metadata = EXCLUDED.metadata
            """
        )
    return out


#: Granted graph-reading views this fixture cannot populate, each with the
#: reason. NOT a convenience list: the probe set below is derived from the
#: dependency closure so a view added later is swept automatically, and this
#: is the only way to opt one out. Entries are asserted to still be empty, so
#: the list cannot decay into a standing allowlist.
_UNPOPULATED_BY_FIXTURE: dict[str, str] = {
    "okg.v_agent_sessions": (
        "agent transcript surface; populated by the sessions source, not by "
        "the docsite fixture this test publishes"
    ),
    "okg.v_agent_messages": "same source as okg.v_agent_sessions",
    "okg.v_tool_use_events": "same source as okg.v_agent_sessions",
}

#: The column whose presence proves a row came from the published generation
#: rather than from a placeholder. Views not listed are probed for rows and
#: swept for constant NULLs, just without an identity-column assertion.
_GRAPH_VIEW_PROBES = {
    "okg.v_nodes": "node_id",
    "okg.v_edges": "edge_id",
    "okg.v_branch_layered_nodes": "node_id",
    "okg.v_branch_layered_edges": "edge_id",
    "okg.v_documents": "doc_id",
    "okg.v_doc_chunks": "chunk_id",
    "okg.v_chunk_sequence": "chunk_id",
    "okg.v_table_cells": "cell_key",
    "okg.v_subtypes": "subtype",
}

#: Columns the sweep finds all-NULL for a reason that lives in the FIXTURE
#: rather than in the schema — a lane this test's published corpus does not
#: drive. Each needs a reason, and each is asserted to STILL be all-NULL, so
#: the list cannot decay into a standing allowlist that quietly covers a real
#: constant-NULL column later.
#:
#: Empty, and it should stay that way: the fixture drives every lane the
#: promoted views project, including the table-grid error path and the
#: per-subtype catalog metadata. Prefer enriching the fixture over adding an
#: entry here — an entry is a column this test stops checking.
_UNEXERCISED_COLUMNS: dict[str, str] = {}


def test_published_generation_reads_back_through_every_promoted_view(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The integration acceptance: SELECTs return the published generation's
    rows using only the views the canonical files install.

    No Python stub installation and no zero-row placeholder is involved —
    `migrate` executed views.sql as written, which is the whole point of the
    change. Each view is read through `run_sql`, the production path: it
    expands composite okg.v_* views into subqueries, maps the base graph views
    onto the physical tables, and runs the result inside a Chronos checkout
    pinned to the session's generation.
    """
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.run_sql import run_sql

    published = _publish_fixture(clean_runtime, tmp_path)

    empty: list[str] = []
    all_null: list[str] = []
    observed_all_null: set[str] = set()
    unexpectedly_populated: list[str] = []
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            # DERIVED, not listed. The set is every granted view whose
            # dependency closure reaches the graph tables, so a view added
            # later is swept without anyone remembering to add it — the same
            # reasoning as the guard test above. A hand-written probe list had
            # silently omitted five granted graph views.
            #
            # Intersected with the views VIEWS.SQL DECLARES, because the
            # closure is over the live catalog and the catalog is not only
            # ours: a deployment's own views.sql, an operator's view, or a
            # stray `okg.v_*` left behind by another test in the same session
            # all land in it. Sweeping those asserts that someone else's view
            # returns rows under OUR fixture, which is not a claim this test
            # can make. Declared-and-granted-and-graph-reading is the surface
            # this change owns.
            substrate_views = _views_declared_in_views_sql()
            targets = [
                f"okg.{name}"
                for name in _granted_graph_reading_views(cur)
                if name in substrate_views
            ]
            assert len(targets) >= 8, (
                "the derived probe set collapsed; it should cover the whole "
                f"graph-reading family, got {targets}"
            )
            declared = {view: _declared_columns(cur, view) for view in targets}
        session = open_session(conn, deployment_name="default")
        assert session.generation_id == published["generation_id"]

        for view in targets:
            columns = declared[view]
            probe = _GRAPH_VIEW_PROBES.get(view)
            if probe is not None:
                assert probe in columns, f"{view} lost its {probe} column: {columns}"
            # The VIEW's declared columns, never `SELECT *`. run_sql maps the
            # base graph views onto okg.graph_nodes / okg.graph_edges BY NAME,
            # so `SELECT * FROM okg.v_nodes` expands to the PHYSICAL table's
            # column list (embedding, source_record_id, ...) and would sweep
            # columns the view does not expose — measuring the wrong contract
            # and, worse, passing a test of okg.v_nodes that okg.v_nodes never
            # took part in.
            projection = ", ".join(f'"{column}"' for column in columns)
            payload = run_sql(conn, session, f"SELECT {projection} FROM {view}")  # noqa: S608
            rows = payload["rows"]
            if view in _UNPOPULATED_BY_FIXTURE:
                # Opted out of the row requirement, with a reason. Assert it is
                # STILL empty, so the opt-out cannot outlive the gap it names.
                if rows:
                    unexpectedly_populated.append(view)
                continue
            if not rows:
                empty.append(f"{view}: returned no rows at the published pin")
                continue
            # Per-column sweep: a column NULL across every returned row is a
            # contract the deployment cannot honour. This is the automated
            # check req.views-real asks for, run where it is meaningful — on a
            # deployment that has published.
            for index, column in enumerate(payload["columns"]):
                if all(row[index] is None for row in rows):
                    qualified = f"{view}.{column}"
                    observed_all_null.add(qualified)
                    if qualified not in _UNEXERCISED_COLUMNS:
                        all_null.append(qualified)

    assert not empty, "\n  ".join(["promoted views served nothing:", *empty])
    assert not all_null, (
        "these promoted-view columns are NULL in every row the published "
        "generation returns; a frozen column that never carries a value is "
        "not a contract:\n  " + "\n  ".join(all_null)
    )
    # The allowance list cannot decay into a standing allowlist: an entry that
    # no longer describes an all-NULL column must be DELETED, which is how a
    # richer fixture (or a retired column) proves itself.
    stale = sorted(set(_UNEXERCISED_COLUMNS) - observed_all_null)
    assert not stale, (
        "these columns are no longer all-NULL, so their entries in "
        "_UNEXERCISED_COLUMNS are stale and must be removed: "
        + ", ".join(stale)
    )
    assert not unexpectedly_populated, (
        "these views now return rows under this fixture, so their entries in "
        "_UNPOPULATED_BY_FIXTURE are stale — delete them and let the sweep "
        "cover them: " + ", ".join(unexpectedly_populated)
    )


def _views_declared_in_views_sql() -> set[str]:
    """Unqualified names of the `okg.*` views views.sql declares.

    The same line-anchored form `okg.substrate.db.view_convergence` uses to
    decide which views it owns, for the same reason: a declaration starts its
    own line, while prose about one lives behind a `--`.
    """
    return set(
        re.findall(
            r"^[ \t]*CREATE[ \t]+(?:OR[ \t]+REPLACE[ \t]+)?VIEW[ \t]+okg\.(\w+)",
            VIEWS_SQL.read_text(encoding="utf-8"),
            re.IGNORECASE | re.MULTILINE,
        )
    )


def _declared_columns(cur: psycopg.Cursor, view: str) -> list[str]:
    schema, name = view.split(".", 1)
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
        """,
        (schema, name),
    )
    return [str(row[0]) for row in cur.fetchall()]


def test_published_rows_are_the_fixture_rows(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Row-level, not just non-empty: the read path returns what was published."""
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.run_sql import run_sql

    _publish_fixture(clean_runtime, tmp_path)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        nodes = run_sql(
            conn,
            session,
            "SELECT node_id, subtype FROM okg.v_nodes ORDER BY node_id",
        )
        docs = run_sql(
            conn,
            session,
            "SELECT doc_id, path, chunk_count FROM okg.v_documents",
        )
        chunks = run_sql(
            conn,
            session,
            "SELECT chunk_id, doc_id, path FROM okg.v_doc_chunks",
        )

    assert nodes["rows"] == [
        ["chunk:canonical", "document_chunk"],
        ["chunk:canonical-table", "document_chunk"],
        ["chunk:canonical-table-error", "document_chunk"],
        ["doc:canonical", "document"],
    ]
    # chunk_count is 3: the prose chunk and both table chunks, each reached by
    # a `contains` edge from the document.
    assert docs["rows"] == [["doc:canonical", "canonical.md", 3]]
    assert sorted(chunks["rows"]) == [
        ["chunk:canonical", "doc:canonical", "canonical.md"],
        ["chunk:canonical-table", "doc:canonical", "canonical.md"],
        ["chunk:canonical-table-error", "doc:canonical", "canonical.md"],
    ]


# --------------------------------------------------------------------------- #
# The gate lands in `okg schema quality`                                      #
# --------------------------------------------------------------------------- #


def test_schema_quality_view_reads_section_is_green(okg_migrated: str) -> None:
    from okg.substrate.cli.schema import run_quality

    envelope = run_quality(dsn=okg_migrated, include_lint=False)
    sections = {section["name"]: section for section in envelope["sections"]}

    assert "view_reads" in sections, (
        "the 'no view reads an always-empty table' check must land in "
        "`okg schema quality`, not only in this test file"
    )
    section = sections["view_reads"]
    assert section["gating"] is True
    assert section["status"] == "pass", section
    # Not a vacuous pass: the tier actually looked at a populated surface.
    # An empty promoted surface is reported as a FAILURE by the check itself
    # (absence is not agreement), but assert the count here too so a future
    # narrowing of `promoted_views` cannot quietly empty it.
    assert section["view_count"] > 20, section


#: (case name, view body, columns the gate must call constant-NULL).
#:
#: The gate's constant-NULL finding needs BOTH a data signal (all rows NULL)
#: and a definition signal (every projection of the column is a bare NULL
#: literal). The definition half is a pattern over PostgreSQL's deparsed view
#: body, and a pattern is exactly the kind of check that passes its author's
#: examples and misses the real thing — so these cases are built as REAL views
#: and read back through pg_get_viewdef, not asserted against hand-written SQL.
_LITERAL_NULL_CASES = [
    (
        "fake_history_bounds",
        "SELECT 1::bigint AS node_id, NULL::bigint AS valid_from_gen, "
        "NULL::bigint AS valid_to_gen",
        {"valid_from_gen", "valid_to_gen"},
    ),
    (
        # THE regression case. `pg_get_viewdef` aliases a select-list item only
        # when the alias differs from the expression's own name, so a UNION's
        # first arm deparses positionally while a constant arm aliases every
        # column. An alias-counting implementation sees ONE occurrence per
        # column — the literal one — and condemns the whole view. That is what
        # okg.v_nodes actually looks like.
        "union_positional_first_arm",
        "SELECT 'x'::text AS run_id, 1 AS n "
        "UNION ALL SELECT NULL::text, NULL::int",
        set(),
    ),
    (
        # okg.v_source_runs' shape: a fallback arm projects NULL while the
        # primary arm projects a value, and both arms alias. The column CAN
        # carry one.
        "union_named_both_arms",
        "SELECT 'x'::text AS run_id, 1 AS n "
        "UNION ALL SELECT NULL::text AS run_id, 2 AS n",
        set(),
    ),
    (
        # Both arms literal: nothing can ever fill it.
        "both_arms_null",
        "SELECT NULL::bigint AS bounded, 1 AS n "
        "UNION ALL SELECT NULL::bigint AS bounded, 2 AS n",
        {"bounded"},
    ),
    (
        # okg.v_documents' byte_count after this change. NULLIF starts with
        # the four letters NULL and is not a bare NULL.
        "nullif_is_not_a_bare_null",
        "SELECT NULLIF('a', '')::text AS byte_count, 1 AS n",
        set(),
    ),
    (
        # Commas inside a call must not split the select list.
        "coalesce_with_commas",
        "SELECT COALESCE(NULL::text, 'x') AS run_id, NULL::text AS dead",
        {"dead"},
    ),
    (
        # A column whose name is a prefix of another's must not borrow its
        # projection.
        "prefix_collision",
        "SELECT NULL::text AS path, 'p'::text AS pathname",
        {"path"},
    ),
]

#: Live views the detector must leave CLEAR. Every one has a shape that an
#: alias-counting implementation misclassified: the four graph views carry the
#: honest-read guard's all-NULL constant arm, and v_source_runs carries a
#: watermark fallback arm. A false positive on any of them reds the build for
#: a view that is telling the truth, and the cheapest way to get green again
#: is to weaken the gate — so these are asserted, not assumed.
_MUST_STAY_CLEAR = (
    "v_nodes",
    "v_edges",
    "v_branch_layered_nodes",
    "v_branch_layered_edges",
    "v_documents",
    "v_doc_chunks",
    "v_source_runs",
    "v_runtime_queue_health",
    "v_generations",
)


@pytest.mark.parametrize("view", _MUST_STAY_CLEAR)
def test_constant_null_detection_clears_honest_views(
    okg_migrated: str, view: str
) -> None:
    """No shipped view is accused of a constant-NULL column."""
    module = _load_view_reads_module()
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        columns = module._view_columns(cur, view)
        assert columns, f"okg.{view} is not installed"
        flagged = module._literal_null_columns(cur, view, columns)
    assert flagged == set(), (
        f"okg.{view} was accused of constant-NULL columns {sorted(flagged)}; "
        "these are honest views, so this is a detector defect, not a schema one"
    )


@pytest.mark.parametrize(
    ("case", "body", "expected"),
    _LITERAL_NULL_CASES,
    ids=[case for case, _body, _expected in _LITERAL_NULL_CASES],
)
def test_constant_null_detection_distinguishes_the_shapes(
    clean_runtime: str, case: str, body: str, expected: set[str]
) -> None:
    """The gate's definition signal, case by case.

    A false POSITIVE here is worse than a false negative: it would red the
    build for a view that is telling the truth (a fallback arm, or the guard
    arm the read path depends on), and the cheapest way to make that green
    again is to weaken the gate.
    """
    module = _load_view_reads_module()
    view = f"v_probe_literal_{case}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE VIEW okg.{view} AS {body}")  # noqa: S608
        try:
            columns = module._view_columns(cur, view)
            assert module._literal_null_columns(cur, view, columns) == expected
        finally:
            cur.execute(f"DROP VIEW IF EXISTS okg.{view}")


def _load_view_reads_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "check_view_reads", REPO_ROOT / "scripts" / "check_view_reads.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_view_reads_gate_refuses_the_shapes_this_change_retired(
    clean_runtime: str,
) -> None:
    """Non-vacuity. A gate that cannot red is not a gate.

    Recreates the two retired shapes — the `WHERE false` projectable stub and
    the constant-NULL-bounds history view — and asserts the gate names both,
    then that it greens once they are gone.
    """
    module = _load_view_reads_module()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE OR REPLACE VIEW okg.v_probe_projectable_stub AS
            SELECT NULL::bigint AS fact_id, NULL::text AS node_id WHERE false
            """
        )
        # Reads okg.catalog_versions purely because the migration's bootstrap
        # seed guarantees it a row: the point of this probe is the constant-
        # NULL BOUNDS the retired history views projected, not where the live
        # half came from.
        cur.execute(
            """
            CREATE OR REPLACE VIEW okg.v_probe_nodes_history AS
            SELECT catalog_version_id::text AS node_id,
                   NULL::bigint AS valid_from_gen,
                   NULL::bigint AS valid_to_gen
              FROM okg.catalog_versions
            """
        )
        cur.execute("GRANT SELECT ON okg.v_probe_projectable_stub TO okg_mcp")
        cur.execute("GRANT SELECT ON okg.v_probe_nodes_history TO okg_mcp")
        cur.execute("SELECT count(*) FROM okg.catalog_versions")
        assert cur.fetchone()[0] > 0, (
            "the probe needs a populated source, or the constant-NULL sweep "
            "would be skipped as a no_rows view and prove nothing"
        )

    try:
        result = module.run_view_read_checks(dsn=clean_runtime)
        flagged = {
            (finding["check"], finding["view"]) for finding in result["findings"]
        }
        assert result["ok"] is False
        assert ("structurally_empty", "okg.v_probe_projectable_stub") in flagged, (
            f"the WHERE-false stub class must red the gate; got {flagged}"
        )
        assert ("constant_null_column", "okg.v_probe_nodes_history") in flagged, (
            f"the constant-NULL-bounds class must red the gate; got {flagged}"
        )
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP VIEW IF EXISTS okg.v_probe_projectable_stub")
                cur.execute("DROP VIEW IF EXISTS okg.v_probe_nodes_history")

    assert module.run_view_read_checks(dsn=clean_runtime)["ok"] is True
