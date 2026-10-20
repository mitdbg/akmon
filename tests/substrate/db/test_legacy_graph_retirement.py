from __future__ import annotations

from collections.abc import Iterable

import psycopg
import pytest
from psycopg import sql

from okg.substrate.db.management import (
    legacy_graph_storage_retirement_plan,
    retire_legacy_graph_storage,
)


pytestmark = pytest.mark.postgres


# Every relation default retirement must leave in place. Membership is
# asserted with to_regclass (a catalog lookup, not a read), so listing a
# Chronos checkout surface here is safe.
_PRESERVED_RELATIONS = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.graph_generations",
    "okg.branches",
    "okg.branch_merge_requests",
    "okg.edge_fact_strandings",
    # okg.projectable_node_facts / okg.projectable_edge_facts were here until
    # `schema-canonical-read-path` deleted them with the live complex they
    # filtered for (design §2.1). Nothing to preserve.
    "okg.v_generations",
    "okg.v_nodes",
    "okg.v_edges",
    "okg.v_branch_layered_nodes",
    "okg.v_branch_layered_edges",
)

# The subset of _PRESERVED_RELATIONS whose ROWS this file compares across
# retirement: the base tables that actually hold audit content (fact logs,
# generation rows, branch metadata, stranding audit rows). The okg.v_* /
# okg.projectable_* views are excluded from the COUNT because okg.v_nodes
# and okg.v_edges are Chronos checkout surfaces: on a migrated schema their
# bodies select FROM okg.graph_nodes / okg.graph_edges and carry the
# okg.require_graph_checkout guard arm, so a bare `SELECT count(*)` over
# them raises ObjectNotInPrerequisiteState by design (see
# okg.require_graph_checkout in src/okg/substrate/db/management.py). This
# file drives plain psycopg sessions, so counting those views read a
# checkout surface bare -- which the honest-read guard correctly refuses.
# okg.v_branch_layered_nodes / okg.v_branch_layered_edges are excluded for
# exactly the same reason since `schema-canonical-read-path`: they are
# defined over okg.v_nodes / okg.v_edges, so they inherit the guard arm
# rather than reading the graph tables unguarded. okg.v_generations is
# excluded too, so that the counted set is exactly "base tables holding
# audit rows" rather than a list that happens to work.
#
# What the views still get here is an EXISTENCE assertion via
# _existing_relations (to_regclass, a catalog lookup that issues no read),
# which is what caught the original #346 defect -- that helper DROPPED
# them.
#
# Keep this an explicit tuple rather than deriving it from relkind at
# runtime, so a view added to _PRESERVED_RELATIONS later cannot silently
# join the counted set. _assert_audit_relations_cover_base_tables guards
# the opposite direction: a new base table must not silently fall out.
LEGACY_LIVE_TABLES = ("okg.nodes_live", "okg.edges_live")

_AUDIT_ROW_RELATIONS = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.graph_generations",
    "okg.branches",
    "okg.branch_merge_requests",
    "okg.edge_fact_strandings",
)


def _existing_relations(cur: psycopg.Cursor, relations: Iterable[str]) -> set[str]:
    existing: set[str] = set()
    for relation in relations:
        cur.execute("SELECT to_regclass(%s)", (relation,))
        if cur.fetchone()[0] is not None:
            existing.add(relation)
    return existing


def _relation_counts(cur: psycopg.Cursor, relations: Iterable[str]) -> dict[str, int]:
    """Count rows in each relation over a bare SQL session.

    Only pass relations that are safe to read bare -- see
    ``_AUDIT_ROW_RELATIONS``. Passing a Chronos checkout surface such as
    ``okg.v_nodes`` raises ``ObjectNotInPrerequisiteState``.
    """
    counts: dict[str, int] = {}
    for relation in relations:
        cur.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(*relation.split(".", 1))
            )
        )
        counts[relation] = int(cur.fetchone()[0])
    return counts


def _assert_audit_relations_cover_base_tables(cur: psycopg.Cursor) -> None:
    """Every preserved BASE TABLE must be in the counted set.

    ``_AUDIT_ROW_RELATIONS`` is written out by hand so that a view cannot
    silently join the counted set. This closes the opposite direction: a
    base table added to ``_PRESERVED_RELATIONS`` later must not silently
    fall OUT of it, leaving its rows unchecked while the relation still
    looks covered. Classification comes from pg_class.relkind -- a catalog
    lookup that reads no table rows, so it is safe for checkout surfaces.
    """
    assert set(_AUDIT_ROW_RELATIONS) <= set(_PRESERVED_RELATIONS)
    cur.execute(
        """
        SELECT n.nspname || '.' || c.relname
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'r'
           AND n.nspname || '.' || c.relname = ANY(%s)
        """,
        (list(_PRESERVED_RELATIONS),),
    )
    preserved_base_tables = {row[0] for row in cur.fetchall()}
    assert preserved_base_tables == set(_AUDIT_ROW_RELATIONS)


def _view_definition(cur: psycopg.Cursor, relation: str) -> str:
    cur.execute("SELECT pg_get_viewdef(%s::regclass)", (relation,))
    return str(cur.fetchone()[0])


def _seed_retirement_sentinels(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        INSERT INTO okg.edge_fact_strandings (
          fact_id, first_seen_gen, last_retry_gen, retry_count, reason
        )
        VALUES (
          987654321, 1, 1, 0, 'legacy_retirement_regression'
        )
        ON CONFLICT (fact_id) DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO okg.branches (
          deployment_name, name, role, status, metadata
        )
        VALUES (
          'legacy-retirement-regression', 'default', 'default', 'active',
          '{"test":"legacy_retirement"}'::jsonb
        )
        RETURNING branch_id
        """
    )
    default_branch_id = int(cur.fetchone()[0])
    cur.execute(
        """
        INSERT INTO okg.branches (
          deployment_name, name, role, status, metadata
        )
        VALUES (
          'legacy-retirement-regression', 'experiment', 'experiment',
          'active', '{"test":"legacy_retirement"}'::jsonb
        )
        RETURNING branch_id
        """
    )
    experiment_branch_id = int(cur.fetchone()[0])
    cur.execute(
        """
        INSERT INTO okg.branch_merge_requests (
          deployment_name, source_branch_id, target_branch_id, status,
          predicate, verdict, metadata
        )
        VALUES (
          'legacy-retirement-regression', %s, %s, 'pending',
          '{}'::jsonb, '{}'::jsonb,
          '{"test":"legacy_retirement"}'::jsonb
        )
        """,
        (experiment_branch_id, default_branch_id),
    )


def _create_minimal_legacy_live_tables(
    cur: psycopg.Cursor,
    *,
    reset: bool = False,
) -> None:
    if reset:
        cur.execute("DROP TABLE IF EXISTS okg.nodes_live, okg.edges_live CASCADE")
        create_clause = "CREATE TABLE"
    else:
        create_clause = "CREATE TABLE IF NOT EXISTS"
    cur.execute(
        f"""
        {create_clause} okg.nodes_live (
          node_id text PRIMARY KEY,
          subtype text NOT NULL DEFAULT 'legacy',
          attrs jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          text_hash text,
          valid_from_gen bigint NOT NULL DEFAULT 1,
          valid_to_gen bigint
        )
        """
    )
    cur.execute(
        f"""
        {create_clause} okg.edges_live (
          edge_id bigint PRIMARY KEY,
          src text NOT NULL DEFAULT 'legacy-src',
          dst text NOT NULL DEFAULT 'legacy-dst',
          edge_type text NOT NULL DEFAULT 'legacy_edge',
          attrs jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          valid_from_gen bigint NOT NULL DEFAULT 1,
          valid_to_gen bigint
        )
        """
    )


def test_default_retirement_preserves_audit_tables(okg_migrated: str) -> None:
    """Default Chronos retirement drops only obsolete live-table storage.

    Regression for schema-quality issue #346: the helper previously dropped
    fact logs, generation rows, branch metadata, stranding audit rows, and
    read views. Run inside one transaction and roll it back so even a failure
    does not damage the shared migrated fixture.

    Preservation is checked two ways: every relation in the preserved set must
    still EXIST (a to_regclass catalog lookup), and the audit base tables must
    hold the same ROWS. Row counts are taken only over
    ``_AUDIT_ROW_RELATIONS`` -- counting through okg.v_nodes / okg.v_edges
    would read a Chronos checkout surface bare, which
    okg.require_graph_checkout refuses by design.

    Scope of the row check: ``_seed_retirement_sentinels`` seeds okg.branches,
    okg.branch_merge_requests and okg.edge_fact_strandings, so those three
    counts are non-zero and genuinely load-bearing. okg.node_facts and
    okg.edge_facts are empty in a migrated fixture, so for them the count
    comparison is 0 == 0 and only the existence assertion has teeth against a
    DROP -- which is the shape the #346 defect actually took.
    """

    with psycopg.connect(okg_migrated) as conn:
        try:
            with conn.cursor() as cur:
                _assert_audit_relations_cover_base_tables(cur)
                _seed_retirement_sentinels(cur)
                _create_minimal_legacy_live_tables(cur)
                before = _relation_counts(cur, _AUDIT_ROW_RELATIONS)
                existing_before = _existing_relations(cur, _PRESERVED_RELATIONS)
                assert existing_before == set(_PRESERVED_RELATIONS)

                retire_legacy_graph_storage(conn)

                cur.execute("SELECT to_regclass('okg.nodes_live')")
                assert cur.fetchone()[0] is None
                cur.execute("SELECT to_regclass('okg.edges_live')")
                assert cur.fetchone()[0] is None
                assert _existing_relations(cur, _PRESERVED_RELATIONS) == set(
                    _PRESERVED_RELATIONS
                )
                assert _relation_counts(cur, _AUDIT_ROW_RELATIONS) == before

                cur.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1
                        FROM okg.edge_fact_strandings
                       WHERE fact_id = 987654321
                         AND reason = 'legacy_retirement_regression'
                    )
                    """
                )
                assert cur.fetchone()[0] is True
        finally:
            conn.rollback()


def test_retirement_refuses_while_a_view_still_reads_the_legacy_tables(
    okg_migrated: str,
) -> None:
    """Retirement no longer rewrites views. It refuses, and names the reader.

    This test previously asserted the opposite: that retirement REPOINTED a
    legacy-pointing okg.v_nodes / okg.v_edges onto the Chronos graph tables. It
    did so by executing a copy of the view definitions that lived in
    db/management.py — the same copy that made views.sql a description of a
    retired architecture rather than the read path, which is what
    `schema-canonical-read-path` removed.

    The replacement contract is an ordering one, and `migrate()` already
    satisfies it: views.sql installs (through the frozen-shape convergence
    pass) BEFORE retirement runs, so by then no view reads the legacy tables.
    A caller that retires first is refused by name rather than having its read
    layer silently rewritten by a definition nobody could find in the SQL
    files — or, worse, hitting a bare DependentObjectsStillExist from the
    restrictive DROP.
    """
    from okg.substrate.errors import SubstrateError

    with psycopg.connect(okg_migrated) as conn:
        try:
            with conn.cursor() as cur:
                _create_minimal_legacy_live_tables(cur)
                cur.execute(
                    """
                    CREATE VIEW okg.v_legacy_reader AS
                    SELECT node_id FROM okg.nodes_live
                    """
                )

            with pytest.raises(SubstrateError) as raised:
                retire_legacy_graph_storage(conn)

            assert "okg.v_legacy_reader" in str(raised.value)
            assert "views.sql" in (raised.value.fix_hint or "")
            # Refused, not half-done: the tables are still there.
            with conn.cursor() as cur:
                assert _existing_relations(cur, LEGACY_LIVE_TABLES) == set(
                    LEGACY_LIVE_TABLES
                )
        finally:
            conn.rollback()


def test_retirement_proceeds_once_the_read_path_is_chronos_backed(
    okg_migrated: str,
) -> None:
    """The migrate ordering, end to end: canonical views installed, then drop.

    The fixture is a migrated database, so views.sql has already bound
    okg.v_nodes / okg.v_edges to okg.graph_nodes / okg.graph_edges. Recreating
    the legacy tables underneath that is exactly the state an upgraded
    deployment is in when migrate reaches retirement.
    """
    with psycopg.connect(okg_migrated) as conn:
        try:
            with conn.cursor() as cur:
                _create_minimal_legacy_live_tables(cur)
                nodes_view = _view_definition(cur, "okg.v_nodes")
                edges_view = _view_definition(cur, "okg.v_edges")
                assert "graph_nodes" in nodes_view
                assert "nodes_live" not in nodes_view
                assert "graph_edges" in edges_view
                assert "edges_live" not in edges_view
                # The honest-read guard arm rides the canonical definition, so
                # retirement cannot drop it: it never touches the views.
                assert "require_graph_checkout" in nodes_view
                assert "require_graph_checkout" in edges_view

            retire_legacy_graph_storage(conn)

            with conn.cursor() as cur:
                assert _existing_relations(cur, LEGACY_LIVE_TABLES) == set()
                assert _existing_relations(
                    cur, ("okg.v_nodes", "okg.v_edges")
                ) == {"okg.v_nodes", "okg.v_edges"}
                assert _view_definition(cur, "okg.v_nodes") == nodes_view
                assert _view_definition(cur, "okg.v_edges") == edges_view
        finally:
            conn.rollback()


def test_broad_retirement_requires_dry_run_scope(okg_migrated: str) -> None:
    """The default retirement report lists exact targets before mutation."""

    with psycopg.connect(okg_migrated) as conn:
        try:
            with conn.cursor() as cur:
                _create_minimal_legacy_live_tables(cur, reset=True)
                cur.execute(
                    """
                    INSERT INTO okg.nodes_live (node_id)
                    VALUES ('dry-run-node-a'), ('dry-run-node-b')
                    ON CONFLICT (node_id) DO NOTHING
                    """
                )
                cur.execute(
                    """
                    INSERT INTO okg.edges_live (edge_id)
                    VALUES (987654321), (987654322), (987654323)
                    ON CONFLICT (edge_id) DO NOTHING
                    """
                )

                plan = legacy_graph_storage_retirement_plan(conn)

                assert plan["mode"] == "legacy_live_table_retirement"
                targets = {
                    row["relation"]: row
                    for row in plan["destructive_targets"]
                }
                assert set(targets) == {"okg.nodes_live", "okg.edges_live"}
                assert targets["okg.nodes_live"] == {
                    "relation": "okg.nodes_live",
                    "action": "drop_table_restrict",
                    "exists": True,
                    "row_count": 2,
                }
                assert targets["okg.edges_live"] == {
                    "relation": "okg.edges_live",
                    "action": "drop_table_restrict",
                    "exists": True,
                    "row_count": 3,
                }

                preserved = {
                    row["relation"]
                    for row in plan["preserved_relations"]
                }
                assert preserved == set(_PRESERVED_RELATIONS)
                assert set(targets).isdisjoint(preserved)
        finally:
            conn.rollback()
