"""Upgrade-path regressions for the #796 alias/receipt schema additions.

`okg.aliases.normalized_alias` and `okg.entity_merge_receipts` land via the
0000 schema snapshot, which `okg migrate` re-applies wholesale. These tests pin
the three ways a deployment can meet the new code before (or without) that
migrate: an un-backfilled alias table, an alias write against the pre-#796
column set, and `okg trace node` against a database with no receipts table.
"""
from __future__ import annotations

import argparse

import psycopg
import pytest

from okg.substrate.catalog.aliases import (
    alias_schema_guard,
    backfill_normalized_aliases,
)
from okg.substrate.entity_resolution import (
    EntityResolutionUnavailable,
    EntityResolutionStage,
)
from okg.substrate.errors import SchemaValidationFailed
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

pytestmark = pytest.mark.postgres


def _seed_nodes(conn: psycopg.Connection, rows: list[tuple[str, str]]) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="concept",
                attrs={"name": name},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
            for node_id, name in rows
        ],
        branch="default",
        source="fixture",
    )


def _config() -> dict[str, object]:
    return {
        "deployment_name": "test-deployment",
        "branch_name": "default",
        "policies": {
            "concept": {
                "name_field": "name",
                "id_prefix": "concept",
                "identity_fields": ["name"],
            }
        },
    }


def _applied_catalog_version(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return int(cur.fetchone()[0])


def test_backfill_populates_rows_that_predate_the_column(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _applied_catalog_version(conn)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.aliases "
                "(alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, 'concept', %s)",
                [
                    ("Signal Region", "concept:curated", cv),
                    ("Extended–ＡＢＣＤ", "concept:extended", cv),
                    ("!!!", "concept:punctuation", cv),
                ],
            )

        assert backfill_normalized_aliases(conn) == 3

        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias, normalized_alias FROM okg.aliases ORDER BY alias"
            )
            stored = dict(cur.fetchall())

    assert stored["Signal Region"] == "signal region"
    assert stored["Extended–ＡＢＣＤ"] == "extended abcd"
    # A name that normalizes to nothing is written as '', never left NULL --
    # otherwise the bounded scan would re-select it forever.
    assert stored["!!!"] == ""


def test_backfill_is_idempotent_and_batched(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _applied_catalog_version(conn)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.aliases "
                "(alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, 'concept:curated', 'concept', %s)",
                [(f"Alias {n}", cv) for n in range(25)],
            )

        assert backfill_normalized_aliases(conn, batch_size=10) == 25
        # Converged: a second pass rewrites nothing.
        assert backfill_normalized_aliases(conn, batch_size=10) == 0


def test_resolution_fails_closed_on_a_half_normalized_alias_table(
    clean_runtime: str,
) -> None:
    """A straggling NULL must not silently mint around a curated alias."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _applied_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, scope, catalog_version_id) "
                "VALUES ('Signal Region', 'concept:curated', 'concept', %s)",
                (cv,),
            )

        # A case variant would miss the exact-surface arm and mint a duplicate,
        # which is precisely what the guard has to stop.
        with pytest.raises(EntityResolutionUnavailable, match="normalized_alias"):
            EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id="concept:proposed",
                subtype="concept",
                attrs={"name": "signal region"},
            )

        backfill_normalized_aliases(conn)

    # After the backfill the same lookup resolves to the curated canonical
    # instead of minting a duplicate.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_nodes(conn, [("concept:curated", "Canonical Signal Region")])
        decision = EntityResolutionStage(_config()).resolve(
            conn,
            proposed_node_id="concept:proposed",
            subtype="concept",
            attrs={"name": "signal region"},
        )

    assert decision.canonical_node_id == "concept:curated"
    assert decision.method == "curated_alias_exact"


def test_alias_write_against_a_pre_796_column_set_is_actionable(
    clean_runtime: str,
) -> None:
    """The bare psycopg UndefinedColumn is replaced by a named remedy.

    Uses a TEMP table shaped like the pre-#796 okg.aliases rather than
    ALTERing the real one: `clean_runtime` TRUNCATEs a session-scoped
    database but never re-runs DDL, so dropping the column there would
    cascade aliases_normalized_lookup_idx away for every later test.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE pre_796_aliases ("
                "  alias TEXT NOT NULL,"
                "  canonical TEXT NOT NULL,"
                "  scope TEXT,"
                "  catalog_version_id BIGINT NOT NULL)"
            )
        with pytest.raises(SchemaValidationFailed) as excinfo:
            with alias_schema_guard():
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO pre_796_aliases "
                        "(alias, normalized_alias, canonical, scope, "
                        " catalog_version_id) "
                        "VALUES ('a', 'a', 'concept:a', 'concept', 1)"
                    )

    assert "okg migrate" in (excinfo.value.fix_hint or "")
    assert excinfo.value.data["column"] == "normalized_alias"


def test_alias_guard_does_not_mislabel_an_unrelated_missing_column(
    clean_runtime: str,
) -> None:
    """A different UndefinedColumn must not be reported as a migrate problem."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE odd_shape (alias TEXT NOT NULL)")
        with pytest.raises(psycopg.errors.UndefinedColumn):
            with alias_schema_guard():
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO odd_shape (alias, canonical) VALUES ('a', 'b')"
                    )


def test_trace_node_survives_a_database_with_no_merge_receipts(
    clean_runtime: str,
) -> None:
    """`okg trace node` is what an operator runs when things are ALREADY broken."""
    from okg.substrate.cli.main import _cmd_trace

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS okg.entity_merge_receipts")

    args = argparse.Namespace(
        trace_target="node",
        node_id="concept:absent",
        dsn=clean_runtime,
        deployment=None,
        branch="default",
        generation_id=None,
    )
    try:
        assert _cmd_trace(args, as_json=True) == 0
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS okg.entity_merge_receipts ("
                    "  merge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),"
                    "  deployment_name TEXT NOT NULL,"
                    "  branch_name TEXT NOT NULL,"
                    "  survivor_node_id TEXT NOT NULL,"
                    "  retired_node_id TEXT NOT NULL,"
                    "  subtype TEXT NOT NULL,"
                    "  pinned_checkpoint TEXT NOT NULL,"
                    "  catalog_version_id BIGINT NOT NULL,"
                    "  footprint_digest TEXT NOT NULL,"
                    "  plan_digest TEXT NOT NULL,"
                    "  plan JSONB NOT NULL,"
                    "  retired_snapshot JSONB NOT NULL,"
                    "  status TEXT NOT NULL DEFAULT 'awaiting_publish',"
                    "  repair_generation_id TEXT,"
                    "  actor TEXT NOT NULL,"
                    "  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                    "  published_at TIMESTAMPTZ,"
                    "  UNIQUE (deployment_name, branch_name, retired_node_id),"
                    "  UNIQUE (deployment_name, branch_name, plan_digest))"
                )


def test_union_rewrite_matches_exactly_the_rows_the_old_predicate_did(
    clean_runtime: str,
) -> None:
    """The index-friendly UNION must not change which rows resolve.

    `_exact_aliases` was rewritten from one predicate into a UNION of three
    single-index arms (scoped-normalized, global-normalized, exact-surface).
    Splitting a predicate is where scope handling silently drifts, so pin each
    arm and each exclusion. A caller raises when this returns >1, so the
    boundaries decide between resolving, failing closed, and minting.

    This is the correctness half of the change. The performance half -- 70.8ms
    with every row filtered versus 0.435ms at 200k rows -- is a measurement,
    not a unit-testable property, and lives in
    .scratch/index_shape_probe.sh.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _applied_catalog_version(conn)
        other_cv = _applied_catalog_version(conn)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    # Arm A: scoped, matched on the normalized key.
                    ("Signal Region", "signal region", "concept:scoped", "concept", cv),
                    # Arm B: global (NULL scope), matched on the normalized key.
                    ("Signal-Region!", "signal region", "concept:scoped", None, cv),
                    # Arm C: matched on the exact surface form only.
                    ("SiGnAl RgN", "signal rgn", "concept:surface", "concept", cv),
                    # Excluded: a scope this subtype does not own. A distinct
                    # surface form (the PK is (alias, catalog_version_id)) that
                    # normalizes to the same key, so only the scope keeps it out.
                    ("Signal  Region", "signal region", "region:other", "region", cv),
                    # Excluded: a different catalog version.
                    ("Signal Region", "signal region", "concept:old", "concept",
                     other_cv),
                ],
            )

        # Arms A and B agree on one canonical; the foreign scope and the other
        # catalog version are excluded, so this is a clean single match.
        assert EntityResolutionStage._exact_aliases(
            conn, "Signal Region", "signal region", "concept", cv
        ) == ["concept:scoped"]

        # Arm C alone: the surface form hits, the normalized key does not.
        assert EntityResolutionStage._exact_aliases(
            conn, "SiGnAl RgN", "no such normalized key", "concept", cv
        ) == ["concept:surface"]

        # The other catalog version sees only its own row.
        assert EntityResolutionStage._exact_aliases(
            conn, "Signal Region", "signal region", "concept", other_cv
        ) == ["concept:old"]

        # A subtype that owns none of these resolves to nothing rather than
        # picking up the foreign-scope row.
        assert (
            EntityResolutionStage._exact_aliases(
                conn, "Nothing Here", "nothing here", "concept", cv
            )
            == []
        )

        # Disagreeing curated aliases must still surface as >1 for the caller
        # to reject, not be collapsed by the UNION's dedup.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES ('Signal Region Dup', 'signal region', 'concept:rival', "
                "'concept', %s)",
                (cv,),
            )
        assert EntityResolutionStage._exact_aliases(
            conn, "Signal Region", "signal region", "concept", cv
        ) == ["concept:rival", "concept:scoped"]
