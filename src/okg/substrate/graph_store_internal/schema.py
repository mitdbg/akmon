"""Schema and interval-index boundary for the Chronos graph store."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.db.cold_history import (
    HOT_INDEX_MANIFEST_TABLE,
    DEFERRED_INDEX_BUILDS,
    INTERVAL_SOURCE_READ_INDEX_SPECS,
    hot_index_definition_matches,
    spec_index_definition_matches,
)
from okg.substrate.graph_store_internal.common import (
    CONSOLE_COUNT_ROLLUP_PK,
    CONSOLE_COUNT_ROLLUP_TABLE,
    EDGE_TABLE,
    NODE_TABLE,
    ONTOLOGY_ROLLUP_PK,
    ONTOLOGY_ROLLUP_TABLE,
    chronos_interval_index_name,
    graph_registered,
    index_matches_columns,
    quote_ident,
    quote_physical_table,
)
from okg.substrate.graph_store_internal.generation_ordinal import (
    ensure_generation_ordinal_schema,
)
from chronos_core.branching._interval_backend import (
    set_tombstone_keep,
    set_tombstone_keep_default,
)

# Delete-marker payload (okg#3284). A delete in the interval backend writes a
# marker row (deleted = TRUE). Upstream Chronos copies the whole previous row
# into it; OKG sets this OKG-local Chronos option (see
# src/chronos_core/VENDORED.md) so a marker keeps only the primary key, the
# interval columns and the columns below, and NULLs the rest (attrs,
# embedding, ...). These are the marker columns OKG reads: the publisher's
# dirty scans (subtype, edge_type), source_frontier_unpublished_writes()
# (source), history compaction's per-class floors (subtype, edge_type) and
# compensation's candidate scans (source, src, dst). Every other OKG interval
# table (rollups, declarers, maintained derivations) keeps its primary key
# only. Applied at import and again by every ChronosGraphStore construction,
# not in the registration paths: those do not run in a process whose graph is
# already registered or whose bootstrap cache is warm.
TOMBSTONE_KEEP_COLUMNS: dict[str, tuple[str, ...]] = {
    NODE_TABLE: ("subtype", "source"),
    EDGE_TABLE: ("edge_type", "src", "dst", "source"),
}


def configure_tombstone_payload() -> None:
    """Apply OKG's delete-marker keep sets to the vendored Chronos backend."""
    set_tombstone_keep_default(())
    for table, columns in TOMBSTONE_KEEP_COLUMNS.items():
        set_tombstone_keep(table, columns)


configure_tombstone_payload()


def _node_attr_path_index_allowlist() -> set[str] | None:
    """Which node attr-path expression indexes to create.

    ``OKG_CHRONOS_NODE_ATTR_PATH_INDEXES`` selects the optional node
    ``attrs->>'<path>'`` expression indexes (off by default to spare path-less
    deployments the write/disk overhead). Accepted values:

    * ``1`` / ``true`` / ``yes`` / ``on`` / ``all`` -> ALL paths (returns None)
    * ``0`` / ``false`` / ``off`` / unset / empty    -> none (returns ``set()``)
    * a comma-separated list of index SUFFIXES        -> only those, e.g.
      ``node_attr_source_cn_visible,node_attr_semantic_target_sk_vis``

    The list form lets a deployment index only the attr paths its workload
    actually queries (per PACT ``bound-okg-workspace-disk-growth``) instead of
    paying disk + write cost for all of them; suffixes not listed are dropped.
    """
    low = os.environ.get("OKG_CHRONOS_NODE_ATTR_PATH_INDEXES", "0").strip().lower()
    if low in {"1", "true", "yes", "on", "all"}:
        return None
    if low in {"", "0", "false", "no", "off"}:
        return set()
    # Index suffixes are lowercase; lowercase the tokens so a case-typo in the
    # allowlist still matches rather than silently dropping the index.
    return {token.strip() for token in low.split(",") if token.strip()}


# Process-level record of the registry epoch at which the MCP grant catch-up was
# last run for a database, keyed by (dbname, host, port). The grant DO-block
# scans pg_class and GRANTs every public ``_chronos*`` table to okg_mcp; it
# re-fires ~6x per ingest commit from the ensure/apply/checkpoint paths even
# though the set of tables is unchanged. The set only changes when chronos
# registers a NEW physical table, which bumps the per-db registry epoch
# (chronos_core.branching._common). So skip the grant while the epoch is
# unchanged, and re-grant (correctness: the new table must become readable by
# the read role) when it advances.
_MCP_GRANTED_AT_EPOCH: dict[tuple, int] = {}


def _mcp_grant_db_key(conn: Any) -> tuple:
    info = getattr(conn, "info", None)
    if info is not None:
        dbname = getattr(info, "dbname", None)
        if dbname:
            return (
                "pg",
                dbname,
                str(getattr(info, "host", "") or ""),
                getattr(info, "port", "") or "",
            )
    # Fail-safe: a connection we cannot stably identify gets a per-conn key, so
    # the guard is never shared and can never wrongly skip a needed grant.
    return ("conn", id(conn))


def _clear_mcp_grant_guard() -> None:
    """Drop the process-level MCP-grant guard (test-harness reset only).

    For harnesses that drop roles / TRUNCATE chronos tables out-of-band.
    Production grants are stable per process, so this is not on any production
    path.
    """
    _MCP_GRANTED_AT_EPOCH.clear()



#: Characters of `attrs.path` the grep path-prefix btree indexes (PACT
#: mcp-grep-3067). A btree entry must fit in about 2.7 KB; 512 characters
#: of UTF-8 stay under it. Grep's path predicate repeats this bound.
GREP_PATH_INDEX_CHARS = 512


@dataclass(frozen=True)
class ToolIndexSpec:
    """A node index a served read tool depends on, built by `ensure_schema`.

    `okg migrate --apply` builds these; the migration ledger does not list
    them, so readiness reads them from this spec and `pg_indexes` (issue
    #3311). `effect` says what the tool does without the index: `refused`
    (the call fails) or `sequential_scan` (it works, slowly).
    """

    suffix: str
    index_expr: str
    predicate: str
    required_by: str
    effect: str


#: pg_textsearch BM25 index for lexical node search; see the comment where
#: `ensure_schema` builds it.
BM25_INDEX_SPECS: dict[str, tuple[ToolIndexSpec, ...]] = {
    NODE_TABLE: (
        ToolIndexSpec(
            "node_text_bm25_visible",
            "(attrs->>'text') text_bm25_ops",
            "deleted = FALSE AND attrs ? 'text'",
            'search(method="lexical"), passage search and grep ranking',
            "refused",
        ),
    ),
}

#: `search(method="grep")` indexes (PACT mcp-grep-3067, issue #3067, decision
#: 168a); see the comment where `ensure_schema` builds them.
GREP_INDEX_SPECS: dict[str, tuple[ToolIndexSpec, ...]] = {
    NODE_TABLE: (
        ToolIndexSpec(
            "node_text_trgm_visible",
            "USING gin ((attrs->>'text') gin_trgm_ops)",
            "deleted = FALSE AND attrs ? 'text'",
            'search(method="grep")',
            "refused",
        ),
        ToolIndexSpec(
            # Bounded: a btree entry must fit in about 2.7 KB, and a
            # producer may emit a longer path. Grep repeats this
            # expression and rechecks the full value.
            "node_path_prefix_visible",
            f"((left(attrs->>'path', {GREP_PATH_INDEX_CHARS})) text_pattern_ops)",
            "deleted = FALSE AND attrs ? 'path'",
            'search(method="grep") path_prefix',
            "sequential_scan",
        ),
        ToolIndexSpec(
            # Grep's `path:<substring>` term (PACT mcp-grep-rank): a
            # substring anywhere in the path, which the prefix btree
            # cannot answer. A GIN entry holds trigrams, not the
            # value, so a long path needs no bound here.
            "node_path_trgm_visible",
            "USING gin ((attrs->>'path') gin_trgm_ops)",
            "deleted = FALSE AND attrs ? 'path'",
            'search(method="grep") path: terms',
            "refused",
        ),
    ),
}


def tool_index_names(
    tables: dict[str, dict[str, Any]],
) -> list[tuple[str, str, ToolIndexSpec]]:
    """`(index_name, logical_table, spec)` for every tool index
    `ensure_schema` builds on these interval tables.

    `tables` is `_interval_table_metadata`'s shape. A table without an
    `attrs` column gets none, exactly as `ensure_schema` skips it.
    """
    names: list[tuple[str, str, ToolIndexSpec]] = []
    for specs in (BM25_INDEX_SPECS, GREP_INDEX_SPECS):
        for logical_table, table_specs in specs.items():
            table = tables.get(logical_table)
            if not table:
                continue
            columns = set(table["columns"]) | {"live_lo", "live_hi", "deleted"}
            if "attrs" not in columns:
                continue
            for spec in table_specs:
                names.append((
                    chronos_interval_index_name(table["physical_table"], spec.suffix),
                    logical_table,
                    spec,
                ))
    return names



def _is_bounded_path_index(indexdef: str | None) -> bool:
    """Whether a stored path-prefix index definition is the bounded build.

    PostgreSQL stores the expression as ``"left"((attrs ->> 'path'::text),
    512)``: quoted, spaced and cast. Compared with quotes and whitespace
    removed, so the check is true exactly for the current build and a
    re-run of ensure_schema keeps the index.
    """
    normalized = re.sub(r'[\s"]', "", indexdef or "").lower()
    return (
        f"left((attrs->>'path'::text),{GREP_PATH_INDEX_CHARS})" in normalized
        and "text_pattern_ops" in normalized
    )

def _create_partial_index(
    db: Any, index_name: str, physical: str, body: str, predicate: str,
) -> None:
    """One partial index on a Chronos branch table, created if missing.

    The BM25 and grep text/path index loops share this one statement, so the
    reviewed dynamic CREATE inventory in scripts/check_migration_drop_recreate.py
    keeps its three `{quote_ident(index_name)}` sites. `index_name` comes from
    `chronos_interval_index_name` and `physical` from `quote_physical_table`;
    `body` and `predicate` are source constants.
    """
    db.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {quote_ident(index_name)}
          ON {physical} {body}
         WHERE {predicate}
        """
    )


def _add_read_class_columns(db: Any, table: str) -> None:
    """Add the stored Nomos read class columns (PACT
    lane-h-nomos-read-class-stored): nullable, no default, so PostgreSQL adds
    them without rewriting the table. Rows written before them read NULL and
    are decided by the read-time projection."""
    db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS nomos_read_class UUID")
    db.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
        "nomos_read_class_version SMALLINT"
    )


class ChronosGraphSchemaMixin:
    """Chronos graph schema, table-registration, and interval-index operations."""

    context: Any

    def _apply_store_options(self) -> None:
        # Delete markers keep only the columns OKG reads (okg#3284). Applied
        # on every store construction, because the registration paths do not
        # run in a process whose bootstrap cache is warm or whose graph is
        # already registered.
        configure_tombstone_payload()

    def ensure_schema(self) -> None:
        db = self.context.db
        # Least-privilege read roles lack CREATE. If the graph is already
        # provisioned and registered, every statement below is a no-op.
        if getattr(db, "dialect", None) == "postgres" and graph_registered(db):
            return
        db.execute("CREATE SCHEMA IF NOT EXISTS okg")
        has_vector = bool(db.execute("SELECT to_regtype('vector') AS t").fetchone()["t"])
        embedding_column = "embedding vector(2560)," if has_vector else ""
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS okg.graph_nodes (
              node_id TEXT PRIMARY KEY,
              subtype TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              text_hash TEXT,
              {embedding_column}
              embedding_model TEXT,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.graph_edges (
              edge_id BIGINT PRIMARY KEY,
              src TEXT NOT NULL,
              dst TEXT NOT NULL,
              edge_type TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
              provenance TEXT NOT NULL DEFAULT 'explicit',
              confidence REAL NOT NULL DEFAULT 1.0,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        for table in ("okg.graph_nodes", "okg.graph_edges"):
            db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source TEXT")
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "source_revision JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS run_id TEXT")
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "observed_at TIMESTAMPTZ"
            )
        from okg.substrate.attributes import ensure_attribute_schema_db

        ensure_attribute_schema_db(db)
        # The subtype-pair / producer edge-count rollup table. A plain relational
        # table that the interval backend adopts on register_table; the publish
        # running-sum writes it through the branch session in the cycle
        # transaction, so the generation checkpoint pins a consistent rollup.
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.ontology_rollup (
              src_subtype text   NOT NULL,
              edge_type   text   NOT NULL,
              dst_subtype text   NOT NULL,
              producer    text   NOT NULL,
              edge_count  bigint NOT NULL,
              PRIMARY KEY (src_subtype, edge_type, dst_subtype, producer)
            )
            """
        )
        # Branch-aware generation-ordinal registry (ivm-effective-attribute-view /
        # task.generation-ordinal-primitive). Created here for fresh provisions;
        # also self-healed on the mint path for already-registered deployments
        # (ensure_schema short-circuits for a registered graph above).
        ensure_generation_ordinal_schema(db)
        db.commit()
        self.context.register_table(NODE_TABLE, ["node_id"])
        self.context.register_table(EDGE_TABLE, ["edge_id"])
        # Added after registration, the way an upgrade adds them, so a fresh
        # install's interval tables carry the columns in the same position
        # (after the interval columns) as an upgraded one; the migration-
        # convergence lane compares the two dumps column by column.
        for table in (NODE_TABLE, EDGE_TABLE):
            _add_read_class_columns(db, table)
        self.context.register_table(NODE_TABLE, ["node_id"])
        self.context.register_table(EDGE_TABLE, ["edge_id"])
        self.context.register_table(ONTOLOGY_ROLLUP_TABLE, ONTOLOGY_ROLLUP_PK)
        # The console count rollup (console-count-rollup / #444 slice 2): a second
        # maintained-derivation Chronos table, created + registered the same way.
        self.ensure_console_count_rollup_table()
        # Shared-node declarers (issue #1901): a Chronos table the graph
        # writer reads and writes in the same transaction as the node rows.
        self.ensure_node_declarers_table()
        # Shared-edge declarers and the edge ranking (item 92a).
        self.ensure_edge_declarers_table()
        # Waiting cross-source edges (pact cross-source-endpoint-wedge).
        self.ensure_edge_deferrals_table()
        self._grant_chronos_public_tables_to_mcp_if_present()
        self._ensure_interval_source_read_indexes()
        db.commit()

    def ensure_interval_storage_upgrade(self) -> None:
        """Idempotently upgrade interval backing columns and visibility indexes."""
        if getattr(self.context, "backend_name", None) != "interval":
            return
        from okg.substrate.attributes import ensure_attribute_schema_db

        # Existing registered deployments take the migration path below because
        # ensure_schema early-returns for their already-registered graph. Keep
        # new OKG-owned side tables in that path too, otherwise migrate reports
        # success while those tables are still absent.
        ensure_attribute_schema_db(self.context.db)
        ensure_generation_ordinal_schema(self.context.db)
        self.context.register_table(NODE_TABLE, ["node_id"])
        self.context.register_table(EDGE_TABLE, ["edge_id"])
        # Register the rollup on already-provisioned deployments too: ensure_schema
        # early-returns on an already-registered graph, so a deployment
        # provisioned before this table existed would otherwise never get it.
        self.ensure_ontology_rollup_table()
        self.ensure_console_count_rollup_table()
        # Registered deployments reach the shared-node declarer table here,
        # through `okg migrate` (issue #1901).
        self.ensure_node_declarers_table()
        # ...and the shared-edge declarer table and edge ranking (item 92a).
        self.ensure_edge_declarers_table()
        # Waiting cross-source edges (pact cross-source-endpoint-wedge).
        self.ensure_edge_deferrals_table()
        self._grant_chronos_public_tables_to_mcp_if_present()
        self._ensure_interval_source_read_indexes()
        self.context.db.commit()

    def ensure_graph_column_upgrade(self) -> None:
        """Idempotently repair additive OKG graph columns on registered tables.

        ``ensure_schema`` deliberately returns early for already-registered
        Chronos graphs so read-only roles can attach without attempting DDL.
        The migrate path has writer privileges and must still repair additive
        graph-table drift, such as older ``okg.graph_nodes`` tables missing the
        embedding columns expected by read-side health checks and embed workers.
        """
        db = self.context.db
        node_columns, _node_defs = db.table_defs(NODE_TABLE)
        edge_columns, _edge_defs = db.table_defs(EDGE_TABLE)
        if not node_columns or not edge_columns:
            return

        has_vector = bool(
            db.execute("SELECT to_regtype('vector') AS t").fetchone()["t"]
        )
        if has_vector:
            db.execute(
                "ALTER TABLE okg.graph_nodes "
                "ADD COLUMN IF NOT EXISTS embedding vector(2560)"
            )
        db.execute(
            "ALTER TABLE okg.graph_nodes "
            "ADD COLUMN IF NOT EXISTS embedding_model TEXT"
        )
        for table in (NODE_TABLE, EDGE_TABLE):
            db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source TEXT")
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "source_revision JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS run_id TEXT")
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "observed_at TIMESTAMPTZ"
            )
            _add_read_class_columns(db, table)

        if graph_registered(db):
            self.context.register_table(NODE_TABLE, ["node_id"])
            self.context.register_table(EDGE_TABLE, ["edge_id"])
        else:
            db.commit()

    def ensure_ontology_rollup_table(self) -> None:
        """Idempotently create + register the ontology rollup Chronos table.

        Safe to call from any write-privileged path. ``ensure_schema``
        early-returns on an already-registered graph (the least-privilege
        read-role attach path), so deployments provisioned before the rollup
        table existed need this explicit, write-privileged migration. A
        read-only role never reaches here. Registration (not just CREATE) is
        load-bearing: it is what makes the table a Chronos interval table so
        reads as-of a generation reconstruct the checkpoint-consistent rollup.
        """
        db = self.context.db
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.ontology_rollup (
              src_subtype text   NOT NULL,
              edge_type   text   NOT NULL,
              dst_subtype text   NOT NULL,
              producer    text   NOT NULL,
              edge_count  bigint NOT NULL,
              PRIMARY KEY (src_subtype, edge_type, dst_subtype, producer)
            )
            """
        )
        db.commit()
        self.context.register_table(ONTOLOGY_ROLLUP_TABLE, ONTOLOGY_ROLLUP_PK)
        db.commit()

    def ensure_console_count_rollup_table(self) -> None:
        """Idempotently create + register the console count rollup Chronos table.

        The maintained-derivation sibling of ``ensure_ontology_rollup_table``
        (console-count-rollup / #444 slice 2). Carries per-(dimension_kind,
        dimension_value) counts -- nodes by-subtype, edges by-edge-type, nodes
        by-source -- maintained at publish time and read O(1) as-of a pinned
        generation. The TOTALS dimension is NOT here; it is served from
        checkpoint metadata by accelerate-operator-read-path. Registration (not
        just CREATE) is load-bearing: it makes the table a Chronos interval table
        so an as-of-generation read reconstructs the checkpoint-consistent rows.
        Safe to call from any write-privileged path; a read-only role never
        reaches here (``ensure_schema`` early-returns on a registered graph).
        """
        db = self.context.db
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.console_count_rollup (
              dimension_kind  text   NOT NULL,
              dimension_value text   NOT NULL,
              item_count      bigint NOT NULL,
              PRIMARY KEY (dimension_kind, dimension_value)
            )
            """
        )
        db.commit()
        self.context.register_table(
            CONSOLE_COUNT_ROLLUP_TABLE, CONSOLE_COUNT_ROLLUP_PK
        )
        db.commit()

    def ensure_node_declarers_table(self) -> None:
        """Idempotently create + register the shared-node declarer table.

        ``okg.graph_node_declarers`` (issue #1901) records which sources
        declare a substitute node, so a retraction by the owner can pass the
        node to a surviving declarer instead of deleting it. Registration makes
        it a Chronos interval table: its rows branch, merge and checkpoint with
        the graph rows they describe. Only the graph writer reads it; no read
        surface selects it, so a reader on a database without it answers the
        same. Safe to call from any write-privileged path.
        """
        from okg.substrate.graph_store_internal.node_declarers import (
            CREATE_NODE_DECLARERS_TABLE_SQL,
            NODE_DECLARERS_PK,
            NODE_DECLARERS_TABLE,
        )

        db = self.context.db
        db.execute(CREATE_NODE_DECLARERS_TABLE_SQL)
        db.commit()
        self.context.register_table(NODE_DECLARERS_TABLE, NODE_DECLARERS_PK)
        db.commit()

    def ensure_edge_declarers_table(self) -> None:
        """Idempotently create + register the shared-edge declarer table.

        ``okg.graph_edge_declarers`` (item 92a) keeps every producer's
        current version of a ranked edge, so the graph can store the
        highest-ranked one whatever order they wrote in. Registration makes
        it a Chronos interval table: its rows branch, merge and checkpoint
        with the graph rows they describe. The plain ``okg.edge_precedence``
        table holds the deployment's ranking that ``okg migrate`` and the
        worker copy in. Only the graph writer and the code_reference
        enricher read them. Safe to call from any write-privileged path.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            CREATE_EDGE_DECLARERS_TABLE_SQL,
            CREATE_EDGE_PRECEDENCE_TABLE_SQL,
            EDGE_DECLARERS_PK,
            EDGE_DECLARERS_TABLE,
        )

        db = self.context.db
        db.execute(CREATE_EDGE_DECLARERS_TABLE_SQL)
        db.execute(CREATE_EDGE_PRECEDENCE_TABLE_SQL)
        db.commit()
        self.context.register_table(EDGE_DECLARERS_TABLE, EDGE_DECLARERS_PK)
        db.commit()

    def ensure_edge_deferrals_table(self) -> None:
        """Idempotently create + register the waiting-edge table.

        ``okg.graph_edge_deferrals`` keeps an edge whose endpoint another
        source has not written (or whose subtype its clause does not accept)
        until the record exists and the checks pass. Registration makes it a
        Chronos interval table: it branches, merges, checkpoints and rolls
        back with the graph. Its physical tables are withheld from every read
        role by the grant block below. The index on each endpoint makes
        promotion a lookup.
        """
        from okg.substrate.graph_store_internal.edge_deferrals import (
            CREATE_EDGE_DEFERRALS_TABLE_SQL,
            EDGE_DEFERRALS_PK,
            EDGE_DEFERRALS_TABLE,
        )

        db = self.context.db
        db.execute(CREATE_EDGE_DEFERRALS_TABLE_SQL)
        db.commit()
        self.context.register_table(EDGE_DEFERRALS_TABLE, EDGE_DEFERRALS_PK)
        db.commit()
        backend = getattr(self.context, "_backend", None)
        meta = getattr(backend, "tables", {}).get(EDGE_DEFERRALS_TABLE)
        if meta is not None:
            physical = quote_physical_table(meta.physical_name)
            for column in ("src", "dst", "source"):
                index_name = chronos_interval_index_name(
                    meta.physical_name, f"deferral_{column}",
                )
                # A reviewed dynamic CREATE site: scripts/
                # check_migration_drop_recreate.py acknowledges it.
                db.execute(
                    f"CREATE INDEX IF NOT EXISTS {quote_ident(index_name)} "
                    f"ON {physical} ({quote_ident(column)})"
                )
            db.commit()

    def _grant_chronos_public_tables_to_mcp_if_present(self) -> None:
        """Grant MCP read access to Chronos-owned public tables only.

        Chronos creates physical branch/checkpoint tables lazily in ``public``.
        PostgreSQL default privileges cannot be scoped by relation name, so the
        schema bootstrap cannot safely grant all future public tables. Instead,
        every Chronos ensure/write path catches up the concrete ``_chronos*``
        tables that exist at that moment.

        This is the scoped catch-up ``runtime/sql/role_split.sql`` used to
        duplicate with a blanket ``GRANT SELECT ON ALL TABLES`` (removed by
        ``schema-grant-coherence``); the enumerated ``_chronos*`` set is the
        registry, here and in ``okg.grant_registry``'s pattern row.

        Grants that exhaust their retry budget are enqueued in
        ``okg.pending_grants`` and re-attempted at the top of the next run --
        which the epoch memo below schedules on the next registry epoch change.
        ``okg doctor --check pending-grants`` reports whatever is still queued,
        so a grant that never gets a further epoch is visible rather than lost.
        """
        db = self.context.db
        if getattr(db, "dialect", None) != "postgres":
            return
        # Run the grant catch-up at most once per registry epoch per database.
        # The epoch advances whenever chronos registers a new physical table, so
        # a freshly-created _chronos* table always triggers a re-grant; in
        # steady state (no new table) the ~6x/commit DO-block re-fires are
        # skipped.
        from chronos_core.branching._common import registry_epoch

        conn = getattr(db, "raw_connection", None)
        guard_key = _mcp_grant_db_key(conn) if conn is not None else None
        current_epoch = registry_epoch(db)
        if (
            guard_key is not None
            and _MCP_GRANTED_AT_EPOCH.get(guard_key) == current_epoch
        ):
            return
        # Concurrent sources (the ingest layer runs several in one layer, as
        # separate PROCESSES, so the epoch memo above cannot serialize them) run
        # this block simultaneously and contend for the same pg_class ACL tuple.
        # An unhandled error here fails the whole source -- an unguarded GRANT
        # took down `pact`, then ingest, then `okg init`, then the golden path.
        #
        # Every grant is therefore COLLECTED here and applied below through one
        # shared handler: one place to get the concurrency handling right,
        # instead of four near-copies that drift apart.
        db.execute(
            """
            DO $okg_mcp_chronos_grants$
            DECLARE
              rec         record;
              stmt        text;
              stmts       text[] := '{}'::text[];
              queued      text[] := '{}'::text[];
              contended   boolean;
              denied      boolean;
              last_error  text;
              queue_ready boolean := to_regclass('okg.pending_grants')
                                       IS NOT NULL;
              -- The waiting-edge table (pact cross-source-endpoint-wedge) is
              -- never served to a read role: its physical tables, current
              -- and every schema version, stay out of the pattern grant
              -- below and okg_mcp loses any grant an earlier run made.
              withheld    text[] := '{}'::text[];
            BEGIN
              IF to_regclass('public._chronos_branch_tables') IS NOT NULL THEN
                withheld := withheld || ARRAY(
                  SELECT physical_table FROM public._chronos_branch_tables
                   WHERE table_name = 'okg.graph_edge_deferrals'
                );
              END IF;
              IF to_regclass(
                   'public._chronos_branch_table_schema_versions'
                 ) IS NOT NULL THEN
                withheld := withheld || ARRAY(
                  SELECT physical_table
                    FROM public._chronos_branch_table_schema_versions
                   WHERE table_name = 'okg.graph_edge_deferrals'
                );
              END IF;
              -- Collect the retry queue, but do NOT apply it first. A grant
              -- that exhausted its retries on an earlier run is queued in
              -- okg.pending_grants and replayed here, so the next epoch change
              -- repairs it rather than leaving a privilege missing until it
              -- resurfaces as an unrelated permission-denied in some other
              -- subsystem.
              --
              -- ORDER IS LOCK ORDER. Concurrent sources run this same block in
              -- separate processes and take ACL-tuple locks in the order the
              -- statements are emitted; two processes emitting the same
              -- statements in DIFFERENT orders deadlock on pg_class. The queue
              -- is per-database and its contents differ from one run to the
              -- next, so it is appended AFTER the catalog-derived statements
              -- and deduplicated first-occurrence-wins below. A queued
              -- statement that the catalog scan re-derives therefore runs at
              -- its derived position, and the derived sequence is byte-for-byte
              -- the order this block has always used.
              IF queue_ready THEN
                FOR rec IN SELECT statement FROM okg.pending_grants
                            ORDER BY statement
                LOOP
                  -- An earlier build revoked the waiting table from the
                  -- remediator (#4057). A queued copy of that REVOKE would
                  -- replay after the GRANT below and undo it: drop it. (A
                  -- regex, not LIKE: psycopg reads a percent sign as a
                  -- parameter marker.)
                  IF rec.statement ~
                       '^REVOKE ALL ON TABLE .* FROM projection_remediator$'
                  THEN
                    DELETE FROM okg.pending_grants
                     WHERE statement = rec.statement;
                    CONTINUE;
                  END IF;
                  queued := queued || rec.statement;
                END LOOP;
              END IF;

              IF EXISTS (
                SELECT 1 FROM pg_roles
                 WHERE rolname = 'projection_remediator'
              ) THEN
                stmts := stmts || (
                  'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ' ||
                  'okg.graph_nodes, okg.graph_edges TO projection_remediator'
                );
                -- Writing edges reads the deployment's edge ranking, so a
                -- remediation that writes facts must READ it (PACT
                -- remediator-nomos-policy-read). Read only.
                IF to_regclass('okg.edge_precedence') IS NOT NULL THEN
                  stmts := stmts || (
                    'GRANT SELECT ON TABLE okg.edge_precedence ' ||
                    'TO projection_remediator'
                  );
                END IF;
              END IF;

              FOR rec IN
                SELECT quote_ident(n.nspname) || '.' ||
                       quote_ident(c.relname) AS relation_name
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public'
                   AND c.relkind IN ('r', 'p')
                   AND left(c.relname, 8) = '_chronos'
                   AND NOT (c.relname = ANY (withheld))
                 -- ORDER IS LOCK ORDER. Without it this is catalog order,
                 -- which two sessions need not agree on -- and a second
                 -- emitter walking the same relations in a different order is
                 -- exactly the 40P01 deadlock this block already hit once.
                 ORDER BY 1
              LOOP
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
                  stmts := stmts || (
                    'GRANT SELECT ON TABLE ' || rec.relation_name ||
                    ' TO okg_mcp'
                  );
                END IF;
                IF EXISTS (
                  SELECT 1 FROM pg_roles
                   WHERE rolname = 'projection_remediator'
                ) THEN
                  stmts := stmts || (
                    'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ' ||
                    rec.relation_name || ' TO projection_remediator'
                  );
                END IF;
              END LOOP;

              FOR rec IN
                SELECT quote_ident(n.nspname) || '.' ||
                       quote_ident(c.relname) AS relation_name
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public'
                   AND c.relkind = 'S'
                   AND left(c.relname, 8) = '_chronos'
                   AND NOT (c.relname = ANY (withheld))
                 ORDER BY 1
              LOOP
                IF EXISTS (
                  SELECT 1 FROM pg_roles
                   WHERE rolname = 'projection_remediator'
                ) THEN
                  stmts := stmts || (
                    'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' ||
                    rec.relation_name || ' TO projection_remediator'
                  );
                END IF;
              END LOOP;

              FOR rec IN
                SELECT quote_ident(n.nspname) || '.' ||
                       quote_ident(c.relname) AS relation_name
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public'
                   AND c.relkind IN ('r', 'p')
                   AND c.relname = ANY (withheld)
                 ORDER BY 1
              LOOP
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
                  stmts := stmts || (
                    'REVOKE ALL ON TABLE ' || rec.relation_name ||
                    ' FROM okg_mcp'
                  );
                END IF;
                -- The remediator is a writer, not a read role: its retraction
                -- withdraws the producer's waiting rows with the edges, an
                -- interval splice that reads, inserts, updates and deletes
                -- physical rows. It gets the same set as every other _chronos
                -- table (#4057 revoked it, so alias remediation failed).
                IF EXISTS (
                  SELECT 1 FROM pg_roles
                   WHERE rolname = 'projection_remediator'
                ) THEN
                  stmts := stmts || (
                    'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ' ||
                    rec.relation_name || ' TO projection_remediator'
                  );
                END IF;
              END LOOP;

              -- A queued statement is usually also re-derived by the catalog
              -- scan above (the relation still exists), so collapse duplicates
              -- rather than issuing the same GRANT twice. First occurrence
              -- wins and the surviving order is the DERIVED order --
              -- `array_agg(DISTINCT ...)` would sort instead, which silently
              -- re-orders the lock acquisitions and deadlocks concurrent
              -- sources against each other on pg_class.
              stmts := stmts || queued;
              SELECT coalesce(array_agg(d.s ORDER BY d.first_seen), '{}'::text[])
                INTO stmts
                FROM (
                  SELECT u.s, min(u.ord) AS first_seen
                    FROM unnest(stmts) WITH ORDINALITY AS u(s, ord)
                   GROUP BY u.s
                ) AS d;

              -- ONE handler, reached by every grant. Retrying is expressed as a
              -- bounded attempt loop rather than a nested BEGIN, so there is no
              -- second copy of these rules to drift out of sync.
              FOREACH stmt IN ARRAY stmts LOOP
                contended := false;
                denied := false;
                last_error := NULL;
                FOR attempt IN 1..3 LOOP
                  BEGIN
                    EXECUTE stmt;
                    contended := false;
                    denied := false;
                    EXIT;
                  EXCEPTION
                    -- The relation or role vanished after the catalog scan.
                    -- Genuinely nothing to do, and nothing to retry.
                    WHEN undefined_table OR undefined_object THEN
                      EXIT;
                    -- NOT the same thing, and it used to share the branch
                    -- above: "this session may not grant that" leaves the
                    -- privilege ABSENT. Sharing the branch meant a denial fell
                    -- through to the dequeue below and erased its own queue
                    -- row, so a grant that never landed stopped being visible.
                    WHEN insufficient_privilege THEN
                      denied := true;
                      last_error := SQLERRM;
                      EXIT;
                    WHEN internal_error THEN
                      -- `internal_error` is the XX000 CATEGORY code, so it also
                      -- matches XX001 data_corrupted and XX002 index_corrupted.
                      -- Only the concurrency error is benign; corruption must
                      -- stay loud, on every attempt.
                      IF SQLSTATE <> 'XX000' THEN
                        RAISE;
                      END IF;
                      -- "tuple concurrently updated": a competing session
                      -- updated this relation's pg_class row. Retry rather than
                      -- swallow -- the competing statement need NOT have been
                      -- the same GRANT (a concurrent ALTER TABLE ... ADD COLUMN
                      -- from ensure_schema is enough), and in that case
                      -- swallowing would silently drop the privilege. A catalog
                      -- update waits for the competing transaction, so by now it
                      -- has committed and the next attempt sees the new tuple.
                      contended := true;
                      last_error := SQLERRM;
                  END;
                END LOOP;

                -- Exhausting the retries is the one path that can still drop a
                -- grant. It needs three consecutive collisions, each gated by a
                -- blocking wait, so it is rare -- but rare AND invisible is how
                -- a missing privilege resurfaces later as an unrelated
                -- permission-denied.
                --
                -- `schema-grant-coherence` req.scoped-catchup: it is no longer
                -- invisible. The statement is enqueued in okg.pending_grants,
                -- drained at the top of this block on the next epoch change,
                -- and reported by `okg doctor --check pending-grants` until it
                -- applies. The server-log WARNING stays -- it is the only
                -- signal a database too old for the queue table can give.
                --
                -- The queue write is OUTSIDE the exception block above on
                -- purpose: a PL/pgSQL BEGIN...EXCEPTION is a subtransaction, so
                -- a failure in the bookkeeping would roll back the GRANT it was
                -- recording.
                --
                -- Concatenated USING MESSAGE rather than a RAISE format
                -- placeholder: this SQL goes through psycopg, which parses the
                -- placeholder character as a parameter marker and rejects it.
                IF contended OR denied THEN
                  RAISE WARNING USING MESSAGE =
                    'okg: grant not applied, queued for retry ('
                    || CASE WHEN denied THEN 'insufficient privilege'
                            ELSE 'concurrent-update retries exhausted' END
                    || '): ' || stmt;
                  IF queue_ready THEN
                    INSERT INTO okg.pending_grants
                      (statement, last_error)
                    VALUES (stmt, last_error)
                    ON CONFLICT (statement) DO UPDATE
                      SET attempts        = okg.pending_grants.attempts + 1,
                          last_attempt_at = now(),
                          last_error      = EXCLUDED.last_error;
                  END IF;
                ELSIF queue_ready THEN
                  -- Applied, or moot (the relation or role vanished). ONLY
                  -- those two paths reach here: a denial used to fall through
                  -- and erase its own queue row, which made an unapplied grant
                  -- invisible again.
                  DELETE FROM okg.pending_grants WHERE statement = stmt;
                END IF;
              END LOOP;
            END
            $okg_mcp_chronos_grants$
            """
        )
        # Record only after the grants ran without escaping an error, so a hard
        # failure re-attempts on the next call rather than being skipped. Re-read
        # the epoch in case a concurrent registration advanced it during the
        # grant. Note the block absorbs the benign races above, so this does not
        # promise every individual grant landed -- only that none of them failed
        # in a way worth retrying at this level.
        if guard_key is not None:
            _MCP_GRANTED_AT_EPOCH[guard_key] = registry_epoch(db)

    def _ensure_interval_source_read_indexes(self) -> None:
        if getattr(self.context, "backend_name", None) != "interval":
            return
        try:
            tables = self._interval_table_metadata()
        except Exception:  # noqa: BLE001 - metadata may be absent on fresh DBs
            return

        active_hot: dict[tuple[str, str], dict[str, Any]] = {}
        manifest_reg = self.context.db.execute(
            "SELECT to_regclass(?) AS reg",
            (HOT_INDEX_MANIFEST_TABLE,),
        ).fetchone()
        if manifest_reg is not None and manifest_reg["reg"] is not None:
            rows = self.context.db.execute(
                """
                SELECT physical_table, suffix, hot_index_name, floor::text AS floor
                  FROM okg.chronos_hot_index_manifest
                 WHERE state IN ('activating', 'active')
                 ORDER BY activated_at DESC NULLS LAST, created_at DESC
                """
            ).fetchall()
            for row in rows:
                active_hot.setdefault(
                    (row["physical_table"], row["suffix"]),
                    {
                        "hot_index_name": row["hot_index_name"],
                        "floor": row["floor"],
                    },
                )

        for logical_table, table_specs in INTERVAL_SOURCE_READ_INDEX_SPECS.items():
            table = tables.get(logical_table)
            if not table:
                continue
            physical = quote_physical_table(table["physical_table"])
            columns = set(table["columns"]) | {"live_lo", "live_hi", "deleted"}
            for spec in table_specs:
                suffix = spec.suffix
                index_columns = spec.columns
                predicate = spec.index_predicate()
                if not set(index_columns).issubset(columns):
                    continue
                hot = active_hot.get((table["physical_table"], suffix))
                if hot is not None:
                    # The online rotation tool is the only writer of hot
                    # definitions: ensure_schema must never rebuild a multi-GB
                    # index non-concurrently.  Once the manifest says the hot
                    # definition is authoritative, verify it and refuse loudly
                    # if it is absent/invalid instead of recreating the full
                    # definition under a SHARE lock.
                    hot_rows = self.context.db.execute(
                        """
                        SELECT x.indisvalid, x.indisready,
                               t.relname AS table_name,
                               pg_get_expr(x.indpred, x.indrelid) AS predicate,
                               ARRAY(
                                 SELECT pg_get_indexdef(
                                   i.oid, key_position, TRUE
                                 )
                                   FROM generate_series(
                                     1, x.indnkeyatts
                                   ) AS key_position
                                  ORDER BY key_position
                               ) AS indexed_columns
                          FROM pg_class AS i
                          JOIN pg_index AS x ON x.indexrelid = i.oid
                          JOIN pg_class AS t ON t.oid = x.indrelid
                         WHERE i.oid = to_regclass(?)
                        """,
                        (hot["hot_index_name"],),
                    ).fetchall()
                    if (
                        len(hot_rows) != 1
                        or not hot_index_definition_matches(
                            hot_rows[0],
                            physical_table=table["physical_table"],
                            spec=spec,
                            floor=int(hot["floor"]),
                        )
                    ):
                        raise SubstrateError(
                            "active Chronos hot index is absent or invalid",
                            data={
                                "physical_table": table["physical_table"],
                                "suffix": suffix,
                                "hot_index_name": hot["hot_index_name"],
                                "floor": hot["floor"],
                            },
                            fix_hint=(
                                "inspect `okg db cold-history --deployment <name>`; "
                                "do not rebuild the full index manually"
                            ),
                        )
                    continue
                index_name = chronos_interval_index_name(
                    table["physical_table"],
                    suffix,
                )
                if index_name in DEFERRED_INDEX_BUILDS.get():
                    # okg#3112: migrate's online step could not build it
                    # concurrently this time; a plain CREATE INDEX here would
                    # block every graph write for the whole build. The next
                    # migrate or the post-publish index rebuild builds it.
                    continue
                # A plain spec renders exactly as before PACT
                # lane-s-redesign-s1 (quoted columns, no USING clause); an
                # extended one (the GIN index) adds its access method,
                # operator class, key expressions and own predicate.
                quoted_columns = ", ".join(spec.key_sql(quote_ident))
                using = f"USING {spec.method} " if spec.method else ""
                existing_def = self.context.db.execute(
                    "SELECT indexdef FROM pg_indexes WHERE indexname = ?",
                    (index_name,),
                ).fetchall()
                if existing_def and (
                    spec_index_definition_matches(
                        existing_def[0]["indexdef"], spec
                    )
                    if spec.extended
                    else index_matches_columns(
                        existing_def[0]["indexdef"],
                        index_columns,
                    )
                ) is False:
                    self.context.db.execute(
                        f"DROP INDEX IF EXISTS {quote_ident(index_name)}"
                    )
                self.context.db.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {quote_ident(index_name)}
                      ON {physical} {using}({quoted_columns})
                    {"" if predicate is None else f"WHERE {predicate}"}
                    """
                )

        expression_specs = {
            NODE_TABLE: (
                (
                    "node_subtype_path_visible",
                    ("subtype", "(attrs->>'path')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_subtype_file_path_visible",
                    ("subtype", "(attrs->>'file_path')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_semantic_key_visible",
                    ("subtype", "(attrs->>'semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_canonical_name_visible",
                    ("subtype", "(attrs->>'canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_target_sk_visible",
                    ("subtype", "(attrs->>'target_semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_semantic_target_sk_vis",
                    ("subtype", "(attrs->>'semantic_target_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_symbol_sk_visible",
                    ("subtype", "(attrs->>'symbol_semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_code_symbol_sk_vis",
                    ("subtype", "(attrs->>'code_symbol_semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_handler_sk_visible",
                    ("subtype", "(attrs->>'handler_semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_function_sk_visible",
                    ("subtype", "(attrs->>'function_semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_source_sk_visible",
                    ("subtype", "(attrs->>'source_semantic_key')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_target_cn_visible",
                    ("subtype", "(attrs->>'target_canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_symbol_cn_visible",
                    ("subtype", "(attrs->>'symbol_canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_code_symbol_cn_vis",
                    ("subtype", "(attrs->>'code_symbol_canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_handler_cn_visible",
                    ("subtype", "(attrs->>'handler_canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_function_cn_visible",
                    ("subtype", "(attrs->>'function_canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
                (
                    "node_attr_source_cn_visible",
                    ("subtype", "(attrs->>'source_canonical_name')", "live_lo", "live_hi"),
                    "deleted = FALSE",
                ),
            ),
        }
        attr_path_allowlist = _node_attr_path_index_allowlist()
        for logical_table, table_specs in expression_specs.items():
            table = tables.get(logical_table)
            if not table:
                continue
            columns = set(table["columns"]) | {"live_lo", "live_hi", "deleted"}
            if "attrs" not in columns:
                continue
            physical = quote_physical_table(table["physical_table"])
            for suffix, index_parts, predicate in table_specs:
                index_name = chronos_interval_index_name(
                    table["physical_table"],
                    suffix,
                )
                # None => all; a set => only those suffixes. Anything not
                # selected is dropped so the choice sticks across ensure_schema.
                if not (attr_path_allowlist is None or suffix in attr_path_allowlist):
                    self.context.db.execute(
                        f"DROP INDEX IF EXISTS {quote_ident(index_name)}"
                    )
                    continue
                rendered_parts = ", ".join(
                    quote_ident(part) if part in columns else part
                    for part in index_parts
                )
                self.context.db.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {quote_ident(index_name)}
                      ON {physical} ({rendered_parts})
                     WHERE {predicate}
                    """
                )

        # pg_textsearch BM25 index for lexical node search (PACT
        # restore-bm25-lexical-node-search). The legacy nodes_live_text_bm25_idx was
        # orphaned by the chronos-native cutover -- it targets the dropped
        # okg.nodes_live, while the live data lives here on the interval segment, so
        # both the console search and the akmon.search.lexical operator regressed to
        # full scans. USING bm25 (...) WITH (text_config=...) cannot go through the
        # plain expression-index path above, so it gets its own emit.
        #
        # Indexes ALL non-deleted text rows. Visibility is NOT baked into the index:
        # the chronos open-interval live_hi value VARIES per database, so a sentinel
        # predicate would be only incidentally correct. Instead BM25 queries (which
        # bypass the sqlglot checkout, unable to parse <@>) apply the pin's branch-point
        # visibility filter (live_lo <= bp AND bp < live_hi) at read time -- honoring
        # the pinned generation for any deployment and any pin. The index is thus
        # pin-agnostic; retired versions are filtered out by that read-time predicate.
        # Ranked via (attrs->>'text') <@> to_bm25query(q); see
        # okg.substrate.bm25_node_search, console_views/search.py, and
        # mcp/search.py::_search_lexical. pg_textsearch is a required, shared_preload
        # extension (db/migrate.py), installed before this path runs.
        for logical_table, table_specs in BM25_INDEX_SPECS.items():
            table = tables.get(logical_table)
            if not table:
                continue
            columns = set(table["columns"]) | {"live_lo", "live_hi", "deleted"}
            if "attrs" not in columns:
                continue
            physical = quote_physical_table(table["physical_table"])
            for spec in table_specs:
                suffix, index_expr, predicate = spec.suffix, spec.index_expr, spec.predicate
                index_name = chronos_interval_index_name(
                    table["physical_table"],
                    suffix,
                )
                # Self-heal a stale-predicate index: an interim revision built this
                # index PARTIAL on the open-interval live_hi sentinel (which varies per
                # database, so it was only incidentally correct). The current spec
                # indexes all non-deleted text rows and applies visibility at read time.
                # CREATE INDEX IF NOT EXISTS would keep the wrong build, so drop any
                # existing index whose definition still carries a live_hi bound and let
                # the CREATE below rebuild it pin-agnostic.
                existing = self.context.db.execute(
                    "SELECT indexdef FROM pg_indexes WHERE indexname = ?",
                    (index_name,),
                ).fetchall()
                if existing and "live_hi" in (existing[0]["indexdef"] or ""):
                    self.context.db.execute(
                        f"DROP INDEX IF EXISTS {quote_ident(index_name)}"
                    )
                _create_partial_index(
                    self.context.db, index_name, physical,
                    f"USING bm25 ({index_expr}) WITH (text_config='english')",
                    predicate,
                )

        # `search(method="grep")` (PACT mcp-grep-3067, issue #3067, decision
        # 168a): a pg_trgm GIN index on node text (engine E1) answers `~*`
        # with an exact recheck, and a text_pattern_ops btree on `attrs.path`
        # (P1) answers `path_prefix`. Both are pin-agnostic like the BM25
        # index: they cover every non-deleted row carrying the key, and the
        # read applies the branch-point visibility predicate. pg_trgm is a
        # required extension (db/migrate.py REQUIRED_EXTENSIONS). Measured
        # cost: DESIGN.md section 3 and this change's evidence.
        for logical_table, table_specs in GREP_INDEX_SPECS.items():
            table = tables.get(logical_table)
            if not table:
                continue
            columns = set(table["columns"]) | {"live_lo", "live_hi", "deleted"}
            if "attrs" not in columns:
                continue
            physical = quote_physical_table(table["physical_table"])
            for spec in table_specs:
                suffix, index_expr, predicate = spec.suffix, spec.index_expr, spec.predicate
                index_name = chronos_interval_index_name(
                    table["physical_table"],
                    suffix,
                )
                existing = self.context.db.execute(
                    "SELECT indexdef FROM pg_indexes WHERE indexname = ?",
                    (index_name,),
                ).fetchall()
                if (
                    existing
                    and suffix == "node_path_prefix_visible"
                    and not _is_bounded_path_index(existing[0]["indexdef"])
                ):
                    # An earlier build indexed the unbounded path, which
                    # fails on a path past the btree row limit.
                    self.context.db.execute(
                        f"DROP INDEX IF EXISTS {quote_ident(index_name)}"
                    )
                _create_partial_index(
                    self.context.db, index_name, physical, index_expr, predicate,
                )

    def _interval_table_metadata(self) -> dict[str, dict[str, Any]]:
        rows = self.context.db.execute(
            """
            SELECT table_name, physical_table, columns
              FROM _chronos_branch_tables
             WHERE backend = 'interval'
               AND table_name IN (?, ?)
            """,
            (NODE_TABLE, EDGE_TABLE),
        ).fetchall()
        tables = {
            row["table_name"]: {
                "physical_table": row["physical_table"],
                "columns": tuple(json.loads(row["columns"])),
            }
            for row in rows
        }
        missing = {NODE_TABLE, EDGE_TABLE} - set(tables)
        if missing:
            raise SubstrateError(
                "Chronos graph tables are not registered for compaction",
                data={"missing_tables": sorted(missing)},
            )
        return tables
