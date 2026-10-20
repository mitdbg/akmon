"""Approximate nearest-neighbour search for ``search(method="semantic")``.

PACT openrouter-embeddings-hnsw. A backend whose model returns vectors
narrower than the column (``openrouter:<model>@<dims>``, zero-padded to 2560)
gets one pgvector HNSW index on the Chronos node segment:

    USING hnsw ((subvector(embedding, 1, <dims>)::halfvec(<dims>)) halfvec_cosine_ops)
    WHERE deleted = FALSE AND embedding_model = '<model id>'

* **The first ``<dims>`` values only.** The padding is zeros, so the cosine
  distance over the first ``<dims>`` values equals the distance over all
  2560. Indexing 768 values instead of 2560 makes the index about four times
  smaller and faster to build and search. ``halfvec`` (16-bit floats) halves
  it again; HNSW over ``vector`` stops at 2000 dimensions, ``halfvec`` at 4000.
* **One model.** The predicate names the model, so a vector another model
  wrote is never ranked against this model's query.
* **Pin-agnostic, like the BM25 index.** Visibility is not part of the index;
  the read applies the pinned generation's branch point
  (``live_lo <= bp AND bp < live_hi``) itself.

**The read does not go through the Chronos checkout.** The checkout rewrites
SQL through sqlglot, which renders pgvector's ``<=>`` as ``IS NOT DISTINCT
FROM`` (measured on this branch). HNSW serves only ``ORDER BY <indexed
expression> <=> <query> LIMIT n``, so this module queries the physical node
segment directly with the visibility predicate, the same way
:mod:`okg.substrate.bm25_node_search` does for ``<@>``.

**Fails loudly.** An indexed backend with no valid index refuses with
``semantic_index_unavailable`` rather than scanning every vector.

**The index, not a sort.** The read disables sorting for its one statement,
so the planner cannot prefer a visibility B-tree plus a sort over every
visible vector.

**Filters.** ``hnsw.iterative_scan = relaxed_order`` keeps scanning the graph
when the pin, subtype or source filter drops candidates, up to
``hnsw.max_scan_tuples``; an outer ``ORDER BY`` restores exact order among the
returned rows. When fewer rows come back than were asked for, the read counts
the rows that match the filters to tell "the ranking ended" from "the scan
stopped at its bound"; the caller reports the second as a capped result.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from typing import Any, Callable, Optional, Sequence

from psycopg import sql
from psycopg.rows import dict_row, tuple_row

from okg.substrate.embeddings.documents import (
    NONBLANK_BODY_REGEX,
    nonblank_body_sql,
    text_value_sql,
)
from okg.substrate.embeddings.identity import native_dimension_for_model_id
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.common import (
    NODE_TABLE,
    chronos_interval_index_name,
    quote_ident,
    quote_physical_table,
)


__all__ = [
    "ANN_INDEX_SUFFIX",
    "AnnIndexSpec",
    "AnnRanking",
    "EXACT_SCOPE_MAX",
    "SemanticIndexUnavailable",
    "SemanticQueryEmbeddingFailed",
    "SemanticScopeTooLarge",
    "UNINDEXED_SCOPE_MAX",
    "VectorSetRead",
    "ann_rank",
    "inspect_set_index",
    "prepare_set_read",
    "query_vector_for_set",
    "rank_vector_set",
    "set_coverage",
    "ann_spec_for_model_id",
    "require_ann_index",
    "ensure_ann_index",
    "inspect_ann_index",
]

log = logging.getLogger(__name__)

ANN_INDEX_SUFFIX = "node_embedding_hnsw"
#: HNSW build parameters (pgvector defaults, stated so a rebuild is
#: reproducible).
HNSW_M = 16
HNSW_EF_CONSTRUCTION = 64
#: Candidate-list size per search, at most pgvector's maximum. The floor is
#: above pgvector's default of 40: on 50k clustered 768-dim vectors (500
#: clusters) recall@10 against the exact ranking was 0.95 at 40 and 0.98 at
#: 100, at about the same latency (2026-10-09, pgvector 0.8.6).
EF_SEARCH_MIN = 100
EF_SEARCH_MAX = 1000
#: How many index tuples an iterative scan may visit before it stops.
MAX_SCAN_TUPLES = 100_000
DEFAULT_BUILD_MAINTENANCE_WORK_MEM = "2GB"
_MEMORY_SETTING_RE = re.compile(r"^[0-9]+(?:kB|MB|GB)$")


class SemanticIndexUnavailable(SubstrateError):
    """The model's HNSW index is missing, invalid, or built for another model.

    Raised instead of falling back to a scan of every vector."""

    code = "semantic_index_unavailable"


@dataclass(frozen=True)
class AnnIndexSpec:
    """The HNSW index one embedding model is ranked through."""

    model_id: str
    native_dimension: int

    @property
    def key_expression(self) -> str:
        d = int(self.native_dimension)
        return f"(subvector(embedding, 1, {d})::halfvec({d}))"

    @property
    def predicate(self) -> str:
        literal = self.model_id.replace("'", "''")
        return f"deleted = FALSE AND embedding_model = '{literal}'"

    def index_body(self) -> str:
        return (
            f"USING hnsw ({self.key_expression} halfvec_cosine_ops) "
            f"WITH (m = {HNSW_M}, ef_construction = {HNSW_EF_CONSTRUCTION}) "
            f"WHERE {self.predicate}"
        )

    def matches_definition(self, indexdef: str) -> bool:
        """Whether a ``pg_get_indexdef`` text is this spec's build."""
        flat = re.sub(r'[\s"()]', "", indexdef or "").lower()
        d = int(self.native_dimension)
        model = self.model_id.lower().replace("'", "''")
        return (
            "usinghnsw" in flat
            and f"subvectorembedding,1,{d}::halfvec{d}" in flat
            and "halfvec_cosine_ops" in flat
            and f"embedding_model='{model}'::text" in flat
            and "deleted=false" in flat
        )


@dataclass(frozen=True)
class AnnRanking:
    rows: list[dict[str, Any]]
    #: True when the returned rows are every row the filters admit, i.e. the
    #: ranking ended inside the requested window.
    exhausted: bool
    #: True when the scan stopped at ``MAX_SCAN_TUPLES`` with matching rows
    #: left unranked.
    capped: bool


def ann_spec_for_model_id(model_id: Optional[str]) -> Optional[AnnIndexSpec]:
    """The HNSW spec for a model, or ``None`` when the model is not indexed.

    Only a model narrower than the column is indexed (today the
    ``openrouter:`` family). ``hash`` and sentence-transformer corpora keep the
    exact scan they had.
    """
    if not model_id:
        return None
    dims = native_dimension_for_model_id(model_id)
    if dims is None:
        return None
    return AnnIndexSpec(model_id=model_id, native_dimension=dims)


def _node_segment(conn: Any) -> str:
    physical = _registered_node_segment(conn)
    if physical is None:
        raise SemanticIndexUnavailable(
            "the Chronos node table is not registered, so it has no "
            "semantic index",
            data={"table": NODE_TABLE},
            fix_hint="run `okg migrate` against this deployment",
        )
    return physical


def _registered_node_segment(conn: Any) -> Optional[str]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('public._chronos_branch_tables') IS NOT NULL")
        registry = cur.fetchone()
        if not registry or not registry[0]:
            return None
        cur.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = %s",
            (NODE_TABLE,),
        )
        row = cur.fetchone()
    return str(row[0]) if row else None


#: The first pgvector release with ``hnsw.iterative_scan``. Without it an
#: HNSW scan under the pin and scope filters returns at most ``ef_search``
#: candidates and silently under-fills a page.
MIN_PGVECTOR_VERSION = (0, 8)


def _pgvector_version(conn: Any) -> Optional[str]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        row = cur.fetchone()
    return str(row[0]) if row else None


def _version_supported(version: Optional[str]) -> bool:
    if not version:
        return False
    parts = []
    for piece in version.split(".")[:2]:
        digits = "".join(ch for ch in piece if ch.isdigit())
        parts.append(int(digits or 0))
    return tuple(parts) >= MIN_PGVECTOR_VERSION


def inspect_ann_index(conn: Any, spec: AnnIndexSpec) -> dict[str, Any]:
    """State of the spec's index.

    ``unsupported`` (pgvector older than 0.8), ``missing``, ``building`` (a
    ``CREATE INDEX CONCURRENTLY`` in progress, from
    ``pg_stat_progress_create_index``), ``invalid`` (an interrupted build),
    ``mismatched`` (built for another model or width) or ``present``.
    """
    physical = _node_segment(conn)
    index_name = chronos_interval_index_name(physical, ANN_INDEX_SUFFIX)
    version = _pgvector_version(conn)
    report: dict[str, Any] = {
        "model_id": spec.model_id,
        "native_dimension": spec.native_dimension,
        "physical_table": physical,
        "index_name": index_name,
        "pgvector_version": version,
    }
    if not _version_supported(version):
        return {**report, "status": "unsupported"}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT pg_get_indexdef(i.indexrelid) AS indexdef, "
            "       i.indisvalid AS valid, "
            "       pg_relation_size(i.indexrelid) AS bytes, "
            # A build by another role shows NULL ids here unless this role
            # has pg_read_all_stats; such a row counts as building too, so an
            # unidentifiable build in this database is never dropped.
            "       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p "
            "                WHERE p.index_relid = i.indexrelid "
            "                   OR (p.index_relid IS NULL "
            "                       AND p.datname = current_database())) "
            "         AS building "
            "  FROM pg_index AS i "
            " WHERE i.indexrelid = to_regclass(%s)",
            (quote_ident(index_name),),
        )
        row = cur.fetchone()
    if row is None:
        return {**report, "status": "missing"}
    report.update(indexdef=row["indexdef"], bytes=int(row["bytes"]))
    if not row["valid"]:
        return {**report, "status": "building" if row["building"] else "invalid"}
    if not spec.matches_definition(row["indexdef"]):
        return {**report, "status": "mismatched"}
    return {**report, "status": "present"}


def _model_has_rows(conn: Any, physical_table: str, spec: AnnIndexSpec) -> bool:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"SELECT EXISTS (SELECT 1 FROM {quote_physical_table(physical_table)} "
            "WHERE deleted = FALSE AND embedding_model = %s)",
            (spec.model_id,),
        )
        return bool(cur.fetchone()[0])


def _is_shared_memory_error(exc: BaseException) -> bool:
    """The /dev/shm resize failure of a parallel build, and nothing else.

    "out of shared memory" (the lock table) is a different failure that a
    serial rebuild would not fix, so it is not matched.
    """
    return "could not resize shared memory segment" in str(exc).lower()


def ensure_ann_index(
    dsn: str,
    spec: AnnIndexSpec,
    *,
    apply: bool = True,
    maintenance_work_mem: str = DEFAULT_BUILD_MAINTENANCE_WORK_MEM,
    defer_when_empty: bool = False,
    on_progress: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Build the spec's index with ``CREATE INDEX CONCURRENTLY`` if needed.

    An invalid index (an interrupted earlier build) or one built for another
    model or width is dropped concurrently first. A build in progress is never
    dropped: the call reports ``building`` and does nothing. Writers are never
    blocked. With ``apply=False`` this only reports what it would do.

    ``defer_when_empty`` (what migrate passes) builds nothing while no row
    carries the model: that is the state before a backfill, and an index built
    then would be maintained row by row through the whole backfill.
    ``okg db semantic-index --apply`` builds it afterwards in one pass.

    A parallel HNSW build needs shared memory; in a container with a small
    ``/dev/shm`` (Docker's default is 64 MB) it fails with "could not resize
    shared memory segment". The build then drops its invalid index and runs
    once more with ``max_parallel_maintenance_workers = 0``, and the report
    says so (``parallel_fallback``). Giving the container ``--shm-size``
    larger than ``maintenance_work_mem`` avoids the slower serial build.
    """
    from okg.substrate.db.connection import connect as db_connect

    if not _MEMORY_SETTING_RE.match(maintenance_work_mem):
        raise ValueError(
            f"maintenance_work_mem must look like 2GB, 512MB or 65536kB; "
            f"got {maintenance_work_mem!r}"
        )
    say = on_progress or (lambda _msg: None)
    with db_connect(dsn, autocommit=True) as conn:
        if _registered_node_segment(conn) is None:
            # A fresh database: the graph tables appear with the first
            # write, and the next migrate or semantic-index run builds it.
            return {
                "model_id": spec.model_id,
                "native_dimension": spec.native_dimension,
                "status": "not_registered",
                "action": "none",
            }
        before = inspect_ann_index(conn, spec)
        if before["status"] == "unsupported":
            raise SemanticIndexUnavailable(
                "semantic search through HNSW needs pgvector 0.8 or newer "
                f"(iterative scan); this database has {before['pgvector_version']!r}",
                data={k: v for k, v in before.items() if k != "indexdef"},
                fix_hint="ALTER EXTENSION vector UPDATE on a pgvector >= 0.8 install",
            )
        if before["status"] in ("present", "building"):
            return {**before, "action": "none"}
        action = "rebuild" if before["status"] in ("invalid", "mismatched") else "build"
        if defer_when_empty and not _model_has_rows(
            conn, before["physical_table"], spec,
        ):
            return {**before, "action": "deferred", "reason": "no_rows_for_model"}
        if not apply:
            return {**before, "action": f"would_{action}"}
        index_name = before["index_name"]
        physical = quote_physical_table(before["physical_table"])

        def drop() -> None:
            conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {quote_ident(index_name)}")

        def create() -> None:
            conn.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {quote_ident(index_name)} "
                f"ON {physical} {spec.index_body()}"
            )

        if action == "rebuild":
            say(f"dropping {before['status']} index {index_name}")
            drop()
        say(
            f"building {index_name} for {spec.model_id} "
            f"(maintenance_work_mem={maintenance_work_mem})"
        )
        conn.execute(f"SET maintenance_work_mem = '{maintenance_work_mem}'")
        started = time.monotonic()
        parallel_fallback: Optional[str] = None
        try:
            create()
        except Exception as exc:
            if not _is_shared_memory_error(exc):
                raise
            parallel_fallback = str(exc).splitlines()[0][:200]
            say(f"parallel build ran out of shared memory ({parallel_fallback}); "
                "retrying with max_parallel_maintenance_workers = 0")
            drop()
            conn.execute("SET max_parallel_maintenance_workers = 0")
            create()
            conn.execute("RESET max_parallel_maintenance_workers")
        seconds = round(time.monotonic() - started, 3)
        conn.execute("RESET maintenance_work_mem")
        after = inspect_ann_index(conn, spec)
    if after["status"] != "present":
        raise SemanticIndexUnavailable(
            f"building the semantic index left it {after['status']}",
            data={k: v for k, v in after.items() if k != "indexdef"},
            fix_hint="rerun `okg db semantic-index --apply`; it drops an "
            "invalid build first",
        )
    result = {**after, "action": action, "build_seconds": seconds}
    if parallel_fallback is not None:
        result["parallel_fallback"] = parallel_fallback
    return result


def _vector_literal(values: Sequence[float]) -> str:
    return "[" + ",".join(f"{float(v):.7g}" for v in values) + "]"


def ann_statements(
    spec: AnnIndexSpec,
    physical_table: str,
    *,
    scoped_subtypes: bool = False,
    scoped_source_ids: bool = False,
) -> tuple[str, str]:
    """``(ranking statement, admitted-row count statement)`` for :func:`ann_rank`.

    Parameters: ``model_id``, ``bp``, ``q`` (the native-width vector literal),
    ``limit``, and ``scoped_subtypes`` / ``scoped_source_ids`` when scoped.
    The ORDER BY repeats the index expression exactly, which is what lets the
    planner answer it from the HNSW index.
    """
    d = int(spec.native_dimension)
    physical = quote_physical_table(physical_table)
    filters = [
        "n.deleted = FALSE",
        "n.embedding_model = %(model_id)s",
        "n.live_lo <= %(bp)s",
        "%(bp)s < n.live_hi",
    ]
    if scoped_subtypes:
        filters.append("n.subtype = ANY(%(scoped_subtypes)s)")
    if scoped_source_ids:
        filters.append("n.source = ANY(%(scoped_source_ids)s)")
    where = " AND ".join(filters)
    distance = f"(subvector(n.embedding, 1, {d})::halfvec({d}) <=> %(q)s::halfvec({d}))"
    statement = (
        "WITH ranked AS MATERIALIZED ("
        " SELECT n.node_id, n.subtype, n.attrs, n.text_hash, n.source,"
        "        n.source_record_id, n.source_revision, n.embedding_model,"
        f"        {distance} AS distance"
        f"   FROM {physical} AS n"
        f"  WHERE {where}"
        f"  ORDER BY {distance}"
        "  LIMIT %(limit)s)"
        " SELECT * FROM ranked ORDER BY distance, node_id"
    )
    count_statement = f"SELECT count(*) AS n FROM {physical} AS n WHERE {where}"
    return statement, count_statement


def require_ann_index(conn: Any, spec: AnnIndexSpec) -> dict[str, Any]:
    """The index state, or :class:`SemanticIndexUnavailable` unless present.

    Semantic search calls this BEFORE embedding the query, so a refused
    search makes no paid call.
    """
    state = inspect_ann_index(conn, spec)
    if state["status"] != "present":
        hints = {
            "unsupported": "upgrade pgvector to 0.8 or newer (iterative scan)",
            "building": (
                "wait for the running CREATE INDEX CONCURRENTLY to finish "
                "(`okg db semantic-index --deployment <name>` reports it); "
                "do not rerun --apply"
            ),
        }
        raise SemanticIndexUnavailable(
            f"semantic search for {spec.model_id!r} needs its HNSW index, "
            f"which is {state['status']}",
            data={k: v for k, v in state.items() if k != "indexdef"},
            fix_hint=hints.get(
                state["status"],
                "build it with `okg db semantic-index --deployment <name> "
                "--apply` after the backfill",
            ),
        )
    return state


def ann_rank(
    conn: Any,
    session: Any,
    spec: AnnIndexSpec,
    query_vector: Sequence[float],
    *,
    limit: int,
    scoped_subtypes: Optional[Sequence[str]] = None,
    scoped_source_ids: Optional[Sequence[str]] = None,
) -> AnnRanking:
    """The ``limit`` nearest visible rows of ``spec.model_id`` at the session's pin.

    ``query_vector`` may be padded; only its first ``native_dimension`` values
    are used. Returns the same row keys as the exact semantic program.
    """
    from okg.substrate.bm25_node_search import (
        SearchIndexUnavailable,
        branch_point,
    )

    state = require_ann_index(conn, spec)
    d = spec.native_dimension
    if len(query_vector) < d or any(float(v) != 0.0 for v in query_vector[d:]):
        raise SubstrateError(
            f"the query vector does not have the {d}-dimensional zero-padded "
            f"shape {spec.model_id!r} writes",
            data={"model_id": spec.model_id, "length": len(query_vector)},
        )
    try:
        bp = branch_point(conn, session)
    except SearchIndexUnavailable as exc:
        raise SemanticIndexUnavailable(
            str(exc), data=dict(getattr(exc, "data", None) or {}),
        ) from exc
    statement, count_statement = ann_statements(
        spec,
        state["physical_table"],
        scoped_subtypes=scoped_subtypes is not None,
        scoped_source_ids=scoped_source_ids is not None,
    )
    params: dict[str, Any] = {
        "model_id": spec.model_id,
        "bp": bp,
        "q": _vector_literal(query_vector[:d]),
        "limit": int(limit),
    }
    if scoped_subtypes is not None:
        params["scoped_subtypes"] = list(scoped_subtypes)
    if scoped_source_ids is not None:
        params["scoped_source_ids"] = list(scoped_source_ids)
    ef_search = max(EF_SEARCH_MIN, min(EF_SEARCH_MAX, int(limit)))
    settings = (
        # The visibility B-tree indexes compete with HNSW: when the planner
        # judges the pin filter selective it reads them and sorts every
        # visible vector, an exact scan this path exists to avoid (measured on
        # PostgreSQL 17: 400 and 1,500 analyzed rows chose the
        # subtype-visibility index plus a sort). With sorting disabled for
        # this one statement, the HNSW index is the only ordered path; a sort
        # still runs when no index can serve.
        ("enable_sort", "off"),
        ("hnsw.iterative_scan", "relaxed_order"),
        ("hnsw.ef_search", str(ef_search)),
        ("hnsw.max_scan_tuples", str(MAX_SCAN_TUPLES)),
    )
    admitted: Optional[int] = None
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            # set_config(..., true) is transaction-local, but on a connection
            # that is already inside a transaction `conn.transaction()` is a
            # savepoint, and a released savepoint keeps its settings for the
            # rest of the caller's transaction. So read each value first and
            # put it back right after the ranking statement; a failed
            # statement rolls the savepoint, and with it the settings, back.
            previous: list[tuple[str, Optional[str]]] = []
            for name, value in settings:
                cur.execute("SELECT current_setting(%s, true) AS v", (name,))
                previous.append((name, cur.fetchone()["v"]))
                cur.execute("SELECT set_config(%s, %s, true)", (name, value))
            # Planned with its own parameters, never a generic plan, so the
            # ORDER BY keeps matching the index expression.
            cur.execute(statement, params, prepare=False)
            rows = [dict(row) for row in cur.fetchall()]
            for name, value in previous:
                if value is None:
                    cur.execute(sql.SQL("RESET {}").format(sql.Identifier(*name.split("."))))
                else:
                    cur.execute("SELECT set_config(%s, %s, true)", (name, value))
            exhausted = len(rows) < int(limit)
            capped = False
            if exhausted:
                cur.execute(count_statement, params, prepare=False)
                admitted = int(cur.fetchone()["n"])
                if admitted > len(rows):
                    exhausted, capped = False, True
    if capped:
        log.info(
            "semantic ann: scan bound reached for %s: %d of %d admitted rows "
            "ranked (limit %d)", spec.model_id, len(rows), admitted, limit,
        )
    return AnnRanking(rows=rows, exhausted=exhausted, capped=capped)


# --------------------------------------------------------------------------- #
# Vector sets (PACT openrouter-embeddings-hnsw, design v3.2)                   #
# --------------------------------------------------------------------------- #

#: A scope of at most this many visible text nodes (visible at the pin, of a
#: ranked subtype and source, readable under the reader's access rules) is
#: ranked exactly: every eligible vector in it is compared. A larger scope
#: walks the set's HNSW index. The bound is on text nodes, not on vectors,
#: because exact ranking reads and hashes every row of the scope, with or
#: without a vector (about 33 microseconds a row on the C4 copy). Read at call
#: time, so a test can lower it.
EXACT_SCOPE_MAX = 5_000
#: A set with ``index.kind: none`` ranks exactly, and refuses a scope above
#: this many visible text nodes rather than comparing without bound.
UNINDEXED_SCOPE_MAX = 20_000
#: Candidates the index walk hands to the scope check per round trip: the
#: first batch is ``max(2 * limit, WALK_BATCH_MIN)``, each later one twice
#: the last, up to ``WALK_BATCH_MAX``.
WALK_BATCH_MIN = 100
WALK_BATCH_MAX = 4_000


class SemanticScopeTooLarge(SubstrateError):
    """An unindexed set's scope is above the exact-ranking bound."""

    code = "semantic_scope_too_large"


class SemanticQueryEmbeddingFailed(SubstrateError):
    """The query's own embedding failed (timeout or error after the query
    client's attempts), so there is nothing to rank by."""

    code = "semantic_query_embedding_failed"


@dataclass(frozen=True)
class VectorSetRead:
    """What one semantic read over a vector set is pinned to."""

    vector_set: Any
    generation_id: str
    #: The pin's watermark: the highest link ``seq`` (``max_seq``) and the
    #: highest vector ``seq`` (``max_vector_seq``; vectors have their own
    #: counter) the read uses.
    max_seq: int
    max_vector_seq: int
    branch_point: int
    physical_table: str
    #: ``(subtype, template_hash)`` for every subtype the read ranks.
    templates: tuple[tuple[str, str], ...]
    scoped_source_ids: Optional[tuple[str, ...]]
    ranking: str  # "exact" | "approximate"
    #: Visible text nodes in scope, counted up to ``count_bound``; None above.
    text_nodes: Optional[int]
    #: Eligible nodes (in scope, with a vector at the pin), counted up to
    #: ``count_bound`` for an exact scope; None above it, and None (not
    #: counted) for an approximate scope that has any vector at the pin.
    embedded_nodes: Optional[int]
    index: dict[str, Any]
    #: The reader's access-rule predicate over the node row aliased ``n``, as
    #: SQL text for a ``%(name)s`` statement (literal ``%`` doubled); None
    #: when no access rules run. A superset of what the per-row read filter
    #: serves; that filter still runs on every ranked row.
    reader_predicate: Optional[str] = None
    count_bound: int = 0
    #: Every subtype the set covers in the scope, including one with no
    #: servable template (counted in ``text_nodes``, never ranked); empty
    #: means the subtypes of ``templates``.
    subtypes: tuple[str, ...] = ()


def _set_literal(vector_set: Any) -> str:
    set_id = str(vector_set.set_id)
    if not re.fullmatch(r"es_[0-9a-f]{24}", set_id):
        raise ValueError(f"not a vector set id: {set_id!r}")
    return f"'{set_id}'"


def inspect_set_index(conn: Any, vector_set: Any) -> dict[str, Any]:
    """State of a set's index: ``not_indexed`` (kind none), ``unsupported``,
    ``missing``, ``building``, ``invalid`` or ``present``.

    ``present`` with a ``rebuild`` entry means the index of the registered
    build settings is not valid yet (``rebuild.status``) and a valid index
    of the set under older settings (``index_name``) serves reads."""
    from okg.substrate.embeddings.store import set_index_name

    report: dict[str, Any] = {
        "set": vector_set.name,
        "set_id": vector_set.set_id,
        "kind": vector_set.index.kind,
    }
    if vector_set.index.kind == "none":
        return {**report, "status": "not_indexed"}
    name = set_index_name(vector_set)
    report["index_name"] = name
    version = _pgvector_version(conn)
    report["pgvector_version"] = version
    if not _version_supported(version):
        return {**report, "status": "unsupported"}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT i.indisvalid AS valid, "
            "       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p "
            "                WHERE p.index_relid = i.indexrelid "
            "                   OR (p.index_relid IS NULL "
            "                       AND p.datname = current_database())) "
            "         AS building "
            "  FROM pg_index AS i WHERE i.indexrelid = to_regclass(%s)",
            (f"okg.{name}",),
        )
        row = cur.fetchone()
    if row is not None and row["valid"]:
        return {**report, "status": "present"}
    if row is None:
        status = "missing"
    else:
        status = "building" if row["building"] else "invalid"
    # New build settings name a new index, built beside the old one (`okg
    # migrate`, then `okg db embed-index`, which drops the old one only after
    # the new one is valid). Until then a valid index of the set under older
    # settings serves the read: it indexes the same expression with the same
    # predicate, so the planner walks it, and only the graph's recall
    # settings differ. The page names the index it read and the pending one.
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT c.relname FROM pg_index AS i"
            "  JOIN pg_class AS c ON c.oid = i.indexrelid"
            "  JOIN pg_namespace AS ns ON ns.oid = c.relnamespace"
            " WHERE ns.nspname = 'okg'"
            "   AND i.indrelid = to_regclass('okg.text_embeddings')"
            "   AND i.indisvalid AND starts_with(c.relname, %s)"
            "   AND c.relname <> %s"
            # The highest oid is normally the newest build.
            " ORDER BY i.indexrelid DESC LIMIT 1",
            (f"text_embeddings_{str(vector_set.set_id)[3:]}_", name),
        )
        older = cur.fetchone()
    if older is None:
        return {**report, "status": status}
    return {
        **report,
        "status": "present",
        "index_name": str(older[0]),
        "rebuild": {"index_name": name, "status": status},
    }


def prepare_set_read(
    conn: Any,
    session: Any,
    vector_set: Any,
    *,
    scoped_subtypes: Optional[Sequence[str]] = None,
    scoped_source_ids: Optional[Sequence[str]] = None,
    reader_predicate: Optional[str] = None,
) -> VectorSetRead:
    """Everything a read needs before the query is embedded, or a refusal.

    Refuses, before any paid call: a pin with no watermark for the set
    (``embedding_watermark_missing``); for an ``hnsw`` set, pgvector older
    than 0.8 (no iterative index scan, so a filtered walk could not fill a
    page) or a missing / building / invalid index
    (``semantic_index_unavailable``); and an unindexed set whose scope holds
    more than :data:`UNINDEXED_SCOPE_MAX` visible text nodes
    (``semantic_scope_too_large``).

    The scope is decided here, before any ranking (prefilter): visible rows
    at the pin, of the ranked subtypes and sources, kept by
    ``reader_predicate``, whose current content links to a document of a
    servable template (:func:`_scope_templates`) with a vector, both at or
    below the pin's watermark. A scope of at most :data:`EXACT_SCOPE_MAX`
    visible text nodes ranks exactly.

    Cost: every count stops one past its bound, and the scope rows are read
    through the node segment's visible-subtype index. The eligible count
    (the join to links and vectors) runs only for an exact scope, so at
    most ``bound`` rows. An approximate scope only asks whether any link of
    its templates has a vector at the pin (:func:`_scope_has_vector`), and
    reports ``embedded_nodes`` as None (not counted) or 0.
    """
    from okg.substrate.bm25_node_search import SearchIndexUnavailable, branch_point
    from okg.substrate.embeddings.store import watermark_for

    mark = watermark_for(conn, str(session.generation_id), vector_set.set_id)
    index = inspect_set_index(conn, vector_set)
    if index["status"] not in ("present", "not_indexed"):
        if index["status"] == "unsupported":
            message = (
                f"semantic search over vector set {vector_set.name!r} walks its "
                "HNSW index with pgvector's iterative scan, which needs pgvector "
                f"0.8 or newer; this database has {index.get('pgvector_version')!r}"
            )
            hint = (
                "install pgvector 0.8 or newer and run ALTER EXTENSION vector "
                "UPDATE, or give the set `index: {kind: none}` (exact ranking "
                "of small scopes only)"
            )
        else:
            message = (
                f"semantic search over vector set {vector_set.name!r} needs its "
                f"HNSW index, which is {index['status']}"
            )
            hint = (
                "wait for the build to finish"
                if index["status"] == "building"
                else "run `okg migrate --deployment <name> --apply`, which "
                "builds each set's index"
            )
        raise SemanticIndexUnavailable(message, data=index, fix_hint=hint)
    try:
        bp = branch_point(conn, session)
    except SearchIndexUnavailable as exc:
        raise SemanticIndexUnavailable(
            str(exc), data=dict(getattr(exc, "data", None) or {}),
        ) from exc
    physical = _node_segment(conn)

    catalog_version_id = getattr(session, "catalog_version_id", None)
    templates = _scope_templates(
        conn, vector_set,
        catalog_version_id=catalog_version_id,
        scoped_subtypes=scoped_subtypes,
    )
    covered = tuple(_covered_subtypes(
        conn, vector_set, catalog_version_id=catalog_version_id,
        scoped_subtypes=scoped_subtypes,
    ))
    sources = None if scoped_source_ids is None else tuple(scoped_source_ids)
    # Module globals read at call time, so a test can lower them.
    exact_max = int(EXACT_SCOPE_MAX)
    unindexed_max = int(UNINDEXED_SCOPE_MAX)
    unindexed = vector_set.index.kind == "none"
    read = VectorSetRead(
        vector_set=vector_set,
        generation_id=str(session.generation_id),
        max_seq=mark.links,
        max_vector_seq=mark.vectors,
        branch_point=bp,
        physical_table=physical,
        templates=templates,
        scoped_source_ids=sources,
        ranking="exact",
        text_nodes=None,
        embedded_nodes=None,
        index=index,
        reader_predicate=reader_predicate,
        count_bound=exact_max,
        subtypes=covered,
    )
    # How many rows an exact ranking would read, counted only as far as the
    # bound that decides between exact and approximate (or refusal).
    bound = unindexed_max if unindexed else exact_max
    text_nodes = _bounded_count(conn, read, bound=bound, with_vectors=False)
    if text_nodes > bound and unindexed:
        raise SemanticScopeTooLarge(
            f"vector set {vector_set.name!r} has no index, and this scope holds "
            f"more than {bound} visible text nodes, the most it ranks exactly",
            data={"set": vector_set.name, "bound": bound},
            fix_hint="narrow the scope, or give the set `index: {kind: hnsw}`",
        )
    exact = unindexed or text_nodes <= exact_max
    embedded: Optional[int]
    if exact:
        eligible = _bounded_count(conn, read, bound=exact_max, with_vectors=True)
        embedded = eligible if eligible <= exact_max else None
    else:
        # Not counted: counting eligible nodes in a large scope with few
        # vectors reads the whole scope. Zero is still known cheaply.
        embedded = None if _scope_has_vector(conn, read) else 0
    return dataclass_replace(
        read,
        ranking="exact" if exact else "approximate",
        text_nodes=text_nodes if text_nodes <= exact_max else None,
        embedded_nodes=embedded,
    )


def _covered_subtypes(
    conn: Any,
    vector_set: Any,
    *,
    catalog_version_id: Any,
    scoped_subtypes: Optional[Sequence[str]] = None,
) -> list[str]:
    """The catalog subtypes the set covers (and the scope keeps), sorted."""
    with conn.cursor(row_factory=tuple_row) as cur:
        if catalog_version_id is not None:
            cur.execute(
                "SELECT DISTINCT name FROM okg.subtypes WHERE catalog_version_id = %s",
                (catalog_version_id,),
            )
        else:
            cur.execute("SELECT DISTINCT name FROM okg.subtypes")
        catalog_subtypes = {str(row[0]) for row in cur.fetchall()}
    return sorted(
        name for name in catalog_subtypes
        if vector_set.covers(name)
        and (scoped_subtypes is None or name in set(scoped_subtypes))
    )


def _scope_templates(
    conn: Any,
    vector_set: Any,
    *,
    catalog_version_id: Any,
    scoped_subtypes: Optional[Sequence[str]] = None,
) -> tuple[tuple[str, str], ...]:
    """The ``(subtype, template_hash)`` pairs a read may serve links of, for
    every catalog subtype the set covers (and the scope keeps).

    A set keeps its id when its templates change, so its older links stay
    servable: the current template (under the pinned catalog's
    classification) and every template stored for the set
    (``okg.embedding_templates``), each kept only while every header field
    its documents hold is still shown to every reader under both the pinned
    catalog's and the latest catalog's classification. A field that became
    maskable fails closed. Among a node's links to these templates the read
    serves the newest with a vector (:func:`_no_newer_link`).
    """
    from okg.substrate.embeddings.documents import (
        record_hash,
        record_still_safe,
        template_record,
    )
    from okg.substrate.embeddings.store import (
        load_classifications,
        load_template_records,
    )

    subtypes = _covered_subtypes(
        conn, vector_set, catalog_version_id=catalog_version_id,
        scoped_subtypes=scoped_subtypes,
    )
    pinned = load_classifications(
        conn, subtypes, catalog_version_id=catalog_version_id,
    )
    latest = (
        pinned if catalog_version_id is None
        else load_classifications(conn, subtypes)
    )
    out: list[tuple[str, str]] = []
    for name in subtypes:
        current = template_record(vector_set, name, pinned.get(name))
        if record_still_safe(current, (pinned.get(name), latest.get(name))):
            out.append((name, record_hash(current)))
    seen = set(out)
    for subtype, stored_hash, record in load_template_records(
        conn, vector_set.set_id, subtypes,
    ):
        if (subtype, stored_hash) in seen:
            continue
        # A row that does not hash to its key, or names another set or
        # subtype, is not trusted.
        if (
            record_hash(record) != stored_hash
            or record.get("set_id") != vector_set.set_id
            or record.get("subtype") != subtype
        ):
            continue
        if record_still_safe(record, (pinned.get(subtype), latest.get(subtype))):
            out.append((subtype, stored_hash))
            seen.add((subtype, stored_hash))
    return tuple(out)


def _no_newer_link(*, node: str, row_hash: str, subtype: str, link: str) -> str:
    """SQL: no servable link of the same node content newer than ``link``
    (a node_documents alias) has a vector at the pin. With more than one
    servable template a node can have several links; the read ranks only the
    newest that has a vector, so each node appears once and a template
    change takes effect node by node as its vectors arrive. A template that
    becomes current again gets newer copies of its links from the writers
    (``embeddings.store.missing_links``), so it wins again."""
    return (
        "NOT EXISTS ("
        " SELECT 1 FROM okg.node_documents AS d2"
        " JOIN unnest(CAST(%(t_subtypes)s AS text[]), CAST(%(t_hashes)s AS text[]))"
        "      AS t2(subtype, template_hash)"
        f"   ON t2.template_hash = d2.template_hash AND t2.subtype = {subtype}"
        " JOIN okg.text_embeddings AS e2"
        "   ON e2.set_id = d2.set_id AND e2.doc_hash = d2.doc_hash"
        "  AND e2.seq <= %(max_vector_seq)s"
        " WHERE d2.set_id = %(set_id)s"
        f"  AND d2.node_id = {node} AND d2.row_hash = {row_hash}"
        f"  AND d2.seq <= %(max_seq)s AND d2.seq > {link}.seq)"
    )


def set_coverage(
    conn: Any,
    vector_set: Any,
    *,
    generation_id: str,
    catalog_version_id: Any,
    max_seq: int,
    max_vector_seq: int,
    exact: bool = True,
) -> dict[str, Any]:
    """How much of a published generation one set covers, per subtype.

    For ``okg doctor``. Counts every visible text node of the set's subtypes
    at the generation whose text the fill would embed (a non-blank string;
    no bound, no access rules): ``text_nodes``; those whose
    current content links to a document of a servable template at or below
    ``max_seq``: ``linked_nodes``; and those with such a document that has a
    vector at or below ``max_vector_seq``: ``embedded_nodes`` -- the nodes a
    semantic read of that generation can rank. ``method`` is ``exact``.

    Matching a node's current content needs its row hash, a sha256 over
    every visible text node: on a graph of 600,000 text nodes that alone
    takes longer than plain doctor's 15 s statement timeout. So with
    ``exact=False`` and more than :data:`COVERAGE_EXACT_MAX` text nodes
    (counted exactly, without the hash), linked and embedded nodes are
    estimated from a row sample of about :data:`COVERAGE_SAMPLE_ROWS`
    (``TABLESAMPLE BERNOULLI``: rows, not pages, since rows written
    together share a page and a gap): ``method: sample``, ``linked_nodes``
    and ``embedded_nodes`` None, the ``sampled_*`` counts, the scaled
    ``estimated_*`` counts and ``missing_upper_95``, a 95% upper bound on
    nodes without a vector (Wilson score). ``okg doctor --check
    vector-sets`` counts exactly.
    """
    from types import SimpleNamespace

    from okg.substrate.bm25_node_search import branch_point

    covered = _covered_subtypes(conn, vector_set, catalog_version_id=catalog_version_id)
    if not covered:
        return {"method": "exact", "text_nodes": 0, "linked_nodes": 0,
                "embedded_nodes": 0, "subtypes": {}}
    templates = _scope_templates(
        conn, vector_set, catalog_version_id=catalog_version_id,
    )
    params = {
        "bp": branch_point(conn, SimpleNamespace(generation_id=str(generation_id))),
        "subtypes": list(covered),
        # A template hash covers its subtype, and so does the row hash, so
        # a link never matches a node of another subtype.
        "t_hashes": sorted({t[1] for t in templates}),
        "set_id": vector_set.set_id,
        "max_seq": int(max_seq),
        "max_vector_seq": int(max_vector_seq),
        "nonblank_body": NONBLANK_BODY_REGEX,
    }
    physical = quote_physical_table(_node_segment(conn))
    if exact:
        return _coverage_exact(conn, physical, params)
    text = _coverage_text_nodes(conn, physical, params)
    total = sum(text.values())
    if total <= COVERAGE_EXACT_MAX:
        return _coverage_exact(conn, physical, params)
    return _coverage_sample(conn, physical, params, text)


#: Visible text nodes up to which plain doctor counts a set's coverage
#: exactly (about 2.5 s at ~50 us a node); above it, it samples.
COVERAGE_EXACT_MAX = 50_000
#: Visible text nodes a sampled coverage aims to read.
COVERAGE_SAMPLE_ROWS = 10_000

#: A visible node the set's fill would embed: its subtype is covered and its
#: ``text`` is a non-blank string, the renderer's rule
#: (:func:`okg.substrate.embeddings.documents.render_document`). A node
#: with empty or blank text gets no document, so it is not counted missing.
#: The scan (``n``) keeps visible rows of a covered subtype with a ``text``
#: and outputs that value once (:func:`text_value_sql`); the level above
#: (``v``, past ``OFFSET 0``) applies :data:`_COVERAGE_NONBLANK`.
_COVERAGE_VISIBLE = (
    "n.deleted = FALSE AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi"
    " AND n.attrs ? 'text' AND n.subtype = ANY(%(subtypes)s)"
)
_COVERAGE_NONBLANK = nonblank_body_sql("v")


def _coverage_text_nodes(conn: Any, physical: str, params: dict) -> dict[str, int]:
    """Visible text nodes per covered subtype, exactly, with no row hash."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"SELECT v.subtype, count(*)"
            f"  FROM (SELECT n.subtype, {text_value_sql('n')}"
            f"          FROM {physical} AS n"
            f"         WHERE {_COVERAGE_VISIBLE} OFFSET 0) AS v"
            f" WHERE {_COVERAGE_NONBLANK} GROUP BY v.subtype",
            params,
            prepare=False,
        )
        return {str(subtype): int(count) for subtype, count in cur.fetchall()}


def _coverage_exact(conn: Any, physical: str, params: dict) -> dict[str, Any]:
    """Exact coverage as one join: the set's servable links at the
    watermark, grouped by node and content with whether any has a vector,
    joined to the visible text nodes on node id and row hash."""
    from okg.substrate.embeddings.store import row_hash_expression

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"""
            WITH l AS (
              SELECT d.node_id, d.row_hash,
                     bool_or(e.doc_hash IS NOT NULL) AS embedded
                FROM okg.node_documents AS d
                LEFT JOIN okg.text_embeddings AS e
                  ON e.set_id = d.set_id AND e.doc_hash = d.doc_hash
                 AND e.seq <= %(max_vector_seq)s
               WHERE d.set_id = %(set_id)s AND d.seq <= %(max_seq)s
                 AND d.template_hash = ANY(%(t_hashes)s)
               GROUP BY d.node_id, d.row_hash
            )
            SELECT v.subtype, count(*), count(l.node_id),
                   count(*) FILTER (WHERE l.embedded)
              FROM (SELECT n.node_id, n.subtype,
                           {row_hash_expression('n')} AS row_hash,
                           {text_value_sql('n')}
                      FROM {physical} AS n
                     WHERE {_COVERAGE_VISIBLE}
                    OFFSET 0) AS v
              LEFT JOIN l
                ON l.node_id = v.node_id AND l.row_hash = v.row_hash
             WHERE {_COVERAGE_NONBLANK}
             GROUP BY v.subtype
            """,
            params,
            prepare=False,
        )
        rows = cur.fetchall()
    per_subtype = {
        str(subtype): {
            "text_nodes": int(text), "linked_nodes": int(linked),
            "embedded_nodes": int(embedded),
        }
        for subtype, text, linked, embedded in sorted(rows)
    }
    return {
        "method": "exact",
        "text_nodes": sum(v["text_nodes"] for v in per_subtype.values()),
        "linked_nodes": sum(v["linked_nodes"] for v in per_subtype.values()),
        "embedded_nodes": sum(v["embedded_nodes"] for v in per_subtype.values()),
        "subtypes": per_subtype,
    }


def _wilson_upper_95(failures: int, trials: int) -> float:
    """The 95% Wilson score upper bound of a proportion."""
    z = 1.959964
    p = failures / trials
    denominator = 1 + z * z / trials
    centre = p + z * z / (2 * trials)
    spread = z * ((p * (1 - p) / trials + z * z / (4 * trials * trials)) ** 0.5)
    return min(1.0, (centre + spread) / denominator)


def _coverage_sample(
    conn: Any, physical: str, params: dict, text: dict[str, int],
) -> dict[str, Any]:
    """Coverage estimated from a BERNOULLI row sample; see set_coverage."""
    import math

    from okg.substrate.embeddings.store import row_hash_expression

    total = sum(text.values())
    percent = min(100.0, 100.0 * COVERAGE_SAMPLE_ROWS / total)
    link = (
        "SELECT 1 FROM okg.node_documents AS d{vector}"
        " WHERE d.set_id = %(set_id)s AND d.node_id = v.node_id"
        "   AND d.row_hash = v.row_hash AND d.seq <= %(max_seq)s"
        "   AND d.template_hash = ANY(%(t_hashes)s)"
    )
    vector = (
        " JOIN okg.text_embeddings AS e"
        "   ON e.set_id = d.set_id AND e.doc_hash = d.doc_hash"
        "  AND e.seq <= %(max_vector_seq)s"
    )
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"""
            SELECT v.subtype, count(*),
                   count(*) FILTER (WHERE EXISTS ({link.format(vector='')})),
                   count(*) FILTER (WHERE EXISTS ({link.format(vector=vector)}))
              FROM (SELECT n.node_id, n.subtype,
                           {row_hash_expression('n')} AS row_hash,
                           {text_value_sql('n')}
                      FROM {physical} AS n
                           TABLESAMPLE BERNOULLI (CAST(%(percent)s AS real))
                     WHERE {_COVERAGE_VISIBLE}
                    -- OFFSET 0: each sampled row is hashed once.
                    OFFSET 0) AS v
             WHERE {_COVERAGE_NONBLANK}
             GROUP BY v.subtype
            """,
            {**params, "percent": percent},
            prepare=False,
        )
        rows = {str(r[0]): (int(r[1]), int(r[2]), int(r[3])) for r in cur.fetchall()}

    def scaled(count: int, sampled: int, of: int) -> Optional[int]:
        return None if sampled == 0 else round(of * count / sampled)

    per_subtype = {}
    for subtype, nodes in sorted(text.items()):
        sampled, linked, embedded = rows.get(subtype, (0, 0, 0))
        per_subtype[subtype] = {
            "text_nodes": nodes, "linked_nodes": None, "embedded_nodes": None,
            "sampled_nodes": sampled, "sampled_linked": linked,
            "sampled_embedded": embedded,
            "estimated_linked_nodes": scaled(linked, sampled, nodes),
            "estimated_embedded_nodes": scaled(embedded, sampled, nodes),
        }
    sampled = sum(r[0] for r in rows.values())
    linked = sum(r[1] for r in rows.values())
    embedded = sum(r[2] for r in rows.values())
    return {
        "method": "sample",
        "text_nodes": total,
        "linked_nodes": None,
        "embedded_nodes": None,
        "sample_percent": round(percent, 4),
        "sampled_nodes": sampled,
        "sampled_linked": linked,
        "sampled_embedded": embedded,
        "estimated_linked_nodes": scaled(linked, sampled, total),
        "estimated_embedded_nodes": scaled(embedded, sampled, total),
        "missing_upper_95": (
            None if sampled == 0
            else math.ceil(_wilson_upper_95(sampled - embedded, sampled) * total)
        ),
        "subtypes": per_subtype,
    }


def _bounded_count(
    conn: Any, read: VectorSetRead, *, bound: int, with_vectors: bool,
) -> int:
    """Visible text nodes in the read's scope (``with_vectors=False``) or the
    eligible ones (with a vector for their current content at the pin),
    counted up to ``bound + 1``."""
    if with_vectors:
        body, params = _exact_join(read)
    else:
        where, params = _visible_text_where(read)
        body = (
            f" FROM (SELECT {text_value_sql('n')}"
            f"         FROM {quote_physical_table(read.physical_table)} AS n"
            f"        WHERE {where} OFFSET 0) AS v"
            f" WHERE {nonblank_body_sql('v')}"
        )
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"SELECT count(*) FROM (SELECT 1{body} LIMIT %(count_limit)s) AS s",
            {**params, "count_limit": int(bound) + 1},
            prepare=False,
        )
        return int(cur.fetchone()[0])


def _scope_has_vector(conn: Any, read: VectorSetRead) -> bool:
    """Whether any link of the read's servable templates, at or below the
    pin's watermark, names a document with a vector at the pin.

    A necessary condition for any eligible node (template hashes are per
    subtype), so False means the scope ranks nothing. Driven by the links'
    key ``(set_id, template_hash, ...)`` and the vectors' key; it stops at
    the first hit, so it reads few rows unless the scope's templates have
    many links and almost no vectors."""
    if not read.templates:
        return False
    statement, params = _scope_has_vector_statement(read)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(statement, params, prepare=False)
        return bool(cur.fetchone()[0])


def _scope_has_vector_statement(read: VectorSetRead) -> tuple[str, dict[str, Any]]:
    """The statement :func:`_scope_has_vector` runs (one boolean row).

    LATERAL per template: each probe is a range scan of the links' primary
    key ``(set_id, template_hash, node_id, ...)``; ``ORDER BY d.node_id``
    is that key's order, so the index is the cheap plan. Written as one
    join, or without the order, the planner scanned every link of the set
    (98 ms to 1.3 s on the C4 copy at full coverage)."""
    statement = (
        "SELECT EXISTS ("
        " SELECT 1 FROM unnest(CAST(%(t_hashes)s AS text[])) AS t(template_hash)"
        " CROSS JOIN LATERAL ("
        "   SELECT 1 FROM okg.node_documents AS d"
        "    WHERE d.set_id = %(set_id)s AND d.template_hash = t.template_hash"
        "      AND d.seq <= %(max_seq)s"
        "      AND EXISTS (SELECT 1 FROM okg.text_embeddings AS e"
        "                   WHERE e.set_id = d.set_id AND e.doc_hash = d.doc_hash"
        "                     AND e.seq <= %(max_vector_seq)s)"
        "    ORDER BY d.node_id LIMIT 1) AS hit)"
    )
    return statement, {
        "t_hashes": sorted({t[1] for t in read.templates}),
        "set_id": read.vector_set.set_id,
        "max_seq": read.max_seq,
        "max_vector_seq": read.max_vector_seq,
    }


def _visible_text_where(read: VectorSetRead) -> tuple[str, dict[str, Any]]:
    """The scope filter over the node row ``n``: visible at the pin, with a
    ``text``, of a ranked subtype and source, kept by the reader's
    access-rule predicate. Callers select :func:`text_value_sql` beside it
    in an ``OFFSET 0`` subquery and apply :func:`nonblank_body_sql` one
    level up (the fill's rule: a blank text gets no document, so it is
    never counted as a node without a vector); the params bind its
    pattern."""
    filters = [
        "n.deleted = FALSE",
        "n.live_lo <= %(bp)s",
        "%(bp)s < n.live_hi",
        "n.attrs ? 'text'",
        "n.subtype = ANY(%(subtypes)s)",
    ]
    params: dict[str, Any] = {
        "nonblank_body": NONBLANK_BODY_REGEX,
        "bp": read.branch_point,
        "subtypes": list(read.subtypes) or sorted({t[0] for t in read.templates}),
    }
    if read.scoped_source_ids is not None:
        filters.append("n.source = ANY(%(sources)s)")
        params["sources"] = list(read.scoped_source_ids)
    if read.reader_predicate is not None:
        filters.append(f"({read.reader_predicate})")
    return " AND ".join(filters), params


_NODE_COLUMNS = (
    "n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
    "n.source_record_id, n.source_revision"
)


def _exact_join(read: VectorSetRead) -> tuple[str, dict[str, Any]]:
    """Scope rows (``v``) joined to the document of their current content
    (``d``: a servable template, ``row_hash`` equal to the visible row's, the
    newest such link with a vector) and its vector (``e``), both at or below
    the pin's watermark. One row per node."""
    from okg.substrate.embeddings.store import row_hash_expression

    where, params = _visible_text_where(read)
    params.update(
        set_id=read.vector_set.set_id,
        max_seq=read.max_seq,
        max_vector_seq=read.max_vector_seq,
        t_subtypes=[t[0] for t in read.templates],
        t_hashes=[t[1] for t in read.templates],
    )
    body = (
        f" FROM (SELECT {_NODE_COLUMNS}, {row_hash_expression('n')} AS row_hash,"
        f"              {text_value_sql('n')}"
        f"         FROM {quote_physical_table(read.physical_table)} AS n"
        # OFFSET 0 keeps `v` a subquery, so row_hash and the text value are
        # computed once in the node scan's output. Flattened, the planner
        # repeats the sha256 of `attrs::text` in the link join and again in
        # the newer-link check, and reads the text once per test.
        f"        WHERE {where} OFFSET 0) AS v"
        " JOIN unnest(CAST(%(t_subtypes)s AS text[]), CAST(%(t_hashes)s AS text[]))"
        "      AS t(subtype, template_hash) ON t.subtype = v.subtype"
        " JOIN okg.node_documents AS d"
        "   ON d.set_id = %(set_id)s AND d.template_hash = t.template_hash"
        "  AND d.node_id = v.node_id AND d.row_hash = v.row_hash"
        "  AND d.seq <= %(max_seq)s"
        " JOIN okg.text_embeddings AS e"
        "   ON e.set_id = d.set_id AND e.doc_hash = d.doc_hash"
        "  AND e.seq <= %(max_vector_seq)s"
        f" WHERE {nonblank_body_sql('v')}"
        "   AND " + _no_newer_link(
            node="v.node_id", row_hash="v.row_hash", subtype="v.subtype", link="d",
        )
    )
    return body, params


def _width_literal(read: VectorSetRead) -> int:
    return int(read.vector_set.width)


def query_vector_for_set(vector_set: Any, vector: Sequence[float]) -> list[float]:
    """The query vector cut to the set's width.

    A set's own backend returns vectors at the set's width (PACT
    vector-set-pgvector-widths). A node-column plan's OpenRouter backend
    returns them zero-padded to the column width; the set stores only the
    first ``width`` values. Anything after them must be zeros, or the
    vector is not of this set's shape and is refused.
    """
    width = int(vector_set.width)
    values = [float(v) for v in vector]
    if len(values) < width or any(v != 0.0 for v in values[width:]):
        err = SubstrateError(
            f"the query vector does not have the {width}-value zero-padded "
            f"shape of vector set {vector_set.name!r}",
            data={"set": vector_set.name, "width": width, "length": len(values)},
            fix_hint="search with the set's own backend",
        )
        err.code = "embedder_dim_mismatch"
        raise err
    return values[:width]


def rank_vector_set(
    conn: Any, read: VectorSetRead, query_vector: Sequence[float], *, limit: int,
) -> AnnRanking:
    """The ``limit`` nearest eligible nodes at the pin.

    The scope filter runs before ranking, never on a fixed number of
    nearest candidates, so a narrow scope inside a large set still fills its
    page (prefilter). Eligible means: visible at the pin, of a ranked subtype
    and source, kept by the reader's access-rule predicate, and linked to a
    document of a servable template whose ``row_hash`` is the visible
    row's, with a vector; link and vector both at or below the pin's
    watermark. A node with several such links ranks by the newest.

    Exact mode compares every eligible vector. Approximate mode walks the
    set's HNSW index nearest first with pgvector's iterative scan
    (``strict_order``, the set's ``ef_search`` and ``max_scan_tuples``)
    through a server-side cursor, and checks each batch of candidates
    against the scope until ``limit`` nodes pass. A walk that ends at
    ``max_scan_tuples`` before the page is full returns ``capped=True``.

    Why a cursor rather than one statement with the scope join inside: the
    walk then stays the single-table ``ORDER BY <indexed expression> <=>
    <query>`` that HNSW serves, with no join for the planner to reorder,
    hash or sort. The scope check per batch is an indexed lookup
    (``node_documents (set_id, doc_hash)``, the node segment's id index).
    """
    width = _width_literal(read)
    if len(query_vector) != width:
        raise SubstrateError(
            f"the query vector has {len(query_vector)} values; vector set "
            f"{read.vector_set.name!r} is {width} wide",
            data={"set": read.vector_set.name, "width": width},
        )
    q = _vector_literal(query_vector)
    # The set's own column and type (PACT vector-set-pgvector-widths): the
    # expression the set's HNSW index is built on.
    vector_set = read.vector_set
    stored_vector = f"CAST(e.{vector_set.vector_column} AS {vector_set.vector_type})"
    query_cast = f"CAST(%(q)s AS {vector_set.vector_type})"
    distance = f"({stored_vector} <=> {query_cast})"
    if read.ranking == "exact":
        body, params = _exact_join(read)
        params.update(q=q, limit=int(limit))
        with conn.cursor(row_factory=dict_row) as cur:
            # MATERIALIZED: the eligible rows are collected first and only
            # then ordered. Without the fence the planner may order by
            # walking the set's HNSW index (the set id reaches `e` through
            # the join) and filter the walk's first ef_search candidates
            # afterwards: a narrow scope then comes back short or empty
            # (seen on pgvector 0.8.6 with a note-only scope).
            cur.execute(
                "WITH eligible AS MATERIALIZED ("
                " SELECT v.node_id, v.subtype, v.attrs, v.text_hash, v.source,"
                "        v.source_record_id, v.source_revision,"
                f"        {distance} AS distance{body})"
                " SELECT * FROM eligible"
                " ORDER BY distance, node_id LIMIT %(limit)s",
                params,
                prepare=False,
            )
            rows = [dict(row) for row in cur.fetchall()]
        return AnnRanking(rows=rows, exhausted=len(rows) < int(limit), capped=False)

    index = read.vector_set.index
    settings = (
        # The walk's only ordered path is the HNSW index; with sorting off
        # the planner cannot prefer reading the set's rows and sorting them.
        ("enable_sort", "off"),
        # strict_order: the walk returns candidates in distance order, so the
        # first `limit` that pass the scope check are the page.
        ("hnsw.iterative_scan", "strict_order"),
        ("hnsw.ef_search", str(int(index.ef_search))),
        ("hnsw.max_scan_tuples", str(int(index.max_scan_tuples))),
    )
    walk_sql = (
        f"SELECT e.doc_hash, {distance} AS distance"
        "  FROM okg.text_embeddings AS e"
        # The literal set id is what lets the planner use the set's partial
        # index (its predicate is `set_id = '<id>'`).
        f" WHERE e.set_id = {_set_literal(read.vector_set)}"
        "   AND e.seq <= %(max_vector_seq)s"
        f" ORDER BY {stored_vector} <=> {query_cast}"
    )
    check_sql, check_params = _scope_check_statement(read)
    rows: list[dict[str, Any]] = []
    walked = 0
    walk_ended = False
    batch = max(2 * int(limit), WALK_BATCH_MIN)
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            previous: list[tuple[str, Optional[str]]] = []
            for name, value in settings:
                cur.execute("SELECT current_setting(%s, true) AS v", (name,))
                previous.append((name, cur.fetchone()["v"]))
                cur.execute("SELECT set_config(%s, %s, true)", (name, value))
            walk = conn.cursor(name="okg_semantic_set_walk", row_factory=dict_row)
            try:
                walk.execute(walk_sql, {"q": q, "max_vector_seq": read.max_vector_seq})
                while len(rows) < int(limit):
                    candidates = walk.fetchmany(batch)
                    walked += len(candidates)
                    if candidates:
                        cur.execute(
                            check_sql,
                            {
                                **check_params,
                                "docs": [c["doc_hash"] for c in candidates],
                                "dists": [float(c["distance"]) for c in candidates],
                            },
                            prepare=False,
                        )
                        rows.extend(dict(row) for row in cur.fetchall())
                    if len(candidates) < batch:
                        walk_ended = True
                        break
                    batch = min(batch * 2, WALK_BATCH_MAX)
            finally:
                walk.close()
            for name, value in previous:
                if value is None:
                    cur.execute(sql.SQL("RESET {}").format(sql.Identifier(*name.split("."))))
                else:
                    cur.execute("SELECT set_config(%s, %s, true)", (name, value))
    rows.sort(key=lambda row: (float(row["distance"]), str(row["node_id"])))
    rows = rows[: int(limit)]
    exhausted = False
    capped = False
    if walk_ended and len(rows) < int(limit):
        # The walk ended with the page short: either it visited every vector
        # of the set at the pin (the ranking ended), or it stopped at
        # max_scan_tuples with vectors left unvisited (the page may miss
        # nearer eligible nodes). The stored count is a property of the set,
        # not of this reader's scope.
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT count(*) FROM okg.text_embeddings "
                "WHERE set_id = %s AND seq <= %s",
                (read.vector_set.set_id, read.max_vector_seq),
            )
            stored = int(cur.fetchone()[0])
        if walked >= stored:
            exhausted = True
        else:
            capped = True
            log.info(
                "semantic set read: scan bound reached for %s: %d of %d vectors "
                "walked, %d of %d rows found",
                read.vector_set.set_id, walked, stored, len(rows), limit,
            )
    return AnnRanking(rows=rows, exhausted=exhausted, capped=capped)


def _scope_check_statement(read: VectorSetRead) -> tuple[str, dict[str, Any]]:
    """The statement that keeps, of a batch of walked ``(doc_hash,
    distance)`` candidates (parameters ``docs`` and ``dists``), the eligible
    nodes, with the same row keys as the exact ranking."""
    from okg.substrate.embeddings.store import row_hash_expression

    where, params = _visible_text_where(read)
    params.update(
        set_id=read.vector_set.set_id,
        max_seq=read.max_seq,
        max_vector_seq=read.max_vector_seq,
        t_subtypes=[t[0] for t in read.templates],
        t_hashes=[t[1] for t in read.templates],
    )
    statement = (
        f"SELECT {_NODE_COLUMNS}, l.distance"
        "  FROM (SELECT d.node_id, d.row_hash, d.template_hash,"
        "               max(d.seq) AS seq, min(c.distance) AS distance"
        "          FROM unnest(CAST(%(docs)s AS text[]),"
        "                      CAST(%(dists)s AS float8[])) AS c(doc_hash, distance)"
        "          JOIN okg.node_documents AS d"
        "            ON d.set_id = %(set_id)s AND d.doc_hash = c.doc_hash"
        "           AND d.seq <= %(max_seq)s"
        "         GROUP BY d.node_id, d.row_hash, d.template_hash) AS l"
        # LATERAL + OFFSET 0: each candidate's row is looked up by node id
        # and its text value read once (see text_value_sql).
        f"  CROSS JOIN LATERAL (SELECT {_NODE_COLUMNS}, {text_value_sql('n')}"
        f"          FROM {quote_physical_table(read.physical_table)} AS n"
        "          WHERE n.node_id = l.node_id"
        f"            AND {row_hash_expression('n')} = l.row_hash"
        f"            AND {where} OFFSET 0) AS n"
        "  JOIN unnest(CAST(%(t_subtypes)s AS text[]), CAST(%(t_hashes)s AS text[]))"
        "       AS t(subtype, template_hash)"
        "    ON t.subtype = n.subtype AND t.template_hash = l.template_hash"
        f" WHERE {nonblank_body_sql('n')}"
        "   AND " + _no_newer_link(
            node="l.node_id", row_hash="l.row_hash", subtype="n.subtype", link="l",
        )
    )
    return statement, params
