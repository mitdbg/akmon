"""Phase A projector — dirty-set discovery + cache-gated extraction.

@implements_concept: principled-deferral

The projector discovers which nodes a publish cycle must re-examine and
runs the cache-gated extractor/linker stage over them, emitting
`edge_facts`. It no longer writes a live projection: the legacy
`okg.nodes_live` / `okg.edges_live` bitemporal tables this module used to
maintain were deleted by `schema-canonical-read-path`, having been empty
on every deployment since the Chronos cutover. Facts reach the graph
through `ChronosGraphStore.apply_facts`, and narrowing validation — the
`principled-deferral` this module used to perform at `edges_live` INSERT
time — is the publish gate's.

Run inside `Publisher.dispatch_projection(g)`:

  prev_watermark = last_published_fact_watermark()
  dirty = projector.discover_dirty(prev_watermark)
  batches = projector.partition(dirty, n_workers)
  results = projector.run_pool(g_next, batches, n_workers)
  # Pool join inside run_pool is the structural Phase-A/B fence.

Per-batch worker contract:

  - Opens its OWN psycopg connection (autocommit=True).
  - Wraps its work in `with conn.transaction():` (one short txn
    per batch — bounded by partition size, not graph size).
  - Honors `link_cache`: if a node's text_hash and active
    extractor/linker fingerprint are unchanged relative to the
    cached value the extractor is NOT invoked (M3.3 gating). On
    cache miss, the extractor runs and the LinkerRouter maps entities
    to derived edges via the four-field grammar (M3.4).

Source-watermark advancement: grouped by `node_facts.source`,
each batch updates `source_watermarks.projection_cursor` to the
max(fact_id) it observed for that source. Multiple batches racing
on the same source row use `GREATEST` so the cursor never
regresses. Per design.md "source watermarks".

Per-source partitioning (M3 strand fix, design.md §stranded-facts
option C, 2026-04-25): the partition step groups dirty node_ids
BY SOURCE so each batch contains node_ids from exactly one
source. Combined with the per-batch txn (which advances the
source's `projection_cursor` only on commit), partial-batch
failure cannot strand a source's facts: the failed batch's
source stays at its prior cursor; sibling batches for OTHER
sources commit independently. With the prior round-robin
partition, a single source could be split across N batches;
batch 1 commits cursor=X, batch 2 fails — cursor sits at X
while facts ≤ X were never projected, silently filtering them
out of subsequent `discover_dirty` calls.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import psycopg

from okg.substrate.projection.extractor import (
    EntityExtractor,
    NoOpExtractor,
)
from okg.substrate.projection.router import (
    AliasResolver,
    DerivedEdge,
    LinkerRouter,
    PgAliasResolver,
)
DEDUP_KEY_LOOKUP_CHUNK = 100


log = logging.getLogger(__name__)

# The commit-witness constants that stood here — `_PROJECTION_PROGRAM` and its
# `_PROJECTION_CHECKSUM` — stamped every LIVE ROW the projector wrote (PACT
# route-all-writers-through-transform-ir). `schema-canonical-read-path` deleted
# the live-table projection, so there is no row left to stamp and both globals
# were dead. `okg.substrate.transform.programs.projection_program` itself stays:
# transform/coverage.py still names it.


# Edge_id is BIGINT; mint deterministically from (src,edge_type,dst)
# so the extractor is idempotent across runs. Take the lower 63 bits
# of a uuid5 hash to stay positive.
def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(
        uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}",
    ).int
    return h & ((1 << 63) - 1)


def _extractor_fingerprint(
    extractor: EntityExtractor,
    routes: list,
    resolver_factory: Callable[[psycopg.Connection], AliasResolver],
    source_subtypes: Optional[tuple[str, ...]] = None,
) -> str:
    payload = {
        "extractor": (
            getattr(extractor, "fingerprint", None)
            or {
                "class": (
                    f"{extractor.__class__.__module__}."
                    f"{extractor.__class__.__qualname__}"
                ),
                "repr": repr(extractor),
            }
        ),
        "routes": [
            {
                "entity_type": r.entity_type,
                "src_subtype": r.src_subtype,
                "edge_type": r.edge_type,
                "dst_template": r.dst_template,
                "confidence": r.confidence,
            }
            for r in routes
        ],
        "resolver_factory": (
            f"{getattr(resolver_factory, '__module__', '')}."
            f"{getattr(resolver_factory, '__qualname__', repr(resolver_factory))}"
        ),
        "resolver_fingerprint": getattr(
            resolver_factory, "fingerprint", None,
        ),
        "source_subtypes": list(source_subtypes or ()),
    }
    encoded = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class BatchResult:
    """Outcome of one batch worker."""

    batch_index: int
    n_node_ids: int
    n_projected: int           # nodes the batch processed
    n_cache_hits: int          # nodes where text_hash matched link_cache
    n_cache_misses: int        # nodes where text_hash diverged (extractor invoked)
    n_edges_emitted: int = 0   # edge_facts written by the extractor
    sources_touched: dict[str, int] = field(default_factory=dict)  # source → max(fact_id)
    failed: bool = False
    error: Optional[str] = None


class Projector:
    """Stateless projector. Each method opens its own short-txn
    connection (or accepts one). Safe to share between threads.

    @implements_concept: principled-deferral

    What survives here is the dirty-set discovery and the cache-gated
    extractor/linker stage. The live-table projection half — narrowing
    validation deferred to an `okg.edges_live` INSERT, and the bitemporal
    `valid_from_gen` / `valid_to_gen` writes — was retired with the legacy
    live tables themselves (`schema-canonical-read-path`); the graph is
    Chronos interval storage, applied through
    `ChronosGraphStore.apply_facts`, and narrowing enforcement is the
    publish gate's.

    M3.3 wiring: pass an `extractor` (and optionally a routes list
    + resolver factory) to enable cache-miss extraction. The
    default `NoOpExtractor` preserves M3.2 behavior (no edges are
    emitted, no extractor is invoked, cache-miss does nothing
    beyond refreshing link_cache).
    """

    def __init__(
        self,
        dsn: str,
        *,
        extractor: Optional[EntityExtractor] = None,
        routes: Optional[list] = None,  # list[LinkerRoute]
        resolver_factory: Optional[
            Callable[[psycopg.Connection], AliasResolver]
        ] = None,
        deployment_name: str | None = None,
        extractor_source_subtypes: Optional[list[str]] = None,
    ) -> None:
        self.dsn = dsn
        self.deployment_name = deployment_name
        self._extractor = extractor or NoOpExtractor()
        self._routes = list(routes or [])
        # Always instantiate a router; an empty routes list produces
        # a router that returns None for every entity. Combined with
        # NoOpExtractor (empty extract()), the happy path is a no-op
        # without any type-dispatch gates.
        self._router = LinkerRouter(self._routes)
        self._resolver_factory = resolver_factory or PgAliasResolver
        self._extractor_source_subtypes = tuple(
            sorted(
                {
                    str(subtype).strip()
                    for subtype in (extractor_source_subtypes or [])
                    if str(subtype).strip()
                }
            )
        )
        self._extractor_source_subtype_set = set(
            self._extractor_source_subtypes
        )
        self._extractor_fingerprint = _extractor_fingerprint(
            self._extractor,
            self._routes,
            self._resolver_factory,
            self._extractor_source_subtypes,
        )
        self._tracks_extraction_cache = (
            bool(self._routes)
            or not isinstance(self._extractor, NoOpExtractor)
        )

    # ---------- discovery ----------

    def last_published_fact_watermark(
        self, conn: psycopg.Connection, deployment_name: str,
    ) -> int:
        """Highest fact_watermark (node_facts) across published
        generations for this deployment. Returns 0 if none
        published yet (first publish post-bootstrap).
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(max(fact_watermark), 0)
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                """,
                (deployment_name,),
            )
            return cur.fetchone()[0]

    def last_published_edge_fact_watermark(
        self, conn: psycopg.Connection, deployment_name: str,
    ) -> int:
        """Highest edge_fact_watermark across published
        generations for this deployment. Used by
        project_retractions to bound the scan of op='R' facts.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(max(edge_fact_watermark), 0)
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                """,
                (deployment_name,),
            )
            return cur.fetchone()[0]

    def discover_dirty(
        self,
        conn: psycopg.Connection,
        prev_watermark: int,
    ) -> list[str]:
        """Return distinct node_ids with new facts past the watermark.

        At first run after bootstrap, prev_watermark==0 selects the
        whole graph (matching bootstrap's projection). Subsequent
        runs see only incremental churn.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT node_id
                  FROM okg.node_facts
                 WHERE fact_id > %s
                """,
                (prev_watermark,),
            )
            dirty = {r[0] for r in cur.fetchall()}
            dirty.update(self._discover_stale_link_cache_nodes(cur))
            return sorted(dirty)

    def discover_dirty_by_source(
        self,
        conn: psycopg.Connection,
        prev_watermark: int,
    ) -> dict[str, list[str]]:
        """Return {source -> distinct node_ids} for facts past the
        watermark. Used by `dispatch_projection` to feed
        `partition_by_source`, which preserves source isolation
        across batches (the M3 strand fix).

        A node with facts from multiple sources appears under each
        of its sources — extraction is keyed on the node's own
        text_hash, so multi-source nodes converge regardless of
        which batch reaches them first.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, array_agg(DISTINCT node_id)
                  FROM okg.node_facts
                 WHERE fact_id > %s
                 GROUP BY source
                """,
                (prev_watermark,),
            )
            by_source = {row[0]: set(row[1]) for row in cur.fetchall()}
            for source, node_ids in self._discover_stale_link_cache_by_source(
                cur,
            ).items():
                by_source.setdefault(source, set()).update(node_ids)
            return {
                source: sorted(node_ids)
                for source, node_ids in by_source.items()
            }

    def _discover_stale_link_cache_nodes(
        self,
        cur: psycopg.Cursor,
    ) -> set[str]:
        if not self._tracks_extraction_cache:
            return set()
        subtype_clause = ""
        params: list[object] = [
            self._extractor_fingerprint,
            self.deployment_name or "default",
        ]
        if self._extractor_source_subtypes:
            subtype_clause = "AND nl.subtype = ANY(%s)"
            params = [
                list(self._extractor_source_subtypes),
                self._extractor_fingerprint,
                self.deployment_name or "default",
            ]
        # Chronos cutover (cascade step 6): the live node graph is
        # okg.graph_nodes (every row live; no valid_to_gen window).
        cur.execute(
            f"""
            SELECT nl.node_id
              FROM okg.graph_nodes nl
              LEFT JOIN okg.link_cache lc ON lc.node_id = nl.node_id
             WHERE nl.text_hash IS NOT NULL
               {subtype_clause}
               AND (
                 lc.node_id IS NULL
                 OR lc.text_hash IS DISTINCT FROM nl.text_hash
                 OR lc.extractor_fingerprint IS DISTINCT FROM %s
                 OR EXISTS (
                   SELECT 1 FROM okg.link_retry lr
                    WHERE lr.deployment_name = %s
                      AND lr.branch_name = 'default'
                      AND lr.node_id = nl.node_id
                      AND lr.ready_at IS NOT NULL
                 )
               )
            """,
            params,
        )
        return {row[0] for row in cur.fetchall()}

    def _discover_stale_link_cache_by_source(
        self,
        cur: psycopg.Cursor,
    ) -> dict[str, set[str]]:
        if not self._tracks_extraction_cache:
            return {}
        subtype_clause = ""
        params: list[object] = [
            self._extractor_fingerprint,
            self.deployment_name or "default",
        ]
        if self._extractor_source_subtypes:
            subtype_clause = "AND nl.subtype = ANY(%s)"
            params = [
                list(self._extractor_source_subtypes),
                self._extractor_fingerprint,
                self.deployment_name or "default",
            ]
        cur.execute(
            f"""
            WITH stale AS (
              SELECT nl.node_id
                FROM okg.graph_nodes nl
                LEFT JOIN okg.link_cache lc ON lc.node_id = nl.node_id
               WHERE nl.text_hash IS NOT NULL
                 {subtype_clause}
                 AND (
                   lc.node_id IS NULL
                   OR lc.text_hash IS DISTINCT FROM nl.text_hash
                   OR lc.extractor_fingerprint IS DISTINCT FROM %s
                   OR EXISTS (
                     SELECT 1 FROM okg.link_retry lr
                      WHERE lr.deployment_name = %s
                        AND lr.branch_name = 'default'
                        AND lr.node_id = nl.node_id
                        AND lr.ready_at IS NOT NULL
                   )
                 )
            ),
            latest_source AS (
              SELECT DISTINCT ON (nf.node_id)
                     nf.node_id, nf.source
                FROM okg.node_facts nf
                JOIN stale ON stale.node_id = nf.node_id
               ORDER BY nf.node_id, nf.fact_id DESC
            )
            SELECT COALESCE(source, '_unknown') AS source,
                   array_agg(node_id)
              FROM latest_source
             GROUP BY COALESCE(source, '_unknown')
            """,
            params,
        )
        return {row[0]: set(row[1]) for row in cur.fetchall()}

    # ---------- per-cycle source_watermarks sweep ----------

    def sweep_pending_watermarks(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> int:
        """Promote `okg.projection_pending` rows from this
        generation into `okg.source_watermarks.projection_cursor`
        and DELETE the consumed rows.

        Per `redesign-projection-watermarks` (2026-04-25):
        single UPDATE under the publisher's advisory lock; no
        per-row contention with concurrent project_batch
        workers because they only INSERT pending rows (each
        row is a fresh BIGSERIAL).

        Returns the number of distinct sources whose cursors
        advanced.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, projection_cursor)
                SELECT pp.source, max(pp.new_cursor)
                  FROM okg.projection_pending pp
                 WHERE pp.generation_id = %s
                 GROUP BY pp.source
                ON CONFLICT (source) DO UPDATE
                  SET projection_cursor = GREATEST(
                    okg.source_watermarks.projection_cursor,
                    EXCLUDED.projection_cursor
                  )
                """,
                (generation_id,),
            )
            n_sources = cur.rowcount
            cur.execute(
                "DELETE FROM okg.projection_pending "
                " WHERE generation_id = %s",
                (generation_id,),
            )
            return n_sources

    # ---------- retraction / edge projection: RETIRED ----------
    #
    # `project_retractions`, `project_new_edges`,
    # `project_cascade_retractions` and `retire_permanent_strandings` lived
    # here. All four read or wrote `okg.nodes_live` / `okg.edges_live` (and
    # the `okg.projectable_*` admission filters over them), which
    # `schema-canonical-read-path` deleted: Chronos interval storage is the
    # only graph storage in v1.0.
    #
    # They were already dead before the tables were: the chronos cutover
    # (cascade step 5) stopped calling them, because the live tables they
    # wrote were empty on every deployment — see the note at
    # `publisher.py::_cycle_chronos_native`. Retraction and edge application
    # are `ChronosGraphStore.apply_facts` now, and admission rejection safety
    # for committed derived facts is the derived-fact loader's (source,
    # run_id) envelope deny-list (`publisher.py::_load_unapplied_derived_facts`,
    # pinned by tests/substrate/test_admission_rejection_projection.py).

    # ---------- M3.3 extractor + edge emission ----------

    def _record_link_retry(
        self,
        cur: psycopg.Cursor,
        processed_node_ids: list[str],
        retry_rows: list[tuple[str, str, str]],
    ) -> None:
        """Record unresolved-route misses for this batch (#685, ruling 1).

        Runs in the SAME transaction as the link_cache stamp. First clears the
        retry rows for every node re-evaluated this batch (a node that now
        resolves drops out of the retry set), then inserts the current misses.
        The insert is idempotent on (node_id, entity_type, entity_value).
        """
        if processed_node_ids:
            cur.execute(
                "DELETE FROM okg.link_retry "
                "WHERE deployment_name = %s AND branch_name = 'default' "
                "AND node_id = ANY(%s)",
                (self.deployment_name or "default", processed_node_ids),
            )
        if retry_rows:
            cur.executemany(
                """
                INSERT INTO okg.link_retry
                  (deployment_name, branch_name, node_id, entity_type,
                   entity_value, ready_at)
                VALUES (%s, 'default', %s, %s, %s, NULL)
                ON CONFLICT
                  (deployment_name, branch_name, node_id, entity_type,
                   entity_value)
                DO UPDATE SET ready_at = NULL, recorded_at = now()
                """,
                [
                    (self.deployment_name or "default", *row)
                    for row in retry_rows
                ],
            )

    def _extract_and_emit_edges(
        self,
        cur: psycopg.Cursor,
        generation_id: int,
        miss_nodes: list[tuple[str, str, str]],
        batch_index: int,
    ) -> tuple[int, int]:
        """For each cache-miss node, run the extractor, route the
        entities, and emit edge_facts.

        Returns (n_edges_emitted, 0). Returns (0, 0) when no derived edges
        result — typical for NoOpExtractor or an empty router. The second
        element is vestigial: it counted rows written to okg.edges_live, and
        nothing projects to a live table any more. The publisher admits the
        emitted fact range and applies it to the Chronos graph.
        """
        if not miss_nodes:
            return 0, 0

        # AliasResolver borrows the same conn as the cursor.
        resolver = self._resolver_factory(cur.connection)
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        derived: list[DerivedEdge] = []
        # fix-projection-alias-identity #685 (ruling 1): a resolver miss on a
        # `{resolved}` route means the mentioner was projected before its
        # target's alias landed. We record (node_id, entity_type, entity_value)
        # in okg.link_retry, in THIS batch's transaction, right alongside the
        # link_cache stamp -- so an alias-miss is a retryable strand, not a
        # silently-cached "done". A later cycle in which the alias lands clears
        # exactly those nodes' link_cache rows (see the alias-landing hook).
        retry_rows: list[tuple[str, str, str]] = []
        for node_id, subtype, text in miss_nodes:
            if not text:
                continue
            entities = self._extractor.extract(text)
            node_misses: list[tuple[str, str]] = []
            for ent in entities:
                edge = self._router.route(
                    ent,
                    src_node_id=node_id,
                    src_subtype=subtype,
                    resolver=resolver,
                    resolver_misses=node_misses,
                )
                if edge is not None:
                    derived.append(edge)
            for entity_type, entity_value in node_misses:
                retry_rows.append((node_id, entity_type, entity_value))

        processed_node_ids = [n for n, _s, _t in miss_nodes]
        self._record_link_retry(cur, processed_node_ids, retry_rows)

        if not derived:
            return 0, 0

        # Dedupe within the batch on (src, edge_type, dst): the same
        # entity may be mentioned twice in the same text. Keep the
        # first occurrence (preserves extractor order).
        unique: dict[tuple[str, str, str], DerivedEdge] = {}
        for e in derived:
            unique.setdefault((e.src, e.edge_type, e.dst), e)

        # 1. INSERT edge_facts. The dedupe_key per edge is stable
        #    across generations for the same logical extracted edge.
        #    Extractor config changes can force re-extraction through
        #    link_cache invalidation; they must not churn already-live
        #    edges for unchanged (src, edge_type, dst) triples.
        #
        #    NOTE on the dedupe constraint shape: edge_facts is a
        #    Timescale hypertable with UNIQUE (dedupe_key,
        #    observed_at). An ON CONFLICT (dedupe_key) alone won't
        #    match (Timescale requires the partitioning column in
        #    the unique index). We therefore pre-filter with a
        #    SELECT on dedupe_key to avoid duplicates on re-run.
        dedupe_rows = [
            (
                src, etype, dst, e,
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"_extractor|{src}|{etype}|{dst}",
                ).hex,
            )
            for (src, etype, dst), e in unique.items()
        ]
        dedupe_keys = [row[4] for row in dedupe_rows]
        already_present: set[str] = set()
        for start in range(0, len(dedupe_keys), DEDUP_KEY_LOOKUP_CHUNK):
            chunk = dedupe_keys[start:start + DEDUP_KEY_LOOKUP_CHUNK]
            cur.execute(
                "SELECT DISTINCT dedupe_key FROM okg.edge_facts "
                "WHERE dedupe_key = ANY(%s)",
                (chunk,),
            )
            already_present.update(r[0] for r in cur.fetchall())
        edge_ids = [_mint_edge_id(src, etype, dst) for src, etype, dst in unique]
        # Chronos cutover (cascade step 6): the live edge graph is
        # okg.graph_edges (no bitemporal valid_to_gen window). Skip emitting a
        # fact for an edge already present in the live graph.
        cur.execute(
            """
            SELECT edge_id
              FROM okg.graph_edges
             WHERE edge_id = ANY(%s)
            """,
            (edge_ids,),
        )
        already_live = {row[0] for row in cur.fetchall()}

        to_insert = [
            row for row in dedupe_rows
            if row[4] not in already_present
            and _mint_edge_id(row[0], row[1], row[2]) not in already_live
        ]
        for src, etype, dst, e, dedupe_key in to_insert:
            edge_id = _mint_edge_id(src, etype, dst)
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s, %s, %s::jsonb,
                        'linked_heuristic', %s, %s,
                        %s::jsonb, %s::jsonb,
                        %s, %s, %s, 'I')
                """,
                (
                    edge_id, src, dst, etype,
                    json.dumps({
                        "match_type": f"{e.entity_type}_mention",
                        "entity_value": e.entity_value,
                    }),
                    e.confidence,
                    f"_extractor:{e.entity_type}",
                    json.dumps({
                        "src_node_id": src,
                        "entity_type": e.entity_type,
                        "entity_value": e.entity_value,
                    }),
                    json.dumps({"generation_id": generation_id}),
                    observed_at,
                    dedupe_key,
                    run_id,
                ),
            )
        n_edges_emitted = len(to_insert)
        # Chronos cutover (cascade step 5): the extractor only emits
        # edge_facts. Projection into the graph is done by the caller —
        # the publisher admits the fact range and applies it to the
        # Chronos graph (graph_edges) via apply_facts. The legacy direct
        # write into the live edges table was removed; this method never
        # projects, so the second return slot is always 0.
        return n_edges_emitted, 0
