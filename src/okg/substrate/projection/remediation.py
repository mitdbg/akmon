"""Bounded remediation for pre-fix projection-identity damage.

Per `fix-projection-alias-identity` (ruling 4): three passes, each DRY-RUN by
default and mutating only under an explicit `apply=True` (umbrella point (d):
destructive verbs default to dry-run). Each has a stated bound.

(a) `remediate_stranded_link_cache` (#685): a mentioner stranded BEFORE the
    retry mechanism existed left no `okg.link_retry` record, so precise
    enumeration is impossible. The sound pass is a one-time `okg.link_cache`
    invalidation scoped to the extractor's source subtypes (the only nodes
    extraction ever runs on), optionally `--limit`-capped, drained by normal
    projection cycles through the existing cache-miss machinery. Bound =
    link_cache rows in those subtypes.

(b) `remediate_collapsed_aliases` (#686): detect collisions over the current
    graph per (subtype, alias-field); the fail-closed disposition quarantines
    the ambiguous alias rows (delete at the active cv so no new wrong link
    mints) and invalidates link_cache for the src nodes of derived edges minted
    through those values (so the retry mechanism owns them as visibly
    unresolved). Bound = O(colliding values) + O(edges carrying those values).

(c) `enumerate_multi_target_edges` (#710): ENUMERATION-ONLY. Reports every
    offending (src, edge_type) group with all targets and provenance; it NEVER
    auto-picks a survivor (auto-pick by any rule is the original consumer-side
    dict-collapse disease). The deployment owner retracts the wrong edges using
    the report, then applies the annotated catalog so the gate blocks
    recurrence. Bound = a read.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import uuid
from typing import Any, Iterable, Optional

import psycopg

from okg.substrate.catalog.aliases import (
    active_catalog_version_id,
    seed_from_subtype,
)
from okg.substrate.transform.consistency import cardinality_constrained_binding


__all__ = [
    "backfill_link_endpoints",
    "EntityMergePlan",
    "MultiTargetGroup",
    "RemediationResult",
    "enumerate_multi_target_edges",
    "mark_entity_merge_published",
    "plan_entity_merge",
    "remediate_entity_merge",
    "remediate_collapsed_aliases",
    "remediate_stranded_link_cache",
]

# Issue #796 extends this remediation surface without adding more unrelated
# logic to the historical alias/cardinality module.
from okg.substrate.entity_merge import (  # noqa: E402
    EntityMergePlan,
    mark_entity_merge_published,
    plan_entity_merge,
    remediate_entity_merge,
)


@dataclass
class RemediationResult:
    """Outcome of a remediation pass. `applied=False` is a dry-run: the counts
    describe what WOULD change and nothing is mutated."""

    pass_name: str
    applied: bool
    affected_count: int
    details: dict[str, Any] = field(default_factory=dict)


def remediate_stranded_link_cache(
    conn: psycopg.Connection,
    *,
    extractor_source_subtypes: Iterable[str],
    deployment_name: str = "default",
    branch_name: str = "default",
    cursor: Optional[str] = None,
    apply: bool = False,
    limit: Optional[int] = None,
) -> RemediationResult:
    """Page historical source nodes into the production extractor queue."""
    subtypes = sorted({
        str(s).strip() for s in extractor_source_subtypes if str(s).strip()
    })
    if not subtypes:
        return RemediationResult(
            pass_name="stranded_link_cache",
            applied=apply,
            affected_count=0,
            details={"reason": "no extractor source subtypes supplied"},
        )
    if limit is None or not 1 <= int(limit) <= 1000:
        raise ValueError("remediation requires --limit between 1 and 1000")
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout(branch=branch_name)
    rows = graph.query(
        """
        SELECT node_id
          FROM okg.graph_nodes
         WHERE subtype = ANY(:subtypes)
           AND attrs ? 'text'
           AND node_id > :cursor
         ORDER BY node_id
         LIMIT :page_limit
        """,
        {
            "subtypes": subtypes,
            "cursor": cursor or "",
            "page_limit": int(limit) + 1,
        },
    )
    selected = [str(row["node_id"]) for row in rows[: int(limit)]]
    next_cursor = selected[-1] if len(rows) > int(limit) and selected else None
    inserted = 0
    if apply and selected:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.extractor_retry_queue "
                "(deployment_name, branch_name, node_id) VALUES (%s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                [(deployment_name, branch_name, node_id) for node_id in selected],
            )
            inserted = cur.rowcount
    return RemediationResult(
        pass_name="stranded_link_cache",
        applied=apply,
        affected_count=len(selected),
        details={
            "deployment_name": deployment_name,
            "branch_name": branch_name,
            "subtypes": subtypes,
            "limit": int(limit),
            "input_cursor": cursor,
            "next_cursor": next_cursor,
            "node_ids": selected,
            "queued_count": inserted if apply else 0,
        },
    )


def backfill_link_endpoints(
    conn: psycopg.Connection,
    *,
    entity_types: Iterable[str],
    deployment_name: str,
    branch_name: str = "default",
    cursor: Optional[str] = None,
    apply: bool = False,
    limit: Optional[int] = None,
) -> RemediationResult:
    """Queue the records whose misses predate a route's `on_miss` (#3666).

    `deployment_name` has no default: retry rows are keyed by deployment,
    and a wrong name would quietly find nothing.

    Switching `on_miss` on does not re-extract every text record (it is not
    part of the extractor fingerprint). The records that already missed are
    exactly the ones holding unready `okg.link_retry` rows of the route's
    entity type, so this pages those records into the extractor queue; the
    next publish re-extracts them and writes their endpoints. Bound = the
    distinct records with such rows, `limit` (1 to 1000) per call.
    """
    if not str(deployment_name or "").strip():
        raise ValueError("link-endpoint backfill needs --deployment-name")
    types = sorted({str(t).strip() for t in entity_types if str(t).strip()})
    if not types:
        raise ValueError("link-endpoint backfill needs at least one --entity-type")
    if limit is None or not 1 <= int(limit) <= 1000:
        raise ValueError("remediation requires --limit between 1 and 1000")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT node_id
              FROM okg.link_retry
             WHERE deployment_name = %s
               AND branch_name = %s
               AND entity_type = ANY(%s)
               AND ready_at IS NULL
               AND node_id > %s
             ORDER BY node_id
             LIMIT %s
            """,
            (deployment_name, branch_name, types, cursor or "", int(limit) + 1),
        )
        rows = [str(row[0]) for row in cur.fetchall()]
    selected = rows[: int(limit)]
    next_cursor = selected[-1] if len(rows) > int(limit) and selected else None
    inserted = 0
    if apply and selected:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.extractor_retry_queue "
                "(deployment_name, branch_name, node_id) VALUES (%s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                [(deployment_name, branch_name, node_id) for node_id in selected],
            )
            inserted = cur.rowcount
    return RemediationResult(
        pass_name="link_endpoints",
        applied=apply,
        affected_count=len(selected),
        details={
            "deployment_name": deployment_name,
            "branch_name": branch_name,
            "entity_types": types,
            "limit": int(limit),
            "input_cursor": cursor,
            "next_cursor": next_cursor,
            "node_ids": selected,
            "queued_count": inserted if apply else 0,
        },
    )


def remediate_collapsed_aliases(
    conn: psycopg.Connection,
    *,
    subtype: str,
    field: str,
    entity_type: Optional[str] = None,
    deployment_name: str = "default",
    branch_name: str = "default",
    run_id: Optional[str] = None,
    cursor: Optional[str] = None,
    limit: Optional[int] = None,
    apply: bool = False,
) -> RemediationResult:
    """Quarantine collisions and page wrong extractor edges into repair work."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.errors import SubstrateError

    if limit is None or not 1 <= int(limit) <= 1000:
        raise ValueError("remediation requires --limit between 1 and 1000")

    cv = active_catalog_version_id(conn)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)

    def select_page(
        graph: Any,
        colliding_values: list[str],
    ) -> list[dict[str, Any]]:
        if not colliding_values:
            return []
        edge_predicate = (
            "e.attrs->>'entity_value' = ANY(:vals) "
            "AND ((e.attrs->>'entity_value') > :cursor_value OR "
            "((e.attrs->>'entity_value') = :cursor_value "
            "AND e.edge_id > :cursor_edge_id)) "
        )
        edge_params: dict[str, Any] = {
            "vals": colliding_values,
            "cursor_value": cursor_value,
            "cursor_edge_id": cursor_edge_id,
            "page_limit": int(limit) + 1,
        }
        if entity_type is None:
            # Historical collapse remediation owns only extractor-derived
            # edges. Explicit/source-owned edges may carry the same attrs.
            edge_predicate += "AND left(e.source, 11) = '_extractor:'"
        else:
            edge_predicate += "AND e.source = :src"
            edge_params["src"] = f"_extractor:{entity_type}"
        return graph.query(
            "SELECT e.edge_id, e.src, e.dst, e.edge_type, e.source, "
            "e.attrs->>'entity_value' AS entity_value "
            "FROM okg.graph_edges e WHERE " + edge_predicate +
            " ORDER BY entity_value, e.edge_id LIMIT :page_limit",
            edge_params,
        )

    cursor_value, cursor_edge_id = "", -1
    if cursor:
        decoded = json.loads(cursor)
        cursor_value, cursor_edge_id = str(decoded[0]), int(decoded[1])

    def batch_details(
        *,
        collision_snapshot: list[dict[str, Any]],
        pinned_checkpoint: str,
        edge_rows: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], Optional[str]]:
        page_rows = edge_rows[: int(limit)]
        affected_src = sorted({str(row["src"]) for row in page_rows})
        next_cursor = None
        if len(edge_rows) > int(limit) and page_rows:
            last = page_rows[-1]
            next_cursor = json.dumps(
                [str(last["entity_value"]), int(last["edge_id"])],
                separators=(",", ":"),
            )
        details: dict[str, Any] = {
            "catalog_version_id": cv,
            "deployment_name": deployment_name,
            "branch_name": branch_name,
            "pinned_checkpoint": pinned_checkpoint,
            "subtype": subtype,
            "field": field,
            "collisions": collision_snapshot,
            "derived_edge_count": len(page_rows),
            "affected_src_node_ids": affected_src,
            "input_cursor": cursor,
            "next_cursor": next_cursor,
            "limit": int(limit),
            "selected_edge_ids": [int(row["edge_id"]) for row in page_rows],
        }
        return details, page_rows, affected_src, next_cursor

    latest = store.latest_generation(
        deployment_name=deployment_name,
        branch=branch_name,
        status="published",
    )
    if latest is None and apply:
        raise SubstrateError(
            "collapsed-alias remediation requires a published checkpoint",
            data={"deployment_name": deployment_name, "branch_name": branch_name},
        )
    initial_pin = (
        str(latest.checkpoint_id) if latest is not None else "unpublished-head"
    )

    if not apply:
        graph = (
            store.checkout(branch_name)
            if latest is None
            else store.checkout_generation(initial_pin, branch=branch_name)
        )
        preview = seed_from_subtype(
            conn,
            subtype=subtype,
            field=field,
            apply=False,
            branch_name=branch_name,
            _cv_override=cv,
            _graph_override=graph,
        )
        collision_snapshot = [asdict(c) for c in preview.collisions]
        details, _page, _src, _next = batch_details(
            collision_snapshot=collision_snapshot,
            pinned_checkpoint=initial_pin,
            edge_rows=select_page(
                graph,
                [str(c["alias_value"]) for c in collision_snapshot],
            ),
        )
        return RemediationResult(
            pass_name="collapsed_aliases",
            applied=False,
            affected_count=len(collision_snapshot),
            details=details,
        )

    effective_entity_type = entity_type or "*"
    transactional_store = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    )
    session = transactional_store.checkout(branch_name)
    with conn.transaction():
        # One branch lock and one SQL transaction cover the pin, quarantine,
        # queue, graph retractions, and durable receipt. A fault at any point
        # rolls the whole batch back, including Chronos edits.
        transactional_store._lock_branch_write(branch_name)
        quarantine_count = 0
        with conn.cursor() as cur:
            new_run = False
            if run_id is None:
                cv = active_catalog_version_id(conn)
                latest = transactional_store.latest_generation(
                    deployment_name=deployment_name,
                    branch=branch_name,
                    status="published",
                )
                if latest is None:
                    raise SubstrateError(
                        "collapsed-alias remediation lost its published pin"
                    )
                pinned_checkpoint = str(latest.checkpoint_id)
                cur.execute(
                    "SELECT run_id, collision_snapshot "
                    "FROM okg.projection_remediation_runs "
                    "WHERE remediation_kind = 'collapsed_aliases' "
                    "AND deployment_name = %s AND branch_name = %s "
                    "AND subtype = %s AND field = %s AND entity_type = %s "
                    "AND pinned_checkpoint = %s AND catalog_version_id = %s",
                    (
                        deployment_name, branch_name, subtype, field,
                        effective_entity_type, pinned_checkpoint, cv,
                    ),
                )
                existing_run = cur.fetchone()
                if existing_run is None:
                    new_run = True
                else:
                    run_uuid = str(existing_run[0])
                    collision_snapshot = list(existing_run[1] or [])
            else:
                run_uuid = str(uuid.UUID(run_id))
                cur.execute(
                    "SELECT deployment_name, branch_name, subtype, field, "
                    "entity_type, pinned_checkpoint, catalog_version_id, "
                    "collision_snapshot "
                    "FROM okg.projection_remediation_runs WHERE run_id = %s",
                    (run_uuid,),
                )
                run_row = cur.fetchone()
                if run_row is None:
                    raise SubstrateError(
                        "unknown projection remediation run",
                        data={"run_id": run_uuid},
                    )
                expected = (
                    deployment_name, branch_name, subtype, field,
                    effective_entity_type,
                )
                if tuple(map(str, run_row[:5])) != expected:
                    raise SubstrateError(
                        "remediation run scope does not match request",
                        data={"run_id": run_uuid, "expected": list(expected)},
                    )
                pinned_checkpoint = str(run_row[5])
                cv = int(run_row[6])
                collision_snapshot = list(run_row[7] or [])

        if new_run:
            pinned_graph = transactional_store.checkout_generation(
                pinned_checkpoint, branch=branch_name,
            )
            quarantine = seed_from_subtype(
                conn,
                subtype=subtype,
                field=field,
                apply=True,
                branch_name=branch_name,
                _cv_override=cv,
                _graph_override=pinned_graph,
            )
            quarantine_count = quarantine.quarantined_count
            collision_snapshot = [asdict(c) for c in quarantine.collisions]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.projection_remediation_runs
                      (remediation_kind, deployment_name, branch_name,
                       pinned_checkpoint, subtype, field, entity_type,
                       catalog_version_id, collision_snapshot)
                    VALUES ('collapsed_aliases', %s, %s, %s, %s, %s, %s,
                            %s, %s::jsonb)
                    RETURNING run_id
                    """,
                    (
                        deployment_name, branch_name, pinned_checkpoint,
                        subtype, field, effective_entity_type, cv,
                        json.dumps(collision_snapshot),
                    ),
                )
                run_uuid = str(cur.fetchone()[0])

        with conn.cursor() as cur:
            idempotency_key = hashlib.sha256(
                f"{deployment_name}|{branch_name}|{run_uuid}|{cursor or ''}|{limit}".encode()
            ).hexdigest()
            cur.execute(
                "SELECT receipt FROM okg.projection_remediation_batches "
                "WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            prior = cur.fetchone()
            if prior is not None:
                prior_details = dict(prior[0])
                return RemediationResult(
                    pass_name="collapsed_aliases",
                    applied=True,
                    affected_count=len(prior_details.get("collisions", [])),
                    details=prior_details,
                )

        colliding_values = [str(c["alias_value"]) for c in collision_snapshot]
        # A resumed page uses the persisted run snapshot and reasserts its
        # quarantine directly. It never re-derives the repair set from HEAD.
        if colliding_values:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM okg.aliases "
                    "WHERE catalog_version_id = %s AND alias = ANY(%s)",
                    (cv, colliding_values),
                )
                quarantine_count += cur.rowcount

        graph = transactional_store.checkout_generation(
            pinned_checkpoint, branch=branch_name,
        )
        details, page_rows, affected_src, next_cursor = batch_details(
            collision_snapshot=collision_snapshot,
            pinned_checkpoint=pinned_checkpoint,
            edge_rows=select_page(graph, colliding_values),
        )
        details["aliases_quarantined"] = quarantine_count

        with conn.cursor() as cur:
            if affected_src:
                cur.executemany(
                    "INSERT INTO okg.extractor_retry_queue "
                    "(deployment_name, branch_name, node_id, reason) "
                    "VALUES (%s, %s, %s, 'collapsed_alias') "
                    "ON CONFLICT DO NOTHING",
                    [
                        (deployment_name, branch_name, node_id)
                        for node_id in affected_src
                    ],
                )

        retracted_count = 0
        for source in sorted({str(row["source"]) for row in page_rows}):
            source_rows = [row for row in page_rows if str(row["source"]) == source]
            from okg.substrate.fact_application import apply_bounded_fact_batch

            result = apply_bounded_fact_batch(
                conn,
                transactional_store,
                [
                    EdgeFact(
                        str(row["src"]), str(row["dst"]), str(row["edge_type"]),
                        source_record_id={
                            "remediation_run_id": run_uuid,
                            "edge_id": int(row["edge_id"]),
                        },
                        source_revision={"cursor": cursor, "limit": int(limit)},
                        op="R",
                    )
                    for row in source_rows
                ],
                deployment_name=deployment_name,
                producer_id="system:projection_remediation",
                producer_kind="system",
                intent="collapsed_alias_remediation",
                catalog_version_id=cv,
                authority_scope={
                    "pinned_checkpoint": pinned_checkpoint,
                    "source": source,
                    "cursor": cursor,
                    "limit": int(limit),
                },
                source=None,
                target_branch=branch_name,
                run_id=run_uuid,
                completion="partial",
                retractions_allowed=True,
                cross_source_retractions=True,
                strict_endpoints=False,
                session=session,
            )
            retracted_count += result.primitive_total("edges_deleted")

        details.update({
            "run_id": run_uuid,
            "idempotency_key": idempotency_key,
            "retracted_edge_count": retracted_count,
            "queued_source_count": len(affected_src),
        })
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.projection_remediation_batches "
                "(idempotency_key, run_id, input_cursor, requested_limit, receipt) "
                "VALUES (%s, %s, %s, %s, %s::jsonb)",
                (
                    idempotency_key, run_uuid, cursor, int(limit),
                    json.dumps(details),
                ),
            )
            if next_cursor is None:
                if affected_src:
                    cur.execute(
                        "UPDATE okg.projection_remediation_runs "
                        "SET status = 'awaiting_publish' WHERE run_id = %s",
                        (run_uuid,),
                    )
                else:
                    cur.execute(
                        "UPDATE okg.projection_remediation_runs "
                        "SET status = 'completed', completed_at = now() "
                        "WHERE run_id = %s",
                        (run_uuid,),
                    )

    return RemediationResult(
        pass_name="collapsed_aliases",
        applied=True,
        affected_count=len(collision_snapshot),
        details=details,
    )


@dataclass
class MultiTargetGroup:
    """One offending (src, edge_type) group under an at-most-one-target
    narrowing, with all its targets and each edge's provenance."""

    src: str
    edge_type: str
    targets: list[dict[str, Any]]  # [{dst, edge_id, source, provenance}, ...]


def enumerate_multi_target_edges(
    conn: psycopg.Connection,
    *,
    catalog_version_id: Optional[int] = None,
    cursor: Optional[str] = None,
    limit: int = 100,
) -> list[MultiTargetGroup]:
    """(c) Enumeration-only #710 report: every (src, edge_type) group with >1
    distinct target under an at-most-one-target narrowing, with provenance.
    NEVER auto-picks a survivor. Read-only."""
    from okg.substrate.graph_store import ChronosGraphStore

    cv = (
        catalog_version_id
        if catalog_version_id is not None
        else active_catalog_version_id(conn)
    )
    constrained = cardinality_constrained_binding(cv)
    if not 1 <= int(limit) <= 1001:
        raise ValueError("multi-target report limit must be between 1 and 1001")
    bounded_limit = int(limit)
    cursor_src, cursor_edge_type = "", ""
    if cursor:
        decoded = json.loads(cursor)
        cursor_src, cursor_edge_type = str(decoded[0]), str(decoded[1])
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout()
    rows = [
        (r["src"], r["edge_type"], r["dst"], r["edge_id"], r["source"],
         r["provenance"])
        for r in graph.query(
            f"""
            WITH constrained AS {constrained},
            violating AS (
              SELECT e.src, e.edge_type
                FROM okg.graph_edges e
                JOIN okg.graph_nodes ns ON ns.node_id = e.src
                JOIN constrained c
                  ON c.edge_type = e.edge_type
                 AND c.src_subtype = ns.subtype
               GROUP BY e.src, e.edge_type
              HAVING count(DISTINCT e.dst) > 1
            ), grp AS (
              SELECT src, edge_type FROM violating
               WHERE (src > :cursor_src)
                  OR (src = :cursor_src AND edge_type > :cursor_edge_type)
               ORDER BY src, edge_type
               LIMIT :page_limit
            )
            SELECT e.src, e.edge_type, e.dst, e.edge_id,
                   e.source, e.provenance
              FROM okg.graph_edges e
              JOIN grp g ON g.src = e.src AND g.edge_type = e.edge_type
             ORDER BY e.src, e.edge_type, e.dst
            """,
            {
                "cursor_src": cursor_src,
                "cursor_edge_type": cursor_edge_type,
                "page_limit": bounded_limit,
            },
        )
    ]
    groups: dict[tuple[str, str], MultiTargetGroup] = {}
    for src, edge_type, dst, edge_id, source, provenance in rows:
        key = (str(src), str(edge_type))
        grp = groups.get(key)
        if grp is None:
            grp = MultiTargetGroup(src=str(src), edge_type=str(edge_type), targets=[])
            groups[key] = grp
        grp.targets.append({
            "dst": str(dst),
            "edge_id": int(edge_id),
            "source": source,
            "provenance": provenance,
        })
    return list(groups.values())
